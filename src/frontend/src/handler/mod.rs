// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream};
use futures::{Stream, StreamExt};
use pgwire::pg_response::StatementType::{self, ABORT, BEGIN, COMMIT, ROLLBACK, START_TRANSACTION};
use pgwire::pg_response::{PgResponse, PgResponseBuilder, RowSetResult};
use pgwire::pg_server::BoxedError;
use pgwire::types::{Format, Row};
use risingwave_common::bail_not_implemented;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::*;

use self::util::{DataChunkToRowSetAdapter, SourceSchemaCompatExt};
use self::variable::handle_set_time_zone;
use crate::catalog::table_catalog::TableType;
use crate::handler::cancel_job::handle_cancel;
use crate::handler::kill_process::handle_kill;
use crate::scheduler::{DistributedQueryStream, LocalQueryStream};
use crate::session::SessionImpl;
use crate::utils::WithOptions;

mod alter_owner;
mod alter_parallelism;
mod alter_rename;
mod alter_set_schema;
mod alter_source_column;
mod alter_source_with_sr;
mod alter_system;
mod alter_table_column;
pub mod alter_user;
pub mod cancel_job;
mod comment;
pub mod create_connection;
mod create_database;
pub mod create_function;
pub mod create_index;
pub mod create_mv;
pub mod create_schema;
pub mod create_sink;
pub mod create_source;
pub mod create_sql_function;
pub mod create_table;
pub mod create_table_as;
pub mod create_user;
pub mod create_view;
mod describe;
mod drop_connection;
mod drop_database;
pub mod drop_function;
mod drop_index;
pub mod drop_mv;
mod drop_schema;
pub mod drop_sink;
pub mod drop_source;
pub mod drop_table;
pub mod drop_user;
mod drop_view;
pub mod explain;
pub mod extended_handle;
mod flush;
pub mod handle_privilege;
mod kill_process;
pub mod privilege;
pub mod query;
mod show;
mod transaction;
pub mod util;
pub mod variable;
mod wait;

/// The [`PgResponseBuilder`] used by RisingWave.
pub type RwPgResponseBuilder = PgResponseBuilder<PgResponseStream>;

/// The [`PgResponse`] used by RisingWave.
pub type RwPgResponse = PgResponse<PgResponseStream>;

pub enum PgResponseStream {
    LocalQuery(DataChunkToRowSetAdapter<LocalQueryStream>),
    DistributedQuery(DataChunkToRowSetAdapter<DistributedQueryStream>),
    Rows(BoxStream<'static, RowSetResult>),
}

impl Stream for PgResponseStream {
    type Item = std::result::Result<Vec<Row>, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut *self {
            PgResponseStream::LocalQuery(inner) => inner.poll_next_unpin(cx),
            PgResponseStream::DistributedQuery(inner) => inner.poll_next_unpin(cx),
            PgResponseStream::Rows(inner) => inner.poll_next_unpin(cx),
        }
    }
}

impl From<Vec<Row>> for PgResponseStream {
    fn from(rows: Vec<Row>) -> Self {
        Self::Rows(stream::iter(vec![Ok(rows)]).boxed())
    }
}

#[derive(Clone)]
pub struct HandlerArgs {
    pub session: Arc<SessionImpl>,
    pub sql: Arc<str>,
    pub normalized_sql: String,
    pub with_options: WithOptions,
}

impl HandlerArgs {
    pub fn new(session: Arc<SessionImpl>, stmt: &Statement, sql: Arc<str>) -> Result<Self> {
        Ok(Self {
            session,
            sql,
            with_options: WithOptions::try_from(stmt)?,
            normalized_sql: Self::normalize_sql(stmt),
        })
    }

    /// Get normalized SQL from the statement.
    ///
    /// - Generally, the normalized SQL is the unparsed (and formatted) result of the statement.
    /// - For `CREATE` statements, the clauses like `OR REPLACE` and `IF NOT EXISTS` are removed to
    ///   make it suitable for the `SHOW CREATE` statements.
    fn normalize_sql(stmt: &Statement) -> String {
        let mut stmt = stmt.clone();
        match &mut stmt {
            Statement::CreateView { or_replace, .. } => {
                *or_replace = false;
            }
            Statement::CreateTable {
                or_replace,
                if_not_exists,
                ..
            } => {
                *or_replace = false;
                *if_not_exists = false;
            }
            Statement::CreateIndex { if_not_exists, .. } => {
                *if_not_exists = false;
            }
            Statement::CreateSource {
                stmt: CreateSourceStatement { if_not_exists, .. },
                ..
            } => {
                *if_not_exists = false;
            }
            Statement::CreateSink {
                stmt: CreateSinkStatement { if_not_exists, .. },
            } => {
                *if_not_exists = false;
            }
            Statement::CreateConnection {
                stmt: CreateConnectionStatement { if_not_exists, .. },
            } => {
                *if_not_exists = false;
            }
            _ => {}
        }
        stmt.to_string()
    }
}

pub async fn handle(
    session: Arc<SessionImpl>,
    stmt: Statement,
    sql: Arc<str>,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    session.clear_cancel_query_flag();
    let _guard = session.txn_begin_implicit();
    let handler_args = HandlerArgs::new(session, &stmt, sql)?;

    match stmt {
        Statement::Explain {
            statement,
            analyze,
            options,
        } => explain::handle_explain(handler_args, *statement, options, analyze).await,
        Statement::CreateSource { stmt } => {
            create_source::handle_create_source(handler_args, stmt).await
        }
        Statement::CreateSink { stmt } => create_sink::handle_create_sink(handler_args, stmt).await,
        Statement::CreateConnection { stmt } => {
            create_connection::handle_create_connection(handler_args, stmt).await
        }
        Statement::CreateFunction {
            or_replace,
            temporary,
            name,
            args,
            returns,
            params,
            with_options,
        } => {
            // For general udf, `language` clause could be ignored
            // refer: https://github.com/risingwavelabs/risingwave/pull/10608
            if params.language.is_none()
                || !params
                    .language
                    .as_ref()
                    .unwrap()
                    .real_value()
                    .eq_ignore_ascii_case("sql")
            {
                // User defined function with external source (e.g., language [ python / java ])
                create_function::handle_create_function(
                    handler_args,
                    or_replace,
                    temporary,
                    name,
                    args,
                    returns,
                    params,
                    with_options,
                )
                .await
            } else {
                create_sql_function::handle_create_sql_function(
                    handler_args,
                    or_replace,
                    temporary,
                    name,
                    args,
                    returns,
                    params,
                )
                .await
            }
        }
        Statement::CreateTable {
            name,
            columns,
            wildcard_idx,
            constraints,
            query,
            with_options: _, // It is put in OptimizerContext
            // Not supported things
            or_replace,
            temporary,
            if_not_exists,
            source_schema,
            source_watermarks,
            append_only,
            cdc_table_info,
            include_column_options,
        } => {
            if or_replace {
                bail_not_implemented!("CREATE OR REPLACE TABLE");
            }
            if temporary {
                bail_not_implemented!("CREATE TEMPORARY TABLE");
            }
            if let Some(query) = query {
                return create_table_as::handle_create_as(
                    handler_args,
                    name,
                    if_not_exists,
                    query,
                    columns,
                    append_only,
                )
                .await;
            }
            let source_schema = source_schema.map(|s| s.into_v2_with_warning());
            create_table::handle_create_table(
                handler_args,
                name,
                columns,
                wildcard_idx,
                constraints,
                if_not_exists,
                source_schema,
                source_watermarks,
                append_only,
                cdc_table_info,
                include_column_options,
            )
            .await
        }
        Statement::CreateDatabase {
            db_name,
            if_not_exists,
        } => create_database::handle_create_database(handler_args, db_name, if_not_exists).await,
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
        } => create_schema::handle_create_schema(handler_args, schema_name, if_not_exists).await,
        Statement::CreateUser(stmt) => create_user::handle_create_user(handler_args, stmt).await,
        Statement::AlterUser(stmt) => alter_user::handle_alter_user(handler_args, stmt).await,
        Statement::Grant { .. } => {
            handle_privilege::handle_grant_privilege(handler_args, stmt).await
        }
        Statement::Revoke { .. } => {
            handle_privilege::handle_revoke_privilege(handler_args, stmt).await
        }
        Statement::Describe { name } => describe::handle_describe(handler_args, name),
        Statement::ShowObjects {
            object: show_object,
            filter,
        } => show::handle_show_object(handler_args, show_object, filter).await,
        Statement::ShowCreateObject { create_type, name } => {
            show::handle_show_create_object(handler_args, create_type, name)
        }
        Statement::ShowTransactionIsolationLevel => {
            transaction::handle_show_isolation_level(handler_args)
        }
        Statement::Drop(DropStatement {
            object_type,
            object_name,
            if_exists,
            drop_mode,
        }) => {
            let mut cascade = false;
            if let AstOption::Some(DropMode::Cascade) = drop_mode {
                match object_type {
                    ObjectType::MaterializedView
                    | ObjectType::View
                    | ObjectType::Sink
                    | ObjectType::Source
                    | ObjectType::Index
                    | ObjectType::Table => {
                        cascade = true;
                    }
                    ObjectType::Schema
                    | ObjectType::Database
                    | ObjectType::User
                    | ObjectType::Connection => {
                        bail_not_implemented!("DROP CASCADE");
                    }
                };
            };
            match object_type {
                ObjectType::Table => {
                    drop_table::handle_drop_table(handler_args, object_name, if_exists, cascade)
                        .await
                }
                ObjectType::MaterializedView => {
                    drop_mv::handle_drop_mv(handler_args, object_name, if_exists, cascade).await
                }
                ObjectType::Index => {
                    drop_index::handle_drop_index(handler_args, object_name, if_exists, cascade)
                        .await
                }
                ObjectType::Source => {
                    drop_source::handle_drop_source(handler_args, object_name, if_exists, cascade)
                        .await
                }
                ObjectType::Sink => {
                    drop_sink::handle_drop_sink(handler_args, object_name, if_exists, cascade).await
                }
                ObjectType::Database => {
                    drop_database::handle_drop_database(
                        handler_args,
                        object_name,
                        if_exists,
                        drop_mode.into(),
                    )
                    .await
                }
                ObjectType::Schema => {
                    drop_schema::handle_drop_schema(
                        handler_args,
                        object_name,
                        if_exists,
                        drop_mode.into(),
                    )
                    .await
                }
                ObjectType::User => {
                    drop_user::handle_drop_user(
                        handler_args,
                        object_name,
                        if_exists,
                        drop_mode.into(),
                    )
                    .await
                }
                ObjectType::View => {
                    drop_view::handle_drop_view(handler_args, object_name, if_exists, cascade).await
                }
                ObjectType::Connection => {
                    drop_connection::handle_drop_connection(handler_args, object_name, if_exists)
                        .await
                }
            }
        }
        // XXX: should we reuse Statement::Drop for DROP FUNCTION?
        Statement::DropFunction {
            if_exists,
            func_desc,
            option,
        } => drop_function::handle_drop_function(handler_args, if_exists, func_desc, option).await,
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => query::handle_query(handler_args, stmt, formats).await,
        Statement::CreateView {
            materialized,
            if_not_exists,
            name,
            columns,
            query,
            with_options: _, // It is put in OptimizerContext
            or_replace,      // not supported
            emit_mode,
        } => {
            if or_replace {
                bail_not_implemented!("CREATE OR REPLACE VIEW");
            }
            if materialized {
                create_mv::handle_create_mv(
                    handler_args,
                    if_not_exists,
                    name,
                    *query,
                    columns,
                    emit_mode,
                )
                .await
            } else {
                create_view::handle_create_view(handler_args, if_not_exists, name, columns, *query)
                    .await
            }
        }
        Statement::Flush => flush::handle_flush(handler_args).await,
        Statement::Wait => wait::handle_wait(handler_args).await,
        Statement::SetVariable {
            local: _,
            variable,
            value,
        } => variable::handle_set(handler_args, variable, value),
        Statement::SetTimeZone { local: _, value } => handle_set_time_zone(handler_args, value),
        Statement::ShowVariable { variable } => variable::handle_show(handler_args, variable).await,
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            include,
            distributed_by,
            unique,
            if_not_exists,
        } => {
            if unique {
                bail_not_implemented!("create unique index");
            }

            create_index::handle_create_index(
                handler_args,
                if_not_exists,
                name,
                table_name,
                columns.to_vec(),
                include,
                distributed_by,
            )
            .await
        }
        Statement::AlterDatabase {
            name,
            operation: AlterDatabaseOperation::RenameDatabase { database_name },
        } => alter_rename::handle_rename_database(handler_args, name, database_name).await,
        Statement::AlterDatabase {
            name,
            operation: AlterDatabaseOperation::ChangeOwner { new_owner_name },
        } => {
            alter_owner::handle_alter_owner(
                handler_args,
                name,
                new_owner_name,
                StatementType::ALTER_DATABASE,
            )
            .await
        }
        Statement::AlterSchema {
            name,
            operation: AlterSchemaOperation::RenameSchema { schema_name },
        } => alter_rename::handle_rename_schema(handler_args, name, schema_name).await,
        Statement::AlterSchema {
            name,
            operation: AlterSchemaOperation::ChangeOwner { new_owner_name },
        } => {
            alter_owner::handle_alter_owner(
                handler_args,
                name,
                new_owner_name,
                StatementType::ALTER_SCHEMA,
            )
            .await
        }
        Statement::AlterTable {
            name,
            operation:
                operation @ (AlterTableOperation::AddColumn { .. }
                | AlterTableOperation::DropColumn { .. }),
        } => alter_table_column::handle_alter_table_column(handler_args, name, operation).await,
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::RenameTable { table_name },
        } => {
            alter_rename::handle_rename_table(handler_args, TableType::Table, name, table_name)
                .await
        }
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::ChangeOwner { new_owner_name },
        } => {
            alter_owner::handle_alter_owner(
                handler_args,
                name,
                new_owner_name,
                StatementType::ALTER_TABLE,
            )
            .await
        }
        Statement::AlterTable {
            name,
            operation:
                AlterTableOperation::SetParallelism {
                    parallelism,
                    deferred,
                },
        } => {
            alter_parallelism::handle_alter_parallelism(
                handler_args,
                name,
                parallelism,
                StatementType::ALTER_TABLE,
                deferred,
            )
            .await
        }
        Statement::AlterTable {
            name,
            operation: AlterTableOperation::SetSchema { new_schema_name },
        } => {
            alter_set_schema::handle_alter_set_schema(
                handler_args,
                name,
                new_schema_name,
                StatementType::ALTER_TABLE,
                None,
            )
            .await
        }
        Statement::AlterIndex {
            name,
            operation: AlterIndexOperation::RenameIndex { index_name },
        } => alter_rename::handle_rename_index(handler_args, name, index_name).await,
        Statement::AlterIndex {
            name,
            operation:
                AlterIndexOperation::SetParallelism {
                    parallelism,
                    deferred,
                },
        } => {
            alter_parallelism::handle_alter_parallelism(
                handler_args,
                name,
                parallelism,
                StatementType::ALTER_INDEX,
                deferred,
            )
            .await
        }
        Statement::AlterView {
            materialized,
            name,
            operation: AlterViewOperation::RenameView { view_name },
        } => {
            if materialized {
                alter_rename::handle_rename_table(
                    handler_args,
                    TableType::MaterializedView,
                    name,
                    view_name,
                )
                .await
            } else {
                alter_rename::handle_rename_view(handler_args, name, view_name).await
            }
        }
        Statement::AlterView {
            materialized,
            name,
            operation:
                AlterViewOperation::SetParallelism {
                    parallelism,
                    deferred,
                },
        } if materialized => {
            alter_parallelism::handle_alter_parallelism(
                handler_args,
                name,
                parallelism,
                StatementType::ALTER_MATERIALIZED_VIEW,
                deferred,
            )
            .await
        }
        Statement::AlterView {
            materialized,
            name,
            operation: AlterViewOperation::ChangeOwner { new_owner_name },
        } => {
            if materialized {
                alter_owner::handle_alter_owner(
                    handler_args,
                    name,
                    new_owner_name,
                    StatementType::ALTER_MATERIALIZED_VIEW,
                )
                .await
            } else {
                alter_owner::handle_alter_owner(
                    handler_args,
                    name,
                    new_owner_name,
                    StatementType::ALTER_VIEW,
                )
                .await
            }
        }
        Statement::AlterView {
            materialized,
            name,
            operation: AlterViewOperation::SetSchema { new_schema_name },
        } => {
            if materialized {
                alter_set_schema::handle_alter_set_schema(
                    handler_args,
                    name,
                    new_schema_name,
                    StatementType::ALTER_MATERIALIZED_VIEW,
                    None,
                )
                .await
            } else {
                alter_set_schema::handle_alter_set_schema(
                    handler_args,
                    name,
                    new_schema_name,
                    StatementType::ALTER_VIEW,
                    None,
                )
                .await
            }
        }
        Statement::AlterSink {
            name,
            operation: AlterSinkOperation::RenameSink { sink_name },
        } => alter_rename::handle_rename_sink(handler_args, name, sink_name).await,

        Statement::AlterSink {
            name,
            operation: AlterSinkOperation::ChangeOwner { new_owner_name },
        } => {
            alter_owner::handle_alter_owner(
                handler_args,
                name,
                new_owner_name,
                StatementType::ALTER_SINK,
            )
            .await
        }
        Statement::AlterSink {
            name,
            operation: AlterSinkOperation::SetSchema { new_schema_name },
        } => {
            alter_set_schema::handle_alter_set_schema(
                handler_args,
                name,
                new_schema_name,
                StatementType::ALTER_SINK,
                None,
            )
            .await
        }
        Statement::AlterSink {
            name,
            operation:
                AlterSinkOperation::SetParallelism {
                    parallelism,
                    deferred,
                },
        } => {
            alter_parallelism::handle_alter_parallelism(
                handler_args,
                name,
                parallelism,
                StatementType::ALTER_SINK,
                deferred,
            )
            .await
        }
        Statement::AlterSource {
            name,
            operation: AlterSourceOperation::RenameSource { source_name },
        } => alter_rename::handle_rename_source(handler_args, name, source_name).await,
        Statement::AlterSource {
            name,
            operation: operation @ AlterSourceOperation::AddColumn { .. },
        } => alter_source_column::handle_alter_source_column(handler_args, name, operation).await,
        Statement::AlterSource {
            name,
            operation: AlterSourceOperation::ChangeOwner { new_owner_name },
        } => {
            alter_owner::handle_alter_owner(
                handler_args,
                name,
                new_owner_name,
                StatementType::ALTER_SOURCE,
            )
            .await
        }
        Statement::AlterSource {
            name,
            operation: AlterSourceOperation::SetSchema { new_schema_name },
        } => {
            alter_set_schema::handle_alter_set_schema(
                handler_args,
                name,
                new_schema_name,
                StatementType::ALTER_SOURCE,
                None,
            )
            .await
        }
        Statement::AlterSource {
            name,
            operation: AlterSourceOperation::FormatEncode { connector_schema },
        } => {
            alter_source_with_sr::handle_alter_source_with_sr(handler_args, name, connector_schema)
                .await
        }
        Statement::AlterFunction {
            name,
            args,
            operation: AlterFunctionOperation::SetSchema { new_schema_name },
        } => {
            alter_set_schema::handle_alter_set_schema(
                handler_args,
                name,
                new_schema_name,
                StatementType::ALTER_FUNCTION,
                args,
            )
            .await
        }
        Statement::AlterConnection {
            name,
            operation: AlterConnectionOperation::SetSchema { new_schema_name },
        } => {
            alter_set_schema::handle_alter_set_schema(
                handler_args,
                name,
                new_schema_name,
                StatementType::ALTER_CONNECTION,
                None,
            )
            .await
        }
        Statement::AlterSystem { param, value } => {
            alter_system::handle_alter_system(handler_args, param, value).await
        }
        Statement::StartTransaction { modes } => {
            transaction::handle_begin(handler_args, START_TRANSACTION, modes).await
        }
        Statement::Begin { modes } => transaction::handle_begin(handler_args, BEGIN, modes).await,
        Statement::Commit { chain } => {
            transaction::handle_commit(handler_args, COMMIT, chain).await
        }
        Statement::Abort => transaction::handle_rollback(handler_args, ABORT, false).await,
        Statement::Rollback { chain } => {
            transaction::handle_rollback(handler_args, ROLLBACK, chain).await
        }
        Statement::SetTransaction {
            modes,
            snapshot,
            session,
        } => transaction::handle_set(handler_args, modes, snapshot, session).await,
        Statement::CancelJobs(jobs) => handle_cancel(handler_args, jobs).await,
        Statement::Kill(process_id) => handle_kill(handler_args, process_id).await,
        Statement::Comment {
            object_type,
            object_name,
            comment,
        } => comment::handle_comment(handler_args, object_type, object_name, comment).await,
        _ => bail_not_implemented!("Unhandled statement: {}", stmt),
    }
}
