// Copyright 2025 RisingWave Labs
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

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use risingwave_connector::parser::additional_columns::gen_default_addition_col_name;
use risingwave_connector::sink::decouple_checkpoint_log_sink::COMMIT_CHECKPOINT_INTERVAL;
use risingwave_pb::ddl_service::TableJobType;
use risingwave_sqlparser::ast::{ColumnDef, Ident};

use crate::catalog::root_catalog::SchemaPath;
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::{ErrorCode, Result};
use crate::handler::create_source::SqlColumnStrategy;
use crate::handler::{
    get_replace_table_plan, HandlerArgs, ObjectName, PgResponse, RwPgResponse, Statement,
    StatementType,
};
use crate::session::SessionImpl;
use crate::utils::data_type::DataTypeToAst;
use crate::utils::options::RETENTION_SECONDS;
use crate::{bind_data_type, Binder, TableCatalog};

// allowed in with clause but irrelevant to connector
static TABLE_PROPS: LazyLock<HashSet<&str>> =
    LazyLock::new(|| HashSet::from([COMMIT_CHECKPOINT_INTERVAL, RETENTION_SECONDS]));

fn fetch_schema_info(
    session: &Arc<SessionImpl>,
    table_name: ObjectName,
) -> Result<(Arc<TableCatalog>, Arc<SourceCatalog>)> {
    let db_name = session.database();
    let (schema_name, real_table_name) =
        Binder::resolve_schema_qualified_name(db_name.as_str(), table_name.clone())?;
    let search_path = session.config().search_path();
    let user_name = &session.auth_context().user_name;

    let schema_path = SchemaPath::new(schema_name.as_deref(), &search_path, user_name);
    let reader = session.env().catalog_reader().read_guard();

    let (table_def, schema_name) =
        reader.get_any_table_by_name(db_name.as_str(), schema_path, &real_table_name)?;
    session.check_privilege_for_drop_alter(schema_name, &**table_def)?;

    let Some(source_id) = table_def.associated_source_id else {
        return Err(ErrorCode::ProtocolError(format!(
            "Table {} is not associated with a connector",
            real_table_name
        ))
        .into());
    };
    let (source_def, _) =
        reader.get_source_by_id(db_name.as_str(), schema_path, &source_id.table_id())?;
    Ok((table_def.clone(), source_def.clone()))
}

fn rewrite_table_definition(
    original_table_def: &Arc<TableCatalog>,
    original_source_def: &Arc<SourceCatalog>,
    original_statement: Statement,
) -> Result<Statement> {
    let Statement::CreateTable {
        mut columns,
        include_column_options,
        or_replace,
        temporary,
        if_not_exists,
        name,
        wildcard_idx,
        constraints,
        mut with_options,
        append_only,
        on_conflict,
        with_version_column,
        query,
        engine,
        ..
    } = original_statement
    else {
        panic!("unexpected statement: {:?}", original_statement);
    };

    // identical logic with func `handle_addition_columns`, reverse the order to keep the original order of additional columns
    for item in include_column_options.iter().rev() {
        let col_name = if let Some(col_alias) = &item.column_alias {
            col_alias.real_value()
        } else {
            let data_type = if let Some(dt) = &item.header_inner_expect_type {
                Some(bind_data_type(dt)?)
            } else {
                None
            };
            gen_default_addition_col_name(
                original_source_def.connector_name().as_str(),
                item.column_type.real_value().as_str(),
                item.inner_field.as_deref(),
                data_type.as_ref(),
            )
        };
        // find the column def in the catalog
        if let Some(col_def) = original_table_def
            .columns
            .iter()
            .find(|col_def| col_def.name() == col_name)
        {
            columns.push(ColumnDef {
                name: Ident::from(col_name.as_str()),
                data_type: Some(col_def.data_type().to_ast()),
                collation: None,
                options: vec![],
            });
        }
    }

    let new_statement = Statement::CreateTable {
        or_replace,
        temporary,
        if_not_exists,
        name: name.clone(),
        columns: columns.clone(),
        wildcard_idx,
        constraints: constraints.clone(),
        with_options: {
            with_options.retain(|item| {
                TABLE_PROPS.contains(item.name.real_value().to_lowercase().as_str())
            });
            with_options
        },
        format_encode: None,
        source_watermarks: vec![], // no source, no watermark
        append_only,
        on_conflict,
        with_version_column,
        query,
        cdc_table_info: None,
        include_column_options: vec![],
        webhook_info: None,
        engine,
    };
    Ok(new_statement)
}

pub async fn handle_alter_table_drop_connector(
    handler_args: HandlerArgs,
    table_name: ObjectName,
) -> Result<RwPgResponse> {
    let session = handler_args.session;
    let (table_def, source_def) = fetch_schema_info(&session, table_name.clone())?;
    let original_definition = table_def.create_sql_ast_purified()?;

    let new_statement = rewrite_table_definition(&table_def, &source_def, original_definition)?;
    let (_, table, graph, col_index_mapping, _) = get_replace_table_plan(
        &session,
        table_name,
        new_statement,
        &table_def,
        SqlColumnStrategy::FollowUnchecked,
    )
    .await?;

    let catalog_writer = session.catalog_writer()?;
    catalog_writer
        .replace_table(
            None,
            table,
            graph,
            col_index_mapping,
            TableJobType::General as _,
        )
        .await?;

    Ok(PgResponse::empty_result(StatementType::ALTER_TABLE))
}
