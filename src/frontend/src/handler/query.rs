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
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::{PgResponse, StatementType};
use pgwire::types::Format;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{FunctionId, Schema};
use risingwave_common::session_config::QueryMode;
use risingwave_common::types::{DataType, Datum};
use risingwave_sqlparser::ast::{SetExpr, Statement};

use super::extended_handle::{PortalResult, PrepareStatement, PreparedResult};
use super::{create_mv, declare_cursor, PgResponseStream, RwPgResponse};
use crate::binder::{Binder, BoundCreateView, BoundStatement};
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError};
use crate::handler::flush::do_flush;
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::handler::HandlerArgs;
use crate::optimizer::plan_node::Explain;
use crate::optimizer::{
    ExecutionModeDecider, OptimizerContext, OptimizerContextRef, ReadStorageTableVisitor,
    RelationCollectorVisitor, SysTableVisitor,
};
use crate::planner::Planner;
use crate::scheduler::plan_fragmenter::Query;
use crate::scheduler::{
    BatchPlanFragmenter, DistributedQueryStream, ExecutionContext, ExecutionContextRef,
    LocalQueryExecution, LocalQueryStream,
};
use crate::session::SessionImpl;
use crate::PlanRef;

pub async fn handle_query(
    handler_args: HandlerArgs,
    stmt: Statement,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    let session = handler_args.session.clone();
    let plan_fragmenter_result = {
        let context = OptimizerContext::from_handler_args(handler_args);
        let plan_result = gen_batch_plan_by_statement(&session, context.into(), stmt)?;
        // Time zone is used by Hummock time travel query.
        risingwave_expr::expr_context::TIME_ZONE::sync_scope(
            session.config().timezone().to_owned(),
            || gen_batch_plan_fragmenter(&session, plan_result),
        )?
    };
    execute(session, plan_fragmenter_result, formats).await
}

pub fn handle_parse(
    handler_args: HandlerArgs,
    statement: Statement,
    specific_param_types: Vec<Option<DataType>>,
) -> Result<PrepareStatement> {
    let session = handler_args.session;
    let bound_result = gen_bound(&session, statement.clone(), specific_param_types)?;

    Ok(PrepareStatement::Prepared(PreparedResult {
        statement,
        bound_result,
    }))
}

/// Execute a "Portal", which is a prepared statement with bound parameters.
pub async fn handle_execute(
    handler_args: HandlerArgs,
    portal: PortalResult,
) -> Result<RwPgResponse> {
    let PortalResult {
        bound_result,
        result_formats,
        statement,
    } = portal;
    match statement {
        Statement::Query(_)
        | Statement::Insert { .. }
        | Statement::Delete { .. }
        | Statement::Update { .. } => {
            // Execute a batch query
            let session = handler_args.session.clone();
            let plan_fragmenter_result = {
                let context = OptimizerContext::from_handler_args(handler_args);
                let plan_result = gen_batch_query_plan(&session, context.into(), bound_result)?;
                // Time zone is used by Hummock time travel query.
                risingwave_expr::expr_context::TIME_ZONE::sync_scope(
                    session.config().timezone().to_owned(),
                    || gen_batch_plan_fragmenter(&session, plan_result),
                )?
            };
            execute(session, plan_fragmenter_result, result_formats).await
        }
        Statement::CreateView { materialized, .. } if materialized => {
            // Execute a CREATE MATERIALIZED VIEW
            let BoundResult {
                bound,
                dependent_relations,
                dependent_udfs,
                ..
            } = bound_result;
            let create_mv = if let BoundStatement::CreateView(create_mv) = bound {
                create_mv
            } else {
                unreachable!("expect a BoundStatement::CreateView")
            };
            let BoundCreateView {
                or_replace,
                materialized: _,
                if_not_exists,
                name,
                columns,
                query,
                emit_mode,
                with_options,
            } = *create_mv;
            if or_replace {
                bail_not_implemented!("CREATE OR REPLACE VIEW");
            }

            // Hack: replace the `with_options` with the bounded ones.
            let handler_args = HandlerArgs {
                session: handler_args.session.clone(),
                sql: handler_args.sql.clone(),
                normalized_sql: handler_args.normalized_sql.clone(),
                with_options: crate::WithOptions::try_from(with_options.as_slice())?,
            };

            create_mv::handle_create_mv_bound(
                handler_args,
                if_not_exists,
                name,
                *query,
                dependent_relations,
                dependent_udfs,
                columns,
                emit_mode,
            )
            .await
        }
        Statement::DeclareCursor { stmt } => {
            let session = handler_args.session.clone();
            let plan_fragmenter_result = {
                let context = OptimizerContext::from_handler_args(handler_args.clone());
                let plan_result = gen_batch_query_plan(&session, context.into(), bound_result)?;
                gen_batch_plan_fragmenter(&session, plan_result)?
            };
            declare_cursor::handle_bound_declare_query_cursor(
                handler_args,
                stmt.cursor_name,
                plan_fragmenter_result,
            )
            .await
        }
        _ => unreachable!(),
    }
}

pub fn gen_batch_plan_by_statement(
    session: &SessionImpl,
    context: OptimizerContextRef,
    stmt: Statement,
) -> Result<BatchQueryPlanResult> {
    let bound_result = gen_bound(session, stmt, vec![])?;
    gen_batch_query_plan(session, context, bound_result)
}

#[derive(Clone)]
pub struct BoundResult {
    pub(crate) stmt_type: StatementType,
    pub(crate) must_dist: bool,
    pub(crate) bound: BoundStatement,
    pub(crate) param_types: Vec<DataType>,
    pub(crate) parsed_params: Option<Vec<Datum>>,
    pub(crate) dependent_relations: HashSet<TableId>,
    /// TODO(rc): merge with `dependent_relations`
    pub(crate) dependent_udfs: HashSet<FunctionId>,
}

fn gen_bound(
    session: &SessionImpl,
    stmt: Statement,
    specific_param_types: Vec<Option<DataType>>,
) -> Result<BoundResult> {
    let stmt_type = StatementType::infer_from_statement(&stmt)
        .map_err(|err| RwError::from(ErrorCode::InvalidInputSyntax(err)))?;
    let must_dist = must_run_in_distributed_mode(&stmt)?;

    let mut binder = Binder::new_with_param_types(session, specific_param_types);
    let bound = binder.bind(stmt)?;
    session.check_privileges_for_stmt(&bound)?;

    Ok(BoundResult {
        stmt_type,
        must_dist,
        bound,
        param_types: binder.export_param_types()?,
        parsed_params: None,
        dependent_relations: binder.included_relations().clone(),
        dependent_udfs: binder.included_udfs().clone(),
    })
}

pub struct BatchQueryPlanResult {
    pub(crate) plan: PlanRef,
    pub(crate) query_mode: QueryMode,
    pub(crate) schema: Schema,
    pub(crate) stmt_type: StatementType,
    // Note that these relations are only resolved in the binding phase, and it may only be a
    // subset of the final one. i.e. the final one may contain more implicit dependencies on
    // indices.
    pub(crate) dependent_relations: Vec<TableId>,
    pub(crate) read_storage_tables: HashSet<TableId>,
}

fn gen_batch_query_plan(
    session: &SessionImpl,
    context: OptimizerContextRef,
    bind_result: BoundResult,
) -> Result<BatchQueryPlanResult> {
    let BoundResult {
        stmt_type,
        must_dist,
        bound,
        dependent_relations,
        ..
    } = bind_result;

    let mut planner = if matches!(bound, BoundStatement::Query(_)) {
        Planner::new_for_batch_dql(context)
    } else {
        Planner::new_for_batch(context)
    };

    let mut logical = planner.plan(bound)?;
    let schema = logical.schema();
    let batch_plan = logical.gen_batch_plan()?;

    let dependent_relations =
        RelationCollectorVisitor::collect_with(dependent_relations, batch_plan.clone());

    let read_storage_tables = ReadStorageTableVisitor::collect(batch_plan.clone());

    let must_local = must_run_in_local_mode(batch_plan.clone());

    let query_mode = match (must_dist, must_local) {
        (true, true) => {
            return Err(ErrorCode::InternalError(
                "the query is forced to both local and distributed mode by optimizer".to_owned(),
            )
            .into())
        }
        (true, false) => QueryMode::Distributed,
        (false, true) => QueryMode::Local,
        (false, false) => match session.config().query_mode() {
            QueryMode::Auto => determine_query_mode(batch_plan.clone()),
            QueryMode::Local => QueryMode::Local,
            QueryMode::Distributed => QueryMode::Distributed,
        },
    };

    let physical = match query_mode {
        QueryMode::Auto => unreachable!(),
        QueryMode::Local => logical.gen_batch_local_plan()?,
        QueryMode::Distributed => logical.gen_batch_distributed_plan()?,
    };

    Ok(BatchQueryPlanResult {
        plan: physical,
        query_mode,
        schema,
        stmt_type,
        dependent_relations: dependent_relations.into_iter().collect_vec(),
        read_storage_tables,
    })
}

fn must_run_in_distributed_mode(stmt: &Statement) -> Result<bool> {
    fn is_insert_using_select(stmt: &Statement) -> bool {
        fn has_select_query(set_expr: &SetExpr) -> bool {
            match set_expr {
                SetExpr::Select(_) => true,
                SetExpr::Query(query) => has_select_query(&query.body),
                SetExpr::SetOperation { left, right, .. } => {
                    has_select_query(left) || has_select_query(right)
                }
                SetExpr::Values(_) => false,
            }
        }

        matches!(
            stmt,
            Statement::Insert {source, ..} if has_select_query(&source.body)
        )
    }

    let stmt_type = StatementType::infer_from_statement(stmt)
        .map_err(|err| RwError::from(ErrorCode::InvalidInputSyntax(err)))?;

    Ok(matches!(
        stmt_type,
        StatementType::UPDATE
            | StatementType::DELETE
            | StatementType::UPDATE_RETURNING
            | StatementType::DELETE_RETURNING
    ) | is_insert_using_select(stmt))
}

fn must_run_in_local_mode(batch_plan: PlanRef) -> bool {
    SysTableVisitor::has_sys_table(batch_plan)
}

fn determine_query_mode(batch_plan: PlanRef) -> QueryMode {
    if ExecutionModeDecider::run_in_local_mode(batch_plan) {
        QueryMode::Local
    } else {
        QueryMode::Distributed
    }
}

pub struct BatchPlanFragmenterResult {
    pub(crate) plan_fragmenter: BatchPlanFragmenter,
    pub(crate) query_mode: QueryMode,
    pub(crate) schema: Schema,
    pub(crate) stmt_type: StatementType,
    pub(crate) read_storage_tables: HashSet<TableId>,
}

pub fn gen_batch_plan_fragmenter(
    session: &SessionImpl,
    plan_result: BatchQueryPlanResult,
) -> Result<BatchPlanFragmenterResult> {
    let BatchQueryPlanResult {
        plan,
        query_mode,
        schema,
        stmt_type,
        read_storage_tables,
        ..
    } = plan_result;

    tracing::trace!(
        "Generated query plan: {:?}, query_mode:{:?}",
        plan.explain_to_string(),
        query_mode
    );
    let worker_node_manager_reader = WorkerNodeSelector::new(
        session.env().worker_node_manager_ref(),
        session.is_barrier_read(),
    );
    let plan_fragmenter = BatchPlanFragmenter::new(
        worker_node_manager_reader,
        session.env().catalog_reader().clone(),
        session.config().batch_parallelism().0,
        session.config().timezone().to_owned(),
        plan,
    )?;

    Ok(BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        stmt_type,
        read_storage_tables,
    })
}

pub async fn create_stream(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
    formats: Vec<Format>,
) -> Result<(PgResponseStream, Vec<PgFieldDescriptor>)> {
    let BatchPlanFragmenterResult {
        plan_fragmenter,
        query_mode,
        schema,
        stmt_type,
        read_storage_tables,
    } = plan_fragmenter_result;

    let mut can_timeout_cancel = true;
    // Acquire the write guard for DML statements.
    match stmt_type {
        StatementType::INSERT
        | StatementType::INSERT_RETURNING
        | StatementType::DELETE
        | StatementType::DELETE_RETURNING
        | StatementType::UPDATE
        | StatementType::UPDATE_RETURNING => {
            session.txn_write_guard()?;
            can_timeout_cancel = false;
        }
        _ => {}
    }

    let query = plan_fragmenter.generate_complete_query().await?;
    tracing::trace!("Generated query after plan fragmenter: {:?}", &query);

    let pg_descs = schema
        .fields()
        .iter()
        .map(to_pg_field)
        .collect::<Vec<PgFieldDescriptor>>();
    let column_types = schema.fields().iter().map(|f| f.data_type()).collect_vec();

    let row_stream = match query_mode {
        QueryMode::Auto => unreachable!(),
        QueryMode::Local => PgResponseStream::LocalQuery(DataChunkToRowSetAdapter::new(
            local_execute(
                session.clone(),
                query,
                can_timeout_cancel,
                &read_storage_tables,
            )
            .await?,
            column_types,
            formats,
            session.clone(),
        )),
        // Local mode do not support cancel tasks.
        QueryMode::Distributed => {
            PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter::new(
                distribute_execute(
                    session.clone(),
                    query,
                    can_timeout_cancel,
                    read_storage_tables,
                )
                .await?,
                column_types,
                formats,
                session.clone(),
            ))
        }
    };

    Ok((row_stream, pg_descs))
}

async fn execute(
    session: Arc<SessionImpl>,
    plan_fragmenter_result: BatchPlanFragmenterResult,
    formats: Vec<Format>,
) -> Result<RwPgResponse> {
    // Used in counting row count.
    let first_field_format = formats.first().copied().unwrap_or(Format::Text);
    let query_mode = plan_fragmenter_result.query_mode;
    let stmt_type = plan_fragmenter_result.stmt_type;

    let query_start_time = Instant::now();
    let (row_stream, pg_descs) =
        create_stream(session.clone(), plan_fragmenter_result, formats).await?;

    // We need to do some post work after the query is finished and before the `Complete` response
    // it sent. This is achieved by the `callback` in `PgResponse`.
    let callback = async move {
        // Implicitly flush the writes.
        if session.config().implicit_flush() && stmt_type.is_dml() {
            do_flush(&session).await?;
        }

        // update some metrics
        match query_mode {
            QueryMode::Auto => unreachable!(),
            QueryMode::Local => {
                session
                    .env()
                    .frontend_metrics
                    .latency_local_execution
                    .observe(query_start_time.elapsed().as_secs_f64());

                session
                    .env()
                    .frontend_metrics
                    .query_counter_local_execution
                    .inc();
            }
            QueryMode::Distributed => {
                session
                    .env()
                    .query_manager()
                    .query_metrics
                    .query_latency
                    .observe(query_start_time.elapsed().as_secs_f64());

                session
                    .env()
                    .query_manager()
                    .query_metrics
                    .completed_query_counter
                    .inc();
            }
        }

        Ok(())
    };

    Ok(PgResponse::builder(stmt_type)
        .row_cnt_format_opt(Some(first_field_format))
        .values(row_stream, pg_descs)
        .callback(callback)
        .into())
}

pub async fn distribute_execute(
    session: Arc<SessionImpl>,
    query: Query,
    can_timeout_cancel: bool,
    read_storage_tables: HashSet<TableId>,
) -> Result<DistributedQueryStream> {
    let timeout = if cfg!(madsim) {
        None
    } else if can_timeout_cancel {
        Some(session.statement_timeout())
    } else {
        None
    };
    let execution_context: ExecutionContextRef =
        ExecutionContext::new(session.clone(), timeout).into();
    let query_manager = session.env().query_manager().clone();

    query_manager
        .schedule(execution_context, query, read_storage_tables)
        .await
        .map_err(|err| err.into())
}

pub async fn local_execute(
    session: Arc<SessionImpl>,
    query: Query,
    can_timeout_cancel: bool,
    read_storage_tables: &HashSet<TableId>,
) -> Result<LocalQueryStream> {
    let timeout = if cfg!(madsim) {
        None
    } else if can_timeout_cancel {
        Some(session.statement_timeout())
    } else {
        None
    };
    let front_env = session.env();

    let snapshot = session.pinned_snapshot();

    let execution = LocalQueryExecution::new(
        query,
        front_env.clone(),
        snapshot.support_barrier_read(),
        snapshot.batch_query_epoch(read_storage_tables)?,
        session,
        timeout,
    );

    Ok(execution.stream_rows())
}
