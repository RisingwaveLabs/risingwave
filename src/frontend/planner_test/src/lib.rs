// Copyright 2023 RisingWave Labs
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

#![feature(let_chains)]
#![allow(clippy::derive_partial_eq_without_eq)]

//! Data-driven tests.

risingwave_expr_impl::enable!();

mod resolve_id;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
pub use resolve_id::*;
use risingwave_frontend::handler::{
    create_index, create_mv, create_schema, create_source, create_table, create_view, drop_table,
    explain, variable, HandlerArgs,
};
use risingwave_frontend::session::SessionImpl;
use risingwave_frontend::test_utils::{create_proto_file, get_explain_output, LocalFrontend};
use risingwave_frontend::{
    build_graph, explain_stream_graph, Binder, Explain, FrontendOpts, OptimizerContext,
    OptimizerContextRef, PlanRef, Planner, WithOptions,
};
use risingwave_sqlparser::ast::{
    AstOption, DropMode, EmitMode, ExplainOptions, ObjectName, Statement,
};
use risingwave_sqlparser::parser::Parser;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum TestType {
    /// The result of an `EXPLAIN` statement.
    ///
    /// This field is used when `sql` is an `EXPLAIN` statement.
    /// In this case, all other fields are invalid.
    ExplainOutput,

    /// The original logical plan
    LogicalPlan,
    /// Logical plan with optimization `.gen_optimized_logical_plan_for_batch()`
    OptimizedLogicalPlanForBatch,
    /// Logical plan with optimization `.gen_optimized_logical_plan_for_stream()`
    OptimizedLogicalPlanForStream,

    /// Distributed batch plan `.gen_batch_query_plan()`
    BatchPlan,
    /// Proto JSON of generated batch plan
    BatchPlanProto,
    /// Batch plan for local execution `.gen_batch_local_plan()`
    BatchLocalPlan,

    /// Create MV plan `.gen_create_mv_plan()`
    StreamPlan,
    /// Create MV fragments plan
    StreamDistPlan,
    /// Create MV plan with EOWC semantics `.gen_create_mv_plan(.., EmitMode::OnWindowClose)`
    EowcStreamPlan,
    /// Create MV fragments plan with EOWC semantics
    EowcStreamDistPlan,

    /// Create sink plan (assumes blackhole sink)
    /// TODO: Other sinks
    SinkPlan,

    BinderError,
    PlannerError,
    OptimizerError,
    BatchError,
    BatchLocalError,
    StreamError,
    EowcStreamError,
}

pub fn check(actual: Vec<TestCaseResult>, expect: expect_test::ExpectFile) {
    let actual = serde_yaml::to_string(&actual).unwrap();
    expect.assert_eq(&format!("# This file is automatically generated. See `src/frontend/planner_test/README.md` for more information.\n{}",actual));
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestInput {
    /// Id of the test case, used in before.
    pub id: Option<String>,
    /// A brief description of the test case.
    pub name: Option<String>,
    /// Before running the SQL statements, the test runner will execute the specified test cases
    pub before: Option<Vec<String>>,
    /// The resolved statements of the before ids
    #[serde(skip_serializing)]
    before_statements: Option<Vec<String>>,
    /// The SQL statements
    pub sql: String,
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    #[serde(flatten)]
    pub input: TestInput,

    // TODO: these should also be in TestInput, but it affects ordering. So next PR
    /// Support using file content or file location to create source.
    pub create_source: Option<CreateConnector>,
    /// Support using file content or file location to create table with connector.
    pub create_table_with_connector: Option<CreateConnector>,
    /// Provide config map to frontend
    pub with_config_map: Option<BTreeMap<String, String>>,

    /// Specify what output fields to check
    pub expected_outputs: HashSet<TestType>,
}

impl TestCase {
    pub fn id(&self) -> &Option<String> {
        &self.input.id
    }

    pub fn name(&self) -> &Option<String> {
        &self.input.name
    }

    pub fn before(&self) -> &Option<Vec<String>> {
        &self.input.before
    }

    pub fn before_statements(&self) -> &Option<Vec<String>> {
        &self.input.before_statements
    }

    pub fn sql(&self) -> &String {
        &self.input.sql
    }

    pub fn create_source(&self) -> &Option<CreateConnector> {
        &self.create_source
    }

    pub fn create_table_with_connector(&self) -> &Option<CreateConnector> {
        &self.create_table_with_connector
    }

    pub fn with_config_map(&self) -> &Option<BTreeMap<String, String>> {
        &self.with_config_map
    }
}

#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CreateConnector {
    format: String,
    encode: String,
    name: String,
    file: Option<String>,
    is_table: Option<bool>,
}

#[serde_with::skip_serializing_none]
#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TestCaseResult {
    #[serde(flatten)]
    pub input: TestInput,

    /// The original logical plan
    pub logical_plan: Option<String>,

    /// Logical plan with optimization `.gen_optimized_logical_plan_for_batch()`
    pub optimized_logical_plan_for_batch: Option<String>,

    /// Logical plan with optimization `.gen_optimized_logical_plan_for_stream()`
    pub optimized_logical_plan_for_stream: Option<String>,

    /// Distributed batch plan `.gen_batch_query_plan()`
    pub batch_plan: Option<String>,

    /// Proto JSON of generated batch plan
    pub batch_plan_proto: Option<String>,

    /// Batch plan for local execution `.gen_batch_local_plan()`
    pub batch_local_plan: Option<String>,

    /// Generate sink plan
    pub sink_plan: Option<String>,

    /// Create MV plan `.gen_create_mv_plan()`
    pub stream_plan: Option<String>,

    /// Create MV fragments plan
    pub stream_dist_plan: Option<String>,

    /// Create MV plan with EOWC semantics `.gen_create_mv_plan(.., EmitMode::OnWindowClose)`
    pub eowc_stream_plan: Option<String>,

    /// Create MV fragments plan with EOWC semantics
    pub eowc_stream_dist_plan: Option<String>,

    /// Error of binder
    pub binder_error: Option<String>,

    /// Error of planner
    pub planner_error: Option<String>,

    /// Error of optimizer
    pub optimizer_error: Option<String>,

    /// Error of `.gen_batch_query_plan()`
    pub batch_error: Option<String>,

    /// Error of `.gen_batch_local_plan()`
    pub batch_local_error: Option<String>,

    /// Error of `.gen_stream_plan()`
    pub stream_error: Option<String>,

    /// Error of `.gen_stream_plan()` with `emit_on_window_close = true`
    pub eowc_stream_error: Option<String>,

    /// Error of `.gen_sink_plan()`
    pub sink_error: Option<String>,

    /// The result of an `EXPLAIN` statement.
    ///
    /// This field is used when `sql` is an `EXPLAIN` statement.
    /// In this case, all other fields are invalid.
    pub explain_output: Option<String>,

    // TODO: these should also be in TestInput, but it affects ordering. So next PR
    /// Support using file content or file location to create source.
    pub create_source: Option<CreateConnector>,
    /// Support using file content or file location to create table with connector.
    pub create_table_with_connector: Option<CreateConnector>,
    /// Provide config map to frontend
    pub with_config_map: Option<BTreeMap<String, String>>,
}

impl TestCase {
    /// Run the test case, and return the expected output.
    pub async fn run(&self, do_check_result: bool) -> Result<TestCaseResult> {
        let session = {
            let frontend = LocalFrontend::new(FrontendOpts::default()).await;
            frontend.session_ref()
        };

        if let Some(ref config_map) = self.with_config_map() {
            for (key, val) in config_map {
                session.set_config(key, vec![val.to_owned()]).unwrap();
            }
        }

        let placeholder_empty_vec = vec![];

        // Since temp file will be deleted when it goes out of scope, so create source in advance.
        self.do_create_source(session.clone()).await?;
        self.do_create_table_with_connector(session.clone()).await?;

        let mut result: Option<TestCaseResult> = None;
        for sql in self
            .before_statements()
            .as_ref()
            .unwrap_or(&placeholder_empty_vec)
            .iter()
            .chain(std::iter::once(self.sql()))
        {
            result = self
                .run_sql(
                    Arc::from(sql.to_owned()),
                    session.clone(),
                    do_check_result,
                    result,
                )
                .await?;
        }

        let mut result = result.unwrap_or_default();
        result.input = self.input.clone();
        result.create_source = self.create_source().clone();
        result.create_table_with_connector = self.create_table_with_connector().clone();
        result.with_config_map = self.with_config_map().clone();

        Ok(result)
    }

    #[inline(always)]
    fn create_connector_sql(
        is_table: bool,
        connector_name: String,
        connector_format: String,
        connector_encode: String,
    ) -> String {
        let object_to_create = if is_table { "TABLE" } else { "SOURCE" };
        format!(
            r#"CREATE {} {}
    WITH (connector = 'kafka', kafka.topic = 'abc', kafka.servers = 'localhost:1001')
    FORMAT {} ENCODE {} (message = '.test.TestRecord', schema.location = 'file://"#,
            object_to_create, connector_name, connector_format, connector_encode
        )
    }

    async fn do_create_table_with_connector(
        &self,
        session: Arc<SessionImpl>,
    ) -> Result<Option<TestCaseResult>> {
        match self.create_table_with_connector().clone() {
            Some(connector) => {
                if let Some(content) = connector.file {
                    let sql = Self::create_connector_sql(
                        true,
                        connector.name,
                        connector.format,
                        connector.encode,
                    );
                    let temp_file = create_proto_file(content.as_str());
                    self.run_sql(
                        Arc::from(sql + temp_file.path().to_str().unwrap() + "')"),
                        session.clone(),
                        false,
                        None,
                    )
                    .await
                } else {
                    panic!(
                        "{:?} create table with connector must include `file` for the file content",
                        self.id()
                    );
                }
            }
            None => Ok(None),
        }
    }

    // If testcase have create source info, run sql to create source.
    // Support create source by file content or file location.
    async fn do_create_source(&self, session: Arc<SessionImpl>) -> Result<Option<TestCaseResult>> {
        match self.create_source().clone() {
            Some(source) => {
                if let Some(content) = source.file {
                    let sql = Self::create_connector_sql(
                        false,
                        source.name,
                        source.format,
                        source.encode,
                    );
                    let temp_file = create_proto_file(content.as_str());
                    self.run_sql(
                        Arc::from(sql + temp_file.path().to_str().unwrap() + "')"),
                        session.clone(),
                        false,
                        None,
                    )
                    .await
                } else {
                    panic!(
                        "{:?} create source must include `file` for the file content",
                        self.id()
                    );
                }
            }
            None => Ok(None),
        }
    }

    async fn run_sql(
        &self,
        sql: Arc<str>,
        session: Arc<SessionImpl>,
        do_check_result: bool,
        mut result: Option<TestCaseResult>,
    ) -> Result<Option<TestCaseResult>> {
        let statements = Parser::parse_sql(&sql).unwrap();
        for stmt in statements {
            // TODO: `sql` may contain multiple statements here.
            let handler_args = HandlerArgs::new(session.clone(), &stmt, sql.clone())?;
            let _guard = session.txn_begin_implicit();
            match stmt.clone() {
                Statement::Query(_)
                | Statement::Insert { .. }
                | Statement::Delete { .. }
                | Statement::Update { .. } => {
                    if result.is_some() {
                        panic!("two queries in one test case");
                    }
                    let explain_options = ExplainOptions {
                        verbose: true,
                        ..Default::default()
                    };
                    let context = OptimizerContext::new(
                        HandlerArgs::new(session.clone(), &stmt, sql.clone())?,
                        explain_options,
                    );
                    let ret = self.apply_query(&stmt, context.into())?;
                    if do_check_result {
                        check_result(self, &ret)?;
                    }
                    result = Some(ret);
                }
                Statement::CreateTable {
                    name,
                    columns,
                    constraints,
                    if_not_exists,
                    source_schema,
                    source_watermarks,
                    append_only,
                    cdc_table_info,
                    ..
                } => {
                    // TODO(st1page): refacor it
                    let notice = Default::default();
                    let source_schema =
                        source_schema.map(|schema| schema.into_source_schema_v2().0);

                    create_table::handle_create_table(
                        handler_args,
                        name,
                        columns,
                        constraints,
                        if_not_exists,
                        source_schema,
                        source_watermarks,
                        append_only,
                        notice,
                        cdc_table_info,
                    )
                    .await?;
                }
                Statement::CreateSource { stmt } => {
                    if let Err(error) =
                        create_source::handle_create_source(handler_args, stmt).await
                    {
                        let actual_result = TestCaseResult {
                            planner_error: Some(error.to_string()),
                            ..Default::default()
                        };

                        check_result(self, &actual_result)?;
                        result = Some(actual_result);
                    }
                }
                Statement::CreateIndex {
                    name,
                    table_name,
                    columns,
                    include,
                    distributed_by,
                    if_not_exists,
                    // TODO: support unique and if_not_exist in planner test
                    ..
                } => {
                    create_index::handle_create_index(
                        handler_args,
                        if_not_exists,
                        name,
                        table_name,
                        columns,
                        include,
                        distributed_by,
                    )
                    .await?;
                }
                Statement::CreateView {
                    materialized: true,
                    or_replace: false,
                    if_not_exists,
                    name,
                    query,
                    columns,
                    emit_mode,
                    ..
                } => {
                    create_mv::handle_create_mv(
                        handler_args,
                        if_not_exists,
                        name,
                        *query,
                        columns,
                        emit_mode,
                    )
                    .await?;
                }
                Statement::CreateView {
                    materialized: false,
                    or_replace: false,
                    if_not_exists,
                    name,
                    query,
                    columns,
                    ..
                } => {
                    create_view::handle_create_view(
                        handler_args,
                        if_not_exists,
                        name,
                        columns,
                        *query,
                    )
                    .await?;
                }
                Statement::Drop(drop_statement) => {
                    drop_table::handle_drop_table(
                        handler_args,
                        drop_statement.object_name,
                        drop_statement.if_exists,
                        matches!(drop_statement.drop_mode, AstOption::Some(DropMode::Cascade)),
                    )
                    .await?;
                }
                Statement::SetVariable {
                    local: _,
                    variable,
                    value,
                } => {
                    variable::handle_set(handler_args, variable, value).unwrap();
                }
                Statement::Explain {
                    analyze,
                    statement,
                    options,
                } => {
                    if result.is_some() {
                        panic!("two queries in one test case");
                    }
                    let rsp =
                        explain::handle_explain(handler_args, *statement, options, analyze).await?;

                    let explain_output = get_explain_output(rsp).await;
                    let ret = TestCaseResult {
                        explain_output: Some(explain_output),
                        ..Default::default()
                    };
                    if do_check_result {
                        check_result(self, &ret)?;
                    }
                    result = Some(ret);
                }
                Statement::CreateSchema {
                    schema_name,
                    if_not_exists,
                } => {
                    create_schema::handle_create_schema(handler_args, schema_name, if_not_exists)
                        .await?;
                }
                _ => return Err(anyhow!("Unsupported statement type")),
            }
        }
        Ok(result)
    }

    fn apply_query(
        &self,
        stmt: &Statement,
        context: OptimizerContextRef,
    ) -> Result<TestCaseResult> {
        let session = context.session_ctx().clone();
        let mut ret = TestCaseResult::default();

        let bound = {
            let mut binder = Binder::new(&session);
            match binder.bind(stmt.clone()) {
                Ok(bound) => bound,
                Err(err) => {
                    ret.binder_error = Some(err.to_string());
                    return Ok(ret);
                }
            }
        };

        let mut planner = Planner::new(context.clone());

        let mut logical_plan = match planner.plan(bound) {
            Ok(logical_plan) => {
                if self.expected_outputs.contains(&TestType::LogicalPlan) {
                    ret.logical_plan = Some(explain_plan(&logical_plan.clone().into_subplan()));
                }
                logical_plan
            }
            Err(err) => {
                ret.planner_error = Some(err.to_string());
                return Ok(ret);
            }
        };

        if self
            .expected_outputs
            .contains(&TestType::OptimizedLogicalPlanForBatch)
            || self.expected_outputs.contains(&TestType::OptimizerError)
        {
            let optimized_logical_plan_for_batch =
                match logical_plan.gen_optimized_logical_plan_for_batch() {
                    Ok(optimized_logical_plan_for_batch) => optimized_logical_plan_for_batch,
                    Err(err) => {
                        ret.optimizer_error = Some(err.to_string());
                        return Ok(ret);
                    }
                };

            // Only generate optimized_logical_plan_for_batch if it is specified in test case
            if self
                .expected_outputs
                .contains(&TestType::OptimizedLogicalPlanForBatch)
            {
                ret.optimized_logical_plan_for_batch =
                    Some(explain_plan(&optimized_logical_plan_for_batch));
            }
        }

        if self
            .expected_outputs
            .contains(&TestType::OptimizedLogicalPlanForStream)
            || self.expected_outputs.contains(&TestType::OptimizerError)
        {
            let optimized_logical_plan_for_stream =
                match logical_plan.gen_optimized_logical_plan_for_stream() {
                    Ok(optimized_logical_plan_for_stream) => optimized_logical_plan_for_stream,
                    Err(err) => {
                        ret.optimizer_error = Some(err.to_string());
                        return Ok(ret);
                    }
                };

            // Only generate optimized_logical_plan_for_stream if it is specified in test case
            if self
                .expected_outputs
                .contains(&TestType::OptimizedLogicalPlanForStream)
            {
                ret.optimized_logical_plan_for_stream =
                    Some(explain_plan(&optimized_logical_plan_for_stream));
            }
        }

        'batch: {
            // if self.batch_plan.is_some()
            //     || self.batch_plan_proto.is_some()
            //     || self.batch_error.is_some()
            if self.expected_outputs.contains(&TestType::BatchPlan)
                || self.expected_outputs.contains(&TestType::BatchPlanProto)
                || self.expected_outputs.contains(&TestType::BatchError)
            {
                let batch_plan = match logical_plan.gen_batch_plan() {
                    Ok(batch_plan) => match logical_plan.gen_batch_distributed_plan(batch_plan) {
                        Ok(batch_plan) => batch_plan,
                        Err(err) => {
                            ret.batch_error = Some(err.to_string());
                            break 'batch;
                        }
                    },
                    Err(err) => {
                        ret.batch_error = Some(err.to_string());
                        break 'batch;
                    }
                };

                // Only generate batch_plan if it is specified in test case
                if self.expected_outputs.contains(&TestType::BatchPlan) {
                    ret.batch_plan = Some(explain_plan(&batch_plan));
                }

                // Only generate batch_plan_proto if it is specified in test case
                if self.expected_outputs.contains(&TestType::BatchPlanProto) {
                    ret.batch_plan_proto = Some(serde_yaml::to_string(
                        &batch_plan.to_batch_prost_identity(false),
                    )?);
                }
            }
        }

        'local_batch: {
            if self.expected_outputs.contains(&TestType::BatchLocalPlan)
                || self.expected_outputs.contains(&TestType::BatchError)
            {
                let batch_plan = match logical_plan.gen_batch_plan() {
                    Ok(batch_plan) => match logical_plan.gen_batch_local_plan(batch_plan) {
                        Ok(batch_plan) => batch_plan,
                        Err(err) => {
                            ret.batch_error = Some(err.to_string());
                            break 'local_batch;
                        }
                    },
                    Err(err) => {
                        ret.batch_error = Some(err.to_string());
                        break 'local_batch;
                    }
                };

                // Only generate batch_plan if it is specified in test case
                if self.expected_outputs.contains(&TestType::BatchLocalPlan) {
                    ret.batch_local_plan = Some(explain_plan(&batch_plan));
                }
            }
        }

        {
            // stream
            for (
                emit_mode,
                plan,
                ret_plan_str,
                dist_plan,
                ret_dist_plan_str,
                error,
                ret_error_str,
            ) in [
                (
                    EmitMode::Immediately,
                    self.expected_outputs.contains(&TestType::StreamPlan),
                    &mut ret.stream_plan,
                    self.expected_outputs.contains(&TestType::StreamDistPlan),
                    &mut ret.stream_dist_plan,
                    self.expected_outputs.contains(&TestType::StreamError),
                    &mut ret.stream_error,
                ),
                (
                    EmitMode::OnWindowClose,
                    self.expected_outputs.contains(&TestType::EowcStreamPlan),
                    &mut ret.eowc_stream_plan,
                    self.expected_outputs
                        .contains(&TestType::EowcStreamDistPlan),
                    &mut ret.eowc_stream_dist_plan,
                    self.expected_outputs.contains(&TestType::EowcStreamError),
                    &mut ret.eowc_stream_error,
                ),
            ] {
                if !plan && !dist_plan && !error {
                    continue;
                }

                let q = if let Statement::Query(q) = stmt {
                    q.as_ref().clone()
                } else {
                    return Err(anyhow!("expect a query"));
                };

                let stream_plan = match create_mv::gen_create_mv_plan(
                    &session,
                    context.clone(),
                    q,
                    ObjectName(vec!["test".into()]),
                    vec![],
                    Some(emit_mode),
                ) {
                    Ok((stream_plan, _)) => stream_plan,
                    Err(err) => {
                        *ret_error_str = Some(err.to_string());
                        continue;
                    }
                };

                // Only generate stream_plan if it is specified in test case
                if plan {
                    *ret_plan_str = Some(explain_plan(&stream_plan));
                }

                // Only generate stream_dist_plan if it is specified in test case
                if dist_plan {
                    let graph = build_graph(stream_plan);
                    *ret_dist_plan_str = Some(explain_stream_graph(&graph, false));
                }
            }
        }

        'sink: {
            if self.expected_outputs.contains(&TestType::SinkPlan) {
                let sink_name = "sink_test";
                let mut options = HashMap::new();
                options.insert("connector".to_string(), "blackhole".to_string());
                options.insert("type".to_string(), "append-only".to_string());
                let options = WithOptions::new(options);
                let format_desc = (&options).try_into().unwrap();
                match logical_plan.gen_sink_plan(
                    sink_name.to_string(),
                    format!("CREATE SINK {sink_name} AS {}", stmt),
                    options,
                    false,
                    "test_db".into(),
                    "test_table".into(),
                    format_desc,
                ) {
                    Ok(sink_plan) => {
                        ret.sink_plan = Some(explain_plan(&sink_plan.into()));
                        break 'sink;
                    }
                    Err(err) => {
                        ret.sink_error = Some(err.to_string());
                        break 'sink;
                    }
                }
            }
        }

        Ok(ret)
    }
}

fn explain_plan(plan: &PlanRef) -> String {
    plan.explain_to_string()
}

/// Checks that the result matches `test_case.expected_outputs`.
///
/// We don't check the result matches here.
fn check_result(test_case: &TestCase, actual: &TestCaseResult) -> Result<()> {
    macro_rules! check {
        ($field:ident) => {
            paste::paste! {
                let case_contains = test_case.expected_outputs.contains(&TestType:: [< $field:camel >]  );
                let actual_contains = &actual.$field;
                match (case_contains, actual_contains) {
                    (false, None) | (true, Some(_)) => {},
                    (false, Some(e)) => return Err(anyhow!("unexpected {}: {}", stringify!($field), e)),
                    (true, None) => return Err(anyhow!(
                        "expected {}, but there's no such result during execution",
                        stringify!($field)
                    )),
                }
            }
        };
    }

    check!(binder_error);
    check!(planner_error);
    check!(optimizer_error);
    check!(batch_error);
    check!(batch_local_error);
    check!(stream_error);
    check!(eowc_stream_error);

    check!(logical_plan);
    check!(optimized_logical_plan_for_batch);
    check!(optimized_logical_plan_for_stream);
    check!(batch_plan);
    check!(batch_local_plan);
    check!(stream_plan);
    check!(stream_dist_plan);
    check!(eowc_stream_plan);
    check!(eowc_stream_dist_plan);
    check!(batch_plan_proto);
    check!(sink_plan);

    check!(explain_output);

    Ok(())
}

/// `/tests/testdata` directory.
pub fn test_data_dir() -> PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("testdata")
}

pub async fn run_test_file(file_path: &Path, file_content: &str) -> Result<()> {
    let file_name = file_path.file_name().unwrap().to_str().unwrap();
    println!("-- running {file_name} --");

    let mut failed_num = 0;
    let cases: Vec<TestCase> = serde_yaml::from_str(file_content).map_err(|e| {
        if let Some(loc) = e.location() {
            anyhow!(
                "failed to parse yaml: {e}, at {}:{}:{}",
                file_path.display(),
                loc.line(),
                loc.column()
            )
        } else {
            anyhow!("failed to parse yaml: {e}")
        }
    })?;
    let cases = resolve_testcase_id(cases).expect("failed to resolve");
    let mut outputs = vec![];

    for (i, c) in cases.into_iter().enumerate() {
        println!(
            "Running test #{i} (id: {}), SQL:\n{}",
            c.id().clone().unwrap_or_else(|| "<none>".to_string()),
            c.sql()
        );
        match c.run(true).await {
            Ok(case) => {
                outputs.push(case);
            }
            Err(e) => {
                eprintln!(
                    "Test #{i} (id: {}) failed, SQL:\n{}\nError: {}",
                    c.id().clone().unwrap_or_else(|| "<none>".to_string()),
                    c.sql(),
                    e
                );
                failed_num += 1;
            }
        }
    }

    let output_path = test_data_dir().join("output").join(file_name);
    check(outputs, expect_test::expect_file![output_path]);

    if failed_num > 0 {
        println!("\n");
        bail!(format!("{} test cases failed", failed_num));
    }
    Ok(())
}
