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

use itertools::Itertools;
use pgwire::pg_server::{BoxedError, SessionManager};
use risingwave_pb::ddl_service::{replace_job_plan, ReplaceJobPlan, TableSchemaChange};
use risingwave_pb::frontend_service::frontend_service_server::FrontendService;
use risingwave_pb::frontend_service::{GetTableReplacePlanRequest, GetTableReplacePlanResponse};
use risingwave_rpc_client::error::ToTonicStatus;
use risingwave_sqlparser::ast::ObjectName;
use tonic::{Request as RpcRequest, Response as RpcResponse, Status};

use crate::error::RwError;
use crate::handler::create_source::SqlColumnStrategy;
use crate::handler::{get_new_table_definition_for_cdc_table, get_replace_table_plan};
use crate::session::SESSION_MANAGER;

#[derive(thiserror::Error, Debug)]
pub enum AutoSchemaChangeError {
    #[error("frontend error")]
    FrontendError(
        #[from]
        #[backtrace]
        RwError,
    ),
}

impl From<BoxedError> for AutoSchemaChangeError {
    fn from(err: BoxedError) -> Self {
        AutoSchemaChangeError::FrontendError(RwError::from(err))
    }
}

impl From<AutoSchemaChangeError> for tonic::Status {
    fn from(err: AutoSchemaChangeError) -> Self {
        err.to_status(tonic::Code::Internal, "frontend")
    }
}

#[derive(Default)]
pub struct FrontendServiceImpl {}

impl FrontendServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl FrontendService for FrontendServiceImpl {
    async fn get_table_replace_plan(
        &self,
        request: RpcRequest<GetTableReplacePlanRequest>,
    ) -> Result<RpcResponse<GetTableReplacePlanResponse>, Status> {
        let req = request.into_inner();
        tracing::info!("get_table_replace_plan for table {}", req.table_name);

        let table_change = req.table_change.expect("schema change message is required");
        let replace_plan =
            get_new_table_plan(table_change, req.table_name, req.database_id, req.owner).await?;

        Ok(RpcResponse::new(GetTableReplacePlanResponse {
            replace_plan: Some(replace_plan),
        }))
    }
}

/// Get the new table plan for the given table schema change
async fn get_new_table_plan(
    table_change: TableSchemaChange,
    table_name: String,
    database_id: u32,
    owner: u32,
) -> Result<ReplaceJobPlan, AutoSchemaChangeError> {
    tracing::info!("get_new_table_plan for table {}", table_name);

    let session_mgr = SESSION_MANAGER
        .get()
        .expect("session manager has been initialized");

    // get a session object for the corresponding user and database
    let session = session_mgr.create_dummy_session(database_id, owner)?;

    let new_version_columns = table_change
        .columns
        .into_iter()
        .map(|c| c.into())
        .collect_vec();
    let table_name = ObjectName::from(vec![table_name.as_str().into()]);

    let (new_table_definition, original_catalog) =
        get_new_table_definition_for_cdc_table(&session, table_name.clone(), &new_version_columns)
            .await?;
    let (_, table, graph, col_index_mapping, job_type) = get_replace_table_plan(
        &session,
        table_name,
        new_table_definition,
        &original_catalog,
        SqlColumnStrategy::FollowUnchecked, // not used
    )
    .await?;

    Ok(ReplaceJobPlan {
        replace_job: Some(replace_job_plan::ReplaceJob::ReplaceTable(
            replace_job_plan::ReplaceTable {
                table: Some(table),
                source: None, // none for cdc table
                job_type: job_type as _,
            },
        )),
        fragment_graph: Some(graph),
        table_col_index_mapping: Some(col_index_mapping.to_protobuf()),
    })
}
