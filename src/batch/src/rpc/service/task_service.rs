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

use std::sync::Arc;

use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::batch_plan::TaskOutputId;
use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    fast_insert_response, CancelTaskRequest, CancelTaskResponse, CreateTaskRequest, ExecuteRequest,
    FastInsertRequest, FastInsertResponse, GetDataResponse, TaskInfoResponse,
};
use risingwave_storage::dispatch_state_store;
use thiserror_ext::AsReport;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::error::BatchError;
use crate::executor::FastInsertExecutor;
use crate::rpc::service::exchange::GrpcExchangeWriter;
use crate::task::{
    BatchEnvironment, BatchManager, BatchTaskExecution, ComputeNodeContext, StateReporter,
    TASK_STATUS_BUFFER_SIZE,
};

const LOCAL_EXECUTE_BUFFER_SIZE: usize = 64;

#[derive(Clone)]
pub struct BatchServiceImpl {
    mgr: Arc<BatchManager>,
    env: BatchEnvironment,
}

impl BatchServiceImpl {
    pub fn new(mgr: Arc<BatchManager>, env: BatchEnvironment) -> Self {
        BatchServiceImpl { mgr, env }
    }
}

pub type TaskInfoResponseResult = Result<TaskInfoResponse, Status>;
pub type GetDataResponseResult = Result<GetDataResponse, Status>;

#[async_trait::async_trait]
impl TaskService for BatchServiceImpl {
    type CreateTaskStream = ReceiverStream<TaskInfoResponseResult>;
    type ExecuteStream = ReceiverStream<GetDataResponseResult>;

    #[cfg_attr(coverage, coverage(off))]
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<Self::CreateTaskStream>, Status> {
        let CreateTaskRequest {
            task_id,
            plan,
            epoch,
            tracing_context,
            expr_context,
        } = request.into_inner();

        let (state_tx, state_rx) = tokio::sync::mpsc::channel(TASK_STATUS_BUFFER_SIZE);
        let state_reporter = StateReporter::new_with_dist_sender(state_tx);
        let res = self
            .mgr
            .fire_task(
                task_id.as_ref().expect("no task id found"),
                plan.expect("no plan found").clone(),
                epoch.expect("no epoch found"),
                ComputeNodeContext::create(self.env.clone()),
                state_reporter,
                TracingContext::from_protobuf(&tracing_context),
                expr_context.expect("no expression context found"),
            )
            .await;
        match res {
            Ok(_) => Ok(Response::new(ReceiverStream::new(
                // Create receiver stream from state receiver.
                // The state receiver is init in `.async_execute()`.
                // Will be used for receive task status update.
                // Note: we introduce this hack cuz `.execute()` do not produce a status stream,
                // but still share `.async_execute()` and `.try_execute()`.
                state_rx,
            ))),
            Err(e) => {
                error!(error = %e.as_report(), "failed to fire task");
                Err(e.into())
            }
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn cancel_task(
        &self,
        req: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = req.into_inner();
        tracing::trace!("Aborting task: {:?}", req.get_task_id().unwrap());
        self.mgr
            .cancel_task(req.get_task_id().expect("no task id found"));
        Ok(Response::new(CancelTaskResponse { status: None }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn execute(
        &self,
        req: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        let req = req.into_inner();
        let env = self.env.clone();
        let mgr = self.mgr.clone();
        BatchServiceImpl::get_execute_stream(env, mgr, req).await
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn fast_insert(
        &self,
        request: Request<FastInsertRequest>,
    ) -> Result<Response<FastInsertResponse>, Status> {
        let req = request.into_inner();
        let res = self.do_fast_insert(req).await;
        match res {
            Ok(_) => Ok(Response::new(FastInsertResponse {
                status: fast_insert_response::Status::Succeeded.into(),
                error_message: "".to_owned(),
            })),
            Err(e) => match e {
                BatchError::Dml(e) => Ok(Response::new(FastInsertResponse {
                    status: fast_insert_response::Status::DmlFailed.into(),
                    error_message: format!("{}", e.as_report()),
                })),
                _ => {
                    error!(error = %e.as_report(), "failed to fast insert");
                    Err(e.into())
                }
            },
        }
    }
}

impl BatchServiceImpl {
    async fn get_execute_stream(
        env: BatchEnvironment,
        mgr: Arc<BatchManager>,
        req: ExecuteRequest,
    ) -> Result<Response<ReceiverStream<GetDataResponseResult>>, Status> {
        let ExecuteRequest {
            task_id,
            plan,
            epoch,
            tracing_context,
            expr_context,
        } = req;

        let task_id = task_id.expect("no task id found");
        let plan = plan.expect("no plan found").clone();
        let epoch = epoch.expect("no epoch found");
        let tracing_context = TracingContext::from_protobuf(&tracing_context);
        let expr_context = expr_context.expect("no expression context found");

        let context = ComputeNodeContext::create(env.clone());
        trace!(
            "local execute request: plan:{:?} with task id:{:?}",
            plan,
            task_id
        );
        let task = BatchTaskExecution::new(&task_id, plan, context, epoch, mgr.runtime())?;
        let task = Arc::new(task);
        let (tx, rx) = tokio::sync::mpsc::channel(LOCAL_EXECUTE_BUFFER_SIZE);
        if let Err(e) = task
            .clone()
            .async_execute(None, tracing_context, expr_context)
            .await
        {
            error!(
                error = %e.as_report(),
                ?task_id,
                "failed to build executors and trigger execution"
            );
            return Err(e.into());
        }

        let pb_task_output_id = TaskOutputId {
            task_id: Some(task_id.clone()),
            // Since this is local execution path, the exchange would follow single distribution,
            // therefore we would only have one data output.
            output_id: 0,
        };
        let mut output = task.get_task_output(&pb_task_output_id).inspect_err(|e| {
            error!(
                error = %e.as_report(),
                ?task_id,
                "failed to get task output in local execution mode",
            );
        })?;
        let mut writer = GrpcExchangeWriter::new(tx.clone());
        // Always spawn a task and do not block current function.
        mgr.runtime().spawn(async move {
            match output.take_data(&mut writer).await {
                Ok(_) => Ok(()),
                Err(e) => tx.send(Err(e.into())).await,
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn do_fast_insert(&self, insert_req: FastInsertRequest) -> Result<(), BatchError> {
        let table_id = insert_req.table_id;
        let wait_for_persistence = insert_req.wait_for_persistence;
        let (executor, data_chunk) =
            FastInsertExecutor::build(self.env.dml_manager_ref(), insert_req)?;
        let epoch = executor
            .do_execute(data_chunk, wait_for_persistence)
            .await?;
        if wait_for_persistence {
            dispatch_state_store!(self.env.state_store(), store, {
                use risingwave_common::catalog::TableId;
                use risingwave_hummock_sdk::HummockReadEpoch;
                use risingwave_storage::store::TryWaitEpochOptions;
                use risingwave_storage::StateStore;

                store
                    .try_wait_epoch(
                        HummockReadEpoch::Committed(epoch.0),
                        TryWaitEpochOptions {
                            table_id: TableId::new(table_id),
                        },
                    )
                    .await
                    .map_err(BatchError::from)?;
            });
        }
        Ok(())
    }
}
