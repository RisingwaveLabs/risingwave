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

//! Local execution for batch query.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use pgwire::pg_server::SessionId;
use risingwave_batch::error::BatchError;
use risingwave_batch::executor::ExecutorBuilder;
use risingwave_batch::task::{ShutdownToken, TaskId};
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::array::DataChunk;
use risingwave_common::bail;
use risingwave_common::error::BoxedError;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::tracing::{InstrumentStream, TracingContext};
use risingwave_pb::batch_plan::exchange_info::DistributionMode;
use risingwave_pb::batch_plan::exchange_source::LocalExecutePlan::Plan;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{
    ExchangeInfo, ExchangeSource, FastInsertNode, LocalExecutePlan, PbTaskId, PlanFragment,
    PlanNode as PbPlanNode, TaskOutputId,
};
use risingwave_pb::common::{BatchQueryEpoch, WorkerNode};
use risingwave_pb::task_service::FastInsertRequest;
use tracing::debug;

use super::plan_fragmenter::{PartitionInfo, QueryStage, QueryStageRef};
use crate::catalog::{FragmentId, TableId};
use crate::error::RwError;
use crate::optimizer::plan_node::PlanNodeType;
use crate::scheduler::plan_fragmenter::{ExecutionPlanNode, Query, StageId};
use crate::scheduler::task_context::FrontendBatchTaskContext;
use crate::scheduler::{SchedulerError, SchedulerResult};
use crate::session::{FrontendEnv, SessionImpl};

pub struct FastInsertExecution {
    fast_insert_node: FastInsertNode,
    front_env: FrontendEnv,
    session: Arc<SessionImpl>,
    worker_node_manager: WorkerNodeSelector,
}

impl FastInsertExecution {
    pub fn new(
        fast_insert_node: FastInsertNode,
        front_env: FrontendEnv,
        session: Arc<SessionImpl>,
    ) -> Self {
        let worker_node_manager =
            WorkerNodeSelector::new(front_env.worker_node_manager_ref(), false);

        Self {
            fast_insert_node,
            front_env,
            session,
            worker_node_manager,
        }
    }

    pub async fn my_execute(self) -> SchedulerResult<()> {
        let workers = self.choose_worker(
            &TableId::new(self.fast_insert_node.table_id),
            self.fast_insert_node.session_id,
        )?;

        let client = self.session.env().client_pool().get(&workers).await?;
        let request = FastInsertRequest {
            fast_insert_node: Some(self.fast_insert_node),
        };
        let response = client.fast_insert(request).await?;
        println!("WKXLOG response: {:?}", response);
        Ok(())
    }

    #[inline(always)]
    fn get_table_dml_vnode_mapping(
        &self,
        table_id: &TableId,
    ) -> SchedulerResult<WorkerSlotMapping> {
        let guard = self.front_env.catalog_reader().read_guard();

        let table = guard
            .get_any_table_by_id(table_id)
            .map_err(|e| SchedulerError::Internal(anyhow!(e)))?;

        let fragment_id = match table.dml_fragment_id.as_ref() {
            Some(dml_fragment_id) => dml_fragment_id,
            // Backward compatibility for those table without `dml_fragment_id`.
            None => &table.fragment_id,
        };

        self.worker_node_manager
            .manager
            .get_streaming_fragment_mapping(fragment_id)
            .map_err(|e| e.into())
    }

    fn choose_worker(&self, table_id: &TableId, session_id: u32) -> SchedulerResult<WorkerNode> {
        // dml should use streaming vnode mapping
        let vnode_mapping = self.get_table_dml_vnode_mapping(table_id)?;
        let worker_node = {
            let worker_ids = vnode_mapping.iter_unique().collect_vec();
            let candidates = self
                .worker_node_manager
                .manager
                .get_workers_by_worker_slot_ids(&worker_ids)?;
            if candidates.is_empty() {
                return Err(BatchError::EmptyWorkerNodes.into());
            }
            candidates[session_id as usize % candidates.len()].clone()
        };
        Ok(worker_node)
    }
}
