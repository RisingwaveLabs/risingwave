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

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_batch::error::BatchError;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeSelector;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_pb::common::WorkerNode;
use risingwave_rpc_client::ComputeClient;

use crate::catalog::TableId;
use crate::scheduler::{SchedulerError, SchedulerResult};
use crate::session::FrontendEnv;

pub async fn choose_fast_insert_client(
    table_id: &TableId,
    frontend_env: &FrontendEnv,
    request_id: u32,
) -> SchedulerResult<ComputeClient> {
    let worker = choose_worker(table_id, frontend_env, request_id)?;
    let client = frontend_env.client_pool().get(&worker).await?;
    Ok(client)
}

fn get_table_dml_vnode_mapping(
    table_id: &TableId,
    frontend_env: &FrontendEnv,
    worker_node_manager: &WorkerNodeSelector,
) -> SchedulerResult<WorkerSlotMapping> {
    let guard = frontend_env.catalog_reader().read_guard();

    let table = guard
        .get_any_table_by_id(table_id)
        .map_err(|e| SchedulerError::Internal(anyhow!(e)))?;

    let fragment_id = match table.dml_fragment_id.as_ref() {
        Some(dml_fragment_id) => dml_fragment_id,
        // Backward compatibility for those table without `dml_fragment_id`.
        None => &table.fragment_id,
    };

    worker_node_manager
        .manager
        .get_streaming_fragment_mapping(fragment_id)
        .map_err(|e| e.into())
}

fn choose_worker(
    table_id: &TableId,
    frontend_env: &FrontendEnv,
    request_id: u32,
) -> SchedulerResult<WorkerNode> {
    let worker_node_manager =
        WorkerNodeSelector::new(frontend_env.worker_node_manager_ref(), false);

    // dml should use streaming vnode mapping
    let vnode_mapping = get_table_dml_vnode_mapping(table_id, frontend_env, &worker_node_manager)?;
    let worker_node = {
        let worker_ids = vnode_mapping.iter_unique().collect_vec();
        let candidates = worker_node_manager
            .manager
            .get_workers_by_worker_slot_ids(&worker_ids)?;
        if candidates.is_empty() {
            return Err(BatchError::EmptyWorkerNodes.into());
        }
        candidates[request_id as usize % candidates.len()].clone()
    };
    Ok(worker_node)
}
