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

use risingwave_pb::stream_plan::SyncLogStoreNode;
use risingwave_storage::StateStore;
use crate::common::log_store_impl::kv_log_store::KvLogStoreMetrics;

use crate::error::StreamResult;
use crate::executor::Executor;
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct SyncLogStoreExecutorBuilder;

impl ExecutorBuilder for SyncLogStoreExecutorBuilder {
    type Node = SyncLogStoreNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let actor_context = params.actor_context.clone();
        let actor_id = actor_context.id;
        let table_id = 1;
        let streaming_metrics = actor_context.streaming_metrics.as_ref();

        todo!()
        // let table = node.get_table()?;
        // let table = table.clone();
        // let table = table.into_sync_log_store();
        // let table = Arc::new(table);
        // let table = table as Arc<dyn StateTable>;
        //
        // let executor = SyncedKvLogStoreExecutor {
        //     base: ExecutorBase::new(params, node, store),
        //     table,
        // };
        // Ok(Box::new(executor))
    }
}
