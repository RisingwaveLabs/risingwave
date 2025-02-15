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

use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::{KvLogStoreMetrics, KV_LOG_STORE_V2_INFO};
use crate::error::StreamResult;
use crate::executor::{Executor, SyncedKvLogStoreExecutor};
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
        let table = node.log_store_table.as_ref().unwrap().clone();
        let table_id = table.id;

        let metrics = {
            let streaming_metrics = actor_context.streaming_metrics.as_ref();
            let actor_id = actor_context.id;
            let join_fragment_id = 0;
            let name = "sync_log_store";
            let target = "unaligned_hash_join";
            KvLogStoreMetrics::new_inner(
                streaming_metrics,
                actor_id,
                join_fragment_id,
                name,
                target,
            )
        };

        let serde = LogStoreRowSerde::new(
            &table,
            params.vnode_bitmap.map(|b| b.into()),
            &KV_LOG_STORE_V2_INFO,
        );
        // FIXME(kwannoel): Make configurable
        let buffer_max_size = 1000;
        let [upstream] = params.input.try_into().unwrap();

        let executor = SyncedKvLogStoreExecutor::new(
            actor_context,
            table_id,
            metrics,
            serde,
            store,
            buffer_max_size,
            upstream,
        );
        Ok((params.info, executor).into())
    }
}
