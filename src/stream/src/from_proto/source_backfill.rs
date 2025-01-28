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

use risingwave_common::catalog::TableId;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::stream_plan::SourceBackfillNode;

use super::*;
use crate::executor::source::{
    BackfillStateTableHandler, SourceBackfillExecutor, SourceBackfillExecutorInner,
    SourceStateTableHandler, StreamSourceCore,
};

pub struct SourceBackfillExecutorBuilder;

impl ExecutorBuilder for SourceBackfillExecutorBuilder {
    type Node = SourceBackfillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let source_id = TableId::new(node.upstream_source_id);
        let source_name = node.source_name.clone();
        let source_info = node.get_info()?;

        let options_with_secret =
            WithOptionsSecResolved::new(node.with_properties.clone(), node.secret_refs.clone());
        let source_desc_builder = super::source::create_source_desc_builder(
            node.columns.clone(),
            &params,
            source_info.clone(),
            node.row_id_index,
            options_with_secret,
        );

        let source_column_ids: Vec<_> = source_desc_builder
            .column_catalogs_to_source_column_descs()
            .iter()
            .map(|column| column.column_id)
            .collect();

        // FIXME: remove this. It's wrong
        let state_table_handler = SourceStateTableHandler::from_table_catalog(
            node.state_table.as_ref().unwrap(),
            store.clone(),
        )
        .await;
        let backfill_state_table = BackfillStateTableHandler::from_table_catalog(
            node.state_table.as_ref().unwrap(),
            store.clone(),
        )
        .await;
        let stream_source_core = StreamSourceCore::new(
            source_id,
            source_name,
            source_column_ids,
            source_desc_builder,
            state_table_handler,
        );
        let progress = params
            .local_barrier_manager
            .register_create_mview_progress(params.actor_context.id);

        let exec = SourceBackfillExecutorInner::new(
            params.actor_context.clone(),
            params.info.clone(),
            stream_source_core,
            params.executor_stats.clone(),
            params.env.system_params_manager_ref().get_params(),
            backfill_state_table,
            node.rate_limit,
            progress,
        );
        let [input]: [_; 1] = params.input.try_into().unwrap();

        Ok((
            params.info,
            SourceBackfillExecutor { inner: exec, input }.boxed(),
        )
            .into())
    }
}
