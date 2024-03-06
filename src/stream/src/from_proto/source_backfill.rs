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

use risingwave_common::catalog::{default_key_column_name_version_mapping, ColumnId, TableId};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::SourceCtrlOpts;
use risingwave_pb::data::data_type::TypeName as PbTypeName;
use risingwave_pb::plan_common::additional_column::ColumnType;
use risingwave_pb::plan_common::{
    AdditionalColumn, AdditionalColumnKey, ColumnDescVersion, FormatType, PbEncodeType,
};
use risingwave_pb::stream_plan::SourceBackfillNode;

use super::*;
use crate::executor::kafka_backfill_executor::{KafkaBackfillExecutor, KafkaBackfillExecutorInner};
use crate::executor::source::StreamSourceCore;
use crate::executor::state_table_handler::SourceStateTableHandler;
use crate::executor::BackfillStateTableHandler;

pub struct KafkaBackfillExecutorBuilder;

impl ExecutorBuilder for KafkaBackfillExecutorBuilder {
    type Node = SourceBackfillNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let source_id = TableId::new(node.source_id);
        let source_name = node.source_name.clone();
        let source_info = node.get_info()?;

        let source_desc_builder = super::source::create_source_desc_builder(
            node.columns.clone(),
            &params,
            source_info.clone(),
            node.row_id_index,
            node.with_properties.clone(),
        );

        let source_ctrl_opts = SourceCtrlOpts {
            chunk_size: params.env.config().developer.chunk_size,
            rate_limit: None,
        };

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

        let exec = KafkaBackfillExecutorInner::new(
            params.actor_context.clone(),
            params.info.clone(),
            stream_source_core,
            params.executor_stats.clone(),
            params.env.system_params_manager_ref().get_params(),
            source_ctrl_opts.clone(),
            params.env.connector_params(),
            backfill_state_table,
        );
        let [input]: [_; 1] = params.input.try_into().unwrap();

        Ok((
            params.info,
            KafkaBackfillExecutor { inner: exec, input }.boxed(),
        )
            .into())
    }
}
