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

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::LookupNode;
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::executor::{LookupExecutor, LookupExecutorParams};

pub struct LookupExecutorBuilder;

impl ExecutorBuilder for LookupExecutorBuilder {
    type Node = LookupNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let lookup = node;

        let [stream, arrangement]: [_; 2] = params.input.try_into().unwrap();

        let arrangement_order_rules = lookup
            .get_arrangement_table_info()?
            .arrange_key_orders
            .iter()
            .map(ColumnOrder::from_protobuf)
            .collect();

        let arrangement_col_descs = lookup
            .get_arrangement_table_info()?
            .column_descs
            .iter()
            .map(ColumnDesc::from)
            .collect();

        let table_desc: &StorageTableDesc = lookup
            .get_arrangement_table_info()?
            .table_desc
            .as_ref()
            .unwrap();

        let column_ids = lookup
            .get_arrangement_table_info()?
            .get_output_col_idx()
            .iter()
            .map(|&idx| ColumnId::new(table_desc.columns[idx as usize].column_id))
            .collect_vec();

        let storage_table = BatchTable::new_partial(
            store,
            column_ids,
            params.vnode_bitmap.map(Into::into),
            table_desc,
        );

        let exec = LookupExecutor::new(LookupExecutorParams {
            ctx: params.actor_context,
            info: params.info.clone(),
            arrangement,
            stream,
            arrangement_col_descs,
            arrangement_order_rules,
            use_current_epoch: lookup.use_current_epoch,
            stream_join_key_indices: lookup.stream_key.iter().map(|x| *x as usize).collect(),
            arrange_join_key_indices: lookup.arrange_key.iter().map(|x| *x as usize).collect(),
            column_mapping: lookup.column_mapping.iter().map(|x| *x as usize).collect(),
            batch_table: storage_table,
            watermark_epoch: params.watermark_epoch,
            chunk_size: params.env.config().developer.chunk_size,
        });
        Ok((params.info, exec).into())
    }
}
