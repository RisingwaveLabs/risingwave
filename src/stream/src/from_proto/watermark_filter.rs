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

use risingwave_common::catalog::{ColumnId, TableDesc};
use risingwave_expr::expr::build_non_strict_from_prost;
use risingwave_pb::stream_plan::WatermarkFilterNode;
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::WatermarkFilterExecutor;

pub struct WatermarkFilterBuilder;

impl ExecutorBuilder for WatermarkFilterBuilder {
    type Node = WatermarkFilterNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let watermark_descs = node.get_watermark_descs().clone();
        let [watermark_desc]: [_; 1] = watermark_descs.try_into().unwrap();
        let watermark_expr = build_non_strict_from_prost(
            &watermark_desc.expr.unwrap(),
            params.eval_error_report.clone(),
        )?;
        let event_time_col_idx = watermark_desc.watermark_idx as usize;
        let vnodes = Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for watermark filter"),
        );

        // TODO: may use consistent op for watermark filter after we have upsert.
        let [table]: [_; 1] = node.get_tables().clone().try_into().unwrap();
        let desc = TableDesc::from_pb_table(&table).try_to_protobuf()?;
        let column_ids = desc
            .value_indices
            .iter()
            .map(|i| ColumnId::new(*i as _))
            .collect_vec();
        let other_vnodes = Arc::new(!(*vnodes).clone());
        let global_watermark_table =
            BatchTable::new_partial(store.clone(), column_ids, Some(other_vnodes), &desc);

        let table =
            StateTable::from_table_catalog_inconsistent_op(&table, store, Some(vnodes)).await;

        let exec = WatermarkFilterExecutor::new(
            params.actor_context,
            input,
            watermark_expr,
            event_time_col_idx,
            table,
            global_watermark_table,
            params.eval_error_report,
        );
        Ok((params.info, exec).into())
    }
}
