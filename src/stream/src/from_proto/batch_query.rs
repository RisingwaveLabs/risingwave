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

use risingwave_common::catalog::ColumnId;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::BatchPlanNode;
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::executor::{BatchQueryExecutor, DummyExecutor};

pub struct BatchQueryExecutorBuilder;

impl ExecutorBuilder for BatchQueryExecutorBuilder {
    type Node = BatchPlanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        if node.table_desc.is_none() {
            // used in sharing cdc source backfill as a dummy batch plan node
            let mut info = params.info;
            info.identity = "DummyBatchQueryExecutor".to_owned();
            return Ok((info, DummyExecutor).into());
        }

        let table_desc: &StorageTableDesc = node.get_table_desc()?;

        let column_ids = node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();

        let table = BatchTable::new_partial(
            state_store,
            column_ids,
            params.vnode_bitmap.map(Into::into),
            table_desc,
        );
        assert_eq!(table.schema().data_types(), params.info.schema.data_types());

        let exec = BatchQueryExecutor::new(
            table,
            params.env.config().developer.chunk_size,
            params.info.schema.clone(),
        );
        Ok((params.info, exec).into())
    }
}
