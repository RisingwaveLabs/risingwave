use std::sync::Arc;

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::monitor::StateStoreMetrics;
// use risingwave_storage::table::mview::{MViewTable, MViewTableIter};
use risingwave_storage::table::cell_based_table::{CellBasedTable, CellBasedTableRowIter};
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{Executor, ExecutorBuilder, Message, PkIndices, PkIndicesRef};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

const DEFAULT_BATCH_SIZE: usize = 100;

/// [`BatchQueryExecutor`] pushes m-view data batch to the downstream executor. Currently, this
/// executor is used as input of the [`crate::executor::ChainExecutor`] to support MV-on-MV.
pub struct BatchQueryExecutor<S: StateStore> {
    /// The primary key indices of the schema
    pk_indices: PkIndices,
    /// The [`CellBasedTable`] that needs to be queried
    table: CellBasedTable<S>,
    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,
    /// Inner iterator that reads [`CellBasedTable`]
    iter: Option<CellBasedTableRowIter<S>>,

    schema: Schema,

    epoch: Option<u64>,
    /// Logical Operator Info
    op_info: String,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S: StateStore> std::fmt::Debug for BatchQueryExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchQueryExecutor")
            .field("table", &self.table)
            .field("pk_indices", &self.pk_indices)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

pub struct BatchQueryExecutorBuilder {}

impl ExecutorBuilder for BatchQueryExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &stream_plan::StreamNode,
        state_store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::BatchPlanNode)?;
        let table_id = TableId::from(&node.table_ref_id);
        let column_descs = node
            .column_descs
            .iter()
            .map(|column_desc| ColumnDesc::from(column_desc.clone()))
            .collect_vec();
        let keyspace = Keyspace::table_root(state_store, &table_id);
        let table = CellBasedTable::new_adhoc(
            keyspace,
            column_descs,
            Arc::new(StateStoreMetrics::unused()),
        );
        let key_indices = node
            .get_distribution_keys()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        Ok(Box::new(BatchQueryExecutor::new(
            table,
            params.pk_indices,
            params.op_info,
            key_indices,
        )))
    }
}

impl<S: StateStore> BatchQueryExecutor<S> {
    pub fn new(
        table: CellBasedTable<S>,
        pk_indices: PkIndices,
        op_info: String,
        key_indices: Vec<usize>,
    ) -> Self {
        Self::new_with_batch_size(table, pk_indices, DEFAULT_BATCH_SIZE, op_info, key_indices)
    }

    pub fn new_with_batch_size(
        table: CellBasedTable<S>,
        pk_indices: PkIndices,
        batch_size: usize,
        op_info: String,
        key_indices: Vec<usize>,
    ) -> Self {
        let schema = table.schema().clone();
        Self {
            pk_indices,
            table,
            batch_size,
            iter: None,
            schema,
            epoch: None,
            op_info,
            key_indices,
        }
    }
}

#[async_trait]
impl<S: StateStore> Executor for BatchQueryExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        if self.iter.is_none() {
            self.iter = Some(
                self.table
                    .iter(self.epoch.expect("epoch has not been set"))
                    .await
                    .unwrap(),
            );
        }

        let iter = self.iter.as_mut().unwrap();
        let data_chunk = iter
            .collect_data_chunk(&self.table, Some(self.batch_size))
            .await?
            .ok_or_else(|| RwError::from(ErrorCode::Eof))?;

        let ops = vec![Op::Insert; data_chunk.cardinality()];
        let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
        Ok(Message::Chunk(stream_chunk))
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "BatchQueryExecutor"
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }

    fn init(&mut self, epoch: u64) -> Result<()> {
        if let Some(_old) = self.epoch.replace(epoch) {
            panic!("epoch cannot be initialized twice");
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use std::vec;

    use super::*;
    use crate::executor::mview::test_utils::gen_basic_table;

    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;
        let mut node = BatchQueryExecutor::new_with_batch_size(
            table,
            vec![0, 1],
            test_batch_size,
            "BatchQueryExecutor".to_string(),
            vec![],
        );
        node.init(u64::MAX).unwrap();
        let mut batch_cnt = 0;
        while let Ok(Message::Chunk(sc)) = node.next().await {
            let data = *sc.column_at(0).array_ref().datum_at(0).unwrap().as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }
        assert_eq!(batch_cnt, test_batch_count)
    }

    #[should_panic]
    #[tokio::test]
    async fn test_init_epoch_twice() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;
        let mut node = BatchQueryExecutor::new_with_batch_size(
            table,
            vec![0, 1],
            test_batch_size,
            "BatchQueryExecutor".to_string(),
            vec![],
        );
        node.init(u64::MAX).unwrap();
        node.init(0).unwrap();
    }
}
