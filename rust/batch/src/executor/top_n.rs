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

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::vec::Vec;

use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::{HeapElem, OrderPair};
use risingwave_pb::plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

struct TopNHeap {
    order_pairs: Arc<Vec<OrderPair>>,
    min_heap: BinaryHeap<Reverse<HeapElem>>,
    limit: usize,
}

impl TopNHeap {
    fn insert(&mut self, elem: HeapElem) {
        if self.min_heap.len() < self.limit {
            self.min_heap.push(Reverse(elem));
        } else if elem > self.min_heap.peek().unwrap().0 {
            self.min_heap.push(Reverse(elem));
            self.min_heap.pop();
        }
    }

    pub fn fit(&mut self, chunk: DataChunkRef) {
        DataChunk::rechunk(&[chunk], 1)
            .unwrap()
            .into_iter()
            .for_each(|c| {
                let elem = HeapElem {
                    order_pairs: self.order_pairs.clone(),
                    chunk: Arc::new(c),
                    chunk_idx: 0usize, // useless
                    elem_idx: 0usize,
                    encoded_chunk: None,
                };
                self.insert(elem);
            });
    }

    pub fn dump(&mut self) -> Option<DataChunk> {
        if self.min_heap.is_empty() {
            return None;
        }
        let mut chunks = self
            .min_heap
            .drain_sorted()
            .map(|e| e.0.chunk)
            .collect::<Vec<_>>();
        chunks.reverse();
        if let Ok(mut res) = DataChunk::rechunk(&chunks, self.limit) {
            assert_eq!(res.len(), 1);
            Some(res.remove(0))
        } else {
            None
        }
    }
}

pub(super) struct TopNExecutor {
    child: BoxedExecutor,
    top_n_heap: TopNHeap,
    identity: String,
}

impl BoxedExecutorBuilder for TopNExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);

        let top_n_node =
            try_match_expand!(source.plan_node().get_node_body().unwrap(), NodeBody::TopN)?;

        let order_pairs = top_n_node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build()?;
            return Ok(Box::new(
                Self::new(
                    child,
                    order_pairs,
                    top_n_node.get_limit() as usize,
                    source.plan_node().get_identity().clone(),
                )
                .fuse(),
            ));
        }
        Err(InternalError("TopN must have one child".to_string()).into())
    }
}

impl TopNExecutor {
    fn new(
        child: BoxedExecutor,
        order_pairs: Vec<OrderPair>,
        limit: usize,
        identity: String,
    ) -> Self {
        Self {
            top_n_heap: TopNHeap {
                min_heap: BinaryHeap::new(),
                limit,
                order_pairs: Arc::new(order_pairs),
            },
            child,
            identity,
        }
    }
}

#[async_trait::async_trait]
impl Executor for TopNExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;

        while let Some(chunk) = self.child.next().await? {
            self.top_n_heap.fit(Arc::new(chunk));
        }
        self.child.close().await?;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.top_n_heap.dump() {
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::MockExecutor;

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }

    #[tokio::test]
    async fn test_simple_top_n_executor() {
        let col0 = create_column(&[Some(1), Some(2), Some(3)]).unwrap();
        let col1 = create_column(&[Some(3), Some(2), Some(1)]).unwrap();
        let data_chunk = DataChunk::builder().columns(vec![col0, col1]).build();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
        let order_pairs = vec![
            OrderPair {
                column_idx: 1,
                order_type: OrderType::Ascending,
            },
            OrderPair {
                column_idx: 0,
                order_type: OrderType::Ascending,
            },
        ];
        let mut top_n_executor = TopNExecutor::new(
            Box::new(mock_executor),
            order_pairs,
            2usize,
            "TopNExecutor".to_string(),
        );
        let fields = &top_n_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);
        assert_eq!(fields[1].data_type, DataType::Int32);
        top_n_executor.open().await.unwrap();
        let res = top_n_executor.next().await.unwrap();
        assert!(matches!(res, Some(_)));
        if let Some(res) = res {
            assert_eq!(res.cardinality(), 2);
            let col0 = res.column_at(0);
            assert_eq!(col0.array().as_int32().value_at(0), Some(3));
            assert_eq!(col0.array().as_int32().value_at(1), Some(2));
        }
        let res = top_n_executor.next().await.unwrap();
        assert!(matches!(res, None));
        top_n_executor.close().await.unwrap();
    }
}
