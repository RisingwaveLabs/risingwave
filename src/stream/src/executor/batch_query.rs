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

use futures::TryStreamExt;
use risingwave_common::array::Op;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::table::collect_data_chunk;

use crate::executor::prelude::*;

pub struct BatchQueryExecutor<S: StateStore> {
    /// The [`BatchTable`] that needs to be queried
    table: BatchTable<S>,

    /// The number of tuples in one [`StreamChunk`]
    batch_size: usize,

    schema: Schema,
}

impl<S> BatchQueryExecutor<S>
where
    S: StateStore,
{
    pub fn new(table: BatchTable<S>, batch_size: usize, schema: Schema) -> Self {
        Self {
            table,
            batch_size,
            schema,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self, epoch: u64) {
        let iter = self
            .table
            .batch_iter(
                HummockReadEpoch::Committed(epoch),
                false,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;
        let iter = iter.map_ok(|keyed_row| keyed_row.into_owned_row());
        pin_mut!(iter);

        while let Some(data_chunk) =
            collect_data_chunk(&mut iter, &self.schema, Some(self.batch_size))
                .instrument_await("batch_query_executor_collect_chunk")
                .await?
        {
            let ops = vec![Op::Insert; data_chunk.capacity()];
            let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
            yield Message::Chunk(stream_chunk);
        }
    }
}

impl<S> Execute for BatchQueryExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        unreachable!("should call `execute_with_epoch`")
    }

    fn execute_with_epoch(self: Box<Self>, epoch: u64) -> BoxedMessageStream {
        self.execute_inner(epoch).boxed()
    }
}

#[cfg(test)]
mod test {
    use futures_async_stream::for_await;

    use super::*;
    use crate::executor::mview::test_utils::gen_basic_table;

    #[tokio::test]
    async fn test_basic() {
        let test_batch_size = 50;
        let test_batch_count = 5;
        let table = gen_basic_table(test_batch_count * test_batch_size).await;

        let schema = table.schema().clone();
        let stream = BatchQueryExecutor::new(table, test_batch_size, schema)
            .boxed()
            .execute_with_epoch(u64::MAX);
        let mut batch_cnt = 0;

        #[for_await]
        for msg in stream {
            let msg: Message = msg.unwrap();
            let chunk = msg.as_chunk().unwrap();
            let data = *chunk.column_at(0).datum_at(0).unwrap().as_int32();
            assert_eq!(data, (batch_cnt * test_batch_size) as i32);
            batch_cnt += 1;
        }

        assert_eq!(batch_cnt, test_batch_count)
    }
}
