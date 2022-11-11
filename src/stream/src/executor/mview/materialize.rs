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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::{izip, Itertools};
use risingwave_common::array::{Op, RowDeserializer, StreamChunk, Vis};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::catalog::Table;
use risingwave_storage::table::compute_chunk_vnode;
use risingwave_storage::table::streaming_table::mem_table::RowOp;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::cache::{EvictableHashMap, ExecutorCache, LruManagerRef};
use crate::executor::error::StreamExecutorError;
use crate::executor::{
    expect_first_barrier, ActorContext, ActorContextRef, BoxedExecutor, BoxedMessageStream,
    Executor, ExecutorInfo, Message, PkIndicesRef, StreamExecutorResult,
};

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore> {
    input: BoxedExecutor,

    state_table: StateTable<S>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_columns: Vec<usize>,

    actor_context: ActorContextRef,

    info: ExecutorInfo,

    materialize_cache: MaterializeCache,
    handle_pk_conflict: bool,
}

impl<S: StateStore> MaterializeExecutor<S> {
    /// Create a new `MaterializeExecutor` with distribution specified with `distribution_keys` and
    /// `vnodes`. For singleton distribution, `distribution_keys` should be empty and `vnodes`
    /// should be `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: BoxedExecutor,
        store: S,
        key: Vec<OrderPair>,
        executor_id: u64,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
        handle_pk_conflict: bool,
    ) -> Self {
        let arrange_columns: Vec<usize> = key.iter().map(|k| k.column_idx).collect();

        let schema = input.schema().clone();

        let state_table = StateTable::from_table_catalog(table_catalog, store, vnodes).await;

        Self {
            input,
            state_table,
            arrange_columns: arrange_columns.clone(),
            actor_context,
            info: ExecutorInfo {
                schema,
                pk_indices: arrange_columns,
                identity: format!("MaterializeExecutor {:X}", executor_id),
            },
            materialize_cache: MaterializeCache::new(lru_manager, cache_size),
            handle_pk_conflict,
        }
    }

    /// Create a new `MaterializeExecutor` without distribution info for test purpose.
    #[allow(clippy::too_many_arguments)]
    pub async fn for_test(
        input: BoxedExecutor,
        store: S,
        table_id: TableId,
        keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        lru_manager: Option<LruManagerRef>,
        cache_size: usize,
        handle_pk_conflict: bool,
    ) -> Self {
        let arrange_columns: Vec<usize> = keys.iter().map(|k| k.column_idx).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        let schema = input.schema().clone();
        let columns = column_ids
            .into_iter()
            .zip_eq(schema.fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(column_id, field.data_type()))
            .collect_vec();

        let state_table = StateTable::new_without_distribution(
            store,
            table_id,
            columns,
            arrange_order_types,
            arrange_columns.clone(),
        )
        .await;

        Self {
            input,
            state_table,
            arrange_columns: arrange_columns.clone(),
            actor_context: ActorContext::create(0),
            info: ExecutorInfo {
                schema,
                pk_indices: arrange_columns,
                identity: format!("MaterializeExecutor {:X}", executor_id),
            },
            materialize_cache: MaterializeCache::new(lru_manager, cache_size),
            handle_pk_conflict,
        }
    }

    pub fn handle_conflict(&mut self) {
        self.handle_pk_conflict = true;
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let data_types = self.schema().data_types().clone();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            yield match msg {
                Message::Watermark(_) => {
                    todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                }
                Message::Chunk(chunk) => {
                    match self.handle_pk_conflict {
                        true => {
                            // create MaterializeBuffer from chunk
                            let buffer = MaterializeBuffer::fill_buffer_from_chunk(
                                chunk,
                                self.state_table.value_indices(),
                                self.state_table.dist_key_indices(),
                                self.state_table.pk_indices(),
                                self.state_table.pk_serde(),
                                self.state_table.vnodes(),
                            );

                            if buffer.is_empty() {
                                // empty chunk
                                continue;
                            } else {
                                // fill cache
                                for key in buffer.get_keys() {
                                    if self.materialize_cache.get(key).is_none() {
                                        // cache miss
                                        if let Some(storage_value) = self
                                            .state_table
                                            .keyspace()
                                            .get(
                                                &key,
                                                self.state_table.epoch(),
                                                self.state_table.get_read_option(),
                                            )
                                            .await?
                                        {
                                            self.materialize_cache
                                                .insert(key.clone(), Some(storage_value.to_vec()));
                                        } else {
                                            self.materialize_cache.insert(key.clone(), None);
                                        }
                                    }
                                }

                                // handle pk conflict
                                let mut output = buffer.buffer.clone();
                                for (key, row_op) in buffer.into_parts() {
                                    match row_op {
                                        RowOp::Insert(row) => {
                                            if let Some(cache_row) =
                                                self.materialize_cache.get(&key).unwrap()
                                            {
                                                // double insert
                                                output.insert(
                                                    key.clone(),
                                                    RowOp::Update((
                                                        cache_row.to_vec(),
                                                        row.clone(),
                                                    )),
                                                );
                                            }

                                            self.materialize_cache.insert(key, Some(row.clone()));
                                        }
                                        RowOp::Delete(old_row) => {
                                            if let Some(cache_row) =
                                                self.materialize_cache.get(&key).unwrap()
                                            {
                                                if cache_row != &old_row {
                                                    // delete a nonexistent value
                                                    output.insert(
                                                        key.clone(),
                                                        RowOp::Delete(cache_row.to_vec()),
                                                    );
                                                }

                                                self.materialize_cache.insert(key, None);
                                            } else {
                                                // delete a nonexistent pk
                                                output.remove(&key);
                                            }
                                        }
                                        RowOp::Update((old_row, new_row)) => {
                                            if let Some(cache_row) =
                                                self.materialize_cache.get(&key).unwrap()
                                            {
                                                if cache_row != &old_row {
                                                    // update a nonexistent old value
                                                    output.insert(
                                                        key.clone(),
                                                        RowOp::Update((
                                                            cache_row.to_vec(),
                                                            new_row.clone(),
                                                        )),
                                                    );
                                                }
                                                self.materialize_cache.insert(key, Some(new_row));
                                            } else {
                                                // update a nonexistent pk
                                                output.insert(
                                                    key.clone(),
                                                    RowOp::Insert(new_row.clone()),
                                                );
                                                self.materialize_cache.insert(key, Some(new_row));
                                            }
                                        }
                                    }
                                }

                                // // construct output chunk
                                match generator_output(output, data_types.clone())? {
                                    Some(output_chunk) => {
                                        self.state_table.write_chunk(output_chunk.clone());
                                        Message::Chunk(output_chunk)
                                    }
                                    None => continue,
                                }
                            }
                        }
                        false => {
                            self.state_table.write_chunk(chunk.clone());
                            Message::Chunk(chunk)
                        }
                    }
                }
                Message::Barrier(b) => {
                    self.state_table.commit(b.epoch).await?;

                    // Update the vnode bitmap for the state table if asked.
                    if let Some(vnode_bitmap) = b.as_update_vnode_bitmap(self.actor_context.id) {
                        let _ = self.state_table.update_vnode_bitmap(vnode_bitmap);
                    }

                    Message::Barrier(b)
                }
            }
        }
    }
}

/// Construct output `StreamChunk` from given buffer.
fn generator_output(
    output: HashMap<Vec<u8>, RowOp>,
    data_types: Vec<DataType>,
) -> StreamExecutorResult<Option<StreamChunk>> {
    // construct output chunk
    let mut new_ops: Vec<Op> = vec![];
    let mut new_rows: Vec<Vec<u8>> = vec![];
    let row_deserializer = RowDeserializer::new(data_types.clone());
    for (_, row_op) in output {
        match row_op {
            RowOp::Insert(value) => {
                new_ops.push(Op::Insert);
                new_rows.push(value);
            }
            RowOp::Delete(old_value) => {
                new_ops.push(Op::Delete);
                new_rows.push(old_value);
            }
            RowOp::Update((old_value, new_value)) => {
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);
                new_rows.push(old_value);
                new_rows.push(new_value);
            }
        }
    }
    let mut data_chunk_builder = DataChunkBuilder::new(data_types, new_rows.len() + 1);

    for row_bytes in new_rows {
        let res = data_chunk_builder
            .append_one_row_from_datums(row_deserializer.deserialize(row_bytes.as_ref())?.0.iter());
        debug_assert!(res.is_none());
    }

    if let Some(new_data_chunk) = data_chunk_builder.consume_all() {
        let new_stream_chunk = StreamChunk::new(new_ops, new_data_chunk.columns().to_vec(), None);
        Ok(Some(new_stream_chunk))
    } else {
        Ok(None)
    }
}

/// `MaterializeBuffer` is a buffer to handle chunk into `RowOp`.
pub struct MaterializeBuffer {
    buffer: HashMap<Vec<u8>, RowOp>,
}

impl MaterializeBuffer {
    fn new() -> Self {
        Self {
            buffer: HashMap::new(),
        }
    }

    #[allow(clippy::disallowed_methods)]
    fn fill_buffer_from_chunk(
        stream_chunk: StreamChunk,
        value_indices: &Option<Vec<usize>>,
        dist_key_indices: &[usize],
        pk_indices: &[usize],
        pk_serde: &OrderedRowSerde,
        vnodes: &Bitmap,
    ) -> Self {
        let (data_chunk, ops) = stream_chunk.into_parts();

        let value_chunk = if let Some(ref value_indices) = value_indices {
            data_chunk.clone().reorder_columns(value_indices)
        } else {
            data_chunk.clone()
        };
        let values = value_chunk.serialize();

        let mut pks = vec![vec![]; data_chunk.capacity()];
        compute_chunk_vnode(&data_chunk, dist_key_indices, vnodes)
            .into_iter()
            .zip_eq(pks.iter_mut())
            .for_each(|(vnode, vnode_and_pk)| vnode_and_pk.extend(vnode.to_be_bytes()));
        let key_chunk = data_chunk.reorder_columns(pk_indices);
        key_chunk
            .rows_with_holes()
            .zip_eq(pks.iter_mut())
            .for_each(|(r, vnode_and_pk)| {
                if let Some(r) = r {
                    pk_serde.serialize_ref(r, vnode_and_pk);
                }
            });

        let (_, vis) = key_chunk.into_parts();

        let mut buffer = MaterializeBuffer::new();
        match vis {
            Vis::Bitmap(vis) => {
                for ((op, key, value), vis) in izip!(ops, pks, values).zip_eq(vis.iter()) {
                    if vis {
                        match op {
                            Op::Insert | Op::UpdateInsert => buffer.insert(key, value),
                            Op::Delete | Op::UpdateDelete => buffer.delete(key, value),
                        };
                    }
                }
            }
            Vis::Compact(_) => {
                for (op, key, value) in izip!(ops, pks, values) {
                    match op {
                        Op::Insert | Op::UpdateInsert => buffer.insert(key, value),
                        Op::Delete | Op::UpdateDelete => buffer.delete(key, value),
                    };
                }
            }
        }
        buffer
    }

    fn insert(&mut self, pk: Vec<u8>, value: Vec<u8>) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Insert(value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Delete(ref mut old_value) => {
                    let old_val = std::mem::take(old_value);
                    e.insert(RowOp::Update((old_val, value)));
                }
                _ => {
                    e.insert(RowOp::Insert(value));
                }
            },
        }
    }

    fn delete(&mut self, pk: Vec<u8>, old_value: Vec<u8>) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(RowOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                RowOp::Insert(_) => {
                    e.remove();
                }
                _ => {
                    e.insert(RowOp::Delete(old_value));
                }
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    fn get_keys(&self) -> Vec<&Vec<u8>> {
        self.buffer.keys().collect()
    }

    pub fn into_parts(self) -> HashMap<Vec<u8>, RowOp> {
        self.buffer
    }
}
impl<S: StateStore> Executor for MaterializeExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        self.info.identity.as_str()
    }
}

impl<S: StateStore> std::fmt::Debug for MaterializeExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("input info", &self.info())
            .field("arrange_columns", &self.arrange_columns)
            .finish()
    }
}

/// A cache for materialize executors.
pub struct MaterializeCache {
    data: ExecutorCache<Vec<u8>, Option<Vec<u8>>>,
}

impl MaterializeCache {
    pub fn new(lru_manager: Option<LruManagerRef>, cache_size: usize) -> Self {
        let cache = if let Some(lru_manager) = lru_manager {
            ExecutorCache::Managed(lru_manager.create_cache())
        } else {
            ExecutorCache::Local(EvictableHashMap::new(cache_size))
        };
        Self { data: cache }
    }

    pub fn get(&mut self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.data.get(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        self.data.push(key, value);
    }
}

#[cfg(test)]
mod tests {

    use futures::stream::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::Row;
    use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::storage_table::StorageTable;

    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6",
        );
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::new_test_barrier(3)),
            ],
        );

        let order_types = vec![OrderType::Ascending];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
        );

        let mut materialize_executor = Box::new(
            MaterializeExecutor::for_test(
                Box::new(source),
                memory_state_store,
                table_id,
                vec![OrderPair::new(0, OrderType::Ascending)],
                column_ids,
                1,
                None,
                0,
                false,
            )
            .await,
        )
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(3_i32.into()), Some(6_i32.into())])));
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(7_i32.into()), Some(8_i32.into())])));
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_materialize_executor_check_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to override the former.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 2 2
            + 8 3",
        );

        // test delete wrong value, delete inexistent pk
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            U- 2 4
            U+ 2 8
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::new_test_barrier(3)),
                Message::Chunk(chunk3),
                Message::Barrier(Barrier::new_test_barrier(4)),
            ],
        );

        let order_types = vec![OrderType::Ascending];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = StorageTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
        );

        let mut materialize_executor = Box::new(
            MaterializeExecutor::for_test(
                Box::new(source),
                memory_state_store,
                table_id,
                vec![OrderPair::new(0, OrderType::Ascending)],
                column_ids,
                1,
                None,
                1 << 16,
                true,
            )
            .await,
        )
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(3_i32.into()), Some(6_i32.into())])));

                let row = table
                    .get_row(
                        &Row(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(1_i32.into()), Some(4_i32.into())])));

                let row = table
                    .get_row(
                        &Row(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(8_i32.into()), Some(3_i32.into())])));
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(7_i32.into()), Some(8_i32.into())])));
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &Row(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(1_i32.into()), Some(5_i32.into())])));

                let row = table
                    .get_row(
                        &Row(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(Row(vec![Some(2_i32.into()), Some(8_i32.into())])));
            }
            _ => unreachable!(),
        }
    }
}
