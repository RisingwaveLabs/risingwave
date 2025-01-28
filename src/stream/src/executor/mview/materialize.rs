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

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::ops::{Deref, Index};

use bytes::Bytes;
use futures::stream;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, TableId};
use risingwave_common::row::{CompactedRow, RowDeserializer};
use risingwave_common::types::DefaultOrd;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_common::util::value_encoding::{BasicSerde, ValueRowSerializer};
use risingwave_pb::catalog::Table;
use risingwave_storage::mem_table::KeyOp;
use risingwave_storage::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::{StateTableInner, StateTableOpConsistencyLevel};
use crate::common::table::test_utils::gen_pbtable;
use crate::executor::monitor::MaterializeMetrics;
use crate::executor::prelude::*;

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore, SD: ValueRowSerde> {
    input: Executor,

    schema: Schema,

    state_table: StateTableInner<S, SD>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_key_indices: Vec<usize>,

    actor_context: ActorContextRef,

    materialize_cache: MaterializeCache<SD>,

    conflict_behavior: ConflictBehavior,

    version_column_index: Option<u32>,

    may_have_downstream: bool,

    depended_subscription_ids: HashSet<u32>,

    metrics: MaterializeMetrics,
}

fn get_op_consistency_level(
    conflict_behavior: ConflictBehavior,
    may_have_downstream: bool,
    depended_subscriptions: &HashSet<u32>,
) -> StateTableOpConsistencyLevel {
    if !depended_subscriptions.is_empty() {
        StateTableOpConsistencyLevel::LogStoreEnabled
    } else if !may_have_downstream && matches!(conflict_behavior, ConflictBehavior::Overwrite) {
        // Table with overwrite conflict behavior could disable conflict check
        // if no downstream mv depends on it, so we use a inconsistent_op to skip sanity check as well.
        StateTableOpConsistencyLevel::Inconsistent
    } else {
        StateTableOpConsistencyLevel::ConsistentOldValue
    }
}

impl<S: StateStore, SD: ValueRowSerde> MaterializeExecutor<S, SD> {
    /// Create a new `MaterializeExecutor` with distribution specified with `distribution_keys` and
    /// `vnodes`. For singleton distribution, `distribution_keys` should be empty and `vnodes`
    /// should be `None`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        input: Executor,
        schema: Schema,
        store: S,
        arrange_key: Vec<ColumnOrder>,
        actor_context: ActorContextRef,
        vnodes: Option<Arc<Bitmap>>,
        table_catalog: &Table,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
        version_column_index: Option<u32>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();

        let row_serde: BasicSerde = BasicSerde::new(
            Arc::from_iter(table_catalog.value_indices.iter().map(|val| *val as usize)),
            Arc::from(table_columns.into_boxed_slice()),
        );

        let arrange_key_indices: Vec<usize> = arrange_key.iter().map(|k| k.column_index).collect();
        let may_have_downstream = actor_context.initial_dispatch_num != 0;
        let depended_subscription_ids = actor_context
            .related_subscriptions
            .get(&TableId::new(table_catalog.id))
            .cloned()
            .unwrap_or_default();
        let op_consistency_level = get_op_consistency_level(
            conflict_behavior,
            may_have_downstream,
            &depended_subscription_ids,
        );
        // Note: The current implementation could potentially trigger a switch on the inconsistent_op flag. If the storage relies on this flag to perform optimizations, it would be advisable to maintain consistency with it throughout the lifecycle.
        let state_table = StateTableInner::from_table_catalog_with_consistency_level(
            table_catalog,
            store,
            vnodes,
            op_consistency_level,
        )
        .await;

        let mv_metrics = metrics.new_materialize_metrics(
            TableId::new(table_catalog.id),
            actor_context.id,
            actor_context.fragment_id,
        );

        let metrics_info =
            MetricsInfo::new(metrics, table_catalog.id, actor_context.id, "Materialize");

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices,
            actor_context,
            materialize_cache: MaterializeCache::new(
                watermark_epoch,
                metrics_info,
                row_serde,
                version_column_index,
            ),
            conflict_behavior,
            version_column_index,
            may_have_downstream,
            depended_subscription_ids,
            metrics: mv_metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mv_table_id = TableId::new(self.state_table.table_id());

        let data_types = self.schema.data_types();
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.state_table.init_epoch(first_epoch).await?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            self.materialize_cache.evict();

            yield match msg {
                Message::Watermark(w) => Message::Watermark(w),
                Message::Chunk(chunk) => {
                    self.metrics
                        .materialize_input_row_count
                        .inc_by(chunk.cardinality() as u64);

                    // This is an optimization that handles conflicts only when a particular materialized view downstream has no MV dependencies.
                    // This optimization is applied only when there is no specified version column and the is_consistent_op flag of the state table is false,
                    // and the conflict behavior is overwrite.
                    let do_not_handle_conflict = !self.state_table.is_consistent_op()
                        && self.version_column_index.is_none()
                        && self.conflict_behavior == ConflictBehavior::Overwrite;
                    match self.conflict_behavior {
                        ConflictBehavior::Overwrite
                        | ConflictBehavior::IgnoreConflict
                        | ConflictBehavior::DoUpdateIfNotNull
                            if !do_not_handle_conflict =>
                        {
                            if chunk.cardinality() == 0 {
                                // empty chunk
                                continue;
                            }
                            let (data_chunk, ops) = chunk.into_parts();

                            if self.state_table.value_indices().is_some() {
                                // TODO(st1page): when materialize partial columns(), we should
                                // construct some columns in the pk
                                panic!("materialize executor with data check can not handle only materialize partial columns")
                            };
                            let values = data_chunk.serialize();

                            let key_chunk = data_chunk.project(self.state_table.pk_indices());

                            let pks = {
                                let mut pks = vec![vec![]; data_chunk.capacity()];
                                key_chunk
                                    .rows_with_holes()
                                    .zip_eq_fast(pks.iter_mut())
                                    .for_each(|(r, vnode_and_pk)| {
                                        if let Some(r) = r {
                                            self.state_table.pk_serde().serialize(r, vnode_and_pk);
                                        }
                                    });
                                pks
                            };
                            let (_, vis) = key_chunk.into_parts();
                            let row_ops = ops
                                .iter()
                                .zip_eq_debug(pks.into_iter())
                                .zip_eq_debug(values.into_iter())
                                .zip_eq_debug(vis.iter())
                                .filter_map(|(((op, k), v), vis)| vis.then_some((*op, k, v)))
                                .collect_vec();

                            let fixed_changes = self
                                .materialize_cache
                                .handle(
                                    row_ops,
                                    &self.state_table,
                                    &self.conflict_behavior,
                                    &self.metrics,
                                )
                                .await?;

                            match generate_output(fixed_changes, data_types.clone())? {
                                Some(output_chunk) => {
                                    self.state_table.write_chunk(output_chunk.clone());
                                    self.state_table.try_flush().await?;
                                    Message::Chunk(output_chunk)
                                }
                                None => continue,
                            }
                        }
                        ConflictBehavior::IgnoreConflict => unreachable!(),
                        ConflictBehavior::NoCheck
                        | ConflictBehavior::Overwrite
                        | ConflictBehavior::DoUpdateIfNotNull => {
                            self.state_table.write_chunk(chunk.clone());
                            self.state_table.try_flush().await?;
                            Message::Chunk(chunk)
                        } // ConflictBehavior::DoUpdateIfNotNull => unimplemented!(),
                    }
                }
                Message::Barrier(b) => {
                    // If a downstream mv depends on the current table, we need to do conflict check again.
                    if !self.may_have_downstream
                        && b.has_more_downstream_fragments(self.actor_context.id)
                    {
                        self.may_have_downstream = true;
                    }
                    Self::may_update_depended_subscriptions(
                        &mut self.depended_subscription_ids,
                        &b,
                        mv_table_id,
                    );
                    let op_consistency_level = get_op_consistency_level(
                        self.conflict_behavior,
                        self.may_have_downstream,
                        &self.depended_subscription_ids,
                    );
                    self.state_table
                        .commit_may_switch_consistent_op(b.epoch, op_consistency_level)
                        .await?;
                    if !self.state_table.is_consistent_op() {
                        assert_eq!(self.conflict_behavior, ConflictBehavior::Overwrite);
                    }

                    // Update the vnode bitmap for the state table if asked.
                    if let Some(vnode_bitmap) = b.as_update_vnode_bitmap(self.actor_context.id) {
                        let (_, cache_may_stale) =
                            self.state_table.update_vnode_bitmap(vnode_bitmap);

                        if cache_may_stale {
                            self.materialize_cache.data.clear();
                        }
                    }

                    self.metrics
                        .materialize_current_epoch
                        .set(b.epoch.curr as i64);

                    Message::Barrier(b)
                }
            }
        }
    }

    /// return true when changed
    fn may_update_depended_subscriptions(
        depended_subscriptions: &mut HashSet<u32>,
        barrier: &Barrier,
        mv_table_id: TableId,
    ) {
        for subscriber_id in barrier.added_subscriber_on_mv_table(mv_table_id) {
            if !depended_subscriptions.insert(subscriber_id) {
                warn!(
                    ?depended_subscriptions,
                    ?mv_table_id,
                    subscriber_id,
                    "subscription id already exists"
                );
            }
        }

        if let Some(Mutation::DropSubscriptions {
            subscriptions_to_drop,
        }) = barrier.mutation.as_deref()
        {
            for (subscriber_id, upstream_mv_table_id) in subscriptions_to_drop {
                if *upstream_mv_table_id == mv_table_id
                    && !depended_subscriptions.remove(subscriber_id)
                {
                    warn!(
                        ?depended_subscriptions,
                        ?mv_table_id,
                        subscriber_id,
                        "drop non existing subscriber_id id"
                    );
                }
            }
        }
    }
}

impl<S: StateStore> MaterializeExecutor<S, BasicSerde> {
    /// Create a new `MaterializeExecutor` without distribution info for test purpose.
    #[allow(clippy::too_many_arguments)]
    pub async fn for_test(
        input: Executor,
        store: S,
        table_id: TableId,
        keys: Vec<ColumnOrder>,
        column_ids: Vec<ColumnId>,
        watermark_epoch: AtomicU64Ref,
        conflict_behavior: ConflictBehavior,
    ) -> Self {
        let arrange_columns: Vec<usize> = keys.iter().map(|k| k.column_index).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        let schema = input.schema().clone();
        let columns: Vec<ColumnDesc> = column_ids
            .into_iter()
            .zip_eq_fast(schema.fields.iter())
            .map(|(column_id, field)| ColumnDesc::unnamed(column_id, field.data_type()))
            .collect_vec();

        let row_serde = BasicSerde::new(
            Arc::from((0..columns.len()).collect_vec()),
            Arc::from(columns.clone().into_boxed_slice()),
        );
        let state_table = StateTableInner::from_table_catalog(
            &gen_pbtable(
                table_id,
                columns,
                arrange_order_types,
                arrange_columns.clone(),
                0,
            ),
            store,
            None,
        )
        .await;

        let metrics = StreamingMetrics::unused().new_materialize_metrics(table_id, 1, 2);

        Self {
            input,
            schema,
            state_table,
            arrange_key_indices: arrange_columns.clone(),
            actor_context: ActorContext::for_test(0),
            materialize_cache: MaterializeCache::new(
                watermark_epoch,
                MetricsInfo::for_test(),
                row_serde,
                None,
            ),
            conflict_behavior,
            version_column_index: None,
            may_have_downstream: true,
            depended_subscription_ids: HashSet::new(),
            metrics,
        }
    }
}

/// Construct output `StreamChunk` from given buffer.
fn generate_output(
    changes: MaterializeBuffer,
    data_types: Vec<DataType>,
) -> StreamExecutorResult<Option<StreamChunk>> {
    // construct output chunk
    // TODO(st1page): when materialize partial columns(), we should construct some columns in the pk
    let mut new_ops: Vec<Op> = vec![];
    let mut new_rows: Vec<Bytes> = vec![];
    let row_deserializer = RowDeserializer::new(data_types.clone());
    for (_, row_op) in changes.into_parts() {
        match row_op {
            KeyOp::Insert(value) => {
                new_ops.push(Op::Insert);
                new_rows.push(value);
            }
            KeyOp::Delete(old_value) => {
                new_ops.push(Op::Delete);
                new_rows.push(old_value);
            }
            KeyOp::Update((old_value, new_value)) => {
                // if old_value == new_value, we don't need to emit updates to downstream.
                if old_value != new_value {
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                    new_rows.push(old_value);
                    new_rows.push(new_value);
                }
            }
        }
    }
    let mut data_chunk_builder = DataChunkBuilder::new(data_types, new_rows.len() + 1);

    for row_bytes in new_rows {
        let res =
            data_chunk_builder.append_one_row(row_deserializer.deserialize(row_bytes.as_ref())?);
        debug_assert!(res.is_none());
    }

    if let Some(new_data_chunk) = data_chunk_builder.consume_all() {
        let new_stream_chunk = StreamChunk::new(new_ops, new_data_chunk.columns().to_vec());
        Ok(Some(new_stream_chunk))
    } else {
        Ok(None)
    }
}

/// `MaterializeBuffer` is a buffer to handle chunk into `KeyOp`.
pub struct MaterializeBuffer {
    buffer: HashMap<Vec<u8>, KeyOp>,
}

impl MaterializeBuffer {
    fn new() -> Self {
        Self {
            buffer: HashMap::new(),
        }
    }

    pub fn insert(&mut self, pk: Vec<u8>, value: Bytes) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Insert(value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Delete(ref mut old_value) => {
                    let old_val = std::mem::take(old_value);
                    e.insert(KeyOp::Update((old_val, value)));
                }
                KeyOp::Insert(_) | KeyOp::Update(_) => {
                    unreachable!();
                }
            },
        }
    }

    pub fn delete(&mut self, pk: Vec<u8>, old_value: Bytes) {
        let entry: Entry<'_, Vec<u8>, KeyOp> = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Delete(old_value));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(_) => {
                    e.remove();
                }
                KeyOp::Update((prev, _curr)) => {
                    let prev = std::mem::take(prev);
                    e.insert(KeyOp::Delete(prev));
                }
                KeyOp::Delete(_) => {
                    unreachable!();
                }
            },
        }
    }

    pub fn update(&mut self, pk: Vec<u8>, old_value: Bytes, new_value: Bytes) {
        let entry = self.buffer.entry(pk);
        match entry {
            Entry::Vacant(e) => {
                e.insert(KeyOp::Update((old_value, new_value)));
            }
            Entry::Occupied(mut e) => match e.get_mut() {
                KeyOp::Insert(_) => {
                    e.insert(KeyOp::Insert(new_value));
                }
                KeyOp::Update((_prev, curr)) => {
                    *curr = new_value;
                }
                KeyOp::Delete(_) => {
                    unreachable!()
                }
            },
        }
    }

    pub fn into_parts(self) -> HashMap<Vec<u8>, KeyOp> {
        self.buffer
    }
}
impl<S: StateStore, SD: ValueRowSerde> Execute for MaterializeExecutor<S, SD> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for MaterializeExecutor<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("arrange_key_indices", &self.arrange_key_indices)
            .finish()
    }
}

/// A cache for materialize executors.
pub struct MaterializeCache<SD> {
    data: ManagedLruCache<Vec<u8>, CacheValue>,
    row_serde: BasicSerde,
    version_column_index: Option<u32>,
    _serde: PhantomData<SD>,
}

type CacheValue = Option<CompactedRow>;

impl<SD: ValueRowSerde> MaterializeCache<SD> {
    pub fn new(
        watermark_sequence: AtomicU64Ref,
        metrics_info: MetricsInfo,
        row_serde: BasicSerde,
        version_column_index: Option<u32>,
    ) -> Self {
        let cache: ManagedLruCache<Vec<u8>, CacheValue> =
            ManagedLruCache::unbounded(watermark_sequence, metrics_info.clone());
        Self {
            data: cache,
            row_serde,
            version_column_index,
            _serde: PhantomData,
        }
    }

    pub async fn handle<'a, S: StateStore>(
        &mut self,
        row_ops: Vec<(Op, Vec<u8>, Bytes)>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: &ConflictBehavior,
        metrics: &MaterializeMetrics,
    ) -> StreamExecutorResult<MaterializeBuffer> {
        let key_set: HashSet<Box<[u8]>> = row_ops
            .iter()
            .map(|(_, k, _)| k.as_slice().into())
            .collect();
        self.fetch_keys(
            key_set.iter().map(|v| v.deref()),
            table,
            conflict_behavior,
            metrics,
        )
        .await?;
        let mut fixed_changes = MaterializeBuffer::new();
        let row_serde = self.row_serde.clone();
        let version_column_index = self.version_column_index;
        for (op, key, value) in row_ops {
            let mut update_cache = false;
            let fixed_changes = &mut fixed_changes;
            // When you change the fixed_changes buffer, you must mark `update_cache` to true so the cache and downstream can be consistent.
            let fixed_changes = || {
                update_cache = true;
                fixed_changes
            };
            match op {
                Op::Insert | Op::UpdateInsert => {
                    match conflict_behavior {
                        ConflictBehavior::Overwrite => {
                            match self.force_get(&key) {
                                Some(old_row) => {
                                    let mut handle_conflict = true;
                                    if let Some(idx) = version_column_index {
                                        let old_row_deserialized = row_serde
                                            .deserializer
                                            .deserialize(old_row.row.clone())?;
                                        let new_row_deserialized =
                                            row_serde.deserializer.deserialize(value.clone())?;

                                        handle_conflict = should_handle_conflict(
                                            old_row_deserialized.index(idx as usize),
                                            new_row_deserialized.index(idx as usize),
                                        );
                                    }
                                    if handle_conflict {
                                        fixed_changes().update(
                                            key.clone(),
                                            old_row.row.clone(),
                                            value.clone(),
                                        )
                                    } else {
                                        update_cache = false;
                                    }
                                }
                                None => fixed_changes().insert(key.clone(), value.clone()),
                            };
                        }
                        ConflictBehavior::IgnoreConflict => {
                            match self.force_get(&key) {
                                Some(_) => (),
                                None => {
                                    fixed_changes().insert(key.clone(), value.clone());
                                }
                            };
                        }

                        ConflictBehavior::DoUpdateIfNotNull => {
                            match self.force_get(&key) {
                                Some(old_row) => {
                                    // In this section, we compare the new row and old row column by column and perform `DoUpdateIfNotNull` replacement.
                                    // todo(wcy-fdu):find a way to output the resulting new row directly to the downstream chunk, thus avoiding an additional deserialization step.
                                    let mut old_row_deserialized_vec = row_serde
                                        .deserializer
                                        .deserialize(old_row.row.clone())?
                                        .into_inner()
                                        .into_vec();
                                    let new_row_deserialized =
                                        row_serde.deserializer.deserialize(value.clone())?;
                                    let mut handle_conflict = true;
                                    if let Some(idx) = version_column_index {
                                        handle_conflict = should_handle_conflict(
                                            &old_row_deserialized_vec[idx as usize],
                                            new_row_deserialized.index(idx as usize),
                                        );
                                    }
                                    if handle_conflict {
                                        execute_do_update_if_not_null_replacement(
                                            &mut old_row_deserialized_vec,
                                            new_row_deserialized,
                                        );
                                        let new_row = OwnedRow::new(old_row_deserialized_vec);
                                        let new_row_bytes =
                                            Bytes::from(row_serde.serializer.serialize(new_row));
                                        fixed_changes().update(
                                            key.clone(),
                                            old_row.row.clone(),
                                            new_row_bytes.clone(),
                                        );
                                        // Since `DoUpdateIfNotNull` may generate values that differ from both the new row and old row,
                                        // an update cache needs to be done here.
                                        self.data.push(
                                            key.clone(),
                                            Some(CompactedRow { row: new_row_bytes }),
                                        );
                                    }
                                    update_cache = false;
                                }
                                None => {
                                    fixed_changes().insert(key.clone(), value.clone());
                                    update_cache = true;
                                }
                            };
                        }
                        _ => unreachable!(),
                    };

                    if update_cache {
                        match conflict_behavior {
                            ConflictBehavior::Overwrite
                            | ConflictBehavior::IgnoreConflict
                            | ConflictBehavior::DoUpdateIfNotNull => {
                                self.data.push(key, Some(CompactedRow { row: value }));
                            }

                            _ => unreachable!(),
                        }
                    }
                }

                Op::Delete | Op::UpdateDelete => {
                    match conflict_behavior {
                        ConflictBehavior::Overwrite
                        | ConflictBehavior::IgnoreConflict
                        | ConflictBehavior::DoUpdateIfNotNull => {
                            // delete a nonexistent value
                            if let Some(old_row) = self.force_get(&key) {
                                fixed_changes().delete(key.clone(), old_row.row.clone());
                            };
                        }
                        _ => unreachable!(),
                    };

                    if update_cache {
                        self.data.push(key, None);
                    }
                }
            }
        }
        Ok(fixed_changes)
    }

    async fn fetch_keys<'a, S: StateStore>(
        &mut self,
        keys: impl Iterator<Item = &'a [u8]>,
        table: &StateTableInner<S, SD>,
        conflict_behavior: &ConflictBehavior,
        metrics: &MaterializeMetrics,
    ) -> StreamExecutorResult<()> {
        let mut futures = vec![];
        for key in keys {
            metrics.materialize_cache_total_count.inc();

            if self.data.contains(key) {
                metrics.materialize_cache_hit_count.inc();
                continue;
            }
            futures.push(async {
                let key_row = table.pk_serde().deserialize(key).unwrap();
                let row = table.get_row(key_row).await?.map(CompactedRow::from);
                StreamExecutorResult::Ok((key.to_vec(), row))
            });
        }

        let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
        while let Some(result) = buffered.next().await {
            let (key, value) = result?;
            match conflict_behavior {
                ConflictBehavior::Overwrite | ConflictBehavior::DoUpdateIfNotNull => {
                    self.data.push(key, value)
                }
                ConflictBehavior::IgnoreConflict => self.data.push(key, value),
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    pub fn force_get(&mut self, key: &[u8]) -> &CacheValue {
        self.data.get(key).unwrap_or_else(|| {
            panic!(
                "the key {:?} has not been fetched in the materialize executor's cache ",
                key
            )
        })
    }

    fn evict(&mut self) {
        self.data.evict()
    }
}

/// Generates a new row with the behavior of "do update if not null" by replacing columns in the old row with non-empty columns in the new row.
///
/// # Arguments
///
/// * `old_row` - The old row containing the original values.
/// * `new_row` - The new row containing the replacement values.
///
/// # Example
///
/// ```no_run
/// let old_row = vec![Some(1), None, Some(3)];
/// let mut new_row = vec![Some(10), Some(20), None];
///
///
/// // After the execute_do_update_if_not_null_replacement function call, old_row will be [Some(10), Some(20), Some(3)].
/// ```
fn execute_do_update_if_not_null_replacement(
    old_row: &mut Vec<Option<ScalarImpl>>,
    new_row: OwnedRow,
) {
    for (old_col, new_col) in old_row.iter_mut().zip_eq_fast(new_row) {
        if let Some(new_value) = new_col {
            *old_col = Some(new_value);
        }
    }
}
/// Determines whether pk conflict should be handled based on the version column of the new and old rows.
///
/// # Arguments
///
/// * `old_version_column`: The version column value of the old row.
/// * `new_version_column`: The version column value of the new row.
///
/// # Returns
///
/// A boolean value indicating whether a conflict should be handled.
///
/// If both the new row's version column and the old row's version column are Some, the function will return true if the new row's version is greater than or equal to the old row's version.
/// If the new row's version column is None and the old row's version column is Some, do not handle pk conflict.
/// If both the new row's version column and the old row's version column are None, should handle pk conflict.
fn should_handle_conflict(
    old_version_column: &Option<ScalarImpl>,
    new_version_column: &Option<ScalarImpl>,
) -> bool {
    match (old_version_column, new_version_column) {
        (None, None) => true,
        (None, Some(_)) => true,
        (Some(_), None) => false,
        (Some(old_version_col), Some(new_version_col)) => !matches!(
            old_version_col.default_cmp(new_version_col),
            Ordering::Greater
        ),
    }
}

#[cfg(test)]
mod tests {

    use std::iter;
    use std::sync::atomic::AtomicU64;

    use rand::rngs::SmallRng;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::array::stream_chunk::{StreamChunkMut, StreamChunkTestExt};
    use risingwave_common::catalog::Field;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::batch_table::BatchTable;

    use super::*;
    use crate::executor::test_utils::*;

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
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::NoCheck,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    // https://github.com/risingwavelabs/risingwave/issues/13346
    #[tokio::test]
    async fn test_upsert_stream() {
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
            + 1 1",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 2
            - 1 2",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert!(row.is_none());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_check_insert_conflict() {
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
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 4",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(6_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_delete_and_update_conflict() {
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
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
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
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::Overwrite,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 3), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(3_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(5_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_insert_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter needs to be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            + 1 4
            + 2 5
            + 3 6",
        );

        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 1 5
            + 2 6",
        );

        // test delete wrong value, delete inexistent pk
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();
        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(3_i32.into()), Some(6_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(3_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(5_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_ignore_delete_then_insert() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test insert after delete one pk, the latter insert should succeed.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 3
            - 1 3
            + 1 6",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        let _msg1 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();
        let _msg2 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_chunk()
            .unwrap();
        let _msg3 = materialize_executor
            .next()
            .await
            .transpose()
            .unwrap()
            .unwrap()
            .as_barrier()
            .unwrap();

        let row = table
            .get_row(
                &OwnedRow::new(vec![Some(1_i32.into())]),
                HummockReadEpoch::NoWait(u64::MAX),
            )
            .await
            .unwrap();
        assert_eq!(
            row,
            Some(OwnedRow::new(vec![Some(1_i32.into()), Some(6_i32.into())]))
        );
    }

    #[tokio::test]
    async fn test_ignore_delete_and_update_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // test double insert one pk, the latter should be ignored.
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 5
            + 3 6
            U- 8 1
            U+ 8 2
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
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::IgnoreConflict,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                // can read (8, 2), check insert after update
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(2_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(1_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(1_i32.into()), Some(4_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(2_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_do_update_if_not_null_conflict() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // should get (8, 2)
        let chunk1 = StreamChunk::from_pretty(
            " i i
            + 1 4
            + 2 .
            + 3 6
            U- 8 .
            U+ 8 2
            + 8 .",
        );

        // should not get (3, x), should not get (5, 0)
        let chunk2 = StreamChunk::from_pretty(
            " i i
            + 7 8
            - 3 4
            - 5 0",
        );

        // should get (2, None), (7, 8)
        let chunk3 = StreamChunk::from_pretty(
            " i i
            + 1 5
            + 7 .
            U- 2 4
            U+ 2 .
            U- 9 0
            U+ 9 1",
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(chunk1),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(chunk2),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(chunk3),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), PkIndices::new());

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(column_ids[0], DataType::Int32),
            ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ];

        let table = BatchTable::for_test(
            memory_state_store.clone(),
            table_id,
            column_descs,
            order_types,
            vec![0],
            vec![0, 1],
        );

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store,
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            ConflictBehavior::DoUpdateIfNotNull,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.next().await.transpose().unwrap();

        materialize_executor.next().await.transpose().unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(8_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(8_i32.into()), Some(2_i32.into())]))
                );

                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(OwnedRow::new(vec![Some(2_i32.into()), None])));
            }
            _ => unreachable!(),
        }
        materialize_executor.next().await.transpose().unwrap();

        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check delete wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(3_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);

                // check delete wrong pk
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(5_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, None);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.transpose().unwrap();
        // materialize_executor.next().await.transpose().unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.transpose().unwrap() {
            Some(Message::Barrier(_)) => {
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(7_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(7_i32.into()), Some(8_i32.into())]))
                );

                // check update wrong value
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(2_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(row, Some(OwnedRow::new(vec![Some(2_i32.into()), None])));

                // check update wrong pk, should become insert
                let row = table
                    .get_row(
                        &OwnedRow::new(vec![Some(9_i32.into())]),
                        HummockReadEpoch::NoWait(u64::MAX),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    row,
                    Some(OwnedRow::new(vec![Some(9_i32.into()), Some(1_i32.into())]))
                );
            }
            _ => unreachable!(),
        }
    }

    fn gen_fuzz_data(row_number: usize, chunk_size: usize) -> Vec<StreamChunk> {
        const KN: u32 = 4;
        const SEED: u64 = 998244353;
        let mut ret = vec![];
        let mut builder =
            StreamChunkBuilder::new(chunk_size, vec![DataType::Int32, DataType::Int32]);
        let mut rng = SmallRng::seed_from_u64(SEED);

        let random_vis = |c: StreamChunk, rng: &mut SmallRng| -> StreamChunk {
            let len = c.data_chunk().capacity();
            let mut c = StreamChunkMut::from(c);
            for i in 0..len {
                c.set_vis(i, rng.gen_bool(0.5));
            }
            c.into()
        };
        for _ in 0..row_number {
            let k = (rng.next_u32() % KN) as i32;
            let v = rng.next_u32() as i32;
            let op = if rng.gen_bool(0.5) {
                Op::Insert
            } else {
                Op::Delete
            };
            if let Some(c) =
                builder.append_row(op, OwnedRow::new(vec![Some(k.into()), Some(v.into())]))
            {
                ret.push(random_vis(c, &mut rng));
            }
        }
        if let Some(c) = builder.take() {
            ret.push(random_vis(c, &mut rng));
        }
        ret
    }

    async fn fuzz_test_stream_consistent_inner(conflict_behavior: ConflictBehavior) {
        const N: usize = 100000;

        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        let chunks = gen_fuzz_data(N, 128);
        let messages = iter::once(Message::Barrier(Barrier::new_test_barrier(test_epoch(1))))
            .chain(chunks.into_iter().map(Message::Chunk))
            .chain(iter::once(Message::Barrier(Barrier::new_test_barrier(
                test_epoch(2),
            ))))
            .collect();
        // Prepare stream executors.
        let source =
            MockSource::with_messages(messages).into_executor(schema.clone(), PkIndices::new());

        let mut materialize_executor = MaterializeExecutor::for_test(
            source,
            memory_state_store.clone(),
            table_id,
            vec![ColumnOrder::new(0, OrderType::ascending())],
            column_ids,
            Arc::new(AtomicU64::new(0)),
            conflict_behavior,
        )
        .await
        .boxed()
        .execute();
        materialize_executor.expect_barrier().await;

        let order_types = vec![OrderType::ascending()];
        let column_descs = vec![
            ColumnDesc::unnamed(0.into(), DataType::Int32),
            ColumnDesc::unnamed(1.into(), DataType::Int32),
        ];
        let pk_indices = vec![0];

        let mut table = StateTable::from_table_catalog(
            &gen_pbtable(
                TableId::from(1002),
                column_descs.clone(),
                order_types,
                pk_indices,
                0,
            ),
            memory_state_store.clone(),
            None,
        )
        .await;

        while let Message::Chunk(c) = materialize_executor.next().await.unwrap().unwrap() {
            // check with state table's memtable
            table.write_chunk(c);
        }
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_upsert() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::Overwrite).await
    }

    #[tokio::test]
    async fn fuzz_test_stream_consistent_ignore() {
        fuzz_test_stream_consistent_inner(ConflictBehavior::IgnoreConflict).await
    }
}
