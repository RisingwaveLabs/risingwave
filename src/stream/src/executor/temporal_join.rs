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

use std::alloc::Global;
use std::collections::hash_map::Entry;
use std::collections::HashMap;

use either::Either;
use futures::stream::{self, PollNext};
use futures::TryStreamExt;
use itertools::Itertools;
use local_stats_alloc::{SharedStatsAlloc, StatsAlloc};
use lru::DefaultHasher;
use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::{HashKey, NullBitmap};
use risingwave_common::row::RowExt;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common_estimate_size::{EstimateSize, KvSize};
use risingwave_expr::expr::NonStrictExpression;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::table::TableIter;

use super::join::{JoinType, JoinTypePrimitive};
use super::monitor::TemporalJoinMetrics;
use crate::cache::{cache_may_stale, ManagedLruCache};
use crate::common::metrics::MetricsInfo;
use crate::executor::join::builder::JoinStreamChunkBuilder;
use crate::executor::prelude::*;

pub struct TemporalJoinExecutor<
    K: HashKey,
    S: StateStore,
    const T: JoinTypePrimitive,
    const APPEND_ONLY: bool,
> {
    ctx: ActorContextRef,
    #[allow(dead_code)]
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: TemporalSide<K, S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    chunk_size: usize,
    memo_table: Option<StateTable<S>>,
    metrics: TemporalJoinMetrics,
}

#[derive(Default)]
pub struct JoinEntry {
    /// pk -> row
    cached: HashMap<OwnedRow, OwnedRow>,
    kv_heap_size: KvSize,
}

impl EstimateSize for JoinEntry {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl JoinEntry {
    /// Insert into the cache.
    pub fn insert(&mut self, key: OwnedRow, value: OwnedRow) {
        // Lookup might refill the cache before the `insert` messages from the temporal side
        // upstream.
        if let Entry::Vacant(e) = self.cached.entry(key) {
            self.kv_heap_size.add(e.key(), &value);
            e.insert(value);
        }
    }

    /// Delete from the cache.
    pub fn remove(&mut self, key: &OwnedRow) {
        if let Some(value) = self.cached.remove(key) {
            self.kv_heap_size.sub(key, &value);
        } else {
            panic!("key {:?} should be in the cache", key);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.cached.is_empty()
    }
}

struct TemporalSide<K: HashKey, S: StateStore> {
    source: BatchTable<S>,
    table_stream_key_indices: Vec<usize>,
    table_output_indices: Vec<usize>,
    cache: ManagedLruCache<K, JoinEntry, DefaultHasher, SharedStatsAlloc<Global>>,
    join_key_data_types: Vec<DataType>,
}

impl<K: HashKey, S: StateStore> TemporalSide<K, S> {
    /// Fetch records from temporal side table and ensure the entry in the cache.
    /// If already exists, the entry will be promoted.
    async fn fetch_or_promote_keys(
        &mut self,
        keys: impl Iterator<Item = &K>,
        epoch: HummockEpoch,
        metrics: &TemporalJoinMetrics,
    ) -> StreamExecutorResult<()> {
        let mut futs = Vec::with_capacity(keys.size_hint().1.unwrap_or(0));
        for key in keys {
            metrics.temporal_join_total_query_cache_count.inc();

            if self.cache.get(key).is_none() {
                metrics.temporal_join_cache_miss_count.inc();

                futs.push(async {
                    let pk_prefix = key.deserialize(&self.join_key_data_types)?;

                    let iter = self
                        .source
                        .batch_iter_with_pk_bounds(
                            HummockReadEpoch::NoWait(epoch),
                            &pk_prefix,
                            ..,
                            false,
                            PrefetchOptions::default(),
                        )
                        .await?;

                    let mut entry = JoinEntry::default();

                    pin_mut!(iter);
                    while let Some(row) = iter.next_row().await? {
                        entry.insert(
                            row.as_ref()
                                .project(&self.table_stream_key_indices)
                                .into_owned_row(),
                            row.project(&self.table_output_indices).into_owned_row(),
                        );
                    }
                    let key = key.clone();
                    Ok((key, entry)) as StreamExecutorResult<_>
                });
            }
        }

        #[for_await]
        for res in stream::iter(futs).buffered(16) {
            let (key, entry) = res?;
            self.cache.put(key, entry);
        }

        Ok(())
    }

    fn force_peek(&self, key: &K) -> &JoinEntry {
        self.cache.peek(key).expect("key should exists")
    }

    fn update(
        &mut self,
        chunks: Vec<StreamChunk>,
        join_keys: &[usize],
        right_stream_key_indices: &[usize],
    ) -> StreamExecutorResult<()> {
        for chunk in chunks {
            let keys = K::build_many(join_keys, chunk.data_chunk());
            for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
                let Some((op, row)) = r else {
                    continue;
                };
                if self.cache.contains(&key) {
                    // Update cache
                    let mut entry = self.cache.get_mut(&key).unwrap();
                    let stream_key = row.project(right_stream_key_indices).into_owned_row();
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            entry.insert(stream_key, row.into_owned_row())
                        }
                        Op::Delete | Op::UpdateDelete => entry.remove(&stream_key),
                    };
                }
            }
        }
        Ok(())
    }
}

pub(super) enum InternalMessage {
    Chunk(StreamChunk),
    Barrier(Vec<StreamChunk>, Barrier),
    WaterMark(Watermark),
}

#[try_stream(ok = StreamChunk, error = StreamExecutorError)]
async fn chunks_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(_) => {
                // ignore
            }
            Message::Chunk(c) => yield c,
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
async fn internal_messages_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(w) => {
                yield InternalMessage::WaterMark(w);
            }
            Message::Chunk(c) => yield InternalMessage::Chunk(c),
            Message::Barrier(b) if b.epoch != expected_barrier.epoch => {
                return Err(StreamExecutorError::align_barrier(expected_barrier, b));
            }
            Message::Barrier(_) => return Ok(()),
        }
    }
}

// Align the left and right inputs according to their barriers,
// such that in the produced stream, an aligned interval starts with
// any number of `InternalMessage::Chunk(left_chunk)` and followed by
// `InternalMessage::Barrier(right_chunks, barrier)`.
#[try_stream(ok = InternalMessage, error = StreamExecutorError)]
pub(super) async fn align_input<const YIELD_RIGHT_CHUNKS: bool>(left: Executor, right: Executor) {
    let mut left = pin!(left.execute());
    let mut right = pin!(right.execute());
    // Keep producing intervals until stream exhaustion or errors.
    loop {
        let mut right_chunks = vec![];
        // Produce an aligned interval.
        'inner: loop {
            let mut combined = stream::select_with_strategy(
                left.by_ref().map(Either::Left),
                right.by_ref().map(Either::Right),
                |_: &mut ()| PollNext::Left,
            );
            match combined.next().await {
                Some(Either::Left(Ok(Message::Chunk(c)))) => yield InternalMessage::Chunk(c),
                Some(Either::Right(Ok(Message::Chunk(c)))) => {
                    if YIELD_RIGHT_CHUNKS {
                        right_chunks.push(c);
                    }
                }
                Some(Either::Left(Ok(Message::Barrier(b)))) => {
                    let mut remain = chunks_until_barrier(right.by_ref(), b.clone())
                        .try_collect()
                        .await?;
                    if YIELD_RIGHT_CHUNKS {
                        right_chunks.append(&mut remain);
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Right(Ok(Message::Barrier(b)))) => {
                    #[for_await]
                    for internal_message in
                        internal_messages_until_barrier(left.by_ref(), b.clone())
                    {
                        yield internal_message?;
                    }
                    yield InternalMessage::Barrier(right_chunks, b);
                    break 'inner;
                }
                Some(Either::Left(Err(e)) | Either::Right(Err(e))) => return Err(e),
                Some(Either::Left(Ok(Message::Watermark(w)))) => {
                    yield InternalMessage::WaterMark(w);
                }
                Some(Either::Right(Ok(Message::Watermark(_)))) => {
                    // ignore right side watermark
                }
                None => return Ok(()),
            }
        }
    }
}

pub(super) fn apply_indices_map(chunk: StreamChunk, indices: &[usize]) -> StreamChunk {
    let (data_chunk, ops) = chunk.into_parts();
    let (columns, vis) = data_chunk.into_parts();
    let output_columns = indices
        .iter()
        .cloned()
        .map(|idx| columns[idx].clone())
        .collect();
    StreamChunk::with_visibility(ops, output_columns, vis)
}

pub(super) mod phase1 {
    use std::ops::Bound;

    use futures::{pin_mut, StreamExt};
    use futures_async_stream::try_stream;
    use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::hash::{HashKey, NullBitmap};
    use risingwave_common::row::{self, OwnedRow, Row, RowExt};
    use risingwave_common::types::{DataType, DatumRef};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_hummock_sdk::HummockEpoch;
    use risingwave_storage::StateStore;

    use super::{StreamExecutorError, TemporalSide};
    use crate::common::table::state_table::StateTable;
    use crate::executor::monitor::TemporalJoinMetrics;

    pub trait Phase1Evaluation {
        /// Called when a matched row is found.
        #[must_use = "consume chunk if produced"]
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk>;

        /// Called when all matched rows of a join key are appended.
        #[must_use = "consume chunk if produced"]
        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk>;
    }

    pub struct Inner;
    pub struct LeftOuter;
    pub struct LeftOuterWithCond;

    impl Phase1Evaluation for Inner {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            _builder: &mut StreamChunkBuilder,
            _op: Op,
            _left_row: impl Row,
            _right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            None
        }
    }

    impl Phase1Evaluation for LeftOuter {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            matched: bool,
        ) -> Option<StreamChunk> {
            if !matched {
                // If no rows matched, a marker row should be inserted.
                builder.append_row(
                    op,
                    left_row.chain(row::repeat_n(DatumRef::None, right_size)),
                )
            } else {
                None
            }
        }
    }

    impl Phase1Evaluation for LeftOuterWithCond {
        fn append_matched_row(
            op: Op,
            builder: &mut StreamChunkBuilder,
            left_row: impl Row,
            right_row: impl Row,
        ) -> Option<StreamChunk> {
            builder.append_row(op, left_row.chain(right_row))
        }

        fn match_end(
            builder: &mut StreamChunkBuilder,
            op: Op,
            left_row: impl Row,
            right_size: usize,
            _matched: bool,
        ) -> Option<StreamChunk> {
            // A marker row should always be inserted and mark as invisible for non-lookup filters evaluation.
            // The row will be converted to visible in the further steps if no rows matched after all filters evaluated.
            builder.append_row_invisible(
                op,
                left_row.chain(row::repeat_n(DatumRef::None, right_size)),
            )
        }
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn handle_chunk<
        'a,
        K: HashKey,
        S: StateStore,
        E: Phase1Evaluation,
        const APPEND_ONLY: bool,
    >(
        chunk_size: usize,
        right_size: usize,
        full_schema: Vec<DataType>,
        epoch: HummockEpoch,
        left_join_keys: &'a [usize],
        right_table: &'a mut TemporalSide<K, S>,
        memo_table_lookup_prefix: &'a [usize],
        memo_table: &'a mut Option<StateTable<S>>,
        null_matched: &'a K::Bitmap,
        chunk: StreamChunk,
        metrics: &'a TemporalJoinMetrics,
    ) {
        let mut builder = StreamChunkBuilder::new(chunk_size, full_schema);
        let keys = K::build_many(left_join_keys, chunk.data_chunk());
        let to_fetch_keys = chunk
            .visibility()
            .iter()
            .zip_eq_debug(keys.iter())
            .zip_eq_debug(chunk.ops())
            .filter_map(|((vis, key), op)| {
                if vis {
                    if APPEND_ONLY {
                        assert_eq!(*op, Op::Insert);
                        Some(key)
                    } else {
                        match op {
                            Op::Insert | Op::UpdateInsert => Some(key),
                            Op::Delete | Op::UpdateDelete => None,
                        }
                    }
                } else {
                    None
                }
            });
        right_table
            .fetch_or_promote_keys(to_fetch_keys, epoch, metrics)
            .await?;

        for (r, key) in chunk.rows_with_holes().zip_eq_debug(keys.into_iter()) {
            let Some((op, left_row)) = r else {
                continue;
            };

            let mut matched = false;

            if APPEND_ONLY {
                // Append-only temporal join
                if key.null_bitmap().is_subset(null_matched)
                    && let join_entry = right_table.force_peek(&key)
                    && !join_entry.is_empty()
                {
                    matched = true;
                    for right_row in join_entry.cached.values() {
                        if let Some(chunk) =
                            E::append_matched_row(op, &mut builder, left_row, right_row)
                        {
                            yield chunk;
                        }
                    }
                }
            } else {
                // Non-append-only temporal join
                // The memo-table pk and columns:
                // (`join_key` + `left_pk` + `right_pk`) -> (`right_scan_schema` + `join_key` + `left_pk`)
                //
                // Write pattern:
                //   for each left input row (with insert op), construct the memo table pk and insert the row into the memo table.
                // Read pattern:
                //   for each left input row (with delete op), construct pk prefix (`join_key` + `left_pk`) to fetch rows and delete them from the memo table.
                //
                // Temporal join supports inner join and left outer join, additionally, it could contain other conditions.
                // Surprisingly, we could handle them in a unified way with memo table.
                // The memo table would persist rows fetched from the right table and appending the `join_key` and `left_pk` from the left row.
                // The null rows generated by outer join and the other condition somehow is a stateless operation which means we can handle them without the memo table.
                let memo_table = memo_table.as_mut().unwrap();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if key.null_bitmap().is_subset(null_matched)
                            && let join_entry = right_table.force_peek(&key)
                            && !join_entry.is_empty()
                        {
                            matched = true;
                            for right_row in join_entry.cached.values() {
                                let right_row: OwnedRow = right_row.clone();
                                // Insert into memo table
                                memo_table.insert(right_row.clone().chain(
                                    left_row.project(memo_table_lookup_prefix).into_owned_row(),
                                ));
                                if let Some(chunk) = E::append_matched_row(
                                    Op::Insert,
                                    &mut builder,
                                    left_row,
                                    right_row,
                                ) {
                                    yield chunk;
                                }
                            }
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        let mut memo_rows_to_delete = vec![];
                        if key.null_bitmap().is_subset(null_matched) {
                            let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) =
                                &(Bound::Unbounded, Bound::Unbounded);
                            let prefix = left_row.project(memo_table_lookup_prefix);
                            let state_table_iter = memo_table
                                .iter_with_prefix(prefix, sub_range, Default::default())
                                .await?;
                            pin_mut!(state_table_iter);

                            while let Some(memo_row) = state_table_iter.next().await {
                                matched = true;
                                let memo_row = memo_row?.into_owned_row();
                                memo_rows_to_delete.push(memo_row.clone());
                                if let Some(chunk) = E::append_matched_row(
                                    Op::Delete,
                                    &mut builder,
                                    left_row,
                                    memo_row.slice(0..right_size),
                                ) {
                                    yield chunk;
                                }
                            }
                        }
                        for memo_row in memo_rows_to_delete {
                            // Delete from memo table
                            memo_table.delete(memo_row);
                        }
                    }
                }
            }
            if let Some(chunk) = E::match_end(
                &mut builder,
                match op {
                    Op::Insert | Op::UpdateInsert => Op::Insert,
                    Op::Delete | Op::UpdateDelete => Op::Delete,
                },
                left_row,
                right_size,
                matched,
            ) {
                yield chunk;
            }
        }

        if let Some(chunk) = builder.take() {
            yield chunk;
        }
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive, const APPEND_ONLY: bool>
    TemporalJoinExecutor<K, S, T, APPEND_ONLY>
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        left: Executor,
        right: Executor,
        table: BatchTable<S>,
        left_join_keys: Vec<usize>,
        right_join_keys: Vec<usize>,
        null_safe: Vec<bool>,
        condition: Option<NonStrictExpression>,
        output_indices: Vec<usize>,
        table_output_indices: Vec<usize>,
        table_stream_key_indices: Vec<usize>,
        watermark_sequence: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        join_key_data_types: Vec<DataType>,
        memo_table: Option<StateTable<S>>,
    ) -> Self {
        let alloc = StatsAlloc::new(Global).shared();

        let metrics_info = MetricsInfo::new(
            metrics.clone(),
            table.table_id().table_id,
            ctx.id,
            "temporal join",
        );

        let cache = ManagedLruCache::unbounded_with_hasher_in(
            watermark_sequence,
            metrics_info,
            DefaultHasher::default(),
            alloc,
        );

        let metrics = metrics.new_temporal_join_metrics(table.table_id(), ctx.id, ctx.fragment_id);

        Self {
            ctx: ctx.clone(),
            info,
            left,
            right,
            right_table: TemporalSide {
                source: table,
                table_stream_key_indices,
                table_output_indices,
                cache,
                join_key_data_types,
            },
            left_join_keys,
            right_join_keys,
            null_safe,
            condition,
            output_indices,
            chunk_size,
            memo_table,
            metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let right_size = self.right.schema().len();

        let (left_map, _right_map) = JoinStreamChunkBuilder::get_i2o_mapping(
            &self.output_indices,
            self.left.schema().len(),
            right_size,
        );

        let left_to_output: HashMap<usize, usize> = HashMap::from_iter(left_map.iter().cloned());

        let left_stream_key_indices = self.left.pk_indices().to_vec();
        let right_stream_key_indices = self.right.pk_indices().to_vec();
        let memo_table_lookup_prefix = self
            .left_join_keys
            .iter()
            .cloned()
            .chain(left_stream_key_indices)
            .collect_vec();

        let null_matched = K::Bitmap::from_bool_vec(self.null_safe);

        let mut prev_epoch = None;

        let full_schema: Vec<_> = self
            .left
            .schema()
            .data_types()
            .into_iter()
            .chain(self.right.schema().data_types().into_iter())
            .collect();

        let mut wait_first_barrier = true;

        #[for_await]
        for msg in align_input::<true>(self.left, self.right) {
            self.right_table.cache.evict();
            self.metrics
                .temporal_join_cached_entry_count
                .set(self.right_table.cache.len() as i64);
            match msg? {
                InternalMessage::WaterMark(watermark) => {
                    let output_watermark_col_idx = *left_to_output.get(&watermark.col_idx).unwrap();
                    yield Message::Watermark(watermark.with_idx(output_watermark_col_idx));
                }
                InternalMessage::Chunk(chunk) => {
                    let epoch = prev_epoch.expect("Chunk data should come after some barrier.");

                    let full_schema = full_schema.clone();

                    if T == JoinType::Inner {
                        let st1 = phase1::handle_chunk::<K, S, phase1::Inner, APPEND_ONLY>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            epoch,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &memo_table_lookup_prefix,
                            &mut self.memo_table,
                            &null_matched,
                            chunk,
                            &self.metrics,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = if let Some(ref cond) = self.condition {
                                let (data_chunk, ops) = chunk.into_parts();
                                let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                                let passed_bitmap =
                                    Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                                let (columns, vis) = data_chunk.into_parts();
                                let new_vis = vis & passed_bitmap;
                                StreamChunk::with_visibility(ops, columns, new_vis)
                            } else {
                                chunk
                            };
                            let new_chunk = apply_indices_map(new_chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    } else if let Some(ref cond) = self.condition {
                        // Joined result without evaluating non-lookup conditions.
                        let st1 =
                            phase1::handle_chunk::<K, S, phase1::LeftOuterWithCond, APPEND_ONLY>(
                                self.chunk_size,
                                right_size,
                                full_schema,
                                epoch,
                                &self.left_join_keys,
                                &mut self.right_table,
                                &memo_table_lookup_prefix,
                                &mut self.memo_table,
                                &null_matched,
                                chunk,
                                &self.metrics,
                            );
                        let mut matched_count = 0usize;
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let (data_chunk, ops) = chunk.into_parts();
                            let passed_bitmap = cond.eval_infallible(&data_chunk).await;
                            let passed_bitmap =
                                Arc::unwrap_or_clone(passed_bitmap).into_bool().to_bitmap();
                            let (columns, vis) = data_chunk.into_parts();
                            let mut new_vis = BitmapBuilder::with_capacity(vis.len());
                            for (passed, not_match_end) in
                                passed_bitmap.iter().zip_eq_debug(vis.iter())
                            {
                                let is_match_end = !not_match_end;
                                let vis = if is_match_end && matched_count == 0 {
                                    // Nothing is matched, so the marker row should be visible.
                                    true
                                } else if is_match_end {
                                    // reset the count
                                    matched_count = 0;
                                    // rows found, so the marker row should be invisible.
                                    false
                                } else {
                                    if passed {
                                        matched_count += 1;
                                    }
                                    passed
                                };
                                new_vis.append(vis);
                            }
                            let new_chunk = apply_indices_map(
                                StreamChunk::with_visibility(ops, columns, new_vis.finish()),
                                &self.output_indices,
                            );
                            yield Message::Chunk(new_chunk);
                        }
                        // The last row should always be marker row,
                        assert_eq!(matched_count, 0);
                    } else {
                        let st1 = phase1::handle_chunk::<K, S, phase1::LeftOuter, APPEND_ONLY>(
                            self.chunk_size,
                            right_size,
                            full_schema,
                            epoch,
                            &self.left_join_keys,
                            &mut self.right_table,
                            &memo_table_lookup_prefix,
                            &mut self.memo_table,
                            &null_matched,
                            chunk,
                            &self.metrics,
                        );
                        #[for_await]
                        for chunk in st1 {
                            let chunk = chunk?;
                            let new_chunk = apply_indices_map(chunk, &self.output_indices);
                            yield Message::Chunk(new_chunk);
                        }
                    }
                }
                InternalMessage::Barrier(updates, barrier) => {
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    let barrier_epoch = barrier.epoch;
                    if !APPEND_ONLY {
                        if wait_first_barrier {
                            wait_first_barrier = false;
                            yield Message::Barrier(barrier);
                            self.memo_table
                                .as_mut()
                                .unwrap()
                                .init_epoch(barrier_epoch)
                                .await?;
                        } else {
                            self.memo_table
                                .as_mut()
                                .unwrap()
                                .commit(barrier.epoch)
                                .await?;
                            yield Message::Barrier(barrier);
                        }
                    } else {
                        yield Message::Barrier(barrier);
                    }
                    if let Some(vnodes) = update_vnode_bitmap {
                        let prev_vnodes =
                            self.right_table.source.update_vnode_bitmap(vnodes.clone());
                        if cache_may_stale(&prev_vnodes, &vnodes) {
                            self.right_table.cache.clear();
                        }
                    }
                    self.right_table.update(
                        updates,
                        &self.right_join_keys,
                        &right_stream_key_indices,
                    )?;
                    prev_epoch = Some(barrier_epoch.curr);
                }
            }
        }
    }
}

impl<K: HashKey, S: StateStore, const T: JoinTypePrimitive, const APPEND_ONLY: bool> Execute
    for TemporalJoinExecutor<K, S, T, APPEND_ONLY>
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.into_stream().boxed()
    }
}
