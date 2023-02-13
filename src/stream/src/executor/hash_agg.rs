// Copyright 2023 RisingWave Labs
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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use iter_chunks::IterChunks;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{HashCode, HashKey, PrecomputedBuildHasher};
use risingwave_common::row::{Row, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_storage::StateStore;

use self::imp::EmitPolicy;
use super::aggregation::{agg_call_filter_res, iter_table_storage, AggStateStorage};
use super::{
    expect_first_barrier, ActorContextRef, Executor, PkIndicesRef, StreamExecutorResult, Watermark,
};
use crate::cache::{cache_may_stale, new_with_hasher, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::aggregation::{generate_agg_schema, AggCall, AggChangesInfo, AggGroup};
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{BoxedMessageStream, Message, PkIndices};
use crate::task::AtomicU64Ref;

type BoxedAggGroup<S> = Box<AggGroup<S>>;
type AggGroupCache<K, S> = ExecutorCache<K, BoxedAggGroup<S>, PrecomputedBuildHasher>;

mod imp {
    use risingwave_common::row::Row;

    use crate::executor::Watermark;

    pub trait EmitPolicy: Send + Sync + 'static {
        fn should_emit(key: impl Row, buffered_watermarks: &[Option<Watermark>]) -> bool;
    }
}

pub struct EmitImmediate;

impl imp::EmitPolicy for EmitImmediate {
    #[inline(always)]
    fn should_emit(_key: impl Row, _buffered_watermarks: &[Option<Watermark>]) -> bool {
        true
    }
}

pub struct EmitOnWatermarkClose;

impl imp::EmitPolicy for EmitOnWatermarkClose {
    #[inline(always)]
    fn should_emit(key: impl Row, buffered_watermarks: &[Option<Watermark>]) -> bool {
        let Some(Some(cur_watermark)) = buffered_watermarks.get(0) else {
            // There is no watermark value now, so we can't emit any value.
            return false;
        };
        let Some(ref watermark_val) = key.datum_at(0) else {
            // NULL is unexpected in watermark column, however, if it exists, we'll treat it as the largest, so emit it here.
            return true;
        };
        watermark_val <= &cur_watermark.val.as_scalar_ref_impl()
    }
}

/// [`HashAggExecutor`] could process large amounts of data using a state backend. It works as
/// follows:
///
/// * The executor pulls data from the upstream, and apply the data chunks to the corresponding
///   aggregation states.
/// * While processing, it will record which keys have been modified in this epoch using
///   `modified_keys`.
/// * Upon a barrier is received, the executor will call `.flush` on the storage backend, so that
///   all modifications will be flushed to the storage backend. Meanwhile, the executor will go
///   through `modified_keys`, and produce a stream chunk based on the state changes.
pub struct HashAggExecutor<K: HashKey, S: StateStore, E: imp::EmitPolicy = EmitImmediate> {
    input: Box<dyn Executor>,

    extra: HashAggExecutorExtra<K, S>,

    _phantom: PhantomData<E>,
}

struct HashAggExecutorExtra<K: HashKey, S: StateStore> {
    ctx: ActorContextRef,

    /// See [`Executor::schema`].
    schema: Schema,

    /// See [`Executor::pk_indices`].
    pk_indices: PkIndices,

    /// See [`Executor::identity`].
    identity: String,

    /// Pk indices from input
    input_pk_indices: Vec<usize>,

    /// Schema from input
    input_schema: Schema,

    /// A [`HashAggExecutor`] may have multiple [`AggCall`]s.
    agg_calls: Vec<AggCall>,

    /// State storages for each aggregation calls.
    /// `None` means the agg call need not to maintain a state table by itself.
    storages: Vec<AggStateStorage<S>>,

    /// State table for the previous result of all agg calls.
    /// The outputs of all managed agg states are collected and stored in this
    /// table when `flush_data` is called.
    result_table: StateTable<S>,

    /// Indices of the columns
    /// all of the aggregation functions in this executor should depend on same group of keys
    group_key_indices: Vec<usize>,

    /// The evict epoch for `GlobalMemoryManager`. None if using local eviction.
    cache_evict_watermark_epoch: AtomicU64Ref,

    /// How many times have we hit the cache of join executor for the lookup of each key
    lookup_miss_count: AtomicU64,

    total_lookup_count: AtomicU64,

    /// How many times have we hit the cache of join executor for all the lookups generated by one
    /// StreamChunk
    chunk_lookup_miss_count: u64,

    chunk_total_lookup_count: u64,

    metrics: Arc<StreamingMetrics>,

    /// Extreme state cache size
    extreme_cache_size: usize,

    /// Held keys blocked by watermark, which should be emitted later.
    /// The value is a boolean indicates whether the group is changed during the epoch.
    held_keys: HashMap<K, bool>,

    /// The maximum size of the chunk produced by executor at a time.
    chunk_size: usize,

    /// Map group key column idx to its position in group keys.
    group_key_invert_idx: Vec<Option<usize>>,

    /// Buffer watermarks on group keys received since last barrier.
    buffered_watermarks: Vec<Option<Watermark>>,
}

impl<K: HashKey, S: StateStore, E: EmitPolicy> Executor for HashAggExecutor<K, S, E> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.extra.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.extra.pk_indices
    }

    fn identity(&self) -> &str {
        &self.extra.identity
    }
}

impl<K: HashKey, S: StateStore, E: imp::EmitPolicy> HashAggExecutor<K, S, E> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        storages: Vec<AggStateStorage<S>>,
        result_table: StateTable<S>,
        pk_indices: PkIndices,
        extreme_cache_size: usize,
        executor_id: u64,
        group_key_indices: Vec<usize>,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> StreamResult<Self> {
        let input_info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, Some(&group_key_indices));

        let mut group_key_invert_idx = vec![None; input.info().schema.len()];
        for (group_key_seq, group_key_idx) in group_key_indices.iter().enumerate() {
            group_key_invert_idx[*group_key_idx] = Some(group_key_seq);
        }

        Ok(Self {
            input,
            extra: HashAggExecutorExtra {
                ctx,
                schema,
                pk_indices,
                identity: format!("HashAggExecutor {:X}", executor_id),
                input_pk_indices: input_info.pk_indices,
                input_schema: input_info.schema,
                agg_calls,
                extreme_cache_size,
                storages,
                result_table,
                group_key_indices,
                cache_evict_watermark_epoch: watermark_epoch,
                held_keys: HashMap::new(),
                lookup_miss_count: AtomicU64::new(0),
                total_lookup_count: AtomicU64::new(0),
                chunk_lookup_miss_count: 0,
                chunk_total_lookup_count: 0,
                metrics,
                chunk_size,
                group_key_invert_idx,
                buffered_watermarks: Vec::default(),
            },
            _phantom: PhantomData,
        })
    }

    /// Get unique keys, hash codes and visibility map of each key in a batch.
    ///
    /// The returned order is the same as how we get distinct final columns from original columns.
    ///
    /// `keys` are Hash Keys of all the rows
    /// `key_hash_codes` are hash codes of the deserialized `keys`
    /// `visibility`, leave invisible ones out of aggregation
    fn get_unique_keys(
        keys: Vec<K>,
        key_hash_codes: Vec<HashCode>,
        visibility: Option<&Bitmap>,
    ) -> StreamExecutorResult<Vec<(K, HashCode, Bitmap)>> {
        let total_num_rows = keys.len();
        assert_eq!(key_hash_codes.len(), total_num_rows);
        // Each hash key, e.g. `key1` corresponds to a visibility map that not only shadows
        // all the rows whose keys are not `key1`, but also shadows those rows shadowed in the
        // `input` The visibility map of each hash key will be passed into `ManagedStateImpl`.
        let mut key_to_vis_maps = HashMap::new();

        // Give all the unique keys an order and iterate them later,
        // the order is the same as how we get distinct final columns from original columns.
        let mut unique_key_and_hash_codes = Vec::new();

        for (row_idx, (key, hash_code)) in
            keys.iter().zip_eq_fast(key_hash_codes.iter()).enumerate()
        {
            // if the visibility map has already shadowed this row,
            // then we pass
            if let Some(vis_map) = visibility && !vis_map.is_set(row_idx) {
                continue;
            }
            let vis_map = key_to_vis_maps.entry(key).or_insert_with(|| {
                unique_key_and_hash_codes.push((key, hash_code));
                vec![false; total_num_rows]
            });
            vis_map[row_idx] = true;
        }

        let result = unique_key_and_hash_codes
            .into_iter()
            .map(|(key, hash_code)| {
                (
                    key.clone(),
                    *hash_code,
                    key_to_vis_maps.remove(key).unwrap().into_iter().collect(),
                )
            })
            .collect_vec();

        Ok(result)
    }

    async fn apply_chunk(
        HashAggExecutorExtra::<K, S> {
            ref ctx,
            ref identity,
            ref group_key_indices,
            ref agg_calls,
            ref mut storages,
            ref result_table,
            ref input_schema,
            ref input_pk_indices,
            ref extreme_cache_size,
            ref mut held_keys,
            ref schema,
            lookup_miss_count,
            total_lookup_count,
            ref mut chunk_lookup_miss_count,
            ref mut chunk_total_lookup_count,
            ..
        }: &mut HashAggExecutorExtra<K, S>,
        agg_group_cache: &mut AggGroupCache<K, S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        // Compute hash code here before serializing keys to avoid duplicate hash code computation.
        let hash_codes = chunk
            .data_chunk()
            .get_hash_values(group_key_indices, Crc32FastBuilder);
        let keys =
            K::build_from_hash_code(group_key_indices, chunk.data_chunk(), hash_codes.clone());

        // Find unique keys in this batch and generate visibility map for each key
        // TODO: this might be inefficient if there are not too many duplicated keys in one batch.
        let unique_keys = Self::get_unique_keys(keys, hash_codes, chunk.visibility())?;

        let group_key_types = &schema.data_types()[..group_key_indices.len()];

        let futs = unique_keys
            .iter()
            .filter_map(|(key, _, _)| {
                total_lookup_count.fetch_add(1, Ordering::Relaxed);
                if agg_group_cache.contains(key) {
                    None
                } else {
                    lookup_miss_count.fetch_add(1, Ordering::Relaxed);
                    Some(async {
                        // Create `AggGroup` for the current group if not exists. This will fetch
                        // previous agg result from the result table.
                        let agg_group = Box::new(
                            AggGroup::create(
                                Some(key.deserialize(group_key_types)?),
                                agg_calls,
                                storages,
                                result_table,
                                input_pk_indices,
                                *extreme_cache_size,
                                input_schema,
                            )
                            .await?,
                        );
                        Ok::<_, StreamExecutorError>((key.clone(), agg_group))
                    })
                }
            })
            .collect_vec(); // collect is necessary to avoid lifetime issue of `agg_group_cache`

        // If not all the required states/keys are in the cache, this is a chunk-level cache miss.
        if !futs.is_empty() {
            *chunk_lookup_miss_count += 1;
        }
        *chunk_total_lookup_count += 1;
        {
            let mut buffered = stream::iter(futs).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, agg_group) = result?;
                agg_group_cache.put(key, agg_group);
            }
        }

        // Decompose the input chunk.
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();

        // Calculate the row visibility for every agg call.
        let visibilities: Vec<Option<Bitmap>> = agg_calls
            .iter()
            .map(|agg_call| {
                agg_call_filter_res(
                    ctx,
                    identity,
                    agg_call,
                    &columns,
                    visibility.as_ref(),
                    capacity,
                )
            })
            .try_collect()?;

        // Materialize input chunk if needed.
        storages
            .iter_mut()
            .zip_eq_fast(visibilities.iter().map(Option::as_ref))
            .for_each(|(storage, visibility)| {
                if let AggStateStorage::MaterializedInput { table, mapping } = storage {
                    let needed_columns = mapping
                        .upstream_columns()
                        .iter()
                        .map(|col_idx| columns[*col_idx].clone())
                        .collect();
                    table.write_chunk(StreamChunk::new(
                        ops.clone(),
                        needed_columns,
                        visibility.cloned(),
                    ));
                }
            });

        // Apply chunk to each of the state (per agg_call), for each group.
        for (key, _, vis_map) in &unique_keys {
            // Mark the group as held and dirty.
            held_keys.insert(key.clone(), true);
            let agg_group = agg_group_cache.get_mut(key).unwrap().as_mut();
            let visibilities = visibilities
                .iter()
                .map(Option::as_ref)
                .map(|v| v.map_or_else(|| vis_map.clone(), |v| v & vis_map))
                .map(Some)
                .collect();
            agg_group.apply_chunk(storages, &ops, &columns, visibilities)?;
        }

        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn flush_data<'a>(
        &mut HashAggExecutorExtra::<K, S> {
            ref ctx,
            ref group_key_indices,
            ref schema,
            ref mut storages,
            ref mut result_table,
            ref mut held_keys,
            ref lookup_miss_count,
            ref total_lookup_count,
            ref mut chunk_lookup_miss_count,
            ref mut chunk_total_lookup_count,
            ref metrics,
            ref chunk_size,
            ref buffered_watermarks,
            ..
        }: &'a mut HashAggExecutorExtra<K, S>,
        agg_group_cache: &'a mut AggGroupCache<K, S>,
        epoch: EpochPair,
    ) {
        let state_clean_watermark = buffered_watermarks
            .first()
            .and_then(|opt_watermark| opt_watermark.as_ref())
            .map(|watermark| watermark.val.clone());

        let actor_id_str = ctx.id.to_string();
        metrics
            .agg_lookup_miss_count
            .with_label_values(&[&actor_id_str])
            .inc_by(lookup_miss_count.swap(0, Ordering::Relaxed));
        metrics
            .agg_total_lookup_count
            .with_label_values(&[&actor_id_str])
            .inc_by(total_lookup_count.swap(0, Ordering::Relaxed));
        metrics
            .agg_cached_keys
            .with_label_values(&[&actor_id_str])
            .set(agg_group_cache.len() as i64);
        metrics
            .agg_chunk_lookup_miss_count
            .with_label_values(&[&actor_id_str])
            .inc_by(*chunk_lookup_miss_count);
        *chunk_lookup_miss_count = 0;
        metrics
            .agg_chunk_total_lookup_count
            .with_label_values(&[&actor_id_str])
            .inc_by(*chunk_total_lookup_count);
        *chunk_total_lookup_count = 0;

        let dirty_cnt = held_keys.len();
        if dirty_cnt > 0 {
            // Produce the stream chunk
            let group_key_data_types = &schema.data_types()[..group_key_indices.len()];
            let mut to_flush_keys = Vec::with_capacity(held_keys.len());
            let mut to_emit_chunks = IterChunks::chunks(
                held_keys
                    .drain_filter(|k, dirty| {
                        if *dirty {
                            to_flush_keys.push(k.clone());
                        }
                        *dirty = false;

                        let key = K::deserialize(k, group_key_data_types).unwrap();
                        E::should_emit(key, buffered_watermarks)
                    })
                    .map(|(k, _)| k),
                *chunk_size,
            );
            while let Some(batch) = to_emit_chunks.next() {
                let keys_in_batch = batch.into_iter().collect_vec();

                // Create array builders.
                // As the datatype is retrieved from schema, it contains both group key and
                // aggregation state outputs.
                let mut builders = schema.create_array_builders(chunk_size * 2);
                let mut new_ops = Vec::with_capacity(chunk_size * 2);

                let agg_groups = {
                    let mut agg_group_cache_ptr: NonNull<_> = agg_group_cache.into();
                    keys_in_batch
                        .iter()
                        .map(|key| {
                            // SAFETY: `keys_in_batch` is a subset of `held_keys`, which is a
                            // `HashMap`, so we can ensure that every `&mut agg_group_cache` is
                            // unique.
                            unsafe {
                                let agg_group_cache = agg_group_cache_ptr.as_mut();
                                agg_group_cache
                                    .get_mut(key)
                                    .expect("changed group must have corresponding AggGroup")
                            }
                        })
                        .collect_vec()
                };

                // Calculate current outputs, concurrently.
                // FIXME: In fact, we don't need to collect `futs` as `Vec`, but rustc will report
                // a weird error `error: higher-ranked lifetime error`.
                #[expect(clippy::disallowed_methods)]
                let futs: Vec<_> = keys_in_batch
                    .iter()
                    .zip(agg_groups.into_iter())
                    .map(|(key, agg_group)| {
                        // Pop out the agg group temporarily.
                        let storages = &storages;
                        async move {
                            let curr_outputs = agg_group.get_outputs(storages).await?;
                            Ok::<_, StreamExecutorError>((key, agg_group, curr_outputs))
                        }
                    })
                    .collect();
                let outputs_in_batch: Vec<_> = stream::iter(futs)
                    .buffer_unordered(10)
                    .fuse()
                    .try_collect()
                    .await?;

                for (key, agg_group, curr_outputs) in outputs_in_batch {
                    let agg_change_info = agg_group.build_changes(
                        curr_outputs,
                        &mut builders[group_key_indices.len()..],
                        &mut new_ops,
                    );
                    let AggChangesInfo {
                        n_appended_ops,
                        prev_outputs,
                        result_row,
                    } = agg_change_info;
                    if n_appended_ops != 0 {
                        for _ in 0..n_appended_ops {
                            key.deserialize_to_builders(
                                &mut builders[..group_key_indices.len()],
                                group_key_data_types,
                            )?;
                        }
                        if let Some(prev_outputs) = prev_outputs {
                            // FIXME: double deserialization here
                            let group_key = K::deserialize(key, group_key_data_types).unwrap();
                            let old_row = group_key.chain(prev_outputs);
                            result_table.update(old_row, result_row);
                        } else {
                            result_table.insert(result_row);
                        }
                    }
                }

                let columns = builders
                    .into_iter()
                    .map(|builder| builder.finish().into())
                    .collect();

                let chunk = StreamChunk::new(new_ops, columns, None);

                trace!("output_chunk: {:?}", &chunk);
                yield chunk;
            }

            drop(to_emit_chunks);

            for key in to_flush_keys {
                // Flush agg states.
                let agg_group = agg_group_cache
                    .get_mut(&key)
                    .expect("changed group must have corresponding AggGroup")
                    .as_mut();
                agg_group.flush_state_if_needed(storages).await?;
            }

            // Commit all state tables.
            futures::future::try_join_all(iter_table_storage(storages).map(|state_table| async {
                if let Some(watermark) = state_clean_watermark.as_ref() {
                    state_table.update_watermark(watermark.clone())
                };
                state_table.commit(epoch).await
            }))
            .await?;
            if let Some(watermark) = state_clean_watermark.as_ref() {
                result_table.update_watermark(watermark.clone());
            };
            result_table.commit(epoch).await?;

            // Evict cache to target capacity.
            agg_group_cache.evict();
        } else {
            // Nothing to flush.
            // Call commit on state table to increment the epoch.
            iter_table_storage(storages).for_each(|state_table| {
                if let Some(watermark) = state_clean_watermark.as_ref() {
                    state_table.update_watermark(watermark.clone())
                };
                state_table.commit_no_data_expected(epoch);
            });
            if let Some(watermark) = state_clean_watermark.as_ref() {
                result_table.update_watermark(watermark.clone());
            };
            result_table.commit_no_data_expected(epoch);
            return Ok(());
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let HashAggExecutor {
            input, mut extra, ..
        } = self;

        // The cached state managers. `HashKey` -> `AggGroup`.
        let mut agg_group_cache = AggGroupCache::new(new_with_hasher(
            extra.cache_evict_watermark_epoch.clone(),
            PrecomputedBuildHasher,
        ));

        // First barrier
        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        iter_table_storage(&mut extra.storages).for_each(|state_table| {
            state_table.init_epoch(barrier.epoch);
        });
        extra.result_table.init_epoch(barrier.epoch);
        agg_group_cache.update_epoch(barrier.epoch.curr);

        yield Message::Barrier(barrier);

        extra.buffered_watermarks = vec![None; extra.group_key_indices.len()];

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(mut watermark) => {
                    let group_key_seq = extra.group_key_invert_idx[watermark.col_idx];
                    if let Some(group_key_seq) = group_key_seq {
                        watermark.col_idx = group_key_seq;
                        extra.buffered_watermarks[group_key_seq] = Some(watermark);
                    }
                }

                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut extra, &mut agg_group_cache, chunk).await?;
                }
                Message::Barrier(barrier) => {
                    #[for_await]
                    for chunk in Self::flush_data(&mut extra, &mut agg_group_cache, barrier.epoch) {
                        yield Message::Chunk(chunk?);
                    }

                    for buffered_watermark in &mut extra.buffered_watermarks {
                        if let Some(watermark) = buffered_watermark.take() {
                            yield Message::Watermark(watermark);
                        }
                    }

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(extra.ctx.id) {
                        iter_table_storage(&mut extra.storages).for_each(|state_table| {
                            let _ = state_table.update_vnode_bitmap(vnode_bitmap.clone());
                        });
                        let previous_vnode_bitmap =
                            extra.result_table.update_vnode_bitmap(vnode_bitmap.clone());

                        // Manipulate the cache if necessary.
                        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
                            agg_group_cache.clear();
                        }
                    }

                    // Update the current epoch.
                    agg_group_cache.update_epoch(barrier.epoch.curr);

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::row::{AscentOwnedRow, OwnedRow, Row};
    use risingwave_common::types::DataType;
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_expr::expr::*;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use super::{EmitImmediate, EmitOnWatermarkClose};
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::monitor::StreamingMetrics;
    use crate::executor::test_utils::agg_executor::{create_agg_state_table, create_result_table};
    use crate::executor::test_utils::*;
    use crate::executor::{ActorContext, Executor, HashAggExecutor, Message, PkIndices};

    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    async fn new_boxed_hash_agg_executor<S: StateStore>(
        store: S,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        group_key_indices: Vec<usize>,
        pk_indices: PkIndices,
        extreme_cache_size: usize,
        executor_id: u64,
        // TODO: should we use an enum here?
        emit_immediate: bool,
    ) -> Box<dyn Executor> {
        let mut agg_state_tables = Vec::with_capacity(agg_calls.iter().len());
        for (idx, agg_call) in agg_calls.iter().enumerate() {
            agg_state_tables.push(
                create_agg_state_table(
                    store.clone(),
                    TableId::new(idx as u32),
                    agg_call,
                    &group_key_indices,
                    &pk_indices,
                    input.as_ref(),
                )
                .await,
            )
        }

        let result_table = create_result_table(
            store,
            TableId::new(agg_calls.len() as u32),
            &agg_calls,
            &group_key_indices,
            input.as_ref(),
        )
        .await;

        if emit_immediate {
            HashAggExecutor::<SerializedKey, S, EmitImmediate>::new(
                ActorContext::create(123),
                input,
                agg_calls,
                agg_state_tables,
                result_table,
                pk_indices,
                extreme_cache_size,
                executor_id,
                group_key_indices,
                Arc::new(AtomicU64::new(0)),
                Arc::new(StreamingMetrics::unused()),
                1024,
            )
            .unwrap()
            .boxed()
        } else {
            HashAggExecutor::<SerializedKey, S, EmitOnWatermarkClose>::new(
                ActorContext::create(123),
                input,
                agg_calls,
                agg_state_tables,
                result_table,
                pk_indices,
                extreme_cache_size,
                executor_id,
                group_key_indices,
                Arc::new(AtomicU64::new(0)),
                Arc::new(StreamingMetrics::unused()),
                1024,
            )
            .unwrap()
            .boxed()
        }
    }

    // --- Test HashAgg with in-memory KeyedState ---

    #[tokio::test]
    async fn test_local_hash_aggregation_count_in_memory() {
        test_local_hash_aggregation_count(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_global_hash_aggregation_count_in_memory() {
        test_global_hash_aggregation_count(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_in_memory() {
        test_local_hash_aggregation_min(MemoryStateStore::new()).await
    }

    #[tokio::test]
    async fn test_local_hash_aggregation_min_append_only_in_memory() {
        test_local_hash_aggregation_min_append_only(MemoryStateStore::new()).await
    }

    async fn test_local_hash_aggregation_count<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            + 1
            + 2
            + 2",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I
            - 1
            - 2 D
            - 2",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let append_only = false;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            keys,
            vec![],
            1 << 10,
            1,
            true,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 2 2"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 2 2
                U+ 2 1 1 1"
            )
            .sorted_rows(),
        );
    }

    async fn test_global_hash_aggregation_count<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };

        let (mut tx, source) = MockSource::channel(schema, PkIndices::new());
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I I
            + 1 1 1
            + 2 2 2
            + 2 2 2",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I I
            - 1 1 1
            - 2 2 2 D
            - 2 2 2
            + 3 3 3",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another sum state
        let key_indices = vec![0];
        let append_only = false;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            // This is local hash aggregation, so we add another sum state
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 2),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            key_indices,
            vec![],
            1 << 10,
            1,
            true,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I I I
                + 1 1 1 1
                + 2 2 4 4"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I I I
                -  1 1 1 1
                U- 2 2 4 4
                U+ 2 1 2 2
                +  3 1 3 3"
            )
            .sorted_rows(),
        );
    }

    async fn test_local_hash_aggregation_min<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                // group key column
                Field::unnamed(DataType::Int64),
                // data column to get minimum
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I     I    I
            + 1   233 1001
            + 1 23333 1002
            + 2  2333 1003",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I     I    I
            - 1   233 1001
            - 1 23333 1002 D
            - 2  2333 1003",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            keys,
            vec![2],
            1 << 10,
            1,
            true,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2  233
                + 2 1 2333"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I     I
                -  2 1  2333
                U- 1 2   233
                U+ 1 1 23333"
            )
            .sorted_rows(),
        );
    }

    async fn test_local_hash_aggregation_min_append_only<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                // group key column
                Field::unnamed(DataType::Int64),
                // data column to get minimum
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I  I  I
            + 2 5  1000
            + 1 15 1001
            + 1 8  1002
            + 2 5  1003
            + 2 10 1004
            ",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I  I  I
            + 1 20 1005
            + 1 1  1006
            + 2 10 1007
            + 2 20 1008
            ",
        ));
        tx.push_barrier(3, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let append_only = true;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            keys,
            vec![2],
            1 << 10,
            1,
            true,
        )
        .await;
        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                " I I    I
                + 1 2 8
                + 2 3 5"
            )
            .sorted_rows(),
        );

        assert_matches!(
            hash_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = hash_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap().sorted_rows(),
            StreamChunk::from_pretty(
                "  I I  I
                U- 1 2 8
                U+ 1 4 1
                U- 2 3 5
                U+ 2 5 5
                "
            )
            .sorted_rows(),
        );
    }

    #[tokio::test]
    async fn test_window_agg_in_memory() {
        test_window_agg(MemoryStateStore::new()).await;
    }

    async fn test_window_agg<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                // group key column
                Field::unnamed(DataType::Int64),
                // data column to get minimum
                Field::unnamed(DataType::Int64),
                // primary key column
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk

        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I I
            + 1 2 1001
            + 1 4 1002
            + 2 6 1003",
        ));
        tx.push_watermark(0, DataType::Int64, 1i64.into());
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            " I I  I
            + 4 16  1004
            + 3 14 1005
            + 2 12 1006",
        ));
        tx.push_watermark(0, DataType::Int64, 2i64.into());
        tx.push_chunk(StreamChunk::from_pretty(
            " I I  I
            + 3 13 1007
            + 5 11 1008
            + 3 15 1009",
        ));
        tx.push_barrier(3, false);
        tx.push_watermark(0, DataType::Int64, 4i64.into());
        tx.push_barrier(4, false);

        // This is local hash aggregation, so we add another row count state
        let keys = vec![0];
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
        ];

        let return_tys = vec![DataType::Int64, DataType::Int64, DataType::Int64];
        let row_pretty = |s: &str| OwnedRow::from_pretty_with_tys(&return_tys, s);

        let hash_agg = new_boxed_hash_agg_executor(
            store,
            Box::new(source),
            agg_calls,
            keys,
            vec![2],
            1 << 10,
            1,
            false,
        )
        .await;

        let mut hash_agg = hash_agg.execute();

        // Consume the init barrier
        hash_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Chunk(chunk) = msg else { unreachable!() };
        let [(op, row)]: [_; 1] = chunk.rows().collect_vec().try_into().unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row.to_owned_row(), row_pretty("1 2 6"));

        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Watermark(watermark) = msg else { unreachable!() };
        assert_eq!(watermark.col_idx, 0);
        assert_eq!(watermark.val, 1i64.into());

        // Consume a barrier
        hash_agg.next().await.unwrap().unwrap();

        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Chunk(chunk) = msg else { unreachable!() };
        let [(op, row)]: [_; 1] = chunk.rows().collect_vec().try_into().unwrap();
        assert_eq!(op, Op::Insert);
        assert_eq!(row.to_owned_row(), row_pretty("2 2 18"));

        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Watermark(watermark) = msg else { unreachable!() };
        assert_eq!(watermark.col_idx, 0);
        assert_eq!(watermark.val, 2i64.into());

        // Consume a barrier
        hash_agg.next().await.unwrap().unwrap();

        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Chunk(chunk) = msg else { unreachable!() };
        let [(op1, row1), (op2, row2)]: [_; 2] = chunk.sorted_rows().try_into().unwrap();
        assert_eq!(op1, Op::Insert);
        assert_eq!(row1, row_pretty("3 3 42"));
        assert_eq!(op2, Op::Insert);
        assert_eq!(row2, row_pretty("4 1 16"));

        let msg = hash_agg.next().await.unwrap().unwrap();
        let Message::Watermark(watermark) = msg else { unreachable!() };
        assert_eq!(watermark.col_idx, 0);
        assert_eq!(watermark.val, 4i64.into());

        // Consume a barrier
        hash_agg.next().await.unwrap().unwrap();
    }

    trait SortedRows {
        fn sorted_rows(self) -> Vec<(Op, OwnedRow)>;
    }
    impl SortedRows for StreamChunk {
        fn sorted_rows(self) -> Vec<(Op, OwnedRow)> {
            let (chunk, ops) = self.into_parts();
            ops.into_iter()
                .zip_eq_debug(
                    chunk
                        .rows()
                        .map(Row::into_owned_row)
                        .map(AscentOwnedRow::from),
                )
                .sorted()
                .map(|(op, row)| (op, row.into_inner()))
                .collect_vec()
        }
    }
}
