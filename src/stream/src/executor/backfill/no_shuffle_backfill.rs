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

use either::Either;
use futures::stream;
use futures::stream::select_with_strategy;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::{bail, row};
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::BatchTable;

use crate::executor::backfill::utils;
use crate::executor::backfill::utils::{
    compute_bounds, construct_initial_finished_state, create_builder, get_new_pos, mapping_chunk,
    mapping_message, mark_chunk, METADATA_STATE_LEN,
};
use crate::executor::prelude::*;
use crate::task::CreateMviewProgressReporter;

/// Schema: | vnode | pk ... | `backfill_finished` | `row_count` |
/// We can decode that into `BackfillState` on recovery.
#[derive(Debug, Eq, PartialEq)]
pub struct BackfillState {
    current_pos: Option<OwnedRow>,
    old_state: Option<Vec<Datum>>,
    is_finished: bool,
    row_count: u64,
}

/// An implementation of the [RFC: Use Backfill To Let Mv On Mv Stream Again](https://github.com/risingwavelabs/rfcs/pull/13).
/// `BackfillExecutor` is used to create a materialized view on another materialized view.
///
/// It can only buffer chunks between two barriers instead of unbundled memory usage of
/// `RearrangedChainExecutor`.
///
/// It uses the latest epoch to read the snapshot of the upstream mv during two barriers and all the
/// `StreamChunk` of the snapshot read will forward to the downstream.
///
/// It uses `current_pos` to record the progress of the backfill (the pk of the upstream mv) and
/// `current_pos` is initiated as an empty `Row`.
///
/// All upstream messages during the two barriers interval will be buffered and decide to forward or
/// ignore based on the `current_pos` at the end of the later barrier. Once `current_pos` reaches
/// the end of the upstream mv pk, the backfill would finish.
///
/// Notice:
/// The pk we are talking about here refers to the storage primary key.
/// We rely on the scheduler to schedule the `BackfillExecutor` together with the upstream mv/table
/// in the same worker, so that we can read uncommitted data from the upstream table without
/// waiting.
pub struct BackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: BatchTable<S>,
    /// Upstream with the same schema with the upstream table.
    upstream: Executor,

    /// Internal state table for persisting state of backfill state.
    state_table: Option<StateTable<S>>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// PTAL at the docstring for `CreateMviewProgress` to understand how we compute it.
    progress: CreateMviewProgressReporter,

    actor_id: ActorId,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,

    rate_limiter: MonitoredRateLimiter,
}

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream_table: BatchTable<S>,
        upstream: Executor,
        state_table: Option<StateTable<S>>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgressReporter,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        rate_limit: RateLimit,
    ) -> Self {
        let actor_id = progress.actor_id();
        let rate_limiter = RateLimiter::new(rate_limit).monitored(upstream_table.table_id());
        Self {
            upstream_table,
            upstream,
            state_table,
            output_indices,
            progress,
            actor_id,
            metrics,
            chunk_size,
            rate_limiter,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns.
        // We receive a pruned chunk from the upstream table,
        // which will only contain output columns of the scan on the upstream table.
        // The pk indices specify the pk columns of the pruned chunk.
        let pk_indices = self.upstream_table.pk_in_output_indices().unwrap();

        let state_len = pk_indices.len() + METADATA_STATE_LEN;

        let pk_order = self.upstream_table.pk_serializer().get_order_types();

        let upstream_table_id = self.upstream_table.table_id().table_id;

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let mut paused = first_barrier.is_pause_on_startup();
        let first_epoch = first_barrier.epoch;
        let init_epoch = first_barrier.epoch.prev;
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        if let Some(state_table) = self.state_table.as_mut() {
            state_table.init_epoch(first_epoch).await?;
        }

        let BackfillState {
            mut current_pos,
            is_finished,
            row_count,
            mut old_state,
        } = Self::recover_backfill_state(self.state_table.as_ref(), pk_indices.len()).await?;
        tracing::trace!(is_finished, row_count, "backfill state recovered");

        let data_types = self.upstream_table.schema().data_types();

        // Chunk builder will be instantiated with min(rate_limit, self.chunk_size) as the chunk's max size.
        let mut builder = create_builder(
            self.rate_limiter.rate_limit(),
            self.chunk_size,
            data_types.clone(),
        );

        // Use this buffer to construct state,
        // which will then be persisted.
        let mut current_state: Vec<Datum> = vec![None; state_len];

        // If no need backfill, but state was still "unfinished" we need to finish it.
        // So we just update the state + progress to meta at the next barrier to finish progress,
        // and forward other messages.
        //
        // Reason for persisting on second barrier rather than first:
        // We can't update meta with progress as finished until state_table
        // has been updated.
        // We also can't update state_table in first epoch, since state_table
        // expects to have been initialized in previous epoch.

        // The epoch used to snapshot read upstream mv.
        let mut snapshot_read_epoch = init_epoch;

        // Keep track of rows from the snapshot.
        let mut total_snapshot_processed_rows: u64 = row_count;

        // Backfill Algorithm:
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot
        //
        // We construct a backfill stream with upstream as its left input and mv snapshot read
        // stream as its right input. When a chunk comes from upstream, we will buffer it.
        //
        // When a barrier comes from upstream:
        //  - Update the `snapshot_read_epoch`.
        //  - For each row of the upstream chunk buffer, forward it to downstream if its pk <=
        //    `current_pos`, otherwise ignore it.
        //  - reconstruct the whole backfill stream with upstream and new mv snapshot read stream
        //    with the `snapshot_read_epoch`.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if !is_finished {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;

            let metrics = self
                .metrics
                .new_backfill_metrics(upstream_table_id, self.actor_id);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let mut snapshot_read_complete = false;
                let mut has_snapshot_read = false;

                // We should not buffer rows from previous epoch, else we can have duplicates.
                assert!(upstream_chunk_buffer.is_empty());

                {
                    let left_upstream = upstream.by_ref().map(Either::Left);
                    let paused =
                        paused || matches!(self.rate_limiter.rate_limit(), RateLimit::Pause);
                    let right_snapshot = pin!(Self::make_snapshot_stream(
                        &self.upstream_table,
                        snapshot_read_epoch,
                        current_pos.clone(),
                        paused,
                        &self.rate_limiter,
                    )
                    .map(Either::Right));

                    // Prefer to select upstream, so we can stop snapshot stream as soon as the
                    // barrier comes.
                    let mut backfill_stream =
                        select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                            stream::PollNext::Left
                        });

                    #[for_await]
                    for either in &mut backfill_stream {
                        match either {
                            // Upstream
                            Either::Left(msg) => {
                                match msg? {
                                    Message::Barrier(barrier) => {
                                        // We have to process barrier outside of the loop.
                                        // This is because the backfill stream holds a mutable
                                        // reference to our chunk builder.
                                        // We want to create another mutable reference
                                        // to flush remaining chunks from the chunk builder
                                        // on barrier.
                                        // Hence we break here and process it after this block.
                                        pending_barrier = Some(barrier);
                                        break;
                                    }
                                    Message::Chunk(chunk) => {
                                        // Buffer the upstream chunk.
                                        upstream_chunk_buffer.push(chunk.compact());
                                    }
                                    Message::Watermark(_) => {
                                        // Ignore watermark during backfill.
                                    }
                                }
                            }
                            // Snapshot read
                            Either::Right(msg) => {
                                has_snapshot_read = true;
                                match msg? {
                                    None => {
                                        // Consume remaining rows in the builder.
                                        if let Some(data_chunk) = builder.consume_all() {
                                            yield Message::Chunk(Self::handle_snapshot_chunk(
                                                data_chunk,
                                                &mut current_pos,
                                                &mut cur_barrier_snapshot_processed_rows,
                                                &mut total_snapshot_processed_rows,
                                                &pk_indices,
                                                &self.output_indices,
                                            ));
                                        }

                                        // End of the snapshot read stream.
                                        // We should not mark the chunk anymore,
                                        // otherwise, we will ignore some rows
                                        // in the buffer. Here we choose to never mark the chunk.
                                        // Consume with the renaming stream buffer chunk without
                                        // mark.
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            let chunk_cardinality = chunk.cardinality() as u64;
                                            cur_barrier_upstream_processed_rows +=
                                                chunk_cardinality;
                                            yield Message::Chunk(mapping_chunk(
                                                chunk,
                                                &self.output_indices,
                                            ));
                                        }
                                        metrics
                                            .backfill_snapshot_read_row_count
                                            .inc_by(cur_barrier_snapshot_processed_rows);
                                        metrics
                                            .backfill_upstream_output_row_count
                                            .inc_by(cur_barrier_upstream_processed_rows);
                                        break 'backfill_loop;
                                    }
                                    Some(record) => {
                                        // Buffer the snapshot read row.
                                        if let Some(data_chunk) = builder.append_one_row(record) {
                                            yield Message::Chunk(Self::handle_snapshot_chunk(
                                                data_chunk,
                                                &mut current_pos,
                                                &mut cur_barrier_snapshot_processed_rows,
                                                &mut total_snapshot_processed_rows,
                                                &pk_indices,
                                                &self.output_indices,
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Before processing barrier, if did not snapshot read,
                    // do a snapshot read first.
                    // This is so we don't lose the tombstone iteration progress.
                    // If paused, we also can't read any snapshot records.
                    if !has_snapshot_read && !paused {
                        assert!(
                            builder.is_empty(),
                            "Builder should be empty if no snapshot read"
                        );
                        let (_, snapshot) = backfill_stream.into_inner();
                        #[for_await]
                        for msg in snapshot {
                            let Either::Right(msg) = msg else {
                                bail!("BUG: snapshot_read contains upstream messages");
                            };
                            match msg? {
                                None => {
                                    // End of the snapshot read stream.
                                    // We let the barrier handling logic take care of upstream updates.
                                    // But we still want to exit backfill loop, so we mark snapshot read complete.
                                    snapshot_read_complete = true;
                                    break;
                                }
                                Some(row) => {
                                    let chunk = DataChunk::from_rows(&[row], &data_types);
                                    yield Message::Chunk(Self::handle_snapshot_chunk(
                                        chunk,
                                        &mut current_pos,
                                        &mut cur_barrier_snapshot_processed_rows,
                                        &mut total_snapshot_processed_rows,
                                        &pk_indices,
                                        &self.output_indices,
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }
                // When we break out of inner backfill_stream loop, it means we have a barrier.
                // If there are no updates and there are no snapshots left,
                // we already finished backfill and should have exited the outer backfill loop.
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => bail!("BUG: current_backfill loop exited without a barrier"),
                };

                // Process barrier:
                // - consume snapshot rows left in builder
                // - consume upstream buffer chunk
                // - switch snapshot

                // Consume snapshot rows left in builder
                let chunk = builder.consume_all();
                if let Some(chunk) = chunk {
                    yield Message::Chunk(Self::handle_snapshot_chunk(
                        chunk,
                        &mut current_pos,
                        &mut cur_barrier_snapshot_processed_rows,
                        &mut total_snapshot_processed_rows,
                        &pk_indices,
                        &self.output_indices,
                    ));
                }

                // Consume upstream buffer chunk
                // If no current_pos, means we did not process any snapshot
                // yet. In that case
                // we can just ignore the upstream buffer chunk, but still need to clean it.
                if let Some(current_pos) = &current_pos {
                    for chunk in upstream_chunk_buffer.drain(..) {
                        cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                        yield Message::Chunk(mapping_chunk(
                            mark_chunk(chunk, current_pos, &pk_indices, pk_order),
                            &self.output_indices,
                        ));
                    }
                } else {
                    upstream_chunk_buffer.clear()
                }

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                // Update snapshot read epoch.
                snapshot_read_epoch = barrier.epoch.prev;

                self.progress.update(
                    barrier.epoch,
                    snapshot_read_epoch,
                    total_snapshot_processed_rows,
                );

                // Persist state on barrier
                Self::persist_state(
                    barrier.epoch,
                    &mut self.state_table,
                    false,
                    &current_pos,
                    total_snapshot_processed_rows,
                    &mut old_state,
                    &mut current_state,
                )
                .await?;

                tracing::trace!(
                    epoch = ?barrier.epoch,
                    ?current_pos,
                    total_snapshot_processed_rows,
                    "Backfill state persisted"
                );

                // Update snapshot read chunk builder.
                if let Some(mutation) = barrier.mutation.as_deref() {
                    match mutation {
                        Mutation::Pause => {
                            paused = true;
                        }
                        Mutation::Resume => {
                            paused = false;
                        }
                        Mutation::Throttle(actor_to_apply) => {
                            let new_rate_limit_entry = actor_to_apply.get(&self.actor_id);
                            if let Some(new_rate_limit) = new_rate_limit_entry {
                                let new_rate_limit = (*new_rate_limit).into();
                                let old_rate_limit = self.rate_limiter.update(new_rate_limit);
                                if old_rate_limit != new_rate_limit {
                                    tracing::info!(
                                        old_rate_limit = ?old_rate_limit,
                                        new_rate_limit = ?new_rate_limit,
                                        upstream_table_id = upstream_table_id,
                                        actor_id = self.actor_id,
                                        "backfill rate limit changed",
                                    );
                                    // The builder is emptied above via `DataChunkBuilder::consume_all`.
                                    assert!(
                                        builder.is_empty(),
                                        "builder should already be emptied"
                                    );
                                    builder = create_builder(
                                        new_rate_limit,
                                        self.chunk_size,
                                        self.upstream_table.schema().data_types(),
                                    );
                                }
                            }
                        }
                        _ => (),
                    }
                }

                yield Message::Barrier(barrier);

                if snapshot_read_complete {
                    break 'backfill_loop;
                }

                // We will switch snapshot at the start of the next iteration of the backfill loop.
            }
        }

        tracing::trace!("Backfill has finished, waiting for barrier");

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    if is_finished {
                        // If already finished, no need persist any state, but we need to advance the epoch of the state table anyway.
                        if let Some(table) = &mut self.state_table {
                            table.commit(barrier.epoch).await?;
                        }
                    } else {
                        // If snapshot was empty, we do not need to backfill,
                        // but we still need to persist the finished state.
                        // We currently persist it on the second barrier here rather than first.
                        // This is because we can't update state table in first epoch,
                        // since it expects to have been initialized in previous epoch
                        // (there's no epoch before the first epoch).
                        if current_pos.is_none() {
                            current_pos = Some(construct_initial_finished_state(pk_indices.len()))
                        }

                        // We will update current_pos at least once,
                        // since snapshot read has to be non-empty,
                        // Or snapshot was empty and we construct a placeholder state.
                        debug_assert_ne!(current_pos, None);

                        Self::persist_state(
                            barrier.epoch,
                            &mut self.state_table,
                            true,
                            &current_pos,
                            total_snapshot_processed_rows,
                            &mut old_state,
                            &mut current_state,
                        )
                        .await?;
                        tracing::trace!(
                            epoch = ?barrier.epoch,
                            ?current_pos,
                            total_snapshot_processed_rows,
                            "Backfill position persisted after completion"
                        );
                    }

                    // For both backfill finished before recovery,
                    // and backfill which just finished, we need to update mview tracker,
                    // it does not persist this information.
                    self.progress
                        .finish(barrier.epoch, total_snapshot_processed_rows);
                    tracing::trace!(
                        epoch = ?barrier.epoch,
                        "Updated CreateMaterializedTracker"
                    );
                    yield msg;
                    break;
                }
                // Allow other messages to pass through.
                // We won't yield twice here, since if there's a barrier,
                // we will always break out of the loop.
                yield msg;
            }
        }

        tracing::trace!(
            "Backfill has already finished and forward messages directly to the downstream"
        );

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        // We don't need to report backfill progress any longer, as it has finished.
        // It will always be at 100%.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // If already finished, no need persist any state, but we need to advance the epoch of the state table anyway.
                    if let Some(table) = &mut self.state_table {
                        table.commit(barrier.epoch).await?;
                    }
                }

                yield msg;
            }
        }
    }

    async fn recover_backfill_state(
        state_table: Option<&StateTable<S>>,
        pk_len: usize,
    ) -> StreamExecutorResult<BackfillState> {
        let Some(state_table) = state_table else {
            // If no state table, but backfill is present, it must be from an old cluster.
            // In that case backfill must be finished, otherwise it won't have been persisted.
            return Ok(BackfillState {
                current_pos: None,
                is_finished: true,
                row_count: 0,
                old_state: None,
            });
        };
        let mut vnodes = state_table.vnodes().iter_vnodes_scalar();
        let first_vnode = vnodes.next().unwrap();
        let key: &[Datum] = &[Some(first_vnode.into())];
        let row = state_table.get_row(key).await?;
        let expected_state = Self::deserialize_backfill_state(row, pk_len);

        // All vnode partitions should have same state (no scale-in supported).
        for vnode in vnodes {
            let key: &[Datum] = &[Some(vnode.into())];
            let row = state_table.get_row(key).await?;
            let state = Self::deserialize_backfill_state(row, pk_len);
            assert_eq!(state.is_finished, expected_state.is_finished);
        }
        Ok(expected_state)
    }

    fn deserialize_backfill_state(row: Option<OwnedRow>, pk_len: usize) -> BackfillState {
        let Some(row) = row else {
            return BackfillState {
                current_pos: None,
                is_finished: false,
                row_count: 0,
                old_state: None,
            };
        };
        let row = row.into_inner();
        let mut old_state = vec![None; pk_len + METADATA_STATE_LEN];
        old_state[1..row.len() + 1].clone_from_slice(&row);
        let current_pos = Some((&row[0..pk_len]).into_owned_row());
        let is_finished = row[pk_len].clone().map_or(false, |d| d.into_bool());
        let row_count = row
            .get(pk_len + 1)
            .cloned()
            .unwrap_or(None)
            .map_or(0, |d| d.into_int64() as u64);
        BackfillState {
            current_pos,
            is_finished,
            row_count,
            old_state: Some(old_state),
        }
    }

    #[try_stream(ok = Option<OwnedRow>, error = StreamExecutorError)]
    async fn make_snapshot_stream<'a>(
        upstream_table: &'a BatchTable<S>,
        epoch: u64,
        current_pos: Option<OwnedRow>,
        paused: bool,
        rate_limiter: &'a MonitoredRateLimiter,
    ) {
        if paused {
            #[for_await]
            for _ in tokio_stream::pending() {
                bail!("BUG: paused stream should not yield");
            }
        } else {
            // Checked the rate limit is not zero.
            #[for_await]
            for r in
                Self::snapshot_read(upstream_table, HummockReadEpoch::NoWait(epoch), current_pos)
            {
                rate_limiter.wait(1).await;
                yield Some(r?);
            }
        }
        yield None;
    }

    /// Snapshot read the upstream mv.
    /// The rows from upstream snapshot read will be buffered inside the `builder`.
    /// If snapshot is dropped before its rows are consumed,
    /// remaining data in `builder` must be flushed manually.
    /// Otherwise when we scan a new snapshot, it is possible the rows in the `builder` would be
    /// present, Then when we flush we contain duplicate rows.
    #[try_stream(ok = OwnedRow, error = StreamExecutorError)]
    pub async fn snapshot_read(
        upstream_table: &BatchTable<S>,
        epoch: HummockReadEpoch,
        current_pos: Option<OwnedRow>,
    ) {
        let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos);
        let range_bounds = match range_bounds {
            None => {
                return Ok(());
            }
            Some(range_bounds) => range_bounds,
        };

        // We use uncommitted read here, because we have already scheduled the `BackfillExecutor`
        // together with the upstream mv.
        let row_iter = upstream_table
            .batch_iter_with_pk_bounds(
                epoch,
                row::empty(),
                range_bounds,
                true,
                // Here we only use small range prefetch because every barrier change, the executor will recreate a new iterator. So we do not need prefetch too much data.
                PrefetchOptions::prefetch_for_small_range_scan(),
            )
            .await?;

        #[for_await]
        for row in row_iter {
            yield row?;
        }
    }

    async fn persist_state(
        epoch: EpochPair,
        table: &mut Option<StateTable<S>>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        row_count: u64,
        old_state: &mut Option<Vec<Datum>>,
        current_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        // Backwards compatibility with no state table in backfill.
        let Some(table) = table else { return Ok(()) };
        utils::persist_state(
            epoch,
            table,
            is_finished,
            current_pos,
            row_count,
            old_state,
            current_state,
        )
        .await
    }

    /// 1. Converts from data chunk to stream chunk.
    /// 2. Update the current position.
    /// 3. Update Metrics
    /// 4. Map the chunk according to output indices, return
    ///    the stream chunk and do wrapping outside.
    fn handle_snapshot_chunk(
        data_chunk: DataChunk,
        current_pos: &mut Option<OwnedRow>,
        cur_barrier_snapshot_processed_rows: &mut u64,
        total_snapshot_processed_rows: &mut u64,
        pk_indices: &[usize],
        output_indices: &[usize],
    ) -> StreamChunk {
        let ops = vec![Op::Insert; data_chunk.capacity()];
        let chunk = StreamChunk::from_parts(ops, data_chunk);
        // Raise the current position.
        // As snapshot read streams are ordered by pk, so we can
        // just use the last row to update `current_pos`.
        *current_pos = Some(get_new_pos(&chunk, pk_indices));

        let chunk_cardinality = chunk.cardinality() as u64;
        *cur_barrier_snapshot_processed_rows += chunk_cardinality;
        *total_snapshot_processed_rows += chunk_cardinality;

        mapping_chunk(chunk, output_indices)
    }
}

impl<S> Execute for BackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
