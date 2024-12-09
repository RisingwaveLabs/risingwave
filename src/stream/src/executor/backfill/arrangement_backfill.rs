// Copyright 2024 RisingWave Labs
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

use either::Either;
use futures::stream;
use futures::stream::{select_all, select_with_strategy};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::bail;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::ReplicatedStateTable;
#[cfg(debug_assertions)]
use crate::executor::backfill::utils::METADATA_STATE_LEN;
use crate::executor::backfill::utils::{
    compute_bounds, create_builder, create_limiter, get_progress_per_vnode, mapping_chunk,
    mapping_message, mark_chunk_ref_by_vnode, owned_row_iter_with_vnode, persist_state_per_vnode,
    update_pos_by_vnode, BackfillProgressPerVnode, BackfillRateLimiter, BackfillState,
    RowAndVnodeStreamItem,
};
use crate::executor::prelude::*;
use crate::task::CreateMviewProgressReporter;

type Builders = HashMap<VirtualNode, DataChunkBuilder>;

/// Similar to [`super::no_shuffle_backfill::BackfillExecutor`].
/// Main differences:
/// - [`ArrangementBackfillExecutor`] can reside on a different CN, so it can be scaled
///   independently.
/// - To synchronize upstream shared buffer, it is initialized with a [`ReplicatedStateTable`].
pub struct ArrangementBackfillExecutor<S: StateStore, SD: ValueRowSerde> {
    /// Upstream table
    upstream_table: ReplicatedStateTable<S, SD>,

    /// Upstream with the same schema with the upstream table.
    upstream: Executor,

    /// Internal state table for persisting state of backfill state.
    state_table: StateTable<S>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgressReporter,

    actor_id: ActorId,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,

    rate_limit: Option<usize>,
}

impl<S, SD> ArrangementBackfillExecutor<S, SD>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub fn new(
        upstream_table: ReplicatedStateTable<S, SD>,
        upstream: Executor,
        state_table: StateTable<S>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgressReporter,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        rate_limit: Option<usize>,
    ) -> Self {
        Self {
            upstream_table,
            upstream,
            state_table,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
            rate_limit,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        tracing::debug!("backfill executor started");
        // The primary key columns, in the output columns of the upstream_table scan.
        // Table scan scans a subset of the columns of the upstream table.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();
        #[cfg(debug_assertions)]
        let state_len = self.upstream_table.pk_indices().len() + METADATA_STATE_LEN;
        let pk_order = self.upstream_table.pk_serde().get_order_types().to_vec();
        let upstream_table_id = self.upstream_table.table_id();
        let mut upstream_table = self.upstream_table;
        let vnodes = upstream_table.vnodes().clone();
        let mut rate_limit = self.rate_limit;

        // These builders will build data chunks.
        // We must supply them with the full datatypes which correspond to
        // pk + output_indices.
        let snapshot_data_types = self
            .upstream
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();
        let mut builders: Builders = upstream_table
            .vnodes()
            .iter_vnodes()
            .map(|vnode| {
                let builder =
                    create_builder(rate_limit, self.chunk_size, snapshot_data_types.clone());
                (vnode, builder)
            })
            .collect();

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let mut paused = first_barrier.is_pause_on_startup();
        let first_epoch = first_barrier.epoch;
        let is_newly_added = first_barrier.is_newly_added(self.actor_id);
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        self.state_table.init_epoch(first_epoch).await?;

        let progress_per_vnode = get_progress_per_vnode(&self.state_table).await?;

        let is_completely_finished = progress_per_vnode.iter().all(|(_, p)| {
            matches!(
                p.current_state(),
                &BackfillProgressPerVnode::Completed { .. }
            )
        });
        if is_completely_finished {
            assert!(!is_newly_added);
        }

        upstream_table.init_epoch(first_epoch).await?;

        let mut backfill_state: BackfillState = progress_per_vnode.into();

        let to_backfill = !is_completely_finished;

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
        let mut snapshot_read_epoch;

        // Keep track of rows from the snapshot.
        let mut total_snapshot_processed_rows: u64 = backfill_state.get_snapshot_row_count();

        // Arrangement Backfill Algorithm:
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot
        //
        // We construct a backfill stream with upstream as its left input and mv snapshot read
        // stream as its right input. When a chunk comes from upstream, we will buffer it.
        //
        // When a barrier comes from upstream:
        //  Immediately break out of backfill loop.
        //  - For each row of the upstream chunk buffer, compute vnode.
        //  - Get the `current_pos` corresponding to the vnode. Forward it to downstream if its pk
        //    <= `current_pos`, otherwise ignore it.
        //  - Flush all buffered upstream_chunks to replicated state table.
        //  - Update the `snapshot_read_epoch`.
        //  - Reconstruct the whole backfill stream with upstream and new mv snapshot read stream
        //    with the `snapshot_read_epoch`.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if to_backfill {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;

            let mut rate_limiter = rate_limit.and_then(create_limiter);

            let metrics = self
                .metrics
                .new_backfill_metrics(upstream_table_id, self.actor_id);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let mut snapshot_read_complete = false;
                let mut has_snapshot_read = false;

                // NOTE(kwannoel): Scope it so that immutable reference to `upstream_table` can be
                // dropped. Then we can write to `upstream_table` on barrier in the
                // next block.
                {
                    let left_upstream = upstream.by_ref().map(Either::Left);

                    // Check if stream paused
                    let paused = paused || matches!(rate_limit, Some(0));
                    // Create the snapshot stream
                    let right_snapshot = pin!(Self::make_snapshot_stream(
                        &upstream_table,
                        backfill_state.clone(), // FIXME: Use mutable reference instead.
                        paused,
                        &rate_limiter,
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
                                        // We have to process the barrier outside of the loop.
                                        // This is because our state_table reference is still live
                                        // here, we have to break the loop to drop it,
                                        // so we can do replication of upstream state_table.
                                        pending_barrier = Some(barrier);

                                        // Break the for loop and start a new snapshot read stream.
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
                                        for (vnode, builder) in &mut builders {
                                            if let Some(data_chunk) = builder.consume_all() {
                                                yield Message::Chunk(Self::handle_snapshot_chunk(
                                                    data_chunk,
                                                    *vnode,
                                                    &pk_in_output_indices,
                                                    &mut backfill_state,
                                                    &mut cur_barrier_snapshot_processed_rows,
                                                    &mut total_snapshot_processed_rows,
                                                    &self.output_indices,
                                                )?);
                                            }
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
                                    Some(RowAndVnodeStreamItem::Record { vnode, row }) => {
                                        let builder = builders.get_mut(&vnode).unwrap();
                                        if let Some(chunk) = builder.append_one_row(row) {
                                            yield Message::Chunk(Self::handle_snapshot_chunk(
                                                chunk,
                                                vnode,
                                                &pk_in_output_indices,
                                                &mut backfill_state,
                                                &mut cur_barrier_snapshot_processed_rows,
                                                &mut total_snapshot_processed_rows,
                                                &self.output_indices,
                                            )?);
                                        }
                                    }
                                    Some(RowAndVnodeStreamItem::Finished { vnode }) => {
                                        // Consume remaining rows in the builder.
                                        let builder = builders.get_mut(&vnode).unwrap();
                                        if let Some(data_chunk) = builder.consume_all() {
                                            yield Message::Chunk(Self::handle_snapshot_chunk(
                                                data_chunk,
                                                vnode,
                                                &pk_in_output_indices,
                                                &mut backfill_state,
                                                &mut cur_barrier_snapshot_processed_rows,
                                                &mut total_snapshot_processed_rows,
                                                &self.output_indices,
                                            )?);
                                        }
                                        // Update state for that vnode to finished.
                                        backfill_state.finish_progress(
                                            vnode,
                                            upstream_table.pk_indices().len(),
                                        );
                                    }
                                }
                            }
                        }
                    }

                    // Before processing barrier, if did not snapshot read,
                    // do a snapshot read first.
                    // This is so we don't lose the tombstone iteration progress.
                    // Or if s3 read latency is high, we don't fail to read from s3.
                    //
                    // If paused, we can't read any snapshot records, skip this.
                    //
                    // If rate limit is set, respect the rate limit, check if we can read,
                    // If we can't, skip it. If no rate limit set, we can read.
                    let rate_limit_ready = rate_limiter
                        .as_ref()
                        .map(|r| r.check().is_ok())
                        .unwrap_or(true);
                    if !has_snapshot_read && !paused && rate_limit_ready {
                        debug_assert!(builders.values().all(|b| b.is_empty()));
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
                                    // Note that we don't have to drain builders here,
                                    // since we did not do snapshot read at all,
                                    // so there should be no rows in the builders.
                                    snapshot_read_complete = true;
                                    break;
                                }
                                Some(RowAndVnodeStreamItem::Record { vnode, row }) => {
                                    let builder = builders.get_mut(&vnode).unwrap();
                                    if let Some(chunk) = builder.append_one_row(row) {
                                        yield Message::Chunk(Self::handle_snapshot_chunk(
                                            chunk,
                                            vnode,
                                            &pk_in_output_indices,
                                            &mut backfill_state,
                                            &mut cur_barrier_snapshot_processed_rows,
                                            &mut total_snapshot_processed_rows,
                                            &self.output_indices,
                                        )?);
                                    }

                                    break;
                                }
                                Some(RowAndVnodeStreamItem::Finished { vnode }) => {
                                    // Update state for that vnode to finished.
                                    backfill_state
                                        .finish_progress(vnode, upstream_table.pk_indices().len());
                                    // But some other vnodes may not be finished yet.
                                    continue;
                                }
                            }
                        }
                    }
                }

                // Process barrier
                // When we break out of inner backfill_stream loop, it means we have a barrier.
                // If there are no updates and there are no snapshots left,
                // we already finished backfill and should have exited the outer backfill loop.
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => bail!("BUG: current_backfill loop exited without a barrier"),
                };

                // Process barrier:
                // - consume snapshot rows left in builder.
                // - consume upstream buffer chunk
                // - handle mutations
                // - switch snapshot

                // consume snapshot rows left in builder.
                // NOTE(kwannoel): `zip_eq_debug` does not work here,
                // we encounter "higher-ranked lifetime error".
                for (vnode, chunk) in builders.iter_mut().map(|(vnode, b)| {
                    let chunk = b.consume_all().map(|chunk| {
                        let ops = vec![Op::Insert; chunk.capacity()];
                        StreamChunk::from_parts(ops, chunk)
                    });
                    (vnode, chunk)
                }) {
                    if let Some(chunk) = chunk {
                        let chunk_cardinality = chunk.cardinality() as u64;
                        // Raise the current position.
                        // As snapshot read streams are ordered by pk, so we can
                        // just use the last row to update `current_pos`.
                        update_pos_by_vnode(
                            *vnode,
                            &chunk,
                            &pk_in_output_indices,
                            &mut backfill_state,
                            chunk_cardinality,
                        )?;

                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                        total_snapshot_processed_rows += chunk_cardinality;
                        yield Message::Chunk(mapping_chunk(chunk, &self.output_indices));
                    }
                }

                // consume upstream buffer chunk
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                    // FIXME: Replace with `snapshot_is_processed`
                    // Flush downstream.
                    // If no current_pos, means no snapshot processed yet.
                    // Also means we don't need propagate any updates <= current_pos.
                    if backfill_state.has_progress() {
                        yield Message::Chunk(mapping_chunk(
                            mark_chunk_ref_by_vnode(
                                &chunk,
                                &backfill_state,
                                &pk_in_output_indices,
                                &upstream_table,
                                &pk_order,
                            )?,
                            &self.output_indices,
                        ));
                    }

                    // Replicate
                    upstream_table.write_chunk(chunk);
                }

                upstream_table.commit(barrier.epoch).await?;

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                // Update snapshot read epoch.
                snapshot_read_epoch = barrier.epoch.prev;

                // TODO(kwannoel): Not sure if this holds for arrangement backfill.
                // May need to revisit it.
                // Need to check it after scale-in / scale-out.
                self.progress.update(
                    barrier.epoch,
                    snapshot_read_epoch,
                    total_snapshot_processed_rows,
                );

                // Persist state on barrier
                persist_state_per_vnode(
                    barrier.epoch,
                    &mut self.state_table,
                    &mut backfill_state,
                    #[cfg(debug_assertions)]
                    state_len,
                    vnodes.iter_vnodes(),
                )
                .await?;

                tracing::trace!(
                    barrier = ?barrier,
                    "barrier persisted"
                );

                // handle mutations
                if let Some(mutation) = barrier.mutation.as_deref() {
                    use crate::executor::Mutation;
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
                                let new_rate_limit = new_rate_limit.as_ref().map(|x| *x as _);
                                if new_rate_limit != rate_limit {
                                    rate_limit = new_rate_limit;
                                    tracing::info!(
                                        new_rate_limit = ?rate_limit,
                                        "rate limit changed",
                                    );
                                    // The builder is emptied above via `DataChunkBuilder::consume_all`.
                                    for (_, builder) in builders {
                                        assert!(
                                            builder.is_empty(),
                                            "builder should already be emptied"
                                        );
                                    }
                                    builders = upstream_table
                                        .vnodes()
                                        .iter_vnodes()
                                        .map(|vnode| {
                                            let builder = create_builder(
                                                rate_limit,
                                                self.chunk_size,
                                                snapshot_data_types.clone(),
                                            );
                                            (vnode, builder)
                                        })
                                        .collect();
                                    rate_limiter = new_rate_limit.and_then(create_limiter);
                                }
                            }
                        }
                        _ => {}
                    }
                }

                yield Message::Barrier(barrier);

                // We will switch snapshot at the start of the next iteration of the backfill loop.
                // Unless snapshot read is already completed.
                if snapshot_read_complete {
                    break 'backfill_loop;
                }
            }
        }

        tracing::debug!("snapshot read finished, wait to commit state on next barrier");

        // Update our progress as finished in state table.

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    if is_completely_finished {
                        // If already finished, no need to persist any state. But we need to advance the epoch anyway
                        self.state_table.commit(barrier.epoch).await?;
                    } else {
                        // If snapshot was empty, we do not need to backfill,
                        // but we still need to persist the finished state.
                        // We currently persist it on the second barrier here rather than first.
                        // This is because we can't update state table in first epoch,
                        // since it expects to have been initialized in previous epoch
                        // (there's no epoch before the first epoch).
                        for vnode in upstream_table.vnodes().iter_vnodes() {
                            backfill_state
                                .finish_progress(vnode, upstream_table.pk_indices().len());
                        }

                        persist_state_per_vnode(
                            barrier.epoch,
                            &mut self.state_table,
                            &mut backfill_state,
                            #[cfg(debug_assertions)]
                            state_len,
                            vnodes.iter_vnodes(),
                        )
                        .await?;
                    }

                    self.progress
                        .finish(barrier.epoch, total_snapshot_processed_rows);
                    yield msg;
                    break;
                }
                // Allow other messages to pass through.
                // We won't yield twice here, since if there's a barrier,
                // we will always break out of the loop.
                yield msg;
            }
        }

        tracing::debug!("backfill finished");

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // If already finished, no need persist any state, but we need to advance the epoch of the state table anyway.
                    self.state_table.commit(barrier.epoch).await?;
                }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<RowAndVnodeStreamItem>, error = StreamExecutorError)]
    async fn make_snapshot_stream<'a>(
        upstream_table: &'a ReplicatedStateTable<S, SD>,
        backfill_state: BackfillState,
        paused: bool,
        rate_limiter: &'a Option<BackfillRateLimiter>,
    ) {
        if paused {
            #[for_await]
            for _ in tokio_stream::pending() {
                bail!("BUG: paused stream should not yield");
            }
        } else {
            // Checked the rate limit is not zero.
            #[for_await]
            for r in Self::snapshot_read_per_vnode(upstream_table, backfill_state) {
                let r = r?;
                if let Some(rate_limit) = rate_limiter {
                    rate_limit.until_ready().await;
                }
                yield r;
            }
        }
    }

    fn handle_snapshot_chunk(
        chunk: DataChunk,
        vnode: VirtualNode,
        pk_in_output_indices: &[usize],
        backfill_state: &mut BackfillState,
        cur_barrier_snapshot_processed_rows: &mut u64,
        total_snapshot_processed_rows: &mut u64,
        output_indices: &[usize],
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = StreamChunk::from_parts(vec![Op::Insert; chunk.capacity()], chunk);
        // Raise the current position.
        // As snapshot read streams are ordered by pk, so we can
        // just use the last row to update `current_pos`.
        let snapshot_row_count_delta = chunk.cardinality() as u64;
        update_pos_by_vnode(
            vnode,
            &chunk,
            pk_in_output_indices,
            backfill_state,
            snapshot_row_count_delta,
        )?;

        let chunk_cardinality = chunk.cardinality() as u64;
        *cur_barrier_snapshot_processed_rows += chunk_cardinality;
        *total_snapshot_processed_rows += chunk_cardinality;
        Ok(mapping_chunk(chunk, output_indices))
    }

    /// Read snapshot per vnode.
    /// These streams should be sorted in storage layer.
    /// 1. Get row iterator / vnode.
    /// 2. Merge it with `select_all`.
    /// 3. Change it into a chunk iterator with `iter_chunks`.
    /// This means it should fetch a row from each iterator to form a chunk.
    ///
    /// We interleave at chunk per vnode level rather than rows.
    /// This is so that we can compute `current_pos` once per chunk, since they correspond to 1
    /// vnode.
    ///
    /// The stream contains pairs of `(VirtualNode, StreamChunk)`.
    /// The `VirtualNode` is the vnode that the chunk belongs to.
    /// The `StreamChunk` is the chunk that contains the rows from the vnode.
    /// If it's `None`, it means the vnode has no more rows for this snapshot read.
    ///
    /// The `snapshot_read_epoch` is supplied as a parameter for `state_table`.
    /// It is required to ensure we read a fully-checkpointed snapshot the **first time**.
    ///
    /// The rows from upstream snapshot read will be buffered inside the `builder`.
    /// If snapshot is dropped before its rows are consumed,
    /// remaining data in `builder` must be flushed manually.
    /// Otherwise when we scan a new snapshot, it is possible the rows in the `builder` would be
    /// present, Then when we flush we contain duplicate rows.
    #[try_stream(ok = Option<RowAndVnodeStreamItem>, error = StreamExecutorError)]
    async fn snapshot_read_per_vnode(
        upstream_table: &ReplicatedStateTable<S, SD>,
        backfill_state: BackfillState,
    ) {
        let mut iterators = vec![];
        for vnode in upstream_table.vnodes().iter_vnodes() {
            let backfill_progress = backfill_state.get_progress(&vnode)?;
            let current_pos = match backfill_progress {
                BackfillProgressPerVnode::NotStarted => None,
                BackfillProgressPerVnode::Completed { .. } => {
                    // If completed, we don't need to continue reading snapshot.
                    continue;
                }
                BackfillProgressPerVnode::InProgress { current_pos, .. } => {
                    Some(current_pos.clone())
                }
            };

            let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos.clone());
            if range_bounds.is_none() {
                continue;
            }
            let range_bounds = range_bounds.unwrap();

            tracing::trace!(
                vnode = ?vnode,
                current_pos = ?current_pos,
                range_bounds = ?range_bounds,
                "iter_with_vnode_and_output_indices"
            );
            let vnode_row_iter = upstream_table
                .iter_with_vnode_and_output_indices(
                    vnode,
                    &range_bounds,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;

            let vnode_row_iter = Box::pin(owned_row_iter_with_vnode(vnode, vnode_row_iter));
            iterators.push(vnode_row_iter);
        }

        // TODO(kwannoel): We can provide an option between snapshot read in parallel vs serial.
        let vnode_row_iter = select_all(iterators);

        #[for_await]
        for vnode_and_row in vnode_row_iter {
            yield Some(vnode_and_row?);
        }
        yield None;
        return Ok(());
    }
}

impl<S, SD> Execute for ArrangementBackfillExecutor<S, SD>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
