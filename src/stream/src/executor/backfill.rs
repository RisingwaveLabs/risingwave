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

use std::mem;
use std::ops::Bound;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message};
use crate::executor::PkIndices;
use crate::task::{ActorId, CreateMviewProgress};

pub struct BackfillExecutor<S: StateStore> {
    table: StorageTable<S>,

    upstream: BoxedExecutor,

    upstream_indices: Arc<[usize]>,

    current_pos: Row,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], msg: Message) -> Message {
    match msg {
        Message::Watermark(_) => {
            todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
        }

        Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            let mapped_columns = upstream_indices
                .iter()
                .map(|&i| columns[i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(ops, mapped_columns, visibility))
        }
        _ => msg,
    }
}

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    pub fn new(
        table: StorageTable<S>,
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "BackfillExecutor".to_owned(),
            },
            table,
            upstream,
            upstream_indices: upstream_indices.into(),
            current_pos: Row::new(vec![]),
            actor_id: progress.actor_id(),
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let backfill_pk_indices = self.pk_indices().to_vec();
        let upstream_indices = self.upstream_indices.to_vec();
        let table_pk_indices = self.table.pk_indices().to_vec();
        let upstream_pk_indices = self.upstream.pk_indices().to_vec();
        assert_eq!(table_pk_indices, upstream_pk_indices);

        // Project the upstream with `upstream_indices`.
        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices.clone(), msg)));

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;

        // If the barrier is a conf change of creating this mview, init backfill from its epoch.
        // Otherwise, it means we've recovered and the backfill is already finished.
        let to_backfill = first_barrier.is_add_dispatcher(self.actor_id);

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier.clone());

        if to_backfill {
            // The epoch used to snapshot read upstream mv.
            let mut snapshot_read_epoch = init_epoch;

            // Backfill Algorithm:
            //
            //   backfill_stream
            //  /               \
            // upstream       snapshot
            //
            // We construct a backfill stream with upstream as its left input and mv snapshot read
            // stream as its right input. When a chunk comes from upstream, we will
            // buffer it. When a checkpoint barrier comes from upstream:
            //  - Update the `snapshot_read_epoch`.
            //  - For each row of the upstream chunk buffer, forward it to downstream if its pk <=
            //    `current_pos`, otherwise ignore it.
            //  - reconstruct the whole backfill stream with upstream and new mv snapshot read
            //    stream with the `snapshot_read_epoch`.
            // When a chunk comes from snapshot, we forward it to the downstream and raise
            // `current_pos`. When none comes from snapshot, it means backfill has been
            // finished.
            //
            // Once the backfill loop ends, we forward the upstream directly to the downstream.
            'backfill_loop: loop {
                let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

                let mut left_upstream = (&mut upstream).map(Either::Left);

                let right_snapshot = Box::pin(
                    Self::snapshot_read(&self.table, snapshot_read_epoch, self.current_pos.clone())
                        .map(Either::Right),
                );

                println!(
                    "Actor: {:?} snapshot read current_row = {:?}",
                    &self.actor_id, &self.current_pos
                );

                // Prefer to select upstream, so we can stop snapshot stream as soon as the barrier
                // comes.
                let backfill_stream =
                    select_with_strategy(&mut left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                #[for_await]
                for either in backfill_stream {
                    match either {
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // If it is a checkpoint barrier, switch snapshot and consume
                                    // upstream buffer chunk
                                    let checkpoint = barrier.checkpoint;
                                    if checkpoint {
                                        println!(
                                            "Actor: {:?} meet checkpoint barrier epoch = {}",
                                            &self.actor_id, &barrier.epoch.prev
                                        );

                                        // Consume upstream buffer chunk
                                        let mut buffer = vec![];
                                        mem::swap(&mut upstream_chunk_buffer, &mut buffer);

                                        for chunk in buffer {
                                            yield Message::Chunk(Self::mark_chunk(
                                                chunk,
                                                &self.current_pos,
                                                &backfill_pk_indices,
                                            ));
                                        }
                                    }

                                    // Update snapshot read epoch.
                                    snapshot_read_epoch = barrier.epoch.prev;

                                    yield Message::Barrier(barrier);

                                    if checkpoint {
                                        println!(
                                            "Actor: {:?} yield checkpoint barrier epoch = {}",
                                            &self.actor_id, &snapshot_read_epoch
                                        );
                                        self.progress
                                            .update(snapshot_read_epoch, snapshot_read_epoch);
                                        // Break the for loop and start a new snapshot read stream.
                                        break;
                                    }
                                }
                                Message::Chunk(chunk) => {
                                    // Buffer the upstream chunk.
                                    upstream_chunk_buffer.push(chunk);
                                }
                                Message::Watermark(_) => todo!(
                                    "https://github.com/risingwavelabs/risingwave/issues/6042"
                                ),
                            }
                        }
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    // End of the snapshot read stream.
                                    println!("Actor: {} the end of snapshot", &self.actor_id);

                                    // Consume with the renaming stream buffer chunk
                                    let mut buffer = vec![];
                                    mem::swap(&mut upstream_chunk_buffer, &mut buffer);

                                    for chunk in buffer {
                                        yield Message::Chunk(Self::mark_chunk(
                                            chunk,
                                            &self.current_pos,
                                            &backfill_pk_indices,
                                        ));
                                    }

                                    // Finish backfill.
                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    self.current_pos = chunk
                                        .rows()
                                        .last()
                                        .unwrap()
                                        .1
                                        .row_by_indices(&table_pk_indices);

                                    println!(
                                        "Actor: {} snapshot push current_pos = {:?}",
                                        &self.actor_id, &self.current_pos
                                    );

                                    yield mapping(&self.upstream_indices, Message::Chunk(chunk));
                                }
                            }
                        }
                    }
                }
            }

            let mut finish_on_barrier = |msg: &Message| {
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch.curr);
                }
            };

            // Backfill has already finished.
            // Forward messages directly to the downstream.
            #[for_await]
            for msg in upstream {
                let msg: Message = msg?;
                finish_on_barrier(&msg);
                yield msg;
            }
        } else {
            // Forward messages directly to the downstream.

            #[for_await]
            for message in upstream {
                yield message?;
            }
        }
    }

    #[expect(clippy::needless_lifetimes, reason = "code generated by try_stream")]
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(table: &StorageTable<S>, epoch: u64, current_pos: Row) {
        // Empty row means it need to scan from the beginning, so we use Unbounded to scan.
        // Otherwise, use Excluded.
        let range_bounds = if current_pos.0.is_empty() {
            (Bound::Unbounded, Bound::Unbounded)
        } else {
            (Bound::Excluded(current_pos), Bound::Unbounded)
        };
        let iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(epoch),
                Row::empty(),
                range_bounds,
            )
            .await?;

        pin_mut!(iter);

        while let Some(data_chunk) = iter
            .collect_data_chunk(table.schema(), Some(1024))
            .stack_trace("batch_query_executor_collect_chunk")
            .await?
        {
            if data_chunk.cardinality() != 0 {
                let ops = vec![Op::Insert; data_chunk.capacity()];
                let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
                yield Some(stream_chunk);
            }
        }

        yield None;
    }

    /// Mark chunk:
    /// For each row of the chunk, forward it to downstream if its pk <= `current_pos`, otherwise
    /// ignore it. We implement it by changing the visibility bitmap.
    fn mark_chunk(
        chunk: StreamChunk,
        current_pos: &Row,
        backfill_pk_indices: &PkIndices,
    ) -> StreamChunk {
        let chunk = chunk.compact();
        println!("apply chunk {}", chunk.to_pretty_string());
        let (data, ops) = chunk.into_parts();
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        for v in data
            .rows()
            .map(|row| &row.row_by_indices(backfill_pk_indices) <= current_pos)
        {
            new_visibility.append(v);
        }
        let (columns, _) = data.into_parts();
        StreamChunk::new(ops, columns, Some(new_visibility.finish()))
    }
}

impl<S> Executor for BackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
