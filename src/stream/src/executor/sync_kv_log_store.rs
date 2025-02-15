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

//! This contains the synced kv log store implementation.
//! It's meant to buffer a large number of records emitted from upstream,
//! to avoid overwhelming the downstream executor.
//!
//! The synced kv log store polls two futures:
//!
//! 1. Upstream: upstream message source
//!
//!    It will write stream messages to the log store buffer. e.g. `Message::Barrier`, `Message::Chunk`, ...
//!    When writing a stream chunk, if the log store buffer is full, it will:
//!      a. Flush the buffer to the log store.
//!      b. Convert the stream chunk into a reference (`LogStoreBufferItem::Flushed`)
//!         which can read the corresponding chunks in the log store.
//!         We will compact adjacent references,
//!         so it can read multiple chunks if there's a build up.
//!
//!    On receiving barriers, it will:
//!      a. Apply truncation to historical data in the logstore.
//!      b. Flush and checkpoint the logstore data.
//!
//! 2. State store + buffer + recently flushed chunks: the storage components of the logstore.
//!
//!    It will read all historical data from the logstore first. This can be done just by
//!    constructing a state store stream, which will read all data until the latest epoch.
//!    This is a static snapshot of data.
//!    For any subsequently flushed chunks, we will read them via
//!    `flushed_chunk_future`. See the next paragraph below.
//!
//!    We will next read `flushed_chunk_future` (if there's one pre-existing one), see below for how
//!    it's constructed, what it is.
//!
//!    Finally we will pop the earliest item in the buffer.
//!    - If it's a chunk yield it.
//!    - If it's a watermark yield it.
//!    - If it's a flushed chunk reference (`LogStoreBufferItem::Flushed`),
//!      we will read the corresponding chunks in the log store.
//!      This is done by constructing a `flushed_chunk_future` which will read the log store
//!      using the `seq_id`.
//!   - Barrier,
//!     because they are directly propagated from the upstream when polling it.
//!
//! TODO(kwannoel):
//! - [] Add dedicated metrics for sync log store, namespace according to the upstream.
//! - [] Add tests
//! - [] Handle watermark r/w
//! - [] Handle paused stream

use std::collections::VecDeque;
use std::future::pending;
use std::mem::replace;
use std::pin::Pin;

use anyhow::anyhow;
use futures::future::{select, BoxFuture, Either};
use futures::stream::StreamFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::must_match;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult};
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, OpConsistencyLevel, StateStoreRead,
};
use risingwave_storage::StateStore;
use rw_futures_util::drop_either_future;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::state::{
    new_log_store_state, LogStorePostSealCurrentEpoch, LogStoreReadState,
    LogStoreStateWriteChunkFuture, LogStoreWriteState,
};
use crate::common::log_store_impl::kv_log_store::{
    FlushInfo, KvLogStoreMetrics, ReaderTruncationOffsetType, SeqIdType, FIRST_SEQ_ID,
};
use crate::executor::prelude::*;
use crate::executor::{
    Barrier, BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult,
};

type ReadFlushedChunkFuture = BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, u64)>>;

pub struct SyncedKvLogStoreExecutor<S: StateStore> {
    actor_context: ActorContextRef,
    table_id: TableId,
    metrics: KvLogStoreMetrics,
    serde: LogStoreRowSerde,

    // Upstream
    upstream: Executor,

    // Log store state
    state_store: S,
    buffer_max_size: usize,
}
// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    pub(crate) fn new(
        actor_context: ActorContextRef,
        table_id: u32,
        metrics: KvLogStoreMetrics,
        serde: LogStoreRowSerde,
        state_store: S,
        buffer_max_size: usize,
        upstream: Executor,
    ) -> Self {
        Self {
            actor_context,
            table_id: TableId::new(table_id),
            metrics,
            serde,
            state_store,
            upstream,
            buffer_max_size,
        }
    }
}

struct FlushedChunkInfo {
    epoch: u64,
    start_seq_id: SeqIdType,
    end_seq_id: SeqIdType,
    flush_info: FlushInfo,
    vnode_bitmap: Bitmap,
}

enum WriteFuture<S: LocalStateStore> {
    ReceiveFromUpstream {
        future: StreamFuture<BoxedMessageStream>,
        write_state: LogStoreWriteState<S>,
    },
    FlushingChunk {
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        future: Pin<Box<LogStoreStateWriteChunkFuture<S>>>,
        stream: BoxedMessageStream,
    },
    Empty,
}

enum WriteFutureEvent {
    UpstreamMessageReceived(Message),
    ChunkFlushed(FlushedChunkInfo),
}

impl<S: LocalStateStore> WriteFuture<S> {
    fn flush_chunk(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
        chunk: StreamChunk,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> Self {
        Self::FlushingChunk {
            epoch,
            start_seq_id,
            end_seq_id,
            future: Box::pin(write_state.into_write_chunk_future(
                chunk,
                epoch,
                start_seq_id,
                end_seq_id,
            )),
            stream,
        }
    }

    fn receive_from_upstream(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        Self::ReceiveFromUpstream {
            future: stream.into_future(),
            write_state,
        }
    }

    async fn next_event(
        &mut self,
    ) -> StreamExecutorResult<(BoxedMessageStream, LogStoreWriteState<S>, WriteFutureEvent)> {
        match self {
            WriteFuture::ReceiveFromUpstream { future, .. } => {
                let (opt, stream) = future.await;
                must_match!(replace(self, WriteFuture::Empty), WriteFuture::ReceiveFromUpstream { write_state, .. } => {
                    opt
                    .ok_or_else(|| anyhow!("end of upstream input").into())
                    .and_then(|result| result.map(|item| {
                        (stream, write_state, WriteFutureEvent::UpstreamMessageReceived(item))
                    }))
                })
            }
            WriteFuture::FlushingChunk { future, .. } => {
                let (write_state, result) = future.await;
                let result = must_match!(replace(self, WriteFuture::Empty), WriteFuture::FlushingChunk { epoch, start_seq_id, end_seq_id, stream, ..  } => {
                    result.map(|(flush_info, vnode_bitmap)| {
                        (stream, write_state, WriteFutureEvent::ChunkFlushed(FlushedChunkInfo {
                            epoch,
                            start_seq_id,
                            end_seq_id,
                            flush_info,
                            vnode_bitmap,
                        }))
                    })
                });
                result.map_err(Into::into)
            }
            WriteFuture::Empty => {
                unreachable!("should not be polled after ready")
            }
        }
    }
}

// Stream interface
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(self) {
        let mut input = self.upstream.execute();

        // init first epoch + local state store
        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_write_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier.clone());

        let local_state_store = self
            .state_store
            .new_local(NewLocalOptions {
                table_id: self.table_id,
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes: self.serde.vnodes().clone(),
            })
            .await;

        let (mut read_state, mut initial_write_state) =
            new_log_store_state(self.table_id, local_state_store, self.serde);
        initial_write_state.init(first_write_epoch).await?;

        let mut initial_write_epoch = first_write_epoch;

        // We only recreate the consume stream when:
        // 1. On bootstrap
        // 2. On vnode update
        'recreate_consume_stream: loop {
            let mut seq_id = FIRST_SEQ_ID;
            let mut truncation_offset = None;
            let mut buffer = SyncedLogStoreBuffer {
                buffer: VecDeque::new(),
                max_size: self.buffer_max_size,
                next_chunk_id: 0,
                metrics: self.metrics.clone(),
            };
            let mut read_future = ReadFuture::ReadingPersistedStream(
                read_state
                    .read_persisted_log_store(&self.metrics, initial_write_epoch.prev, None)
                    .await?,
            );

            let mut write_future = WriteFuture::receive_from_upstream(input, initial_write_state);

            loop {
                let select_result = {
                    let read_future =
                        read_future.next_chunk(&read_state, &mut buffer, &self.metrics);
                    pin_mut!(read_future);
                    let write_future = write_future.next_event();
                    pin_mut!(write_future);
                    let output = select(write_future, read_future).await;
                    drop_either_future(output)
                };
                match select_result {
                    Either::Left(result) => {
                        // drop the future to ensure that the future must be reset later
                        drop(write_future);
                        let (stream, mut write_state, either) = result?;
                        match either {
                            WriteFutureEvent::UpstreamMessageReceived(msg) => {
                                match msg {
                                    Message::Barrier(barrier) => {
                                        let write_state_post_write_barrier = Self::write_barrier(
                                            &mut write_state,
                                            barrier.clone(),
                                            &self.metrics,
                                            truncation_offset,
                                            &mut buffer,
                                        )
                                        .await?;
                                        seq_id = FIRST_SEQ_ID;
                                        let update_vnode_bitmap =
                                            barrier.as_update_vnode_bitmap(self.actor_context.id);
                                        let barrier_epoch = barrier.epoch;
                                        yield Message::Barrier(barrier);
                                        write_state_post_write_barrier
                                            .post_yield_barrier(update_vnode_bitmap.clone());
                                        if let Some(vnode_bitmap) = update_vnode_bitmap {
                                            // Apply Vnode Update
                                            read_state.update_vnode_bitmap(vnode_bitmap);
                                            initial_write_epoch = barrier_epoch;
                                            input = stream;
                                            initial_write_state = write_state;
                                            continue 'recreate_consume_stream;
                                        } else {
                                            write_future = WriteFuture::receive_from_upstream(
                                                stream,
                                                write_state,
                                            );
                                        }
                                    }
                                    Message::Chunk(chunk) => {
                                        let start_seq_id = seq_id;
                                        seq_id += chunk.cardinality() as SeqIdType;
                                        let end_seq_id = seq_id - 1;
                                        let epoch = write_state.epoch().curr;
                                        if let Some(chunk_to_flush) = buffer.add_or_flush_chunk(
                                            start_seq_id,
                                            end_seq_id,
                                            chunk,
                                            epoch,
                                        ) {
                                            write_future = WriteFuture::flush_chunk(
                                                stream,
                                                write_state,
                                                chunk_to_flush,
                                                epoch,
                                                start_seq_id,
                                                end_seq_id,
                                            );
                                        } else {
                                            write_future = WriteFuture::receive_from_upstream(
                                                stream,
                                                write_state,
                                            );
                                        }
                                    }
                                    // FIXME(kwannoel): This should truncate the logstore,
                                    // it will not bypass like barrier.
                                    Message::Watermark(_watermark) => {
                                        write_future =
                                            WriteFuture::receive_from_upstream(stream, write_state);
                                    }
                                }
                            }
                            WriteFutureEvent::ChunkFlushed(FlushedChunkInfo {
                                start_seq_id,
                                end_seq_id,
                                epoch,
                                flush_info,
                                vnode_bitmap,
                            }) => {
                                buffer.add_flushed_item_to_buffer(
                                    start_seq_id,
                                    end_seq_id,
                                    vnode_bitmap,
                                    epoch,
                                );
                                flush_info.report(&self.metrics);
                                write_future =
                                    WriteFuture::receive_from_upstream(stream, write_state);
                            }
                        }
                    }
                    Either::Right(result) => {
                        let (chunk, new_truncate_offset) = result?;
                        if let Some(new_truncate_offset) = new_truncate_offset {
                            truncation_offset = Some(new_truncate_offset);
                        }
                        yield Message::Chunk(chunk);
                    }
                }
            }
        }
    }
}

enum ReadFuture<S: StateStoreRead> {
    ReadingPersistedStream(Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>),
    ReadingFlushedChunk {
        future: ReadFlushedChunkFuture,
        truncate_offset: ReaderTruncationOffsetType,
    },
    Idle,
}

// Read methods
impl<S: StateStoreRead> ReadFuture<S> {
    // TODO: should change to always return a truncate offset to ensure that each stream chunk has a truncate offset
    async fn next_chunk(
        &mut self,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &KvLogStoreMetrics,
    ) -> StreamExecutorResult<(StreamChunk, Option<ReaderTruncationOffsetType>)> {
        match self {
            ReadFuture::ReadingPersistedStream(stream) => {
                while let Some((_, item)) = stream.try_next().await? {
                    match item {
                        KvLogStoreItem::Barrier { .. } => {
                            continue;
                        }
                        KvLogStoreItem::StreamChunk(chunk) => {
                            // TODO: should have truncate offset when consuming historical data
                            return Ok((chunk, None));
                        }
                    }
                }
                *self = ReadFuture::Idle;
            }
            ReadFuture::ReadingFlushedChunk { .. } | ReadFuture::Idle => {}
        }
        match self {
            ReadFuture::ReadingPersistedStream(_) => {
                unreachable!("must have finished read persisted stream when reaching here")
            }
            ReadFuture::ReadingFlushedChunk { .. } => {}
            ReadFuture::Idle => loop {
                let Some((item_epoch, item)) = buffer.pop_front() else {
                    return pending().await;
                };
                match item {
                    LogStoreBufferItem::StreamChunk {
                        chunk, end_seq_id, ..
                    } => {
                        return Ok((chunk, Some((item_epoch, Some(end_seq_id)))));
                    }
                    LogStoreBufferItem::Flushed {
                        vnode_bitmap,
                        start_seq_id,
                        end_seq_id,
                        chunk_id,
                    } => {
                        let truncate_offset = (item_epoch, Some(end_seq_id));
                        let read_metrics = metrics.flushed_buffer_read_metrics.clone();
                        let future = read_state
                            .read_flushed_chunk(
                                vnode_bitmap,
                                chunk_id,
                                start_seq_id,
                                end_seq_id,
                                item_epoch,
                                read_metrics,
                            )
                            .boxed();
                        *self = ReadFuture::ReadingFlushedChunk {
                            future,
                            truncate_offset,
                        };
                        break;
                    }
                    LogStoreBufferItem::Barrier { .. } => {
                        continue;
                    }
                    LogStoreBufferItem::UpdateVnodes(_) => {
                        unreachable!("UpdateVnodes should not be in buffer")
                    }
                }
            },
        }

        let (future, truncate_offset) = match self {
            ReadFuture::ReadingPersistedStream(_) | ReadFuture::Idle => {
                unreachable!("should be at ReadingFlushedChunk")
            }
            ReadFuture::ReadingFlushedChunk {
                future,
                truncate_offset,
            } => (future, *truncate_offset),
        };

        let (_, chunk, _) = future.await?;
        *self = ReadFuture::Idle;
        Ok((chunk, Some(truncate_offset)))
    }
}

// Write methods
impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    async fn write_barrier<'a>(
        write_state: &'a mut LogStoreWriteState<S::Local>,
        barrier: Barrier,
        metrics: &KvLogStoreMetrics,
        truncation_offset: Option<ReaderTruncationOffsetType>,
        buffer: &mut SyncedLogStoreBuffer,
    ) -> StreamExecutorResult<LogStorePostSealCurrentEpoch<'a, S::Local>> {
        let epoch = barrier.epoch.prev;
        let mut writer = write_state.start_writer(false);
        // FIXME(kwannoel): Handle paused stream.
        writer.write_barrier(epoch, barrier.is_checkpoint())?;

        // FIXME(kwannoel): Flush all unflushed chunks
        // As an optimization we can also change it into flushed items instead.
        // This will reduce memory consumption of logstore.

        // TODO: may stop the for loop when seeing any of flushed item to avoid always iterating the whole buffer
        for (epoch, item) in &mut buffer.buffer {
            match item {
                LogStoreBufferItem::StreamChunk {
                    chunk,
                    start_seq_id,
                    end_seq_id,
                    flushed,
                    ..
                } => {
                    if !*flushed {
                        writer.write_chunk(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                        *flushed = true;
                    }
                }
                LogStoreBufferItem::Flushed { .. }
                | LogStoreBufferItem::Barrier { .. }
                | LogStoreBufferItem::UpdateVnodes(_) => {}
            }
        }

        let (flush_info, _) = writer.finish().await?;
        flush_info.report(metrics);

        // Apply truncation
        let post_seal = write_state.seal_current_epoch(barrier.epoch.curr, truncation_offset);

        // Add to buffer
        buffer.buffer.push_back((
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint: barrier.is_checkpoint(),
                next_epoch: barrier.epoch.curr,
            },
        ));
        buffer.next_chunk_id = 0;
        buffer.update_unconsumed_buffer_metrics();

        Ok(post_seal)
    }
}

struct SyncedLogStoreBuffer {
    buffer: VecDeque<(u64, LogStoreBufferItem)>,
    max_size: usize,
    next_chunk_id: ChunkId,
    metrics: KvLogStoreMetrics,
}

impl SyncedLogStoreBuffer {
    fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        chunk: StreamChunk,
        epoch: u64,
    ) -> Option<StreamChunk> {
        let current_size = self.buffer.len();
        let chunk_size = chunk.cardinality();

        let should_flush_chunk = current_size + chunk_size >= self.max_size;
        if should_flush_chunk {
            Some(chunk)
        } else {
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.buffer.back_mut()
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.buffer.push_back((
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            ));
            tracing::trace!("Adding flushed item to buffer: start_seq_id: {start_seq_id}, end_seq_id: {end_seq_id}, chunk_id: {chunk_id}");
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_unconsumed_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        self.buffer.push_back((
            epoch,
            LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            },
        ));
        self.update_unconsumed_buffer_metrics();
    }

    fn pop_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        self.buffer.pop_front()
    }

    fn update_unconsumed_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        for (_, item) in &self.buffer {
            match item {
                LogStoreBufferItem::StreamChunk { chunk, .. } => {
                    row_count += chunk.cardinality();
                }
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    ..
                } => {
                    row_count += (end_seq_id - start_seq_id) as usize;
                }
                LogStoreBufferItem::Barrier { .. } => {
                    epoch_count += 1;
                }
                LogStoreBufferItem::UpdateVnodes(_) => {}
            }
        }
        self.metrics.buffer_unconsumed_epoch_count.set(epoch_count);
        self.metrics.buffer_unconsumed_row_count.set(row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.buffer.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.buffer
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
    }
}

impl<S> Execute for SyncedKvLogStoreExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::test_prelude::*;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table, test_payload_schema,
    };
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::executor::test_utils::MockSource;

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    // test read/write buffer
    #[tokio::test]
    async fn test_read_write_buffer() {
        init_logger();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let pk_indices = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), pk_indices.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            KvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            source,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   I
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   I
            -   5  10
            -   6  10
            -   8  10
            U-  9  10
            U+ 10  11",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        tx.push_barrier(test_epoch(2), false);

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }
    }

    // test barrier persisted read
    //
    // sequence of events (earliest -> latest):
    // barrier(1) -> chunk(1) -> chunk(2) -> poll(3) items -> barrier(2) -> poll(1) item
    // * poll just means we read from the executor stream.
    #[tokio::test]
    async fn test_barrier_persisted_read() {
        init_logger();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let pk_indices = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), pk_indices.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            KvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            10,
            source,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   I
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   I
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(1));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Barrier(barrier))) => {
                assert_eq!(barrier.epoch.curr, test_epoch(2));
            }
            other => panic!("Expected a barrier message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_eq!(chunk, chunk_1);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(chunk))) => {
                assert_eq!(chunk, chunk_2);
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }
    }

    // When we hit buffer max_chunk, we only store placeholder `FlushedItem`.
    // So we just let capacity = 0, and we will always flush incoming chunks to state store.
    #[tokio::test]
    async fn test_max_chunk_persisted_read() {
        init_logger();

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let column_descs = test_payload_schema(pk_info);
        let fields = column_descs
            .into_iter()
            .map(|desc| Field::new(desc.name.clone(), desc.data_type.clone()))
            .collect_vec();
        let schema = Schema { fields };
        let pk_indices = vec![0];
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema.clone(), pk_indices.clone());

        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));

        let table = gen_test_log_store_table(pk_info);

        let log_store_executor = SyncedKvLogStoreExecutor::new(
            ActorContext::for_test(123),
            table.id,
            KvLogStoreMetrics::for_test(),
            LogStoreRowSerde::new(&table, vnodes, pk_info),
            MemoryStateStore::new(),
            0,
            source,
        )
        .boxed();

        // Init
        tx.push_barrier(test_epoch(1), false);

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            +  10 11",
        );

        let chunk_2 = StreamChunk::from_pretty(
            "   I   T
            -   5  10
            -   6  10
            -   8  10
            U- 10  11
            U+ 10  10",
        );

        tx.push_chunk(chunk_1.clone());
        tx.push_chunk(chunk_2.clone());

        tx.push_barrier(test_epoch(2), false);

        let mut stream = log_store_executor.execute();

        for i in 1..=2 {
            match stream.next().await {
                Some(Ok(Message::Barrier(barrier))) => {
                    assert_eq!(barrier.epoch.curr, test_epoch(i));
                }
                other => panic!("Expected a barrier message, got {:?}", other),
            }
        }

        match stream.next().await {
            Some(Ok(Message::Chunk(actual))) => {
                let expected = StreamChunk::from_pretty(
                    "   I   T
                    +   5  10
                    +   6  10
                    +   8  10
                    +   9  10
                    +  10  11
                    -   5  10
                    -   6  10
                    -   8  10
                    U- 10  11
                    U+ 10  10",
                );
                assert!(
                    check_stream_chunk_eq(&actual, &expected),
                    "Expected: {:#?}, got: {:#?}",
                    expected,
                    actual
                );
            }
            other => panic!("Expected a chunk message, got {:?}", other),
        }
    }
}
