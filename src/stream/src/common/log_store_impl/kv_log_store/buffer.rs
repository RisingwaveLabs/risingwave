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

use std::collections::VecDeque;
use std::ops::DerefMut;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use parking_lot::{Mutex, MutexGuard};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult, TruncateOffset};
use tokio::sync::{oneshot, Notify};

use crate::common::log_store_impl::kv_log_store::{
    KvLogStoreMetrics, ReaderTruncationOffsetType, SeqIdType,
};

#[derive(Clone)]
pub(crate) enum LogStoreBufferItem {
    StreamChunk {
        chunk: StreamChunk,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        flushed: bool,
        chunk_id: ChunkId,
    },

    Flushed {
        vnode_bitmap: Bitmap,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        chunk_id: ChunkId,
    },

    Barrier {
        is_checkpoint: bool,
        next_epoch: u64,
    },

    UpdateVnodes(Arc<Bitmap>),
}

struct LogStoreBufferInner {
    /// Items not read by log reader. Newer item at the front
    unconsumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    /// Items already read by log reader by not truncated. Newer item at the front
    consumed_queue: VecDeque<(u64, LogStoreBufferItem)>,
    row_count: usize,
    max_row_count: usize,

    truncation_list: VecDeque<ReaderTruncationOffsetType>,

    next_chunk_id: ChunkId,

    metrics: KvLogStoreMetrics,
}

impl LogStoreBufferInner {
    fn update_unconsumed_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        for (_, item) in &self.unconsumed_queue {
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
            .set(self.unconsumed_queue.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.unconsumed_queue
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
    }

    fn can_add_stream_chunk(&self) -> bool {
        self.row_count < self.max_row_count
    }

    fn add_item(&mut self, epoch: u64, item: LogStoreBufferItem) {
        if let LogStoreBufferItem::StreamChunk { .. } = item {
            unreachable!("StreamChunk should call try_add_item")
        }
        if let LogStoreBufferItem::Barrier { .. } = &item {
            self.next_chunk_id = 0;
        }
        self.unconsumed_queue.push_front((epoch, item));
        self.update_unconsumed_buffer_metrics();
    }

    pub(crate) fn try_add_stream_chunk(
        &mut self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> Option<StreamChunk> {
        if !self.can_add_stream_chunk() {
            Some(chunk)
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.row_count += chunk.cardinality();
            self.unconsumed_queue.push_front((
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
            None
        }
    }

    fn pop_item(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        if let Some((epoch, item)) = self.unconsumed_queue.pop_back() {
            self.consumed_queue.push_front((epoch, item.clone()));
            self.update_unconsumed_buffer_metrics();
            Some((epoch, item))
        } else {
            None
        }
    }

    fn add_flushed(
        &mut self,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        new_vnode_bitmap: Bitmap,
    ) {
        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.unconsumed_queue.front_mut()
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
            self.add_item(
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            );
        }
        self.update_unconsumed_buffer_metrics();
    }

    fn add_truncate_offset(&mut self, (epoch, seq_id): ReaderTruncationOffsetType) {
        if let Some((prev_epoch, ref mut prev_seq_id)) = self.truncation_list.back_mut()
            && *prev_epoch == epoch
        {
            *prev_seq_id = seq_id;
        } else {
            self.truncation_list.push_back((epoch, seq_id));
        }
    }

    fn rewind(&mut self) {
        while let Some((epoch, item)) = self.consumed_queue.pop_front() {
            self.unconsumed_queue.push_back((epoch, item));
        }
        self.update_unconsumed_buffer_metrics();
    }
}

struct SharedMutex<T>(Arc<Mutex<T>>);

impl<T> Clone for SharedMutex<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> SharedMutex<T> {
    fn new(value: T) -> Self {
        Self(Arc::new(Mutex::new(value)))
    }

    fn inner(&self) -> MutexGuard<'_, T> {
        if let Some(guard) = self.0.try_lock() {
            guard
        } else {
            info!("fall back to lock");
            self.0.lock()
        }
    }
}

pub(crate) struct LogStoreBufferSender {
    init_epoch_tx: Option<oneshot::Sender<u64>>,
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
}

impl LogStoreBufferSender {
    pub(crate) fn init(&mut self, epoch: u64) {
        if let Err(e) = self
            .init_epoch_tx
            .take()
            .expect("should be Some in first init")
            .send(epoch)
        {
            error!("unable to send init epoch: {}", e);
        }
    }

    pub(crate) fn add_flushed(
        &self,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        vnode_bitmap: Bitmap,
    ) {
        self.buffer
            .inner()
            .add_flushed(epoch, start_seq_id, end_seq_id, vnode_bitmap);
        self.update_notify.notify_waiters();
    }

    pub(crate) fn try_add_stream_chunk(
        &self,
        epoch: u64,
        chunk: StreamChunk,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> Option<StreamChunk> {
        let ret = self
            .buffer
            .inner()
            .try_add_stream_chunk(epoch, chunk, start_seq_id, end_seq_id);
        if ret.is_none() {
            // notify when successfully add
            self.update_notify.notify_waiters();
        }
        ret
    }

    pub(crate) fn barrier(&self, epoch: u64, is_checkpoint: bool, next_epoch: u64) {
        self.buffer.inner().add_item(
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint,
                next_epoch,
            },
        );
        self.update_notify.notify_waiters();
    }

    pub(crate) fn update_vnode(&self, epoch: u64, vnode: Arc<Bitmap>) {
        self.buffer
            .inner()
            .add_item(epoch, LogStoreBufferItem::UpdateVnodes(vnode));
        self.update_notify.notify_waiters();
    }

    pub(crate) fn pop_truncation(&self, curr_epoch: u64) -> Option<ReaderTruncationOffsetType> {
        let mut inner = self.buffer.inner();
        let mut ret = None;
        while let Some((epoch, _)) = inner.truncation_list.front()
            && *epoch < curr_epoch
        {
            ret = inner.truncation_list.pop_front();
        }
        ret
    }

    pub(crate) fn flush_all_unflushed(
        &mut self,
        mut flush_fn: impl FnMut(&StreamChunk, u64, SeqIdType, SeqIdType) -> LogStoreResult<()>,
    ) -> LogStoreResult<()> {
        let mut inner_guard = self.buffer.inner();
        let inner = inner_guard.deref_mut();
        for (epoch, item) in inner
            .unconsumed_queue
            .iter_mut()
            .chain(inner.consumed_queue.iter_mut())
        {
            if let LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed,
                ..
            } = item
            {
                if *flushed {
                    // Since we iterate from new data to old data, when we meet a flushed data, the
                    // rest should have been flushed.
                    break;
                }
                flush_fn(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                *flushed = true;
            }
        }
        Ok(())
    }
}

pub(crate) struct LogStoreBufferReceiver {
    init_epoch_rx: Option<oneshot::Receiver<u64>>,
    buffer: SharedMutex<LogStoreBufferInner>,
    update_notify: Arc<Notify>,
}

impl LogStoreBufferReceiver {
    pub(crate) async fn init(&mut self) -> u64 {
        self.init_epoch_rx
            .take()
            .expect("should be Some in first init")
            .await
            .expect("should get the first epoch")
    }

    pub(crate) async fn next_item(&self) -> (u64, LogStoreBufferItem) {
        let notified = self.update_notify.notified();
        if let Some(item) = {
            let opt = self.buffer.inner().pop_item();
            opt
        } {
            item
        } else {
            notified.instrument_await("Wait For New Buffer Item").await;
            self.buffer
                .inner()
                .pop_item()
                .expect("should get the item because notified")
        }
    }

    pub(crate) fn truncate_buffer(&mut self, offset: TruncateOffset) {
        let mut inner = self.buffer.inner();
        let mut latest_offset: Option<ReaderTruncationOffsetType> = None;
        while let Some((epoch, item)) = inner.consumed_queue.back() {
            let epoch = *epoch;
            match item {
                LogStoreBufferItem::StreamChunk {
                    chunk_id,
                    flushed,
                    end_seq_id,
                    chunk,
                    ..
                } => {
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    let flushed = *flushed;
                    let end_seq_id = *end_seq_id;
                    if chunk_offset <= offset {
                        inner.row_count -= chunk.cardinality();
                        inner.consumed_queue.pop_back();
                        if flushed {
                            latest_offset = Some((epoch, Some(end_seq_id)));
                        }
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Flushed {
                    chunk_id,
                    end_seq_id,
                    ..
                } => {
                    let end_seq_id = *end_seq_id;
                    let chunk_offset = TruncateOffset::Chunk {
                        epoch,
                        chunk_id: *chunk_id,
                    };
                    if chunk_offset <= offset {
                        inner.consumed_queue.pop_back();
                        latest_offset = Some((epoch, Some(end_seq_id)));
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::Barrier { .. } => {
                    let chunk_offset = TruncateOffset::Barrier { epoch };
                    if chunk_offset <= offset {
                        inner.consumed_queue.pop_back();
                        latest_offset = Some((epoch, None));
                    } else {
                        break;
                    }
                }
                LogStoreBufferItem::UpdateVnodes(_) => {
                    inner.consumed_queue.pop_back();
                }
            }
        }
        if let Some(offset) = latest_offset {
            inner.add_truncate_offset(offset);
        }
    }

    pub(crate) fn truncate_historical(&mut self, epoch: u64) {
        let mut inner = self.buffer.inner();
        inner.add_truncate_offset((epoch, None));
    }

    pub(crate) fn rewind(&self) {
        self.buffer.inner().rewind()
    }
}

pub(crate) fn new_log_store_buffer(
    max_row_count: usize,
    metrics: KvLogStoreMetrics,
) -> (LogStoreBufferSender, LogStoreBufferReceiver) {
    let buffer = SharedMutex::new(LogStoreBufferInner {
        unconsumed_queue: VecDeque::new(),
        consumed_queue: VecDeque::new(),
        row_count: 0,
        max_row_count,
        truncation_list: VecDeque::new(),
        next_chunk_id: 0,
        metrics,
    });
    let update_notify = Arc::new(Notify::new());
    let (init_epoch_tx, init_epoch_rx) = oneshot::channel();
    let tx = LogStoreBufferSender {
        init_epoch_tx: Some(init_epoch_tx),
        buffer: buffer.clone(),
        update_notify: update_notify.clone(),
    };

    let rx = LogStoreBufferReceiver {
        init_epoch_rx: Some(init_epoch_rx),
        buffer,
        update_notify,
    };

    (tx, rx)
}
