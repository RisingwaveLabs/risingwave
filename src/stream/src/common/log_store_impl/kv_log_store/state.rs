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

use std::future::Future;
use std::sync::Arc;

use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::LogStoreResult;
use risingwave_hummock_sdk::table_watermark::{
    VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_storage::store::{
    InitOptions, LocalStateStore, SealCurrentEpochOptions, StateStoreRead,
};

use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::{
    FlushInfo, ReaderTruncationOffsetType, SeqIdType,
};

pub(crate) struct LogStoreReadState<S: StateStoreRead> {
    pub(super) table_id: TableId,
    pub(super) state_store: Arc<S>,
    pub(super) serde: LogStoreRowSerde,
}

impl<S: StateStoreRead> LogStoreReadState<S> {
    pub(crate) fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        self.serde.update_vnode_bitmap(vnode_bitmap);
    }
}

pub(crate) struct LogStoreWriteState<S: LocalStateStore> {
    state_store: S,
    serde: LogStoreRowSerde,
    epoch: Option<EpochPair>,

    on_post_seal: bool,
}

pub(crate) fn new_log_store_state<S: LocalStateStore>(
    table_id: TableId,
    state_store: S,
    serde: LogStoreRowSerde,
) -> (
    LogStoreReadState<S::FlushedSnapshotReader>,
    LogStoreWriteState<S>,
) {
    let flushed_reader = state_store.new_flushed_snapshot_reader();
    (
        LogStoreReadState {
            table_id,
            state_store: Arc::new(flushed_reader),
            serde: serde.clone(),
        },
        LogStoreWriteState {
            state_store,
            serde,
            epoch: None,
            on_post_seal: false,
        },
    )
}

impl<S: LocalStateStore> LogStoreWriteState<S> {
    pub(crate) async fn init(&mut self, epoch: EpochPair) -> LogStoreResult<()> {
        self.state_store.init(InitOptions::new(epoch)).await?;
        assert_eq!(self.epoch.replace(epoch), None, "cannot init for twice");
        Ok(())
    }

    pub(crate) fn epoch(&self) -> EpochPair {
        self.epoch.expect("should have init")
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.state_store.is_dirty()
    }

    pub(crate) fn start_writer(&mut self, record_vnode: bool) -> LogStoreStateWriter<'_, S> {
        let written_vnodes = if record_vnode {
            Some(BitmapBuilder::zeroed(self.serde.vnodes().len()))
        } else {
            None
        };
        LogStoreStateWriter {
            inner: self,
            flush_info: FlushInfo::new(),
            written_vnodes,
        }
    }

    pub(crate) fn seal_current_epoch(
        &mut self,
        next_epoch: u64,
        truncate_offset: Option<ReaderTruncationOffsetType>,
    ) -> LogStorePostSealCurrentEpoch<'_, S> {
        assert!(!self.on_post_seal);
        let watermark = truncate_offset
            .map(|truncation_offset| {
                vec![VnodeWatermark::new(
                    self.serde.vnodes().clone(),
                    self.serde
                        .serialize_truncation_offset_watermark(truncation_offset),
                )]
            })
            .unwrap_or_default();
        self.state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions {
                table_watermarks: Some((
                    WatermarkDirection::Ascending,
                    watermark,
                    WatermarkSerdeType::PkPrefix,
                )),
                switch_op_consistency_level: None,
            },
        );
        let epoch = self.epoch.as_mut().expect("should have init");
        epoch.prev = epoch.curr;
        epoch.curr = next_epoch;

        self.on_post_seal = true;
        LogStorePostSealCurrentEpoch { inner: self }
    }
}

#[must_use]
pub(crate) struct LogStorePostSealCurrentEpoch<'a, S: LocalStateStore> {
    inner: &'a mut LogStoreWriteState<S>,
}

impl<'a, S: LocalStateStore> LogStorePostSealCurrentEpoch<'a, S> {
    pub(crate) async fn post_yield_barrier(
        self,
        new_vnodes: Option<Arc<Bitmap>>,
    ) -> LogStoreResult<()> {
        if let Some(new_vnodes) = new_vnodes {
            self.inner.serde.update_vnode_bitmap(new_vnodes.clone());
            self.inner
                .state_store
                .update_vnode_bitmap(new_vnodes.clone())
                .await?;
        }
        self.inner.on_post_seal = false;
        Ok(())
    }
}

pub(crate) type LogStoreStateWriteChunkFuture<S: LocalStateStore> =
    impl Future<Output = (LogStoreWriteState<S>, LogStoreResult<(FlushInfo, Bitmap)>)> + 'static;

impl<S: LocalStateStore> LogStoreWriteState<S> {
    pub(crate) fn into_write_chunk_future(
        mut self,
        chunk: StreamChunk,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> LogStoreStateWriteChunkFuture<S> {
        async move {
            let result = try {
                let mut writer = self.start_writer(true);
                writer.write_chunk(&chunk, epoch, start_seq_id, end_seq_id)?;
                let (flush_info, bitmap) = writer.finish().await?;
                (
                    flush_info,
                    bitmap.expect("should exist when pass true when start_writer"),
                )
            };
            (self, result)
        }
    }
}

pub(crate) struct LogStoreStateWriter<'a, S: LocalStateStore> {
    inner: &'a mut LogStoreWriteState<S>,
    flush_info: FlushInfo,
    written_vnodes: Option<BitmapBuilder>,
}

impl<'a, S: LocalStateStore> LogStoreStateWriter<'a, S> {
    pub(crate) fn write_chunk(
        &mut self,
        chunk: &StreamChunk,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> LogStoreResult<()> {
        for (i, (op, row)) in chunk.rows().enumerate() {
            let seq_id = start_seq_id + (i as SeqIdType);
            assert!(seq_id <= end_seq_id);
            let (vnode, key, value) = self.inner.serde.serialize_data_row(epoch, seq_id, op, row);
            if let Some(written_vnodes) = &mut self.written_vnodes {
                written_vnodes.set(vnode.to_index(), true);
            }
            self.flush_info
                .flush_one(key.estimated_size() + value.estimated_size());
            self.inner.state_store.insert(key, value, None)?;
        }
        Ok(())
    }

    pub(crate) fn write_barrier(&mut self, epoch: u64, is_checkpoint: bool) -> LogStoreResult<()> {
        for vnode in self.inner.serde.vnodes().iter_vnodes() {
            let (key, value) = self
                .inner
                .serde
                .serialize_barrier(epoch, vnode, is_checkpoint);
            self.flush_info
                .flush_one(key.estimated_size() + value.estimated_size());
            self.inner.state_store.insert(key, value, None)?;
        }
        Ok(())
    }

    pub(crate) async fn finish(self) -> LogStoreResult<(FlushInfo, Option<Bitmap>)> {
        self.inner.state_store.flush().await?;
        Ok((
            self.flush_info,
            self.written_vnodes.map(|vnodes| vnodes.finish()),
        ))
    }
}
