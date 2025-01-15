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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::{LogStoreResult, LogWriter};
use risingwave_hummock_sdk::table_watermark::{
    VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_storage::store::{InitOptions, LocalStateStore, SealCurrentEpochOptions};
use tokio::sync::watch;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferSender;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::{
    FlushInfo, KvLogStoreMetrics, SeqIdType, FIRST_SEQ_ID,
};

pub struct KvLogStoreWriter<LS: LocalStateStore> {
    _table_id: TableId,

    seq_id: SeqIdType,

    state_store: LS,

    serde: LogStoreRowSerde,

    tx: LogStoreBufferSender,

    metrics: KvLogStoreMetrics,

    paused_notifier: watch::Sender<bool>,

    is_paused: bool,

    identity: String,
}

impl<LS: LocalStateStore> KvLogStoreWriter<LS> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: LS,
        serde: LogStoreRowSerde,
        tx: LogStoreBufferSender,
        metrics: KvLogStoreMetrics,
        paused_notifier: watch::Sender<bool>,
        identity: String,
    ) -> Self {
        Self {
            _table_id: table_id,
            seq_id: FIRST_SEQ_ID,
            state_store,
            serde,
            tx,
            metrics,
            paused_notifier,
            identity,
            is_paused: false,
        }
    }
}

impl<LS: LocalStateStore> LogWriter for KvLogStoreWriter<LS> {
    async fn init(
        &mut self,
        epoch: EpochPair,
        pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        self.state_store.init(InitOptions::new(epoch)).await?;
        if pause_read_on_bootstrap {
            self.pause()?;
            info!("KvLogStore of {} paused on bootstrap", self.identity);
        }
        self.seq_id = FIRST_SEQ_ID;
        self.tx.init(epoch.curr);
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
        // No data is expected when the stream is paused.
        assert!(!self.is_paused);

        if chunk.cardinality() == 0 {
            return Ok(());
        }
        let epoch = self.state_store.epoch();
        let start_seq_id = self.seq_id;
        self.seq_id += chunk.cardinality() as SeqIdType;
        let end_seq_id = self.seq_id - 1;
        if let Some(chunk) = self
            .tx
            .try_add_stream_chunk(epoch, chunk, start_seq_id, end_seq_id)
        {
            // When enter this branch, the chunk cannot be added directly, and should be add to
            // state store and flush
            let mut vnode_bitmap_builder = BitmapBuilder::zeroed(self.serde.vnodes().len());
            let mut flush_info = FlushInfo::new();
            for (i, (op, row)) in chunk.rows().enumerate() {
                let seq_id = start_seq_id + (i as SeqIdType);
                assert!(seq_id <= end_seq_id);
                let (vnode, key, value) = self.serde.serialize_data_row(epoch, seq_id, op, row);
                vnode_bitmap_builder.set(vnode.to_index(), true);
                flush_info.flush_one(key.estimated_size() + value.estimated_size());
                self.state_store.insert(key, value, None)?;
            }
            flush_info.report(&self.metrics);
            self.state_store.flush().await?;

            let vnode_bitmap = vnode_bitmap_builder.finish();
            self.tx
                .add_flushed(epoch, start_seq_id, end_seq_id, vnode_bitmap);
        }
        Ok(())
    }

    async fn flush_current_epoch(
        &mut self,
        next_epoch: u64,
        is_checkpoint: bool,
    ) -> LogStoreResult<()> {
        let epoch = self.state_store.epoch();
        let mut flush_info = FlushInfo::new();

        // When the stream is paused, donot flush barrier to ensure there is no dirty data in state store.
        // Besides, barrier on a paused stream is useless in log store because it won't change the log store state.
        if !self.is_paused {
            for vnode in self.serde.vnodes().iter_vnodes() {
                let (key, value) = self.serde.serialize_barrier(epoch, vnode, is_checkpoint);
                flush_info.flush_one(key.estimated_size() + value.estimated_size());
                self.state_store.insert(key, value, None)?;
            }
        }
        self.tx
            .flush_all_unflushed(|chunk, epoch, start_seq_id, end_seq_id| {
                for (i, (op, row)) in chunk.rows().enumerate() {
                    let seq_id = start_seq_id + (i as SeqIdType);
                    assert!(seq_id <= end_seq_id);
                    let (_, key, value) = self.serde.serialize_data_row(epoch, seq_id, op, row);
                    flush_info.flush_one(key.estimated_size() + value.estimated_size());
                    self.state_store.insert(key, value, None)?;
                }
                Ok(())
            })?;

        // No data is expected when the stream is paused.
        if self.is_paused {
            assert_eq!(flush_info.flush_count, 0);
            assert_eq!(flush_info.flush_size, 0);
            assert!(!self.state_store.is_dirty());
        }
        flush_info.report(&self.metrics);

        let watermark = self.tx.pop_truncation(epoch).map(|truncation_offset| {
            VnodeWatermark::new(
                self.serde.vnodes().clone(),
                self.serde
                    .serialize_truncation_offset_watermark(truncation_offset),
            )
        });
        self.state_store.flush().await?;
        let watermark = watermark.into_iter().collect_vec();
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
        self.tx.barrier(epoch, is_checkpoint, next_epoch);
        self.seq_id = FIRST_SEQ_ID;
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> LogStoreResult<()> {
        self.serde.update_vnode_bitmap(new_vnodes.clone());
        self.state_store.update_vnode_bitmap(new_vnodes.clone());
        self.tx.update_vnode(self.state_store.epoch(), new_vnodes);
        Ok(())
    }

    fn pause(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is paused", self.identity);
        assert!(!self.is_paused);
        self.is_paused = true;
        self.paused_notifier
            .send(true)
            .map_err(|_| anyhow!("unable to set pause"))
    }

    fn resume(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is resumed", self.identity);
        assert!(self.is_paused);
        self.is_paused = false;
        self.paused_notifier
            .send(false)
            .map_err(|_| anyhow!("unable to set resume"))
    }
}
