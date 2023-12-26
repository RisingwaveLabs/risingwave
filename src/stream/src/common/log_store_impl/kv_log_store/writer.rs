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

use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::sink::log_store::{LogStoreResult, LogWriter};
use risingwave_hummock_sdk::table_watermark::{VnodeWatermark, WatermarkDirection};
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

    is_paused: watch::Sender<bool>,

    identity: String,
}

impl<LS: LocalStateStore> KvLogStoreWriter<LS> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: LS,
        serde: LogStoreRowSerde,
        tx: LogStoreBufferSender,
        metrics: KvLogStoreMetrics,
        is_paused: watch::Sender<bool>,
        identity: String,
    ) -> Self {
        Self {
            _table_id: table_id,
            seq_id: FIRST_SEQ_ID,
            state_store,
            serde,
            tx,
            metrics,
            is_paused,
            identity,
        }
    }
}

impl<LS: LocalStateStore> LogWriter for KvLogStoreWriter<LS> {
    async fn init(
        &mut self,
        epoch: EpochPair,
        pause_read_on_bootstrap: bool,
    ) -> LogStoreResult<()> {
        self.state_store
            .init(InitOptions::new_with_epoch(epoch))
            .await?;
        if pause_read_on_bootstrap {
            self.pause()?;
            info!("KvLogStore of {} paused on bootstrap", self.identity);
        }
        self.seq_id = FIRST_SEQ_ID;
        self.tx.init(epoch.curr);
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> LogStoreResult<()> {
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
            let mut vnode_bitmap_builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
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
            self.state_store.flush(Vec::new()).await?;

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
        for vnode in self.serde.vnodes().iter_vnodes() {
            let (key, value) = self.serde.serialize_barrier(epoch, vnode, is_checkpoint);
            flush_info.flush_one(key.estimated_size() + value.estimated_size());
            self.state_store.insert(key, value, None)?;
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
        flush_info.report(&self.metrics);
        let mut watermark = None;
        if let Some(truncation_offset) = self.tx.pop_truncation(epoch) {
            watermark = Some(VnodeWatermark::new(
                self.serde.vnodes().clone(),
                self.serde
                    .serialize_truncation_offset_watermark(truncation_offset),
            ));
        }
        self.state_store.flush(vec![]).await?;
        let watermark = watermark.into_iter().collect_vec();
        self.state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions::new(watermark, WatermarkDirection::Ascending),
        );
        self.tx.barrier(epoch, is_checkpoint, next_epoch);
        self.seq_id = FIRST_SEQ_ID;
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> LogStoreResult<()> {
        self.serde.update_vnode_bitmap(new_vnodes.clone());
        self.tx.update_vnode(self.state_store.epoch(), new_vnodes);
        Ok(())
    }

    fn pause(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is paused", self.identity);
        self.is_paused
            .send(true)
            .map_err(|_| anyhow!("unable to set pause"))
    }

    fn resume(&mut self) -> LogStoreResult<()> {
        info!("KvLogStore of {} is resumed", self.identity);
        self.is_paused
            .send(false)
            .map_err(|_| anyhow!("unable to set resume"))
    }
}
