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

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::{is_max_epoch, MAX_SPILL_TIMES};
use risingwave_hummock_sdk::key::{is_empty_key_range, vnode_range, TableKey, TableKeyRange};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch};
use tracing::{warn, Instrument};

use super::version::{StagingData, VersionUpdate};
use crate::error::StorageResult;
use crate::hummock::event_handler::hummock_event_handler::HummockEventSender;
use crate::hummock::event_handler::{HummockEvent, HummockReadVersionRef, LocalInstanceGuard};
use crate::hummock::iterator::{
    ConcatIteratorInner, Forward, HummockIteratorUnion, MergeIterator, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator,
};
use crate::hummock::store::version::{read_filter_for_version, HummockVersionReader};
use crate::hummock::utils::{
    do_delete_sanity_check, do_insert_sanity_check, do_update_sanity_check, wait_for_epoch,
    ENABLE_SANITY_CHECK,
};
use crate::hummock::write_limiter::WriteLimiterRef;
use crate::hummock::{MemoryLimiter, SstableIterator};
use crate::mem_table::{KeyOp, MemTable, MemTableHummockIterator};
use crate::monitor::{HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic};
use crate::panic_store::PanicStateStoreIter;
use crate::storage_value::StorageValue;
use crate::store::*;

/// `LocalHummockStorage` is a handle for a state table shard to access data from and write data to
/// the hummock state backend. It is created via `HummockStorage::new_local`.

pub struct LocalHummockStorage {
    mem_table: MemTable,

    spill_offset: u16,
    epoch: Option<u64>,

    table_id: TableId,
    op_consistency_level: OpConsistencyLevel,
    table_option: TableOption,

    instance_guard: LocalInstanceGuard,

    /// Read handle.
    read_version: HummockReadVersionRef,

    /// This indicates that this `LocalHummockStorage` replicates another `LocalHummockStorage`.
    /// It's used by executors in different CNs to synchronize states.
    ///
    /// Within `LocalHummockStorage` we use this flag to avoid uploading local state to be
    /// persisted, so we won't have duplicate data.
    ///
    /// This also handles a corner case where an executor doing replication
    /// is scheduled to the same CN as its Upstream executor.
    /// In that case, we use this flag to avoid reading the same data twice,
    /// by ignoring the replicated `ReadVersion`.
    is_replicated: bool,

    /// Event sender.
    event_sender: HummockEventSender,

    memory_limiter: Arc<MemoryLimiter>,

    hummock_version_reader: HummockVersionReader,

    stats: Arc<HummockStateStoreMetrics>,

    write_limiter: WriteLimiterRef,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,

    mem_table_spill_threshold: usize,
}

impl LocalHummockStorage {
    /// See `HummockReadVersion::update` for more details.
    pub fn update(&self, info: VersionUpdate) {
        self.read_version.write().update(info)
    }

    pub async fn get_inner(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        let table_key_range = (
            Bound::Included(table_key.clone()),
            Bound::Included(table_key.clone()),
        );

        let (table_key_range, read_snapshot) = read_filter_for_version(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        if is_empty_key_range(&table_key_range) {
            return Ok(None);
        }

        self.hummock_version_reader
            .get(table_key, epoch, read_options, read_snapshot)
            .await
    }

    pub async fn wait_for_epoch(&self, wait_epoch: u64) -> StorageResult<()> {
        wait_for_epoch(&self.version_update_notifier_tx, wait_epoch).await
    }

    pub async fn iter_flushed(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let (table_key_range, read_snapshot) = read_filter_for_version(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        let table_key_range = table_key_range;

        self.hummock_version_reader
            .iter(table_key_range, epoch, read_options, read_snapshot)
            .await
    }

    fn mem_table_iter(&self) -> MemTableHummockIterator<'_> {
        MemTableHummockIterator::new(
            &self.mem_table.buffer,
            EpochWithGap::new(self.epoch(), self.spill_offset),
            self.table_id,
        )
    }

    pub async fn iter_all(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> StorageResult<LocalHummockStorageIterator<'_>> {
        let (table_key_range, read_snapshot) = read_filter_for_version(
            epoch,
            read_options.table_id,
            table_key_range,
            &self.read_version,
        )?;

        self.hummock_version_reader
            .iter_with_memtable(
                table_key_range,
                epoch,
                read_options,
                read_snapshot,
                self.mem_table_iter(),
            )
            .await
    }

    pub async fn may_exist_inner(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<bool> {
        if self.mem_table.iter(key_range.clone()).next().is_some() {
            return Ok(true);
        }

        let (key_range, read_snapshot) = read_filter_for_version(
            HummockEpoch::MAX, // Use MAX epoch to make sure we read from latest
            read_options.table_id,
            key_range,
            &self.read_version,
        )?;

        self.hummock_version_reader
            .may_exist(key_range, read_options, read_snapshot)
            .await
    }
}

impl StateStoreRead for LocalHummockStorage {
    type ChangeLogIter = PanicStateStoreIter<StateStoreReadLogItem>;
    type Iter = HummockStorageIterator;

    fn get(
        &self,
        key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<Bytes>>> + '_ {
        assert!(epoch <= self.epoch());
        self.get_inner(key, epoch, read_options)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
        assert!(epoch <= self.epoch());
        self.iter_flushed(key_range, epoch, read_options)
            .instrument(tracing::trace_span!("hummock_iter"))
    }

    async fn iter_log(
        &self,
        _epoch_range: (u64, u64),
        _key_range: TableKeyRange,
        _options: ReadLogOptions,
    ) -> StorageResult<Self::ChangeLogIter> {
        unimplemented!()
    }
}

impl LocalStateStore for LocalHummockStorage {
    type Iter<'a> = LocalHummockStorageIterator<'a>;

    fn may_exist(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<bool>> + Send + '_ {
        self.may_exist_inner(key_range, read_options)
    }

    async fn get(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        match self.mem_table.buffer.get(&key) {
            None => self.get_inner(key, self.epoch(), read_options).await,
            Some(op) => match op {
                KeyOp::Insert(value) | KeyOp::Update((_, value)) => Ok(Some(value.clone())),
                KeyOp::Delete(_) => Ok(None),
            },
        }
    }

    async fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<Self::Iter<'_>> {
        let (l_vnode_inclusive, r_vnode_exclusive) = vnode_range(&key_range);
        assert_eq!(
            r_vnode_exclusive - l_vnode_inclusive,
            1,
            "read range {:?} for table {} iter contains more than one vnode",
            key_range,
            read_options.table_id
        );
        self.iter_all(key_range.clone(), self.epoch(), read_options)
            .await
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, old_val, new_val)?,
        };

        Ok(())
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        self.mem_table.delete(key, old_val)?;

        Ok(())
    }

    async fn flush(&mut self) -> StorageResult<usize> {
        let buffer = self.mem_table.drain().into_parts();
        let mut kv_pairs = Vec::with_capacity(buffer.len());
        for (key, key_op) in buffer {
            match key_op {
                // Currently, some executors do not strictly comply with these semantics. As
                // a workaround you may call disable the check by initializing the
                // state store with `is_consistent_op=false`.
                KeyOp::Insert(value) => {
                    if ENABLE_SANITY_CHECK {
                        do_insert_sanity_check(
                            &key,
                            &value,
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(value)));
                }
                KeyOp::Delete(old_value) => {
                    if ENABLE_SANITY_CHECK {
                        do_delete_sanity_check(
                            &key,
                            &old_value,
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_delete()));
                }
                KeyOp::Update((old_value, new_value)) => {
                    if ENABLE_SANITY_CHECK {
                        do_update_sanity_check(
                            &key,
                            &old_value,
                            &new_value,
                            self,
                            self.epoch(),
                            self.table_id,
                            self.table_option,
                            &self.op_consistency_level,
                        )
                        .await?;
                    }
                    kv_pairs.push((key, StorageValue::new_put(new_value)));
                }
            }
        }
        self.flush_inner(
            kv_pairs,
            WriteOptions {
                epoch: self.epoch(),
                table_id: self.table_id,
            },
        )
        .await
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        if self.mem_table_spill_threshold != 0
            && self.mem_table.kv_size.size() > self.mem_table_spill_threshold
        {
            if self.spill_offset < MAX_SPILL_TIMES {
                let table_id_label = self.table_id.table_id().to_string();
                self.flush().await?;
                self.stats
                    .mem_table_spill_counts
                    .with_label_values(&[table_id_label.as_str()])
                    .inc();
            } else {
                tracing::warn!("No mem table spill occurs, the gap epoch exceeds available range.");
            }
        }

        Ok(())
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        let epoch = options.epoch;
        if self.is_replicated {
            self.wait_for_epoch(epoch.prev).await?;
        }
        assert!(
            self.epoch.replace(epoch.curr).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
        Ok(())
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, mut opts: SealCurrentEpochOptions) {
        assert!(!self.is_dirty());
        if let Some(new_level) = &opts.switch_op_consistency_level {
            self.mem_table.op_consistency_level.update(new_level);
            self.op_consistency_level.update(new_level);
        }
        let prev_epoch = self
            .epoch
            .replace(next_epoch)
            .expect("should have init epoch before seal the first epoch");
        self.spill_offset = 0;
        assert!(
            next_epoch > prev_epoch,
            "new epoch {} should be greater than current epoch: {}",
            next_epoch,
            prev_epoch
        );
        if let Some((direction, watermarks)) = &mut opts.table_watermarks {
            let mut read_version = self.read_version.write();
            read_version.filter_regress_watermarks(watermarks);
            if !watermarks.is_empty() {
                read_version.update(VersionUpdate::NewTableWatermark {
                    direction: *direction,
                    epoch: prev_epoch,
                    vnode_watermarks: watermarks.clone(),
                });
            }
        }
        self.event_sender
            .send(HummockEvent::LocalSealEpoch {
                instance_id: self.instance_id(),
                table_id: self.table_id,
                epoch: prev_epoch,
                opts,
            })
            .expect("should be able to send")
    }

    fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        let mut read_version = self.read_version.write();
        assert!(read_version.staging().is_empty(), "There is uncommitted staging data in read version table_id {:?} instance_id {:?} on vnode bitmap update",
            self.table_id(), self.instance_id()
        );
        read_version.update_vnode_bitmap(vnodes)
    }

    async fn wait_epoch(&self, epoch: HummockEpoch) -> StorageResult<()> {
        assert!(!is_max_epoch(epoch), "epoch should not be MAX EPOCH");
        self.wait_for_epoch(epoch).await
    }
}

impl LocalHummockStorage {
    async fn flush_inner(
        &mut self,
        kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        let epoch = write_options.epoch;
        let table_id = write_options.table_id;

        let table_id_label = table_id.to_string();
        self.stats
            .write_batch_tuple_counts
            .with_label_values(&[table_id_label.as_str()])
            .inc_by(kv_pairs.len() as _);
        let timer = self
            .stats
            .write_batch_duration
            .with_label_values(&[table_id_label.as_str()])
            .start_timer();

        let imm_size = if !kv_pairs.is_empty() {
            let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
            let size = SharedBufferBatch::measure_batch_size(&sorted_items);
            self.write_limiter.wait_permission(self.table_id).await;
            let limiter = self.memory_limiter.as_ref();
            let tracker = if let Some(tracker) = limiter.try_require_memory(size as u64) {
                tracker
            } else {
                warn!(
                    "blocked at requiring memory: {}, current {}",
                    size,
                    limiter.get_memory_usage()
                );
                self.event_sender
                    .send(HummockEvent::BufferMayFlush)
                    .expect("should be able to send");
                let tracker = limiter
                    .require_memory(size as u64)
                    .verbose_instrument_await("hummock_require_memory")
                    .await;
                warn!(
                    "successfully requiring memory: {}, current {}",
                    size,
                    limiter.get_memory_usage()
                );
                tracker
            };

            let instance_id = self.instance_guard.instance_id;
            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                self.spill_offset,
                sorted_items,
                size,
                table_id,
                instance_id,
                Some(tracker),
            );
            self.spill_offset += 1;
            let imm_size = imm.size();
            self.update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            // insert imm to uploader
            if !self.is_replicated {
                self.event_sender
                    .send(HummockEvent::ImmToUploader(imm))
                    .unwrap();
            }
            imm_size
        } else {
            0
        };

        timer.observe_duration();

        self.stats
            .write_batch_size
            .with_label_values(&[table_id_label.as_str()])
            .observe(imm_size as _);
        Ok(imm_size)
    }
}

impl LocalHummockStorage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_guard: LocalInstanceGuard,
        read_version: HummockReadVersionRef,
        hummock_version_reader: HummockVersionReader,
        event_sender: HummockEventSender,
        memory_limiter: Arc<MemoryLimiter>,
        write_limiter: WriteLimiterRef,
        option: NewLocalOptions,
        version_update_notifier_tx: Arc<tokio::sync::watch::Sender<HummockEpoch>>,
        mem_table_spill_threshold: usize,
    ) -> Self {
        let stats = hummock_version_reader.stats().clone();
        Self {
            mem_table: MemTable::new(option.op_consistency_level.clone()),
            spill_offset: 0,
            epoch: None,
            table_id: option.table_id,
            op_consistency_level: option.op_consistency_level,
            table_option: option.table_option,
            is_replicated: option.is_replicated,
            instance_guard,
            read_version,
            event_sender,
            memory_limiter,
            hummock_version_reader,
            stats,
            write_limiter,
            version_update_notifier_tx,
            mem_table_spill_threshold,
        }
    }

    /// See `HummockReadVersion::update` for more details.
    pub fn read_version(&self) -> HummockReadVersionRef {
        self.read_version.clone()
    }

    pub fn table_id(&self) -> TableId {
        self.instance_guard.table_id
    }

    pub fn instance_id(&self) -> u64 {
        self.instance_guard.instance_id
    }
}

pub type StagingDataIterator = MergeIterator<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
pub type HummockStorageIteratorPayloadInner<'a> = MergeIterator<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        SstableIterator,
        ConcatIteratorInner<SstableIterator>,
        MemTableHummockIterator<'a>,
    >,
>;

pub type HummockStorageIterator = HummockStorageIteratorInner<'static>;
pub type LocalHummockStorageIterator<'a> = HummockStorageIteratorInner<'a>;

pub struct HummockStorageIteratorInner<'a> {
    inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
    initial_read: bool,
    stats_guard: IterLocalMetricsGuard,
}

impl<'a> StateStoreIter for HummockStorageIteratorInner<'a> {
    async fn try_next<'b>(&'b mut self) -> StorageResult<Option<StateStoreIterItemRef<'b>>> {
        let iter = &mut self.inner;
        if !self.initial_read {
            self.initial_read = true;
        } else {
            iter.next().await?;
        }

        if iter.is_valid() {
            Ok(Some((iter.key(), iter.value())))
        } else {
            Ok(None)
        }
    }
}

impl<'a> HummockStorageIteratorInner<'a> {
    pub fn new(
        inner: UserIterator<HummockStorageIteratorPayloadInner<'a>>,
        metrics: Arc<HummockStateStoreMetrics>,
        table_id: TableId,
        local_stats: StoreLocalStatistic,
    ) -> Self {
        Self {
            inner,
            initial_read: false,
            stats_guard: IterLocalMetricsGuard::new(metrics, table_id, local_stats),
        }
    }
}

impl<'a> Drop for HummockStorageIteratorInner<'a> {
    fn drop(&mut self) {
        self.inner
            .collect_local_statistic(&mut self.stats_guard.local_stats);
    }
}
