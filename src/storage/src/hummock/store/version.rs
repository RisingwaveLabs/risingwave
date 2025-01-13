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

use std::cmp::Ordering;
use std::collections::vec_deque::VecDeque;
use std::collections::HashMap;
use std::ops::Bound::Included;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::future::try_join_all;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::MAX_SPILL_TIMES;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, FullKey, TableKey, TableKeyRange, UserKey,
};
use risingwave_hummock_sdk::key_range::KeyRangeCommon;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarksIndex, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::LevelType;
use sync_point::sync_point;
use tracing::warn;

use crate::error::StorageResult;
use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::change_log::ChangeLogIterator;
use crate::hummock::iterator::{
    BackwardUserIterator, HummockIterator, IteratorFactory, MergeIterator, UserIterator,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::sstable::{SstableIteratorReadOptions, SstableIteratorType};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::{
    filter_single_sst, prune_nonoverlapping_ssts, prune_overlapping_ssts, range_overlap,
    search_sst_idx,
};
use crate::hummock::{
    get_from_batch, get_from_sstable_info, hit_sstable_bloom_filter, BackwardIteratorFactory,
    ForwardIteratorFactory, HummockError, HummockResult, HummockStorageIterator,
    HummockStorageIteratorInner, HummockStorageRevIteratorInner, ReadVersionTuple, Sstable,
    SstableIterator,
};
use crate::mem_table::{
    ImmId, ImmutableMemtable, MemTableHummockIterator, MemTableHummockRevIterator,
};
use crate::monitor::{
    GetLocalMetricsGuard, HummockStateStoreMetrics, IterLocalMetricsGuard, StoreLocalStatistic,
};
use crate::store::{gen_min_epoch, ReadLogOptions, ReadOptions};

pub type CommittedVersion = PinnedVersion;

/// Data not committed to Hummock. There are two types of staging data:
/// - Immutable memtable: data that has been written into local state store but not persisted.
/// - Uncommitted SST: data that has been uploaded to persistent storage but not committed to
///   hummock version.

#[derive(Clone, Debug, PartialEq)]
pub struct StagingSstableInfo {
    // newer data comes first
    sstable_infos: Vec<LocalSstableInfo>,
    old_value_sstable_infos: Vec<LocalSstableInfo>,
    /// Epochs whose data are included in the Sstable. The newer epoch comes first.
    /// The field must not be empty.
    epochs: Vec<HummockEpoch>,
    // newer data at the front
    imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
    imm_size: usize,
}

impl StagingSstableInfo {
    pub fn new(
        sstable_infos: Vec<LocalSstableInfo>,
        old_value_sstable_infos: Vec<LocalSstableInfo>,
        epochs: Vec<HummockEpoch>,
        imm_ids: HashMap<LocalInstanceId, Vec<ImmId>>,
        imm_size: usize,
    ) -> Self {
        // the epochs are sorted from higher epoch to lower epoch
        assert!(epochs.is_sorted_by(|epoch1, epoch2| epoch2 <= epoch1));
        Self {
            sstable_infos,
            old_value_sstable_infos,
            epochs,
            imm_ids,
            imm_size,
        }
    }

    pub fn sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.sstable_infos
    }

    pub fn old_value_sstable_infos(&self) -> &Vec<LocalSstableInfo> {
        &self.old_value_sstable_infos
    }

    pub fn imm_size(&self) -> usize {
        self.imm_size
    }

    pub fn epochs(&self) -> &Vec<HummockEpoch> {
        &self.epochs
    }

    pub fn imm_ids(&self) -> &HashMap<LocalInstanceId, Vec<ImmId>> {
        &self.imm_ids
    }
}

#[derive(Clone)]
pub enum StagingData {
    ImmMem(ImmutableMemtable),
    Sst(Arc<StagingSstableInfo>),
}

pub enum VersionUpdate {
    /// a new staging data entry will be added.
    Staging(StagingData),
    CommittedSnapshot(CommittedVersion),
    NewTableWatermark {
        direction: WatermarkDirection,
        epoch: HummockEpoch,
        vnode_watermarks: Vec<VnodeWatermark>,
        watermark_type: WatermarkSerdeType,
    },
}

#[derive(Clone)]
pub struct StagingVersion {
    // newer data comes first
    // Note: Currently, building imm and writing to staging version is not atomic, and therefore
    // imm of smaller batch id may be added later than one with greater batch id
    pub imm: VecDeque<ImmutableMemtable>,

    // newer data comes first
    pub sst: VecDeque<Arc<StagingSstableInfo>>,
}

impl StagingVersion {
    /// Get the overlapping `imm`s and `sst`s that overlap respectively with `table_key_range` and
    /// the user key range derived from `table_id`, `epoch` and `table_key_range`.
    pub fn prune_overlap<'a>(
        &'a self,
        max_epoch_inclusive: HummockEpoch,
        table_id: TableId,
        table_key_range: &'a TableKeyRange,
    ) -> (
        impl Iterator<Item = &'a ImmutableMemtable> + 'a,
        impl Iterator<Item = &'a SstableInfo> + 'a,
    ) {
        let (ref left, ref right) = table_key_range;
        let left = left.as_ref().map(|key| TableKey(key.0.as_ref()));
        let right = right.as_ref().map(|key| TableKey(key.0.as_ref()));
        let overlapped_imms = self.imm.iter().filter(move |imm| {
            // retain imm which is overlapped with (min_epoch_exclusive, max_epoch_inclusive]
            imm.min_epoch() <= max_epoch_inclusive
                && imm.table_id == table_id
                && range_overlap(
                    &(left, right),
                    &imm.start_table_key(),
                    Included(&imm.end_table_key()),
                )
        });

        // TODO: Remove duplicate sst based on sst id
        let overlapped_ssts = self
            .sst
            .iter()
            .filter(move |staging_sst| {
                let sst_max_epoch = *staging_sst.epochs.last().expect("epochs not empty");
                sst_max_epoch <= max_epoch_inclusive
            })
            .flat_map(move |staging_sst| {
                // TODO: sstable info should be concat-able after each streaming table owns a read
                // version. May use concat sstable iter instead in some cases.
                staging_sst
                    .sstable_infos
                    .iter()
                    .map(|sstable| &sstable.sst_info)
                    .filter(move |sstable: &&SstableInfo| {
                        filter_single_sst(sstable, table_id, table_key_range)
                    })
            });
        (overlapped_imms, overlapped_ssts)
    }

    pub fn is_empty(&self) -> bool {
        self.imm.is_empty() && self.sst.is_empty()
    }
}

#[derive(Clone)]
/// A container of information required for reading from hummock.
pub struct HummockReadVersion {
    table_id: TableId,
    instance_id: LocalInstanceId,

    /// Local version for staging data.
    staging: StagingVersion,

    /// Remote version for committed data.
    committed: CommittedVersion,

    /// Indicate if this is replicated. If it is, we should ignore it during
    /// global state store read, to avoid duplicated results.
    /// Otherwise for local state store, it is fine, see we will see the
    /// `ReadVersion` just for that local state store.
    is_replicated: bool,

    table_watermarks: Option<TableWatermarksIndex>,

    // Vnode bitmap corresponding to the read version
    // It will be initialized after local state store init
    vnodes: Arc<Bitmap>,
}

impl HummockReadVersion {
    pub fn new_with_replication_option(
        table_id: TableId,
        instance_id: LocalInstanceId,
        committed_version: CommittedVersion,
        is_replicated: bool,
        vnodes: Arc<Bitmap>,
    ) -> Self {
        // before build `HummockReadVersion`, we need to get the a initial version which obtained
        // from meta. want this initialization after version is initialized (now with
        // notification), so add a assert condition to guarantee correct initialization order
        assert!(committed_version.is_valid());
        Self {
            table_id,
            instance_id,
            table_watermarks: {
                match committed_version.table_watermarks.get(&table_id) {
                    Some(table_watermarks) => match table_watermarks.watermark_type {
                        WatermarkSerdeType::PkPrefix => Some(TableWatermarksIndex::new_committed(
                            table_watermarks.clone(),
                            committed_version
                                .state_table_info
                                .info()
                                .get(&table_id)
                                .expect("should exist")
                                .committed_epoch,
                        )),

                        WatermarkSerdeType::NonPkPrefix => None, /* do not fill the non-pk prefix watermark to index */
                    },
                    None => None,
                }
            },
            staging: StagingVersion {
                imm: VecDeque::default(),
                sst: VecDeque::default(),
            },

            committed: committed_version,

            is_replicated,
            vnodes,
        }
    }

    pub fn new(
        table_id: TableId,
        instance_id: LocalInstanceId,
        committed_version: CommittedVersion,
        vnodes: Arc<Bitmap>,
    ) -> Self {
        Self::new_with_replication_option(table_id, instance_id, committed_version, false, vnodes)
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Updates the read version with `VersionUpdate`.
    /// There will be three data types to be processed
    /// `VersionUpdate::Staging`
    ///     - `StagingData::ImmMem` -> Insert into memory's `staging_imm`
    ///     - `StagingData::Sst` -> Update the sst to memory's `staging_sst` and remove the
    ///       corresponding `staging_imms` according to the `batch_id`
    /// `VersionUpdate::CommittedDelta` -> Unimplemented yet
    /// `VersionUpdate::CommittedSnapshot` -> Update `committed_version` , and clean up related
    /// `staging_sst` and `staging_imm` in memory according to epoch
    pub fn update(&mut self, info: VersionUpdate) {
        match info {
            VersionUpdate::Staging(staging) => match staging {
                // TODO: add a check to ensure that the added batch id of added imm is greater than
                // the batch id of imm at the front
                StagingData::ImmMem(imm) => {
                    if let Some(item) = self.staging.imm.front() {
                        // check batch_id order from newest to old
                        debug_assert!(item.batch_id() < imm.batch_id());
                    }

                    self.staging.imm.push_front(imm)
                }
                StagingData::Sst(staging_sst_ref) => {
                    let Some(imms) = staging_sst_ref.imm_ids.get(&self.instance_id) else {
                        warn!(
                            instance_id = self.instance_id,
                            "no related imm in sst input"
                        );
                        return;
                    };

                    // old data comes first
                    for imm_id in imms.iter().rev() {
                        let check_err = match self.staging.imm.pop_back() {
                            None => Some("empty".to_owned()),
                            Some(prev_imm_id) => {
                                if prev_imm_id.batch_id() == *imm_id {
                                    None
                                } else {
                                    Some(format!(
                                        "miss match id {} {}",
                                        prev_imm_id.batch_id(),
                                        *imm_id
                                    ))
                                }
                            }
                        };
                        assert!(
                            check_err.is_none(),
                            "should be valid staging_sst.size {},
                                    staging_sst.imm_ids {:?},
                                    staging_sst.epochs {:?},
                                    local_imm_ids {:?},
                                    instance_id {}
                                    check_err {:?}",
                            staging_sst_ref.imm_size,
                            staging_sst_ref.imm_ids,
                            staging_sst_ref.epochs,
                            self.staging
                                .imm
                                .iter()
                                .map(|imm| imm.batch_id())
                                .collect_vec(),
                            self.instance_id,
                            check_err
                        );
                    }

                    self.staging.sst.push_front(staging_sst_ref);
                }
            },

            VersionUpdate::CommittedSnapshot(committed_version) => {
                if let Some(info) = committed_version
                    .state_table_info
                    .info()
                    .get(&self.table_id)
                {
                    let committed_epoch = info.committed_epoch;
                    self.staging.imm.retain(|imm| {
                        if self.is_replicated {
                            imm.min_epoch() > committed_epoch
                        } else {
                            assert!(imm.min_epoch() > committed_epoch);
                            true
                        }
                    });

                    self.staging.sst.retain(|sst| {
                        sst.epochs.first().expect("epochs not empty") > &committed_epoch
                    });

                    // check epochs.last() > MCE
                    assert!(self.staging.sst.iter().all(|sst| {
                        sst.epochs.last().expect("epochs not empty") > &committed_epoch
                    }));

                    if let Some(committed_watermarks) =
                        self.committed.table_watermarks.get(&self.table_id)
                    {
                        if let Some(watermark_index) = &mut self.table_watermarks {
                            watermark_index.apply_committed_watermarks(
                                committed_watermarks.clone(),
                                committed_epoch,
                            );
                        } else {
                            self.table_watermarks = Some(TableWatermarksIndex::new_committed(
                                committed_watermarks.clone(),
                                committed_epoch,
                            ));
                        }
                    }
                }

                self.committed = committed_version;
            }
            VersionUpdate::NewTableWatermark {
                direction,
                epoch,
                vnode_watermarks,
                watermark_type,
            } => {
                assert_eq!(WatermarkSerdeType::PkPrefix, watermark_type);
                if let Some(watermark_index) = &mut self.table_watermarks {
                    watermark_index.add_epoch_watermark(
                        epoch,
                        Arc::from(vnode_watermarks),
                        direction,
                    );
                } else {
                    self.table_watermarks = Some(TableWatermarksIndex::new(
                        direction,
                        epoch,
                        vnode_watermarks,
                        self.committed.table_committed_epoch(self.table_id),
                    ));
                }
            }
        }
    }

    pub fn staging(&self) -> &StagingVersion {
        &self.staging
    }

    pub fn committed(&self) -> &CommittedVersion {
        &self.committed
    }

    /// We have assumption that the watermark is increasing monotonically. Therefore,
    /// here if the upper layer usage has passed an regressed watermark, we should
    /// filter out the regressed watermark. Currently the kv log store may write
    /// regressed watermark
    pub fn filter_regress_watermarks(&self, watermarks: &mut Vec<VnodeWatermark>) {
        if let Some(watermark_index) = &self.table_watermarks {
            watermark_index.filter_regress_watermarks(watermarks)
        }
    }

    pub fn latest_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.table_watermarks
            .as_ref()
            .and_then(|watermark_index| watermark_index.latest_watermark(vnode))
    }

    pub fn is_replicated(&self) -> bool {
        self.is_replicated
    }

    pub fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        std::mem::replace(&mut self.vnodes, vnodes)
    }

    pub fn contains(&self, vnode: VirtualNode) -> bool {
        self.vnodes.is_set(vnode.to_index())
    }

    pub fn vnodes(&self) -> Arc<Bitmap> {
        self.vnodes.clone()
    }
}

pub fn read_filter_for_version(
    epoch: HummockEpoch,
    table_id: TableId,
    mut table_key_range: TableKeyRange,
    read_version: &RwLock<HummockReadVersion>,
) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
    let read_version_guard = read_version.read();

    let committed_version = read_version_guard.committed().clone();

    if let Some(watermark) = read_version_guard.table_watermarks.as_ref() {
        watermark.rewrite_range_with_table_watermark(epoch, &mut table_key_range)
    }

    let (imm_iter, sst_iter) =
        read_version_guard
            .staging()
            .prune_overlap(epoch, table_id, &table_key_range);

    let imms = imm_iter.cloned().collect();
    let ssts = sst_iter.cloned().collect();

    Ok((table_key_range, (imms, ssts, committed_version)))
}

#[derive(Clone)]
pub struct HummockVersionReader {
    sstable_store: SstableStoreRef,

    /// Statistics
    state_store_metrics: Arc<HummockStateStoreMetrics>,
    preload_retry_times: usize,
}

/// use `HummockVersionReader` to reuse `get` and `iter` implement for both `batch_query` and
/// `streaming_query`
impl HummockVersionReader {
    pub fn new(
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        preload_retry_times: usize,
    ) -> Self {
        Self {
            sstable_store,
            state_store_metrics,
            preload_retry_times,
        }
    }

    pub fn stats(&self) -> &Arc<HummockStateStoreMetrics> {
        &self.state_store_metrics
    }
}

const SLOW_ITER_FETCH_META_DURATION_SECOND: f64 = 5.0;

impl HummockVersionReader {
    pub async fn get<O>(
        &self,
        table_key: TableKey<Bytes>,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: ReadVersionTuple,
        on_key_value_fn: impl crate::store::KeyValueFn<O>,
    ) -> StorageResult<Option<O>> {
        let (imms, uncommitted_ssts, committed_version) = read_version_tuple;

        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        let mut stats_guard =
            GetLocalMetricsGuard::new(self.state_store_metrics.clone(), read_options.table_id);
        let local_stats = &mut stats_guard.local_stats;
        local_stats.found_key = true;

        // 1. read staging data
        for imm in &imms {
            // skip imm that only holding out-of-date data
            if imm.max_epoch() < min_epoch {
                continue;
            }

            local_stats.staging_imm_get_count += 1;

            if let Some((data, data_epoch)) = get_from_batch(
                imm,
                TableKey(table_key.as_ref()),
                epoch,
                &read_options,
                local_stats,
            ) {
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    data.into_user_value()
                        .map(|v| {
                            on_key_value_fn(
                                FullKey::new_with_gap_epoch(
                                    read_options.table_id,
                                    table_key.to_ref(),
                                    data_epoch,
                                ),
                                v.as_ref(),
                            )
                        })
                        .transpose()?
                });
            }
        }

        // 2. order guarantee: imm -> sst
        let dist_key_hash = read_options.prefix_hint.as_ref().map(|dist_key| {
            Sstable::hash_for_bloom_filter(dist_key.as_ref(), read_options.table_id.table_id())
        });

        // Here epoch passed in is pure epoch, and we will seek the constructed `full_key` later.
        // Therefore, it is necessary to construct the `full_key` with `MAX_SPILL_TIMES`, otherwise, the iterator might skip keys with spill offset greater than 0.
        let full_key = FullKey::new_with_gap_epoch(
            read_options.table_id,
            TableKey(table_key.clone()),
            EpochWithGap::new(epoch, MAX_SPILL_TIMES),
        );
        for local_sst in &uncommitted_ssts {
            local_stats.staging_sst_get_count += 1;
            if let Some(iter) = get_from_sstable_info(
                self.sstable_store.clone(),
                local_sst,
                full_key.to_ref(),
                &read_options,
                dist_key_hash,
                local_stats,
            )
            .await?
            {
                debug_assert!(iter.is_valid());
                let data_epoch = iter.key().epoch_with_gap;
                return Ok(if data_epoch.pure_epoch() < min_epoch {
                    None
                } else {
                    iter.value()
                        .into_user_value()
                        .map(|v| {
                            on_key_value_fn(
                                FullKey::new_with_gap_epoch(
                                    read_options.table_id,
                                    table_key.to_ref(),
                                    data_epoch,
                                ),
                                v,
                            )
                        })
                        .transpose()?
                });
            }
        }
        let single_table_key_range = table_key.clone()..=table_key.clone();
        // 3. read from committed_version sst file
        // Because SST meta records encoded key range,
        // the filter key needs to be encoded as well.
        assert!(committed_version.is_valid());
        for level in committed_version.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            match level.level_type {
                LevelType::Overlapping | LevelType::Unspecified => {
                    let sstable_infos = prune_overlapping_ssts(
                        &level.table_infos,
                        read_options.table_id,
                        &single_table_key_range,
                    );
                    for sstable_info in sstable_infos {
                        local_stats.overlapping_get_count += 1;
                        if let Some(iter) = get_from_sstable_info(
                            self.sstable_store.clone(),
                            sstable_info,
                            full_key.to_ref(),
                            &read_options,
                            dist_key_hash,
                            local_stats,
                        )
                        .await?
                        {
                            debug_assert!(iter.is_valid());
                            let data_epoch = iter.key().epoch_with_gap;
                            return Ok(if data_epoch.pure_epoch() < min_epoch {
                                None
                            } else {
                                iter.value()
                                    .into_user_value()
                                    .map(|v| {
                                        on_key_value_fn(
                                            FullKey::new_with_gap_epoch(
                                                read_options.table_id,
                                                table_key.to_ref(),
                                                data_epoch,
                                            ),
                                            v,
                                        )
                                    })
                                    .transpose()?
                            });
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let mut table_info_idx =
                        search_sst_idx(&level.table_infos, full_key.user_key.as_ref());
                    if table_info_idx == 0 {
                        continue;
                    }
                    table_info_idx = table_info_idx.saturating_sub(1);
                    let ord = level.table_infos[table_info_idx]
                        .key_range
                        .compare_right_with_user_key(full_key.user_key.as_ref());
                    // the case that the key falls into the gap between two ssts
                    if ord == Ordering::Less {
                        sync_point!("HUMMOCK_V2::GET::SKIP_BY_NO_FILE");
                        continue;
                    }

                    local_stats.non_overlapping_get_count += 1;
                    if let Some(iter) = get_from_sstable_info(
                        self.sstable_store.clone(),
                        &level.table_infos[table_info_idx],
                        full_key.to_ref(),
                        &read_options,
                        dist_key_hash,
                        local_stats,
                    )
                    .await?
                    {
                        debug_assert!(iter.is_valid());
                        let data_epoch = iter.key().epoch_with_gap;
                        return Ok(if data_epoch.pure_epoch() < min_epoch {
                            None
                        } else {
                            iter.value()
                                .into_user_value()
                                .map(|v| {
                                    on_key_value_fn(
                                        FullKey::new_with_gap_epoch(
                                            read_options.table_id,
                                            table_key.to_ref(),
                                            data_epoch,
                                        ),
                                        v,
                                    )
                                })
                                .transpose()?
                        });
                    }
                }
            }
        }
        stats_guard.local_stats.found_key = false;
        Ok(None)
    }

    pub async fn iter(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
    ) -> StorageResult<HummockStorageIterator> {
        self.iter_with_memtable(
            table_key_range,
            epoch,
            read_options,
            read_version_tuple,
            None,
        )
        .await
    }

    pub async fn iter_with_memtable<'a, 'b>(
        &'a self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
        memtable_iter: Option<MemTableHummockIterator<'b>>,
    ) -> StorageResult<HummockStorageIteratorInner<'b>> {
        let user_key_range_ref = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range = (
            user_key_range_ref.0.map(|key| key.cloned()),
            user_key_range_ref.1.map(|key| key.cloned()),
        );
        let mut factory = ForwardIteratorFactory::default();
        let mut local_stats = StoreLocalStatistic::default();
        let (imms, uncommitted_ssts, committed) = read_version_tuple;
        let table_id = read_options.table_id;
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        self.iter_inner(
            table_key_range,
            epoch,
            read_options,
            imms,
            uncommitted_ssts,
            &committed,
            &mut local_stats,
            &mut factory,
        )
        .await?;
        let merge_iter = factory.build(memtable_iter);
        // the epoch_range left bound for iterator read
        let mut user_iter = UserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter.rewind().await?;
        Ok(HummockStorageIteratorInner::new(
            user_iter,
            self.state_store_metrics.clone(),
            table_id,
            local_stats,
        ))
    }

    pub async fn rev_iter<'a, 'b>(
        &'a self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        read_version_tuple: (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion),
        memtable_iter: Option<MemTableHummockRevIterator<'b>>,
    ) -> StorageResult<HummockStorageRevIteratorInner<'b>> {
        let user_key_range_ref = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range = (
            user_key_range_ref.0.map(|key| key.cloned()),
            user_key_range_ref.1.map(|key| key.cloned()),
        );
        let mut factory = BackwardIteratorFactory::default();
        let mut local_stats = StoreLocalStatistic::default();
        let (imms, uncommitted_ssts, committed) = read_version_tuple;
        let table_id = read_options.table_id;
        let min_epoch = gen_min_epoch(epoch, read_options.retention_seconds.as_ref());
        self.iter_inner(
            table_key_range,
            epoch,
            read_options,
            imms,
            uncommitted_ssts,
            &committed,
            &mut local_stats,
            &mut factory,
        )
        .await?;
        let merge_iter = factory.build(memtable_iter);
        // the epoch_range left bound for iterator read
        let mut user_iter = BackwardUserIterator::new(
            merge_iter,
            user_key_range,
            epoch,
            min_epoch,
            Some(committed),
        );
        user_iter.rewind().await?;
        Ok(HummockStorageRevIteratorInner::new(
            user_iter,
            self.state_store_metrics.clone(),
            table_id,
            local_stats,
        ))
    }

    pub async fn iter_inner<F: IteratorFactory>(
        &self,
        table_key_range: TableKeyRange,
        epoch: u64,
        read_options: ReadOptions,
        imms: Vec<ImmutableMemtable>,
        uncommitted_ssts: Vec<SstableInfo>,
        committed: &CommittedVersion,
        local_stats: &mut StoreLocalStatistic,
        factory: &mut F,
    ) -> StorageResult<()> {
        local_stats.staging_imm_iter_count = imms.len() as u64;
        for imm in imms {
            factory.add_batch_iter(imm);
        }

        // 2. build iterator from committed
        // Because SST meta records encoded key range,
        // the filter key range needs to be encoded as well.
        let user_key_range = bound_table_key_range(read_options.table_id, &table_key_range);
        let user_key_range_ref = (
            user_key_range.0.as_ref().map(UserKey::as_ref),
            user_key_range.1.as_ref().map(UserKey::as_ref),
        );
        let mut staging_sst_iter_count = 0;
        // encode once
        let bloom_filter_prefix_hash = read_options
            .prefix_hint
            .as_ref()
            .map(|hint| Sstable::hash_for_bloom_filter(hint, read_options.table_id.table_id()));
        let mut sst_read_options = SstableIteratorReadOptions::from_read_options(&read_options);
        if read_options.prefetch_options.prefetch {
            sst_read_options.must_iterated_end_user_key =
                Some(user_key_range.1.map(|key| key.cloned()));
            sst_read_options.max_preload_retry_times = self.preload_retry_times;
        }
        let sst_read_options = Arc::new(sst_read_options);
        for sstable_info in &uncommitted_ssts {
            let table_holder = self
                .sstable_store
                .sstable(sstable_info, local_stats)
                .await?;

            if let Some(prefix_hash) = bloom_filter_prefix_hash.as_ref() {
                if !hit_sstable_bloom_filter(
                    &table_holder,
                    &user_key_range_ref,
                    *prefix_hash,
                    local_stats,
                ) {
                    continue;
                }
            }

            staging_sst_iter_count += 1;
            factory.add_staging_sst_iter(F::SstableIteratorType::create(
                table_holder,
                self.sstable_store.clone(),
                sst_read_options.clone(),
                sstable_info,
            ));
        }
        local_stats.staging_sst_iter_count = staging_sst_iter_count;

        let timer = Instant::now();

        for level in committed.levels(read_options.table_id) {
            if level.table_infos.is_empty() {
                continue;
            }

            if level.level_type == LevelType::Nonoverlapping {
                let mut table_infos = prune_nonoverlapping_ssts(
                    &level.table_infos,
                    user_key_range_ref,
                    read_options.table_id.table_id(),
                )
                .peekable();

                if table_infos.peek().is_none() {
                    continue;
                }
                let sstable_infos = table_infos.cloned().collect_vec();
                if sstable_infos.len() > 1 {
                    factory.add_concat_sst_iter(
                        sstable_infos,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                    );
                    local_stats.non_overlapping_iter_count += 1;
                } else {
                    let sstable = self
                        .sstable_store
                        .sstable(&sstable_infos[0], local_stats)
                        .await?;

                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            local_stats,
                        ) {
                            continue;
                        }
                    }
                    // Since there is only one sst to be included for the current non-overlapping
                    // level, there is no need to create a ConcatIterator on it.
                    // We put the SstableIterator in `overlapping_iters` just for convenience since
                    // it overlaps with SSTs in other levels. In metrics reporting, we still count
                    // it in `non_overlapping_iter_count`.
                    factory.add_overlapping_sst_iter(F::SstableIteratorType::create(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                        &sstable_infos[0],
                    ));
                    local_stats.non_overlapping_iter_count += 1;
                }
            } else {
                let table_infos = prune_overlapping_ssts(
                    &level.table_infos,
                    read_options.table_id,
                    &table_key_range,
                );
                // Overlapping
                let fetch_meta_req = table_infos.rev().collect_vec();
                if fetch_meta_req.is_empty() {
                    continue;
                }
                for sstable_info in fetch_meta_req {
                    let sstable = self
                        .sstable_store
                        .sstable(sstable_info, local_stats)
                        .await?;
                    assert_eq!(sstable_info.object_id, sstable.id);
                    if let Some(dist_hash) = bloom_filter_prefix_hash.as_ref() {
                        if !hit_sstable_bloom_filter(
                            &sstable,
                            &user_key_range_ref,
                            *dist_hash,
                            local_stats,
                        ) {
                            continue;
                        }
                    }
                    factory.add_overlapping_sst_iter(F::SstableIteratorType::create(
                        sstable,
                        self.sstable_store.clone(),
                        sst_read_options.clone(),
                        sstable_info,
                    ));
                    local_stats.overlapping_iter_count += 1;
                }
            }
        }
        let fetch_meta_duration_sec = timer.elapsed().as_secs_f64();
        if fetch_meta_duration_sec > SLOW_ITER_FETCH_META_DURATION_SECOND {
            let table_id_string = read_options.table_id.to_string();
            tracing::warn!("Fetching meta while creating an iter to read table_id {:?} at epoch {:?} is slow: duration = {:?}s, cache unhits = {:?}.",
                table_id_string, epoch, fetch_meta_duration_sec, local_stats.cache_meta_block_miss);
            self.state_store_metrics
                .iter_slow_fetch_meta_cache_unhits
                .set(local_stats.cache_meta_block_miss as i64);
        }
        Ok(())
    }

    pub async fn iter_log(
        &self,
        version: PinnedVersion,
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
    ) -> HummockResult<ChangeLogIterator> {
        let change_log = {
            let table_change_logs = version.table_change_log_read_lock();
            if let Some(change_log) = table_change_logs.get(&options.table_id) {
                change_log.filter_epoch(epoch_range).cloned().collect_vec()
            } else {
                Vec::new()
            }
        };

        if let Some(max_epoch_change_log) = change_log.last() {
            let (_, max_epoch) = epoch_range;
            if !max_epoch_change_log.epochs.contains(&max_epoch) {
                warn!(
                    max_epoch,
                    change_log_epochs = ?change_log.iter().flat_map(|epoch_log| epoch_log.epochs.iter()).collect_vec(),
                    table_id = options.table_id.table_id,
                    "max_epoch does not exist"
                );
            }
        }
        let read_options = Arc::new(SstableIteratorReadOptions {
            cache_policy: Default::default(),
            must_iterated_end_user_key: None,
            max_preload_retry_times: 0,
            prefetch_for_large_query: false,
        });

        async fn make_iter(
            sstable_infos: impl Iterator<Item = &SstableInfo>,
            sstable_store: &SstableStoreRef,
            read_options: Arc<SstableIteratorReadOptions>,
            local_stat: &mut StoreLocalStatistic,
        ) -> HummockResult<MergeIterator<SstableIterator>> {
            let iters = try_join_all(sstable_infos.map(|sstable_info| {
                let sstable_store = sstable_store.clone();
                let read_options = read_options.clone();
                async move {
                    let mut local_stat = StoreLocalStatistic::default();
                    let table_holder = sstable_store.sstable(sstable_info, &mut local_stat).await?;
                    Ok::<_, HummockError>((
                        SstableIterator::new(
                            table_holder,
                            sstable_store,
                            read_options,
                            sstable_info,
                        ),
                        local_stat,
                    ))
                }
            }))
            .await?;
            Ok::<_, HummockError>(MergeIterator::new(iters.into_iter().map(
                |(iter, stats)| {
                    local_stat.add(&stats);
                    iter
                },
            )))
        }

        let mut local_stat = StoreLocalStatistic::default();

        let new_value_iter = make_iter(
            change_log
                .iter()
                .flat_map(|log| log.new_value.iter())
                .filter(|sst| filter_single_sst(sst, options.table_id, &key_range)),
            &self.sstable_store,
            read_options.clone(),
            &mut local_stat,
        )
        .await?;
        let old_value_iter = make_iter(
            change_log
                .iter()
                .flat_map(|log| log.old_value.iter())
                .filter(|sst| filter_single_sst(sst, options.table_id, &key_range)),
            &self.sstable_store,
            read_options.clone(),
            &mut local_stat,
        )
        .await?;
        ChangeLogIterator::new(
            epoch_range,
            key_range,
            new_value_iter,
            old_value_iter,
            options.table_id,
            IterLocalMetricsGuard::new(
                self.state_store_metrics.clone(),
                options.table_id,
                local_stat,
            ),
        )
        .await
    }
}
