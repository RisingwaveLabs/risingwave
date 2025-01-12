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

use std::collections::HashSet;
use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use arc_swap::ArcSwap;
use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::is_max_epoch;
use risingwave_common_service::{NotificationClient, ObserverManager};
use risingwave_hummock_sdk::key::{
    is_empty_key_range, vnode, vnode_range, TableKey, TableKeyRange,
};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::TableWatermarksIndex;
use risingwave_hummock_sdk::version::{HummockVersion, LocalHummockVersion};
use risingwave_hummock_sdk::{HummockReadEpoch, HummockSstableObjectId, SyncResult};
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;

use super::local_hummock_storage::LocalHummockStorage;
use super::version::{read_filter_for_version, CommittedVersion, HummockVersionReader};
use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
#[cfg(any(test, feature = "test"))]
use crate::compaction_catalog_manager::{CompactionCatalogManager, FakeRemoteTableAccessor};
use crate::error::StorageResult;
use crate::hummock::backup_reader::{BackupReader, BackupReaderRef};
use crate::hummock::compactor::{
    new_compaction_await_tree_reg_ref, CompactionAwaitTreeRegRef, CompactorContext,
};
use crate::hummock::event_handler::hummock_event_handler::{BufferTracker, HummockEventSender};
use crate::hummock::event_handler::{
    HummockEvent, HummockEventHandler, HummockVersionUpdate, ReadOnlyReadVersionMapping,
};
use crate::hummock::iterator::change_log::ChangeLogIterator;
use crate::hummock::local_version::pinned_version::{start_pinned_version_worker, PinnedVersion};
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::observer_manager::HummockObserverNode;
use crate::hummock::time_travel_version_cache::SimpleTimeTravelVersionCache;
use crate::hummock::utils::{wait_for_epoch, wait_for_update};
use crate::hummock::write_limiter::{WriteLimiter, WriteLimiterRef};
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockStorageIterator, HummockStorageRevIterator,
    MemoryLimiter, SstableObjectIdManager, SstableObjectIdManagerRef, SstableStoreRef,
};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::{CompactorMetrics, HummockStateStoreMetrics};
use crate::opts::StorageOpts;
use crate::store::*;

struct HummockStorageShutdownGuard {
    shutdown_sender: HummockEventSender,
}

impl Drop for HummockStorageShutdownGuard {
    fn drop(&mut self) {
        let _ = self
            .shutdown_sender
            .send(HummockEvent::Shutdown)
            .inspect_err(|e| tracing::debug!(event = ?e.0, "unable to send shutdown"));
    }
}

/// `HummockStorage` is the entry point of the Hummock state store backend.
/// It implements the `StateStore` and `StateStoreRead` traits but without any write method
/// since all writes should be done via `LocalHummockStorage` to ensure the single writer property
/// of hummock. `LocalHummockStorage` instance can be created via `new_local` call.
/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    hummock_event_sender: HummockEventSender,
    // only used in test for setting hummock version in uploader
    _version_update_sender: UnboundedSender<HummockVersionUpdate>,

    context: CompactorContext,

    compaction_catalog_manager_ref: CompactionCatalogManagerRef,

    sstable_object_id_manager: SstableObjectIdManagerRef,

    buffer_tracker: BufferTracker,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,

    recent_versions: Arc<ArcSwap<RecentVersions>>,

    hummock_version_reader: HummockVersionReader,

    _shutdown_guard: Arc<HummockStorageShutdownGuard>,

    read_version_mapping: ReadOnlyReadVersionMapping,

    backup_reader: BackupReaderRef,

    write_limiter: WriteLimiterRef,

    compact_await_tree_reg: Option<CompactionAwaitTreeRegRef>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    simple_time_travel_version_cache: Arc<SimpleTimeTravelVersionCache>,
}

pub type ReadVersionTuple = (Vec<ImmutableMemtable>, Vec<SstableInfo>, CommittedVersion);

pub fn get_committed_read_version_tuple(
    version: PinnedVersion,
    table_id: TableId,
    mut key_range: TableKeyRange,
    epoch: HummockEpoch,
) -> (TableKeyRange, ReadVersionTuple) {
    if let Some(table_watermarks) = version.table_watermarks.get(&table_id) {
        TableWatermarksIndex::new_committed(
            table_watermarks.clone(),
            version
                .state_table_info
                .info()
                .get(&table_id)
                .expect("should exist when having table watermark")
                .committed_epoch,
        )
        .rewrite_range_with_table_watermark(epoch, &mut key_range)
    }
    (key_range, (vec![], vec![], version))
}

impl HummockStorage {
    /// Creates a [`HummockStorage`].
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        compaction_catalog_manager_ref: CompactionCatalogManagerRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        compactor_metrics: Arc<CompactorMetrics>,
        await_tree_config: Option<await_tree::Config>,
    ) -> HummockResult<Self> {
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            options.sstable_id_remote_fetch_number,
        ));
        let backup_reader = BackupReader::new(
            &options.backup_storage_url,
            &options.backup_storage_directory,
            &options.object_store_config,
        )
        .await
        .map_err(HummockError::read_backup_error)?;
        let write_limiter = Arc::new(WriteLimiter::default());
        let (version_update_tx, mut version_update_rx) = unbounded_channel();

        let observer_manager = ObserverManager::new(
            notification_client,
            HummockObserverNode::new(
                compaction_catalog_manager_ref.clone(),
                backup_reader.clone(),
                version_update_tx.clone(),
                write_limiter.clone(),
            ),
        )
        .await;
        observer_manager.start().await;

        let hummock_version = match version_update_rx.recv().await {
            Some(HummockVersionUpdate::PinnedVersion(version)) => *version,
            _ => unreachable!("the hummock observer manager is the first one to take the event tx. Should be full hummock version")
        };

        let (pin_version_tx, pin_version_rx) = unbounded_channel();
        let pinned_version = PinnedVersion::new(hummock_version, pin_version_tx);
        tokio::spawn(start_pinned_version_worker(
            pin_version_rx,
            hummock_meta_client.clone(),
            options.max_version_pinning_duration_sec,
        ));

        let await_tree_reg = await_tree_config.map(new_compaction_await_tree_reg_ref);

        let compactor_context = CompactorContext::new_local_compact_context(
            options.clone(),
            sstable_store.clone(),
            compactor_metrics.clone(),
            await_tree_reg.clone(),
        );

        let hummock_event_handler = HummockEventHandler::new(
            version_update_rx,
            pinned_version,
            compactor_context.clone(),
            compaction_catalog_manager_ref.clone(),
            sstable_object_id_manager.clone(),
            state_store_metrics.clone(),
        );

        let event_tx = hummock_event_handler.event_sender();

        let instance = Self {
            context: compactor_context,
            compaction_catalog_manager_ref: compaction_catalog_manager_ref.clone(),
            sstable_object_id_manager,
            buffer_tracker: hummock_event_handler.buffer_tracker().clone(),
            version_update_notifier_tx: hummock_event_handler.version_update_notifier_tx(),
            hummock_event_sender: event_tx.clone(),
            _version_update_sender: version_update_tx,
            recent_versions: hummock_event_handler.recent_versions(),
            hummock_version_reader: HummockVersionReader::new(
                sstable_store,
                state_store_metrics.clone(),
                options.max_preload_io_retry_times,
            ),
            _shutdown_guard: Arc::new(HummockStorageShutdownGuard {
                shutdown_sender: event_tx,
            }),
            read_version_mapping: hummock_event_handler.read_version_mapping(),
            backup_reader,
            write_limiter,
            compact_await_tree_reg: await_tree_reg,
            hummock_meta_client,
            simple_time_travel_version_cache: Arc::new(SimpleTimeTravelVersionCache::new(
                options.time_travel_version_cache_capacity,
            )),
        };

        tokio::spawn(hummock_event_handler.start_hummock_event_handler_worker());

        Ok(instance)
    }
}

impl HummockStorageReadSnapshot {
    /// Gets the value of a specified `key` in the table specified in `read_options`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    /// if `key` has consistent hash virtual node value, then such value is stored in `value_meta`
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    async fn get_inner(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> StorageResult<Option<StateStoreKeyedRow>> {
        let key_range = (Bound::Included(key.clone()), Bound::Included(key.clone()));

        let (key_range, read_version_tuple) = self
            .build_read_version_tuple(self.raw_epoch, key_range, &read_options)
            .await?;

        if is_empty_key_range(&key_range) {
            return Ok(None);
        }

        self.hummock_version_reader
            .get(key, self.raw_epoch, read_options, read_version_tuple)
            .await
    }

    async fn iter_inner(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageIterator> {
        let (key_range, read_version_tuple) = self
            .build_read_version_tuple(self.raw_epoch, key_range, &read_options)
            .await?;

        self.hummock_version_reader
            .iter(key_range, self.raw_epoch, read_options, read_version_tuple)
            .await
    }

    async fn rev_iter_inner(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> StorageResult<HummockStorageRevIterator> {
        let (key_range, read_version_tuple) = self
            .build_read_version_tuple(self.raw_epoch, key_range, &read_options)
            .await?;

        self.hummock_version_reader
            .rev_iter(
                key_range,
                self.raw_epoch,
                read_options,
                read_version_tuple,
                None,
            )
            .await
    }

    async fn get_time_travel_version(
        &self,
        epoch: u64,
        table_id: TableId,
    ) -> StorageResult<PinnedVersion> {
        let meta_client = self.hummock_meta_client.clone();
        let fetch = async move {
            let pb_version = meta_client
                .get_version_by_epoch(epoch, table_id.table_id())
                .await
                .inspect_err(|e| tracing::error!("{}", e.to_report_string()))
                .map_err(|e| HummockError::meta_error(e.to_report_string()))?;
            let version = HummockVersion::from_rpc_protobuf(&pb_version);
            let (tx, _rx) = unbounded_channel();
            Ok(PinnedVersion::new(version, tx))
        };
        let version = self
            .simple_time_travel_version_cache
            .get_or_insert(table_id.table_id, epoch, fetch)
            .await?;
        Ok(version)
    }

    async fn build_read_version_tuple(
        &self,
        epoch: u64,
        key_range: TableKeyRange,
        read_options: &ReadOptions,
    ) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
        if read_options.read_version_from_backup {
            self.build_read_version_tuple_from_backup(epoch, self.table_id, key_range)
                .await
        } else if read_options.read_committed {
            self.build_read_version_tuple_from_committed(epoch, self.table_id, key_range)
                .await
        } else {
            self.build_read_version_tuple_from_all(epoch, self.table_id, key_range)
        }
    }

    async fn build_read_version_tuple_from_backup(
        &self,
        epoch: u64,
        table_id: TableId,
        key_range: TableKeyRange,
    ) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
        match self
            .backup_reader
            .try_get_hummock_version(table_id, epoch)
            .await
        {
            Ok(Some(backup_version)) => Ok(get_committed_read_version_tuple(
                backup_version,
                table_id,
                key_range,
                epoch,
            )),
            Ok(None) => Err(HummockError::read_backup_error(format!(
                "backup include epoch {} not found",
                epoch
            ))
            .into()),
            Err(e) => Err(e),
        }
    }

    async fn build_read_version_tuple_from_committed(
        &self,
        epoch: u64,
        table_id: TableId,
        key_range: TableKeyRange,
    ) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
        let version = match self
            .recent_versions
            .load()
            .get_safe_version(table_id, epoch)
        {
            Some(version) => version,
            None => self.get_time_travel_version(epoch, table_id).await?,
        };
        Ok(get_committed_read_version_tuple(
            version, table_id, key_range, epoch,
        ))
    }

    fn build_read_version_tuple_from_all(
        &self,
        epoch: u64,
        table_id: TableId,
        key_range: TableKeyRange,
    ) -> StorageResult<(TableKeyRange, ReadVersionTuple)> {
        let pinned_version = self.recent_versions.load().latest_version().clone();
        let info = pinned_version.state_table_info.info().get(&table_id);

        // check epoch if lower mce
        let ret = if let Some(info) = info
            && epoch <= info.committed_epoch
        {
            if epoch < info.committed_epoch {
                return Err(
                    HummockError::expired_epoch(table_id, info.committed_epoch, epoch).into(),
                );
            }
            // read committed_version directly without build snapshot
            get_committed_read_version_tuple(pinned_version, table_id, key_range, epoch)
        } else {
            let vnode = vnode(&key_range);
            let mut matched_replicated_read_version_cnt = 0;
            let read_version_vec = {
                let read_guard = self.read_version_mapping.read();
                read_guard
                    .get(&table_id)
                    .map(|v| {
                        v.values()
                            .filter(|v| {
                                let read_version = v.read();
                                if read_version.contains(vnode) {
                                    if read_version.is_replicated() {
                                        matched_replicated_read_version_cnt += 1;
                                        false
                                    } else {
                                        // Only non-replicated read version with matched vnode is considered
                                        true
                                    }
                                } else {
                                    false
                                }
                            })
                            .cloned()
                            .collect_vec()
                    })
                    .unwrap_or_default()
            };

            // When the system has just started and no state has been created, the memory state
            // may be empty
            if read_version_vec.is_empty() {
                let table_committed_epoch = info.map(|info| info.committed_epoch);
                if matched_replicated_read_version_cnt > 0 {
                    tracing::warn!(
                        "Read(table_id={} vnode={} epoch={}) is not allowed on replicated read version ({} found). Fall back to committed version (epoch={:?})",
                        table_id,
                        vnode.to_index(),
                        epoch,
                        matched_replicated_read_version_cnt,
                        table_committed_epoch,
                    );
                } else {
                    tracing::debug!(
                        "No read version found for read(table_id={} vnode={} epoch={}). Fall back to committed version (epoch={:?})",
                        table_id,
                        vnode.to_index(),
                        epoch,
                        table_committed_epoch
                    );
                }
                get_committed_read_version_tuple(pinned_version, table_id, key_range, epoch)
            } else {
                if read_version_vec.len() != 1 {
                    let read_version_vnodes = read_version_vec
                        .into_iter()
                        .map(|v| {
                            let v = v.read();
                            v.vnodes().iter_ones().collect_vec()
                        })
                        .collect_vec();
                    return Err(HummockError::other(format!("There are {} read version associated with vnode {}. read_version_vnodes={:?}", read_version_vnodes.len(), vnode.to_index(), read_version_vnodes)).into());
                }
                read_filter_for_version(
                    epoch,
                    table_id,
                    key_range,
                    read_version_vec.first().unwrap(),
                )?
            }
        };

        Ok(ret)
    }
}

impl HummockStorage {
    async fn new_local_inner(&self, option: NewLocalOptions) -> LocalHummockStorage {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::RegisterReadVersion {
                table_id: option.table_id,
                new_read_version_sender: tx,
                is_replicated: option.is_replicated,
                vnodes: option.vnodes.clone(),
            })
            .unwrap();

        let (basic_read_version, instance_guard) = rx.await.unwrap();
        let version_update_notifier_tx = self.version_update_notifier_tx.clone();
        LocalHummockStorage::new(
            instance_guard,
            basic_read_version,
            self.hummock_version_reader.clone(),
            self.hummock_event_sender.clone(),
            self.buffer_tracker.get_memory_limiter().clone(),
            self.write_limiter.clone(),
            option,
            version_update_notifier_tx,
            self.context.storage_opts.mem_table_spill_threshold,
        )
    }

    pub async fn clear_shared_buffer(&self) {
        let (tx, rx) = oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::Clear(tx, None))
            .expect("should send success");
        rx.await.expect("should wait success");
    }

    pub async fn clear_tables(&self, table_ids: HashSet<TableId>) {
        if !table_ids.is_empty() {
            let (tx, rx) = oneshot::channel();
            self.hummock_event_sender
                .send(HummockEvent::Clear(tx, Some(table_ids)))
                .expect("should send success");
            rx.await.expect("should wait success");
        }
    }

    /// Declare the start of an epoch. This information is provided for spill so that the spill task won't
    /// include data of two or more syncs.
    // TODO: remove this method when we support spill task that can include data of more two or more syncs
    pub fn start_epoch(&self, epoch: HummockEpoch, table_ids: HashSet<TableId>) {
        let _ = self
            .hummock_event_sender
            .send(HummockEvent::StartEpoch { epoch, table_ids });
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.context.sstable_store.clone()
    }

    pub fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        &self.sstable_object_id_manager
    }

    pub fn compaction_catalog_manager_ref(&self) -> CompactionCatalogManagerRef {
        self.compaction_catalog_manager_ref.clone()
    }

    pub fn get_memory_limiter(&self) -> Arc<MemoryLimiter> {
        self.buffer_tracker.get_memory_limiter().clone()
    }

    pub fn get_pinned_version(&self) -> PinnedVersion {
        self.recent_versions.load().latest_version().clone()
    }

    pub fn backup_reader(&self) -> BackupReaderRef {
        self.backup_reader.clone()
    }

    pub fn compaction_await_tree_reg(&self) -> Option<&await_tree::Registry> {
        self.compact_await_tree_reg.as_ref()
    }

    pub async fn min_uncommitted_sst_id(&self) -> Option<HummockSstableObjectId> {
        let (tx, rx) = oneshot::channel();
        self.hummock_event_sender
            .send(HummockEvent::GetMinUncommittedSstId { result_tx: tx })
            .expect("should send success");
        rx.await.expect("should await success")
    }

    pub async fn sync(
        &self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) -> StorageResult<SyncResult> {
        let (tx, rx) = oneshot::channel();
        let _ = self.hummock_event_sender.send(HummockEvent::SyncEpoch {
            sync_result_sender: tx,
            sync_table_epochs,
        });
        let synced_data = rx
            .await
            .map_err(|_| HummockError::other("failed to receive sync result"))??;
        Ok(synced_data.into_sync_result())
    }
}

#[derive(Clone)]
pub struct HummockStorageReadSnapshot {
    raw_epoch: u64,
    table_id: TableId,
    recent_versions: Arc<ArcSwap<RecentVersions>>,
    hummock_version_reader: HummockVersionReader,
    read_version_mapping: ReadOnlyReadVersionMapping,
    backup_reader: BackupReaderRef,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    simple_time_travel_version_cache: Arc<SimpleTimeTravelVersionCache>,
}

impl StateStoreRead for HummockStorageReadSnapshot {
    type Iter = HummockStorageIterator;
    type RevIter = HummockStorageRevIterator;

    fn get_keyed_row(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Option<StateStoreKeyedRow>>> + Send + '_ {
        self.get_inner(key, read_options)
    }

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
        let (l_vnode_inclusive, r_vnode_exclusive) = vnode_range(&key_range);
        assert_eq!(
            r_vnode_exclusive - l_vnode_inclusive,
            1,
            "read range {:?} for table {} iter contains more than one vnode",
            key_range,
            self.table_id
        );
        self.iter_inner(key_range, read_options)
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
        let (l_vnode_inclusive, r_vnode_exclusive) = vnode_range(&key_range);
        assert_eq!(
            r_vnode_exclusive - l_vnode_inclusive,
            1,
            "read range {:?} for table {} iter contains more than one vnode",
            key_range,
            self.table_id
        );
        self.rev_iter_inner(key_range, read_options)
    }
}

impl StateStoreReadLog for HummockStorage {
    type ChangeLogIter = ChangeLogIterator;

    async fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> StorageResult<u64> {
        fn next_epoch(
            version: &LocalHummockVersion,
            epoch: u64,
            table_id: TableId,
        ) -> HummockResult<Option<u64>> {
            let table_change_log = version.table_change_log.get(&table_id).ok_or_else(|| {
                HummockError::next_epoch(format!("table {} has been dropped", table_id))
            })?;
            table_change_log.next_epoch(epoch).map_err(|_| {
                HummockError::next_epoch(format!(
                    "invalid epoch {}, change log epoch: {:?}",
                    epoch,
                    table_change_log.epochs().collect_vec()
                ))
            })
        }
        {
            // fast path
            let recent_versions = self.recent_versions.load();
            if let Some(next_epoch) =
                next_epoch(recent_versions.latest_version(), epoch, options.table_id)?
            {
                return Ok(next_epoch);
            }
        }
        let mut next_epoch_ret = None;
        wait_for_update(
            &self.version_update_notifier_tx,
            |version| {
                if let Some(next_epoch) = next_epoch(version, epoch, options.table_id)? {
                    next_epoch_ret = Some(next_epoch);
                    Ok(true)
                } else {
                    Ok(false)
                }
            },
            || format!("wait next_epoch: epoch: {} {}", epoch, options.table_id),
        )
        .await?;
        Ok(next_epoch_ret.expect("should be set before wait_for_update returns"))
    }

    async fn iter_log(
        &self,
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
    ) -> StorageResult<Self::ChangeLogIter> {
        let version = self.recent_versions.load().latest_version().clone();
        let iter = self
            .hummock_version_reader
            .iter_log(version, epoch_range, key_range, options)
            .await?;
        Ok(iter)
    }
}

impl HummockStorage {
    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    async fn try_wait_epoch_impl(
        &self,
        wait_epoch: HummockReadEpoch,
        table_id: TableId,
    ) -> StorageResult<()> {
        match wait_epoch {
            HummockReadEpoch::Committed(wait_epoch) => {
                assert!(!is_max_epoch(wait_epoch), "epoch should not be MAX EPOCH");
                wait_for_epoch(&self.version_update_notifier_tx, wait_epoch, table_id).await?;
            }
            HummockReadEpoch::BatchQueryCommitted(wait_epoch, wait_version_id) => {
                assert!(!is_max_epoch(wait_epoch), "epoch should not be MAX EPOCH");
                // fast path by checking recent_versions
                {
                    let recent_versions = self.recent_versions.load();
                    let latest_version = recent_versions.latest_version();
                    if latest_version.id >= wait_version_id
                        && let Some(committed_epoch) =
                            latest_version.table_committed_epoch(table_id)
                        && committed_epoch >= wait_epoch
                    {
                        return Ok(());
                    }
                }
                wait_for_update(
                    &self.version_update_notifier_tx,
                    |version| {
                        if wait_version_id > version.id() {
                            return Ok(false);
                        }
                        let committed_epoch =
                            version.table_committed_epoch(table_id).ok_or_else(|| {
                                // In batch query, since we have ensured that the current version must be after the
                                // `wait_version_id`, when seeing that the table_id not exist in the latest version,
                                // the table must have been dropped.
                                HummockError::wait_epoch(format!(
                                    "table id {} has been dropped",
                                    table_id
                                ))
                            })?;
                        Ok(committed_epoch >= wait_epoch)
                    },
                    || {
                        format!(
                            "try_wait_epoch: epoch: {}, version_id: {:?}",
                            wait_epoch, wait_version_id
                        )
                    },
                )
                .await?;
            }
            _ => {}
        };
        Ok(())
    }
}

impl StateStore for HummockStorage {
    type Local = LocalHummockStorage;
    type ReadSnapshot = HummockStorageReadSnapshot;

    /// Waits until the local hummock version contains the epoch. If `wait_epoch` is `Current`,
    /// we will only check whether it is le `sealed_epoch` and won't wait.
    async fn try_wait_epoch(
        &self,
        wait_epoch: HummockReadEpoch,
        options: TryWaitEpochOptions,
    ) -> StorageResult<()> {
        self.try_wait_epoch_impl(wait_epoch, options.table_id).await
    }

    fn new_local(&self, option: NewLocalOptions) -> impl Future<Output = Self::Local> + Send + '_ {
        self.new_local_inner(option)
    }

    async fn new_read_snapshot(
        &self,
        epoch: HummockReadEpoch,
        options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        self.try_wait_epoch_impl(epoch, options.table_id).await?;
        Ok(HummockStorageReadSnapshot {
            raw_epoch: epoch.get_epoch(),
            table_id: options.table_id,
            recent_versions: self.recent_versions.clone(),
            hummock_version_reader: self.hummock_version_reader.clone(),
            read_version_mapping: self.read_version_mapping.clone(),
            backup_reader: self.backup_reader.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            simple_time_travel_version_cache: self.simple_time_travel_version_cache.clone(),
        })
    }
}

#[cfg(any(test, feature = "test"))]
impl HummockStorage {
    pub async fn seal_and_sync_epoch(
        &self,
        epoch: u64,
        table_ids: HashSet<TableId>,
    ) -> StorageResult<risingwave_hummock_sdk::SyncResult> {
        self.sync(vec![(epoch, table_ids)]).await
    }

    /// Used in the compaction test tool
    pub async fn update_version_and_wait(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        let version_id = version.id;
        self._version_update_sender
            .send(HummockVersionUpdate::PinnedVersion(Box::new(version)))
            .unwrap();
        loop {
            if self.recent_versions.load().latest_version().id() >= version_id {
                break;
            }

            yield_now().await
        }
    }

    pub async fn wait_version(&self, version: HummockVersion) {
        use tokio::task::yield_now;
        loop {
            if self.recent_versions.load().latest_version().id() >= version.id {
                break;
            }

            yield_now().await
        }
    }

    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_buffer_size()
    }

    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn for_test(
        options: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> HummockResult<Self> {
        let compaction_catalog_manager = Arc::new(CompactionCatalogManager::new(Box::new(
            FakeRemoteTableAccessor {},
        )));

        Self::new(
            options,
            sstable_store,
            hummock_meta_client,
            notification_client,
            compaction_catalog_manager,
            Arc::new(HummockStateStoreMetrics::unused()),
            Arc::new(CompactorMetrics::unused()),
            None,
        )
        .await
    }

    pub fn storage_opts(&self) -> &Arc<StorageOpts> {
        &self.context.storage_opts
    }

    pub fn version_reader(&self) -> &HummockVersionReader {
        &self.hummock_version_reader
    }

    pub async fn wait_version_update(
        &self,
        old_id: risingwave_hummock_sdk::HummockVersionId,
    ) -> risingwave_hummock_sdk::HummockVersionId {
        use tokio::task::yield_now;
        loop {
            let cur_id = self.recent_versions.load().latest_version().id();
            if cur_id > old_id {
                return cur_id;
            }
            yield_now().await;
        }
    }
}
