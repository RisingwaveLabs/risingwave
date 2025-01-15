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

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use await_tree::InstrumentAwait;
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::{Histogram, IntGauge};
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::UintGauge;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{HummockVersionCommon, LocalHummockVersionDelta};
use risingwave_hummock_sdk::{HummockEpoch, SyncResult};
use tokio::spawn;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use super::refiller::{CacheRefillConfig, CacheRefiller};
use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::compactor::{await_tree_key, compact, CompactorContext};
use crate::hummock::event_handler::refiller::{CacheRefillerEvent, SpawnRefillTask};
use crate::hummock::event_handler::uploader::{
    HummockUploader, SpawnUploadTask, SyncedData, UploadTaskOutput,
};
use crate::hummock::event_handler::{
    HummockEvent, HummockReadVersionRef, HummockVersionUpdate, ReadOnlyReadVersionMapping,
    ReadOnlyRwLockRef,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::store::version::{
    HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use crate::hummock::{HummockResult, MemoryLimiter, SstableObjectIdManager, SstableStoreRef};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

#[derive(Clone)]
pub struct BufferTracker {
    flush_threshold: usize,
    min_batch_flush_size: usize,
    global_buffer: Arc<MemoryLimiter>,
    global_upload_task_size: UintGauge,
}

impl BufferTracker {
    pub fn from_storage_opts(config: &StorageOpts, global_upload_task_size: UintGauge) -> Self {
        let capacity = config.shared_buffer_capacity_mb * (1 << 20);
        let flush_threshold = (capacity as f32 * config.shared_buffer_flush_ratio) as usize;
        let shared_buffer_min_batch_flush_size =
            config.shared_buffer_min_batch_flush_size_mb * (1 << 20);
        assert!(
            flush_threshold < capacity,
            "flush_threshold {} should be less or equal to capacity {}",
            flush_threshold,
            capacity
        );
        Self::new(
            capacity,
            flush_threshold,
            global_upload_task_size,
            shared_buffer_min_batch_flush_size,
        )
    }

    #[cfg(test)]
    fn for_test_with_config(flush_threshold: usize, min_batch_flush_size: usize) -> Self {
        Self::new(
            usize::MAX,
            flush_threshold,
            UintGauge::new("test", "test").unwrap(),
            min_batch_flush_size,
        )
    }

    fn new(
        capacity: usize,
        flush_threshold: usize,
        global_upload_task_size: UintGauge,
        min_batch_flush_size: usize,
    ) -> Self {
        assert!(capacity >= flush_threshold);
        Self {
            flush_threshold,
            global_buffer: Arc::new(MemoryLimiter::new(capacity as u64)),
            global_upload_task_size,
            min_batch_flush_size,
        }
    }

    pub fn for_test() -> Self {
        Self::from_storage_opts(
            &StorageOpts::default(),
            UintGauge::new("test", "test").unwrap(),
        )
    }

    pub fn get_buffer_size(&self) -> usize {
        self.global_buffer.get_memory_usage() as usize
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_buffer
    }

    pub fn global_upload_task_size(&self) -> &UintGauge {
        &self.global_upload_task_size
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_flush(&self) -> bool {
        self.get_buffer_size() > self.flush_threshold + self.global_upload_task_size.get() as usize
    }

    pub fn need_more_flush(&self, curr_batch_flush_size: usize) -> bool {
        curr_batch_flush_size < self.min_batch_flush_size || self.need_flush()
    }

    #[cfg(test)]
    pub(crate) fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }
}

#[derive(Clone)]
pub struct HummockEventSender {
    inner: UnboundedSender<HummockEvent>,
    event_count: IntGauge,
}

pub fn event_channel(event_count: IntGauge) -> (HummockEventSender, HummockEventReceiver) {
    let (tx, rx) = unbounded_channel();
    (
        HummockEventSender {
            inner: tx,
            event_count: event_count.clone(),
        },
        HummockEventReceiver {
            inner: rx,
            event_count,
        },
    )
}

impl HummockEventSender {
    pub fn send(&self, event: HummockEvent) -> Result<(), SendError<HummockEvent>> {
        self.inner.send(event)?;
        self.event_count.inc();
        Ok(())
    }
}

pub struct HummockEventReceiver {
    inner: UnboundedReceiver<HummockEvent>,
    event_count: IntGauge,
}

impl HummockEventReceiver {
    async fn recv(&mut self) -> Option<HummockEvent> {
        let event = self.inner.recv().await?;
        self.event_count.dec();
        Some(event)
    }
}

struct HummockEventHandlerMetrics {
    event_handler_on_upload_finish_latency: Histogram,
    event_handler_on_apply_version_update: Histogram,
    event_handler_on_recv_version_update: Histogram,
}

pub struct HummockEventHandler {
    hummock_event_tx: HummockEventSender,
    hummock_event_rx: HummockEventReceiver,
    version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    /// A copy of `read_version_mapping` but owned by event handler
    local_read_version_mapping: HashMap<LocalInstanceId, (TableId, HummockReadVersionRef)>,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    recent_versions: Arc<ArcSwap<RecentVersions>>,

    uploader: HummockUploader,
    refiller: CacheRefiller,

    last_instance_id: LocalInstanceId,

    metrics: HummockEventHandlerMetrics,
}

async fn flush_imms(
    payload: Vec<ImmutableMemtable>,
    compactor_context: CompactorContext,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
) -> HummockResult<UploadTaskOutput> {
    compact(
        compactor_context,
        sstable_object_id_manager,
        payload,
        compaction_catalog_manager_ref,
    )
    .verbose_instrument_await("shared_buffer_compact")
    .await
}

impl HummockEventHandler {
    pub fn new(
        version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
        pinned_version: PinnedVersion,
        compactor_context: CompactorContext,
        compaction_catalog_manager_ref: CompactionCatalogManagerRef,
        sstable_object_id_manager: Arc<SstableObjectIdManager>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        let upload_compactor_context = compactor_context.clone();
        let upload_task_latency = state_store_metrics.uploader_upload_task_latency.clone();
        let wait_poll_latency = state_store_metrics.uploader_wait_poll_latency.clone();
        let recent_versions = RecentVersions::new(
            pinned_version,
            compactor_context
                .storage_opts
                .max_cached_recent_versions_number,
            state_store_metrics.clone(),
        );
        let buffer_tracker = BufferTracker::from_storage_opts(
            &compactor_context.storage_opts,
            state_store_metrics.uploader_uploading_task_size.clone(),
        );
        Self::new_inner(
            version_update_rx,
            compactor_context.sstable_store.clone(),
            state_store_metrics,
            CacheRefillConfig::from_storage_opts(&compactor_context.storage_opts),
            recent_versions,
            buffer_tracker,
            Arc::new(move |payload, task_info| {
                static NEXT_UPLOAD_TASK_ID: LazyLock<AtomicUsize> =
                    LazyLock::new(|| AtomicUsize::new(0));
                let tree_root = upload_compactor_context.await_tree_reg.as_ref().map(|reg| {
                    let upload_task_id = NEXT_UPLOAD_TASK_ID.fetch_add(1, Relaxed);
                    reg.register(
                        await_tree_key::SpawnUploadTask { id: upload_task_id },
                        format!("Spawn Upload Task: {}", task_info),
                    )
                });
                let upload_task_latency = upload_task_latency.clone();
                let wait_poll_latency = wait_poll_latency.clone();
                let upload_compactor_context = upload_compactor_context.clone();
                let compaction_catalog_manager_ref = compaction_catalog_manager_ref.clone();
                let sstable_object_id_manager = sstable_object_id_manager.clone();
                spawn({
                    let future = async move {
                        let _timer = upload_task_latency.start_timer();
                        let mut output = flush_imms(
                            payload
                                .into_values()
                                .flat_map(|imms| imms.into_iter())
                                .collect(),
                            upload_compactor_context.clone(),
                            compaction_catalog_manager_ref.clone(),
                            sstable_object_id_manager.clone(),
                        )
                        .await?;
                        assert!(
                            output
                                .wait_poll_timer
                                .replace(wait_poll_latency.start_timer())
                                .is_none(),
                            "should not set timer before"
                        );
                        Ok(output)
                    };
                    if let Some(tree_root) = tree_root {
                        tree_root.instrument(future).left_future()
                    } else {
                        future.right_future()
                    }
                })
            }),
            CacheRefiller::default_spawn_refill_task(),
        )
    }

    fn new_inner(
        version_update_rx: UnboundedReceiver<HummockVersionUpdate>,
        sstable_store: SstableStoreRef,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        refill_config: CacheRefillConfig,
        recent_versions: RecentVersions,
        buffer_tracker: BufferTracker,
        spawn_upload_task: SpawnUploadTask,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let (hummock_event_tx, hummock_event_rx) =
            event_channel(state_store_metrics.event_handler_pending_event.clone());
        let (version_update_notifier_tx, _) =
            tokio::sync::watch::channel(recent_versions.latest_version().clone());
        let version_update_notifier_tx = Arc::new(version_update_notifier_tx);
        let read_version_mapping = Arc::new(RwLock::new(HashMap::default()));

        let metrics = HummockEventHandlerMetrics {
            event_handler_on_upload_finish_latency: state_store_metrics
                .event_handler_latency
                .with_label_values(&["on_upload_finish"]),
            event_handler_on_apply_version_update: state_store_metrics
                .event_handler_latency
                .with_label_values(&["apply_version"]),
            event_handler_on_recv_version_update: state_store_metrics
                .event_handler_latency
                .with_label_values(&["recv_version_update"]),
        };

        let uploader = HummockUploader::new(
            state_store_metrics.clone(),
            recent_versions.latest_version().clone(),
            spawn_upload_task,
            buffer_tracker,
        );
        let refiller = CacheRefiller::new(refill_config, sstable_store, spawn_refill_task);

        Self {
            hummock_event_tx,
            hummock_event_rx,
            version_update_rx,
            version_update_notifier_tx,
            recent_versions: Arc::new(ArcSwap::from_pointee(recent_versions)),
            read_version_mapping,
            local_read_version_mapping: Default::default(),
            uploader,
            refiller,
            last_instance_id: 0,
            metrics,
        }
    }

    pub fn version_update_notifier_tx(&self) -> Arc<tokio::sync::watch::Sender<PinnedVersion>> {
        self.version_update_notifier_tx.clone()
    }

    pub fn recent_versions(&self) -> Arc<ArcSwap<RecentVersions>> {
        self.recent_versions.clone()
    }

    pub fn read_version_mapping(&self) -> ReadOnlyReadVersionMapping {
        ReadOnlyRwLockRef::new(self.read_version_mapping.clone())
    }

    pub fn event_sender(&self) -> HummockEventSender {
        self.hummock_event_tx.clone()
    }

    pub fn buffer_tracker(&self) -> &BufferTracker {
        self.uploader.buffer_tracker()
    }
}

// Handler for different events
impl HummockEventHandler {
    /// This function will be performed under the protection of the `read_version_mapping` read
    /// lock, and add write lock on each `read_version` operation
    fn for_each_read_version(
        &self,
        instances: impl IntoIterator<Item = LocalInstanceId>,
        mut f: impl FnMut(LocalInstanceId, &mut HummockReadVersion),
    ) {
        let instances = {
            #[cfg(debug_assertions)]
            {
                // check duplication on debug_mode
                let mut id_set = std::collections::HashSet::new();
                for instance in instances {
                    assert!(id_set.insert(instance));
                }
                id_set
            }
            #[cfg(not(debug_assertions))]
            {
                instances
            }
        };
        let mut pending = VecDeque::new();
        let mut total_count = 0;
        for instance_id in instances {
            let Some((_, read_version)) = self.local_read_version_mapping.get(&instance_id) else {
                continue;
            };
            total_count += 1;
            if let Some(mut write_guard) = read_version.try_write() {
                f(instance_id, &mut write_guard);
            } else {
                pending.push_back(instance_id);
            }
        }
        if !pending.is_empty() {
            if pending.len() * 10 > total_count {
                // Only print warn log when failed to acquire more than 10%
                warn!(
                    pending_count = pending.len(),
                    total_count, "cannot acquire lock for all read version"
                );
            } else {
                debug!(
                    pending_count = pending.len(),
                    total_count, "cannot acquire lock for all read version"
                );
            }
        }

        const TRY_LOCK_TIMEOUT: Duration = Duration::from_millis(1);

        while let Some(instance_id) = pending.pop_front() {
            let (_, read_version) = self
                .local_read_version_mapping
                .get(&instance_id)
                .expect("have checked exist before");
            if let Some(mut write_guard) = read_version.try_write_for(TRY_LOCK_TIMEOUT) {
                f(instance_id, &mut write_guard);
            } else {
                warn!(instance_id, "failed to get lock again for instance");
                pending.push_back(instance_id);
            }
        }
    }

    fn handle_uploaded_sst_inner(&mut self, staging_sstable_info: Arc<StagingSstableInfo>) {
        trace!("data_flushed. SST size {}", staging_sstable_info.imm_size());
        self.for_each_read_version(
            staging_sstable_info.imm_ids().keys().cloned(),
            |_, read_version| {
                read_version.update(VersionUpdate::Staging(StagingData::Sst(
                    staging_sstable_info.clone(),
                )))
            },
        )
    }

    fn handle_sync_epoch(
        &mut self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
        sync_result_sender: oneshot::Sender<HummockResult<SyncedData>>,
    ) {
        debug!(?sync_table_epochs, "awaiting for epoch to be synced",);
        self.uploader
            .start_sync_epoch(sync_result_sender, sync_table_epochs);
    }

    fn handle_clear(&mut self, notifier: oneshot::Sender<()>, table_ids: Option<HashSet<TableId>>) {
        info!(
            current_version_id = ?self.uploader.hummock_version().id(),
            "handle clear event"
        );

        self.uploader.clear(table_ids.clone());

        if table_ids.is_none() {
            assert!(
                self.local_read_version_mapping.is_empty(),
                "read version mapping not empty when clear. remaining tables: {:?}",
                self.local_read_version_mapping
                    .values()
                    .map(|(_, read_version)| read_version.read().table_id())
                    .collect_vec()
            );
        }

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });

        info!("clear finished");
    }

    fn handle_version_update(&mut self, version_payload: HummockVersionUpdate) {
        let _timer = self
            .metrics
            .event_handler_on_recv_version_update
            .start_timer();
        let pinned_version = self
            .refiller
            .last_new_pinned_version()
            .cloned()
            .unwrap_or_else(|| self.uploader.hummock_version().clone());

        let mut sst_delta_infos = vec![];
        if let Some(new_pinned_version) = Self::resolve_version_update_info(
            pinned_version.clone(),
            version_payload,
            Some(&mut sst_delta_infos),
        ) {
            self.refiller
                .start_cache_refill(sst_delta_infos, pinned_version, new_pinned_version);
        }
    }

    fn resolve_version_update_info(
        mut pinned_version: PinnedVersion,
        version_payload: HummockVersionUpdate,
        mut sst_delta_infos: Option<&mut Vec<SstDeltaInfo>>,
    ) -> Option<PinnedVersion> {
        match version_payload {
            HummockVersionUpdate::VersionDeltas(version_deltas) => {
                let mut version_to_apply = pinned_version.version().clone();
                let table_change_log_to_apply = pinned_version.take_change_log();
                {
                    let mut table_change_log_to_apply_guard = table_change_log_to_apply.write();
                    for version_delta in &version_deltas {
                        assert_eq!(version_to_apply.id, version_delta.prev_id);

                        // apply change-log-delta
                        {
                            let mut state_table_info = version_to_apply.state_table_info.clone();
                            let (changed_table_info, _is_commit_epoch) = state_table_info
                                .apply_delta(
                                    &version_delta.state_table_info_delta,
                                    &version_delta.removed_table_ids,
                                );

                            HummockVersionCommon::<SstableInfo, SstableInfo>::apply_change_log_delta(
                            &mut *table_change_log_to_apply_guard,
                            &version_delta.change_log_delta,
                            &version_delta.removed_table_ids,
                            &version_delta.state_table_info_delta,
                            &changed_table_info,
                        );
                        }

                        let local_hummock_version_delta =
                            LocalHummockVersionDelta::from(version_delta);
                        if let Some(sst_delta_infos) = &mut sst_delta_infos {
                            sst_delta_infos.extend(
                                version_to_apply
                                    .build_sst_delta_infos(&local_hummock_version_delta)
                                    .into_iter(),
                            );
                        }
                        version_to_apply.apply_version_delta(&local_hummock_version_delta);
                    }
                }

                pinned_version.new_pin_version_with_table_change_log(
                    version_to_apply,
                    table_change_log_to_apply,
                )
            }
            HummockVersionUpdate::PinnedVersion(version) => {
                pinned_version.new_pin_version(*version)
            }
        }
    }

    fn apply_version_update(
        &mut self,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
    ) {
        let _timer = self
            .metrics
            .event_handler_on_apply_version_update
            .start_timer();
        self.recent_versions.rcu(|prev_recent_versions| {
            prev_recent_versions.with_new_version(new_pinned_version.clone())
        });

        {
            self.for_each_read_version(
                self.local_read_version_mapping.keys().cloned(),
                |_, read_version| {
                    read_version
                        .update(VersionUpdate::CommittedSnapshot(new_pinned_version.clone()))
                },
            );
        }

        self.version_update_notifier_tx.send_if_modified(|state| {
            assert_eq!(pinned_version.id(), state.id());
            if state.id() == new_pinned_version.id() {
                return false;
            }
            assert!(new_pinned_version.id() > state.id());
            *state = new_pinned_version.clone();
            true
        });

        debug!("update to hummock version: {}", new_pinned_version.id(),);

        self.uploader.update_pinned_version(new_pinned_version);
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        loop {
            tokio::select! {
                sst = self.uploader.next_uploaded_sst() => {
                    self.handle_uploaded_sst(sst);
                }
                event = self.refiller.next_event() => {
                    let CacheRefillerEvent {pinned_version, new_pinned_version } = event;
                    self.apply_version_update(pinned_version, new_pinned_version);
                }
                event = pin!(self.hummock_event_rx.recv()) => {
                    let Some(event) = event else { break };
                    match event {
                        HummockEvent::Shutdown => {
                            info!("event handler shutdown");
                            return;
                        },
                        event => {
                            self.handle_hummock_event(event);
                        }
                    }
                }
                version_update = pin!(self.version_update_rx.recv()) => {
                    let Some(version_update) = version_update else {
                        warn!("version update stream ends. event handle shutdown");
                        return;
                    };
                    self.handle_version_update(version_update);
                }
            }
        }
    }

    fn handle_uploaded_sst(&mut self, sst: Arc<StagingSstableInfo>) {
        let _timer = self
            .metrics
            .event_handler_on_upload_finish_latency
            .start_timer();
        self.handle_uploaded_sst_inner(sst);
    }

    /// Gracefully shutdown if returns `true`.
    fn handle_hummock_event(&mut self, event: HummockEvent) {
        match event {
            HummockEvent::BufferMayFlush => {
                self.uploader.may_flush();
            }
            HummockEvent::SyncEpoch {
                sync_result_sender,
                sync_table_epochs,
            } => {
                self.handle_sync_epoch(sync_table_epochs, sync_result_sender);
            }
            HummockEvent::Clear(notifier, table_ids) => {
                self.handle_clear(notifier, table_ids);
            }
            HummockEvent::Shutdown => {
                unreachable!("shutdown is handled specially")
            }
            HummockEvent::StartEpoch { epoch, table_ids } => {
                self.uploader.start_epoch(epoch, table_ids);
            }
            HummockEvent::InitEpoch {
                instance_id,
                init_epoch,
            } => {
                let table_id = self
                    .local_read_version_mapping
                    .get(&instance_id)
                    .expect("should exist")
                    .0;
                self.uploader
                    .init_instance(instance_id, table_id, init_epoch);
            }
            HummockEvent::ImmToUploader { instance_id, imm } => {
                assert!(
                    self.local_read_version_mapping.contains_key(&instance_id),
                    "add imm from non-existing read version instance: instance_id: {}, table_id {}",
                    instance_id,
                    imm.table_id,
                );
                self.uploader.add_imm(instance_id, imm);
                self.uploader.may_flush();
            }

            HummockEvent::LocalSealEpoch {
                next_epoch,
                opts,
                instance_id,
            } => {
                self.uploader
                    .local_seal_epoch(instance_id, next_epoch, opts);
            }

            #[cfg(any(test, feature = "test"))]
            HummockEvent::FlushEvent(sender) => {
                let _ = sender.send(()).inspect_err(|e| {
                    error!("unable to send flush result: {:?}", e);
                });
            }

            HummockEvent::RegisterReadVersion {
                table_id,
                new_read_version_sender,
                is_replicated,
                vnodes,
            } => {
                let pinned_version = self.recent_versions.load().latest_version().clone();
                let instance_id = self.generate_instance_id();
                let basic_read_version = Arc::new(RwLock::new(
                    HummockReadVersion::new_with_replication_option(
                        table_id,
                        instance_id,
                        pinned_version,
                        is_replicated,
                        vnodes,
                    ),
                ));

                debug!(
                    "new read version registered: table_id: {}, instance_id: {}",
                    table_id, instance_id
                );

                {
                    self.local_read_version_mapping
                        .insert(instance_id, (table_id, basic_read_version.clone()));
                    let mut read_version_mapping_guard = self.read_version_mapping.write();

                    read_version_mapping_guard
                        .entry(table_id)
                        .or_default()
                        .insert(instance_id, basic_read_version.clone());
                }

                match new_read_version_sender.send((
                    basic_read_version,
                    LocalInstanceGuard {
                        table_id,
                        instance_id,
                        event_sender: Some(self.hummock_event_tx.clone()),
                    },
                )) {
                    Ok(_) => {}
                    Err((_, mut guard)) => {
                        warn!(
                            "RegisterReadVersion send fail table_id {:?} instance_is {:?}",
                            table_id, instance_id
                        );
                        guard.event_sender.take().expect("sender is just set");
                        self.destroy_read_version(instance_id);
                    }
                }
            }

            HummockEvent::DestroyReadVersion { instance_id } => {
                self.uploader.may_destroy_instance(instance_id);
                self.destroy_read_version(instance_id);
            }
            HummockEvent::GetMinUncommittedSstId { result_tx } => {
                let _ = result_tx
                    .send(self.uploader.min_uncommitted_sst_id())
                    .inspect_err(|e| {
                        error!("unable to send get_min_uncommitted_sst_id result: {:?}", e);
                    });
            }
        }
    }

    fn destroy_read_version(&mut self, instance_id: LocalInstanceId) {
        {
            {
                debug!("read version deregister: instance_id: {}", instance_id);
                let (table_id, _) = self
                    .local_read_version_mapping
                    .remove(&instance_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "DestroyHummockInstance inexist instance instance_id {}",
                            instance_id
                        )
                    });
                let mut read_version_mapping_guard = self.read_version_mapping.write();
                let entry = read_version_mapping_guard
                    .get_mut(&table_id)
                    .unwrap_or_else(|| {
                        panic!(
                            "DestroyHummockInstance table_id {} instance_id {} fail",
                            table_id, instance_id
                        )
                    });
                entry.remove(&instance_id).unwrap_or_else(|| {
                    panic!(
                        "DestroyHummockInstance inexist instance table_id {} instance_id {}",
                        table_id, instance_id
                    )
                });
                if entry.is_empty() {
                    read_version_mapping_guard.remove(&table_id);
                }
            }
        }
    }

    fn generate_instance_id(&mut self) -> LocalInstanceId {
        self.last_instance_id += 1;
        self.last_instance_id
    }
}

pub(super) fn send_sync_result(
    sender: oneshot::Sender<HummockResult<SyncedData>>,
    result: HummockResult<SyncedData>,
) {
    let _ = sender.send(result).inspect_err(|e| {
        error!("unable to send sync result. Err: {:?}", e);
    });
}

impl SyncedData {
    pub fn into_sync_result(self) -> SyncResult {
        {
            let SyncedData {
                uploaded_ssts,
                table_watermarks,
            } = self;
            let mut sync_size = 0;
            let mut uncommitted_ssts = Vec::new();
            let mut old_value_ssts = Vec::new();
            // The newly uploaded `sstable_infos` contains newer data. Therefore,
            // `newly_upload_ssts` at the front
            for sst in uploaded_ssts {
                sync_size += sst.imm_size();
                uncommitted_ssts.extend(sst.sstable_infos().iter().cloned());
                old_value_ssts.extend(sst.old_value_sstable_infos().iter().cloned());
            }
            SyncResult {
                sync_size,
                uncommitted_ssts,
                table_watermarks: table_watermarks.clone(),
                old_value_ssts,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::poll_fn;
    use std::sync::Arc;
    use std::task::Poll;

    use futures::FutureExt;
    use parking_lot::Mutex;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::{PbHummockVersion, StateTableInfo};
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;

    use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
    use crate::hummock::event_handler::refiller::{CacheRefillConfig, CacheRefiller};
    use crate::hummock::event_handler::uploader::test_utils::{
        gen_imm, gen_imm_inner, prepare_uploader_order_test_spawn_task_fn, TEST_TABLE_ID,
    };
    use crate::hummock::event_handler::uploader::UploadTaskOutput;
    use crate::hummock::event_handler::{
        HummockEvent, HummockEventHandler, HummockReadVersionRef, LocalInstanceGuard,
    };
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::local_version::recent_versions::RecentVersions;
    use crate::hummock::store::version::{StagingData, VersionUpdate};
    use crate::hummock::test_utils::default_opts_for_test;
    use crate::hummock::HummockError;
    use crate::mem_table::ImmutableMemtable;
    use crate::monitor::HummockStateStoreMetrics;
    use crate::store::SealCurrentEpochOptions;

    #[tokio::test]
    async fn test_old_epoch_sync_fail() {
        let epoch0 = test_epoch(233);

        let initial_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1,
                state_table_info: HashMap::from_iter([(
                    TEST_TABLE_ID.table_id,
                    StateTableInfo {
                        committed_epoch: epoch0,
                        compaction_group_id: StaticCompactionGroupId::StateDefault as _,
                    },
                )]),
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let (_version_update_tx, version_update_rx) = unbounded_channel();

        let epoch1 = epoch0.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(Mutex::new(Some(rx)));

        let storage_opt = default_opts_for_test();
        let metrics = Arc::new(HummockStateStoreMetrics::unused());

        let event_handler = HummockEventHandler::new_inner(
            version_update_rx,
            mock_sstable_store().await,
            metrics.clone(),
            CacheRefillConfig::from_storage_opts(&storage_opt),
            RecentVersions::new(initial_version.clone(), 10, metrics.clone()),
            BufferTracker::from_storage_opts(
                &storage_opt,
                metrics.uploader_uploading_task_size.clone(),
            ),
            Arc::new(move |_, info| {
                assert_eq!(info.epochs.len(), 1);
                let epoch = info.epochs[0];
                match epoch {
                    epoch if epoch == epoch1 => {
                        let rx = rx.lock().take().unwrap();
                        spawn(async move {
                            rx.await.unwrap();
                            Err(HummockError::other("fail"))
                        })
                    }
                    epoch if epoch == epoch2 => spawn(async move {
                        Ok(UploadTaskOutput {
                            new_value_ssts: vec![],
                            old_value_ssts: vec![],
                            wait_poll_timer: None,
                        })
                    }),
                    _ => unreachable!(),
                }
            }),
            CacheRefiller::default_spawn_refill_task(),
        );

        let event_tx = event_handler.event_sender();

        let send_event = |event| event_tx.send(event).unwrap();

        let join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        let (read_version, guard) = {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::RegisterReadVersion {
                table_id: TEST_TABLE_ID,
                new_read_version_sender: tx,
                is_replicated: false,
                vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            });
            rx.await.unwrap()
        };

        send_event(HummockEvent::StartEpoch {
            epoch: epoch1,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });

        send_event(HummockEvent::InitEpoch {
            instance_id: guard.instance_id,
            init_epoch: epoch1,
        });

        let imm1 = gen_imm(epoch1).await;
        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm1.clone())));

        send_event(HummockEvent::ImmToUploader {
            instance_id: guard.instance_id,
            imm: imm1,
        });

        send_event(HummockEvent::StartEpoch {
            epoch: epoch2,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });

        send_event(HummockEvent::LocalSealEpoch {
            instance_id: guard.instance_id,
            next_epoch: epoch2,
            opts: SealCurrentEpochOptions::for_test(),
        });

        let imm2 = gen_imm(epoch2).await;
        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm2.clone())));

        send_event(HummockEvent::ImmToUploader {
            instance_id: guard.instance_id,
            imm: imm2,
        });

        let epoch3 = epoch2.next_epoch();
        send_event(HummockEvent::StartEpoch {
            epoch: epoch3,
            table_ids: HashSet::from_iter([TEST_TABLE_ID]),
        });
        send_event(HummockEvent::LocalSealEpoch {
            instance_id: guard.instance_id,
            next_epoch: epoch3,
            opts: SealCurrentEpochOptions::for_test(),
        });

        let (tx1, mut rx1) = oneshot::channel();
        send_event(HummockEvent::SyncEpoch {
            sync_result_sender: tx1,
            sync_table_epochs: vec![(epoch1, HashSet::from_iter([TEST_TABLE_ID]))],
        });
        assert!(poll_fn(|cx| Poll::Ready(rx1.poll_unpin(cx).is_pending())).await);
        let (tx2, mut rx2) = oneshot::channel();
        send_event(HummockEvent::SyncEpoch {
            sync_result_sender: tx2,
            sync_table_epochs: vec![(epoch2, HashSet::from_iter([TEST_TABLE_ID]))],
        });
        assert!(poll_fn(|cx| Poll::Ready(rx2.poll_unpin(cx).is_pending())).await);

        tx.send(()).unwrap();
        rx1.await.unwrap().unwrap_err();
        rx2.await.unwrap().unwrap_err();

        send_event(HummockEvent::Shutdown);
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_clear_tables() {
        let table_id1 = TableId::new(1);
        let table_id2 = TableId::new(2);
        let epoch0 = test_epoch(233);

        let initial_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1,
                state_table_info: HashMap::from_iter([
                    (
                        table_id1.table_id,
                        StateTableInfo {
                            committed_epoch: epoch0,
                            compaction_group_id: StaticCompactionGroupId::StateDefault as _,
                        },
                    ),
                    (
                        table_id2.table_id,
                        StateTableInfo {
                            committed_epoch: epoch0,
                            compaction_group_id: StaticCompactionGroupId::StateDefault as _,
                        },
                    ),
                ]),
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let (_version_update_tx, version_update_rx) = unbounded_channel();

        let epoch1 = epoch0.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();

        let imm_size = gen_imm_inner(TEST_TABLE_ID, epoch1, 0, None).await.size();

        // The buffer can hold at most 1 imm. When a new imm is added, the previous one will be spilled, and the newly added one will be retained.
        let buffer_tracker = BufferTracker::for_test_with_config(imm_size * 2 - 1, 1);
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();

        let gen_imm = |table_id, epoch, spill_offset| {
            let imm = gen_imm_inner(table_id, epoch, spill_offset, Some(&*memory_limiter))
                .now_or_never()
                .unwrap();
            assert_eq!(imm.size(), imm_size);
            imm
        };
        let imm1_1 = gen_imm(table_id1, epoch1, 0);
        let imm1_2_1 = gen_imm(table_id1, epoch2, 0);

        let storage_opt = default_opts_for_test();
        let metrics = Arc::new(HummockStateStoreMetrics::unused());

        let (spawn_task, new_task_notifier) = prepare_uploader_order_test_spawn_task_fn(false);

        let event_handler = HummockEventHandler::new_inner(
            version_update_rx,
            mock_sstable_store().await,
            metrics.clone(),
            CacheRefillConfig::from_storage_opts(&storage_opt),
            RecentVersions::new(initial_version.clone(), 10, metrics.clone()),
            buffer_tracker,
            spawn_task,
            CacheRefiller::default_spawn_refill_task(),
        );

        let event_tx = event_handler.event_sender();

        let send_event = |event| event_tx.send(event).unwrap();
        let flush_event = || async {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::FlushEvent(tx));
            rx.await.unwrap();
        };
        let start_epoch = |table_id, epoch| {
            send_event(HummockEvent::StartEpoch {
                epoch,
                table_ids: HashSet::from_iter([table_id]),
            })
        };
        let init_epoch = |instance: &LocalInstanceGuard, init_epoch| {
            send_event(HummockEvent::InitEpoch {
                instance_id: instance.instance_id,
                init_epoch,
            })
        };
        let write_imm = |read_version: &HummockReadVersionRef,
                         instance: &LocalInstanceGuard,
                         imm: &ImmutableMemtable| {
            read_version
                .write()
                .update(VersionUpdate::Staging(StagingData::ImmMem(imm.clone())));

            send_event(HummockEvent::ImmToUploader {
                instance_id: instance.instance_id,
                imm: imm.clone(),
            });
        };
        let seal_epoch = |instance: &LocalInstanceGuard, next_epoch| {
            send_event(HummockEvent::LocalSealEpoch {
                instance_id: instance.instance_id,
                next_epoch,
                opts: SealCurrentEpochOptions::for_test(),
            })
        };
        let sync_epoch = |table_id, new_sync_epoch| {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::SyncEpoch {
                sync_result_sender: tx,
                sync_table_epochs: vec![(new_sync_epoch, HashSet::from_iter([table_id]))],
            });
            rx
        };

        let join_handle = spawn(event_handler.start_hummock_event_handler_worker());

        let (read_version1, guard1) = {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::RegisterReadVersion {
                table_id: table_id1,
                new_read_version_sender: tx,
                is_replicated: false,
                vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            });
            rx.await.unwrap()
        };

        let (read_version2, guard2) = {
            let (tx, rx) = oneshot::channel();
            send_event(HummockEvent::RegisterReadVersion {
                table_id: table_id2,
                new_read_version_sender: tx,
                is_replicated: false,
                vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            });
            rx.await.unwrap()
        };

        // prepare data of table1
        let (task1_1_finish_tx, task1_1_rx) = {
            start_epoch(table_id1, epoch1);

            init_epoch(&guard1, epoch1);

            write_imm(&read_version1, &guard1, &imm1_1);

            start_epoch(table_id1, epoch2);

            seal_epoch(&guard1, epoch2);

            let (wait_task_start, task_finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard1.instance_id,
                vec![imm1_1.batch_id()],
            )]));

            let mut rx = sync_epoch(table_id1, epoch1);
            wait_task_start.await;
            assert!(poll_fn(|cx| Poll::Ready(rx.poll_unpin(cx).is_pending())).await);

            write_imm(&read_version1, &guard1, &imm1_2_1);
            flush_event().await;

            (task_finish_tx, rx)
        };
        // by now, the state in uploader of table_id1
        // unsync:  epoch2 -> [imm1_2]
        // syncing: epoch1 -> [imm1_1]

        let (task1_2_finish_tx, _finish_txs) = {
            let mut finish_txs = vec![];
            let imm2_1_1 = gen_imm(table_id2, epoch1, 0);
            start_epoch(table_id2, epoch1);
            init_epoch(&guard2, epoch1);
            let (wait_task_start, task1_2_finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard1.instance_id,
                vec![imm1_2_1.batch_id()],
            )]));
            write_imm(&read_version2, &guard2, &imm2_1_1);
            wait_task_start.await;

            let imm2_1_2 = gen_imm(table_id2, epoch1, 1);
            let (wait_task_start, finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard2.instance_id,
                vec![imm2_1_2.batch_id(), imm2_1_1.batch_id()],
            )]));
            finish_txs.push(finish_tx);
            write_imm(&read_version2, &guard2, &imm2_1_2);
            wait_task_start.await;

            let imm2_1_3 = gen_imm(table_id2, epoch1, 2);
            write_imm(&read_version2, &guard2, &imm2_1_3);
            start_epoch(table_id2, epoch2);
            seal_epoch(&guard2, epoch2);
            let (wait_task_start, finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard2.instance_id,
                vec![imm2_1_3.batch_id()],
            )]));
            finish_txs.push(finish_tx);
            let _sync_rx = sync_epoch(table_id2, epoch1);
            wait_task_start.await;

            let imm2_2_1 = gen_imm(table_id2, epoch2, 0);
            write_imm(&read_version2, &guard2, &imm2_2_1);
            flush_event().await;
            let imm2_2_2 = gen_imm(table_id2, epoch2, 1);
            write_imm(&read_version2, &guard2, &imm2_2_2);
            let (wait_task_start, finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard2.instance_id,
                vec![imm2_2_2.batch_id(), imm2_2_1.batch_id()],
            )]));
            finish_txs.push(finish_tx);
            wait_task_start.await;

            let imm2_2_3 = gen_imm(table_id2, epoch2, 2);
            write_imm(&read_version2, &guard2, &imm2_2_3);

            // by now, the state in uploader of table_id2
            // syncing: epoch1 -> spill: [imm2_1_2, imm2_1_1], sync: [imm2_1_3]
            // unsync: epoch2 -> spilling: [imm2_2_2, imm2_2_1], imm: [imm2_2_3]
            // the state in uploader of table_id1
            // unsync:  epoch2 -> spilling [imm1_2]
            // syncing: epoch1 -> [imm1_1]

            drop(guard2);
            let (clear_tx, clear_rx) = oneshot::channel();
            send_event(HummockEvent::Clear(
                clear_tx,
                Some(HashSet::from_iter([table_id2])),
            ));
            clear_rx.await.unwrap();
            (task1_2_finish_tx, finish_txs)
        };

        let imm1_2_2 = gen_imm(table_id1, epoch2, 1);
        write_imm(&read_version1, &guard1, &imm1_2_2);
        start_epoch(table_id1, epoch3);
        seal_epoch(&guard1, epoch3);

        let (tx2, mut sync_rx2) = oneshot::channel();
        let (wait_task_start, task1_2_2_finish_tx) = new_task_notifier(HashMap::from_iter([(
            guard1.instance_id,
            vec![imm1_2_2.batch_id()],
        )]));
        send_event(HummockEvent::SyncEpoch {
            sync_result_sender: tx2,
            sync_table_epochs: vec![(epoch2, HashSet::from_iter([table_id1]))],
        });
        wait_task_start.await;
        assert!(poll_fn(|cx| Poll::Ready(sync_rx2.poll_unpin(cx).is_pending())).await);

        task1_1_finish_tx.send(()).unwrap();
        let sync_data1 = task1_1_rx.await.unwrap().unwrap();
        sync_data1
            .uploaded_ssts
            .iter()
            .all(|sst| sst.epochs() == &vec![epoch1]);
        task1_2_finish_tx.send(()).unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx2.poll_unpin(cx).is_pending())).await);
        task1_2_2_finish_tx.send(()).unwrap();
        let sync_data2 = sync_rx2.await.unwrap().unwrap();
        sync_data2
            .uploaded_ssts
            .iter()
            .all(|sst| sst.epochs() == &vec![epoch2]);

        send_event(HummockEvent::Shutdown);
        join_handle.await.unwrap();
    }
}
