// Copyright 2022 RisingWave Labs
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

use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use await_tree::{InstrumentAwait, SpanExt};
use futures::FutureExt;
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::{Histogram, IntGauge, IntGaugeVec};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::metrics::UintGauge;
use risingwave_hummock_sdk::{HummockEpoch, SyncResult};
use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
use risingwave_pb::meta::{PbServingTableVnodeMappings, PbTableRefillRuntimeConfig};
use tokio::spawn;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tracing::{debug, error, info, trace, warn};

use super::refiller::{CacheRefillConfig, CacheRefiller, table_cache_refill_monitor_snapshot};
use super::version_update::{ResolvedVersionUpdate, VersionUpdateEvent, VersionUpdateQueue};
use super::{LocalInstanceGuard, LocalInstanceId, ReadVersionMappingType};
use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::compactor::{CompactorContext, await_tree_key, compact};
use crate::hummock::event_handler::pin_cache_membership::PinCacheMembership;
use crate::hummock::event_handler::uploader::{
    HummockUploader, SpawnUploadTask, SyncedData, UploadTaskOutput,
};
use crate::hummock::event_handler::{
    HummockEvent, HummockObserverEvent, HummockReadVersionRef, HummockVersionUpdate,
    ReadOnlyReadVersionMapping, ReadOnlyRwLockRef,
};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::store::version::{HummockReadVersion, StagingSstableInfo, VersionUpdate};
use crate::hummock::table_cache_metadata::{SharedTableCacheMetadata, TableCacheMetadata};
use crate::hummock::{HummockResult, MemoryLimiter, ObjectIdManager};
use crate::mem_table::ImmutableMemtable;
use crate::monitor::HummockStateStoreMetrics;
use crate::opts::StorageOpts;

#[derive(Clone)]
pub(crate) struct BufferTracker {
    flush_threshold: usize,
    min_batch_flush_size: usize,
    global_uploading_memory_limiter: Arc<MemoryLimiter>,
    uploader_imm_size: UintGauge,
    uploader_uploading_task_size: UintGauge,
}

impl BufferTracker {
    pub fn from_storage_opts(config: &StorageOpts, metrics: &HummockStateStoreMetrics) -> Self {
        let capacity = config.shared_buffer_capacity_mb * (1 << 20);
        let flush_threshold = (capacity as f32 * config.shared_buffer_flush_ratio) as usize;
        let min_batch_flush_size = config.shared_buffer_min_batch_flush_size_mb * (1 << 20);
        assert!(
            flush_threshold < capacity,
            "flush_threshold {} should be less or equal to capacity {}",
            flush_threshold,
            capacity
        );
        Self {
            flush_threshold,
            min_batch_flush_size,
            global_uploading_memory_limiter: Arc::new(MemoryLimiter::new(capacity as u64)),
            uploader_imm_size: metrics.uploader_imm_size.clone(),
            uploader_uploading_task_size: metrics.uploader_uploading_task_size.clone(),
        }
    }

    #[cfg(test)]
    fn for_test_with_config(flush_threshold: usize, min_batch_flush_size: usize) -> Self {
        Self {
            flush_threshold,
            min_batch_flush_size,
            ..Self::for_test()
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::from_storage_opts(&StorageOpts::default(), &HummockStateStoreMetrics::unused())
    }

    pub fn get_memory_limiter(&self) -> &Arc<MemoryLimiter> {
        &self.global_uploading_memory_limiter
    }

    pub fn global_upload_task_size(&self) -> &UintGauge {
        &self.uploader_uploading_task_size
    }

    /// Return true when the buffer size minus current upload task size is still greater than the
    /// flush threshold.
    pub fn need_flush(&self) -> bool {
        self.uploader_imm_size.get()
            > self.flush_threshold as u64 + self.uploader_uploading_task_size.get()
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
    event_count: IntGaugeVec,
}

pub fn event_channel(event_count: IntGaugeVec) -> (HummockEventSender, HummockEventReceiver) {
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
        let event_type = event.event_name();
        self.inner.send(event)?;
        get_event_pending_gauge(&self.event_count, event_type).inc();
        Ok(())
    }
}

pub struct HummockEventReceiver {
    inner: UnboundedReceiver<HummockEvent>,
    event_count: IntGaugeVec,
}

impl HummockEventReceiver {
    async fn recv(&mut self) -> Option<HummockEvent> {
        let event = self.inner.recv().await?;
        let event_type = event.event_name();
        get_event_pending_gauge(&self.event_count, event_type).dec();
        Some(event)
    }
}

thread_local! {
    static EVENT_PENDING_GAUGE_CACHE: RefCell<HashMap<&'static str, IntGauge>> = RefCell::new(HashMap::new());
}

fn get_event_pending_gauge(event_count: &IntGaugeVec, event_type: &'static str) -> IntGauge {
    EVENT_PENDING_GAUGE_CACHE.with(|cache| {
        cache
            .borrow_mut()
            .entry(event_type)
            .or_insert_with(|| event_count.with_label_values(&[event_type]))
            .clone()
    })
}

struct HummockEventHandlerMetrics {
    event_handler_on_upload_finish_latency: Histogram,
    event_handler_on_apply_version_update: Histogram,
    event_handler_on_recv_version_update: Histogram,
    event_handler_on_spiller: Histogram,
    event_handler_on_apply_table_refill_runtime_config: Histogram,
}

fn table_cache_refill_policy_from_protobuf(
    pb_policy: risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy,
) -> Option<(TableId, CacheRefillPolicy)> {
    let table_id = TableId::from(pb_policy.table_id);
    let policy = match PbCacheRefillPolicy::try_from(pb_policy.policy) {
        Ok(policy) => CacheRefillPolicy::from_protobuf(policy)?,
        Err(_) => {
            tracing::warn!(
                table_id = pb_policy.table_id,
                policy = pb_policy.policy,
                "ignore unknown table cache refill policy"
            );
            return None;
        }
    };
    Some((table_id, policy))
}

fn serving_table_vnode_mappings_to_map(
    mappings: PbServingTableVnodeMappings,
) -> HashMap<TableId, Bitmap> {
    mappings
        .mappings
        .into_iter()
        .filter_map(|mapping| {
            let Some(bitmap) = mapping.bitmap else {
                tracing::warn!(
                    table_id = mapping.table_id,
                    "ignore serving table vnode mapping without bitmap"
                );
                return None;
            };
            Some((TableId::from(mapping.table_id), Bitmap::from(bitmap)))
        })
        .collect()
}

#[derive(Default)]
struct LocalReadVersions {
    by_instance: HashMap<LocalInstanceId, (TableId, HummockReadVersionRef)>,
    by_table: HashMap<TableId, HashMap<LocalInstanceId, HummockReadVersionRef>>,
}

impl LocalReadVersions {
    fn insert(
        &mut self,
        instance_id: LocalInstanceId,
        table_id: TableId,
        read_version: HummockReadVersionRef,
    ) {
        assert!(
            !self.by_instance.contains_key(&instance_id),
            "duplicated local read version instance_id {}",
            instance_id
        );
        assert!(
            !self
                .by_table
                .get(&table_id)
                .is_some_and(|read_versions| read_versions.contains_key(&instance_id)),
            "duplicated local read version table_id {} instance_id {}",
            table_id,
            instance_id
        );
        self.by_instance
            .insert(instance_id, (table_id, read_version.clone()));
        self.by_table
            .entry(table_id)
            .or_default()
            .insert(instance_id, read_version);
    }

    fn remove(&mut self, instance_id: LocalInstanceId) -> (TableId, HummockReadVersionRef) {
        let (table_id, read_version) = self.by_instance.remove(&instance_id).unwrap_or_else(|| {
            panic!(
                "DestroyHummockInstance nonexistent instance instance_id {}",
                instance_id
            )
        });
        let table_read_versions = self.by_table.get_mut(&table_id).unwrap_or_else(|| {
            panic!(
                "DestroyHummockInstance table_id {} instance_id {} fail in local mapping",
                table_id, instance_id
            )
        });
        let removed = table_read_versions.remove(&instance_id).unwrap_or_else(|| {
            panic!(
                "DestroyHummockInstance nonexistent instance in local mapping table_id {} instance_id {}",
                table_id, instance_id
            )
        });
        debug_assert!(Arc::ptr_eq(&read_version, &removed));
        if table_read_versions.is_empty() {
            self.by_table.remove(&table_id);
        }
        (table_id, read_version)
    }

    fn get(&self, instance_id: &LocalInstanceId) -> Option<&(TableId, HummockReadVersionRef)> {
        self.by_instance.get(instance_id)
    }

    fn contains_instance(&self, instance_id: &LocalInstanceId) -> bool {
        self.by_instance.contains_key(instance_id)
    }

    fn instance_ids(&self) -> impl Iterator<Item = LocalInstanceId> + '_ {
        self.by_instance.keys().copied()
    }

    fn is_empty(&self) -> bool {
        self.by_instance.is_empty() && self.by_table.is_empty()
    }

    fn table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.by_table.keys().copied()
    }

    fn table_vnodes(&self, table_id: TableId) -> Option<Bitmap> {
        let read_versions = self.by_table.get(&table_id)?;
        let mut vnodes = None;
        for read_version in read_versions.values() {
            let read_version_vnodes = read_version.read().vnodes();
            let current_vnodes =
                vnodes.get_or_insert_with(|| Bitmap::zeros(read_version_vnodes.as_ref().len()));
            *current_vnodes |= read_version_vnodes.as_ref();
        }
        vnodes
    }
}

pub(crate) struct HummockEventHandler {
    hummock_event_tx: HummockEventSender,
    hummock_event_rx: HummockEventReceiver,
    observer_event_rx: UnboundedReceiver<HummockObserverEvent>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    /// A copy of `read_version_mapping` but owned by event handler
    local_read_versions: LocalReadVersions,

    version_update_notifier_tx: Arc<tokio::sync::watch::Sender<PinnedVersion>>,
    recent_versions: Arc<ArcSwap<RecentVersions>>,

    uploader: HummockUploader,
    refiller: CacheRefiller,
    cache_metadata: SharedTableCacheMetadata,
    pin_membership: Option<PinCacheMembership>,
    version_updates: VersionUpdateQueue,

    last_instance_id: LocalInstanceId,

    metrics: HummockEventHandlerMetrics,
}

async fn flush_imms(
    payload: Vec<ImmutableMemtable>,
    compactor_context: CompactorContext,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    object_id_manager: Arc<ObjectIdManager>,
) -> HummockResult<UploadTaskOutput> {
    compact(
        compactor_context,
        object_id_manager,
        payload,
        compaction_catalog_manager_ref,
    )
    .instrument_await("shared_buffer_compact".verbose())
    .await
}

impl HummockEventHandler {
    // Table refill runtime config applies by field presence for both snapshot
    // and update notifications.
    fn apply_table_refill_runtime_config(&mut self, config: PbTableRefillRuntimeConfig) {
        if let Some(policies) = config.table_cache_refill_policies {
            let pinned_table_ids = policies
                .pinned_table_ids
                .into_iter()
                .map(TableId::new)
                .collect();
            let foyer_policies = policies
                .table_policies
                .into_iter()
                .chain(policies.internal_table_policies)
                .filter_map(table_cache_refill_policy_from_protobuf)
                .collect();
            self.replace_table_cache_refill_runtime_snapshot(foyer_policies, pinned_table_ids);
        }
        if let Some(mappings) = config.serving_table_vnode_mappings {
            self.replace_serving_table_vnode_mapping(serving_table_vnode_mappings_to_map(mappings));
        }
    }

    pub(super) fn replace_table_cache_refill_runtime_snapshot(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
        mut pinned: HashSet<TableId>,
    ) {
        let mut metadata = self.cache_metadata.write();
        if let Some(membership) = &mut self.pin_membership {
            membership.replace_pinned_tables(metadata.pinned_tables(), &pinned);
        } else {
            pinned.clear();
        }
        let changed = metadata.replace_policy_snapshot(policies, pinned);
        self.refiller.on_metadata_changed(&changed, &metadata);
    }

    fn replace_serving_table_vnode_mapping(&mut self, mapping: HashMap<TableId, Bitmap>) {
        let mut metadata = self.cache_metadata.write();
        let changed = metadata.replace_serving_mapping(mapping);
        self.refiller.on_metadata_changed(&changed, &metadata);
    }

    fn update_streaming_table_vnodes(&mut self, table: TableId) {
        let bitmap = self.streaming_table_vnodes(table);
        let mut metadata = self.cache_metadata.write();
        let changed = metadata.update_streaming_table_vnodes(table, bitmap);
        self.refiller.on_metadata_changed(&changed, &metadata);
    }

    pub fn new(
        role: Role,
        observer_event_rx: UnboundedReceiver<HummockObserverEvent>,
        pinned_version: PinnedVersion,
        compactor_context: CompactorContext,
        compaction_catalog_manager_ref: CompactionCatalogManagerRef,
        object_id_manager: Arc<ObjectIdManager>,
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
        let buffer_tracker =
            BufferTracker::from_storage_opts(&compactor_context.storage_opts, &state_store_metrics);
        let refill_config = CacheRefillConfig::from_storage_opts(&compactor_context.storage_opts);
        let refill_store = compactor_context.sstable_store;
        let refill_version = recent_versions.latest_version().clone();
        let pin_membership = refill_store
            .pin_cache()
            .map(|cache| PinCacheMembership::new(cache.clone(), refill_version));
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            role,
            refill_config.table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        );
        Self::new_inner(
            observer_event_rx,
            state_store_metrics,
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
                let object_id_manager = object_id_manager.clone();
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
                            object_id_manager.clone(),
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
            refiller,
            cache_metadata,
            pin_membership,
        )
    }

    fn new_inner(
        observer_event_rx: UnboundedReceiver<HummockObserverEvent>,
        state_store_metrics: Arc<HummockStateStoreMetrics>,
        recent_versions: RecentVersions,
        buffer_tracker: BufferTracker,
        spawn_upload_task: SpawnUploadTask,
        refiller: CacheRefiller,
        cache_metadata: SharedTableCacheMetadata,
        pin_membership: Option<PinCacheMembership>,
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
            event_handler_on_spiller: state_store_metrics
                .event_handler_latency
                .with_label_values(&["spiller"]),
            event_handler_on_apply_table_refill_runtime_config: state_store_metrics
                .event_handler_latency
                .with_label_values(&["apply_table_refill_runtime_config"]),
        };

        let uploader = HummockUploader::new(
            state_store_metrics,
            recent_versions.latest_version().clone(),
            spawn_upload_task,
            buffer_tracker,
        );
        let version_updates = VersionUpdateQueue::new(recent_versions.latest_version().clone());

        Self {
            hummock_event_tx,
            hummock_event_rx,
            observer_event_rx,
            version_update_notifier_tx,
            recent_versions: Arc::new(ArcSwap::from_pointee(recent_versions)),
            read_version_mapping,
            local_read_versions: Default::default(),
            uploader,
            refiller,
            cache_metadata,
            pin_membership,
            version_updates,
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
    /// Applies the function to local read versions selected by instance ids.
    /// Each read version is protected by its own write lock.
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
            let Some((_, read_version)) = self.local_read_versions.get(&instance_id) else {
                continue;
            };
            total_count += 1;
            match read_version.try_write() {
                Some(mut write_guard) => {
                    f(instance_id, &mut write_guard);
                }
                _ => {
                    pending.push_back(instance_id);
                }
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
                .local_read_versions
                .get(&instance_id)
                .expect("have checked exist before");
            match read_version.try_write_for(TRY_LOCK_TIMEOUT) {
                Some(mut write_guard) => {
                    f(instance_id, &mut write_guard);
                }
                _ => {
                    warn!(instance_id, "failed to get lock again for instance");
                    pending.push_back(instance_id);
                }
            }
        }
    }

    fn handle_uploaded_ssts_inner(&mut self, ssts: Vec<Arc<StagingSstableInfo>>) {
        match ssts.as_slice() {
            [] => {
                if cfg!(debug_assertions) {
                    panic!("empty ssts")
                }
            }
            [staging_sstable_info] => {
                trace!("data_flushed. SST size {}", staging_sstable_info.imm_size());
                self.for_each_read_version(
                    staging_sstable_info.imm_ids().keys().cloned(),
                    |_, read_version| {
                        read_version.update(VersionUpdate::Sst(staging_sstable_info.clone()))
                    },
                )
            }
            ssts => {
                warn!(
                    batch_size = ssts.len(),
                    "handle multiple uploaded ssts in batch"
                );
                let affected_instances: HashSet<_> = ssts
                    .iter()
                    .flat_map(|sst| {
                        trace!("data_flushed. SST size {}", sst.imm_size());
                        sst.imm_ids().keys()
                    })
                    .copied()
                    .collect();
                self.for_each_read_version(affected_instances, |instance_id, read_version| {
                    for sst in ssts {
                        if sst.imm_ids().contains_key(&instance_id) {
                            read_version.update(VersionUpdate::Sst(sst.clone()));
                        }
                    }
                })
            }
        }
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
            ?table_ids,
            "handle clear event"
        );

        self.uploader.clear(table_ids.clone());

        if table_ids.is_none() {
            assert!(
                self.local_read_versions.is_empty(),
                "read version mapping not empty when clear. remaining tables: {:?}",
                self.local_read_versions.table_ids().collect_vec()
            );
        }

        // Notify completion of the Clear event.
        let _ = notifier.send(()).inspect_err(|e| {
            error!("failed to notify completion of clear event: {:?}", e);
        });

        info!("clear finished");
    }

    pub(crate) fn handle_observer_event(&mut self, event: HummockObserverEvent) {
        match event {
            HummockObserverEvent::VersionUpdate(version_update) => {
                self.handle_version_update(version_update);
            }
            HummockObserverEvent::TableRefillRuntimeConfig(_, config) => {
                let _timer = self
                    .metrics
                    .event_handler_on_apply_table_refill_runtime_config
                    .start_timer();
                self.apply_table_refill_runtime_config(config);
            }
        }
    }

    fn streaming_table_vnodes(&self, table_id: TableId) -> Option<Bitmap> {
        self.local_read_versions.table_vnodes(table_id)
    }

    fn handle_version_update(&mut self, version_payload: HummockVersionUpdate) {
        let _timer = self
            .metrics
            .event_handler_on_recv_version_update
            .start_timer();
        let mut sst_delta_infos = Vec::new();
        if let Some(ResolvedVersionUpdate {
            event,
            version_deltas,
        }) = self
            .version_updates
            .resolve_update(version_payload, |before, delta| {
                sst_delta_infos.extend(before.build_sst_delta_infos(delta));
            })
        {
            let metadata = self.cache_metadata.read();
            if let Some(membership) = &mut self.pin_membership {
                membership.advance_version(
                    &sst_delta_infos,
                    event.new_pinned_version.clone(),
                    version_deltas.as_deref(),
                    metadata.pinned_tables(),
                );
            }
            // Membership advances independently; shared metadata is read-only during submission.
            let condition = self.refiller.start_cache_refill(
                sst_delta_infos,
                event.new_pinned_version.clone(),
                version_deltas.as_deref(),
                &metadata,
            );
            self.version_updates.enqueue(event, condition);
        }
    }

    /// Publishes a non-empty FIFO batch, keeping its targets pinned until all holders advance.
    fn publish_version_updates(&mut self, events: Vec<VersionUpdateEvent>) {
        let (first_event, remaining_events) = events
            .split_first()
            .expect("version update batch must not be empty");
        let latest_pinned_version = &events.last().unwrap().new_pinned_version;
        if events.len() > 1 {
            warn!(
                count = events.len(),
                "handle multiple version updates in batch"
            );
        }
        let _timer = self
            .metrics
            .event_handler_on_apply_version_update
            .start_timer();
        self.recent_versions.rcu(|prev_recent_versions| {
            let mut recent_versions =
                prev_recent_versions.with_new_version(first_event.new_pinned_version.clone());
            for event in remaining_events {
                recent_versions =
                    recent_versions.with_new_version(event.new_pinned_version.clone());
            }
            recent_versions
        });

        self.for_each_read_version(
            self.local_read_versions.instance_ids(),
            |_, read_version| {
                for event in &events {
                    read_version.update(VersionUpdate::CommittedSnapshot(
                        event.new_pinned_version.clone(),
                    ));
                }
            },
        );

        self.version_update_notifier_tx.send_modify(|state| {
            assert_eq!(first_event.prev_version_id, state.id());
            assert!(latest_pinned_version.id() > state.id());
            // The queue enforces the chain; watch readers only observe the batch's final version.
            *state = latest_pinned_version.clone();
        });

        debug!("update to hummock version: {}", latest_pinned_version.id());

        self.uploader
            .update_pinned_version(latest_pinned_version.clone());
        if let Some(membership) = &mut self.pin_membership {
            let removed = membership.on_version_published(latest_pinned_version.id());
            self.refiller.cancel_objects(removed);
        }
    }
}

impl HummockEventHandler {
    pub async fn start_hummock_event_handler_worker(mut self) {
        loop {
            tokio::select! {
                ssts = self.uploader.next_uploaded_ssts() => {
                    self.handle_uploaded_ssts(ssts);
                }
                events = self.version_updates.next_events() => {
                    self.publish_version_updates(events);
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
                observer_event = pin!(self.observer_event_rx.recv()) => {
                    let Some(observer_event) = observer_event else {
                        warn!("observer event stream ends. event handle shutdown");
                        return;
                    };
                    self.handle_observer_event(observer_event);
                }
            }
        }
    }

    fn handle_uploaded_ssts(&mut self, ssts: Vec<Arc<StagingSstableInfo>>) {
        let _timer = self
            .metrics
            .event_handler_on_upload_finish_latency
            .start_timer();
        self.handle_uploaded_ssts_inner(ssts);
    }

    /// Gracefully shutdown if returns `true`.
    fn handle_hummock_event(&mut self, event: HummockEvent) {
        match event {
            HummockEvent::BufferMayFlush => {
                self.uploader
                    .may_flush(&self.metrics.event_handler_on_spiller);
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
                    .local_read_versions
                    .get(&instance_id)
                    .expect("should exist")
                    .0;
                self.uploader
                    .init_instance(instance_id, table_id, init_epoch);
            }
            HummockEvent::ImmToUploader { instance_id, imms } => {
                assert!(
                    self.local_read_versions.contains_instance(&instance_id),
                    "add imm from non-existing read version instance: instance_id: {}, table_id {:?}",
                    instance_id,
                    imms.first().map(|(imm, _)| imm.table_id),
                );
                self.uploader.add_imms(instance_id, imms);
                self.uploader
                    .may_flush(&self.metrics.event_handler_on_spiller);
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
                    self.local_read_versions.insert(
                        instance_id,
                        table_id,
                        basic_read_version.clone(),
                    );
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

                self.update_streaming_table_vnodes(table_id);
            }

            HummockEvent::DestroyReadVersion { instance_id } => {
                let table_id = self
                    .local_read_versions
                    .get(&instance_id)
                    .unwrap_or_else(|| {
                        panic!("query nonexistent instance instance_id {instance_id}")
                    })
                    .0;
                self.uploader.may_destroy_instance(instance_id);
                self.destroy_read_version(instance_id);
                self.update_streaming_table_vnodes(table_id);
            }
            HummockEvent::GetMinUncommittedObjectId { result_tx } => {
                let _ = result_tx
                    .send(self.uploader.min_uncommitted_object_id())
                    .inspect_err(|e| {
                        error!("unable to send get_min_uncommitted_sst_id result: {:?}", e);
                    });
            }
            HummockEvent::GetTableCacheRefillMonitorSnapshot { result_tx } => {
                let _ = result_tx
                    .send(table_cache_refill_monitor_snapshot(
                        &self.cache_metadata.read(),
                    ))
                    .inspect_err(|_| {
                        error!("unable to send table cache refill monitor snapshot result");
                    });
            }
            HummockEvent::RegisterVectorWriter {
                table_id,
                init_epoch,
            } => self.uploader.register_vector_writer(table_id, init_epoch),
            HummockEvent::VectorWriterSealEpoch {
                table_id,
                next_epoch,
                add,
            } => {
                self.uploader
                    .vector_writer_seal_epoch(table_id, next_epoch, add);
            }
            HummockEvent::DropVectorWriter { table_id } => {
                self.uploader.drop_vector_writer(table_id);
            }
        }
    }

    fn destroy_read_version(&mut self, instance_id: LocalInstanceId) {
        {
            {
                debug!("read version deregister: instance_id: {}", instance_id);
                let (table_id, _) = self.local_read_versions.remove(instance_id);
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
                        "DestroyHummockInstance nonexistent instance table_id {} instance_id {}",
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
                vector_index_adds,
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
                table_watermarks,
                old_value_ssts,
                vector_index_adds,
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
    use std::time::Duration;

    use futures::FutureExt;
    use parking_lot::Mutex;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::TableId;
    use risingwave_common::config::Role;
    use risingwave_common::config::streaming::CacheRefillPolicy;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::insert_new_sub_level;
    use risingwave_hummock_sdk::version::{
        GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, IntraLevelDelta,
    };
    use risingwave_pb::hummock::hummock_version::PbLevels;
    use risingwave_pb::hummock::{
        PbHummockVersion, PbLevelType, PbOverlappingLevel, StateTableInfo,
    };
    use risingwave_pb::meta::serving_table_vnode_mappings::PbServingTableVnodeMapping;
    use risingwave_pb::meta::table_cache_refill_policies::PbTableCacheRefillPolicy;
    use risingwave_pb::meta::table_cache_refill_policies::table_cache_refill_policy::PbCacheRefillPolicy;
    use risingwave_pb::meta::{
        PbServingTableVnodeMappings, PbTableRefillRuntimeConfig, TableCacheRefillPolicies,
    };
    use tokio::spawn;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::sync::oneshot;

    use crate::hummock::HummockError;
    use crate::hummock::event_handler::hummock_event_handler::BufferTracker;
    use crate::hummock::event_handler::pin_cache_membership::PinCacheMembership;
    use crate::hummock::event_handler::refiller::{
        CacheRefillConfig, CacheRefiller, table_cache_refill_monitor_snapshot,
    };
    use crate::hummock::event_handler::uploader::UploadTaskOutput;
    use crate::hummock::event_handler::uploader::test_utils::{
        TEST_TABLE_ID, gen_imm_inner, gen_imm_with_unlimit,
        prepare_uploader_order_test_spawn_task_fn,
    };
    use crate::hummock::event_handler::{
        HummockEvent, HummockEventHandler, HummockObserverEvent, HummockReadVersionRef,
        HummockVersionUpdate, LocalInstanceGuard, ReadOnlyRwLockRef,
    };
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::local_version::recent_versions::RecentVersions;
    use crate::hummock::pin_cache::PinCache;
    use crate::hummock::pin_cache::test_utils::in_memory_object_store;
    use crate::hummock::table_cache_metadata::TableCacheMetadata;
    use crate::hummock::test_utils::{default_opts_for_test, gen_test_sstable_with_table_vnodes};
    use crate::mem_table::ImmutableMemtable;
    use crate::monitor::HummockStateStoreMetrics;
    use crate::store::SealCurrentEpochOptions;

    #[tokio::test]
    async fn test_version_updates_wait_for_foyer_and_publish_in_order() {
        use risingwave_hummock_sdk::version::HummockVersionDelta;
        use risingwave_pb::hummock::StateTableInfoDelta;
        use tokio::sync::Semaphore;

        use super::HummockVersionUpdate;
        use crate::hummock::local_version::pinned_version::PinVersionAction::{Pin, Unpin};

        let metrics = Arc::new(HummockStateStoreMetrics::unused());
        let opts = default_opts_for_test();
        let (spawn_upload_task, _) = prepare_uploader_order_test_spawn_task_fn(false);
        let (pin_tx, mut pin_rx) = unbounded_channel();
        let version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1.into(),
                state_table_info: HashMap::from_iter([(
                    TEST_TABLE_ID,
                    StateTableInfo {
                        committed_epoch: test_epoch(1),
                        compaction_group_id: StaticCompactionGroupId::StateDefault,
                    },
                )]),
                ..Default::default()
            }),
            pin_tx,
        );
        let permits = Arc::new([Semaphore::new(0), Semaphore::new(0), Semaphore::new(0)]);
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (finished_tx, mut finished_rx) = unbounded_channel();
        let refill_config = CacheRefillConfig::from_storage_opts(&opts);
        let metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::Streaming,
            opts.cache_refill_table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            mock_sstable_store().await,
            ReadOnlyRwLockRef::new(metadata.clone()),
        )
        .with_foyer_spawn_task(Arc::new({
            let permits = permits.clone();
            let started = started.clone();
            move |_, _| {
                let index = started.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let permits = permits.clone();
                let finished_tx = finished_tx.clone();
                spawn(async move {
                    let _permit = permits[index].acquire().await.unwrap();
                    finished_tx.send(index).unwrap();
                })
            }
        }));
        let mut handler = HummockEventHandler::new_inner(
            unbounded_channel().1,
            metrics.clone(),
            RecentVersions::new(version, 10, metrics.clone()),
            BufferTracker::from_storage_opts(&opts, &metrics),
            spawn_upload_task,
            refiller,
            metadata,
            None,
        );
        let (tx, rx) = oneshot::channel();
        handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id: TEST_TABLE_ID,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        });
        let (read_version, _guard) = rx.await.unwrap();
        let reader_snapshot = read_version.read().committed().clone();
        let mut version_updates_rx = handler.version_update_notifier_tx.subscribe();
        // A full snapshot and a multi-delta payload both enter through the production handler.
        handler.handle_version_update(HummockVersionUpdate::PinnedVersion(Box::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 2.into(),
                state_table_info: HashMap::from_iter([(
                    TEST_TABLE_ID,
                    StateTableInfo {
                        committed_epoch: test_epoch(2),
                        compaction_group_id: StaticCompactionGroupId::StateDefault,
                    },
                )]),
                ..Default::default()
            }),
        )));
        handler.handle_version_update(HummockVersionUpdate::VersionDeltas(
            (3..=4)
                .map(|id| {
                    let mut delta = HummockVersionDelta::default();
                    delta.id = id.into();
                    delta.prev_id = (id - 1).into();
                    delta.state_table_info_delta.insert(
                        TEST_TABLE_ID,
                        StateTableInfoDelta {
                            committed_epoch: test_epoch(id),
                            compaction_group_id: StaticCompactionGroupId::StateDefault,
                        },
                    );
                    delta
                })
                .collect(),
        ));
        assert_eq!(started.load(std::sync::atomic::Ordering::Relaxed), 2);
        for expected_id in [1, 2, 4] {
            assert!(matches!(pin_rx.try_recv(), Ok(Pin(id)) if id.as_raw_id() == expected_id));
        }
        assert!(pin_rx.try_recv().is_err());
        permits[1].add_permits(1);
        assert_eq!(finished_rx.recv().await, Some(1));
        assert!(
            handler
                .version_updates
                .next_events()
                .now_or_never()
                .is_none()
        );
        assert_eq!(handler.uploader.hummock_version().id().as_raw_id(), 1);
        assert_eq!(
            handler.version_update_notifier_tx.borrow().id().as_raw_id(),
            1
        );
        assert_eq!(read_version.read().committed().id().as_raw_id(), 1);
        assert!(!version_updates_rx.has_changed().unwrap());
        assert!(pin_rx.try_recv().is_err());

        permits[0].add_permits(1);
        assert_eq!(finished_rx.recv().await, Some(0));
        let events = handler.version_updates.next_events().await;
        assert_eq!(
            events
                .iter()
                .map(|event| (
                    event.prev_version_id.as_raw_id(),
                    event.new_pinned_version.id().as_raw_id(),
                ))
                .collect::<Vec<_>>(),
            vec![(1, 2), (2, 4)]
        );
        // Dequeue transfers the target pins to `events`, including the intermediate version.
        assert!(pin_rx.try_recv().is_err());
        handler.publish_version_updates(events);
        assert!(version_updates_rx.has_changed().unwrap());
        assert_eq!(version_updates_rx.borrow_and_update().id().as_raw_id(), 4);
        drop(version_updates_rx);
        assert_eq!(read_version.read().committed().id().as_raw_id(), 4);
        // History retains the committing intermediate version after publication.
        assert!(pin_rx.try_recv().is_err());
        assert_eq!(handler.uploader.hummock_version().id().as_raw_id(), 4);
        assert_eq!(
            handler.version_update_notifier_tx.borrow().id().as_raw_id(),
            4
        );
        assert_eq!(
            handler
                .recent_versions
                .load()
                .latest_version()
                .id()
                .as_raw_id(),
            4
        );
        assert_eq!(
            handler
                .recent_versions
                .load()
                .get_safe_version(TEST_TABLE_ID, test_epoch(2))
                .unwrap()
                .id()
                .as_raw_id(),
            2
        );

        let mut delta = HummockVersionDelta::default();
        delta.prev_id = 4.into();
        delta.id = 5.into();
        handler.handle_version_update(HummockVersionUpdate::VersionDeltas(vec![delta]));
        assert_eq!(started.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert!(matches!(pin_rx.try_recv(), Ok(Pin(id)) if id.as_raw_id() == 5));
        assert!(pin_rx.try_recv().is_err());
        // Main detaches already-started Foyer tasks when the handler shuts down.
        drop(handler);
        // Dropping history releases version 2; the unpublished version 5 is also released.
        let unpinned_versions = (0..2)
            .map(|_| {
                let Unpin(id) = pin_rx.try_recv().unwrap() else {
                    panic!("expected an unpin action");
                };
                id.as_raw_id()
            })
            .collect::<HashSet<_>>();
        assert_eq!(unpinned_versions, HashSet::from([2, 5]));
        assert!(pin_rx.try_recv().is_err());
        permits[2].add_permits(1);
        assert_eq!(finished_rx.recv().await, Some(2));
        // Reader-owned snapshots outlive the queue and handler, until their last holder drops.
        drop(read_version);
        assert!(matches!(pin_rx.try_recv(), Ok(Unpin(id)) if id.as_raw_id() == 4));
        assert!(pin_rx.try_recv().is_err());
        drop(reader_snapshot);
        assert!(matches!(pin_rx.try_recv(), Ok(Unpin(id)) if id.as_raw_id() == 1));
        assert!(pin_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_pin_refill_gates_publication_and_retires_membership() {
        let table = TableId::from(233);
        let ordinary_table = TableId::from(234);
        let group = 1.into();
        let store = mock_sstable_store().await;
        let (_, input) =
            gen_test_sstable_with_table_vnodes(&store, 880, &[(table, 0), (ordinary_table, 0)])
                .await;
        let (_, output) =
            gen_test_sstable_with_table_vnodes(&store, 881, &[(table, 0), (ordinary_table, 0)])
                .await;
        let cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
            .await
            .unwrap();
        let store = Arc::new(
            Arc::into_inner(store)
                .unwrap()
                .with_pin_cache(cache.clone()),
        );
        let mut initial = HummockVersion::from_rpc_protobuf(&PbHummockVersion {
            id: 1.into(),
            levels: [(
                group,
                PbLevels {
                    group_id: group,
                    l0: Some(PbOverlappingLevel::default()),
                    ..Default::default()
                },
            )]
            .into(),
            state_table_info: [table, ordinary_table]
                .into_iter()
                .map(|table| {
                    (
                        table,
                        StateTableInfo {
                            compaction_group_id: group,
                            committed_epoch: test_epoch(1),
                        },
                    )
                })
                .collect(),
            ..Default::default()
        });
        insert_new_sub_level(
            &mut initial.levels.get_mut(&group).unwrap().l0,
            1,
            PbLevelType::Overlapping,
            vec![input.clone()],
            None,
        );
        let initial = PinnedVersion::new(initial, unbounded_channel().0);
        let metrics = Arc::new(HummockStateStoreMetrics::unused());
        let options = default_opts_for_test();
        let (spawn_upload_task, _) = prepare_uploader_order_test_spawn_task_fn(false);
        let foyer_gate = Arc::new(tokio::sync::Semaphore::new(0));
        let worker_gate = foyer_gate.clone();
        let (_observer_tx, observer_rx) = unbounded_channel();
        let refill_config = CacheRefillConfig::from_storage_opts(&options);
        let refill_store = store;
        let refill_version = initial.clone();
        let pin_membership = refill_store
            .pin_cache()
            .map(|cache| PinCacheMembership::new(cache.clone(), refill_version));
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::Streaming,
            refill_config.table_cache_refill_default_policy,
        )));
        let expected_output = output.object_id;
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        )
        // Isolate handler publication behind one controlled backend completion.
        .with_foyer_spawn_task(Arc::new(move |deltas, _| {
            assert_eq!(
                deltas
                    .iter()
                    .flat_map(|delta| &delta.insert_sst_infos)
                    .map(|sst| sst.object_id)
                    .collect::<Vec<_>>(),
                [expected_output],
                "Foyer must retain the ordinary projection of a shared Pin object"
            );
            let gate = worker_gate.clone();
            tokio::spawn(async move {
                gate.acquire().await.unwrap().forget();
            })
        }));
        let mut handler = HummockEventHandler::new_inner(
            observer_rx,
            metrics.clone(),
            RecentVersions::new(initial, 10, metrics.clone()),
            BufferTracker::from_storage_opts(&options, &metrics),
            spawn_upload_task,
            refiller,
            cache_metadata,
            pin_membership,
        );
        handler.handle_observer_event(HummockObserverEvent::TableRefillRuntimeConfig(
            risingwave_pb::meta::subscribe_response::Operation::Update,
            PbTableRefillRuntimeConfig {
                table_cache_refill_policies: Some(TableCacheRefillPolicies {
                    table_policies: vec![PbTableCacheRefillPolicy {
                        table_id: ordinary_table.as_raw_id(),
                        policy: PbCacheRefillPolicy::Streaming as i32,
                    }],
                    internal_table_policies: vec![],
                    pinned_table_ids: vec![table.as_raw_id()],
                }),
                serving_table_vnode_mappings: None,
                ..Default::default()
            },
        ));
        let notifier = handler.version_update_notifier_tx.clone();
        let recent_versions = handler.recent_versions.clone();
        let events = handler.hummock_event_tx.clone();
        let (tx, rx) = oneshot::channel();
        handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id: table,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        });
        let (read_version, guard) = rx.await.unwrap();
        let (tx, rx) = oneshot::channel();
        handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id: ordinary_table,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        });
        let (ordinary_read_version, ordinary_guard) = rx.await.unwrap();
        let snapshot = table_cache_refill_monitor_snapshot(&handler.cache_metadata.read());
        assert!(!snapshot.contexts.contains_key(&table));
        assert_eq!(
            snapshot.contexts[&ordinary_table].policy,
            CacheRefillPolicy::Streaming
        );
        assert!(
            snapshot.contexts[&ordinary_table]
                .streaming_vnode_bitmap
                .as_ref()
                .unwrap()
                .all()
        );
        let mut compacted = HummockVersionDelta::default();
        compacted.prev_id = 1.into();
        compacted.id = 2.into();
        compacted.group_deltas.insert(
            group,
            GroupDeltas {
                // Meta emits input-level deletion followed by target-level insertion.
                group_deltas: vec![
                    GroupDelta::IntraLevel(IntraLevelDelta::new(
                        0,
                        0,
                        [input.sst_id].into(),
                        vec![],
                        0,
                        0,
                    )),
                    GroupDelta::IntraLevel(IntraLevelDelta::new(
                        0,
                        1,
                        HashSet::new(),
                        vec![output.clone()],
                        0,
                        0,
                    )),
                ],
            },
        );
        handler.handle_observer_event(HummockObserverEvent::VersionUpdate(
            HummockVersionUpdate::VersionDeltas(vec![compacted]),
        ));
        // No spawned job has been polled yet. Received membership is already established.
        assert!(cache.is_registered(input.object_id));
        assert!(cache.is_registered(output.object_id));
        assert!(cache.get(output.object_id).is_none());
        let worker = tokio::spawn(handler.start_hummock_event_handler_worker());
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.get(output.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Pin refill must finish while publication is blocked");
        assert!(cache.is_registered(input.object_id));
        assert!(cache.is_registered(output.object_id));
        assert_eq!(notifier.borrow().id(), 1);
        assert_eq!(recent_versions.load().latest_version().id(), 1);
        assert_eq!(read_version.read().committed().id(), 1);

        foyer_gate.add_permits(1);
        let mut published = notifier.subscribe();
        tokio::time::timeout(
            Duration::from_secs(1),
            published.wait_for(|version| version.id() == 2),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recent_versions.load().latest_version().id(), 2);
        assert_eq!(read_version.read().committed().id(), 2);
        assert_eq!(ordinary_read_version.read().committed().id(), 2);
        // The retirement callback runs after notifying readers in the same handler turn.
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.is_registered(input.object_id) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(cache.is_registered(output.object_id));
        drop((guard, ordinary_guard));
        events.send(HummockEvent::Shutdown).unwrap();
        tokio::time::timeout(Duration::from_secs(1), worker)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn test_runtime_config_fields_are_independent_full_replacements() {
        let old_table_id = TableId::new(233);
        let internal_table_id = TableId::new(234);
        let new_table_id = TableId::new(235);
        let old_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let new_vnodes = Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 0..8);
        let storage_opt = default_opts_for_test();
        let refill_config = CacheRefillConfig::from_storage_opts(&storage_opt);
        let refill_store = mock_sstable_store().await;
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::Both,
            refill_config.table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        );
        let metrics = Arc::new(HummockStateStoreMetrics::unused());
        let (spawn_upload_task, _) = prepare_uploader_order_test_spawn_task_fn(false);
        let mut handler = HummockEventHandler::new_inner(
            unbounded_channel().1,
            metrics.clone(),
            RecentVersions::new(
                PinnedVersion::new(HummockVersion::default(), unbounded_channel().0),
                10,
                metrics.clone(),
            ),
            BufferTracker::from_storage_opts(&storage_opt, &metrics),
            spawn_upload_task,
            refiller,
            cache_metadata,
            None,
        );

        handler.apply_table_refill_runtime_config(PbTableRefillRuntimeConfig {
            table_cache_refill_policies: Some(TableCacheRefillPolicies {
                table_policies: vec![PbTableCacheRefillPolicy {
                    table_id: old_table_id.as_raw_id(),
                    policy: PbCacheRefillPolicy::Serving as i32,
                }],
                internal_table_policies: vec![PbTableCacheRefillPolicy {
                    table_id: internal_table_id.as_raw_id(),
                    policy: PbCacheRefillPolicy::Both as i32,
                }],
                pinned_table_ids: vec![internal_table_id.as_raw_id()],
            }),
            serving_table_vnode_mappings: Some(PbServingTableVnodeMappings {
                mappings: vec![PbServingTableVnodeMapping {
                    table_id: old_table_id.as_raw_id(),
                    bitmap: Some(old_vnodes.to_protobuf()),
                }],
            }),
            ..Default::default()
        });
        let snapshot = table_cache_refill_monitor_snapshot(&handler.cache_metadata.read());
        assert_eq!(
            snapshot.policies,
            HashMap::from([
                (old_table_id, CacheRefillPolicy::Serving),
                (internal_table_id, CacheRefillPolicy::Both),
            ])
        );
        // Without a local Pin backend, pin intent must not replace the Foyer policy.
        assert_eq!(
            snapshot.contexts[&internal_table_id].policy,
            CacheRefillPolicy::Both
        );
        assert_eq!(
            snapshot.serving_table_vnode_mapping,
            HashMap::from([(old_table_id, old_vnodes)])
        );

        handler.apply_table_refill_runtime_config(PbTableRefillRuntimeConfig {
            serving_table_vnode_mappings: Some(PbServingTableVnodeMappings {
                mappings: vec![PbServingTableVnodeMapping {
                    table_id: new_table_id.as_raw_id(),
                    bitmap: Some(new_vnodes.to_protobuf()),
                }],
            }),
            ..Default::default()
        });
        let snapshot = table_cache_refill_monitor_snapshot(&handler.cache_metadata.read());
        assert_eq!(
            snapshot.policies,
            HashMap::from([
                (old_table_id, CacheRefillPolicy::Serving),
                (internal_table_id, CacheRefillPolicy::Both),
            ])
        );
        assert_eq!(
            snapshot.serving_table_vnode_mapping,
            HashMap::from([(new_table_id, new_vnodes.clone())])
        );

        handler.apply_table_refill_runtime_config(PbTableRefillRuntimeConfig {
            table_cache_refill_policies: Some(TableCacheRefillPolicies::default()),
            ..Default::default()
        });
        let snapshot = table_cache_refill_monitor_snapshot(&handler.cache_metadata.read());
        assert!(snapshot.policies.is_empty());
        assert_eq!(
            snapshot.serving_table_vnode_mapping,
            HashMap::from([(new_table_id, new_vnodes)])
        );

        handler.apply_table_refill_runtime_config(PbTableRefillRuntimeConfig {
            serving_table_vnode_mappings: Some(PbServingTableVnodeMappings::default()),
            ..Default::default()
        });
        let snapshot = table_cache_refill_monitor_snapshot(&handler.cache_metadata.read());
        assert!(snapshot.policies.is_empty());
        assert!(snapshot.serving_table_vnode_mapping.is_empty());
    }

    #[test]
    fn test_runtime_config_ignores_invalid_entries() {
        let table_id = TableId::new(233);
        assert!(
            super::table_cache_refill_policy_from_protobuf(PbTableCacheRefillPolicy {
                table_id: table_id.as_raw_id(),
                policy: i32::MAX,
            })
            .is_none()
        );

        assert!(
            super::serving_table_vnode_mappings_to_map(PbServingTableVnodeMappings {
                mappings: vec![PbServingTableVnodeMapping {
                    table_id: table_id.as_raw_id(),
                    bitmap: None,
                }],
            })
            .is_empty()
        );
    }

    #[tokio::test]
    async fn test_read_version_events_update_refiller_streaming_mapping() {
        fn assert_vnodes(vnodes: &Bitmap, expected: std::ops::Range<usize>) {
            assert_eq!(vnodes.count_ones(), expected.len());
            assert!(expected.into_iter().all(|vnode| vnodes.is_set(vnode)));
        }

        let table_id = TableId::new(233);
        let other_table_id = TableId::new(234);
        let (_observer_event_tx, observer_event_rx) = unbounded_channel();
        let metrics = Arc::new(HummockStateStoreMetrics::unused());
        let storage_opt = default_opts_for_test();
        let (spawn_upload_task, _) = prepare_uploader_order_test_spawn_task_fn(false);
        let pinned_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1.into(),
                ..Default::default()
            }),
            unbounded_channel().0,
        );
        let refill_config = CacheRefillConfig::from_storage_opts(&storage_opt);
        let refill_store = mock_sstable_store().await;
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::Streaming,
            refill_config.table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        );
        let mut event_handler = HummockEventHandler::new_inner(
            observer_event_rx,
            metrics.clone(),
            RecentVersions::new(pinned_version, 10, metrics.clone()),
            BufferTracker::from_storage_opts(&storage_opt, &metrics),
            spawn_upload_task,
            refiller,
            cache_metadata,
            None,
        );

        let (tx, rx) = oneshot::channel();
        event_handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 0..8)),
        });
        let (_read_version_1, mut guard_1) = rx.await.unwrap();

        let (tx, rx) = oneshot::channel();
        event_handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 8..16)),
        });
        let (_read_version_2, mut guard_2) = rx.await.unwrap();

        let (tx, rx) = oneshot::channel();
        event_handler.handle_hummock_event(HummockEvent::RegisterReadVersion {
            table_id: other_table_id,
            new_read_version_sender: tx,
            is_replicated: false,
            vnodes: Arc::new(Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 32..40)),
        });
        let (_other_read_version, mut other_guard) = rx.await.unwrap();

        let snapshot = table_cache_refill_monitor_snapshot(&event_handler.cache_metadata.read());
        let vnodes = snapshot
            .streaming_table_vnode_mapping
            .get(&table_id)
            .unwrap();
        assert_vnodes(vnodes, 0..16);
        assert_vnodes(
            snapshot
                .streaming_table_vnode_mapping
                .get(&other_table_id)
                .unwrap(),
            32..40,
        );

        guard_1.event_sender.take();
        event_handler.handle_hummock_event(HummockEvent::DestroyReadVersion {
            instance_id: guard_1.instance_id,
        });
        let snapshot = table_cache_refill_monitor_snapshot(&event_handler.cache_metadata.read());
        assert_vnodes(
            snapshot
                .streaming_table_vnode_mapping
                .get(&table_id)
                .unwrap(),
            8..16,
        );

        guard_2.event_sender.take();
        event_handler.handle_hummock_event(HummockEvent::DestroyReadVersion {
            instance_id: guard_2.instance_id,
        });
        let snapshot = table_cache_refill_monitor_snapshot(&event_handler.cache_metadata.read());
        assert!(
            !snapshot
                .streaming_table_vnode_mapping
                .contains_key(&table_id)
        );
        assert_vnodes(
            snapshot
                .streaming_table_vnode_mapping
                .get(&other_table_id)
                .unwrap(),
            32..40,
        );

        other_guard.event_sender.take();
        event_handler.handle_hummock_event(HummockEvent::DestroyReadVersion {
            instance_id: other_guard.instance_id,
        });
    }

    #[tokio::test]
    async fn test_old_epoch_sync_fail() {
        let epoch0 = test_epoch(233);

        let initial_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1.into(),
                state_table_info: HashMap::from_iter([(
                    TEST_TABLE_ID,
                    StateTableInfo {
                        committed_epoch: epoch0,
                        compaction_group_id: StaticCompactionGroupId::StateDefault,
                    },
                )]),
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let (_observer_event_tx, observer_event_rx) = unbounded_channel();

        let epoch1 = epoch0.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let (tx, rx) = oneshot::channel();
        let rx = Arc::new(Mutex::new(Some(rx)));

        let storage_opt = default_opts_for_test();
        let metrics = Arc::new(HummockStateStoreMetrics::unused());

        let refill_config = CacheRefillConfig::from_storage_opts(&storage_opt);
        let refill_store = mock_sstable_store().await;
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::None,
            refill_config.table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        );
        let event_handler = HummockEventHandler::new_inner(
            observer_event_rx,
            metrics.clone(),
            RecentVersions::new(initial_version.clone(), 10, metrics.clone()),
            BufferTracker::from_storage_opts(&storage_opt, &metrics),
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
            refiller,
            cache_metadata,
            None,
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

        read_version.write().init();

        send_event(HummockEvent::InitEpoch {
            instance_id: guard.instance_id,
            init_epoch: epoch1,
        });

        let (imm1, tracker1) = gen_imm_with_unlimit(epoch1);
        read_version.write().add_pending_imm(imm1.clone(), tracker1);

        send_event(HummockEvent::ImmToUploader {
            instance_id: guard.instance_id,
            imms: read_version.write().start_upload_pending_imms(),
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

        {
            let (imm2, tracker2) = gen_imm_with_unlimit(epoch2);
            let mut read_version = read_version.write();
            read_version.add_pending_imm(imm2, tracker2);

            send_event(HummockEvent::ImmToUploader {
                instance_id: guard.instance_id,
                imms: read_version.start_upload_pending_imms(),
            });
        }

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
    async fn test_clear_one_table_preserves_other_table_uploads() {
        let table_id1 = TableId::new(1);
        let table_id2 = TableId::new(2);
        let epoch0 = test_epoch(233);

        let initial_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1.into(),
                state_table_info: HashMap::from_iter([
                    (
                        table_id1,
                        StateTableInfo {
                            committed_epoch: epoch0,
                            compaction_group_id: StaticCompactionGroupId::StateDefault,
                        },
                    ),
                    (
                        table_id2,
                        StateTableInfo {
                            committed_epoch: epoch0,
                            compaction_group_id: StaticCompactionGroupId::StateDefault,
                        },
                    ),
                ]),
                ..Default::default()
            }),
            unbounded_channel().0,
        );

        let (_observer_event_tx, observer_event_rx) = unbounded_channel();

        let epoch1 = epoch0.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();

        let imm_size = gen_imm_inner(TEST_TABLE_ID, epoch1, 0).size();

        // The buffer can hold at most 1 imm. When a new imm is added, the previous one will be spilled, and the newly added one will be retained.
        let buffer_tracker = BufferTracker::for_test_with_config(imm_size * 2 - 1, 1);
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();

        let gen_imm = |table_id, epoch, spill_offset| {
            let imm = gen_imm_inner(table_id, epoch, spill_offset);
            assert_eq!(imm.size(), imm_size);
            imm
        };
        let imm1_1 = gen_imm(table_id1, epoch1, 0);
        let imm1_2_1 = gen_imm(table_id1, epoch2, 0);

        let storage_opt = default_opts_for_test();
        let metrics = Arc::new(HummockStateStoreMetrics::unused());

        let (spawn_task, new_task_notifier) = prepare_uploader_order_test_spawn_task_fn(false);

        let refill_config = CacheRefillConfig::from_storage_opts(&storage_opt);
        let refill_store = mock_sstable_store().await;
        let cache_metadata = Arc::new(parking_lot::RwLock::new(TableCacheMetadata::new(
            Role::None,
            refill_config.table_cache_refill_default_policy,
        )));
        let refiller = CacheRefiller::new(
            refill_config,
            refill_store,
            ReadOnlyRwLockRef::new(cache_metadata.clone()),
        );
        let event_handler = HummockEventHandler::new_inner(
            observer_event_rx,
            metrics.clone(),
            RecentVersions::new(initial_version.clone(), 10, metrics.clone()),
            buffer_tracker,
            spawn_task,
            refiller,
            cache_metadata,
            None,
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
        let event_tx_clone = event_tx.clone();
        let write_imm = {
            let memory_limiter = memory_limiter.clone();
            move |read_version: &HummockReadVersionRef,
                  instance: &LocalInstanceGuard,
                  imm: &ImmutableMemtable| {
                let memory_limiter = memory_limiter.clone();
                let event_tx = event_tx_clone.clone();
                let read_version = read_version.clone();
                let imm = imm.clone();
                let instance_id = instance.instance_id;
                async move {
                    let tracker = memory_limiter.require_memory(imm.size() as _).await;
                    let mut read_version = read_version.write();
                    read_version.add_pending_imm(imm.clone(), tracker);

                    event_tx
                        .send(HummockEvent::ImmToUploader {
                            instance_id,
                            imms: read_version.start_upload_pending_imms(),
                        })
                        .unwrap();
                }
            }
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

            read_version1.write().init();
            init_epoch(&guard1, epoch1);

            write_imm(&read_version1, &guard1, &imm1_1).await;

            start_epoch(table_id1, epoch2);

            seal_epoch(&guard1, epoch2);

            let (wait_task_start, task_finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard1.instance_id,
                vec![imm1_1.batch_id()],
            )]));

            let mut rx = sync_epoch(table_id1, epoch1);
            wait_task_start.await;
            assert!(poll_fn(|cx| Poll::Ready(rx.poll_unpin(cx).is_pending())).await);

            write_imm(&read_version1, &guard1, &imm1_2_1).await;
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
            read_version2.write().init();
            init_epoch(&guard2, epoch1);
            let (wait_task_start, task1_2_finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard1.instance_id,
                vec![imm1_2_1.batch_id()],
            )]));
            write_imm(&read_version2, &guard2, &imm2_1_1).await;
            wait_task_start.await;

            let imm2_1_2 = gen_imm(table_id2, epoch1, 1);
            let (wait_task_start, finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard2.instance_id,
                vec![imm2_1_2.batch_id(), imm2_1_1.batch_id()],
            )]));
            finish_txs.push(finish_tx);
            write_imm(&read_version2, &guard2, &imm2_1_2).await;
            wait_task_start.await;

            let imm2_1_3 = gen_imm(table_id2, epoch1, 2);
            write_imm(&read_version2, &guard2, &imm2_1_3).await;
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
            write_imm(&read_version2, &guard2, &imm2_2_1).await;
            flush_event().await;
            let imm2_2_2 = gen_imm(table_id2, epoch2, 1);
            write_imm(&read_version2, &guard2, &imm2_2_2).await;
            let (wait_task_start, finish_tx) = new_task_notifier(HashMap::from_iter([(
                guard2.instance_id,
                vec![imm2_2_2.batch_id(), imm2_2_1.batch_id()],
            )]));
            finish_txs.push(finish_tx);
            wait_task_start.await;

            let imm2_2_3 = gen_imm(table_id2, epoch2, 2);
            write_imm(&read_version2, &guard2, &imm2_2_3).await;

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
        write_imm(&read_version1, &guard1, &imm1_2_2).await;
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
        assert!(!sync_data1.uploaded_ssts.is_empty());
        assert!(
            sync_data1
                .uploaded_ssts
                .iter()
                .all(|sst| sst.epochs().as_slice() == [epoch1])
        );
        task1_2_finish_tx.send(()).unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx2.poll_unpin(cx).is_pending())).await);
        task1_2_2_finish_tx.send(()).unwrap();
        let sync_data2 = sync_rx2.await.unwrap().unwrap();
        assert!(!sync_data2.uploaded_ssts.is_empty());
        assert!(
            sync_data2
                .uploaded_ssts
                .iter()
                .all(|sst| sst.epochs().as_slice() == [epoch2])
        );

        send_event(HummockEvent::Shutdown);
        join_handle.await.unwrap();
    }
}
