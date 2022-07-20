// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::borrow::BorrowMut;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::Bound::{Excluded, Included};
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use function_name::named;
use itertools::Itertools;
use prost::Message;
use risingwave_common::monitor::rwlock::MonitoredRwLock;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{
    get_remote_sst_id, CompactionGroupId, HummockCompactionTaskId, HummockContextId, HummockEpoch,
    HummockRefCount, HummockSSTableId, HummockVersionId, LocalSstableInfo, FIRST_VERSION_ID,
};
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactTask, CompactTaskAssignment, HummockPinnedSnapshot, HummockPinnedVersion,
    HummockSnapshot, HummockStaleSstables, HummockVersion, HummockVersionDelta, Level, LevelDelta,
    LevelType, SstableIdInfo, SstableInfo,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::MetaLeaderInfo;
use tokio::sync::RwLockWriteGuard;

use crate::cluster::{ClusterManagerRef, META_NODE_ID};
use crate::hummock::compaction::{CompactStatus, ManualCompactionOption};
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::hummock::compaction_scheduler::CompactionRequestChannelRef;
use crate::hummock::error::{Error, Result};
use crate::hummock::metrics_utils::{trigger_commit_stat, trigger_sst_stat};
use crate::hummock::model::{sstable_id_info, CurrentHummockVersionId, INVALID_TIMESTAMP};
use crate::hummock::CompactorManagerRef;
use crate::manager::{IdCategory, MetaSrvEnv};
use crate::model::{
    BTreeMapTransaction, MetadataModel, Transactional, ValTransaction, VarTransaction,
};
use crate::rpc::metrics::MetaMetrics;
use crate::rpc::{META_CF_NAME, META_LEADER_KEY};
use crate::storage::{MetaStore, Transaction};

// Update to states are performed as follow:
// - Initialize ValTransaction for the meta state to update
// - Make changes on the ValTransaction.
// - Call `commit_multi_var` to commit the changes via meta store transaction. If transaction
//   succeeds, the in-mem state will be updated by the way.
pub struct HummockManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    compaction_group_manager: CompactionGroupManagerRef<S>,
    // When trying to locks compaction and versioning at the same time, compaction lock should
    // be requested before versioning lock.
    compaction: MonitoredRwLock<Compaction>,
    versioning: MonitoredRwLock<Versioning>,
    max_committed_epoch: AtomicU64,

    metrics: Arc<MetaMetrics>,

    // `compaction_scheduler` is used to schedule a compaction for specified CompactionGroupId
    compaction_scheduler: parking_lot::RwLock<Option<CompactionRequestChannelRef>>,

    compactor_manager: CompactorManagerRef,
}

pub type HummockManagerRef<S> = Arc<HummockManager<S>>;

#[derive(Default)]
struct Compaction {
    /// Compaction task that is already assigned to a compactor
    compact_task_assignment: BTreeMap<HummockCompactionTaskId, CompactTaskAssignment>,
    /// `CompactStatus` of each compaction group
    compaction_statuses: BTreeMap<CompactionGroupId, CompactStatus>,
    /// Available compaction task ids for use
    next_task_ids: VecDeque<HummockCompactionTaskId>,
}

impl Compaction {
    /// Gets a new compaction task id locally. If no id is available locally, fetch some ids via
    /// `get_more_ids` first.
    async fn get_next_task_id<F>(&mut self, get_more_ids: F) -> Result<HummockCompactionTaskId>
    where
        F: Future<Output = Result<Vec<HummockCompactionTaskId>>>,
    {
        if self.next_task_ids.is_empty() {
            let new_ids = get_more_ids.await?;
            self.next_task_ids.extend(new_ids);
        }
        self.next_task_ids
            .pop_front()
            .ok_or_else(|| Error::InternalError("cannot get compaction task id".to_string()))
    }
}

/// Commit multiple `ValTransaction`s to state store and upon success update the local in-mem state
/// by the way
/// After called, the `ValTransaction` will be dropped.
macro_rules! commit_multi_var {
    ($hummock_mgr:expr, $context_id:expr, $($val_txn:expr),*) => {
        {
            async {
                let mut trx = Transaction::default();
                // Apply the change in `ValTransaction` to trx
                $(
                    $val_txn.apply_to_txn(&mut trx)?;
                )*
                // Commit to state store
                $hummock_mgr.commit_trx($hummock_mgr.env.meta_store(), trx, $context_id, $hummock_mgr.env.get_leader_info())
                .await?;
                // Upon successful commit, commit the change to local in-mem state
                $(
                    $val_txn.commit();
                )*
                Result::Ok(())
            }.await
        }
    };
}

/// Acquire read lock of the lock with `lock_name`.
/// The macro will use macro `function_name` to get the name of the function of method that calls
/// the lock, and therefore, anyone to call this macro should ensured that the caller method has the
/// macro #[named]
macro_rules! read_lock {
    ($hummock_mgr:expr, $lock_name:ident) => {
        async {
            $hummock_mgr
                .$lock_name
                .read(&[function_name!(), stringify!($lock_name), "read"])
                .await
        }
    };
}

/// Acquire write lock of the lock with `lock_name`.
/// The macro will use macro `function_name` to get the name of the function of method that calls
/// the lock, and therefore, anyone to call this macro should ensured that the caller method has the
/// macro #[named]
macro_rules! write_lock {
    ($hummock_mgr:expr, $lock_name:ident) => {
        async {
            $hummock_mgr
                .$lock_name
                .write(&[function_name!(), stringify!($lock_name), "write"])
                .await
        }
    };
}

macro_rules! start_measure_real_process_timer {
    ($hummock_mgr:expr) => {
        $hummock_mgr
            .metrics
            .hummock_manager_real_process_time
            .with_label_values(&[function_name!()])
            .start_timer()
    };
}

#[derive(Default)]
struct Versioning {
    current_version_id: CurrentHummockVersionId,
    current_version: HummockVersion,
    hummock_versions: BTreeMap<HummockVersionId, Vec<u64>>,
    hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta>,
    pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    stale_sstables: BTreeMap<HummockVersionId, HummockStaleSstables>,
    sstable_id_infos: BTreeMap<HummockSSTableId, SstableIdInfo>,
}

impl Versioning {
    pub fn current_version_ref(&self) -> &HummockVersion {
        &self.current_version
    }

    pub fn current_version(&self) -> HummockVersion {
        self.current_version_ref().clone()
    }
}

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        metrics: Arc<MetaMetrics>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Result<HummockManager<S>> {
        let instance = HummockManager {
            env,
            versioning: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
            ),
            compaction: MonitoredRwLock::new(
                metrics.hummock_manager_lock_time.clone(),
                Default::default(),
            ),
            metrics,
            cluster_manager,
            compaction_group_manager,
            compaction_scheduler: parking_lot::RwLock::new(None),
            compactor_manager,
            max_committed_epoch: AtomicU64::new(0),
        };

        instance.load_meta_store_state().await?;
        instance.release_invalid_contexts().await?;
        instance.cancel_unassigned_compaction_task().await?;
        // Release snapshots pinned by meta on restarting.
        instance.release_contexts([META_NODE_ID]).await?;
        Ok(instance)
    }

    /// Load state from meta store.
    #[named]
    async fn load_meta_store_state(&self) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let mut versioning_guard = write_lock!(self, versioning).await;
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
    }

    /// Load state from meta store.
    async fn load_meta_store_state_impl(
        &self,
        compaction_guard: &mut RwLockWriteGuard<'_, Compaction>,
        versioning_guard: &mut RwLockWriteGuard<'_, Versioning>,
    ) -> Result<()> {
        let compaction_statuses = CompactStatus::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|cg| (cg.compaction_group_id(), cg))
            .collect::<BTreeMap<CompactionGroupId, CompactStatus>>();
        if !compaction_statuses.is_empty() {
            compaction_guard.compaction_statuses = compaction_statuses;
        } else {
            // Initialize compact status for each compaction group
            let mut compaction_statuses =
                BTreeMapTransaction::new(&mut compaction_guard.compaction_statuses);
            for compaction_group in self.compaction_group_manager.compaction_groups().await {
                let compact_status = CompactStatus::new(
                    compaction_group.group_id(),
                    Arc::new(compaction_group.compaction_config().clone()),
                );
                compaction_statuses.insert(compact_status.compaction_group_id(), compact_status);
            }
            commit_multi_var!(self, None, compaction_statuses)?;
        }
        compaction_guard.compact_task_assignment =
            CompactTaskAssignment::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|assigned| (assigned.key().unwrap(), assigned))
                .collect();

        versioning_guard.current_version_id = CurrentHummockVersionId::get(self.env.meta_store())
            .await?
            .unwrap_or_else(CurrentHummockVersionId::new);

        let versions = HummockVersion::list(self.env.meta_store()).await?;

        let hummock_version_deltas: BTreeMap<_, _> =
            HummockVersionDelta::list(self.env.meta_store())
                .await?
                .into_iter()
                .map(|version_delta| (version_delta.id, version_delta))
                .collect();

        // Insert the initial version.
        let mut redo_state = if versions.is_empty() {
            let mut init_version = HummockVersion {
                id: FIRST_VERSION_ID,
                levels: Default::default(),
                max_committed_epoch: INVALID_EPOCH,
                safe_epoch: INVALID_EPOCH,
            };
            // Initialize independent levels via corresponding compaction group' config.
            for compaction_group in self.compaction_group_manager.compaction_groups().await {
                let mut levels = vec![Level {
                    level_idx: 0u32,
                    level_type: LevelType::Overlapping as i32,
                    table_infos: vec![],
                    total_file_size: 0,
                }];
                for l in 0..compaction_group.compaction_config().max_level {
                    levels.push(Level {
                        level_idx: (l + 1) as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                        total_file_size: 0,
                    });
                }
                init_version
                    .levels
                    .insert(compaction_group.group_id(), Levels { levels });
            }
            init_version.insert(self.env.meta_store()).await?;
            init_version
        } else {
            versions.first().unwrap().clone()
        };
        versioning_guard
            .hummock_versions
            .insert(redo_state.id, redo_state.get_sst_ids());

        for version_delta in hummock_version_deltas.values() {
            if version_delta.prev_id == redo_state.id {
                redo_state.apply_version_delta(version_delta);

                versioning_guard
                    .hummock_versions
                    .insert(redo_state.id, redo_state.get_sst_ids());
            }
        }
        self.max_committed_epoch
            .store(redo_state.max_committed_epoch, Ordering::Relaxed);
        versioning_guard.current_version = redo_state;
        versioning_guard.hummock_version_deltas = hummock_version_deltas;

        versioning_guard.pinned_versions = HummockPinnedVersion::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();
        versioning_guard.pinned_snapshots = HummockPinnedSnapshot::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|p| (p.context_id, p))
            .collect();

        versioning_guard.stale_sstables = HummockStaleSstables::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|s| (s.version_id, s))
            .collect();

        versioning_guard.sstable_id_infos = SstableIdInfo::list(self.env.meta_store())
            .await?
            .into_iter()
            .map(|s| (s.id, s))
            .collect();

        Ok(())
    }

    /// We use worker node id as the `context_id`.
    /// If the `context_id` is provided, the transaction will abort if the `context_id` is not
    /// valid, which means the worker node is not a valid member of the cluster.
    /// This operation is protected by mutex of compaction, so that other thread can not
    /// call `release_contexts` even if it has removed `context_id` from cluster manager.
    async fn commit_trx(
        &self,
        meta_store: &S,
        mut trx: Transaction,
        context_id: Option<HummockContextId>,
        info: MetaLeaderInfo,
    ) -> Result<()> {
        if let Some(context_id) = context_id {
            if context_id == META_NODE_ID {
                // Using the preserved meta id is allowed.
            } else if self
                .cluster_manager
                .get_worker_by_id(context_id)
                .await
                .is_none()
            {
                // The worker is not found in cluster.
                return Err(Error::InvalidContext(context_id));
            }
        }
        trx.check_equal(
            META_CF_NAME.to_owned(),
            META_LEADER_KEY.as_bytes().to_vec(),
            info.encode_to_vec(),
        );
        meta_store.txn(trx).await.map_err(Into::into)
    }

    /// Pin the current greatest hummock version. The pin belongs to `context_id`
    /// and will be unpinned when `context_id` is invalidated.
    #[named]
    pub async fn pin_version(
        &self,
        context_id: HummockContextId,
        last_pinned: HummockVersionId,
    ) -> Result<(bool, Vec<HummockVersionDelta>, Option<HummockVersion>)> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let current_version_id = versioning.current_version_id.clone();
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: 0,
            },
        );

        let version_id = current_version_id.id();

        let (is_delta, ret_deltas) = {
            if last_pinned <= version_id
                && versioning.hummock_version_deltas.contains_key(&last_pinned)
            {
                (
                    true,
                    versioning
                        .hummock_version_deltas
                        .range((Excluded(last_pinned), Included(version_id)))
                        .map(|(_, delta)| delta.clone())
                        .collect_vec(),
                )
            } else {
                (false, vec![])
            }
        };

        if context_pinned_version.min_pinned_id == 0 {
            context_pinned_version.min_pinned_id = version_id;
            commit_multi_var!(self, Some(context_id), context_pinned_version)?;
        }

        let ret = Ok((
            is_delta,
            ret_deltas,
            if is_delta {
                None
            } else {
                Some(versioning.current_version.clone())
            },
        ));

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        ret
    }

    /// Unpin all pins which belongs to `context_id` and has an id which is older than
    /// `unpin_before`. All versions >= `unpin_before` will be treated as if they are all pinned by
    /// this `context_id` so they will not be vacummed.
    #[named]
    pub async fn unpin_version_before(
        &self,
        context_id: HummockContextId,
        unpin_before: HummockVersionId,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let mut context_pinned_version = pinned_versions.new_entry_txn_or_default(
            context_id,
            HummockPinnedVersion {
                context_id,
                min_pinned_id: 0,
            },
        );

        context_pinned_version.min_pinned_id = unpin_before;
        commit_multi_var!(self, Some(context_id), context_pinned_version)?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Remove this context from context pin info.
    #[named]
    pub async fn unpin_version(&self, context_id: HummockContextId) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning_guard.pinned_versions);
        let release_version = pinned_versions.remove(context_id);
        if release_version.is_some() {
            commit_multi_var!(self, Some(context_id), pinned_versions)?;
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Make sure `max_commited_epoch` is pinned and return it.
    #[named]
    pub async fn pin_snapshot(&self, context_id: HummockContextId) -> Result<HummockSnapshot> {
        let max_committed_epoch = self.max_committed_epoch.load(Ordering::Relaxed);
        let mut guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut guard.pinned_snapshots);
        let mut context_pinned_snapshot = pinned_snapshots.new_entry_txn_or_default(
            context_id,
            HummockPinnedSnapshot {
                context_id,
                minimal_pinned_snapshot: 0,
            },
        );
        if context_pinned_snapshot.minimal_pinned_snapshot == 0 {
            context_pinned_snapshot.minimal_pinned_snapshot = max_committed_epoch;
            commit_multi_var!(self, Some(context_id), context_pinned_snapshot)?;
        }

        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    pub fn get_last_epoch(&self) -> Result<HummockSnapshot> {
        let max_committed_epoch = self.max_committed_epoch.load(Ordering::Relaxed);
        Ok(HummockSnapshot {
            epoch: max_committed_epoch,
        })
    }

    #[named]
    pub async fn unpin_snapshot(&self, context_id: HummockContextId) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots);
        let release_snapshot = pinned_snapshots.remove(context_id);

        if release_snapshot.is_some() {
            commit_multi_var!(self, Some(context_id), pinned_snapshots)?;
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Unpin all snapshots smaller than specified epoch for current context.
    #[named]
    pub async fn unpin_snapshot_before(
        &self,
        context_id: HummockContextId,
        hummock_snapshot: HummockSnapshot,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        // Use the max_committed_epoch in storage as the snapshot ts so only committed changes are
        // visible in the snapshot.
        let max_committed_epoch = versioning_guard.current_version.max_committed_epoch;
        // Ensure the unpin will not clean the latest one.
        #[cfg(not(test))]
        {
            assert!(hummock_snapshot.epoch <= max_committed_epoch);
        }
        let last_read_epoch = std::cmp::min(hummock_snapshot.epoch, max_committed_epoch);

        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning_guard.pinned_snapshots);
        let mut context_pinned_snapshot = pinned_snapshots.new_entry_txn_or_default(
            context_id,
            HummockPinnedSnapshot {
                context_id,
                minimal_pinned_snapshot: 0,
            },
        );

        // Unpin the snapshots pinned by meta but frontend doesn't know. Also equal to unpin all
        // epochs below specific watermark.
        if context_pinned_snapshot.minimal_pinned_snapshot < last_read_epoch
            || context_pinned_snapshot.minimal_pinned_snapshot == 0
        {
            context_pinned_snapshot.minimal_pinned_snapshot = last_read_epoch;
            commit_multi_var!(self, Some(context_id), context_pinned_snapshot)?;
        }

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    #[named]
    pub async fn get_compact_task_impl(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: Option<ManualCompactionOption>,
    ) -> Result<Option<CompactTask>> {
        let start_time = Instant::now();
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let task_id = compaction
            .get_next_task_id(async {
                let batch_size = 10;
                self.env
                    .id_gen_manager()
                    .generate_interval::<{ IdCategory::HummockCompactionTask }>(batch_size)
                    .await
                    .map(|id| {
                        (id as HummockCompactionTaskId
                            ..(id + batch_size) as HummockCompactionTaskId)
                            .collect_vec()
                    })
                    .map_err(Error::from)
            })
            .await?;
        let mut compact_status = VarTransaction::new(
            compaction
                .compaction_statuses
                .get_mut(&compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(compaction_group_id))?,
        );
        let current_version = read_lock!(self, versioning).await.current_version();
        let compact_task = compact_status.get_compact_task(
            current_version.get_compaction_group_levels(compaction_group_id),
            task_id as HummockCompactionTaskId,
            compaction_group_id,
            manual_compaction_option,
        );

        let ret = match compact_task {
            None => Ok(None),
            Some(mut compact_task) => {
                let existing_table_ids_from_meta = self
                    .compaction_group_manager
                    .internal_table_ids_by_compaction_group_id(compaction_group_id)
                    .await?;

                compact_task.watermark = {
                    let versioning_guard = read_lock!(self, versioning).await;
                    let max_committed_epoch = versioning_guard.current_version.max_committed_epoch;
                    versioning_guard
                        .pinned_snapshots
                        .values()
                        .map(|v| v.minimal_pinned_snapshot)
                        .fold(max_committed_epoch, std::cmp::min)
                };

                // to get all relational table_id from sst_info
                let table_ids = compact_task
                    .input_ssts
                    .iter()
                    .flat_map(|level| {
                        level
                            .table_infos
                            .iter()
                            .flat_map(|sst_info| sst_info.table_ids.iter().cloned())
                            .collect_vec()
                    })
                    .collect::<HashSet<u32>>();

                for table_id in table_ids {
                    // to found exist table_id from
                    if existing_table_ids_from_meta.contains(&table_id) {
                        compact_task.existing_table_ids.push(table_id);
                    }
                }

                // build table_options
                let compaction_group = self
                    .compaction_group_manager
                    .compaction_group(compaction_group_id)
                    .await
                    .unwrap();
                compact_task.table_options = compaction_group
                    .table_id_to_options()
                    .iter()
                    .filter(|id_to_option| compact_task.existing_table_ids.contains(id_to_option.0))
                    .map(|id_to_option| (*id_to_option.0, id_to_option.1.into()))
                    .collect();
                compact_task.current_epoch_time = Epoch::now().0;

                compact_task.compaction_filter_mask =
                    compact_status.compaction_config.compaction_filter_mask;

                commit_multi_var!(self, None, compact_status)?;
                tracing::trace!(
                    "For compaction group {}: pick up {} tables in level {} to compact, The number of total tables is {}. cost time: {:?}",
                    compaction_group_id,
                    compact_task.input_ssts[0].table_infos.len(),
                    compact_task.input_ssts[0].level_idx,
                    current_version.get_compaction_group_levels(compaction_group_id)[compact_task.input_ssts[0].level_idx as usize]
                        .table_infos
                        .len(),
                    start_time.elapsed()
                );
                Ok(Some(compact_task))
            }
        };

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        ret
    }

    pub async fn get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> Result<Option<CompactTask>> {
        self.get_compact_task_impl(compaction_group_id, None).await
    }

    pub async fn manual_get_compact_task(
        &self,
        compaction_group_id: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<Option<CompactTask>> {
        self.get_compact_task_impl(compaction_group_id, Some(manual_compaction_option))
            .await
    }

    /// Assigns a compaction task to a compactor
    #[named]
    pub async fn assign_compaction_task<T: Future<Output = bool>>(
        &self,
        compact_task: &CompactTask,
        assignee_context_id: HummockContextId,
        send_task: T,
    ) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let _timer = start_measure_real_process_timer!(self);
        if !send_task.await {
            return Err(Error::CompactorUnreachable(assignee_context_id));
        }

        let compaction = compaction_guard.deref_mut();
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        if let Some(assignment) = compact_task_assignment.get(&compact_task.task_id) {
            return Err(Error::CompactionTaskAlreadyAssigned(
                compact_task.task_id,
                assignment.context_id,
            ));
        }
        compact_task_assignment.insert(
            compact_task.task_id,
            CompactTaskAssignment {
                compact_task: Some(compact_task.clone()),
                context_id: assignee_context_id,
            },
        );
        commit_multi_var!(self, Some(assignee_context_id), compact_task_assignment)?;

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// `report_compact_task` is retryable. `task_id` in `compact_task` parameter is used as the
    /// idempotency key. Return Ok(false) to indicate the `task_id` is not found, which may have
    /// been processed previously.
    #[named]
    pub async fn report_compact_task(&self, compact_task: &CompactTask) -> Result<bool> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let start_time = Instant::now();
        let compaction = compaction_guard.deref_mut();
        let mut compact_status = VarTransaction::new(
            compaction
                .compaction_statuses
                .get_mut(&compact_task.compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(
                    compact_task.compaction_group_id,
                ))?,
        );
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        let assignee_context_id = match compact_task_assignment.remove(compact_task.task_id) {
            None => {
                // The task is not found.
                return Ok(false);
            }
            Some(assignment) => assignment.context_id,
        };
        compact_status.report_compact_task(compact_task);
        if compact_task.task_status {
            // The compaction task is finished.
            let mut versioning_guard = write_lock!(self, versioning).await;
            let old_version = versioning_guard.current_version();
            let versioning = versioning_guard.deref_mut();
            let mut current_version_id = VarTransaction::new(&mut versioning.current_version_id);
            let mut hummock_version_deltas =
                BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
            let mut stale_sstables = BTreeMapTransaction::new(&mut versioning.stale_sstables);
            let mut sstable_id_infos = BTreeMapTransaction::new(&mut versioning.sstable_id_infos);
            let mut version_stale_sstables = stale_sstables.new_entry_txn_or_default(
                old_version.id,
                HummockStaleSstables {
                    version_id: old_version.id,
                    id: vec![],
                },
            );
            let mut version_delta = HummockVersionDelta {
                prev_id: old_version.id,
                max_committed_epoch: old_version.max_committed_epoch,
                ..Default::default()
            };
            let level_deltas = &mut version_delta
                .level_deltas
                .entry(compact_task.compaction_group_id)
                .or_default()
                .level_deltas;
            for level in &compact_task.input_ssts {
                version_stale_sstables
                    .id
                    .extend(level.table_infos.iter().map(|sst| sst.id).collect_vec());
                let level_delta = LevelDelta {
                    level_idx: level.level_idx,
                    removed_table_ids: level.table_infos.iter().map(|sst| sst.id).collect_vec(),
                    ..Default::default()
                };
                level_deltas.push(level_delta);
            }
            let level_delta = LevelDelta {
                level_idx: compact_task.target_level,
                inserted_table_infos: compact_task.sorted_output_ssts.clone(),
                ..Default::default()
            };
            level_deltas.push(level_delta);
            let mut new_version = CompactStatus::apply_compact_result(compact_task, old_version);
            version_delta.safe_epoch = new_version.safe_epoch;

            current_version_id.increase();
            new_version.id = current_version_id.id();
            version_delta.id = current_version_id.id();
            hummock_version_deltas.insert(version_delta.id, version_delta);

            for SstableInfo { id: ref sst_id, .. } in &compact_task.sorted_output_ssts {
                match sstable_id_infos.get_mut(*sst_id) {
                    None => {
                        return Err(Error::InternalError(format!(
                            "invalid sst id {}, may have been vacuumed",
                            sst_id
                        )));
                    }
                    Some(mut sst_id_info) => {
                        sst_id_info.meta_create_timestamp = sstable_id_info::get_timestamp_now();
                    }
                }
            }

            commit_multi_var!(
                self,
                Some(assignee_context_id),
                compact_status,
                compact_task_assignment,
                current_version_id,
                hummock_version_deltas,
                version_stale_sstables,
                sstable_id_infos
            )?;
            versioning
                .hummock_versions
                .insert(new_version.id, new_version.get_sst_ids());
            versioning.current_version = new_version;
        } else {
            // The compaction task is cancelled.
            commit_multi_var!(
                self,
                Some(assignee_context_id),
                compact_status,
                compact_task_assignment
            )?;
        }

        tracing::trace!(
            "Reported compaction task. {}. cost time: {:?}",
            compact_task_to_string(compact_task),
            start_time.elapsed(),
        );

        trigger_sst_stat(
            &self.metrics,
            compaction
                .compaction_statuses
                .get(&compact_task.compaction_group_id)
                .ok_or(Error::InvalidCompactionGroup(
                    compact_task.compaction_group_id,
                ))?,
            read_lock!(self, versioning).await.current_version_ref(),
            compact_task.compaction_group_id,
        );

        self.try_send_compaction_request(compact_task.compaction_group_id);

        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(true)
    }

    /// Caller should ensure `epoch` > `max_committed_epoch`
    #[named]
    pub async fn commit_epoch(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        // Warn of table_ids that is not found in expected compaction group.
        // It indicates:
        // 1. Either these table_ids are never registered to any compaction group. This is FATAL
        // since compaction filter will remove these valid states incorrectly.
        // 2. Or the owners of these table_ids have been dropped, but their stale states are still
        // committed. This is OK since compaction filter will remove these stale states
        // later.
        for (compaction_group_id, sst) in &sstables {
            let compaction_group = self
                .compaction_group_manager
                .compaction_group(*compaction_group_id)
                .await
                .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id));
            for table_id in sst
                .table_ids
                .iter()
                .filter(|t| !compaction_group.member_table_ids().contains(t))
            {
                tracing::warn!(
                    "table {} doesn't belong to expected compaction group {}",
                    table_id,
                    compaction_group_id
                );
            }
        }

        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let old_version = versioning_guard.current_version();
        let versioning = versioning_guard.deref_mut();
        let mut current_version_id = VarTransaction::new(&mut versioning.current_version_id);
        let mut hummock_version_deltas =
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
        let mut sstable_id_infos = BTreeMapTransaction::new(&mut versioning.sstable_id_infos);
        current_version_id.increase();
        let new_version_id = current_version_id.id();
        let mut new_version_delta = hummock_version_deltas.new_entry_insert_txn(
            current_version_id.id(),
            HummockVersionDelta {
                prev_id: old_version.id,
                safe_epoch: old_version.safe_epoch,
                ..Default::default()
            },
        );
        let mut new_hummock_version = old_version.clone();
        new_hummock_version.id = current_version_id.id();
        new_version_delta.id = current_version_id.id();
        if epoch <= new_hummock_version.max_committed_epoch {
            return Err(Error::InternalError(format!(
                "Epoch {} <= max_committed_epoch {}",
                epoch, new_hummock_version.max_committed_epoch
            )));
        }

        // Track SSTs in meta.
        // TODO: If a epoch contains many SSTs, modifying sstable_id_infos will result many KVs in
        // the meta store transaction. To avoid etcd errors if the aforementioned case
        // happens, we temporarily set a large value for etcd's max-txn-ops. But we need to
        // formally fix this because the performance degradation is not acceptable anyway.
        let mut total_files_size = 0;
        for sst in sstables.iter().map(|(_, sst)| sst) {
            match sstable_id_infos.get_mut(sst.id) {
                None => {
                    return Err(Error::InternalError(format!(
                        "Invalid SST id {}, may have been vacuumed",
                        sst.id
                    )));
                }
                Some(mut sst_id_info) => {
                    if sst_id_info.meta_delete_timestamp != INVALID_TIMESTAMP {
                        return Err(Error::InternalError(format!(
                            "SST id {} has been marked for vacuum",
                            sst.id
                        )));
                    }
                    if sst_id_info.meta_create_timestamp != INVALID_TIMESTAMP {
                        return Err(Error::InternalError(format!(
                            "SST id {} has been committed",
                            sst.id
                        )));
                    }
                    sst_id_info.meta_create_timestamp = sstable_id_info::get_timestamp_now();
                    total_files_size += sst.file_size;
                }
            }
        }

        let mut modified_compaction_groups = vec![];
        // Append SSTs to a new version.
        for (compaction_group_id, sstables) in &sstables.into_iter().group_by(|(cg_id, _)| *cg_id) {
            modified_compaction_groups.push(compaction_group_id);
            let group_sstables = sstables.into_iter().map(|(_, sst)| sst).collect_vec();
            let level_deltas = &mut new_version_delta
                .level_deltas
                .entry(compaction_group_id)
                .or_default()
                .level_deltas;
            let level_delta = LevelDelta {
                level_idx: 0,
                inserted_table_infos: group_sstables.clone(),
                ..Default::default()
            };
            level_deltas.push(level_delta);

            let version_first_level = new_hummock_version
                .get_compaction_group_levels_mut(compaction_group_id)
                .first_mut()
                .expect("Expect at least one level");
            assert_eq!(version_first_level.level_idx, 0);
            assert_eq!(
                version_first_level.level_type,
                LevelType::Overlapping as i32
            );
            version_first_level.table_infos.extend(group_sstables);
            version_first_level.total_file_size += total_files_size;
        }

        // Create a new_version, possibly merely to bump up the version id and max_committed_epoch.
        new_version_delta.max_committed_epoch = epoch;
        new_hummock_version.max_committed_epoch = epoch;
        commit_multi_var!(
            self,
            None,
            new_version_delta,
            current_version_id,
            sstable_id_infos
        )?;
        versioning
            .hummock_versions
            .insert(new_version_id, new_hummock_version.get_sst_ids());
        versioning.current_version = new_hummock_version;
        self.max_committed_epoch.store(epoch, Ordering::Release);

        // Update metrics
        trigger_commit_stat(&self.metrics, versioning.current_version_ref());

        tracing::trace!("new committed epoch {}", epoch);

        self.env
            .notification_manager()
            .notify_frontend_asynchronously(
                Operation::Update, // Frontends don't care about operation.
                Info::HummockSnapshot(HummockSnapshot { epoch }),
            );

        drop(versioning_guard);

        // commit_epoch may contains SSTs from any compaction group
        for id in modified_compaction_groups {
            self.try_send_compaction_request(id);
        }

        #[cfg(test)]
        {
            self.check_state_consistency().await;
        }

        Ok(())
    }

    #[named]
    pub async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        // TODO id_gen_manager generates u32, we need u64
        let sstable_id = get_remote_sst_id(
            self.env
                .id_gen_manager()
                .generate::<{ IdCategory::HummockSSTableId }>()
                .await
                .map(|id| id as HummockSSTableId)?,
        );

        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let new_sst_id_info = SstableIdInfo {
            id: sstable_id,
            id_create_timestamp: sstable_id_info::get_timestamp_now(),
            meta_create_timestamp: INVALID_TIMESTAMP,
            meta_delete_timestamp: INVALID_TIMESTAMP,
        };
        new_sst_id_info.insert(self.env.meta_store()).await?;

        // Update in-mem state after transaction succeeds.
        versioning_guard
            .sstable_id_infos
            .insert(new_sst_id_info.id, new_sst_id_info);

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(sstable_id)
    }

    /// Release resources pinned by these contexts, including:
    /// - Version
    /// - Snapshot
    /// - Compaction task
    #[named]
    pub async fn release_contexts(
        &self,
        context_ids: impl AsRef<[HummockContextId]>,
    ) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut compact_task_assignment =
            BTreeMapTransaction::new(&mut compaction.compact_task_assignment);
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let mut pinned_versions = BTreeMapTransaction::new(&mut versioning.pinned_versions);
        let mut pinned_snapshots = BTreeMapTransaction::new(&mut versioning.pinned_snapshots);
        for context_id in context_ids.as_ref() {
            tracing::debug!("Release context {}", *context_id);
            for assignment in compact_task_assignment.tree_ref().values() {
                if assignment.context_id != *context_id {
                    continue;
                }
                let task = assignment
                    .compact_task
                    .as_ref()
                    .expect("compact_task shouldn't be None");
                let mut compact_status = compact_statuses
                    .get_mut(task.compaction_group_id)
                    .ok_or(Error::InvalidCompactionGroup(task.compaction_group_id))?;
                compact_status.report_compact_task(
                    assignment
                        .compact_task
                        .as_ref()
                        .expect("compact_task shouldn't be None"),
                );
            }
            let task_ids_to_remove = compact_task_assignment
                .tree_ref()
                .iter()
                .filter_map(|(task_id, v)| {
                    if v.context_id == *context_id {
                        Some(*task_id)
                    } else {
                        None
                    }
                })
                .collect_vec();
            for task_id in task_ids_to_remove {
                compact_task_assignment.remove(task_id);
            }
            pinned_versions.remove(*context_id);
            pinned_snapshots.remove(*context_id);
        }
        commit_multi_var!(
            self,
            None,
            compact_statuses,
            compact_task_assignment,
            pinned_versions,
            pinned_snapshots
        )?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            drop(compaction_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// List version ids in ascending order.
    #[named]
    pub async fn list_version_ids_asc(&self) -> Result<Vec<HummockVersionId>> {
        let versioning_guard = read_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let version_ids = versioning_guard
            .hummock_versions
            .keys()
            .cloned()
            .collect_vec();
        Ok(version_ids)
    }

    #[named]
    pub async fn proceed_version_checkpoint(&self) -> risingwave_common::error::Result<()> {
        let mut checkpoint = HummockVersion::list(self.env.meta_store())
            .await?
            .into_iter()
            .at_most_one()
            .unwrap()
            .unwrap();
        let old_checkpoint_id = checkpoint.id;
        let mut version_deltas_to_delete = BTreeMap::new();

        {
            let mut versioning_guard = write_lock!(self, versioning).await;
            let new_checkpoint_id = *versioning_guard
                .hummock_versions
                .first_key_value()
                .unwrap()
                .0;
            for (_, version_delta) in versioning_guard
                .hummock_version_deltas
                .range((Excluded(old_checkpoint_id), Included(new_checkpoint_id)))
            {
                checkpoint.apply_version_delta(version_delta);
            }
            checkpoint.insert(self.env.meta_store()).await?;

            version_deltas_to_delete.append(&mut versioning_guard.hummock_version_deltas);
            versioning_guard.hummock_version_deltas =
                version_deltas_to_delete.split_off(&(new_checkpoint_id + 1));
        }

        let mut trx = Transaction::default();
        for version_delta_item in version_deltas_to_delete.values() {
            version_delta_item.delete_in_transaction(&mut trx)?;
        }
        let ret = self
            .commit_trx(self.env.meta_store(), trx, None, self.env.get_leader_info())
            .await;

        if ret.is_err() {
            let mut versioning_guard = write_lock!(self, versioning).await;
            versioning_guard
                .hummock_version_deltas
                .append(&mut version_deltas_to_delete);
            return Err(ret.unwrap_err().into());
        }

        Ok(())
    }

    /// Get the reference count of given version id
    #[named]
    pub async fn get_version_pin_count(
        &self,
        version_id: HummockVersionId,
    ) -> Result<HummockRefCount> {
        let versioning_guard = read_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let count = versioning_guard
            .pinned_versions
            .values()
            .filter(|version_pin| version_pin.min_pinned_id <= version_id)
            .count();
        Ok(count as HummockRefCount)
    }

    #[named]
    pub async fn get_ssts_to_delete(
        &self,
        version_id: HummockVersionId,
    ) -> Result<Vec<HummockSSTableId>> {
        let versioning_guard = read_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        Ok(versioning_guard
            .stale_sstables
            .get(&version_id)
            .map(|s| s.id.clone())
            .unwrap_or_default())
    }

    #[named]
    pub async fn delete_will_not_be_used_ssts(
        &self,
        version_id: HummockVersionId,
        ssts_in_use: &HashSet<HummockSSTableId>,
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let mut stale_sstables = BTreeMapTransaction::new(&mut versioning.stale_sstables);
        let mut sstable_id_infos = BTreeMapTransaction::new(&mut versioning.sstable_id_infos);
        if let Some(mut ssts_to_delete) = stale_sstables.get_mut(version_id) {
            // Delete sstables that are stale in the view of `version_id` Version, and
            // are Not referred by any other older Version.
            // No newer version would use any stale sstables in the current Version.
            let num_ssts_to_delete = ssts_to_delete.id.len();
            for idx in (0..num_ssts_to_delete).rev() {
                let sst_id = ssts_to_delete.id[idx];
                if !ssts_in_use.contains(&sst_id) && let Some(mut sst_id_info) = sstable_id_infos.get_mut(sst_id) {
                    sst_id_info.meta_delete_timestamp = sstable_id_info::get_timestamp_now();
                    // We don't want to repetitively set the delete timestamp of these that have been set,
                    // so we remove these ones.
                    ssts_to_delete.id.swap_remove(idx);
                }
            }
        }

        commit_multi_var!(self, None, stale_sstables, sstable_id_infos)?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Delete metadata of the given `version_ids`
    #[named]
    pub async fn delete_versions(&self, version_ids: &[HummockVersionId]) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let versioning = versioning_guard.deref_mut();
        let pinned_versions_ref = &versioning.pinned_versions;
        let mut hummock_versions = BTreeMapTransaction::new(&mut versioning.hummock_versions);
        let mut stale_sstables = BTreeMapTransaction::new(&mut versioning.stale_sstables);
        for version_id in version_ids {
            if hummock_versions.remove(*version_id).is_none() {
                continue;
            }
            if let Some(ssts_to_delete) = stale_sstables.get(version_id) {
                if !ssts_to_delete.id.is_empty() {
                    return Err(Error::InternalError(format!(
                        "Version {} still has stale ssts undeleted:{:?}",
                        version_id, ssts_to_delete.id
                    )));
                }
            }
            stale_sstables.remove(*version_id);

            for version_pin in pinned_versions_ref.values() {
                assert!(
                    version_pin.min_pinned_id > *version_id,
                    "version still referenced shouldn't be deleted."
                );
            }
        }
        // We do not call `commit_multi_var!(hummock_versions)` because meta store does not store
        // multiple `HummockVersion`s. Currently only memory stores multiple `HummockVersion`s, so
        // we only need to commit in memory.
        commit_multi_var!(self, None, stale_sstables)?;
        hummock_versions.commit_memory();

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    // TODO: use proc macro to call check_state_consistency
    #[named]
    #[cfg(test)]
    pub async fn check_state_consistency(&self) {
        use std::borrow::Borrow;
        let mut compaction_guard = write_lock!(self, compaction).await;
        let mut versioning_guard = write_lock!(self, versioning).await;
        let get_state =
            |compaction_guard: &RwLockWriteGuard<'_, Compaction>,
             versioning_guard: &RwLockWriteGuard<'_, Versioning>| {
                let compact_statuses_copy = compaction_guard.compaction_statuses.clone();
                let compact_task_assignment_copy = compaction_guard.compact_task_assignment.clone();
                let current_version_id_copy = versioning_guard.current_version_id.clone();
                // let hummmock_versions_copy = versioning_guard.hummock_versions.clone();
                let pinned_versions_copy = versioning_guard.pinned_versions.clone();
                let pinned_snapshots_copy = versioning_guard.pinned_snapshots.clone();
                let stale_sstables_copy = versioning_guard.stale_sstables.clone();
                let sst_id_infos_copy = versioning_guard.sstable_id_infos.clone();
                (
                    compact_statuses_copy,
                    compact_task_assignment_copy,
                    current_version_id_copy,
                    // hummmock_versions_copy,
                    pinned_versions_copy,
                    pinned_snapshots_copy,
                    stale_sstables_copy,
                    sst_id_infos_copy,
                )
            };
        let mem_state = get_state(compaction_guard.borrow(), versioning_guard.borrow());
        self.load_meta_store_state_impl(
            compaction_guard.borrow_mut(),
            versioning_guard.borrow_mut(),
        )
        .await
        .expect("Failed to load state from meta store");
        let loaded_state = get_state(compaction_guard.borrow(), versioning_guard.borrow());
        assert_eq!(
            mem_state, loaded_state,
            "hummock in-mem state is inconsistent with meta store state",
        );
    }

    /// When `version_id` is `None`, this function returns all the `SstableIdInfo` across all the
    /// versions. With `version_id` being specified, this function returns all the
    /// `SstableIdInfo` of `version_id` Version.
    #[named]
    pub async fn list_sstable_id_infos(
        &self,
        version_id: Option<HummockVersionId>,
    ) -> Result<Vec<SstableIdInfo>> {
        let versioning_guard = read_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        if version_id.is_none() {
            Ok(versioning_guard
                .sstable_id_infos
                .values()
                .cloned()
                .collect_vec())
        } else {
            let version_id = version_id.unwrap();
            let versioning = versioning_guard.hummock_versions.get(&version_id);
            versioning
                .map(|versioning| {
                    versioning
                        .iter()
                        .map(|id| versioning_guard.sstable_id_infos.get(id).unwrap().clone())
                        .collect_vec()
                })
                .ok_or_else(|| {
                    Error::InternalError(format!(
                        "list_sstable_id_infos cannot find version:{}",
                        version_id
                    ))
                })
        }
    }

    #[named]
    pub async fn delete_sstable_ids(&self, sst_ids: impl AsRef<[HummockSSTableId]>) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut sstable_id_infos = BTreeMapTransaction::new(&mut versioning_guard.sstable_id_infos);

        // Update in-mem state after transaction succeeds.
        for sst_id in sst_ids.as_ref() {
            sstable_id_infos.remove(*sst_id);
        }

        commit_multi_var!(self, None, sstable_id_infos)?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        Ok(())
    }

    /// Release invalid contexts, aka worker node ids which are no longer valid in `ClusterManager`.
    #[named]
    async fn release_invalid_contexts(&self) -> Result<Vec<HummockContextId>> {
        let active_context_ids = {
            let compaction_guard = read_lock!(self, compaction).await;
            let versioning_guard = read_lock!(self, versioning).await;
            let _timer = start_measure_real_process_timer!(self);
            let mut active_context_ids = HashSet::new();
            active_context_ids.extend(
                compaction_guard
                    .compact_task_assignment
                    .values()
                    .map(|c| c.context_id),
            );
            active_context_ids.extend(versioning_guard.pinned_versions.keys());
            active_context_ids.extend(versioning_guard.pinned_snapshots.keys());
            active_context_ids
        };

        let mut invalid_context_ids = vec![];
        for active_context_id in &active_context_ids {
            if !self.check_context(*active_context_id).await {
                invalid_context_ids.push(*active_context_id);
            }
        }

        self.release_contexts(&invalid_context_ids).await?;

        Ok(invalid_context_ids)
    }

    /// Checks whether `context_id` is valid.
    pub async fn check_context(&self, context_id: HummockContextId) -> bool {
        self.cluster_manager
            .get_worker_by_id(context_id)
            .await
            .is_some()
    }

    /// Marks SSTs which haven't been added in meta (`meta_create_timestamp` is not set) for at
    /// least `sst_retention_interval` since `id_create_timestamp`
    #[named]
    pub async fn mark_orphan_ssts(
        &self,
        sst_retention_interval: Duration,
    ) -> Result<Vec<SstableIdInfo>> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let _timer = start_measure_real_process_timer!(self);
        let mut sstable_id_infos = BTreeMapTransaction::new(&mut versioning_guard.sstable_id_infos);

        let now = sstable_id_info::get_timestamp_now();
        let mut marked = vec![];
        for (sstable_id, sstable_id_info) in sstable_id_infos.tree_ref().iter() {
            let mut sstable_id_info = sstable_id_info.clone();
            if sstable_id_info.meta_delete_timestamp != INVALID_TIMESTAMP {
                continue;
            }
            let is_orphan = sstable_id_info.meta_create_timestamp == INVALID_TIMESTAMP
                && now >= sstable_id_info.id_create_timestamp
                && now - sstable_id_info.id_create_timestamp >= sst_retention_interval.as_secs();
            if is_orphan {
                sstable_id_info.meta_delete_timestamp = now;
                marked.push((*sstable_id, sstable_id_info.clone()));
            }
        }
        if marked.is_empty() {
            return Ok(vec![]);
        }

        for (sstable_id, sstable_id_info) in &marked {
            sstable_id_infos.insert(*sstable_id, sstable_id_info.clone());
        }

        commit_multi_var!(self, None, sstable_id_infos)?;

        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }

        tracing::debug!("Mark {:?} as orphan SSTs", marked);
        Ok(marked.into_iter().map(|t| t.1).collect())
    }

    /// Gets current version without pinning it.
    #[named]
    pub async fn get_current_version(&self) -> HummockVersion {
        read_lock!(self, versioning).await.current_version()
    }

    pub fn set_compaction_scheduler(&self, sender: CompactionRequestChannelRef) {
        *self.compaction_scheduler.write() = Some(sender);
    }

    /// Cancels pending compaction tasks which are not yet assigned to any compactor.
    #[named]
    async fn cancel_unassigned_compaction_task(&self) -> Result<()> {
        let mut compaction_guard = write_lock!(self, compaction).await;
        let compaction = compaction_guard.deref_mut();
        let mut compact_statuses = BTreeMapTransaction::new(&mut compaction.compaction_statuses);
        let mut cancelled_count = 0;
        let mut modified_group_status = vec![];
        for (group_id, compact_status) in compact_statuses.tree_ref().iter() {
            let mut compact_status = compact_status.clone();
            let count = compact_status.cancel_compaction_tasks_if(|pending_task_id| {
                !compaction
                    .compact_task_assignment
                    .contains_key(&pending_task_id)
            });
            if count > 0 {
                cancelled_count += count;
                modified_group_status.push((*group_id, compact_status));
            }
        }
        for (group_id, compact_status) in modified_group_status {
            compact_statuses.insert(group_id, compact_status);
        }
        if cancelled_count > 0 {
            commit_multi_var!(self, None, compact_statuses)?;
        }
        #[cfg(test)]
        {
            drop(compaction_guard);
            self.check_state_consistency().await;
        }
        Ok(())
    }

    /// Sends a compaction request to compaction scheduler.
    pub fn try_send_compaction_request(&self, compaction_group: CompactionGroupId) -> bool {
        if let Some(sender) = self.compaction_scheduler.read().as_ref() {
            return sender.try_send(compaction_group);
        }
        false
    }

    #[named]
    pub async fn trigger_manual_compaction(
        &self,
        compaction_group: CompactionGroupId,
        manual_compaction_option: ManualCompactionOption,
    ) -> Result<()> {
        let start_time = Instant::now();

        // 1. select_compactor
        let compactor = self.compactor_manager.random_compactor();
        let compactor = match compactor {
            None => {
                tracing::warn!("trigger_manual_compaction No compactor is available.");
                return Err(Error::InternalError(format!(
                    "trigger_manual_compaction No compactor is available. compaction_group {}",
                    compaction_group
                )));
            }

            Some(compactor) => compactor,
        };

        // 2. manual_get_compact_task
        let compact_task = self
            .manual_get_compact_task(compaction_group, manual_compaction_option)
            .await;
        let compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                // No compaction task available.
                return Err(Error::InternalError(format!(
                    "trigger_manual_compaction No compaction_task is available. compaction_group {}",
                    compaction_group
                )));
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                return Err(Error::InternalError(format!(
                    "Failed to get compaction task: {:#?} compaction_group {}",
                    err, compaction_group
                )));
            }
        };

        let send_task = async {
            tokio::time::timeout(Duration::from_secs(3), async {
                compactor
                    .send_task(Some(compact_task.clone()), None)
                    .await
                    .is_ok()
            })
            .await
            .unwrap_or(false)
        };

        match self
            .assign_compaction_task(&compact_task, compactor.context_id(), send_task)
            .await
        {
            Ok(_) => {}

            Err(error) => {
                // cancel task in memory
                let mut compaction_guard = write_lock!(self, compaction).await;
                let compaction = compaction_guard.deref_mut();
                let compact_status = compaction
                    .compaction_statuses
                    .get_mut(&compact_task.task_id)
                    .unwrap();
                compact_status.cancel_compaction_tasks_if(|pending_task_id| {
                    pending_task_id == compact_task.task_id
                });

                return Err(error);
            }
        }

        tracing::info!(
            "Trigger manual compaction task. {}. cost time: {:?}",
            compact_task_to_string(&compact_task),
            start_time.elapsed(),
        );

        Ok(())
    }

    pub fn compactor_manager_ref_for_test(&self) -> CompactorManagerRef {
        self.compactor_manager.clone()
    }

    #[named]
    pub async fn compaction_task_from_assignment_for_test(
        &self,
        task_id: u64,
    ) -> Option<CompactTaskAssignment> {
        let compaction_guard = read_lock!(self, compaction).await;
        let assignment_ref = &compaction_guard.compact_task_assignment;
        assignment_ref.get(&task_id).cloned()
    }

    pub fn compaction_group_manager_ref_for_test(&self) -> CompactionGroupManagerRef<S> {
        self.compaction_group_manager.clone()
    }
}
