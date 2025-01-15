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

mod split_assignment;
mod worker;
use std::borrow::BorrowMut;
use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::panic_if_debug;
use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::{
    fill_adaptive_split, ConnectorProperties, SourceEnumeratorContext, SourceEnumeratorInfo,
    SplitId, SplitImpl, SplitMetaData,
};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_meta_model::SourceId;
use risingwave_pb::catalog::Source;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::Dispatcher;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio::{select, time};
pub use worker::create_source_worker;
use worker::{create_source_worker_async, ConnectorSourceWorkerHandle};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::MetadataManager;
use crate::model::{ActorId, FragmentId, StreamJobFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::MetaResult;

pub type SourceManagerRef = Arc<SourceManager>;
pub type SplitAssignment = HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>;
pub type ThrottleConfig = HashMap<FragmentId, HashMap<ActorId, Option<u32>>>;

/// `SourceManager` keeps fetching the latest split metadata from the external source services ([`worker::ConnectorSourceWorker::tick`]),
/// and sends a split assignment command if split changes detected ([`Self::tick`]).
pub struct SourceManager {
    pub paused: Mutex<()>,
    barrier_scheduler: BarrierScheduler,
    core: Mutex<SourceManagerCore>,
    pub metrics: Arc<MetaMetrics>,
}
pub struct SourceManagerCore {
    metadata_manager: MetadataManager,

    /// Managed source loops
    managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
    /// Fragments associated with each source
    source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// `source_id` -> `(fragment_id, upstream_fragment_id)`
    backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,

    /// Splits assigned per actor,
    /// incl. both `Source` and `SourceBackfill`.
    actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

pub struct SourceManagerRunningInfo {
    pub source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    pub backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
}

impl SourceManagerCore {
    fn new(
        metadata_manager: MetadataManager,
        managed_sources: HashMap<SourceId, ConnectorSourceWorkerHandle>,
        source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
        actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
    ) -> Self {
        Self {
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            actor_splits,
        }
    }

    /// Updates states after all kinds of source change.
    pub fn apply_source_change(&mut self, source_change: SourceChange) {
        let mut added_source_fragments = Default::default();
        let mut added_backfill_fragments = Default::default();
        let mut split_assignment = Default::default();
        let mut dropped_actors = Default::default();
        let mut fragment_replacements = Default::default();
        let mut dropped_source_fragments = Default::default();
        let mut dropped_source_ids = Default::default();

        match source_change {
            SourceChange::CreateJob {
                added_source_fragments: added_source_fragments_,
                added_backfill_fragments: added_backfill_fragments_,
                split_assignment: split_assignment_,
            } => {
                added_source_fragments = added_source_fragments_;
                added_backfill_fragments = added_backfill_fragments_;
                split_assignment = split_assignment_;
            }
            SourceChange::SplitChange(split_assignment_) => {
                split_assignment = split_assignment_;
            }
            SourceChange::DropMv {
                dropped_source_fragments: dropped_source_fragments_,
                dropped_actors: dropped_actors_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
                dropped_actors = dropped_actors_;
            }
            SourceChange::ReplaceJob {
                dropped_source_fragments: dropped_source_fragments_,
                dropped_actors: dropped_actors_,
                added_source_fragments: added_source_fragments_,
                split_assignment: split_assignment_,
                fragment_replacements: fragment_replacements_,
            } => {
                dropped_source_fragments = dropped_source_fragments_;
                dropped_actors = dropped_actors_;
                added_source_fragments = added_source_fragments_;
                split_assignment = split_assignment_;
                fragment_replacements = fragment_replacements_;
            }
            SourceChange::DropSource {
                dropped_source_ids: dropped_source_ids_,
            } => {
                dropped_source_ids = dropped_source_ids_;
            }
            SourceChange::Reschedule {
                split_assignment: split_assignment_,
                dropped_actors: dropped_actors_,
            } => {
                split_assignment = split_assignment_;
                dropped_actors = dropped_actors_;
            }
        }

        for source_id in dropped_source_ids {
            let dropped_fragments = self.source_fragments.remove(&source_id);

            if let Some(handle) = self.managed_sources.remove(&source_id) {
                handle.terminate(dropped_fragments);
            }
            if let Some(_fragments) = self.backfill_fragments.remove(&source_id) {
                // TODO: enable this assertion after we implemented cleanup for backfill fragments
                // debug_assert!(
                //     fragments.is_empty(),
                //     "when dropping source, there should be no backfill fragments, got: {:?}",
                //     fragments
                // );
            }
        }

        for (source_id, fragments) in added_source_fragments {
            self.source_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (source_id, fragments) in added_backfill_fragments {
            // Note: when the backfill fragment is considered created, backfill must be finished.
            let handle = self.managed_sources.get(&source_id).unwrap_or_else(|| {
                panic!(
                    "source {} not found when adding backfill fragments {:?}",
                    source_id, fragments
                );
            });
            handle.finish_backfill(fragments.iter().map(|(id, _up_id)| *id).collect());
            self.backfill_fragments
                .entry(source_id)
                .or_default()
                .extend(fragments);
        }

        for (_, actor_splits) in split_assignment {
            for (actor_id, splits) in actor_splits {
                // override previous splits info
                self.actor_splits.insert(actor_id, splits);
            }
        }

        for actor_id in dropped_actors {
            self.actor_splits.remove(&actor_id);
        }

        for (source_id, fragment_ids) in dropped_source_fragments {
            self.drop_source_fragments(Some(source_id), fragment_ids);
        }

        for (old_fragment_id, new_fragment_id) in fragment_replacements {
            // TODO: add source_id to the fragment_replacements to avoid iterating all sources
            self.drop_source_fragments(None, BTreeSet::from([old_fragment_id]));

            for fragment_ids in self.backfill_fragments.values_mut() {
                let mut new_backfill_fragment_ids = fragment_ids.clone();
                for (fragment_id, upstream_fragment_id) in fragment_ids.iter() {
                    assert_ne!(
                        fragment_id, upstream_fragment_id,
                        "backfill fragment should not be replaced"
                    );
                    if *upstream_fragment_id == old_fragment_id {
                        new_backfill_fragment_ids.remove(&(*fragment_id, *upstream_fragment_id));
                        new_backfill_fragment_ids.insert((*fragment_id, new_fragment_id));
                    }
                }
                *fragment_ids = new_backfill_fragment_ids;
            }
        }
    }

    fn drop_source_fragments(
        &mut self,
        source_id: Option<SourceId>,
        dropped_fragment_ids: BTreeSet<FragmentId>,
    ) {
        if let Some(source_id) = source_id {
            if let Entry::Occupied(mut entry) = self.source_fragments.entry(source_id) {
                let mut dropped_ids = vec![];
                let managed_fragment_ids = entry.get_mut();
                for fragment_id in &dropped_fragment_ids {
                    managed_fragment_ids.remove(fragment_id);
                    dropped_ids.push(*fragment_id);
                }
                if let Some(handle) = self.managed_sources.get(&source_id) {
                    handle.drop_fragments(dropped_ids);
                } else {
                    panic_if_debug!(
                        "source {source_id} not found when dropping fragment {dropped_ids:?}",
                    );
                }
                if managed_fragment_ids.is_empty() {
                    entry.remove();
                }
            }
        } else {
            for (source_id, fragment_ids) in &mut self.source_fragments {
                let mut dropped_ids = vec![];
                for fragment_id in &dropped_fragment_ids {
                    if fragment_ids.remove(fragment_id) {
                        dropped_ids.push(*fragment_id);
                    }
                }
                if !dropped_ids.is_empty() {
                    if let Some(handle) = self.managed_sources.get(source_id) {
                        handle.drop_fragments(dropped_ids);
                    } else {
                        panic_if_debug!(
                            "source {source_id} not found when dropping fragment {dropped_ids:?}",
                        );
                    }
                }
            }
        }
    }
}

impl SourceManager {
    const DEFAULT_SOURCE_TICK_INTERVAL: Duration = Duration::from_secs(10);

    pub async fn new(
        barrier_scheduler: BarrierScheduler,
        metadata_manager: MetadataManager,
        metrics: Arc<MetaMetrics>,
    ) -> MetaResult<Self> {
        let mut managed_sources = HashMap::new();
        {
            let sources = metadata_manager.list_sources().await?;
            for source in sources {
                create_source_worker_async(source, &mut managed_sources, metrics.clone())?
            }
        }

        let source_fragments = metadata_manager
            .catalog_controller
            .load_source_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids.into_iter().map(|id| id as _).collect(),
                )
            })
            .collect();
        let backfill_fragments = metadata_manager
            .catalog_controller
            .load_backfill_fragment_ids()
            .await?
            .into_iter()
            .map(|(source_id, fragment_ids)| {
                (
                    source_id as SourceId,
                    fragment_ids
                        .into_iter()
                        .map(|(id, up_id)| (id as _, up_id as _))
                        .collect(),
                )
            })
            .collect();
        let actor_splits = metadata_manager
            .catalog_controller
            .load_actor_splits()
            .await?
            .into_iter()
            .map(|(actor_id, splits)| {
                (
                    actor_id as ActorId,
                    splits
                        .to_protobuf()
                        .splits
                        .iter()
                        .map(|split| SplitImpl::try_from(split).unwrap())
                        .collect(),
                )
            })
            .collect();

        let core = Mutex::new(SourceManagerCore::new(
            metadata_manager,
            managed_sources,
            source_fragments,
            backfill_fragments,
            actor_splits,
        ));

        Ok(Self {
            barrier_scheduler,
            core,
            paused: Mutex::new(()),
            metrics,
        })
    }

    /// For replacing job (alter table/source, create sink into table).
    pub async fn handle_replace_job(
        &self,
        dropped_job_fragments: &StreamJobFragments,
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        split_assignment: SplitAssignment,
        fragment_replacements: HashMap<FragmentId, FragmentId>,
    ) {
        // Extract the fragments that include source operators.
        let dropped_source_fragments = dropped_job_fragments.stream_source_fragments().clone();

        let fragments = &dropped_job_fragments.fragments;

        let dropped_actors = dropped_source_fragments
            .values()
            .flatten()
            .flat_map(|fragment_id| fragments.get(fragment_id).unwrap().get_actors())
            .map(|actor| actor.get_actor_id())
            .collect::<HashSet<_>>();

        self.apply_source_change(SourceChange::ReplaceJob {
            dropped_source_fragments,
            dropped_actors,
            added_source_fragments,
            split_assignment,
            fragment_replacements,
        })
        .await;
    }

    /// Updates states after all kinds of source change.
    /// e.g., split change (`post_collect` barrier) or scaling (`post_apply_reschedule`).
    pub async fn apply_source_change(&self, source_change: SourceChange) {
        let mut core = self.core.lock().await;
        core.apply_source_change(source_change);
    }

    /// create and register connector worker for source.
    pub async fn register_source(&self, source: &Source) -> MetaResult<()> {
        tracing::debug!("register_source: {}", source.get_id());
        let mut core = self.core.lock().await;
        if let Entry::Vacant(e) = core.managed_sources.entry(source.get_id() as _) {
            let handle = create_source_worker(source, self.metrics.clone())
                .await
                .context("failed to create source worker")?;
            e.insert(handle);
        } else {
            tracing::warn!("source {} already registered", source.get_id());
        }
        Ok(())
    }

    /// register connector worker for source.
    pub async fn register_source_with_handle(
        &self,
        source_id: SourceId,
        handle: ConnectorSourceWorkerHandle,
    ) {
        let mut core = self.core.lock().await;
        if let Entry::Vacant(e) = core.managed_sources.entry(source_id) {
            e.insert(handle);
        } else {
            tracing::warn!("source {} already registered", source_id);
        }
    }

    pub async fn list_assignments(&self) -> HashMap<ActorId, Vec<SplitImpl>> {
        let core = self.core.lock().await;
        core.actor_splits.clone()
    }

    pub async fn get_running_info(&self) -> SourceManagerRunningInfo {
        let core = self.core.lock().await;
        SourceManagerRunningInfo {
            source_fragments: core.source_fragments.clone(),
            backfill_fragments: core.backfill_fragments.clone(),
            actor_splits: core.actor_splits.clone(),
        }
    }

    /// Checks whether the external source metadata has changed, and sends a split assignment command
    /// if it has.
    ///
    /// This is also how a newly created `SourceExecutor` is initialized.
    /// (force `tick` in `Self::create_source_worker`)
    ///
    /// The command will first updates `SourceExecutor`'s splits, and finally calls `Self::apply_source_change`
    /// to update states in `SourceManager`.
    async fn tick(&self) -> MetaResult<()> {
        let split_assignment = {
            let core_guard = self.core.lock().await;
            core_guard.reassign_splits().await?
        };

        for (database_id, split_assignment) in split_assignment {
            if !split_assignment.is_empty() {
                let command = Command::SourceChangeSplit(split_assignment);
                tracing::info!(command = ?command, "pushing down split assignment command");
                self.barrier_scheduler
                    .run_command(database_id, command)
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn run(&self) -> MetaResult<()> {
        let mut ticker = time::interval(Self::DEFAULT_SOURCE_TICK_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let _pause_guard = self.paused.lock().await;
            if let Err(e) = self.tick().await {
                tracing::error!(
                    error = %e.as_report(),
                    "error happened while running source manager tick",
                );
            }
        }
    }

    /// Pause the tick loop in source manager until the returned guard is dropped.
    pub async fn pause_tick(&self) -> MutexGuard<'_, ()> {
        tracing::debug!("pausing tick lock in source manager");
        self.paused.lock().await
    }
}

pub enum SourceChange {
    /// `CREATE SOURCE` (shared), or `CREATE MV`.
    /// Note this is applied _after_ the job is successfully created (`post_collect` barrier).
    CreateJob {
        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        added_backfill_fragments: HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>,
        split_assignment: SplitAssignment,
    },
    SplitChange(SplitAssignment),
    /// `DROP SOURCE` or `DROP MV`
    DropSource {
        dropped_source_ids: Vec<SourceId>,
    },
    DropMv {
        // FIXME: we should consider source backfill fragments here for MV on shared source.
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        dropped_actors: HashSet<ActorId>,
    },
    ReplaceJob {
        dropped_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        dropped_actors: HashSet<ActorId>,

        added_source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
        split_assignment: SplitAssignment,
        fragment_replacements: HashMap<FragmentId, FragmentId>,
    },
    Reschedule {
        split_assignment: SplitAssignment,
        dropped_actors: HashSet<ActorId>,
    },
}

pub fn build_actor_connector_splits(
    splits: &HashMap<ActorId, Vec<SplitImpl>>,
) -> HashMap<u32, ConnectorSplits> {
    splits
        .iter()
        .map(|(&actor_id, splits)| {
            (
                actor_id,
                ConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                },
            )
        })
        .collect()
}

pub fn build_actor_split_impls(
    actor_splits: &HashMap<u32, ConnectorSplits>,
) -> HashMap<ActorId, Vec<SplitImpl>> {
    actor_splits
        .iter()
        .map(|(actor_id, ConnectorSplits { splits })| {
            (
                *actor_id,
                splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .collect(),
            )
        })
        .collect()
}
