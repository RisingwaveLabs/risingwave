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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model_v2::ObjectId;
use risingwave_pb::catalog::{CreateType, Table};
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use super::command::CommandContext;
use crate::barrier::{
    Command, TableActorMap, TableDefinitionMap, TableFragmentMap, TableInternalTableMap,
    TableStreamJobMap, TableUpstreamMvCountMap,
};
use crate::manager::{
    DdlType, MetadataManager, MetadataManagerV1, MetadataManagerV2, StreamingJob,
};
use crate::model::{ActorId, TableFragments};
use crate::{MetaError, MetaResult};

type ConsumedRows = u64;

#[derive(Clone, Copy, Debug)]
enum BackfillState {
    Init,
    ConsumingUpstream(#[allow(dead_code)] Epoch, ConsumedRows),
    Done(ConsumedRows),
}

/// Progress of all actors containing backfill executors while creating mview.
#[derive(Debug)]
pub(super) struct Progress {
    states: HashMap<ActorId, BackfillState>,

    done_count: usize,

    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,

    /// Total key count in the upstream materialized view
    upstream_total_key_count: u64,

    /// Consumed rows
    consumed_rows: u64,

    /// DDL definition
    definition: String,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the backfill executors.
    fn new(
        actors: impl IntoIterator<Item = ActorId>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
    ) -> Self {
        let states = actors
            .into_iter()
            .map(|a| (a, BackfillState::Init))
            .collect::<HashMap<_, _>>();
        assert!(!states.is_empty());

        Self {
            states,
            done_count: 0,
            upstream_mv_count,
            upstream_total_key_count,
            consumed_rows: 0,
            definition,
        }
    }

    /// Update the progress of `actor`.
    fn update(&mut self, actor: ActorId, new_state: BackfillState, upstream_total_key_count: u64) {
        self.upstream_total_key_count = upstream_total_key_count;
        let total_actors = self.states.len();
        tracing::debug!(?actor, "update progress for actor");
        match self.states.remove(&actor).unwrap() {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, old_consumed_rows) => {
                self.consumed_rows -= old_consumed_rows;
            }
            BackfillState::Done(_) => panic!("should not report done multiple times"),
        };
        match &new_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, new_consumed_rows) => {
                self.consumed_rows += new_consumed_rows;
            }
            BackfillState::Done(new_consumed_rows) => {
                tracing::debug!("actor {} done", actor);
                self.consumed_rows += new_consumed_rows;
                self.done_count += 1;
                tracing::debug!(
                    "{} actors out of {} complete",
                    self.done_count,
                    total_actors,
                );
            }
        };
        self.states.insert(actor, new_state);
        self.calculate_progress();
    }

    /// Returns whether all backfill executors are done.
    fn is_done(&self) -> bool {
        self.done_count == self.states.len()
    }

    /// Returns the ids of all actors containing the backfill executors for the mview tracked by this
    /// [`Progress`].
    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.states.keys().cloned()
    }

    /// `progress` = `consumed_rows` / `upstream_total_key_count`
    fn calculate_progress(&self) -> f64 {
        if self.is_done() || self.states.is_empty() {
            return 1.0;
        }
        let mut upstream_total_key_count = self.upstream_total_key_count as f64;
        if upstream_total_key_count == 0.0 {
            upstream_total_key_count = 1.0
        }
        let mut progress = self.consumed_rows as f64 / upstream_total_key_count;
        if progress >= 1.0 {
            progress = 0.99;
        }
        progress
    }
}

/// There are 2 kinds of `TrackingJobs`:
/// 1. `New`. This refers to the "New" type of tracking job.
///    It is instantiated and managed by the stream manager.
///    On recovery, the stream manager will stop managing the job.
/// 2. `Recovered`. This refers to the "Recovered" type of tracking job.
///    On recovery, the barrier manager will recover and start managing the job.
pub enum TrackingJob {
    New(TrackingCommand),
    RecoveredV1(RecoveredTrackingJobV1),
    RecoveredV2(RecoveredTrackingJobV2),
}

impl TrackingJob {
    /// Returns whether the `TrackingJob` requires a checkpoint to complete.
    pub(crate) fn is_checkpoint_required(&self) -> bool {
        match self {
            // Recovered tracking job is always a streaming job,
            // It requires a checkpoint to complete.
            TrackingJob::RecoveredV1(_) | TrackingJob::RecoveredV2(_) => true,
            TrackingJob::New(command) => {
                command.context.kind.is_initial() || command.context.kind.is_checkpoint()
            }
        }
    }

    pub(crate) async fn pre_finish(&self) -> MetaResult<()> {
        match &self {
            TrackingJob::New(command) => match &command.context.command {
                Command::CreateStreamingJob {
                    table_fragments,
                    streaming_job,
                    internal_tables,
                    replace_table,
                    ..
                } => match command.context.metadata_manager() {
                    MetadataManager::V1(mgr) => {
                        mgr.fragment_manager
                            .mark_table_fragments_created(table_fragments.table_id())
                            .await?;
                        mgr.catalog_manager
                            .finish_stream_job(streaming_job.clone(), internal_tables.clone())
                            .await?;
                        Ok(())
                    }
                    MetadataManager::V2(mgr) => {
                        mgr.catalog_controller
                            .finish_streaming_job(streaming_job.id() as i32, replace_table.clone())
                            .await?;
                        Ok(())
                    }
                },
                _ => Ok(()),
            },
            TrackingJob::RecoveredV1(recovered) => {
                let manager = &recovered.metadata_manager;
                manager
                    .fragment_manager
                    .mark_table_fragments_created(recovered.fragments.table_id())
                    .await?;
                manager
                    .catalog_manager
                    .finish_stream_job(
                        recovered.streaming_job.clone(),
                        recovered.internal_tables.clone(),
                    )
                    .await?;
                Ok(())
            }
            TrackingJob::RecoveredV2(recovered) => {
                recovered
                    .metadata_manager
                    .catalog_controller
                    .finish_streaming_job(recovered.id, None)
                    .await?;
                Ok(())
            }
        }
    }

    pub(crate) fn table_to_create(&self) -> Option<TableId> {
        match self {
            TrackingJob::New(command) => command.context.table_to_create(),
            TrackingJob::RecoveredV1(recovered) => Some(recovered.fragments.table_id()),
            TrackingJob::RecoveredV2(recovered) => Some((recovered.id as u32).into()),
        }
    }

    pub(crate) async fn notify_finish_failed(&self, err: MetaError) {
        let id = match self {
            TrackingJob::New(command) => command.context.table_to_create(),
            TrackingJob::RecoveredV1(recovered) => Some(recovered.fragments.table_id()),
            TrackingJob::RecoveredV2(recovered) => Some((recovered.id as u32).into()),
        };
        let Some(id) = id else {
            return;
        };
        match self {
            TrackingJob::New(command) => match command.context.metadata_manager() {
                MetadataManager::V1(mgr) => {
                    mgr.notify_finish_failed(id.table_id, err).await;
                }
                MetadataManager::V2(mgr) => {
                    mgr.notify_finish_failed(id.table_id, err).await;
                }
            },
            TrackingJob::RecoveredV1(recovered) => {
                recovered
                    .metadata_manager
                    .notify_finish_failed(id.table_id, err)
                    .await;
            }
            TrackingJob::RecoveredV2(recovered) => {
                recovered
                    .metadata_manager
                    .notify_finish_failed(id.table_id, err)
                    .await;
            }
        }
    }
}

impl std::fmt::Debug for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackingJob::New(command) => write!(
                f,
                "TrackingJob::New({:?})",
                command.context.table_to_create()
            ),
            TrackingJob::RecoveredV1(recovered) => {
                write!(
                    f,
                    "TrackingJob::RecoveredV1({:?})",
                    recovered.fragments.table_id()
                )
            }
            TrackingJob::RecoveredV2(recovered) => {
                write!(f, "TrackingJob::RecoveredV2({:?})", recovered.id)
            }
        }
    }
}

pub struct RecoveredTrackingJobV1 {
    pub fragments: TableFragments,
    pub streaming_job: StreamingJob,
    pub internal_tables: Vec<Table>,
    pub metadata_manager: MetadataManagerV1,
}

pub struct RecoveredTrackingJobV2 {
    pub id: ObjectId,
    pub metadata_manager: MetadataManagerV2,
}

/// The command tracking by the [`CreateMviewProgressTracker`].
pub(super) struct TrackingCommand {
    /// The context of the command.
    pub context: Arc<CommandContext>,
}

/// Tracking is done as follows:
/// 1. We identify a `StreamJob` by its `TableId` of its `Materialized` table.
/// 2. For each stream job, there are several actors which run its tasks.
/// 3. With `progress_map` we can use the ID of the `StreamJob` to view its progress.
/// 4. With `actor_map` we can use an actor's `ActorId` to find the ID of the `StreamJob`.
pub(super) struct CreateMviewProgressTracker {
    /// Progress of the create-mview DDL indicated by the `TableId`.
    progress_map: HashMap<TableId, (Progress, TrackingJob)>,

    /// Find the epoch of the create-mview DDL by the actor containing the backfill executors.
    actor_map: HashMap<ActorId, TableId>,

    /// Get notified when we finished Create MV and collect a barrier(checkpoint = true)
    finished_jobs: Vec<TrackingJob>,
}

impl CreateMviewProgressTracker {
    /// This step recovers state from the meta side:
    /// 1. `Tables`.
    /// 2. `TableFragments`.
    ///
    /// Other state are persisted by the `BackfillExecutor`, such as:
    /// 1. `CreateMviewProgress`.
    /// 2. `Backfill` position.
    pub fn recover_v1(
        table_map: TableActorMap,
        mut upstream_mv_counts: TableUpstreamMvCountMap,
        mut definitions: TableDefinitionMap,
        version_stats: HummockVersionStats,
        mut table_fragment_map: TableFragmentMap,
        mut table_internal_table_map: TableInternalTableMap,
        mut table_stream_job_map: TableStreamJobMap,
        metadata_manager: MetadataManagerV1,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        let table_map: HashMap<_, HashSet<ActorId>> = table_map.into();
        for (creating_table_id, actors) in table_map {
            let mut states = HashMap::new();
            tracing::debug!(?actors, "recover progress for actors");
            for actor in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0));
            }

            let progress = Self::recover_progress(
                creating_table_id,
                &mut upstream_mv_counts,
                &mut definitions,
                &version_stats,
            );
            let internal_tables = table_internal_table_map.remove(&creating_table_id).unwrap();
            let streaming_job = table_stream_job_map.remove(&creating_table_id).unwrap();
            let tracking_job = TrackingJob::RecoveredV1(RecoveredTrackingJobV1 {
                fragments: table_fragment_map.remove(&creating_table_id).unwrap(),
                metadata_manager: metadata_manager.clone(),
                internal_tables,
                streaming_job,
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
            finished_jobs: Vec::new(),
        }
    }

    pub fn recover_v2(
        table_map: TableActorMap,
        mut upstream_mv_counts: TableUpstreamMvCountMap,
        mut definitions: TableDefinitionMap,
        version_stats: HummockVersionStats,
        metadata_manager: MetadataManagerV2,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        let table_map: HashMap<_, HashSet<ActorId>> = table_map.into();
        for (creating_table_id, actors) in table_map {
            let mut states = HashMap::new();
            for actor in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0));
            }

            let progress = Self::recover_progress(
                creating_table_id,
                &mut upstream_mv_counts,
                &mut definitions,
                &version_stats,
            );
            let tracking_job = TrackingJob::RecoveredV2(RecoveredTrackingJobV2 {
                id: creating_table_id.table_id as i32,
                metadata_manager: metadata_manager.clone(),
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
            finished_jobs: Vec::new(),
        }
    }

    pub fn recover_progress(
        creating_table_id: TableId,
        upstream_mv_counts: &mut TableUpstreamMvCountMap,
        definitions: &mut TableDefinitionMap,
        version_stats: &HummockVersionStats,
    ) -> Progress {
        let states = HashMap::new();
        let upstream_mv_count = upstream_mv_counts.remove(&creating_table_id).unwrap();
        let upstream_total_key_count = upstream_mv_count
            .iter()
            .map(|(upstream_mv, count)| {
                *count as u64
                    * version_stats
                        .table_stats
                        .get(&upstream_mv.table_id)
                        .map_or(0, |stat| stat.total_key_count as u64)
            })
            .sum();
        let definition = definitions.remove(&creating_table_id).unwrap();
        Progress {
            states,
            done_count: 0, // Fill only after first barrier pass
            upstream_mv_count,
            upstream_total_key_count,
            consumed_rows: 0, // Fill only after first barrier pass
            definition,
        }
    }

    pub fn new() -> Self {
        Self {
            progress_map: Default::default(),
            actor_map: Default::default(),
            finished_jobs: Vec::new(),
        }
    }

    pub fn gen_ddl_progress(&self) -> HashMap<u32, DdlProgress> {
        self.progress_map
            .iter()
            .map(|(table_id, (x, _))| {
                let table_id = table_id.table_id;
                let ddl_progress = DdlProgress {
                    id: table_id as u64,
                    statement: x.definition.clone(),
                    progress: format!("{:.2}%", x.calculate_progress() * 100.0),
                };
                (table_id, ddl_progress)
            })
            .collect()
    }

    /// Stash a command to finish later.
    pub(super) fn stash_command_to_finish(&mut self, finished_job: TrackingJob) {
        self.finished_jobs.push(finished_job);
    }

    /// Finish stashed jobs.
    /// If checkpoint, means all jobs can be finished.
    /// If not checkpoint, jobs which do not require checkpoint can be finished.
    ///
    /// Returns whether there are still remaining stashed jobs to finish.
    pub(super) async fn finish_jobs(&mut self, checkpoint: bool) -> MetaResult<bool> {
        tracing::trace!(finished_jobs=?self.finished_jobs, progress_map=?self.progress_map, "finishing jobs");
        for job in self
            .finished_jobs
            .extract_if(|job| checkpoint || !job.is_checkpoint_required())
        {
            // The command is ready to finish. We can now call `pre_finish`.
            job.pre_finish().await?;
        }
        Ok(!self.finished_jobs.is_empty())
    }

    pub(super) fn cancel_command(&mut self, id: TableId) {
        let _ = self.progress_map.remove(&id);
        self.finished_jobs
            .retain(|x| x.table_to_create() != Some(id));
        self.actor_map.retain(|_, table_id| *table_id != id);
    }

    /// Notify all tracked commands that error encountered and clear them.
    pub async fn abort_all(&mut self, err: &MetaError) {
        self.actor_map.clear();
        for job in self.finished_jobs.drain(..) {
            job.notify_finish_failed(err.clone()).await;
        }
        for (_, (_, job)) in self.progress_map.drain() {
            job.notify_finish_failed(err.clone()).await;
        }
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track is empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        command: TrackingCommand,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        let actors = command.context.actors_to_track();
        if actors.is_empty() {
            // The command can be finished immediately.
            return Some(TrackingJob::New(command));
        }

        let (
            creating_mv_id,
            upstream_mv_count,
            upstream_total_key_count,
            definition,
            ddl_type,
            create_type,
        ) = if let Command::CreateStreamingJob {
            table_fragments,
            dispatchers,
            upstream_root_actors,
            definition,
            ddl_type,
            create_type,
            ..
        } = &command.context.command
        {
            // Keep track of how many times each upstream MV appears.
            let mut upstream_mv_count = HashMap::new();
            for (table_id, actors) in upstream_root_actors {
                assert!(!actors.is_empty());
                let dispatch_count: usize = dispatchers
                    .iter()
                    .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                    .map(|(_, v)| v.len())
                    .sum();
                upstream_mv_count.insert(*table_id, dispatch_count / actors.len());
            }

            let upstream_total_key_count: u64 = upstream_mv_count
                .iter()
                .map(|(upstream_mv, count)| {
                    *count as u64
                        * version_stats
                            .table_stats
                            .get(&upstream_mv.table_id)
                            .map_or(0, |stat| stat.total_key_count as u64)
                })
                .sum();
            (
                table_fragments.table_id(),
                upstream_mv_count,
                upstream_total_key_count,
                definition.to_string(),
                ddl_type,
                create_type,
            )
        } else {
            unreachable!("Must be CreateStreamingJob.");
        };

        for &actor in &actors {
            self.actor_map.insert(actor, creating_mv_id);
        }

        let progress = Progress::new(
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            definition,
        );
        if *ddl_type == DdlType::Sink && *create_type == CreateType::Background {
            // We return the original tracking job immediately.
            // This is because sink can be decoupled with backfill progress.
            // We don't need to wait for sink to finish backfill.
            // This still contains the notifiers, so we can tell listeners
            // that the sink job has been created.
            Some(TrackingJob::New(command))
        } else {
            let old = self
                .progress_map
                .insert(creating_mv_id, (progress, TrackingJob::New(command)));
            assert!(old.is_none());
            None
        }
    }

    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, returns the command.
    pub fn update(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        let actor = progress.backfill_actor_id;
        let Some(table_id) = self.actor_map.get(&actor).copied() else {
            // On restart, backfill will ALWAYS notify CreateMviewProgressTracker,
            // even if backfill is finished on recovery.
            // This is because we don't know if only this actor is finished,
            // OR the entire stream job is finished.
            // For the first case, we must notify meta.
            // For the second case, we can still notify meta, but ignore it here.
            tracing::info!(
                "no tracked progress for actor {}, the stream job could already be finished",
                actor
            );
            return None;
        };

        let new_state = if progress.done {
            BackfillState::Done(progress.consumed_rows)
        } else {
            BackfillState::ConsumingUpstream(progress.consumed_epoch.into(), progress.consumed_rows)
        };

        match self.progress_map.entry(table_id) {
            Entry::Occupied(mut o) => {
                let progress = &mut o.get_mut().0;

                let upstream_total_key_count: u64 = progress
                    .upstream_mv_count
                    .iter()
                    .map(|(upstream_mv, count)| {
                        assert_ne!(*count, 0);
                        *count as u64
                            * version_stats
                                .table_stats
                                .get(&upstream_mv.table_id)
                                .map_or(0, |stat| stat.total_key_count as u64)
                    })
                    .sum();

                progress.update(actor, new_state, upstream_total_key_count);

                if progress.is_done() {
                    tracing::debug!(
                        "all actors done for creating mview with table_id {}!",
                        table_id
                    );

                    // Clean-up the mapping from actors to DDL table_id.
                    for actor in o.get().0.actors() {
                        self.actor_map.remove(&actor);
                    }
                    Some(o.remove().1)
                } else {
                    None
                }
            }
            Entry::Vacant(_) => {
                tracing::warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                None
            }
        }
    }
}
