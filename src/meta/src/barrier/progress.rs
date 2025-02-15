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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::ObjectId;
use risingwave_pb::catalog::CreateType;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::PbBarrierCompleteResponse;

use crate::barrier::info::BarrierInfo;
use crate::barrier::{
    Command, CreateStreamingJobCommandInfo, CreateStreamingJobType, ReplaceStreamJobPlan,
};
use crate::manager::{MetadataManager, StreamingJobType};
use crate::model::{ActorId, BackfillUpstreamType, StreamJobFragments};
use crate::MetaResult;

type ConsumedRows = u64;

#[derive(Clone, Copy, Debug)]
enum BackfillState {
    Init,
    ConsumingUpstream(#[expect(dead_code)] Epoch, ConsumedRows),
    Done(ConsumedRows),
}

/// Progress of all actors containing backfill executors while creating mview.
#[derive(Debug)]
pub(super) struct Progress {
    // `states` and `done_count` decides whether the progress is done. See `is_done`.
    states: HashMap<ActorId, BackfillState>,
    done_count: usize,

    /// Tells whether the backfill is from source or mv.
    backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,

    // The following row counts are used to calculate the progress. See `calculate_progress`.
    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,
    /// Total key count of all the upstream materialized views
    upstream_mvs_total_key_count: u64,
    mv_backfill_consumed_rows: u64,
    source_backfill_consumed_rows: u64,

    /// DDL definition
    definition: String,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the backfill executors.
    fn new(
        actors: impl IntoIterator<Item = (ActorId, BackfillUpstreamType)>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
    ) -> Self {
        let mut states = HashMap::new();
        let mut backfill_upstream_types = HashMap::new();
        for (actor, backfill_upstream_type) in actors {
            states.insert(actor, BackfillState::Init);
            backfill_upstream_types.insert(actor, backfill_upstream_type);
        }
        assert!(!states.is_empty());

        Self {
            states,
            backfill_upstream_types,
            done_count: 0,
            upstream_mv_count,
            upstream_mvs_total_key_count: upstream_total_key_count,
            mv_backfill_consumed_rows: 0,
            source_backfill_consumed_rows: 0,
            definition,
        }
    }

    /// Update the progress of `actor`.
    fn update(&mut self, actor: ActorId, new_state: BackfillState, upstream_total_key_count: u64) {
        self.upstream_mvs_total_key_count = upstream_total_key_count;
        let total_actors = self.states.len();
        let backfill_upstream_type = self.backfill_upstream_types.get(&actor).unwrap();
        tracing::debug!(?actor, states = ?self.states, "update progress for actor");

        let mut old = 0;
        let mut new = 0;
        match self.states.remove(&actor).unwrap() {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, old_consumed_rows) => {
                old = old_consumed_rows;
            }
            BackfillState::Done(_) => panic!("should not report done multiple times"),
        };
        match &new_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, new_consumed_rows) => {
                new = *new_consumed_rows;
            }
            BackfillState::Done(new_consumed_rows) => {
                tracing::debug!("actor {} done", actor);
                new = *new_consumed_rows;
                self.done_count += 1;
                tracing::debug!(
                    "{} actors out of {} complete",
                    self.done_count,
                    total_actors,
                );
            }
        };
        debug_assert!(new >= old, "backfill progress should not go backward");
        match backfill_upstream_type {
            BackfillUpstreamType::MView => {
                self.mv_backfill_consumed_rows += new - old;
            }
            BackfillUpstreamType::Source => {
                self.source_backfill_consumed_rows += new - old;
            }
            BackfillUpstreamType::Values => {
                // do not consider progress for values
            }
        }
        self.states.insert(actor, new_state);
    }

    /// Returns whether all backfill executors are done.
    fn is_done(&self) -> bool {
        tracing::trace!(
            "Progress::is_done? {}, {}, {:?}",
            self.done_count,
            self.states.len(),
            self.states
        );
        self.done_count == self.states.len()
    }

    /// Returns the ids of all actors containing the backfill executors for the mview tracked by this
    /// [`Progress`].
    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.states.keys().cloned()
    }

    /// `progress` = `consumed_rows` / `upstream_total_key_count`
    fn calculate_progress(&self) -> String {
        if self.is_done() || self.states.is_empty() {
            return "100%".to_owned();
        }
        let mut mv_count = 0;
        let mut source_count = 0;
        for backfill_upstream_type in self.backfill_upstream_types.values() {
            match backfill_upstream_type {
                BackfillUpstreamType::MView => mv_count += 1,
                BackfillUpstreamType::Source => source_count += 1,
                BackfillUpstreamType::Values => (),
            }
        }

        let mv_progress = (mv_count > 0).then_some({
            if self.upstream_mvs_total_key_count == 0 {
                "99.99%".to_owned()
            } else {
                let mut progress = self.mv_backfill_consumed_rows as f64
                    / (self.upstream_mvs_total_key_count as f64);
                if progress > 1.0 {
                    progress = 0.9999;
                }
                format!(
                    "{:.2}% ({}/{})",
                    progress * 100.0,
                    self.mv_backfill_consumed_rows,
                    self.upstream_mvs_total_key_count
                )
            }
        });
        let source_progress = (source_count > 0).then_some(format!(
            "{} rows consumed",
            self.source_backfill_consumed_rows
        ));
        match (mv_progress, source_progress) {
            (Some(mv_progress), Some(source_progress)) => {
                format!(
                    "MView Backfill: {}, Source Backfill: {}",
                    mv_progress, source_progress
                )
            }
            (Some(mv_progress), None) => mv_progress,
            (None, Some(source_progress)) => source_progress,
            (None, None) => "Unknown".to_owned(),
        }
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
    Recovered(RecoveredTrackingJob),
}

impl TrackingJob {
    /// Notify metadata manager that the job is finished.
    pub(crate) async fn finish(self, metadata_manager: &MetadataManager) -> MetaResult<()> {
        match self {
            TrackingJob::New(command) => {
                let CreateStreamingJobCommandInfo { streaming_job, .. } = &command.info;
                metadata_manager
                    .catalog_controller
                    .finish_streaming_job(
                        streaming_job.id() as i32,
                        command.replace_stream_job.clone(),
                    )
                    .await?;
                Ok(())
            }
            TrackingJob::Recovered(recovered) => {
                metadata_manager
                    .catalog_controller
                    .finish_streaming_job(recovered.id, None)
                    .await?;
                Ok(())
            }
        }
    }

    pub(crate) fn table_to_create(&self) -> TableId {
        match self {
            TrackingJob::New(command) => command.info.stream_job_fragments.stream_job_id(),
            TrackingJob::Recovered(recovered) => (recovered.id as u32).into(),
        }
    }
}

impl std::fmt::Debug for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackingJob::New(command) => write!(
                f,
                "TrackingJob::New({:?})",
                command.info.stream_job_fragments.stream_job_id()
            ),
            TrackingJob::Recovered(recovered) => {
                write!(f, "TrackingJob::RecoveredV2({:?})", recovered.id)
            }
        }
    }
}

pub struct RecoveredTrackingJob {
    pub id: ObjectId,
}

/// The command tracking by the [`CreateMviewProgressTracker`].
pub(super) struct TrackingCommand {
    pub info: CreateStreamingJobCommandInfo,
    pub replace_stream_job: Option<ReplaceStreamJobPlan>,
}

/// Tracking is done as follows:
/// 1. We identify a `StreamJob` by its `TableId` of its `Materialized` table.
/// 2. For each stream job, there are several actors which run its tasks.
/// 3. With `progress_map` we can use the ID of the `StreamJob` to view its progress.
/// 4. With `actor_map` we can use an actor's `ActorId` to find the ID of the `StreamJob`.
#[derive(Default, Debug)]
pub(super) struct CreateMviewProgressTracker {
    /// Progress of the create-mview DDL indicated by the `TableId`.
    progress_map: HashMap<TableId, (Progress, TrackingJob)>,

    actor_map: HashMap<ActorId, TableId>,

    /// Stash of finished jobs. They will be finally finished on checkpoint.
    pending_finished_jobs: Vec<TrackingJob>,
}

impl CreateMviewProgressTracker {
    /// This step recovers state from the meta side:
    /// 1. `Tables`.
    /// 2. `TableFragments`.
    ///
    /// Other state are persisted by the `BackfillExecutor`, such as:
    /// 1. `CreateMviewProgress`.
    /// 2. `Backfill` position.
    pub fn recover(
        mview_map: HashMap<TableId, (String, StreamJobFragments)>,
        version_stats: &HummockVersionStats,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        for (creating_table_id, (definition, table_fragments)) in mview_map {
            let mut states = HashMap::new();
            let mut backfill_upstream_types = HashMap::new();
            let actors = table_fragments.tracking_progress_actor_ids();
            for (actor, backfill_upstream_type) in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0));
                backfill_upstream_types.insert(actor, backfill_upstream_type);
            }

            let progress = Self::recover_progress(
                states,
                backfill_upstream_types,
                table_fragments.upstream_table_counts(),
                definition,
                version_stats,
            );
            let tracking_job = TrackingJob::Recovered(RecoveredTrackingJob {
                id: creating_table_id.table_id as i32,
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
            pending_finished_jobs: Vec::new(),
        }
    }

    /// ## How recovery works
    ///
    /// The progress (number of rows consumed) is persisted in state tables.
    /// During recovery, the backfill executor will restore the number of rows consumed,
    /// and then it will just report progress like newly created executors.
    fn recover_progress(
        states: HashMap<ActorId, BackfillState>,
        backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,
        upstream_mv_count: HashMap<TableId, usize>,
        definition: String,
        version_stats: &HummockVersionStats,
    ) -> Progress {
        let upstream_mvs_total_key_count =
            calculate_total_key_count(&upstream_mv_count, version_stats);
        Progress {
            states,
            backfill_upstream_types,
            done_count: 0, // Fill only after first barrier pass
            upstream_mv_count,
            upstream_mvs_total_key_count,
            mv_backfill_consumed_rows: 0, // Fill only after first barrier pass
            source_backfill_consumed_rows: 0, // Fill only after first barrier pass
            definition,
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
                    progress: x.calculate_progress(),
                };
                (table_id, ddl_progress)
            })
            .collect()
    }

    pub(super) fn update_tracking_jobs<'a>(
        &mut self,
        info: Option<(
            &CreateStreamingJobCommandInfo,
            Option<&ReplaceStreamJobPlan>,
        )>,
        create_mview_progress: impl IntoIterator<Item = &'a CreateMviewProgress>,
        version_stats: &HummockVersionStats,
    ) {
        {
            {
                // Save `finished_commands` for Create MVs.
                let finished_commands = {
                    let mut commands = vec![];
                    // Add the command to tracker.
                    if let Some((create_job_info, replace_stream_job)) = info
                        && let Some(command) =
                            self.add(create_job_info, replace_stream_job, version_stats)
                    {
                        // Those with no actors to track can be finished immediately.
                        commands.push(command);
                    }
                    // Update the progress of all commands.
                    for progress in create_mview_progress {
                        // Those with actors complete can be finished immediately.
                        if let Some(command) = self.update(progress, version_stats) {
                            tracing::trace!(?progress, "finish progress");
                            commands.push(command);
                        } else {
                            tracing::trace!(?progress, "update progress");
                        }
                    }
                    commands
                };

                for command in finished_commands {
                    self.stash_command_to_finish(command);
                }
            }
        }
    }

    /// Apply a collected epoch node command to the tracker
    /// Return the finished jobs when the barrier kind is `Checkpoint`
    pub(super) fn apply_collected_command(
        &mut self,
        command: Option<&Command>,
        barrier_info: &BarrierInfo,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
        version_stats: &HummockVersionStats,
    ) -> Vec<TrackingJob> {
        let new_tracking_job_info =
            if let Some(Command::CreateStreamingJob { info, job_type, .. }) = command {
                match job_type {
                    CreateStreamingJobType::Normal => Some((info, None)),
                    CreateStreamingJobType::SinkIntoTable(replace_stream_job) => {
                        Some((info, Some(replace_stream_job)))
                    }
                    CreateStreamingJobType::SnapshotBackfill(_) => {
                        // The progress of SnapshotBackfill won't be tracked here
                        None
                    }
                }
            } else {
                None
            };
        self.update_tracking_jobs(
            new_tracking_job_info,
            resps
                .into_iter()
                .flat_map(|resp| resp.create_mview_progress.iter()),
            version_stats,
        );
        for table_id in command.map(Command::tables_to_drop).into_iter().flatten() {
            // the cancelled command is possibly stashed in `finished_commands` and waiting
            // for checkpoint, we should also clear it.
            self.cancel_command(table_id);
        }
        if barrier_info.kind.is_checkpoint() {
            self.take_finished_jobs()
        } else {
            vec![]
        }
    }

    /// Stash a command to finish later.
    pub(super) fn stash_command_to_finish(&mut self, finished_job: TrackingJob) {
        self.pending_finished_jobs.push(finished_job);
    }

    /// Finish stashed jobs on checkpoint.
    pub(super) fn take_finished_jobs(&mut self) -> Vec<TrackingJob> {
        tracing::trace!(finished_jobs=?self.pending_finished_jobs, progress_map=?self.progress_map, "take_finished_jobs");
        take(&mut self.pending_finished_jobs)
    }

    pub(super) fn has_pending_finished_jobs(&self) -> bool {
        !self.pending_finished_jobs.is_empty()
    }

    pub(super) fn cancel_command(&mut self, id: TableId) {
        let _ = self.progress_map.remove(&id);
        self.pending_finished_jobs
            .retain(|x| x.table_to_create() != id);
        self.actor_map.retain(|_, table_id| *table_id != id);
    }

    /// Notify all tracked commands that error encountered and clear them.
    pub fn abort_all(&mut self) {
        self.actor_map.clear();
        self.pending_finished_jobs.clear();
        self.progress_map.clear();
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track is empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        info: &CreateStreamingJobCommandInfo,
        replace_stream_job: Option<&ReplaceStreamJobPlan>,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        tracing::trace!(?info, "add job to track");
        let (info, actors, replace_table_info) = {
            let CreateStreamingJobCommandInfo {
                stream_job_fragments,
                ..
            } = info;
            let actors = stream_job_fragments.tracking_progress_actor_ids();
            if actors.is_empty() {
                // The command can be finished immediately.
                return Some(TrackingJob::New(TrackingCommand {
                    info: info.clone(),
                    replace_stream_job: replace_stream_job.cloned(),
                }));
            }
            (info.clone(), actors, replace_stream_job.cloned())
        };

        let CreateStreamingJobCommandInfo {
            stream_job_fragments: table_fragments,
            definition,
            job_type,
            create_type,
            ..
        } = &info;

        let creating_mv_id = table_fragments.stream_job_id();
        let upstream_mv_count = table_fragments.upstream_table_counts();
        let upstream_total_key_count: u64 =
            calculate_total_key_count(&upstream_mv_count, version_stats);

        for (actor, _backfill_upstream_type) in &actors {
            self.actor_map.insert(*actor, creating_mv_id);
        }

        let progress = Progress::new(
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            definition.clone(),
        );
        if *job_type == StreamingJobType::Sink && *create_type == CreateType::Background {
            // We return the original tracking job immediately.
            // This is because sink can be decoupled with backfill progress.
            // We don't need to wait for sink to finish backfill.
            // This still contains the notifiers, so we can tell listeners
            // that the sink job has been created.
            Some(TrackingJob::New(TrackingCommand {
                info,
                replace_stream_job: replace_table_info,
            }))
        } else {
            let old = self.progress_map.insert(
                creating_mv_id,
                (
                    progress,
                    TrackingJob::New(TrackingCommand {
                        info,
                        replace_stream_job: replace_table_info,
                    }),
                ),
            );
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
        tracing::trace!(?progress, "update progress");
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

                let upstream_total_key_count: u64 =
                    calculate_total_key_count(&progress.upstream_mv_count, version_stats);

                tracing::debug!(?table_id, "updating progress for table");
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

fn calculate_total_key_count(
    table_count: &HashMap<TableId, usize>,
    version_stats: &HummockVersionStats,
) -> u64 {
    table_count
        .iter()
        .map(|(table_id, count)| {
            assert_ne!(*count, 0);
            *count as u64
                * version_stats
                    .table_stats
                    .get(&table_id.table_id)
                    .map_or(0, |stat| stat.total_key_count as u64)
        })
        .sum()
}
