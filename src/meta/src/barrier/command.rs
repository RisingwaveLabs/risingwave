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

use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::ActorMapping;
use risingwave_common::types::Timestamptz;
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::WorkerId;
use risingwave_pb::catalog::{CreateType, Table};
use risingwave_pb::common::PbWorkerNode;
use risingwave_pb::meta::table_fragments::PbActorStatus;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::barrier::BarrierKind as PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::throttle_mutation::RateLimit;
use risingwave_pb::stream_plan::update_mutation::*;
use risingwave_pb::stream_plan::{
    AddMutation, BarrierMutation, CombinedMutation, Dispatcher, Dispatchers,
    DropSubscriptionsMutation, PauseMutation, ResumeMutation, SourceChangeSplitMutation,
    StopMutation, StreamActor, SubscriptionUpstreamInfo, ThrottleMutation, UpdateMutation,
};
use tracing::warn;

use super::info::{CommandFragmentChanges, InflightStreamingJobInfo};
use crate::barrier::info::BarrierInfo;
use crate::barrier::InflightSubscriptionInfo;
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::{DdlType, StreamingJob};
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments, TableParallelism};
use crate::stream::{build_actor_connector_splits, SplitAssignment, ThrottleConfig};

/// [`Reschedule`] is for the [`Command::RescheduleFragment`], which is used for rescheduling actors
/// in some fragment, like scaling or migrating.
#[derive(Debug, Clone)]
pub struct Reschedule {
    /// Added actors in this fragment.
    pub added_actors: HashMap<WorkerId, Vec<ActorId>>,

    /// Removed actors in this fragment.
    pub removed_actors: Vec<ActorId>,

    /// Vnode bitmap updates for some actors in this fragment.
    pub vnode_bitmap_updates: HashMap<ActorId, Bitmap>,

    /// The upstream fragments of this fragment, and the dispatchers that should be updated.
    pub upstream_fragment_dispatcher_ids: Vec<(FragmentId, DispatcherId)>,
    /// New hash mapping of the upstream dispatcher to be updated.
    ///
    /// This field exists only when there's upstream fragment and the current fragment is
    /// hash-sharded.
    pub upstream_dispatcher_mapping: Option<ActorMapping>,

    /// The downstream fragments of this fragment.
    pub downstream_fragment_ids: Vec<FragmentId>,

    /// Reassigned splits for source actors.
    /// It becomes the `actor_splits` in [`UpdateMutation`].
    /// `Source` and `SourceBackfill` are handled together here.
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,

    pub newly_created_actors: Vec<(StreamActor, PbActorStatus)>,
}

/// Replacing an old table with a new one. All actors in the table job will be rebuilt.
/// Used for `ALTER TABLE` ([`Command::ReplaceTable`]) and sink into table ([`Command::CreateStreamingJob`]).
#[derive(Debug, Clone)]
pub struct ReplaceTablePlan {
    pub old_table_fragments: TableFragments,
    pub new_table_fragments: TableFragments,
    pub merge_updates: Vec<MergeUpdate>,
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
    /// For a table with connector, the `SourceExecutor` actor will also be rebuilt with new actor ids.
    /// We need to reassign splits for it.
    ///
    /// Note that there's no `SourceBackfillExecutor` involved for table with connector, so we don't need to worry about
    /// `backfill_splits`.
    pub init_split_assignment: SplitAssignment,
    /// The `StreamingJob` info of the table to be replaced. Must be `StreamingJob::Table`
    pub streaming_job: StreamingJob,
    /// The temporary dummy table fragments id of new table fragment
    pub dummy_id: u32,
}

impl ReplaceTablePlan {
    fn fragment_changes(&self) -> HashMap<FragmentId, CommandFragmentChanges> {
        let mut fragment_changes = HashMap::new();
        for fragment in self.new_table_fragments.fragments.values() {
            let fragment_change = CommandFragmentChanges::NewFragment(
                self.streaming_job.id().into(),
                InflightFragmentInfo {
                    actors: fragment
                        .actors
                        .iter()
                        .map(|actor| {
                            (
                                actor.actor_id,
                                self.new_table_fragments
                                    .actor_status
                                    .get(&actor.actor_id)
                                    .expect("should exist")
                                    .worker_id() as WorkerId,
                            )
                        })
                        .collect(),
                    state_table_ids: fragment
                        .state_table_ids
                        .iter()
                        .map(|table_id| TableId::new(*table_id))
                        .collect(),
                },
            );
            assert!(fragment_changes
                .insert(fragment.fragment_id, fragment_change)
                .is_none());
        }
        for fragment in self.old_table_fragments.fragments.values() {
            assert!(fragment_changes
                .insert(fragment.fragment_id, CommandFragmentChanges::RemoveFragment)
                .is_none());
        }
        fragment_changes
    }
}

#[derive(educe::Educe, Clone)]
#[educe(Debug)]
pub struct CreateStreamingJobCommandInfo {
    #[educe(Debug(ignore))]
    pub table_fragments: TableFragments,
    /// Refer to the doc on [`crate::manager::MetadataManager::get_upstream_root_fragments`] for the meaning of "root".
    pub upstream_root_actors: HashMap<TableId, Vec<ActorId>>,
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
    pub init_split_assignment: SplitAssignment,
    pub definition: String,
    pub ddl_type: DdlType,
    pub create_type: CreateType,
    pub streaming_job: StreamingJob,
    pub internal_tables: Vec<Table>,
}

impl CreateStreamingJobCommandInfo {
    pub(super) fn new_fragment_info(
        &self,
    ) -> impl Iterator<Item = (FragmentId, InflightFragmentInfo)> + '_ {
        self.table_fragments.fragments.values().map(|fragment| {
            (
                fragment.fragment_id,
                InflightFragmentInfo {
                    actors: fragment
                        .actors
                        .iter()
                        .map(|actor| {
                            (
                                actor.actor_id,
                                self.table_fragments
                                    .actor_status
                                    .get(&actor.actor_id)
                                    .expect("should exist")
                                    .worker_id() as WorkerId,
                            )
                        })
                        .collect(),
                    state_table_ids: fragment
                        .state_table_ids
                        .iter()
                        .map(|table_id| TableId::new(*table_id))
                        .collect(),
                },
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotBackfillInfo {
    pub upstream_mv_table_ids: HashSet<TableId>,
}

#[derive(Debug, Clone)]
pub enum CreateStreamingJobType {
    Normal,
    SinkIntoTable(ReplaceTablePlan),
    SnapshotBackfill(SnapshotBackfillInfo),
}

/// [`Command`] is the input of [`crate::barrier::worker::GlobalBarrierWorker`]. For different commands,
/// it will build different barriers to send, and may do different stuffs after the barrier is
/// collected.
#[derive(Debug, strum::Display)]
pub enum Command {
    /// `Flush` command will generate a checkpoint barrier. After the barrier is collected and committed
    /// all messages before the checkpoint barrier should have been committed.
    Flush,

    /// `Pause` command generates a `Pause` barrier with the provided [`PausedReason`] **only if**
    /// the cluster is not already paused. Otherwise, a barrier with no mutation will be generated.
    Pause(PausedReason),

    /// `Resume` command generates a `Resume` barrier with the provided [`PausedReason`] **only
    /// if** the cluster is paused with the same reason. Otherwise, a barrier with no mutation
    /// will be generated.
    Resume(PausedReason),

    /// `DropStreamingJobs` command generates a `Stop` barrier to stop the given
    /// [`Vec<ActorId>`]. The catalog has ensured that these streaming jobs are safe to be
    /// dropped by reference counts before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the table fragments info from meta store.
    DropStreamingJobs {
        actors: Vec<ActorId>,
        unregistered_state_table_ids: HashSet<TableId>,
        unregistered_fragment_ids: HashSet<FragmentId>,
    },

    /// `CreateStreamingJob` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Inactive` at first, will STILL
    /// be collected since the barrier should be passthrough.
    ///
    /// After the barrier is collected, these newly created actors will be marked as `Running`. And
    /// it adds the table fragments info to meta store. However, the creating progress will **last
    /// for a while** until the `finish` channel is signaled, then the state of `TableFragments`
    /// will be set to `Created`.
    CreateStreamingJob {
        info: CreateStreamingJobCommandInfo,
        job_type: CreateStreamingJobType,
    },
    MergeSnapshotBackfillStreamingJobs(
        HashMap<TableId, (SnapshotBackfillInfo, InflightStreamingJobInfo)>,
    ),
    /// `CancelStreamingJob` command generates a `Stop` barrier including the actors of the given
    /// table fragment.
    ///
    /// The collecting and cleaning part works exactly the same as `DropStreamingJobs` command.
    CancelStreamingJob(TableFragments),

    /// `Reschedule` command generates a `Update` barrier by the [`Reschedule`] of each fragment.
    /// Mainly used for scaling and migration.
    ///
    /// Barriers from which actors should be collected, and the post behavior of this command are
    /// very similar to `Create` and `Drop` commands, for added and removed actors, respectively.
    RescheduleFragment {
        reschedules: HashMap<FragmentId, Reschedule>,
        table_parallelism: HashMap<TableId, TableParallelism>,
        // should contain the actor ids in upstream and downstream fragment of `reschedules`
        fragment_actors: HashMap<FragmentId, HashSet<ActorId>>,
    },

    /// `ReplaceTable` command generates a `Update` barrier with the given `merge_updates`. This is
    /// essentially switching the downstream of the old table fragments to the new ones, and
    /// dropping the old table fragments. Used for table schema change.
    ///
    /// This can be treated as a special case of `RescheduleFragment`, while the upstream fragment
    /// of the Merge executors are changed additionally.
    ReplaceTable(ReplaceTablePlan),

    /// `SourceSplitAssignment` generates a `Splits` barrier for pushing initialized splits or
    /// changed splits.
    SourceSplitAssignment(SplitAssignment),

    /// `Throttle` command generates a `Throttle` barrier with the given throttle config to change
    /// the `rate_limit` of `FlowControl` Executor after `StreamScan` or Source.
    Throttle(ThrottleConfig),

    /// `CreateSubscription` command generates a `CreateSubscriptionMutation` to notify
    /// materialize executor to start storing old value for subscription.
    CreateSubscription {
        subscription_id: u32,
        upstream_mv_table_id: TableId,
        retention_second: u64,
    },

    /// `DropSubscription` command generates a `DropSubscriptionsMutation` to notify
    /// materialize executor to stop storing old value when there is no
    /// subscription depending on it.
    DropSubscription {
        subscription_id: u32,
        upstream_mv_table_id: TableId,
    },
}

impl Command {
    pub fn pause(reason: PausedReason) -> Self {
        Self::Pause(reason)
    }

    pub fn resume(reason: PausedReason) -> Self {
        Self::Resume(reason)
    }

    pub(crate) fn fragment_changes(&self) -> Option<HashMap<FragmentId, CommandFragmentChanges>> {
        match self {
            Command::Flush => None,
            Command::Pause(_) => None,
            Command::Resume(_) => None,
            Command::DropStreamingJobs {
                unregistered_fragment_ids,
                ..
            } => Some(
                unregistered_fragment_ids
                    .iter()
                    .map(|fragment_id| (*fragment_id, CommandFragmentChanges::RemoveFragment))
                    .collect(),
            ),
            Command::CreateStreamingJob { info, job_type } => {
                assert!(
                    !matches!(job_type, CreateStreamingJobType::SnapshotBackfill(_)),
                    "should handle fragment changes separately for snapshot backfill"
                );
                let mut changes: HashMap<_, _> = info
                    .new_fragment_info()
                    .map(|(fragment_id, fragment_info)| {
                        (
                            fragment_id,
                            CommandFragmentChanges::NewFragment(
                                info.streaming_job.id().into(),
                                fragment_info,
                            ),
                        )
                    })
                    .collect();

                if let CreateStreamingJobType::SinkIntoTable(plan) = job_type {
                    let extra_change = plan.fragment_changes();
                    changes.extend(extra_change);
                }

                Some(changes)
            }
            Command::CancelStreamingJob(table_fragments) => Some(
                table_fragments
                    .fragments
                    .values()
                    .map(|fragment| (fragment.fragment_id, CommandFragmentChanges::RemoveFragment))
                    .collect(),
            ),
            Command::RescheduleFragment { reschedules, .. } => Some(
                reschedules
                    .iter()
                    .map(|(fragment_id, reschedule)| {
                        (
                            *fragment_id,
                            CommandFragmentChanges::Reschedule {
                                new_actors: reschedule
                                    .added_actors
                                    .iter()
                                    .flat_map(|(node_id, actors)| {
                                        actors.iter().map(|actor_id| (*actor_id, *node_id))
                                    })
                                    .collect(),
                                to_remove: reschedule.removed_actors.iter().cloned().collect(),
                            },
                        )
                    })
                    .collect(),
            ),
            Command::ReplaceTable(plan) => Some(plan.fragment_changes()),
            Command::MergeSnapshotBackfillStreamingJobs(_) => None,
            Command::SourceSplitAssignment(_) => None,
            Command::Throttle(_) => None,
            Command::CreateSubscription { .. } => None,
            Command::DropSubscription { .. } => None,
        }
    }

    /// If we need to send a barrier to modify actor configuration, we will pause the barrier
    /// injection. return true.
    pub fn should_pause_inject_barrier(&self) -> bool {
        // Note: the meaning for `Pause` is not pausing the periodic barrier injection, but for
        // pausing the sources on compute nodes. However, when `Pause` is used for configuration
        // change like scaling and migration, it must pause the concurrent checkpoint to ensure the
        // previous checkpoint has been done.
        matches!(self, Self::Pause(PausedReason::ConfigChange))
    }

    pub fn need_checkpoint(&self) -> bool {
        // todo! Reviewing the flow of different command to reduce the amount of checkpoint
        !matches!(self, Command::Resume(_))
    }
}

#[derive(Debug, Clone)]
pub enum BarrierKind {
    Initial,
    Barrier,
    /// Hold a list of previous non-checkpoint prev-epoch + current prev-epoch
    Checkpoint(Vec<u64>),
}

impl BarrierKind {
    pub fn to_protobuf(&self) -> PbBarrierKind {
        match self {
            BarrierKind::Initial => PbBarrierKind::Initial,
            BarrierKind::Barrier => PbBarrierKind::Barrier,
            BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
        }
    }

    pub fn is_checkpoint(&self) -> bool {
        matches!(self, BarrierKind::Checkpoint(_))
    }

    pub fn as_str_name(&self) -> &'static str {
        match self {
            BarrierKind::Initial => "Initial",
            BarrierKind::Barrier => "Barrier",
            BarrierKind::Checkpoint(_) => "Checkpoint",
        }
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext {
    /// Resolved info in this barrier loop.
    pub node_map: HashMap<WorkerId, PbWorkerNode>,
    pub subscription_info: InflightSubscriptionInfo,

    pub barrier_info: BarrierInfo,

    pub table_ids_to_commit: HashSet<TableId>,

    pub command: Option<Command>,

    /// The tracing span of this command.
    ///
    /// Differs from [`crate::barrier::TracedEpoch`], this span focuses on the lifetime of the corresponding
    /// barrier, including the process of waiting for the barrier to be sent, flowing through the
    /// stream graph on compute nodes, and finishing its `post_collect` stuffs.
    pub _span: tracing::Span,
}

impl std::fmt::Debug for CommandContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommandContext")
            .field("barrier_info", &self.barrier_info)
            .field("command", &self.command)
            .finish()
    }
}

impl CommandContext {
    pub(super) fn new(
        node_map: HashMap<WorkerId, PbWorkerNode>,
        barrier_info: BarrierInfo,
        subscription_info: InflightSubscriptionInfo,
        table_ids_to_commit: HashSet<TableId>,
        command: Option<Command>,
        span: tracing::Span,
    ) -> Self {
        Self {
            node_map,
            subscription_info,
            barrier_info,
            table_ids_to_commit,
            command,
            _span: span,
        }
    }

    pub fn get_truncate_epoch(&self, retention_second: u64) -> Epoch {
        let Some(truncate_timestamptz) = Timestamptz::from_secs(
            self.barrier_info
                .prev_epoch
                .value()
                .as_timestamptz()
                .timestamp()
                - retention_second as i64,
        ) else {
            warn!(retention_second, prev_epoch = ?self.barrier_info.prev_epoch.value(), "invalid retention second value");
            return self.barrier_info.prev_epoch.value();
        };
        Epoch::from_unix_millis(truncate_timestamptz.timestamp_millis() as u64)
    }
}

impl Command {
    /// Generate a mutation for the given command.
    pub fn to_mutation(&self, current_paused_reason: Option<PausedReason>) -> Option<Mutation> {
        let mutation =
            match self {
                Command::Flush => None,

                Command::Pause(_) => {
                    // Only pause when the cluster is not already paused.
                    if current_paused_reason.is_none() {
                        Some(Mutation::Pause(PauseMutation {}))
                    } else {
                        None
                    }
                }

                Command::Resume(reason) => {
                    // Only resume when the cluster is paused with the same reason.
                    if current_paused_reason == Some(*reason) {
                        Some(Mutation::Resume(ResumeMutation {}))
                    } else {
                        None
                    }
                }

                Command::SourceSplitAssignment(change) => {
                    let mut diff = HashMap::new();

                    for actor_splits in change.values() {
                        diff.extend(actor_splits.clone());
                    }

                    Some(Mutation::Splits(SourceChangeSplitMutation {
                        actor_splits: build_actor_connector_splits(&diff),
                    }))
                }

                Command::Throttle(config) => {
                    let mut actor_to_apply = HashMap::new();
                    for per_fragment in config.values() {
                        actor_to_apply.extend(per_fragment.iter().map(|(actor_id, limit)| {
                            (*actor_id, RateLimit { rate_limit: *limit })
                        }));
                    }

                    Some(Mutation::Throttle(ThrottleMutation {
                        actor_throttle: actor_to_apply,
                    }))
                }

                Command::DropStreamingJobs { actors, .. } => Some(Mutation::Stop(StopMutation {
                    actors: actors.clone(),
                })),

                Command::CreateStreamingJob {
                    info:
                        CreateStreamingJobCommandInfo {
                            table_fragments,
                            dispatchers,
                            init_split_assignment: split_assignment,
                            ..
                        },
                    job_type,
                } => {
                    let actor_dispatchers = dispatchers
                        .iter()
                        .map(|(&actor_id, dispatchers)| {
                            (
                                actor_id,
                                Dispatchers {
                                    dispatchers: dispatchers.clone(),
                                },
                            )
                        })
                        .collect();
                    let added_actors = table_fragments.actor_ids();
                    let actor_splits = split_assignment
                        .values()
                        .flat_map(build_actor_connector_splits)
                        .collect();
                    let subscriptions_to_add =
                        if let CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info) =
                            job_type
                        {
                            snapshot_backfill_info
                                .upstream_mv_table_ids
                                .iter()
                                .map(|table_id| SubscriptionUpstreamInfo {
                                    subscriber_id: table_fragments.table_id().table_id,
                                    upstream_mv_table_id: table_id.table_id,
                                })
                                .collect()
                        } else {
                            Default::default()
                        };
                    let add = Some(Mutation::Add(AddMutation {
                        actor_dispatchers,
                        added_actors,
                        actor_splits,
                        // If the cluster is already paused, the new actors should be paused too.
                        pause: current_paused_reason.is_some(),
                        subscriptions_to_add,
                    }));

                    if let CreateStreamingJobType::SinkIntoTable(ReplaceTablePlan {
                        old_table_fragments,
                        new_table_fragments: _,
                        merge_updates,
                        dispatchers,
                        init_split_assignment,
                        ..
                    }) = job_type
                    {
                        // TODO: support in v2.
                        let update = Self::generate_update_mutation_for_replace_table(
                            old_table_fragments,
                            merge_updates,
                            dispatchers,
                            init_split_assignment,
                        );

                        Some(Mutation::Combined(CombinedMutation {
                            mutations: vec![
                                BarrierMutation { mutation: add },
                                BarrierMutation { mutation: update },
                            ],
                        }))
                    } else {
                        add
                    }
                }
                Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge) => {
                    Some(Mutation::DropSubscriptions(DropSubscriptionsMutation {
                        info: jobs_to_merge
                            .iter()
                            .flat_map(|(table_id, (backfill_info, _))| {
                                backfill_info.upstream_mv_table_ids.iter().map(
                                    move |upstream_table_id| SubscriptionUpstreamInfo {
                                        subscriber_id: table_id.table_id,
                                        upstream_mv_table_id: upstream_table_id.table_id,
                                    },
                                )
                            })
                            .collect(),
                    }))
                }

                Command::CancelStreamingJob(table_fragments) => {
                    let actors = table_fragments.actor_ids();
                    Some(Mutation::Stop(StopMutation { actors }))
                }

                Command::ReplaceTable(ReplaceTablePlan {
                    old_table_fragments,
                    merge_updates,
                    dispatchers,
                    init_split_assignment,
                    ..
                }) => Self::generate_update_mutation_for_replace_table(
                    old_table_fragments,
                    merge_updates,
                    dispatchers,
                    init_split_assignment,
                ),

                Command::RescheduleFragment {
                    reschedules,
                    fragment_actors,
                    ..
                } => {
                    let mut dispatcher_update = HashMap::new();
                    for reschedule in reschedules.values() {
                        for &(upstream_fragment_id, dispatcher_id) in
                            &reschedule.upstream_fragment_dispatcher_ids
                        {
                            // Find the actors of the upstream fragment.
                            let upstream_actor_ids = fragment_actors
                                .get(&upstream_fragment_id)
                                .expect("should contain");

                            // Record updates for all actors.
                            for &actor_id in upstream_actor_ids {
                                // Index with the dispatcher id to check duplicates.
                                dispatcher_update
                                    .try_insert(
                                        (actor_id, dispatcher_id),
                                        DispatcherUpdate {
                                            actor_id,
                                            dispatcher_id,
                                            hash_mapping: reschedule
                                                .upstream_dispatcher_mapping
                                                .as_ref()
                                                .map(|m| m.to_protobuf()),
                                            added_downstream_actor_id: reschedule
                                                .added_actors
                                                .values()
                                                .flatten()
                                                .cloned()
                                                .collect(),
                                            removed_downstream_actor_id: reschedule
                                                .removed_actors
                                                .clone(),
                                        },
                                    )
                                    .unwrap();
                            }
                        }
                    }
                    let dispatcher_update = dispatcher_update.into_values().collect();

                    let mut merge_update = HashMap::new();
                    for (&fragment_id, reschedule) in reschedules {
                        for &downstream_fragment_id in &reschedule.downstream_fragment_ids {
                            // Find the actors of the downstream fragment.
                            let downstream_actor_ids = fragment_actors
                                .get(&downstream_fragment_id)
                                .expect("should contain");

                            // Downstream removed actors should be skipped
                            // Newly created actors of the current fragment will not dispatch Update
                            // barriers to them
                            let downstream_removed_actors: HashSet<_> = reschedules
                                .get(&downstream_fragment_id)
                                .map(|downstream_reschedule| {
                                    downstream_reschedule
                                        .removed_actors
                                        .iter()
                                        .copied()
                                        .collect()
                                })
                                .unwrap_or_default();

                            // Record updates for all actors.
                            for &actor_id in downstream_actor_ids {
                                if downstream_removed_actors.contains(&actor_id) {
                                    continue;
                                }

                                // Index with the fragment id to check duplicates.
                                merge_update
                                    .try_insert(
                                        (actor_id, fragment_id),
                                        MergeUpdate {
                                            actor_id,
                                            upstream_fragment_id: fragment_id,
                                            new_upstream_fragment_id: None,
                                            added_upstream_actor_id: reschedule
                                                .added_actors
                                                .values()
                                                .flatten()
                                                .cloned()
                                                .collect(),
                                            removed_upstream_actor_id: reschedule
                                                .removed_actors
                                                .clone(),
                                        },
                                    )
                                    .unwrap();
                            }
                        }
                    }
                    let merge_update = merge_update.into_values().collect();

                    let mut actor_vnode_bitmap_update = HashMap::new();
                    for reschedule in reschedules.values() {
                        // Record updates for all actors in this fragment.
                        for (&actor_id, bitmap) in &reschedule.vnode_bitmap_updates {
                            let bitmap = bitmap.to_protobuf();
                            actor_vnode_bitmap_update
                                .try_insert(actor_id, bitmap)
                                .unwrap();
                        }
                    }

                    let dropped_actors = reschedules
                        .values()
                        .flat_map(|r| r.removed_actors.iter().copied())
                        .collect();

                    let mut actor_splits = HashMap::new();

                    for reschedule in reschedules.values() {
                        for (actor_id, splits) in &reschedule.actor_splits {
                            actor_splits.insert(
                                *actor_id as ActorId,
                                ConnectorSplits {
                                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                                },
                            );
                        }
                    }

                    // we don't create dispatchers in reschedule scenario
                    let actor_new_dispatchers = HashMap::new();

                    let mutation = Mutation::Update(UpdateMutation {
                        dispatcher_update,
                        merge_update,
                        actor_vnode_bitmap_update,
                        dropped_actors,
                        actor_splits,
                        actor_new_dispatchers,
                    });
                    tracing::debug!("update mutation: {mutation:?}");
                    Some(mutation)
                }

                Command::CreateSubscription {
                    upstream_mv_table_id,
                    subscription_id,
                    ..
                } => Some(Mutation::Add(AddMutation {
                    actor_dispatchers: Default::default(),
                    added_actors: vec![],
                    actor_splits: Default::default(),
                    pause: false,
                    subscriptions_to_add: vec![SubscriptionUpstreamInfo {
                        upstream_mv_table_id: upstream_mv_table_id.table_id,
                        subscriber_id: *subscription_id,
                    }],
                })),
                Command::DropSubscription {
                    upstream_mv_table_id,
                    subscription_id,
                } => Some(Mutation::DropSubscriptions(DropSubscriptionsMutation {
                    info: vec![SubscriptionUpstreamInfo {
                        subscriber_id: *subscription_id,
                        upstream_mv_table_id: upstream_mv_table_id.table_id,
                    }],
                })),
            };

        mutation
    }

    pub fn actors_to_create(&self) -> Option<HashMap<WorkerId, Vec<StreamActor>>> {
        match self {
            Command::CreateStreamingJob { info, job_type } => {
                let mut map = match job_type {
                    CreateStreamingJobType::Normal => HashMap::new(),
                    CreateStreamingJobType::SinkIntoTable(replace_table) => {
                        replace_table.new_table_fragments.actors_to_create()
                    }
                    CreateStreamingJobType::SnapshotBackfill(_) => {
                        // for snapshot backfill, the actors to create is measured separately
                        return None;
                    }
                };
                for (worker_id, new_actors) in info.table_fragments.actors_to_create() {
                    map.entry(worker_id).or_default().extend(new_actors)
                }
                Some(map)
            }
            Command::RescheduleFragment { reschedules, .. } => {
                let mut map: HashMap<WorkerId, Vec<_>> = HashMap::new();
                for (actor, status) in reschedules
                    .values()
                    .flat_map(|reschedule| reschedule.newly_created_actors.iter())
                {
                    let worker_id = status.location.as_ref().unwrap().worker_node_id as _;
                    map.entry(worker_id).or_default().push(actor.clone());
                }
                Some(map)
            }
            Command::ReplaceTable(replace_table) => {
                Some(replace_table.new_table_fragments.actors_to_create())
            }
            _ => None,
        }
    }

    fn generate_update_mutation_for_replace_table(
        old_table_fragments: &TableFragments,
        merge_updates: &[MergeUpdate],
        dispatchers: &HashMap<ActorId, Vec<Dispatcher>>,
        init_split_assignment: &SplitAssignment,
    ) -> Option<Mutation> {
        let dropped_actors = old_table_fragments.actor_ids();

        let actor_new_dispatchers = dispatchers
            .iter()
            .map(|(&actor_id, dispatchers)| {
                (
                    actor_id,
                    Dispatchers {
                        dispatchers: dispatchers.clone(),
                    },
                )
            })
            .collect();

        let actor_splits = init_split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        Some(Mutation::Update(UpdateMutation {
            actor_new_dispatchers,
            merge_update: merge_updates.to_owned(),
            dropped_actors,
            actor_splits,
            ..Default::default()
        }))
    }

    /// Returns the paused reason after executing the current command.
    pub fn next_paused_reason(
        this: Option<&Self>,
        current_paused_reason: Option<PausedReason>,
    ) -> Option<PausedReason> {
        match this {
            Some(Command::Pause(reason)) => {
                // Only pause when the cluster is not already paused.
                if current_paused_reason.is_none() {
                    Some(*reason)
                } else {
                    current_paused_reason
                }
            }

            Some(Command::Resume(reason)) => {
                // Only resume when the cluster is paused with the same reason.
                if current_paused_reason == Some(*reason) {
                    None
                } else {
                    current_paused_reason
                }
            }

            _ => current_paused_reason,
        }
    }

    /// For `CancelStreamingJob`, returns the table id of the target table.
    pub fn table_to_cancel(&self) -> Option<TableId> {
        match self {
            Command::CancelStreamingJob(table_fragments) => Some(table_fragments.table_id()),
            _ => None,
        }
    }
}
