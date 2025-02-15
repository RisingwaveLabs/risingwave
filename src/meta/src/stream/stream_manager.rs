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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use futures::future::join_all;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_meta_model::ObjectId;
use risingwave_pb::catalog::{CreateType, Subscription, Table};
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::Dispatcher;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, Mutex};
use tracing::Instrument;

use super::{
    JobParallelismTarget, JobReschedulePolicy, JobReschedulePostUpdates, JobRescheduleTarget,
    JobResourceGroupTarget, Locations, RescheduleOptions, ScaleControllerRef,
};
use crate::barrier::{
    BarrierScheduler, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, SnapshotBackfillInfo,
};
use crate::error::bail_invalid_parameter;
use crate::manager::{
    MetaSrvEnv, MetadataManager, NotificationVersion, StreamingJob, StreamingJobType,
};
use crate::model::{ActorId, FragmentId, StreamJobFragments, TableParallelism};
use crate::stream::{SourceChange, SourceManagerRef};
use crate::{MetaError, MetaResult};

pub type GlobalStreamManagerRef = Arc<GlobalStreamManager>;

#[derive(Default)]
pub struct CreateStreamingJobOption {
    // leave empty as a placeholder for future option if there is any
}

/// [`CreateStreamingJobContext`] carries one-time infos for creating a streaming job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct CreateStreamingJobContext {
    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<FragmentId, HashMap<ActorId, Vec<Dispatcher>>>,

    /// Internal tables in the streaming job.
    pub internal_tables: BTreeMap<u32, Table>,

    /// The locations of the actors to build in the streaming job.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the upstream mview actors to update.
    pub existing_locations: Locations,

    /// DDL definition.
    pub definition: String,

    pub mv_table_id: Option<u32>,

    pub create_type: CreateType,

    pub job_type: StreamingJobType,

    /// Context provided for potential replace table, typically used when sinking into a table.
    pub replace_table_job_info: Option<(StreamingJob, ReplaceStreamJobContext, StreamJobFragments)>,

    pub snapshot_backfill_info: Option<SnapshotBackfillInfo>,
    pub cross_db_snapshot_backfill_info: SnapshotBackfillInfo,

    pub option: CreateStreamingJobOption,

    pub streaming_job: StreamingJob,
}

impl CreateStreamingJobContext {
    pub fn internal_tables(&self) -> Vec<Table> {
        self.internal_tables.values().cloned().collect()
    }
}

pub enum CreatingState {
    Failed { reason: MetaError },
    // sender is used to notify the canceling result.
    Canceling { finish_tx: oneshot::Sender<()> },
    Created { version: NotificationVersion },
}

struct StreamingJobExecution {
    id: TableId,
    shutdown_tx: Option<Sender<CreatingState>>,
}

impl StreamingJobExecution {
    fn new(id: TableId, shutdown_tx: Sender<CreatingState>) -> Self {
        Self {
            id,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

#[derive(Default)]
struct CreatingStreamingJobInfo {
    streaming_jobs: Mutex<HashMap<TableId, StreamingJobExecution>>,
}

impl CreatingStreamingJobInfo {
    async fn add_job(&self, job: StreamingJobExecution) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.insert(job.id, job);
    }

    async fn delete_job(&self, job_id: TableId) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.remove(&job_id);
    }

    async fn cancel_jobs(
        &self,
        job_ids: Vec<TableId>,
    ) -> (HashMap<TableId, oneshot::Receiver<()>>, Vec<TableId>) {
        let mut jobs = self.streaming_jobs.lock().await;
        let mut receivers = HashMap::new();
        let mut recovered_job_ids = vec![];
        for job_id in job_ids {
            if let Some(job) = jobs.get_mut(&job_id) {
                if let Some(shutdown_tx) = job.shutdown_tx.take() {
                    let (tx, rx) = oneshot::channel();
                    if shutdown_tx
                        .send(CreatingState::Canceling { finish_tx: tx })
                        .await
                        .is_ok()
                    {
                        receivers.insert(job_id, rx);
                    } else {
                        tracing::warn!(id=?job_id, "failed to send canceling state");
                    }
                }
            } else {
                // If these job ids do not exist in streaming_jobs,
                // we can infer they either:
                // 1. are entirely non-existent,
                // 2. OR they are recovered streaming jobs, and managed by BarrierManager.
                recovered_job_ids.push(job_id);
            }
        }
        (receivers, recovered_job_ids)
    }
}

type CreatingStreamingJobInfoRef = Arc<CreatingStreamingJobInfo>;

/// [`ReplaceStreamJobContext`] carries one-time infos for replacing the plan of an existing stream job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct ReplaceStreamJobContext {
    /// The old job fragments to be replaced.
    pub old_fragments: StreamJobFragments,

    /// The updates to be applied to the downstream chain actors. Used for schema change.
    pub merge_updates: HashMap<FragmentId, Vec<MergeUpdate>>,

    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<FragmentId, HashMap<ActorId, Vec<Dispatcher>>>,

    /// The locations of the actors to build in the new job to replace.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the downstream chain actors to update.
    pub existing_locations: Locations,

    pub streaming_job: StreamingJob,

    pub tmp_id: u32,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager {
    pub env: MetaSrvEnv,

    pub metadata_manager: MetadataManager,

    /// Broadcasts and collect barriers
    pub barrier_scheduler: BarrierScheduler,

    /// Maintains streaming sources from external system like kafka
    pub source_manager: SourceManagerRef,

    /// Creating streaming job info.
    creating_job_info: CreatingStreamingJobInfoRef,

    pub scale_controller: ScaleControllerRef,
}

impl GlobalStreamManager {
    pub fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        barrier_scheduler: BarrierScheduler,
        source_manager: SourceManagerRef,
        scale_controller: ScaleControllerRef,
    ) -> MetaResult<Self> {
        Ok(Self {
            env,
            metadata_manager,
            barrier_scheduler,
            source_manager,
            creating_job_info: Arc::new(CreatingStreamingJobInfo::default()),
            scale_controller,
        })
    }

    /// Create streaming job, it works as follows:
    ///
    /// 1. Broadcast the actor info based on the scheduling result in the context, build the hanging
    ///    channels in upstream worker nodes.
    /// 2. (optional) Get the split information of the `StreamSource` via source manager and patch
    ///    actors.
    /// 3. Notify related worker nodes to update and build the actors.
    /// 4. Store related meta data.
    ///
    /// This function is a wrapper over [`Self::create_streaming_job_impl`].
    pub async fn create_streaming_job(
        self: &Arc<Self>,
        stream_job_fragments: StreamJobFragments,
        ctx: CreateStreamingJobContext,
    ) -> MetaResult<NotificationVersion> {
        let table_id = stream_job_fragments.stream_job_id();
        let database_id = ctx.streaming_job.database_id().into();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let execution = StreamingJobExecution::new(table_id, sender.clone());
        self.creating_job_info.add_job(execution).await;

        let stream_manager = self.clone();
        let fut = async move {
            let res = stream_manager
                .create_streaming_job_impl(stream_job_fragments, ctx)
                .await;
            match res {
                Ok(version) => {
                    let _ = sender
                        .send(CreatingState::Created { version })
                        .await
                        .inspect_err(|_| tracing::warn!("failed to notify created: {table_id}"));
                }
                Err(err) => {
                    let _ = sender
                        .send(CreatingState::Failed {
                            reason: err.clone(),
                        })
                        .await
                        .inspect_err(|_| {
                            tracing::warn!(error = %err.as_report(), "failed to notify failed: {table_id}")
                        });
                }
            }
        }
        .in_current_span();
        tokio::spawn(fut);

        while let Some(state) = receiver.recv().await {
            match state {
                CreatingState::Failed { reason } => {
                    tracing::debug!(id=?table_id, "stream job failed");
                    // FIXME(kwannoel): For creating stream jobs
                    // we need to clean up the resources in the stream manager.
                    self.creating_job_info.delete_job(table_id).await;
                    return Err(reason);
                }
                CreatingState::Canceling { finish_tx } => {
                    tracing::debug!(id=?table_id, "cancelling streaming job");
                    if let Ok(table_fragments) = self
                        .metadata_manager
                        .get_job_fragments_by_id(&table_id)
                        .await
                    {
                        // try to cancel buffered creating command.
                        if self
                            .barrier_scheduler
                            .try_cancel_scheduled_create(database_id, table_id)
                        {
                            tracing::debug!("cancelling streaming job {table_id} in buffer queue.");
                        } else if !table_fragments.is_created() {
                            tracing::debug!(
                                "cancelling streaming job {table_id} by issue cancel command."
                            );
                            self.metadata_manager
                                .catalog_controller
                                .try_abort_creating_streaming_job(table_id.table_id as _, true)
                                .await?;

                            self.barrier_scheduler
                                .run_command(database_id, Command::cancel(&table_fragments))
                                .await?;
                        } else {
                            // streaming job is already completed.
                            continue;
                        }
                        let _ = finish_tx.send(()).inspect_err(|_| {
                            tracing::warn!("failed to notify cancelled: {table_id}")
                        });
                        self.creating_job_info.delete_job(table_id).await;
                        return Err(MetaError::cancelled("create"));
                    }
                }
                CreatingState::Created { version } => {
                    self.creating_job_info.delete_job(table_id).await;
                    return Ok(version);
                }
            }
        }
        self.creating_job_info.delete_job(table_id).await;
        bail!("receiver failed to get notification version for finished stream job")
    }

    /// The function will only return after backfilling finishes
    /// ([`crate::manager::MetadataManager::wait_streaming_job_finished`]).
    async fn create_streaming_job_impl(
        &self,
        stream_job_fragments: StreamJobFragments,
        CreateStreamingJobContext {
            streaming_job,
            dispatchers,
            definition,
            create_type,
            job_type,
            replace_table_job_info,
            internal_tables,
            snapshot_backfill_info,
            cross_db_snapshot_backfill_info,
            ..
        }: CreateStreamingJobContext,
    ) -> MetaResult<NotificationVersion> {
        let mut replace_table_command = None;

        tracing::debug!(
            table_id = %stream_job_fragments.stream_job_id(),
            "built actors finished"
        );

        let need_pause = replace_table_job_info.is_some();

        if let Some((streaming_job, context, stream_job_fragments)) = replace_table_job_info {
            self.metadata_manager
                .catalog_controller
                .prepare_streaming_job(&stream_job_fragments, &streaming_job, true)
                .await?;

            let tmp_table_id = stream_job_fragments.stream_job_id();
            let init_split_assignment = self.source_manager.allocate_splits(&tmp_table_id).await?;

            replace_table_command = Some(ReplaceStreamJobPlan {
                old_fragments: context.old_fragments,
                new_fragments: stream_job_fragments,
                merge_updates: context.merge_updates,
                dispatchers: context.dispatchers,
                init_split_assignment,
                streaming_job,
                tmp_id: tmp_table_id.table_id,
            });
        }

        let table_id = stream_job_fragments.stream_job_id();

        // Here we need to consider:
        // - Shared source
        // - Table with connector
        // - MV on shared source
        let mut init_split_assignment = self.source_manager.allocate_splits(&table_id).await?;
        init_split_assignment.extend(
            self.source_manager
                .allocate_splits_for_backfill(&table_id, &dispatchers)
                .await?,
        );

        let source_change = SourceChange::CreateJobFinished {
            finished_backfill_fragments: stream_job_fragments.source_backfill_fragments()?,
        };

        let info = CreateStreamingJobCommandInfo {
            stream_job_fragments,
            dispatchers,
            init_split_assignment,
            definition: definition.clone(),
            streaming_job: streaming_job.clone(),
            internal_tables: internal_tables.into_values().collect_vec(),
            job_type,
            create_type,
        };

        let job_type = if let Some(snapshot_backfill_info) = snapshot_backfill_info {
            tracing::debug!(
                ?snapshot_backfill_info,
                "sending Command::CreateSnapshotBackfillStreamingJob"
            );
            CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info)
        } else {
            tracing::debug!("sending Command::CreateStreamingJob");
            if let Some(replace_table_command) = replace_table_command {
                CreateStreamingJobType::SinkIntoTable(replace_table_command)
            } else {
                CreateStreamingJobType::Normal
            }
        };

        let command = Command::CreateStreamingJob {
            info,
            job_type,
            cross_db_snapshot_backfill_info,
        };

        if need_pause {
            // Special handling is required when creating sink into table, we need to pause the stream to avoid data loss.
            self.barrier_scheduler
                .run_config_change_command_with_pause(streaming_job.database_id().into(), command)
                .await?;
        } else {
            self.barrier_scheduler
                .run_command(streaming_job.database_id().into(), command)
                .await?;
        }

        tracing::debug!(?streaming_job, "first barrier collected for stream job");
        let version = self
            .metadata_manager
            .wait_streaming_job_finished(streaming_job.id() as _)
            .await?;
        self.source_manager.apply_source_change(source_change).await;
        tracing::debug!(?streaming_job, "stream job finish");
        Ok(version)
    }

    /// Send replace job command to barrier scheduler.
    pub async fn replace_stream_job(
        &self,
        new_fragments: StreamJobFragments,
        ReplaceStreamJobContext {
            old_fragments,
            merge_updates,
            dispatchers,
            tmp_id,
            streaming_job,
            ..
        }: ReplaceStreamJobContext,
    ) -> MetaResult<()> {
        let tmp_table_id = new_fragments.stream_job_id();
        let init_split_assignment = if streaming_job.is_source() {
            self.source_manager
                .allocate_splits_for_replace_source(&tmp_table_id, &merge_updates)
                .await?
        } else {
            self.source_manager.allocate_splits(&tmp_table_id).await?
        };

        self.barrier_scheduler
            .run_config_change_command_with_pause(
                streaming_job.database_id().into(),
                Command::ReplaceStreamJob(ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    merge_updates,
                    dispatchers,
                    init_split_assignment,
                    streaming_job,
                    tmp_id,
                }),
            )
            .await?;

        Ok(())
    }

    /// Drop streaming jobs by barrier manager, and clean up all related resources. The error will
    /// be ignored because the recovery process will take over it in cleaning part. Check
    /// [`Command::DropStreamingJobs`] for details.
    pub async fn drop_streaming_jobs(
        &self,
        database_id: DatabaseId,
        removed_actors: Vec<ActorId>,
        streaming_job_ids: Vec<ObjectId>,
        state_table_ids: Vec<risingwave_meta_model::TableId>,
        fragment_ids: HashSet<FragmentId>,
    ) {
        if !removed_actors.is_empty()
            || !streaming_job_ids.is_empty()
            || !state_table_ids.is_empty()
        {
            let _ = self
                .barrier_scheduler
                .run_command(
                    database_id,
                    Command::DropStreamingJobs {
                        table_fragments_ids: streaming_job_ids
                            .iter()
                            .map(|job_id| TableId::new(*job_id as _))
                            .collect(),
                        actors: removed_actors,
                        unregistered_state_table_ids: state_table_ids
                            .into_iter()
                            .map(|table_id| TableId::new(table_id as _))
                            .collect(),
                        unregistered_fragment_ids: fragment_ids,
                    },
                )
                .await
                .inspect_err(|err| {
                    tracing::error!(error = ?err.as_report(), "failed to run drop command");
                });
        }
    }

    /// Cancel streaming jobs and return the canceled table ids.
    /// 1. Send cancel message to stream jobs (via `cancel_jobs`).
    /// 2. Send cancel message to recovered stream jobs (via `barrier_scheduler`).
    ///
    /// Cleanup of their state will be cleaned up after the `CancelStreamJob` command succeeds,
    /// by the barrier manager for both of them.
    pub async fn cancel_streaming_jobs(&self, table_ids: Vec<TableId>) -> Vec<TableId> {
        if table_ids.is_empty() {
            return vec![];
        }

        let _reschedule_job_lock = self.reschedule_lock_read_guard().await;
        let (receivers, recovered_job_ids) = self.creating_job_info.cancel_jobs(table_ids).await;

        let futures = receivers.into_iter().map(|(id, receiver)| async move {
            if receiver.await.is_ok() {
                tracing::info!("canceled streaming job {id}");
                Some(id)
            } else {
                tracing::warn!("failed to cancel streaming job {id}");
                None
            }
        });
        let mut cancelled_ids = join_all(futures).await.into_iter().flatten().collect_vec();

        // NOTE(kwannoel): For recovered stream jobs, we can directly cancel them by running the barrier command,
        // since Barrier manager manages the recovered stream jobs.
        let futures = recovered_job_ids.into_iter().map(|id| async move {
            tracing::debug!(?id, "cancelling recovered streaming job");
            let result: MetaResult<()> = try {
                let fragment = self
                    .metadata_manager.get_job_fragments_by_id(&id)
                    .await?;
                if fragment.is_created() {
                    Err(MetaError::invalid_parameter(format!(
                        "streaming job {} is already created",
                        id
                    )))?;
                }

                let (_, database_id) = self.metadata_manager
                    .catalog_controller
                    .try_abort_creating_streaming_job(id.table_id as _, true)
                    .await?;

                if let Some(database_id) = database_id {
                    self.barrier_scheduler
                        .run_command(DatabaseId::new(database_id as _), Command::cancel(&fragment))
                        .await?;
                }
            };
            match result {
                Ok(_) => {
                    tracing::info!(?id, "cancelled recovered streaming job");
                    Some(id)
                }
                Err(err) => {
                    tracing::error!(error=?err.as_report(), "failed to cancel recovered streaming job {id}, does it correspond to any jobs in `SHOW JOBS`?");
                    None
                }
            }
        });
        let cancelled_recovered_ids = join_all(futures).await.into_iter().flatten().collect_vec();

        cancelled_ids.extend(cancelled_recovered_ids);
        cancelled_ids
    }

    pub(crate) async fn reschedule_streaming_job(
        &self,
        table_id: u32,
        target: JobRescheduleTarget,
        deferred: bool,
    ) -> MetaResult<()> {
        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;
        let background_jobs = self
            .metadata_manager
            .list_background_creating_jobs()
            .await?;

        if !background_jobs.is_empty() {
            let related_jobs = self
                .scale_controller
                .resolve_related_no_shuffle_jobs(&background_jobs)
                .await?;

            for job in background_jobs {
                if related_jobs.contains(&job) {
                    bail!("Cannot alter the job {} because the related job {} is currently being created", table_id, job.table_id);
                }
            }
        }

        let JobRescheduleTarget {
            parallelism: parallelism_change,
            resource_group: resource_group_change,
        } = target;

        let database_id = DatabaseId::new(
            self.metadata_manager
                .catalog_controller
                .get_object_database_id(table_id as ObjectId)
                .await? as _,
        );
        let table_id = TableId::new(table_id);

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?
            .into_iter()
            .filter(|w| w.is_streaming_schedulable())
            .collect_vec();

        // Check if the provided parallelism is valid.
        let available_parallelism = worker_nodes
            .iter()
            .map(|w| w.compute_node_parallelism())
            .sum::<usize>();
        let max_parallelism = self
            .metadata_manager
            .get_job_max_parallelism(table_id)
            .await?;

        if let JobParallelismTarget::Update(parallelism) = parallelism_change {
            match parallelism {
                TableParallelism::Adaptive => {
                    if available_parallelism > max_parallelism {
                        tracing::warn!(
                        "too many parallelism available, use max parallelism {} will be limited",
                        max_parallelism
                    );
                    }
                }
                TableParallelism::Fixed(parallelism) => {
                    if parallelism > max_parallelism {
                        bail_invalid_parameter!(
                            "specified parallelism {} should not exceed max parallelism {}",
                            parallelism,
                            max_parallelism
                        );
                    }
                }
                TableParallelism::Custom => {
                    bail_invalid_parameter!("should not alter parallelism to custom")
                }
            }
        }

        let table_parallelism_assignment = match &parallelism_change {
            JobParallelismTarget::Update(parallelism) => HashMap::from([(table_id, *parallelism)]),
            JobParallelismTarget::Refresh => HashMap::new(),
        };
        let resource_group_assignment = match &resource_group_change {
            JobResourceGroupTarget::Update(target) => {
                HashMap::from([(table_id.table_id() as ObjectId, target.clone())])
            }
            JobResourceGroupTarget::Keep => HashMap::new(),
        };

        if deferred {
            tracing::debug!(
                "deferred mode enabled for job {}, set the parallelism directly to parallelism {:?}, resource group {:?}",
                table_id,
                parallelism_change,
                resource_group_change,
            );
            self.scale_controller
                .post_apply_reschedule(
                    &HashMap::new(),
                    &JobReschedulePostUpdates {
                        parallelism_updates: table_parallelism_assignment,
                        resource_group_updates: resource_group_assignment,
                    },
                )
                .await?;
        } else {
            let reschedule_plan = self
                .scale_controller
                .generate_job_reschedule_plan(JobReschedulePolicy {
                    targets: HashMap::from([(
                        table_id.table_id,
                        JobRescheduleTarget {
                            parallelism: parallelism_change,
                            resource_group: resource_group_change,
                        },
                    )]),
                })
                .await?;

            if reschedule_plan.reschedules.is_empty() {
                tracing::debug!("empty reschedule plan generated for job {}, set the parallelism directly to {:?}", table_id, reschedule_plan.post_updates);
                self.scale_controller
                    .post_apply_reschedule(&HashMap::new(), &reschedule_plan.post_updates)
                    .await?;
            } else {
                self.reschedule_actors(
                    database_id,
                    reschedule_plan,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: false,
                        skip_create_new_actors: false,
                    },
                )
                .await?;
            }
        };

        Ok(())
    }

    // Don't need to add actor, just send a command
    pub async fn create_subscription(
        self: &Arc<Self>,
        subscription: &Subscription,
    ) -> MetaResult<()> {
        let command = Command::CreateSubscription {
            subscription_id: subscription.id,
            upstream_mv_table_id: TableId::new(subscription.dependent_table_id),
            retention_second: subscription.retention_seconds,
        };

        tracing::debug!("sending Command::CreateSubscription");
        self.barrier_scheduler
            .run_command(subscription.database_id.into(), command)
            .await?;
        Ok(())
    }

    // Don't need to add actor, just send a command
    pub async fn drop_subscription(
        self: &Arc<Self>,
        database_id: DatabaseId,
        subscription_id: u32,
        table_id: u32,
    ) {
        let command = Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id: TableId::new(table_id),
        };

        tracing::debug!("sending Command::DropSubscriptions");
        let _ = self
            .barrier_scheduler
            .run_command(database_id, command)
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err.as_report(), "failed to run drop command");
            });
    }
}
