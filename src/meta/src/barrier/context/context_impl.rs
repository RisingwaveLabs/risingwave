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

use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::catalog::DatabaseId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::PbFragmentTypeFlag;
use risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest;
use risingwave_pb::stream_service::WaitEpochCommitRequest;
use risingwave_rpc_client::StreamingControlHandle;

use crate::barrier::command::CommandContext;
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::progress::TrackingJob;
use crate::barrier::{
    BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, Command, CreateStreamingJobCommandInfo,
    CreateStreamingJobType, DatabaseRuntimeInfoSnapshot, RecoveryReason, ReplaceStreamJobPlan,
    Scheduled,
};
use crate::hummock::CommitEpochInfo;
use crate::stream::SourceChange;
use crate::{MetaError, MetaResult};

impl GlobalBarrierWorkerContext for GlobalBarrierWorkerContextImpl {
    async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        self.hummock_manager.commit_epoch(commit_info).await?;
        Ok(self.hummock_manager.get_version_stats().await)
    }

    async fn next_scheduled(&self) -> Scheduled {
        self.scheduled_barriers.next_scheduled().await
    }

    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    ) {
        if database_id.is_none() {
            self.set_status(BarrierManagerStatus::Recovering(recovery_reason));
        }

        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked(database_id, "cluster is under recovering");
    }

    fn mark_ready(&self, database_id: Option<DatabaseId>) {
        self.scheduled_barriers.mark_ready(database_id);
        if database_id.is_none() {
            self.set_status(BarrierManagerStatus::Running);
        }
    }

    async fn post_collect_command<'a>(&'a self, command: &'a CommandContext) -> MetaResult<()> {
        command.post_collect(self).await
    }

    async fn notify_creating_job_failed(&self, err: &MetaError) {
        self.metadata_manager.notify_finish_failed(err).await
    }

    async fn finish_creating_job(&self, job: TrackingJob) -> MetaResult<()> {
        job.finish(&self.metadata_manager).await
    }

    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        init_request: &PbInitRequest,
    ) -> MetaResult<StreamingControlHandle> {
        self.new_control_stream_impl(node, init_request).await
    }

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        self.reload_runtime_info_impl().await
    }

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>> {
        self.reload_database_runtime_info_impl(database_id).await
    }
}

impl GlobalBarrierWorkerContextImpl {
    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }
}

impl CommandContext {
    pub async fn wait_epoch_commit(
        &self,
        barrier_manager_context: &GlobalBarrierWorkerContextImpl,
    ) -> MetaResult<()> {
        let table_id = self.table_ids_to_commit.iter().next().cloned();
        // try wait epoch on an existing random table id
        let Some(table_id) = table_id else {
            // no need to wait epoch when there is no table id
            return Ok(());
        };
        let futures = self.node_map.values().map(|worker_node| async {
            let client = barrier_manager_context
                .env
                .stream_client_pool()
                .get(worker_node)
                .await?;
            let request = WaitEpochCommitRequest {
                epoch: self.barrier_info.prev_epoch(),
                table_id: table_id.table_id,
            };
            client.wait_epoch_commit(request).await
        });

        try_join_all(futures).await?;

        Ok(())
    }

    /// Do some stuffs after barriers are collected and the new storage version is committed, for
    /// the given command.
    pub async fn post_collect(
        &self,
        barrier_manager_context: &GlobalBarrierWorkerContextImpl,
    ) -> MetaResult<()> {
        let Some(command) = &self.command else {
            return Ok(());
        };
        match command {
            Command::Flush => {}

            Command::Throttle(_) => {}

            Command::Pause(reason) => {
                if let PausedReason::ConfigChange = reason {
                    // After the `Pause` barrier is collected and committed, we must ensure that the
                    // storage version with this epoch is synced to all compute nodes before the
                    // execution of the next command of `Update`, as some newly created operators
                    // may immediately initialize their states on that barrier.
                    self.wait_epoch_commit(barrier_manager_context).await?;
                }
            }

            Command::Resume(_) => {}

            Command::SourceChangeSplit(split_assignment) => {
                barrier_manager_context
                    .metadata_manager
                    .update_actor_splits_by_split_assignment(split_assignment)
                    .await?;
                barrier_manager_context
                    .source_manager
                    .apply_source_change(SourceChange::SplitChange(split_assignment.clone()))
                    .await;
            }

            Command::DropStreamingJobs {
                unregistered_state_table_ids,
                ..
            } => {
                barrier_manager_context
                    .hummock_manager
                    .unregister_table_ids(unregistered_state_table_ids.iter().cloned())
                    .await?;
            }
            Command::CreateStreamingJob {
                info,
                job_type,
                cross_db_snapshot_backfill_info,
            } => {
                let mut is_sink_into_table = false;
                match job_type {
                    CreateStreamingJobType::SinkIntoTable(
                        replace_plan @ ReplaceStreamJobPlan {
                            old_fragments,
                            new_fragments,
                            dispatchers,
                            init_split_assignment,
                            ..
                        },
                    ) => {
                        is_sink_into_table = true;
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .post_collect_job_fragments(
                                new_fragments.stream_job_id().table_id as _,
                                new_fragments.actor_ids(),
                                dispatchers,
                                init_split_assignment,
                            )
                            .await?;
                        barrier_manager_context
                            .source_manager
                            .handle_replace_job(
                                old_fragments,
                                new_fragments.stream_source_fragments(),
                                init_split_assignment.clone(),
                                replace_plan,
                            )
                            .await;
                    }
                    CreateStreamingJobType::Normal => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if (fragment.fragment_type_mask
                                            & PbFragmentTypeFlag::CrossDbSnapshotBackfillStreamScan as u32)
                                            != 0
                                        {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                None,
                                cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                    CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info) => {
                        barrier_manager_context
                            .metadata_manager
                            .catalog_controller
                            .fill_snapshot_backfill_epoch(
                                info.stream_job_fragments.fragments.iter().filter_map(
                                    |(fragment_id, fragment)| {
                                        if (fragment.fragment_type_mask
                                            & (PbFragmentTypeFlag::SnapshotBackfillStreamScan as u32 | PbFragmentTypeFlag::CrossDbSnapshotBackfillStreamScan as u32))
                                            != 0
                                        {
                                            Some(*fragment_id as _)
                                        } else {
                                            None
                                        }
                                    },
                                ),
                                Some(snapshot_backfill_info),
                                cross_db_snapshot_backfill_info,
                            )
                            .await?
                    }
                }

                // Do `post_collect_job_fragments` of the original streaming job in the end, so that in any previous failure,
                // we won't mark the job as `Creating`, and then the job will be later clean by the recovery triggered by the returned error.
                let CreateStreamingJobCommandInfo {
                    stream_job_fragments,
                    dispatchers,
                    init_split_assignment,
                    ..
                } = info;
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        stream_job_fragments.stream_job_id().table_id as _,
                        stream_job_fragments.actor_ids(),
                        dispatchers,
                        init_split_assignment,
                    )
                    .await?;

                if !is_sink_into_table {
                    barrier_manager_context
                        .source_manager
                        .apply_source_change(SourceChange::CreateJob {
                            added_source_fragments: stream_job_fragments.stream_source_fragments(),
                            added_backfill_fragments: stream_job_fragments
                                .source_backfill_fragments()?,
                            split_assignment: init_split_assignment.clone(),
                        })
                        .await;
                }
            }
            Command::RescheduleFragment {
                reschedules,
                post_updates,
                ..
            } => {
                barrier_manager_context
                    .scale_controller
                    .post_apply_reschedule(reschedules, post_updates)
                    .await?;
            }

            Command::ReplaceStreamJob(
                replace_plan @ ReplaceStreamJobPlan {
                    old_fragments,
                    new_fragments,
                    dispatchers,
                    init_split_assignment,
                    to_drop_state_table_ids,
                    ..
                },
            ) => {
                // Update actors and actor_dispatchers for new table fragments.
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .post_collect_job_fragments(
                        new_fragments.stream_job_id().table_id as _,
                        new_fragments.actor_ids(),
                        dispatchers,
                        init_split_assignment,
                    )
                    .await?;

                // Apply the split changes in source manager.
                barrier_manager_context
                    .source_manager
                    .handle_replace_job(
                        old_fragments,
                        new_fragments.stream_source_fragments(),
                        init_split_assignment.clone(),
                        replace_plan,
                    )
                    .await;
                barrier_manager_context
                    .hummock_manager
                    .unregister_table_ids(to_drop_state_table_ids.iter().cloned())
                    .await?;
            }

            Command::CreateSubscription {
                subscription_id, ..
            } => {
                barrier_manager_context
                    .metadata_manager
                    .catalog_controller
                    .finish_create_subscription_catalog(*subscription_id)
                    .await?
            }
            Command::DropSubscription { .. } => {}
            Command::MergeSnapshotBackfillStreamingJobs(_) => {}
        }

        Ok(())
    }
}
