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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::WorkerSlotId;
use risingwave_meta_model::StreamingParallelism;
use risingwave_pb::stream_plan::StreamActor;
use thiserror_ext::AsReport;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use super::BarrierWorkerRuntimeInfoSnapshot;
use crate::barrier::context::GlobalBarrierWorkerContextImpl;
use crate::barrier::info::InflightDatabaseInfo;
use crate::barrier::{DatabaseRuntimeInfoSnapshot, InflightSubscriptionInfo};
use crate::controller::fragment::InflightFragmentInfo;
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, StreamJobFragments, TableParallelism};
use crate::stream::{
    JobParallelismTarget, JobReschedulePolicy, JobRescheduleTarget, JobResourceGroupTarget,
    RescheduleOptions, SourceChange,
};
use crate::{model, MetaResult};

impl GlobalBarrierWorkerContextImpl {
    /// Clean catalogs for creating streaming jobs that are in foreground mode or table fragments not persisted.
    async fn clean_dirty_streaming_jobs(&self, database_id: Option<DatabaseId>) -> MetaResult<()> {
        let database_id = database_id.map(|database_id| database_id.database_id as _);
        self.metadata_manager
            .catalog_controller
            .clean_dirty_subscription(database_id)
            .await?;
        let dirty_associated_source_ids = self
            .metadata_manager
            .catalog_controller
            .clean_dirty_creating_jobs(database_id)
            .await?;

        // unregister cleaned sources.
        self.source_manager
            .apply_source_change(SourceChange::DropSource {
                dropped_source_ids: dirty_associated_source_ids,
            })
            .await;

        Ok(())
    }

    async fn purge_state_table_from_hummock(
        &self,
        all_state_table_ids: &HashSet<TableId>,
    ) -> MetaResult<()> {
        self.hummock_manager.purge(all_state_table_ids).await?;
        Ok(())
    }

    async fn list_background_mv_progress(&self) -> MetaResult<Vec<(String, StreamJobFragments)>> {
        let mgr = &self.metadata_manager;
        let mviews = mgr
            .catalog_controller
            .list_background_creating_mviews(false)
            .await?;

        try_join_all(mviews.into_iter().map(|mview| async move {
            let table_id = TableId::new(mview.table_id as _);
            let stream_job_fragments = mgr
                .catalog_controller
                .get_job_fragments_by_id(mview.table_id)
                .await?;
            assert_eq!(stream_job_fragments.stream_job_id(), table_id);
            Ok((mview.definition, stream_job_fragments))
        }))
        .await
        // If failed, enter recovery mode.
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_graph_info(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<DatabaseId, InflightDatabaseInfo>> {
        let database_id = database_id.map(|database_id| database_id.database_id as _);
        let all_actor_infos = self
            .metadata_manager
            .catalog_controller
            .load_all_actors(database_id)
            .await?;

        Ok(all_actor_infos
            .into_iter()
            .map(|(loaded_database_id, actor_infos)| {
                if let Some(database_id) = database_id {
                    assert_eq!(database_id, loaded_database_id);
                }
                (
                    DatabaseId::new(loaded_database_id as _),
                    InflightDatabaseInfo::new(actor_infos.into_iter().map(
                        |(job_id, actor_infos)| {
                            (
                                TableId::new(job_id as _),
                                actor_infos
                                    .into_iter()
                                    .map(|(fragment_id, info)| (fragment_id as _, info)),
                            )
                        },
                    )),
                )
            })
            .collect())
    }

    pub(super) async fn reload_runtime_info_impl(
        &self,
    ) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot> {
        {
            {
                {
                    self.clean_dirty_streaming_jobs(None)
                        .await
                        .context("clean dirty streaming jobs")?;

                    // Mview progress needs to be recovered.
                    tracing::info!("recovering mview progress");
                    let background_jobs = {
                        let jobs = self
                            .list_background_mv_progress()
                            .await
                            .context("recover mview progress should not fail")?;
                        let mut background_jobs = HashMap::new();
                        for (definition, stream_job_fragments) in jobs {
                            if stream_job_fragments
                                .tracking_progress_actor_ids()
                                .is_empty()
                            {
                                // If there's no tracking actor in the mview, we can finish the job directly.
                                self.metadata_manager
                                    .catalog_controller
                                    .finish_streaming_job(
                                        stream_job_fragments.stream_job_id().table_id as _,
                                        None,
                                    )
                                    .await?;
                            } else {
                                background_jobs
                                    .try_insert(
                                        stream_job_fragments.stream_job_id(),
                                        (definition, stream_job_fragments),
                                    )
                                    .expect("non-duplicate");
                            }
                        }
                        background_jobs
                    };

                    tracing::info!("recovered mview progress");

                    // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
                    let _ = self.scheduled_barriers.pre_apply_drop_cancel(None);

                    let mut active_streaming_nodes =
                        ActiveStreamingWorkerNodes::new_snapshot(self.metadata_manager.clone())
                            .await?;

                    let background_streaming_jobs = background_jobs.keys().cloned().collect_vec();
                    info!(
                        "background streaming jobs: {:?} total {}",
                        background_streaming_jobs,
                        background_streaming_jobs.len()
                    );

                    let unreschedulable_jobs = {
                        let mut unreschedulable_jobs = HashSet::new();

                        for job_id in background_streaming_jobs {
                            let scan_types = self
                                .metadata_manager
                                .get_job_backfill_scan_types(&job_id)
                                .await?;

                            if scan_types
                                .values()
                                .any(|scan_type| !scan_type.is_reschedulable())
                            {
                                unreschedulable_jobs.insert(job_id);
                            }
                        }

                        unreschedulable_jobs
                    };

                    if !unreschedulable_jobs.is_empty() {
                        tracing::info!(
                            "unreschedulable background jobs: {:?}",
                            unreschedulable_jobs
                        );
                    }

                    // Resolve actor info for recovery. If there's no actor to recover, most of the
                    // following steps will be no-op, while the compute nodes will still be reset.
                    // FIXME: Transactions should be used.
                    // TODO(error-handling): attach context to the errors and log them together, instead of inspecting everywhere.
                    let mut info = if !self.env.opts.disable_automatic_parallelism_control
                        && unreschedulable_jobs.is_empty()
                    {
                        info!("trigger offline scaling");
                        self.scale_actors(&active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "scale actors failed");
                            })?;

                        self.resolve_graph_info(None).await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    } else {
                        info!("trigger actor migration");
                        // Migrate actors in expired CN to newly joined one.
                        self.migrate_actors(&mut active_streaming_nodes)
                            .await
                            .inspect_err(|err| {
                                warn!(error = %err.as_report(), "migrate actors failed");
                            })?
                    };

                    if self.scheduled_barriers.pre_apply_drop_cancel(None) {
                        info = self.resolve_graph_info(None).await.inspect_err(|err| {
                            warn!(error = %err.as_report(), "resolve actor info failed");
                        })?
                    }

                    let info = info;

                    self.purge_state_table_from_hummock(
                        &InflightFragmentInfo::existing_table_ids(
                            info.values().flat_map(|database| database.fragment_infos()),
                        )
                        .collect(),
                    )
                    .await
                    .context("purge state table from hummock")?;

                    let state_table_committed_epochs: HashMap<_, _> = self
                        .hummock_manager
                        .on_current_version(|version| {
                            version
                                .state_table_info
                                .info()
                                .iter()
                                .map(|(table_id, info)| (*table_id, info.committed_epoch))
                                .collect()
                        })
                        .await;

                    let subscription_infos = self
                        .metadata_manager
                        .get_mv_depended_subscriptions(None)
                        .await?
                        .into_iter()
                        .map(|(database_id, mv_depended_subscriptions)| {
                            (
                                database_id,
                                InflightSubscriptionInfo {
                                    mv_depended_subscriptions,
                                },
                            )
                        })
                        .collect();

                    // update and build all actors.
                    let stream_actors = self.load_all_actors().await.inspect_err(|err| {
                        warn!(error = %err.as_report(), "update actors failed");
                    })?;

                    let background_jobs = {
                        let jobs = self
                            .list_background_mv_progress()
                            .await
                            .context("recover mview progress should not fail")?;
                        let mut background_jobs = HashMap::new();
                        for (definition, stream_job_fragments) in jobs {
                            background_jobs
                                .try_insert(
                                    stream_job_fragments.stream_job_id(),
                                    (definition, stream_job_fragments),
                                )
                                .expect("non-duplicate");
                        }
                        background_jobs
                    };

                    // get split assignments for all actors
                    let source_splits = self.source_manager.list_assignments().await;
                    Ok(BarrierWorkerRuntimeInfoSnapshot {
                        active_streaming_nodes,
                        database_fragment_infos: info,
                        state_table_committed_epochs,
                        subscription_infos,
                        stream_actors,
                        source_splits,
                        background_jobs,
                        hummock_version_stats: self.hummock_manager.get_version_stats().await,
                    })
                }
            }
        }
    }

    pub(super) async fn reload_database_runtime_info_impl(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>> {
        self.clean_dirty_streaming_jobs(Some(database_id))
            .await
            .context("clean dirty streaming jobs")?;

        // Mview progress needs to be recovered.
        tracing::info!(?database_id, "recovering mview progress of database");
        let background_jobs = self
            .list_background_mv_progress()
            .await
            .context("recover mview progress of database should not fail")?;
        tracing::info!(?database_id, "recovered mview progress");

        // This is a quick path to accelerate the process of dropping and canceling streaming jobs.
        let _ = self
            .scheduled_barriers
            .pre_apply_drop_cancel(Some(database_id));

        let info = self
            .resolve_graph_info(Some(database_id))
            .await
            .inspect_err(|err| {
                warn!(error = %err.as_report(), "resolve actor info failed");
            })?;
        assert!(info.len() <= 1);
        let Some(info) = info.into_iter().next().map(|(loaded_database_id, info)| {
            assert_eq!(loaded_database_id, database_id);
            info
        }) else {
            return Ok(None);
        };

        let background_jobs = {
            let jobs = background_jobs;
            let mut background_jobs = HashMap::new();
            for (definition, stream_job_fragments) in jobs {
                if !info.contains_job(stream_job_fragments.stream_job_id()) {
                    continue;
                }
                if stream_job_fragments
                    .tracking_progress_actor_ids()
                    .is_empty()
                {
                    // If there's no tracking actor in the mview, we can finish the job directly.
                    self.metadata_manager
                        .catalog_controller
                        .finish_streaming_job(
                            stream_job_fragments.stream_job_id().table_id as _,
                            None,
                        )
                        .await?;
                } else {
                    background_jobs
                        .try_insert(
                            stream_job_fragments.stream_job_id(),
                            (definition, stream_job_fragments),
                        )
                        .expect("non-duplicate");
                }
            }
            background_jobs
        };

        let state_table_committed_epochs: HashMap<_, _> = self
            .hummock_manager
            .on_current_version(|version| {
                version
                    .state_table_info
                    .info()
                    .iter()
                    .map(|(table_id, info)| (*table_id, info.committed_epoch))
                    .collect()
            })
            .await;

        let subscription_infos = self
            .metadata_manager
            .get_mv_depended_subscriptions(Some(database_id))
            .await?;
        assert!(subscription_infos.len() <= 1);
        let mv_depended_subscriptions = subscription_infos
            .into_iter()
            .next()
            .map(|(loaded_database_id, subscriptions)| {
                assert_eq!(loaded_database_id, database_id);
                subscriptions
            })
            .unwrap_or_default();
        let subscription_info = InflightSubscriptionInfo {
            mv_depended_subscriptions,
        };

        // update and build all actors.
        let stream_actors = self.load_all_actors().await.inspect_err(|err| {
            warn!(error = %err.as_report(), "update actors failed");
        })?;

        // get split assignments for all actors
        let source_splits = self.source_manager.list_assignments().await;
        Ok(Some(DatabaseRuntimeInfoSnapshot {
            database_fragment_info: info,
            state_table_committed_epochs,
            subscription_info,
            stream_actors,
            source_splits,
            background_jobs,
        }))
    }
}

impl GlobalBarrierWorkerContextImpl {
    // Migration timeout.
    const RECOVERY_FORCE_MIGRATION_TIMEOUT: Duration = Duration::from_secs(300);

    /// Migrate actors in expired CNs to newly joined ones, return true if any actor is migrated.
    async fn migrate_actors(
        &self,
        active_nodes: &mut ActiveStreamingWorkerNodes,
    ) -> MetaResult<HashMap<DatabaseId, InflightDatabaseInfo>> {
        let mgr = &self.metadata_manager;

        // all worker slots used by actors
        let all_inuse_worker_slots: HashSet<_> = mgr
            .catalog_controller
            .all_inuse_worker_slots()
            .await?
            .into_iter()
            .collect();

        let active_worker_slots: HashSet<_> = active_nodes
            .current()
            .values()
            .flat_map(|node| {
                (0..node.compute_node_parallelism()).map(|idx| WorkerSlotId::new(node.id, idx))
            })
            .collect();

        let expired_worker_slots: BTreeSet<_> = all_inuse_worker_slots
            .difference(&active_worker_slots)
            .cloned()
            .collect();

        if expired_worker_slots.is_empty() {
            info!("no expired worker slots, skipping.");
            return self.resolve_graph_info(None).await;
        }

        info!("start migrate actors.");
        let mut to_migrate_worker_slots = expired_worker_slots.into_iter().rev().collect_vec();
        info!("got to migrate worker slots {:#?}", to_migrate_worker_slots);

        let mut inuse_worker_slots: HashSet<_> = all_inuse_worker_slots
            .intersection(&active_worker_slots)
            .cloned()
            .collect();

        let start = Instant::now();
        let mut plan = HashMap::new();
        'discovery: while !to_migrate_worker_slots.is_empty() {
            let mut new_worker_slots = active_nodes
                .current()
                .values()
                .flat_map(|worker| {
                    (0..worker.compute_node_parallelism())
                        .map(move |i| WorkerSlotId::new(worker.id, i as _))
                })
                .collect_vec();

            new_worker_slots.retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));
            let to_migration_size = to_migrate_worker_slots.len();
            let mut available_size = new_worker_slots.len();

            if available_size < to_migration_size
                && start.elapsed() > Self::RECOVERY_FORCE_MIGRATION_TIMEOUT
            {
                let mut factor = 2;

                while available_size < to_migration_size {
                    let mut extended_worker_slots = active_nodes
                        .current()
                        .values()
                        .flat_map(|worker| {
                            (0..worker.compute_node_parallelism() * factor)
                                .map(move |i| WorkerSlotId::new(worker.id, i as _))
                        })
                        .collect_vec();

                    extended_worker_slots
                        .retain(|worker_slot| !inuse_worker_slots.contains(worker_slot));

                    extended_worker_slots.sort_by(|a, b| {
                        a.slot_idx()
                            .cmp(&b.slot_idx())
                            .then(a.worker_id().cmp(&b.worker_id()))
                    });

                    available_size = extended_worker_slots.len();
                    new_worker_slots = extended_worker_slots;

                    factor *= 2;
                }

                tracing::info!(
                    "migration timed out, extending worker slots to {:?} by factor {}",
                    new_worker_slots,
                    factor,
                );
            }

            if !new_worker_slots.is_empty() {
                debug!("new worker slots found: {:#?}", new_worker_slots);
                for target_worker_slot in new_worker_slots {
                    if let Some(from) = to_migrate_worker_slots.pop() {
                        debug!(
                            "plan to migrate from worker slot {} to {}",
                            from, target_worker_slot
                        );
                        inuse_worker_slots.insert(target_worker_slot);
                        plan.insert(from, target_worker_slot);
                    } else {
                        break 'discovery;
                    }
                }
            }

            if to_migrate_worker_slots.is_empty() {
                break;
            }

            // wait to get newly joined CN
            let changed = active_nodes
                .wait_changed(
                    Duration::from_millis(5000),
                    Self::RECOVERY_FORCE_MIGRATION_TIMEOUT,
                    |active_nodes| {
                        let current_nodes = active_nodes
                            .current()
                            .values()
                            .map(|node| (node.id, &node.host, node.compute_node_parallelism()))
                            .collect_vec();
                        warn!(
                            current_nodes = ?current_nodes,
                            "waiting for new workers to join, elapsed: {}s",
                            start.elapsed().as_secs()
                        );
                    },
                )
                .await;
            warn!(?changed, "get worker changed or timed out. Retry migrate");
        }

        info!("migration plan {:?}", plan);

        mgr.catalog_controller.migrate_actors(plan).await?;

        info!("migrate actors succeed.");

        self.resolve_graph_info(None).await
    }

    async fn scale_actors(&self, active_nodes: &ActiveStreamingWorkerNodes) -> MetaResult<()> {
        let Ok(_guard) = self.scale_controller.reschedule_lock.try_write() else {
            return Err(anyhow!("scale_actors failed to acquire reschedule_lock").into());
        };

        match self.scale_controller.integrity_check().await {
            Ok(_) => {
                info!("integrity check passed");
            }
            Err(_) => {
                bail!("integrity check failed");
            }
        }

        let mgr = &self.metadata_manager;

        debug!("start resetting actors distribution");

        let available_workers: HashMap<_, _> = active_nodes
            .current()
            .values()
            .filter(|worker| worker.is_streaming_schedulable())
            .map(|worker| (worker.id, worker.clone()))
            .collect();

        info!(
            "target worker ids for offline scaling: {:?}",
            available_workers
        );

        let available_parallelism = active_nodes
            .current()
            .values()
            .map(|worker_node| worker_node.compute_node_parallelism())
            .sum();

        let mut table_parallelisms = HashMap::new();

        let reschedule_targets: HashMap<_, _> = {
            let streaming_parallelisms = mgr
                .catalog_controller
                .get_all_streaming_parallelisms()
                .await?;

            let mut result = HashMap::new();

            for (object_id, streaming_parallelism) in streaming_parallelisms {
                let actual_fragment_parallelism = mgr
                    .catalog_controller
                    .get_actual_job_fragment_parallelism(object_id)
                    .await?;

                let table_parallelism = match streaming_parallelism {
                    StreamingParallelism::Adaptive => model::TableParallelism::Adaptive,
                    StreamingParallelism::Custom => model::TableParallelism::Custom,
                    StreamingParallelism::Fixed(n) => model::TableParallelism::Fixed(n as _),
                };

                let target_parallelism = Self::derive_target_parallelism(
                    available_parallelism,
                    table_parallelism,
                    actual_fragment_parallelism,
                    self.env.opts.default_parallelism,
                );

                if target_parallelism != table_parallelism {
                    tracing::info!(
                        "resetting table {} parallelism from {:?} to {:?}",
                        object_id,
                        table_parallelism,
                        target_parallelism
                    );
                }

                table_parallelisms.insert(TableId::new(object_id as u32), target_parallelism);

                let parallelism_change = JobParallelismTarget::Update(target_parallelism);

                result.insert(
                    object_id as u32,
                    JobRescheduleTarget {
                        parallelism: parallelism_change,
                        resource_group: JobResourceGroupTarget::Keep,
                    },
                );
            }

            result
        };

        info!(
            "target table parallelisms for offline scaling: {:?}",
            reschedule_targets
        );

        let plan = self
            .scale_controller
            .generate_job_reschedule_plan(JobReschedulePolicy {
                targets: reschedule_targets,
            })
            .await?;

        let mut compared_table_parallelisms = table_parallelisms.clone();

        // skip reschedule if no reschedule is generated.
        let reschedule_fragment = if plan.reschedules.is_empty() {
            HashMap::new()
        } else {
            self.scale_controller
                .analyze_reschedule_plan(
                    plan.reschedules,
                    RescheduleOptions {
                        resolve_no_shuffle_upstream: true,
                        skip_create_new_actors: true,
                    },
                    &mut compared_table_parallelisms,
                )
                .await?
        };

        // Because custom parallelism doesn't exist, this function won't result in a no-shuffle rewrite for table parallelisms.
        debug_assert_eq!(compared_table_parallelisms, table_parallelisms);

        info!("post applying reschedule for offline scaling");

        if let Err(e) = self
            .scale_controller
            .post_apply_reschedule(&reschedule_fragment, &plan.post_updates)
            .await
        {
            tracing::error!(
                error = %e.as_report(),
                "failed to apply reschedule for offline scaling in recovery",
            );

            return Err(e);
        }

        info!("scaling actors succeed.");
        Ok(())
    }

    // We infer the new parallelism strategy based on the prior level of parallelism of the table.
    // If the parallelism strategy is Fixed or Auto, we won't make any modifications.
    // For Custom, we'll assess the parallelism of the core fragment;
    // if the parallelism is higher than the currently available parallelism, we'll set it to Adaptive.
    // If it's lower, we'll set it to Fixed.
    // If it was previously set to Adaptive, but the default_parallelism in the configuration isn’t Full,
    // and it matches the actual fragment parallelism, in this case, it will be handled by downgrading to Fixed.
    fn derive_target_parallelism(
        available_parallelism: usize,
        assigned_parallelism: TableParallelism,
        actual_fragment_parallelism: Option<usize>,
        default_parallelism: DefaultParallelism,
    ) -> TableParallelism {
        match assigned_parallelism {
            TableParallelism::Custom => {
                if let Some(fragment_parallelism) = actual_fragment_parallelism {
                    if fragment_parallelism >= available_parallelism {
                        TableParallelism::Adaptive
                    } else {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                } else {
                    TableParallelism::Adaptive
                }
            }
            TableParallelism::Adaptive => {
                match (default_parallelism, actual_fragment_parallelism) {
                    (DefaultParallelism::Default(n), Some(fragment_parallelism))
                        if fragment_parallelism == n.get() =>
                    {
                        TableParallelism::Fixed(fragment_parallelism)
                    }
                    _ => TableParallelism::Adaptive,
                }
            }
            _ => assigned_parallelism,
        }
    }

    /// Update all actors in compute nodes.
    async fn load_all_actors(&self) -> MetaResult<HashMap<ActorId, StreamActor>> {
        self.metadata_manager.all_active_actors().await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    #[test]
    fn test_derive_target_parallelism() {
        // total 10, assigned custom, actual 5, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(5),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 10, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(10),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned custom, actual 11, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                Some(11),
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned fixed(5), actual _, default full -> fixed(5)
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Custom,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual _, default full -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                None,
                DefaultParallelism::Full,
            )
        );

        // total 10, assigned adaptive, actual 5, default 5 -> fixed(5)
        assert_eq!(
            TableParallelism::Fixed(5),
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(5),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );

        // total 10, assigned adaptive, actual 6, default 5 -> adaptive
        assert_eq!(
            TableParallelism::Adaptive,
            GlobalBarrierWorkerContextImpl::derive_target_parallelism(
                10,
                TableParallelism::Adaptive,
                Some(6),
                DefaultParallelism::Default(NonZeroUsize::new(5).unwrap()),
            )
        );
    }
}
