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

use std::collections::HashMap;
use std::mem::replace;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use itertools::Itertools;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::Recovery;
use risingwave_pb::stream_service::streaming_control_stream_response::Response;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tonic::Status;
use tracing::{debug, error, info, warn, Instrument};

use crate::barrier::checkpoint::{CheckpointControl, CheckpointControlEvent};
use crate::barrier::complete_task::{BarrierCompleteOutput, CompletingTask};
use crate::barrier::context::{GlobalBarrierWorkerContext, GlobalBarrierWorkerContextImpl};
use crate::barrier::rpc::{merge_node_rpc_errors, ControlStreamManager};
use crate::barrier::schedule::PeriodicBarriers;
use crate::barrier::{
    schedule, BarrierManagerRequest, BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot,
    RecoveryReason,
};
use crate::error::MetaErrorInner;
use crate::hummock::HummockManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{ScaleControllerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

/// [`crate::barrier::worker::GlobalBarrierWorker`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::worker::GlobalBarrierWorker`] provides a set of interfaces like a state machine,
/// accepting [`crate::barrier::command::Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`crate::barrier::command::Command`].
pub(super) struct GlobalBarrierWorker<C> {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    periodic_barriers: PeriodicBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    pub(super) context: Arc<C>,

    env: MetaSrvEnv,

    checkpoint_control: CheckpointControl,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    sink_manager: SinkCoordinatorManager,

    control_stream_manager: ControlStreamManager,
}

impl GlobalBarrierWorker<GlobalBarrierWorkerContextImpl> {
    /// Create a new [`crate::barrier::worker::GlobalBarrierWorker`].
    pub async fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let active_streaming_nodes =
            ActiveStreamingWorkerNodes::uninitialized(metadata_manager.clone());

        let status = Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting)));

        let context = Arc::new(GlobalBarrierWorkerContextImpl::new(
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            env.clone(),
        ));

        let control_stream_manager = ControlStreamManager::new(env.clone());

        let checkpoint_frequency = env.system_params_reader().await.checkpoint_frequency() as _;
        let interval =
            Duration::from_millis(env.system_params_reader().await.barrier_interval_ms() as u64);
        let periodic_barriers = PeriodicBarriers::new(interval, checkpoint_frequency);
        tracing::info!(
            "Starting barrier scheduler with: checkpoint_frequency={:?}",
            checkpoint_frequency,
        );

        Self {
            enable_recovery,
            periodic_barriers,
            in_flight_barrier_nums,
            context,
            env,
            checkpoint_control: CheckpointControl::default(),
            completing_task: CompletingTask::None,
            request_rx,
            active_streaming_nodes,
            sink_manager,
            control_stream_manager,
        }
    }

    pub fn start(self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Check whether we should pause on bootstrap from the system parameter and reset it.
    async fn take_pause_on_bootstrap(&mut self) -> MetaResult<bool> {
        let paused = self
            .env
            .system_params_reader()
            .await
            .pause_on_next_bootstrap();
        if paused {
            warn!(
                "The cluster will bootstrap with all data sources paused as specified by the system parameter `{}`. \
                 It will now be reset to `false`. \
                 To resume the data sources, either restart the cluster again or use `risectl meta resume`.",
                PAUSE_ON_NEXT_BOOTSTRAP_KEY
            );
            self.env
                .system_params_manager_impl_ref()
                .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                .await?;
        }
        Ok(paused)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(mut self, shutdown_rx: Receiver<()>) {
        tracing::info!(
            "Starting barrier manager with: enable_recovery={}, in_flight_barrier_nums={}",
            self.enable_recovery,
            self.in_flight_barrier_nums,
        );

        if !self.enable_recovery {
            let job_exist = self
                .context
                .metadata_manager
                .catalog_controller
                .has_any_streaming_jobs()
                .await
                .unwrap();
            if job_exist {
                panic!(
                    "Some streaming jobs already exist in meta, please start with recovery enabled \
                or clean up the metadata using `./risedev clean-data`"
                );
            }
        }

        {
            // Bootstrap recovery. Here we simply trigger a recovery process to achieve the
            // consistency.
            // Even if there's no actor to recover, we still go through the recovery process to
            // inject the first `Initial` barrier.
            let span = tracing::info_span!("bootstrap_recovery");
            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "normal_recovery",
                0,
                None,
                None,
                None,
            );

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);

            self.recovery(paused, None, RecoveryReason::Bootstrap)
                .instrument(span)
                .await;
        }

        self.run_inner(shutdown_rx).await
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    async fn run_inner(mut self, mut shutdown_rx: Receiver<()>) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;

        // Start the event loop.
        loop {
            tokio::select! {
                biased;

                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    break;
                }

                request = self.request_rx.recv() => {
                    if let Some(request) = request {
                        match request {
                            BarrierManagerRequest::GetDdlProgress(result_tx) => {
                                let progress = self.checkpoint_control.gen_ddl_progress();
                                if result_tx.send(progress).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }// Handle adhoc recovery triggered by user.
                            BarrierManagerRequest::AdhocRecovery(sender) => {
                                self.adhoc_recovery().await;
                                if sender.send(()).is_err() {
                                    warn!("failed to notify finish of adhoc recovery");
                                }
                            }
                        }
                    } else {
                        tracing::info!("end of request stream. meta node may be shutting down. Stop global barrier manager");
                        return;
                    }
                }

                changed_worker = self.active_streaming_nodes.changed() => {
                    #[cfg(debug_assertions)]
                    {
                        self.active_streaming_nodes.validate_change().await;
                    }

                    info!(?changed_worker, "worker changed");

                    if let ActiveStreamingWorkerChange::Add(node) | ActiveStreamingWorkerChange::Update(node) = changed_worker {
                        self.control_stream_manager.add_worker(node, self.checkpoint_control.inflight_infos(), &*self.context).await;
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    if let LocalNotification::SystemParamsChange(p) = notification {
                        {
                            self.periodic_barriers.set_min_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.periodic_barriers
                                .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                        }
                    }
                }
                complete_result = self
                    .completing_task
                    .next_completed_barrier(
                        &mut self.periodic_barriers,
                        &mut self.checkpoint_control,
                        &mut self.control_stream_manager,
                        &self.context,
                        &self.env,
                ) => {
                    match complete_result {
                        Ok(output) => {
                            self.checkpoint_control.ack_completed(output);
                        }
                        Err(e) => {
                            self.failure_recovery(e).await;
                        }
                    }
                },
                event = self.checkpoint_control.next_event() => {
                    let result: MetaResult<()> = try {
                        match event {
                            CheckpointControlEvent::EnteringInitializing(entering_initializing) => {
                                let database_id = entering_initializing.database_id();
                                let error = merge_node_rpc_errors(&format!("database {} reset", database_id), entering_initializing.action.0.iter().filter_map(|(worker_id, resp)| {
                                    resp.root_err.as_ref().map(|root_err| {
                                        (*worker_id, ScoredError {
                                            error: Status::internal(&root_err.err_msg),
                                            score: Score(root_err.score)
                                        })
                                    })
                                }));
                                Self::report_collect_failure(&self.env, &error);
                                if let Some(runtime_info) = self.context.reload_database_runtime_info(database_id).await? {
                                    runtime_info.validate(database_id, &self.active_streaming_nodes).inspect_err(|e| {
                                        warn!(database_id = database_id.database_id, err = ?e.as_report(), ?runtime_info, "reloaded database runtime info failed to validate");
                                    })?;
                                    entering_initializing.enter(runtime_info, &mut self.control_stream_manager);
                                } else {
                                    info!(database_id = database_id.database_id, "database removed after reloading empty runtime info");
                                    entering_initializing.remove();
                                }
                            }
                            CheckpointControlEvent::EnteringRunning(entering_running) => {
                                self.context.mark_ready(Some(entering_running.database_id()));
                                entering_running.enter();
                            }
                        }
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                (worker_id, resp_result) = self.control_stream_manager.next_response() => {
                     let resp = match resp_result {
                        Err(e) => {
                            if self.checkpoint_control.is_failed_at_worker_err(worker_id) {
                                let errors = self.control_stream_manager.collect_errors(worker_id, e).await;
                                let err = merge_node_rpc_errors("get error from control stream", errors);
                                Self::report_collect_failure(&self.env, &err);
                                self.failure_recovery(err).await;
                            }  else {
                                warn!(worker_id, "no barrier to collect from worker, ignore err");
                            }
                            continue;
                        }
                        Ok(resp) => resp,
                    };
                    let result: MetaResult<()> = try {
                        match resp {
                            Response::CompleteBarrier(resp) => {
                                self.checkpoint_control.barrier_collected(resp, &mut self.control_stream_manager)?;
                            },
                            Response::ReportDatabaseFailure(resp) => {
                                if !self.enable_recovery {
                                    panic!("database failure reported but recovery not enabled: {:?}", resp)
                                }
                                if let Some(entering_recovery) = self.checkpoint_control.on_report_failure(resp, &mut self.control_stream_manager) {
                                    let database_id = entering_recovery.database_id();
                                    warn!(database_id = database_id.database_id, "database entering recovery");
                                    self.context.abort_and_mark_blocked(Some(database_id), RecoveryReason::Failover(anyhow!("reset database: {}", database_id).into()));
                                    // TODO: add log on blocking time
                                    let output = self.completing_task.wait_completing_task().await?;
                                    entering_recovery.enter(output, &mut self.control_stream_manager);
                                }
                            }
                            Response::ResetDatabase(resp) => {
                                self.checkpoint_control.on_reset_database_resp(worker_id, resp);
                            }
                            other @ Response::Init(_) | other @ Response::Shutdown(_) => {
                                Err(anyhow!("get expected response: {:?}", other))?;
                            }
                        }
                    };
                    if let Err(e) = result {
                        self.failure_recovery(e).await;
                    }
                }
                new_barrier = self.periodic_barriers.next_barrier(&*self.context),
                    if self
                        .checkpoint_control
                        .can_inject_barrier(self.in_flight_barrier_nums) => {
                    if let Err(e) = self.checkpoint_control.handle_new_barrier(new_barrier, &mut self.control_stream_manager) {
                        self.failure_recovery(e).await;
                    }
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_on_err(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        let is_err = match replace(&mut self.completing_task, CompletingTask::None) {
            CompletingTask::None => false,
            CompletingTask::Completing {
                epochs_to_ack,
                join_handle,
                ..
            } => {
                info!("waiting for completing command to finish in recovery");
                match join_handle.await {
                    Err(e) => {
                        warn!(err = ?e.as_report(), "failed to join completing task");
                        true
                    }
                    Ok(Err(e)) => {
                        warn!(err = ?e.as_report(), "failed to complete barrier during clear");
                        true
                    }
                    Ok(Ok(hummock_version_stats)) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                epochs_to_ack,
                                hummock_version_stats,
                            });
                        false
                    }
                }
            }
            CompletingTask::Err(_) => true,
        };
        if !is_err {
            // continue to finish the pending collected barrier.
            while let Some(task) = self.checkpoint_control.next_complete_barrier_task(None) {
                let epochs_to_ack = task.epochs_to_ack();
                match task
                    .complete_barrier(&*self.context, self.env.clone())
                    .await
                {
                    Ok(hummock_version_stats) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                epochs_to_ack,
                                hummock_version_stats,
                            });
                    }
                    Err(e) => {
                        error!(
                            err = ?e.as_report(),
                            "failed to complete barrier during recovery"
                        );
                        break;
                    }
                }
            }
        }
        self.checkpoint_control.clear_on_err(err);
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Set barrier manager status.
    async fn failure_recovery(&mut self, err: MetaError) {
        self.clear_on_err(&err).await;

        if self.enable_recovery {
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
            );

            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "failure_recovery",
                0,
                None,
                None,
                None,
            );

            let reason = RecoveryReason::Failover(err.clone());

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(false, Some(err), reason)
                .instrument(span)
                .await;
        } else {
            panic!(
                "a streaming error occurred while recovery is disabled, aborting: {:?}",
                err.as_report()
            );
        }
    }

    async fn adhoc_recovery(&mut self) {
        let err = MetaErrorInner::AdhocRecovery.into();
        self.clear_on_err(&err).await;

        let span = tracing::info_span!(
            "adhoc_recovery",
            error = %err.as_report(),
        );

        crate::telemetry::report_event(
            risingwave_pb::telemetry::TelemetryEventStage::Recovery,
            "adhoc_recovery",
            0,
            None,
            None,
            None,
        );

        // No need to clean dirty tables for barrier recovery,
        // The foreground stream job should cleanup their own tables.
        self.recovery(false, Some(err), RecoveryReason::Adhoc)
            .instrument(span)
            .await;
    }
}

impl<C> GlobalBarrierWorker<C> {
    /// Send barrier-complete-rpc and wait for responses from all CNs
    pub(super) fn report_collect_failure(env: &MetaSrvEnv, error: &MetaError) {
        // Record failure in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventCollectBarrierFail {
            error: error.to_report_string(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::CollectBarrierFail(event)]);
    }
}

mod retry_strategy {
    use std::time::Duration;

    use tokio_retry::strategy::{jitter, ExponentialBackoff};

    // Retry base interval in milliseconds.
    const RECOVERY_RETRY_BASE_INTERVAL: u64 = 20;
    // Retry max interval.
    const RECOVERY_RETRY_MAX_INTERVAL: Duration = Duration::from_secs(5);

    mod retry_backoff_future {
        use std::future::Future;
        use std::time::Duration;

        use tokio::time::sleep;

        pub(crate) type RetryBackoffFuture = impl Future<Output = ()> + Unpin + Send + 'static;
        pub(super) fn get_retry_backoff_future(duration: Duration) -> RetryBackoffFuture {
            Box::pin(sleep(duration))
        }
    }
    pub(crate) use retry_backoff_future::*;

    pub(crate) type RetryBackoffStrategy =
        impl Iterator<Item = RetryBackoffFuture> + Send + 'static;

    #[inline(always)]
    /// Initialize a retry strategy for operation in recovery.
    pub(crate) fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(RECOVERY_RETRY_BASE_INTERVAL)
            .max_delay(RECOVERY_RETRY_MAX_INTERVAL)
            .map(jitter)
    }

    pub(crate) fn get_retry_backoff_strategy() -> RetryBackoffStrategy {
        get_retry_strategy().map(get_retry_backoff_future)
    }
}

pub(crate) use retry_strategy::*;
use risingwave_common::error::tonic::extra::{Score, ScoredError};

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Recovery the whole cluster from the latest epoch.
    ///
    /// If `paused_reason` is `Some`, all data sources (including connectors and DMLs) will be
    /// immediately paused after recovery, until the user manually resume them either by restarting
    /// the cluster or `risectl` command. Used for debugging purpose.
    ///
    /// Returns the new state of the barrier manager after recovery.
    pub async fn recovery(
        &mut self,
        is_paused: bool,
        err: Option<MetaError>,
        recovery_reason: RecoveryReason,
    ) {
        self.context.abort_and_mark_blocked(None, recovery_reason);
        // Clear all control streams to release resources (connections to compute nodes) first.
        self.control_stream_manager.clear();

        self.recovery_inner(is_paused, err).await;
        self.context.mark_ready(None);
    }

    async fn recovery_inner(&mut self, is_paused: bool, err: Option<MetaError>) {
        tracing::info!("recovery start!");
        let retry_strategy = get_retry_strategy();

        // We take retry into consideration because this is the latency user sees for a cluster to
        // get recovered.
        let recovery_timer = GLOBAL_META_METRICS.recovery_latency.start_timer();

        let new_state = tokio_retry::Retry::spawn(retry_strategy, || async {
            if let Some(err) = &err {
                self.context.notify_creating_job_failed(err).await;
            };
            let runtime_info_snapshot = self
                .context
                .reload_runtime_info()
                .await?;
            runtime_info_snapshot.validate().inspect_err(|e| {
                warn!(err = ?e.as_report(), ?runtime_info_snapshot, "reloaded runtime info failed to validate");
            })?;
            let BarrierWorkerRuntimeInfoSnapshot {
                active_streaming_nodes,
                database_fragment_infos,
                mut state_table_committed_epochs,
                mut subscription_infos,
                mut stream_actors,
                mut source_splits,
                mut background_jobs,
                hummock_version_stats,
            } = runtime_info_snapshot;

            self.sink_manager.reset().await;

            let mut control_stream_manager = ControlStreamManager::new(self.env.clone());
            let reset_start_time = Instant::now();
            control_stream_manager
                .reset(
                    active_streaming_nodes.current(),
                    &*self.context,
                )
                .await
                .inspect_err(|err| {
                    warn!(error = %err.as_report(), "reset compute nodes failed");
                })?;
            info!(elapsed=?reset_start_time.elapsed(), "control stream reset");

            let recovery_result: MetaResult<_> = try {
                let mut collected_databases = HashMap::new();
                let mut collecting_databases = HashMap::new();
                for (database_id, info) in database_fragment_infos {
                    control_stream_manager.add_partial_graph(database_id, None)?;
                    let (node_to_collect, database, prev_epoch) = control_stream_manager.inject_database_initial_barrier(
                        database_id,
                        info,
                        &mut state_table_committed_epochs,
                        &mut stream_actors,
                        &mut source_splits,
                        &mut background_jobs,
                        subscription_infos.remove(&database_id).unwrap_or_default(),
                        is_paused,
                        &hummock_version_stats,
                    )?;
                    if !node_to_collect.is_empty() {
                        assert!(collecting_databases.insert(database_id, (node_to_collect, database, prev_epoch)).is_none());
                    } else {
                        warn!(database_id = database_id.database_id, "database has no node to inject initial barrier");
                        assert!(collected_databases.insert(database_id, database).is_none());
                    }
                }
                while !collecting_databases.is_empty() {
                    let (worker_id, result) =
                        control_stream_manager.next_response().await;
                    let resp = result?;
                    let resp = match resp {
                        Response::CompleteBarrier(resp) => {
                            resp
                        }
                        other => {
                            return Err(anyhow!("expect Response::CollectBarrier but get {:?}", other).into());
                        }
                    };
                    let database_id = DatabaseId::new(resp.database_id);
                    let (node_to_collect, _, prev_epoch) = collecting_databases.get_mut(&database_id).expect("should exist");
                    assert_eq!(resp.epoch, *prev_epoch);
                    assert!(node_to_collect.remove(&worker_id));
                    if node_to_collect.is_empty() {
                        let (_, database, _) = collecting_databases.remove(&database_id).expect("should exist");
                        assert!(collected_databases.insert(database_id, database).is_none());
                    }
                }
                debug!("collected initial barrier");
                if !stream_actors.is_empty() {
                    warn!(actor_ids = ?stream_actors.keys().collect_vec(), "unused stream actors in recovery");
                }
                if !source_splits.is_empty() {
                    warn!(actor_ids = ?source_splits.keys().collect_vec(), "unused actor source splits in recovery");
                }
                if !background_jobs.is_empty() {
                    warn!(job_ids = ?background_jobs.keys().collect_vec(), "unused recovered background mview in recovery");
                }
                if !subscription_infos.is_empty() {
                    warn!(?subscription_infos, "unused subscription infos in recovery");
                }
                if !state_table_committed_epochs.is_empty() {
                    warn!(?state_table_committed_epochs, "unused state table committed epoch in recovery");
                }
                (
                    active_streaming_nodes,
                    control_stream_manager,
                    CheckpointControl::new(
                        collected_databases,
                        hummock_version_stats,
                    ),
                )
            };
            if recovery_result.is_err() {
                GLOBAL_META_METRICS.recovery_failure_cnt.inc();
            }
            recovery_result
        })
            .instrument(tracing::info_span!("recovery_attempt"))
            .await
            .expect("Retry until recovery success.");

        recovery_timer.observe_duration();

        (
            self.active_streaming_nodes,
            self.control_stream_manager,
            self.checkpoint_control,
        ) = new_state;

        tracing::info!("recovery success");

        self.env
            .notification_manager()
            .notify_frontend_without_version(Operation::Update, Info::Recovery(Recovery {}));
    }
}
