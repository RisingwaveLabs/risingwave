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

mod compaction_executor;
mod compaction_filter;
pub mod compaction_utils;
use risingwave_hummock_sdk::compact_task::{CompactTask, ValidationTask};
use risingwave_pb::compactor::{dispatch_compaction_task_request, DispatchCompactionTaskRequest};
use risingwave_pb::hummock::report_compaction_task_request::Event as ReportCompactionTaskEvent;
use risingwave_rpc_client::GrpcCompactorProxyClient;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tonic::Request;

pub mod compactor_runner;
mod context;
pub mod fast_compactor_runner;
mod iterator;
mod shared_buffer_compact;
pub(super) mod task_progress;

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use await_tree::InstrumentAwait;
pub use compaction_executor::CompactionExecutor;
pub use compaction_filter::{
    CompactionFilter, DummyCompactionFilter, MultiCompactionFilter, StateCleanUpCompactionFilter,
    TtlCompactionFilter,
};
pub use context::{
    await_tree_key, new_compaction_await_tree_reg_ref, CompactionAwaitTreeRegRef, CompactorContext,
};
use futures::{pin_mut, StreamExt};
pub use iterator::{ConcatSstableIterator, SstableStreamIterator};
use more_asserts::assert_ge;
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStatsMap};
use risingwave_hummock_sdk::{
    compact_task_to_string, HummockCompactionTaskId, HummockSstableObjectId, LocalSstableInfo,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::subscribe_compaction_event_request::{
    Event as RequestEvent, HeartBeat, PullTask, ReportTask,
};
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::{
    CompactTaskProgress, ReportCompactionTaskRequest, SubscribeCompactionEventRequest,
    SubscribeCompactionEventResponse,
};
use risingwave_rpc_client::HummockMetaClient;
pub use shared_buffer_compact::{compact, merge_imms_in_memory};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub use self::compaction_utils::{
    check_compaction_result, check_flush_result, CompactionStatistics, RemoteBuilderFactory,
    TaskConfig,
};
pub use self::task_progress::TaskProgress;
use super::multi_builder::CapacitySplitTableBuilder;
use super::{
    GetObjectId, HummockResult, SstableBuilderOptions, SstableObjectIdManager, Xor16FilterBuilder,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgentRef, CompactionCatalogManager, CompactionCatalogManagerRef,
};
use crate::hummock::compactor::compaction_utils::calculate_task_parallelism;
use crate::hummock::compactor::compactor_runner::{compact_and_build_sst, compact_done};
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::{
    validate_ssts, BlockedXor16FilterBuilder, FilterBuilder, SharedComapctorObjectIdManager,
    SstableWriterFactory, UnifiedSstableWriterFactory,
};
use crate::monitor::CompactorMetrics;

/// Implementation of Hummock compaction.
pub struct Compactor {
    /// The context of the compactor.
    context: CompactorContext,
    object_id_getter: Box<dyn GetObjectId>,
    task_config: TaskConfig,
    options: SstableBuilderOptions,
    get_id_time: Arc<AtomicU64>,
}

pub type CompactOutput = (usize, Vec<LocalSstableInfo>, CompactionStatistics);

impl Compactor {
    /// Create a new compactor.
    pub fn new(
        context: CompactorContext,
        options: SstableBuilderOptions,
        task_config: TaskConfig,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        Self {
            context,
            options,
            task_config,
            get_id_time: Arc::new(AtomicU64::new(0)),
            object_id_getter,
        }
    }

    /// Compact the given key range and merge iterator.
    /// Upon a successful return, the built SSTs are already uploaded to object store.
    ///
    /// `task_progress` is only used for tasks on the compactor.
    async fn compact_key_range(
        &self,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        task_id: Option<HummockCompactionTaskId>,
        split_index: Option<usize>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        // Monitor time cost building shared buffer to SSTs.
        let compact_timer = if self.context.is_share_buffer_compact {
            self.context
                .compactor_metrics
                .write_build_l0_sst_duration
                .start_timer()
        } else {
            self.context
                .compactor_metrics
                .compact_sst_duration
                .start_timer()
        };

        let (split_table_outputs, table_stats_map) = {
            let factory = UnifiedSstableWriterFactory::new(self.context.sstable_store.clone());
            if self.task_config.use_block_based_filter {
                self.compact_key_range_impl::<_, BlockedXor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    compaction_catalog_agent_ref,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            } else {
                self.compact_key_range_impl::<_, Xor16FilterBuilder>(
                    factory,
                    iter,
                    compaction_filter,
                    compaction_catalog_agent_ref,
                    task_progress.clone(),
                    self.object_id_getter.clone(),
                )
                .verbose_instrument_await("compact")
                .await?
            }
        };

        compact_timer.observe_duration();

        Self::report_progress(
            self.context.compactor_metrics.clone(),
            task_progress,
            &split_table_outputs,
            self.context.is_share_buffer_compact,
        );

        self.context
            .compactor_metrics
            .get_table_id_total_time_duration
            .observe(self.get_id_time.load(Ordering::Relaxed) as f64 / 1000.0 / 1000.0);

        debug_assert!(split_table_outputs
            .iter()
            .all(|table_info| table_info.sst_info.table_ids.is_sorted()));

        if task_id.is_some() {
            // skip shared buffer compaction
            tracing::info!(
                "Finish Task {:?} split_index {:?} sst count {}",
                task_id,
                split_index,
                split_table_outputs.len()
            );
        }
        Ok((split_table_outputs, table_stats_map))
    }

    pub fn report_progress(
        metrics: Arc<CompactorMetrics>,
        task_progress: Option<Arc<TaskProgress>>,
        ssts: &Vec<LocalSstableInfo>,
        is_share_buffer_compact: bool,
    ) {
        for sst_info in ssts {
            let sst_size = sst_info.file_size();
            if let Some(tracker) = &task_progress {
                tracker.inc_ssts_uploaded();
                tracker.dec_num_pending_write_io();
            }
            if is_share_buffer_compact {
                metrics.shared_buffer_to_sstable_size.observe(sst_size as _);
            } else {
                metrics.compaction_upload_sst_counts.inc();
            }
        }
    }

    async fn compact_key_range_impl<F: SstableWriterFactory, B: FilterBuilder>(
        &self,
        writer_factory: F,
        iter: impl HummockIterator<Direction = Forward>,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Option<Arc<TaskProgress>>,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        let builder_factory = RemoteBuilderFactory::<F, B> {
            object_id_getter,
            limiter: self.context.memory_limiter.clone(),
            options: self.options.clone(),
            policy: self.task_config.cache_policy,
            remote_rpc_cost: self.get_id_time.clone(),
            compaction_catalog_agent_ref: compaction_catalog_agent_ref.clone(),
            sstable_writer_factory: writer_factory,
            _phantom: PhantomData,
        };

        let mut sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            self.context.compactor_metrics.clone(),
            task_progress.clone(),
            self.task_config.table_vnode_partition.clone(),
            self.context
                .storage_opts
                .compactor_concurrent_uploading_sst_count,
            compaction_catalog_agent_ref,
        );
        let compaction_statistics = compact_and_build_sst(
            &mut sst_builder,
            &self.task_config,
            self.context.compactor_metrics.clone(),
            iter,
            compaction_filter,
        )
        .verbose_instrument_await("compact_and_build_sst")
        .await?;

        let ssts = sst_builder
            .finish()
            .verbose_instrument_await("builder_finish")
            .await?;

        Ok((ssts, compaction_statistics))
    }
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[cfg_attr(coverage, coverage(off))]
#[must_use]
pub fn start_compactor(
    compactor_context: CompactorContext,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_object_id_manager: Arc<SstableObjectIdManager>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let stream_retry_interval = Duration::from_secs(30);
    let task_progress = compactor_context.task_progress_manager.clone();
    let periodic_event_update_interval = Duration::from_millis(1000);

    let max_task_parallelism: u32 = (compactor_context.compaction_executor.worker_num() as f32
        * compactor_context.storage_opts.compactor_max_task_multiplier)
        .ceil() as u32;
    let running_task_parallelism = Arc::new(AtomicU32::new(0));

    const MAX_PULL_TASK_COUNT: u32 = 4;
    let max_pull_task_count = std::cmp::min(max_task_parallelism, MAX_PULL_TASK_COUNT);

    assert_ge!(
        compactor_context.storage_opts.compactor_max_task_multiplier,
        0.0
    );

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();
        let mut min_interval = tokio::time::interval(stream_retry_interval);
        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);

        // This outer loop is to recreate stream.
        'start_stream: loop {
            // reset state
            // pull_task_ack.store(true, Ordering::SeqCst);
            let mut pull_task_ack = true;
            tokio::select! {
                // Wait for interval.
                _ = min_interval.tick() => {},
                // Shutdown compactor.
                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return;
                }
            }

            let (request_sender, response_event_stream) =
                match hummock_meta_client.subscribe_compaction_event().await {
                    Ok((request_sender, response_event_stream)) => {
                        tracing::debug!("Succeeded subscribe_compaction_event.");
                        (request_sender, response_event_stream)
                    }

                    Err(e) => {
                        tracing::warn!(
                            error = %e.as_report(),
                            "Subscribing to compaction tasks failed with error. Will retry.",
                        );
                        continue 'start_stream;
                    }
                };

            pin_mut!(response_event_stream);

            let executor = compactor_context.compaction_executor.clone();
            let sstable_object_id_manager = sstable_object_id_manager.clone();

            // This inner loop is to consume stream or report task progress.
            let mut event_loop_iteration_now = Instant::now();
            'consume_stream: loop {
                {
                    // report
                    compactor_context
                        .compactor_metrics
                        .compaction_event_loop_iteration_latency
                        .observe(event_loop_iteration_now.elapsed().as_millis() as _);
                    event_loop_iteration_now = Instant::now();
                }

                let running_task_parallelism = running_task_parallelism.clone();
                let request_sender = request_sender.clone();
                let event: Option<Result<SubscribeCompactionEventResponse, _>> = tokio::select! {
                    _ = periodic_event_interval.tick() => {
                        let progress_list = get_task_progress(task_progress.clone());

                        if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                            event: Some(RequestEvent::HeartBeat(
                                HeartBeat {
                                    progress: progress_list
                                }
                            )),
                            create_at: SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Clock may have gone backwards")
                                .as_millis() as u64,
                        }) {
                            tracing::warn!(error = %e.as_report(), "Failed to report task progress");
                            // re subscribe stream
                            continue 'start_stream;
                        }


                        let mut pending_pull_task_count = 0;
                        if pull_task_ack {
                            // TODO: Compute parallelism on meta side
                            pending_pull_task_count = (max_task_parallelism - running_task_parallelism.load(Ordering::SeqCst)).min(max_pull_task_count);

                            if pending_pull_task_count > 0 {
                                if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                                    event: Some(RequestEvent::PullTask(
                                        PullTask {
                                            pull_task_count: pending_pull_task_count,
                                        }
                                    )),
                                    create_at: SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .expect("Clock may have gone backwards")
                                        .as_millis() as u64,
                                }) {
                                    tracing::warn!(error = %e.as_report(), "Failed to pull task");

                                    // re subscribe stream
                                    continue 'start_stream;
                                } else {
                                    pull_task_ack = false;
                                }
                            }
                        }

                        tracing::info!(
                            running_parallelism_count = %running_task_parallelism.load(Ordering::SeqCst),
                            pull_task_ack = %pull_task_ack,
                            pending_pull_task_count = %pending_pull_task_count
                        );

                        continue;
                    }
                    event = response_event_stream.next() => {
                        event
                    }

                    _ = &mut shutdown_rx => {
                        tracing::info!("Compactor is shutting down");
                        return
                    }
                };

                fn send_report_task_event(
                    compact_task: &CompactTask,
                    table_stats: TableStatsMap,
                    object_timestamps: HashMap<HummockSstableObjectId, u64>,
                    request_sender: &mpsc::UnboundedSender<SubscribeCompactionEventRequest>,
                ) {
                    if let Err(e) = request_sender.send(SubscribeCompactionEventRequest {
                        event: Some(RequestEvent::ReportTask(ReportTask {
                            task_id: compact_task.task_id,
                            task_status: compact_task.task_status.into(),
                            sorted_output_ssts: compact_task
                                .sorted_output_ssts
                                .iter()
                                .map(|sst| sst.into())
                                .collect(),
                            table_stats_change: to_prost_table_stats_map(table_stats),
                            object_timestamps,
                        })),
                        create_at: SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64,
                    }) {
                        let task_id = compact_task.task_id;
                        tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}");
                    }
                }

                match event {
                    Some(Ok(SubscribeCompactionEventResponse { event, create_at })) => {
                        let event = match event {
                            Some(event) => event,
                            None => continue 'consume_stream,
                        };
                        let shutdown = shutdown_map.clone();
                        let context = compactor_context.clone();
                        let consumed_latency_ms = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .expect("Clock may have gone backwards")
                            .as_millis() as u64
                            - create_at;
                        context
                            .compactor_metrics
                            .compaction_event_consumed_latency
                            .observe(consumed_latency_ms as _);

                        let sstable_object_id_manager = sstable_object_id_manager.clone();
                        let compaction_catalog_manager_ref = compaction_catalog_manager_ref.clone();

                        match event {
                            ResponseEvent::CompactTask(compact_task) => {
                                let compact_task = CompactTask::from(compact_task);
                                let parallelism =
                                    calculate_task_parallelism(&compact_task, &context);

                                assert_ne!(parallelism, 0, "splits cannot be empty");

                                if (max_task_parallelism
                                    - running_task_parallelism.load(Ordering::SeqCst))
                                    < parallelism as u32
                                {
                                    tracing::warn!(
                                        "Not enough core parallelism to serve the task {} task_parallelism {} running_task_parallelism {} max_task_parallelism {}",
                                        compact_task.task_id,
                                        parallelism,
                                        max_task_parallelism,
                                        running_task_parallelism.load(Ordering::Relaxed),
                                    );
                                    let (compact_task, table_stats, object_timestamps) =
                                        compact_done(
                                            compact_task,
                                            context.clone(),
                                            vec![],
                                            TaskStatus::NoAvailCpuResourceCanceled,
                                        );

                                    send_report_task_event(
                                        &compact_task,
                                        table_stats,
                                        object_timestamps,
                                        &request_sender,
                                    );

                                    continue 'consume_stream;
                                }

                                running_task_parallelism
                                    .fetch_add(parallelism as u32, Ordering::SeqCst);
                                executor.spawn(async move {
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let task_id = compact_task.task_id;
                                    shutdown.lock().unwrap().insert(task_id, tx);

                                    let ((compact_task, table_stats, object_timestamps), _memory_tracker)= compactor_runner::compact(
                                        context.clone(),
                                        compact_task,
                                        rx,
                                        Box::new(sstable_object_id_manager.clone()),
                                        compaction_catalog_manager_ref.clone(),
                                    )
                                    .await;

                                    shutdown.lock().unwrap().remove(&task_id);
                                    running_task_parallelism.fetch_sub(parallelism as u32, Ordering::SeqCst);

                                    send_report_task_event(
                                        &compact_task,
                                        table_stats,
                                        object_timestamps,
                                        &request_sender,
                                    );

                                    let enable_check_compaction_result =
                                    context.storage_opts.check_compaction_result;
                                    let need_check_task = !compact_task.sorted_output_ssts.is_empty() && compact_task.task_status == TaskStatus::Success;

                                    if enable_check_compaction_result && need_check_task {
                                        let compact_table_ids = compact_task.build_compact_table_ids();
                                        match compaction_catalog_manager_ref.acquire(compact_table_ids).await {
                                            Ok(compaction_catalog_agent_ref) =>  {
                                                match check_compaction_result(&compact_task, context.clone(), compaction_catalog_agent_ref).await
                                                {
                                                    Err(e) => {
                                                        tracing::warn!(error = %e.as_report(), "Failed to check compaction task {}",compact_task.task_id);
                                                    }
                                                    Ok(true) => (),
                                                    Ok(false) => {
                                                        panic!("Failed to pass consistency check for result of compaction task:\n{:?}", compact_task_to_string(&compact_task));
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                tracing::warn!(error = %e.as_report(), "failed to acquire compaction catalog agent");
                                            }
                                        }
                                    }
                                });
                            }
                            ResponseEvent::VacuumTask(_) => {
                                unreachable!("unexpected vacuum task");
                            }
                            ResponseEvent::FullScanTask(_) => {
                                unreachable!("unexpected scan task");
                            }
                            ResponseEvent::ValidationTask(validation_task) => {
                                let validation_task = ValidationTask::from(validation_task);
                                executor.spawn(async move {
                                    validate_ssts(validation_task, context.sstable_store.clone())
                                        .await;
                                });
                            }
                            ResponseEvent::CancelCompactTask(cancel_compact_task) => {
                                if let Some(tx) = shutdown
                                    .lock()
                                    .unwrap()
                                    .remove(&cancel_compact_task.task_id)
                                {
                                    if tx.send(()).is_err() {
                                        tracing::warn!(
                                            "Cancellation of compaction task failed. task_id: {}",
                                            cancel_compact_task.task_id
                                        );
                                    }
                                } else {
                                    tracing::warn!(
                                        "Attempting to cancel non-existent compaction task. task_id: {}",
                                            cancel_compact_task.task_id
                                    );
                                }
                            }

                            ResponseEvent::PullTaskAck(_pull_task_ack) => {
                                // set flag
                                pull_task_ack = true;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Failed to consume stream. {}", e.message());
                        continue 'start_stream;
                    }
                    _ => {
                        // The stream is exhausted
                        continue 'start_stream;
                    }
                }
            }
        }
    });

    (join_handle, shutdown_tx)
}

/// The background compaction thread that receives compaction tasks from hummock compaction
/// manager and runs compaction tasks.
#[cfg_attr(coverage, coverage(off))]
#[must_use]
pub fn start_shared_compactor(
    grpc_proxy_client: GrpcCompactorProxyClient,
    mut receiver: mpsc::UnboundedReceiver<Request<DispatchCompactionTaskRequest>>,
    context: CompactorContext,
) -> (JoinHandle<()>, Sender<()>) {
    type CompactionShutdownMap = Arc<Mutex<HashMap<u64, Sender<()>>>>;
    let task_progress = context.task_progress_manager.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let periodic_event_update_interval = Duration::from_millis(1000);

    let join_handle = tokio::spawn(async move {
        let shutdown_map = CompactionShutdownMap::default();

        let mut periodic_event_interval = tokio::time::interval(periodic_event_update_interval);
        let executor = context.compaction_executor.clone();
        let report_heartbeat_client = grpc_proxy_client.clone();
        'consume_stream: loop {
            let request: Option<Request<DispatchCompactionTaskRequest>> = tokio::select! {
                _ = periodic_event_interval.tick() => {
                    let progress_list = get_task_progress(task_progress.clone());
                    let report_compaction_task_request = ReportCompactionTaskRequest{
                        event: Some(ReportCompactionTaskEvent::HeartBeat(
                            HeartBeat {
                                progress: progress_list
                            }
                        )),
                     };
                    if let Err(e) = report_heartbeat_client.report_compaction_task(report_compaction_task_request).await{
                        tracing::warn!(error = %e.as_report(), "Failed to report heartbeat");
                    }
                    continue
                }


                _ = &mut shutdown_rx => {
                    tracing::info!("Compactor is shutting down");
                    return
                }

                request = receiver.recv() => {
                    request
                }

            };
            match request {
                Some(request) => {
                    let context = context.clone();
                    let shutdown = shutdown_map.clone();

                    let cloned_grpc_proxy_client = grpc_proxy_client.clone();
                    executor.spawn(async move {
                        let DispatchCompactionTaskRequest {
                            tables,
                            output_object_ids,
                            task: dispatch_task,
                        } = request.into_inner();
                        let table_id_to_catalog = tables.into_iter().fold(HashMap::new(), |mut acc, table| {
                            acc.insert(table.id, table);
                            acc
                        });

                        let mut output_object_ids_deque: VecDeque<_> = VecDeque::new();
                        output_object_ids_deque.extend(output_object_ids);
                        let shared_compactor_object_id_manager =
                            SharedComapctorObjectIdManager::new(output_object_ids_deque, cloned_grpc_proxy_client.clone(), context.storage_opts.sstable_id_remote_fetch_number);
                            match dispatch_task.unwrap() {
                                dispatch_compaction_task_request::Task::CompactTask(compact_task) => {
                                    let compact_task = CompactTask::from(&compact_task);
                                    let (tx, rx) = tokio::sync::oneshot::channel();
                                    let task_id = compact_task.task_id;
                                    shutdown.lock().unwrap().insert(task_id, tx);

                                    let compaction_catalog_agent_ref = CompactionCatalogManager::build_compaction_catalog_agent(table_id_to_catalog);
                                    let ((compact_task, table_stats, object_timestamps), _memory_tracker)= compactor_runner::compact_with_agent(
                                        context.clone(),
                                        compact_task,
                                        rx,
                                        Box::new(shared_compactor_object_id_manager),
                                        compaction_catalog_agent_ref.clone(),
                                    )
                                    .await;
                                    shutdown.lock().unwrap().remove(&task_id);
                                    let report_compaction_task_request = ReportCompactionTaskRequest {
                                        event: Some(ReportCompactionTaskEvent::ReportTask(ReportTask {
                                            task_id: compact_task.task_id,
                                            task_status: compact_task.task_status.into(),
                                            sorted_output_ssts: compact_task
                                                .sorted_output_ssts
                                                .iter()
                                                .map(|sst| sst.into())
                                                .collect(),
                                            table_stats_change: to_prost_table_stats_map(table_stats),
                                            object_timestamps,
                                        })),
                                    };

                                    match cloned_grpc_proxy_client
                                        .report_compaction_task(report_compaction_task_request)
                                        .await
                                    {
                                        Ok(_) => {
                                            // TODO: remove this method after we have running risingwave cluster with fast compact algorithm stably for a long time.
                                            let enable_check_compaction_result = context.storage_opts.check_compaction_result;
                                            let need_check_task = !compact_task.sorted_output_ssts.is_empty() && compact_task.task_status == TaskStatus::Success;
                                            if enable_check_compaction_result && need_check_task {
                                                match check_compaction_result(&compact_task, context.clone(),compaction_catalog_agent_ref).await {
                                                    Err(e) => {
                                                        tracing::warn!(error = %e.as_report(), "Failed to check compaction task {}", task_id);
                                                    },
                                                    Ok(true) => (),
                                                    Ok(false) => {
                                                        panic!("Failed to pass consistency check for result of compaction task:\n{:?}", compact_task_to_string(&compact_task));
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => tracing::warn!(error = %e.as_report(), "Failed to report task {task_id:?}"),
                                    }

                                }
                                dispatch_compaction_task_request::Task::VacuumTask(_) => {
                                    unreachable!("unexpected vacuum task");
                                }
                                dispatch_compaction_task_request::Task::FullScanTask(_) => {
                                    unreachable!("unexpected scan task");
                                }
                                dispatch_compaction_task_request::Task::ValidationTask(validation_task) => {
                                    let validation_task = ValidationTask::from(validation_task);
                                    validate_ssts(validation_task, context.sstable_store.clone()).await;
                                }
                                dispatch_compaction_task_request::Task::CancelCompactTask(cancel_compact_task) => {
                                    if let Some(tx) = shutdown
                                        .lock()
                                        .unwrap()
                                        .remove(&cancel_compact_task.task_id)
                                    {
                                        if tx.send(()).is_err() {
                                            tracing::warn!(
                                                "Cancellation of compaction task failed. task_id: {}",
                                                cancel_compact_task.task_id
                                            );
                                        }
                                    } else {
                                        tracing::warn!(
                                            "Attempting to cancel non-existent compaction task. task_id: {}",
                                            cancel_compact_task.task_id
                                        );
                                    }
                                }
                            }
                    });
                }
                None => continue 'consume_stream,
            }
        }
    });
    (join_handle, shutdown_tx)
}

fn get_task_progress(
    task_progress: Arc<
        parking_lot::lock_api::Mutex<parking_lot::RawMutex, HashMap<u64, Arc<TaskProgress>>>,
    >,
) -> Vec<CompactTaskProgress> {
    let mut progress_list = Vec::new();
    for (&task_id, progress) in &*task_progress.lock() {
        progress_list.push(progress.snapshot(task_id));
    }
    progress_list
}
