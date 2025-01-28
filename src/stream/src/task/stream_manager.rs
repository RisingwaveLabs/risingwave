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

use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Instant;

use async_recursion::async_recursion;
use await_tree::InstrumentAwait;
use futures::future::join_all;
use futures::stream::BoxStream;
use futures::{FutureExt, TryFutureExt};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnId, DatabaseId, Field, Schema, TableId};
use risingwave_common::config::MetricLevel;
use risingwave_common::must_match;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamActor, StreamNode, StreamScanNode, StreamScanType};
use risingwave_pb::stream_service::streaming_control_stream_request::InitRequest;
use risingwave_pb::stream_service::{
    StreamingControlStreamRequest, StreamingControlStreamResponse,
};
use risingwave_storage::monitor::HummockTraceFutureExt;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::{dispatch_state_store, StateStore};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use tonic::Status;

use super::{unique_executor_id, unique_operator_id};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::subtask::SubtaskHandle;
use crate::executor::{
    Actor, ActorContext, ActorContextRef, DispatchExecutor, DispatcherImpl, Execute, Executor,
    ExecutorInfo, MergeExecutorInput, SnapshotBackfillExecutor, TroublemakerExecutor,
    WrapperExecutor,
};
use crate::from_proto::{create_executor, MergeExecutorBuilder};
use crate::task::barrier_manager::{
    ControlStreamHandle, EventSender, LocalActorOperation, LocalBarrierWorker,
};
use crate::task::{
    ActorId, FragmentId, LocalBarrierManager, SharedContext, StreamActorManager, StreamEnvironment,
    UpDownActorIds,
};

#[cfg(test)]
pub static LOCAL_TEST_ADDR: std::sync::LazyLock<risingwave_common::util::addr::HostAddr> =
    std::sync::LazyLock::new(|| "127.0.0.1:2333".parse().unwrap());

pub type ActorHandle = JoinHandle<()>;

pub type AtomicU64Ref = Arc<AtomicU64>;

pub mod await_tree_key {
    /// Await-tree key type for actors.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Actor(pub crate::task::ActorId);

    /// Await-tree key type for barriers.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct BarrierAwait {
        pub prev_epoch: u64,
    }
}

/// `LocalStreamManager` manages all stream executors in this project.
#[derive(Clone)]
pub struct LocalStreamManager {
    await_tree_reg: Option<await_tree::Registry>,

    pub env: StreamEnvironment,

    actor_op_tx: EventSender<LocalActorOperation>,
}

/// Report expression evaluation errors to the actor context.
///
/// The struct can be cheaply cloned.
#[derive(Clone)]
pub struct ActorEvalErrorReport {
    pub actor_context: ActorContextRef,
    pub identity: Arc<str>,
}

impl risingwave_expr::expr::EvalErrorReport for ActorEvalErrorReport {
    fn report(&self, err: risingwave_expr::ExprError) {
        self.actor_context.on_compute_error(err, &self.identity);
    }
}

pub struct ExecutorParams {
    pub env: StreamEnvironment,

    /// Basic information about the executor.
    pub info: ExecutorInfo,

    /// Executor id, unique across all actors.
    pub executor_id: u64,

    /// Operator id, unique for each operator in fragment.
    pub operator_id: u64,

    /// Information of the operator from plan node, like `StreamHashJoin { .. }`.
    // TODO: use it for `identity`
    pub op_info: String,

    /// The input executor.
    pub input: Vec<Executor>,

    /// `FragmentId` of the actor
    pub fragment_id: FragmentId,

    /// Metrics
    pub executor_stats: Arc<StreamingMetrics>,

    /// Actor context
    pub actor_context: ActorContextRef,

    /// Vnodes owned by this executor. Represented in bitmap.
    pub vnode_bitmap: Option<Bitmap>,

    /// Used for reporting expression evaluation errors.
    pub eval_error_report: ActorEvalErrorReport,

    /// `watermark_epoch` field in `MemoryManager`
    pub watermark_epoch: AtomicU64Ref,

    pub shared_context: Arc<SharedContext>,

    pub local_barrier_manager: LocalBarrierManager,
}

impl Debug for ExecutorParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorParams")
            .field("info", &self.info)
            .field("executor_id", &self.executor_id)
            .field("operator_id", &self.operator_id)
            .field("op_info", &self.op_info)
            .field("input", &self.input.len())
            .field("actor_id", &self.actor_context.id)
            .finish_non_exhaustive()
    }
}

impl LocalStreamManager {
    pub fn new(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_config: Option<await_tree::Config>,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        if !env.config().unsafe_enable_strict_consistency {
            // If strict consistency is disabled, should disable storage sanity check.
            // Since this is a special config, we have to check it here.
            risingwave_storage::hummock::utils::disable_sanity_check();
        }

        let await_tree_reg = await_tree_config.clone().map(await_tree::Registry::new);

        let (actor_op_tx, actor_op_rx) = unbounded_channel();

        let _join_handle = LocalBarrierWorker::spawn(
            env.clone(),
            streaming_metrics,
            await_tree_reg.clone(),
            watermark_epoch,
            actor_op_rx,
        );
        Self {
            await_tree_reg,
            env,
            actor_op_tx: EventSender(actor_op_tx),
        }
    }

    /// Get the registry of await-trees.
    pub fn await_tree_reg(&self) -> Option<&await_tree::Registry> {
        self.await_tree_reg.as_ref()
    }

    /// Receive a new control stream request from meta. Notify the barrier worker to reset the CN and use the new control stream
    /// to receive control message from meta
    pub fn handle_new_control_stream(
        &self,
        sender: UnboundedSender<Result<StreamingControlStreamResponse, Status>>,
        request_stream: BoxStream<'static, Result<StreamingControlStreamRequest, Status>>,
        init_request: InitRequest,
    ) {
        self.actor_op_tx
            .send_event(LocalActorOperation::NewControlStream {
                handle: ControlStreamHandle::new(sender, request_stream),
                init_request,
            })
    }

    pub async fn take_receiver(
        &self,
        database_id: DatabaseId,
        ids: UpDownActorIds,
    ) -> StreamResult<Receiver> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::TakeReceiver {
                database_id,
                ids,
                result_sender,
            })
            .await?
    }

    pub async fn inspect_barrier_state(&self) -> StreamResult<String> {
        info!("start inspecting barrier state");
        let start = Instant::now();
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::InspectState { result_sender })
            .inspect(|result| {
                info!(
                    ok = result.is_ok(),
                    time = ?start.elapsed(),
                    "finish inspecting barrier state"
                );
            })
            .await
    }

    pub async fn shutdown(&self) -> StreamResult<()> {
        self.actor_op_tx
            .send_and_await(|result_sender| LocalActorOperation::Shutdown { result_sender })
            .await
    }
}

impl LocalBarrierWorker {
    /// Force stop all actors on this worker, and then drop their resources.
    pub(super) async fn reset(&mut self, init_request: InitRequest) {
        join_all(
            self.state
                .databases
                .values_mut()
                .map(|database| database.abort()),
        )
        .await;
        if let Some(m) = self.actor_manager.await_tree_reg.as_ref() {
            m.clear();
        }

        if let Some(hummock) = self.actor_manager.env.state_store().as_hummock() {
            hummock
                .clear_shared_buffer()
                .verbose_instrument_await("store_clear_shared_buffer")
                .await
        }
        self.actor_manager.env.dml_manager_ref().clear();
        *self = Self::new(self.actor_manager.clone(), init_request.databases);
    }
}

impl StreamActorManager {
    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &self,
        env: StreamEnvironment,
        input: Executor,
        dispatchers: &[stream_plan::Dispatcher],
        actor_id: ActorId,
        fragment_id: FragmentId,
        shared_context: &Arc<SharedContext>,
    ) -> StreamResult<DispatchExecutor> {
        let dispatcher_impls = dispatchers
            .iter()
            .map(|dispatcher| DispatcherImpl::new(shared_context, actor_id, dispatcher))
            .try_collect()?;

        Ok(DispatchExecutor::new(
            input,
            dispatcher_impls,
            actor_id,
            fragment_id,
            shared_context.clone(),
            self.streaming_metrics.clone(),
            env.config().developer.chunk_size,
        ))
    }

    fn get_executor_id(actor_context: &ActorContext, node: &StreamNode) -> u64 {
        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        unique_executor_id(actor_context.id, node.operator_id)
    }

    fn get_executor_info(node: &StreamNode, executor_id: u64) -> ExecutorInfo {
        let schema: Schema = node.fields.iter().map(Field::from).collect();

        let pk_indices = node
            .get_stream_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        let identity = format!("{} {:X}", node.get_node_body().unwrap(), executor_id);
        ExecutorInfo {
            schema,
            pk_indices,
            identity,
        }
    }

    fn create_snapshot_backfill_input(
        &self,
        upstream_node: &StreamNode,
        actor_context: &ActorContextRef,
        shared_context: &Arc<SharedContext>,
        chunk_size: usize,
    ) -> StreamResult<MergeExecutorInput> {
        let info = Self::get_executor_info(
            upstream_node,
            Self::get_executor_id(actor_context, upstream_node),
        );

        let upstream_merge = must_match!(upstream_node.get_node_body().unwrap(), NodeBody::Merge(upstream_merge) => {
            upstream_merge
        });

        MergeExecutorBuilder::new_input(
            shared_context.clone(),
            self.streaming_metrics.clone(),
            actor_context.clone(),
            info,
            upstream_merge,
            chunk_size,
        )
    }

    #[expect(clippy::too_many_arguments)]
    async fn create_snapshot_backfill_node(
        &self,
        stream_node: &StreamNode,
        node: &StreamScanNode,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        shared_context: &Arc<SharedContext>,
        env: StreamEnvironment,
        local_barrier_manager: &LocalBarrierManager,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream_node, _]: &[_; 2] = stream_node.input.as_slice().try_into().unwrap();
        let chunk_size = env.config().developer.chunk_size;
        let upstream = self.create_snapshot_backfill_input(
            upstream_node,
            actor_context,
            shared_context,
            chunk_size,
        )?;

        let table_desc: &StorageTableDesc = node.get_table_desc()?;

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let column_ids = node
            .upstream_column_ids
            .iter()
            .map(ColumnId::from)
            .collect_vec();

        let progress = local_barrier_manager.register_create_mview_progress(actor_context.id);

        let vnodes = vnode_bitmap.map(Arc::new);
        let barrier_rx = local_barrier_manager.subscribe_barrier(actor_context.id);

        let upstream_table =
            BatchTable::new_partial(state_store.clone(), column_ids, vnodes.clone(), table_desc);

        let state_table = node.get_state_table()?;
        let state_table =
            StateTable::from_table_catalog(state_table, state_store.clone(), vnodes).await;

        let executor = SnapshotBackfillExecutor::new(
            upstream_table,
            state_table,
            upstream,
            output_indices,
            actor_context.clone(),
            progress,
            chunk_size,
            node.rate_limit.into(),
            barrier_rx,
            self.streaming_metrics.clone(),
            node.snapshot_backfill_epoch,
        )
        .boxed();

        let info = Self::get_executor_info(
            stream_node,
            Self::get_executor_id(actor_context, stream_node),
        );

        if crate::consistency::insane() {
            let mut troubled_info = info.clone();
            troubled_info.identity = format!("{} (troubled)", info.identity);
            Ok((
                info,
                TroublemakerExecutor::new((troubled_info, executor).into(), chunk_size),
            )
                .into())
        } else {
            Ok((info, executor).into())
        }
    }

    /// Create a chain(tree) of nodes, with given `store`.
    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn create_nodes_inner(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        store: impl StateStore,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        has_stateful: bool,
        subtasks: &mut Vec<SubtaskHandle>,
        shared_context: &Arc<SharedContext>,
        local_barrier_manager: &LocalBarrierManager,
    ) -> StreamResult<Executor> {
        if let NodeBody::StreamScan(stream_scan) = node.get_node_body().unwrap()
            && let Ok(StreamScanType::SnapshotBackfill) = stream_scan.get_stream_scan_type()
        {
            return dispatch_state_store!(env.state_store(), store, {
                self.create_snapshot_backfill_node(
                    node,
                    stream_scan,
                    actor_context,
                    vnode_bitmap,
                    shared_context,
                    env,
                    local_barrier_manager,
                    store,
                )
                .await
            });
        }

        // The "stateful" here means that the executor may issue read operations to the state store
        // massively and continuously. Used to decide whether to apply the optimization of subtasks.
        fn is_stateful_executor(stream_node: &StreamNode) -> bool {
            matches!(
                stream_node.get_node_body().unwrap(),
                NodeBody::HashAgg(_)
                    | NodeBody::HashJoin(_)
                    | NodeBody::DeltaIndexJoin(_)
                    | NodeBody::Lookup(_)
                    | NodeBody::StreamScan(_)
                    | NodeBody::StreamCdcScan(_)
                    | NodeBody::DynamicFilter(_)
                    | NodeBody::GroupTopN(_)
                    | NodeBody::Now(_)
            )
        }
        let is_stateful = is_stateful_executor(node);

        // Create the input executor before creating itself
        let mut input = Vec::with_capacity(node.input.iter().len());
        for input_stream_node in &node.input {
            input.push(
                self.create_nodes_inner(
                    fragment_id,
                    input_stream_node,
                    env.clone(),
                    store.clone(),
                    actor_context,
                    vnode_bitmap.clone(),
                    has_stateful || is_stateful,
                    subtasks,
                    shared_context,
                    local_barrier_manager,
                )
                .await?,
            );
        }

        let op_info = node.get_identity().clone();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = Self::get_executor_id(actor_context, node);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);

        let info = Self::get_executor_info(node, executor_id);

        let eval_error_report = ActorEvalErrorReport {
            actor_context: actor_context.clone(),
            identity: info.identity.clone().into(),
        };

        // Build the executor with params.
        let executor_params = ExecutorParams {
            env: env.clone(),

            info: info.clone(),
            executor_id,
            operator_id,
            op_info,
            input,
            fragment_id,
            executor_stats: self.streaming_metrics.clone(),
            actor_context: actor_context.clone(),
            vnode_bitmap,
            eval_error_report,
            watermark_epoch: self.watermark_epoch.clone(),
            shared_context: shared_context.clone(),
            local_barrier_manager: local_barrier_manager.clone(),
        };

        let executor = create_executor(executor_params, node, store).await?;

        // Wrap the executor for debug purpose.
        let wrapped = WrapperExecutor::new(
            executor,
            actor_context.clone(),
            env.config().developer.enable_executor_row_count,
        );
        let executor = (info, wrapped).into();

        // If there're multiple stateful executors in this actor, we will wrap it into a subtask.
        let executor = if has_stateful && is_stateful {
            // TODO(bugen): subtask does not work with tracing spans.
            // let (subtask, executor) = subtask::wrap(executor, actor_context.id);
            // subtasks.push(subtask);
            // executor.boxed()

            let _ = subtasks;
            executor
        } else {
            executor
        };

        Ok(executor)
    }

    /// Create a chain(tree) of nodes and return the head executor.
    #[expect(clippy::too_many_arguments)]
    async fn create_nodes(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
        shared_context: &Arc<SharedContext>,
        local_barrier_manager: &LocalBarrierManager,
    ) -> StreamResult<(Executor, Vec<SubtaskHandle>)> {
        let mut subtasks = vec![];

        let executor = dispatch_state_store!(env.state_store(), store, {
            self.create_nodes_inner(
                fragment_id,
                node,
                env,
                store,
                actor_context,
                vnode_bitmap,
                false,
                &mut subtasks,
                shared_context,
                local_barrier_manager,
            )
            .await
        })?;

        Ok((executor, subtasks))
    }

    async fn create_actor(
        self: Arc<Self>,
        actor: StreamActor,
        shared_context: Arc<SharedContext>,
        related_subscriptions: Arc<HashMap<TableId, HashSet<u32>>>,
        local_barrier_manager: LocalBarrierManager,
    ) -> StreamResult<Actor<DispatchExecutor>> {
        {
            let actor_id = actor.actor_id;
            let streaming_config = self.env.config().clone();
            let actor_context = ActorContext::create(
                &actor,
                self.env.total_mem_usage(),
                self.streaming_metrics.clone(),
                actor.dispatcher.len(),
                related_subscriptions,
                self.env.meta_client().clone(),
                streaming_config,
            );
            let vnode_bitmap = actor.vnode_bitmap.as_ref().map(|b| b.into());
            let expr_context = actor.expr_context.clone().unwrap();

            let (executor, subtasks) = self
                .create_nodes(
                    actor.fragment_id,
                    actor.get_nodes()?,
                    self.env.clone(),
                    &actor_context,
                    vnode_bitmap,
                    &shared_context,
                    &local_barrier_manager,
                )
                .await?;

            let dispatcher = self.create_dispatcher(
                self.env.clone(),
                executor,
                &actor.dispatcher,
                actor_id,
                actor.fragment_id,
                &shared_context,
            )?;
            let actor = Actor::new(
                dispatcher,
                subtasks,
                self.streaming_metrics.clone(),
                actor_context.clone(),
                expr_context,
                local_barrier_manager,
            );
            Ok(actor)
        }
    }
}

impl StreamActorManager {
    pub(super) fn spawn_actor(
        self: &Arc<Self>,
        actor: StreamActor,
        related_subscriptions: Arc<HashMap<TableId, HashSet<u32>>>,
        current_shared_context: Arc<SharedContext>,
        local_barrier_manager: LocalBarrierManager,
    ) -> (JoinHandle<()>, Option<JoinHandle<()>>) {
        {
            let monitor = tokio_metrics::TaskMonitor::new();
            let stream_actor_ref = &actor;
            let actor_id = stream_actor_ref.actor_id;
            let handle = {
                let trace_span =
                    format!("Actor {actor_id}: `{}`", stream_actor_ref.mview_definition);
                let barrier_manager = local_barrier_manager.clone();
                // wrap the future of `create_actor` with `boxed` to avoid stack overflow
                let actor = self.clone().create_actor(actor, current_shared_context, related_subscriptions, barrier_manager.clone()).boxed().and_then(|actor| actor.run()).map(move |result| {
                    if let Err(err) = result {
                        // TODO: check error type and panic if it's unexpected.
                        // Intentionally use `?` on the report to also include the backtrace.
                        tracing::error!(actor_id, error = ?err.as_report(), "actor exit with error");
                        barrier_manager.notify_failure(actor_id, err);
                    }
                });
                let traced = match &self.await_tree_reg {
                    Some(m) => m
                        .register(await_tree_key::Actor(actor_id), trace_span)
                        .instrument(actor)
                        .left_future(),
                    None => actor.right_future(),
                };
                let instrumented = monitor.instrument(traced);
                let with_config = crate::CONFIG.scope(self.env.config().clone(), instrumented);
                // If hummock tracing is not enabled, it directly returns wrapped future.
                let may_track_hummock = with_config.may_trace_hummock();

                self.runtime.spawn(may_track_hummock)
            };

            let monitor_handle = if self.streaming_metrics.level >= MetricLevel::Debug
                || self.env.config().developer.enable_actor_tokio_metrics
            {
                tracing::info!("Tokio metrics are enabled.");
                let streaming_metrics = self.streaming_metrics.clone();
                let actor_monitor_task = self.runtime.spawn(async move {
                    let metrics = streaming_metrics.new_actor_metrics(actor_id);
                    loop {
                        let task_metrics = monitor.cumulative();
                        metrics
                            .actor_execution_time
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_duration
                            .set(task_metrics.total_fast_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_cnt
                            .set(task_metrics.total_fast_poll_count as i64);
                        metrics
                            .actor_slow_poll_duration
                            .set(task_metrics.total_slow_poll_duration.as_secs_f64());
                        metrics
                            .actor_slow_poll_cnt
                            .set(task_metrics.total_slow_poll_count as i64);
                        metrics
                            .actor_poll_duration
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_poll_cnt
                            .set(task_metrics.total_poll_count as i64);
                        metrics
                            .actor_idle_duration
                            .set(task_metrics.total_idle_duration.as_secs_f64());
                        metrics
                            .actor_idle_cnt
                            .set(task_metrics.total_idled_count as i64);
                        metrics
                            .actor_scheduled_duration
                            .set(task_metrics.total_scheduled_duration.as_secs_f64());
                        metrics
                            .actor_scheduled_cnt
                            .set(task_metrics.total_scheduled_count as i64);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                Some(actor_monitor_task)
            } else {
                None
            };
            (handle, monitor_handle)
        }
    }
}

impl LocalBarrierWorker {
    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub fn update_actor_info(
        &mut self,
        database_id: DatabaseId,
        new_actor_infos: impl Iterator<Item = ActorInfo>,
    ) {
        Self::get_or_insert_database_shared_context(
            &mut self.state.current_shared_context,
            database_id,
            &self.actor_manager,
        )
        .add_actors(new_actor_infos);
    }
}

#[cfg(test)]
pub mod test_utils {
    use risingwave_pb::common::HostAddress;

    use super::*;

    pub fn helper_make_local_actor(actor_id: u32) -> ActorInfo {
        ActorInfo {
            actor_id,
            host: Some(HostAddress {
                host: LOCAL_TEST_ADDR.host.clone(),
                port: LOCAL_TEST_ADDR.port as i32,
            }),
        }
    }
}
