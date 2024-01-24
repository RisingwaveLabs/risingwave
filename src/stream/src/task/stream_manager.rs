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

use core::time::Duration;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use anyhow::anyhow;
use async_recursion::async_recursion;
use futures::FutureExt;
use hytra::TrAdder;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::config::{MetricLevel, StreamingConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamActor, StreamNode};
use risingwave_storage::monitor::HummockTraceFutureExt;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;

use super::{unique_executor_id, unique_operator_id, BarrierCompleteResult};
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::subtask::SubtaskHandle;
use crate::executor::*;
use crate::from_proto::create_executor;
use crate::task::{ActorId, FragmentId, LocalBarrierManager, SharedContext, StreamEnvironment};

#[cfg(test)]
pub static LOCAL_TEST_ADDR: std::sync::LazyLock<HostAddr> =
    std::sync::LazyLock::new(|| "127.0.0.1:2333".parse().unwrap());

pub type ActorHandle = JoinHandle<()>;

pub type AtomicU64Ref = Arc<AtomicU64>;

pub struct LocalStreamManagerCore {
    /// Runtime for the streaming actors.
    runtime: BackgroundShutdownRuntime,

    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    handles: HashMap<ActorId, ActorHandle>,

    /// Stores all actor information, taken after actor built.
    actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Stores all actor tokio runtime monitoring tasks.
    actor_monitor_tasks: HashMap<ActorId, ActorHandle>,
}

/// `LocalStreamManager` manages all stream executors in this project.
pub struct LocalStreamManager {
    core: Mutex<LocalStreamManagerCore>,

    state_store: StateStoreImpl,
    context: Arc<SharedContext>,
    streaming_metrics: Arc<StreamingMetrics>,

    total_mem_val: Arc<TrAdder<i64>>,

    /// Watermark epoch number.
    watermark_epoch: AtomicU64Ref,

    local_barrier_manager: LocalBarrierManager,

    /// Manages the await-trees of all actors.
    await_tree_reg: Option<Mutex<await_tree::Registry<ActorId>>>,
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
    pub input: Vec<BoxedExecutor>,

    /// FragmentId of the actor
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
        addr: HostAddr,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        config: StreamingConfig,
        await_tree_config: Option<await_tree::Config>,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        let local_barrier_manager =
            LocalBarrierManager::new(state_store.clone(), streaming_metrics.clone());
        let context = Arc::new(SharedContext::new(addr, &config));
        let core = LocalStreamManagerCore::new(context.config.actor_runtime_worker_threads_num);
        Self {
            state_store,
            context,
            total_mem_val: Arc::new(TrAdder::new()),
            core: Mutex::new(core),
            streaming_metrics,
            watermark_epoch,
            local_barrier_manager,
            await_tree_reg: await_tree_config
                .map(|config| Mutex::new(await_tree::Registry::new(config))),
        }
    }

    /// Print the traces of all actors periodically, used for debugging only.
    pub fn spawn_print_trace(self: Arc<Self>) -> JoinHandle<!> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(5000)).await;
                let mut o = std::io::stdout().lock();

                for (k, trace) in self
                    .await_tree_reg
                    .as_ref()
                    .expect("async stack trace not enabled")
                    .lock()
                    .iter()
                {
                    writeln!(o, ">> Actor {}\n\n{}", k, trace).ok();
                }
            }
        })
    }

    /// Get await-tree contexts for all actors.
    pub fn get_actor_traces(&self) -> HashMap<ActorId, await_tree::TreeContext> {
        match &self.await_tree_reg.as_ref() {
            Some(mgr) => mgr.lock().iter().map(|(k, v)| (*k, v)).collect(),
            None => Default::default(),
        }
    }

    /// Get all existing actor ids.
    pub fn all_actor_ids(&self) -> HashSet<ActorId> {
        self.core.lock().handles.keys().cloned().collect()
    }

    /// Broadcast a barrier to all senders. Save a receiver in barrier manager
    pub async fn send_barrier(
        &self,
        barrier: Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> StreamResult<()> {
        if barrier.kind == BarrierKind::Initial {
            self.watermark_epoch
                .store(barrier.epoch.curr, std::sync::atomic::Ordering::SeqCst);
        }
        self.local_barrier_manager
            .send_barrier(barrier, actor_ids_to_send, actor_ids_to_collect)
            .await?;
        Ok(())
    }

    /// Reset the state of the barrier manager.
    pub fn reset_barrier_manager(&self) {
        self.local_barrier_manager.reset();
    }

    /// Use `epoch` to find collect rx. And wait for all actor to be collected before
    /// returning.
    pub async fn collect_barrier(&self, epoch: u64) -> StreamResult<BarrierCompleteResult> {
        self.local_barrier_manager
            .await_epoch_completed(epoch)
            .await
    }

    pub async fn clear_storage_buffer(&self) {
        dispatch_state_store!(self.state_store.clone(), store, {
            store.clear_shared_buffer().await.unwrap();
        });
    }

    /// Drop the resources of the given actors.
    pub fn drop_actors(&self, actors: &[ActorId]) -> StreamResult<()> {
        self.context.drop_actors(actors);
        let mut core = self.core.lock();
        for &id in actors {
            core.drop_actor(id);
        }
        tracing::debug!(actors = ?actors, "drop actors");
        Ok(())
    }

    /// Force stop all actors on this worker, and then drop their resources.
    pub async fn stop_all_actors(&self) -> StreamResult<()> {
        let actor_handles = self.core.lock().drain_actor_handles();
        for (actor_id, handle) in &actor_handles {
            tracing::debug!("force stopping actor {}", actor_id);
            handle.abort();
        }
        for (actor_id, handle) in actor_handles {
            tracing::debug!("join actor {}", actor_id);
            let result = handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
        self.context.clear_channels();
        self.context.actor_infos.write().clear();
        self.core.lock().clear_state();
        if let Some(m) = self.await_tree_reg.as_ref() {
            m.lock().clear();
        }
        self.reset_barrier_manager();
        // Clear shared buffer in storage to release memory
        self.clear_storage_buffer().await;

        Ok(())
    }

    pub fn update_actors(&self, actors: &[stream_plan::StreamActor]) -> StreamResult<()> {
        let mut core = self.core.lock();
        core.update_actors(actors)
    }

    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub async fn build_actors(
        &self,
        actors: &[ActorId],
        env: StreamEnvironment,
    ) -> StreamResult<()> {
        let actors = {
            let mut core = self.core.lock();
            actors
                .iter()
                .map(|actor_id| {
                    core.actors
                        .remove(actor_id)
                        .ok_or_else(|| anyhow!("No such actor with actor id:{}", actor_id))
                })
                .try_collect()?
        };
        let actors = self.create_actors(actors, env).await?;
        self.core.lock().spawn_actors(
            actors,
            &self.streaming_metrics,
            &self.local_barrier_manager,
            self.await_tree_reg.as_ref(),
        );
        Ok(())
    }

    pub fn context(&self) -> &Arc<SharedContext> {
        &self.context
    }

    pub fn total_mem_usage(&self) -> usize {
        self.total_mem_val.get() as usize
    }
}

impl LocalStreamManagerCore {
    fn new(actor_runtime_worker_threads_num: Option<usize>) -> Self {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = actor_runtime_worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-streaming")
                .enable_all()
                .build()
                .unwrap()
        };

        Self {
            runtime: runtime.into(),
            handles: HashMap::new(),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
        }
    }
}

impl LocalStreamManager {
    /// Create dispatchers with downstream information registered before
    fn create_dispatcher(
        &self,
        input: BoxedExecutor,
        dispatchers: &[stream_plan::Dispatcher],
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> StreamResult<DispatchExecutor> {
        let dispatcher_impls = dispatchers
            .iter()
            .map(|dispatcher| DispatcherImpl::new(&self.context, actor_id, dispatcher))
            .try_collect()?;

        Ok(DispatchExecutor::new(
            input,
            dispatcher_impls,
            actor_id,
            fragment_id,
            self.context.clone(),
            self.streaming_metrics.clone(),
        ))
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
    ) -> StreamResult<BoxedExecutor> {
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
                )
                .await?,
            );
        }

        let op_info = node.get_identity().clone();
        let pk_indices = node
            .get_stream_key()
            .iter()
            .map(|idx| *idx as usize)
            .collect::<Vec<_>>();

        // We assume that the operator_id of different instances from the same RelNode will be the
        // same.
        let executor_id = unique_executor_id(actor_context.id, node.operator_id);
        let operator_id = unique_operator_id(fragment_id, node.operator_id);
        let schema: Schema = node.fields.iter().map(Field::from).collect();

        let identity = format!("{} {:X}", node.get_node_body().unwrap(), executor_id);
        let eval_error_report = ActorEvalErrorReport {
            actor_context: actor_context.clone(),
            identity: identity.clone().into(),
        };

        // Build the executor with params.
        let executor_params = ExecutorParams {
            env: env.clone(),

            info: ExecutorInfo {
                schema: schema.clone(),
                pk_indices: pk_indices.clone(),
                identity: identity.clone(),
            },

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
            shared_context: self.context.clone(),
            local_barrier_manager: self.local_barrier_manager.clone(),
        };

        let executor = create_executor(executor_params, node, store).await?;
        assert_eq!(
            executor.pk_indices(),
            &pk_indices,
            "`pk_indices` of {} not consistent with what derived by optimizer",
            executor.identity()
        );
        assert_eq!(
            executor.schema(),
            &schema,
            "`schema` of {} not consistent with what derived by optimizer",
            executor.identity()
        );

        // Wrap the executor for debug purpose.
        let executor = WrapperExecutor::new(
            executor,
            actor_context.clone(),
            env.config().developer.enable_executor_row_count,
        )
        .boxed();

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
    async fn create_nodes(
        &self,
        fragment_id: FragmentId,
        node: &stream_plan::StreamNode,
        env: StreamEnvironment,
        actor_context: &ActorContextRef,
        vnode_bitmap: Option<Bitmap>,
    ) -> StreamResult<(BoxedExecutor, Vec<SubtaskHandle>)> {
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
            )
            .await
        })?;

        Ok((executor, subtasks))
    }

    async fn create_actors(
        &self,
        actors: Vec<StreamActor>,
        env: StreamEnvironment,
    ) -> StreamResult<Vec<Actor<DispatchExecutor>>> {
        let mut ret = Vec::with_capacity(actors.len());
        for actor in actors {
            let actor_id = actor.actor_id;
            let actor_context = ActorContext::create(
                &actor,
                self.total_mem_val.clone(),
                self.streaming_metrics.clone(),
                env.config().unique_user_stream_errors,
                actor.dispatcher.len(),
            );
            let vnode_bitmap = actor.vnode_bitmap.as_ref().map(|b| b.into());
            let expr_context = actor.expr_context.clone().unwrap();

            let (executor, subtasks) = self
                .create_nodes(
                    actor.fragment_id,
                    actor.get_nodes()?,
                    env.clone(),
                    &actor_context,
                    vnode_bitmap,
                )
                // If hummock tracing is not enabled, it directly returns wrapped future.
                .may_trace_hummock()
                .await?;

            let dispatcher =
                self.create_dispatcher(executor, &actor.dispatcher, actor_id, actor.fragment_id)?;
            let actor = Actor::new(
                dispatcher,
                subtasks,
                self.streaming_metrics.clone(),
                actor_context.clone(),
                expr_context,
                self.local_barrier_manager.clone(),
            );

            ret.push(actor);
        }
        Ok(ret)
    }
}

impl LocalStreamManagerCore {
    fn spawn_actors(
        &mut self,
        actors: Vec<Actor<DispatchExecutor>>,
        streaming_metrics: &Arc<StreamingMetrics>,
        barrier_manager: &LocalBarrierManager,
        await_tree_reg: Option<&Mutex<await_tree::Registry<ActorId>>>,
    ) {
        for actor in actors {
            let monitor = tokio_metrics::TaskMonitor::new();
            let actor_context = actor.actor_context.clone();
            let actor_id = actor_context.id;

            let handle = {
                let trace_span = format!("Actor {actor_id}: `{}`", actor_context.mview_definition);
                let barrier_manager = barrier_manager.clone();
                let actor = actor.run().map(move |result| {
                    if let Err(err) = result {
                        // TODO: check error type and panic if it's unexpected.
                        // Intentionally use `?` on the report to also include the backtrace.
                        tracing::error!(actor_id, error = ?err.as_report(), "actor exit with error");
                        barrier_manager.notify_failure(actor_id, err);
                    }
                });
                let traced = match await_tree_reg {
                    Some(m) => m
                        .lock()
                        .register(actor_id, trace_span)
                        .instrument(actor)
                        .left_future(),
                    None => actor.right_future(),
                };
                let instrumented = monitor.instrument(traced);
                #[cfg(enable_task_local_alloc)]
                {
                    let metrics = streaming_metrics.clone();
                    let actor_id_str = actor_id.to_string();
                    let fragment_id_str = actor_context.fragment_id.to_string();
                    let allocation_stated = task_stats_alloc::allocation_stat(
                        instrumented,
                        Duration::from_millis(1000),
                        move |bytes| {
                            metrics
                                .actor_memory_usage
                                .with_label_values(&[&actor_id_str, &fragment_id_str])
                                .set(bytes as i64);

                            actor_context.store_mem_usage(bytes);
                        },
                    );
                    self.runtime.spawn(allocation_stated)
                }
                #[cfg(not(enable_task_local_alloc))]
                {
                    self.runtime.spawn(instrumented)
                }
            };
            self.handles.insert(actor_id, handle);

            if streaming_metrics.level >= MetricLevel::Debug {
                tracing::info!("Tokio metrics are enabled because metrics_level >= Debug");
                let actor_id_str = actor_id.to_string();
                let metrics = streaming_metrics.clone();
                let actor_monitor_task = self.runtime.spawn(async move {
                    loop {
                        let task_metrics = monitor.cumulative();
                        metrics
                            .actor_execution_time
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_fast_poll_duration.as_secs_f64());
                        metrics
                            .actor_fast_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_fast_poll_count as i64);
                        metrics
                            .actor_slow_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_slow_poll_duration.as_secs_f64());
                        metrics
                            .actor_slow_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_slow_poll_count as i64);
                        metrics
                            .actor_poll_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_duration.as_secs_f64());
                        metrics
                            .actor_poll_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_poll_count as i64);
                        metrics
                            .actor_idle_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_idle_duration.as_secs_f64());
                        metrics
                            .actor_idle_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_idled_count as i64);
                        metrics
                            .actor_scheduled_duration
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_scheduled_duration.as_secs_f64());
                        metrics
                            .actor_scheduled_cnt
                            .with_label_values(&[&actor_id_str])
                            .set(task_metrics.total_scheduled_count as i64);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                self.actor_monitor_tasks
                    .insert(actor_id, actor_monitor_task);
            }
        }
    }

    pub fn take_all_handles(&mut self) -> StreamResult<HashMap<ActorId, ActorHandle>> {
        Ok(std::mem::take(&mut self.handles))
    }

    pub fn remove_actor_handles(
        &mut self,
        actor_ids: &[ActorId],
    ) -> StreamResult<Vec<ActorHandle>> {
        actor_ids
            .iter()
            .map(|actor_id| {
                self.handles
                    .remove(actor_id)
                    .ok_or_else(|| anyhow!("No such actor with actor id:{}", actor_id).into())
            })
            .try_collect()
    }
}

impl LocalStreamManager {
    /// This function could only be called once during the lifecycle of `LocalStreamManager` for
    /// now.
    pub fn update_actor_info(&self, new_actor_infos: &[ActorInfo]) -> StreamResult<()> {
        let mut actor_infos = self.context.actor_infos.write();
        for actor in new_actor_infos {
            let ret = actor_infos.insert(actor.get_actor_id(), actor.clone());
            if let Some(prev_actor) = ret
                && actor != &prev_actor
            {
                bail!(
                    "actor info mismatch when broadcasting {}",
                    actor.get_actor_id()
                );
            }
        }
        Ok(())
    }
}

impl LocalStreamManagerCore {
    /// `drop_actor` is invoked by meta node via RPC once the stop barrier arrives at the
    /// sink. All the actors in the actors should stop themselves before this method is invoked.
    fn drop_actor(&mut self, actor_id: ActorId) {
        self.actor_monitor_tasks
            .remove(&actor_id)
            .inspect(|handle| handle.abort());
        self.actors.remove(&actor_id);

        // Task should have already stopped when this method is invoked. There might be some
        // clean-up work left (like dropping in-memory data structures), but we don't have to wait
        // for them to finish, in order to make this request non-blocking.
        self.handles.remove(&actor_id);
    }

    fn drain_actor_handles(&mut self) -> Vec<(ActorId, ActorHandle)> {
        self.handles.drain().collect()
    }

    /// `stop_all_actors` is invoked by meta node via RPC for recovery purpose. Different from the
    /// `drop_actor`, the execution of the actors will be aborted.
    fn clear_state(&mut self) {
        self.actors.clear();
        self.actor_monitor_tasks.clear();
    }

    fn update_actors(&mut self, actors: &[stream_plan::StreamActor]) -> StreamResult<()> {
        for actor in actors {
            self.actors
                .try_insert(actor.get_actor_id(), actor.clone())
                .map_err(|_| anyhow!("duplicated actor {}", actor.get_actor_id()))?;
        }

        Ok(())
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
