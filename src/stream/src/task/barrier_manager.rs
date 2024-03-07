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
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use parking_lot::Mutex;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;
use rw_futures_util::{pending_on_none, AttachedFuture};
use thiserror_ext::AsReport;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use self::managed_state::ManagedBarrierState;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::task::{
    ActorHandle, ActorId, AtomicU64Ref, SharedContext, StreamEnvironment, UpDownActorIds,
};

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgress;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_storage::store::SyncResult;

use crate::executor::exchange::permit::Receiver;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Actor, Barrier, DispatchExecutor};
use crate::task::barrier_manager::progress::BackfillState;

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service.
#[derive(Debug)]
pub struct BarrierCompleteResult {
    /// The result returned from `sync` of `StateStore`.
    pub sync_result: Option<SyncResult>,

    /// The updated creation progress of materialized view after this barrier.
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
}

pub(super) enum LocalBarrierEvent {
    RegisterSender {
        actor_id: ActorId,
        sender: UnboundedSender<Barrier>,
    },
    ReportActorCollected {
        actor_id: ActorId,
        barrier: Barrier,
    },
    ReportCreateProgress {
        current_epoch: u64,
        actor: ActorId,
        state: BackfillState,
    },
    #[cfg(test)]
    Flush(oneshot::Sender<()>),
}

pub(super) enum LocalActorOperation {
    InjectBarrier {
        barrier: Barrier,
        actor_ids_to_send: HashSet<ActorId>,
        actor_ids_to_collect: HashSet<ActorId>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    Reset {
        prev_epoch: u64,
        result_sender: oneshot::Sender<()>,
    },
    AwaitEpochCompleted {
        epoch: u64,
        result_sender: oneshot::Sender<StreamResult<BarrierCompleteResult>>,
    },
    DropActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<()>,
    },
    UpdateActors {
        actors: Vec<stream_plan::StreamActor>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    BuildActors {
        actors: Vec<ActorId>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    UpdateActorInfo {
        new_actor_infos: Vec<ActorInfo>,
        result_sender: oneshot::Sender<StreamResult<()>>,
    },
    TakeReceiver {
        ids: UpDownActorIds,
        result_sender: oneshot::Sender<StreamResult<Receiver>>,
    },
    #[cfg(test)]
    GetCurrentSharedContext(oneshot::Sender<Arc<SharedContext>>),
}

pub(crate) struct StreamActorManagerState {
    /// Each processor runs in a future. Upon receiving a `Terminate` message, they will exit.
    /// `handles` store join handles of these futures, and therefore we could wait their
    /// termination.
    pub(super) handles: HashMap<ActorId, ActorHandle>,

    /// Stores all actor information, taken after actor built.
    pub(super) actors: HashMap<ActorId, stream_plan::StreamActor>,

    /// Stores all actor tokio runtime monitoring tasks.
    pub(super) actor_monitor_tasks: HashMap<ActorId, ActorHandle>,

    #[expect(clippy::type_complexity)]
    pub(super) creating_actors: FuturesUnordered<
        AttachedFuture<
            JoinHandle<StreamResult<Vec<Actor<DispatchExecutor>>>>,
            oneshot::Sender<StreamResult<()>>,
        >,
    >,
}

impl StreamActorManagerState {
    fn new() -> Self {
        Self {
            handles: HashMap::new(),
            actors: HashMap::new(),
            actor_monitor_tasks: HashMap::new(),
            creating_actors: FuturesUnordered::new(),
        }
    }

    async fn next_created_actors(
        &mut self,
    ) -> (
        oneshot::Sender<StreamResult<()>>,
        StreamResult<Vec<Actor<DispatchExecutor>>>,
    ) {
        let (join_result, sender) = pending_on_none(self.creating_actors.next()).await;
        (
            sender,
            try { join_result.context("failed to join creating actors futures")?? },
        )
    }
}

pub(crate) struct StreamActorManager {
    pub(super) env: StreamEnvironment,
    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Watermark epoch number.
    pub(super) watermark_epoch: AtomicU64Ref,

    /// Manages the await-trees of all actors.
    pub(super) await_tree_reg: Option<Arc<Mutex<await_tree::Registry<ActorId>>>>,

    /// Runtime for the streaming actors.
    pub(super) runtime: BackgroundShutdownRuntime,
}

/// [`LocalBarrierWorker`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierWorker`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub(super) struct LocalBarrierWorker {
    /// Stores all streaming job source sender.
    barrier_senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,

    /// Current barrier collection state.
    state: ManagedBarrierState,

    /// Record all unexpected exited actors.
    failure_actors: HashMap<ActorId, StreamError>,

    epoch_result_sender: HashMap<u64, oneshot::Sender<StreamResult<BarrierCompleteResult>>>,

    pub(super) actor_manager: Arc<StreamActorManager>,

    pub(super) actor_manager_state: StreamActorManagerState,

    pub(super) current_shared_context: Arc<SharedContext>,

    barrier_event_rx: UnboundedReceiver<LocalBarrierEvent>,

    actor_failure_rx: UnboundedReceiver<(ActorId, StreamError)>,

    root_failure: Option<StreamError>,
}

impl LocalBarrierWorker {
    pub(super) fn new(actor_manager: Arc<StreamActorManager>) -> Self {
        let (event_tx, event_rx) = unbounded_channel();
        let (failure_tx, failure_rx) = unbounded_channel();
        let shared_context = Arc::new(SharedContext::new(
            actor_manager.env.server_address().clone(),
            actor_manager.env.config(),
            LocalBarrierManager {
                barrier_event_sender: event_tx,
                actor_failure_sender: failure_tx,
            },
        ));
        Self {
            barrier_senders: HashMap::new(),
            failure_actors: HashMap::default(),
            state: ManagedBarrierState::new(
                actor_manager.env.state_store(),
                actor_manager.streaming_metrics.clone(),
            ),
            epoch_result_sender: HashMap::default(),
            actor_manager,
            actor_manager_state: StreamActorManagerState::new(),
            current_shared_context: shared_context,
            barrier_event_rx: event_rx,
            actor_failure_rx: failure_rx,
            root_failure: None,
        }
    }

    async fn run(mut self, mut actor_op_rx: UnboundedReceiver<LocalActorOperation>) {
        loop {
            select! {
                biased;
                (sender, create_actors_result) = self.actor_manager_state.next_created_actors() => {
                    self.handle_actor_created(sender, create_actors_result);
                }
                completed_epoch = self.state.next_completed_epoch() => {
                    self.on_epoch_completed(completed_epoch);
                },
                // Note: it's important to select in a biased way to ensure that
                // barrier event is handled before actor_op, because we must ensure
                // that register sender is handled before inject barrier.
                event = self.barrier_event_rx.recv() => {
                    self.handle_barrier_event(event.expect("should not be none"));
                },
                failure = self.actor_failure_rx.recv() => {
                    let (actor_id, err) = failure.unwrap();
                    self.notify_failure(actor_id, err).await;
                },
                actor_op = actor_op_rx.recv() => {
                    if let Some(actor_op) = actor_op {
                        match actor_op {
                            LocalActorOperation::Reset {
                                result_sender, prev_epoch} => {
                                self.reset(prev_epoch).await;
                                let _ = result_sender.send(());
                            }
                            actor_op => {
                                self.handle_actor_op(actor_op);
                            }
                        }
                    }
                    else {
                        break;
                    }
                }
            }
        }
    }

    fn handle_actor_created(
        &mut self,
        sender: oneshot::Sender<StreamResult<()>>,
        create_actor_result: StreamResult<Vec<Actor<DispatchExecutor>>>,
    ) {
        let result = create_actor_result.map(|actors| {
            self.spawn_actors(actors);
        });

        let _ = sender.send(result);
    }

    fn handle_barrier_event(&mut self, event: LocalBarrierEvent) {
        match event {
            LocalBarrierEvent::RegisterSender { actor_id, sender } => {
                self.register_sender(actor_id, sender);
            }
            LocalBarrierEvent::ReportActorCollected { actor_id, barrier } => {
                self.collect(actor_id, &barrier)
            }
            LocalBarrierEvent::ReportCreateProgress {
                current_epoch,
                actor,
                state,
            } => {
                self.update_create_mview_progress(current_epoch, actor, state);
            }
            #[cfg(test)]
            LocalBarrierEvent::Flush(sender) => sender.send(()).unwrap(),
        }
    }

    fn handle_actor_op(&mut self, actor_op: LocalActorOperation) {
        match actor_op {
            LocalActorOperation::InjectBarrier {
                barrier,
                actor_ids_to_send,
                actor_ids_to_collect,
                result_sender,
            } => {
                let result = self.send_barrier(&barrier, actor_ids_to_send, actor_ids_to_collect);
                let _ = result_sender.send(result).inspect_err(|e| {
                    warn!(err=?e, "fail to send inject barrier result");
                });
            }
            LocalActorOperation::Reset { .. } => {
                unreachable!("Reset event should be handled separately in async context")
            }

            LocalActorOperation::AwaitEpochCompleted {
                epoch,
                result_sender,
            } => {
                self.await_epoch_completed(epoch, result_sender);
            }
            LocalActorOperation::DropActors {
                actors,
                result_sender,
            } => {
                self.drop_actors(&actors);
                let _ = result_sender.send(());
            }
            LocalActorOperation::UpdateActors {
                actors,
                result_sender,
            } => {
                let result = self.update_actors(actors);
                let _ = result_sender.send(result);
            }
            LocalActorOperation::BuildActors {
                actors,
                result_sender,
            } => self.start_create_actors(&actors, result_sender),
            LocalActorOperation::UpdateActorInfo {
                new_actor_infos,
                result_sender,
            } => {
                let _ = result_sender.send(self.update_actor_info(new_actor_infos));
            }
            LocalActorOperation::TakeReceiver { ids, result_sender } => {
                let _ = result_sender.send(self.current_shared_context.take_receiver(ids));
            }
            #[cfg(test)]
            LocalActorOperation::GetCurrentSharedContext(sender) => {
                let _ = sender.send(self.current_shared_context.clone());
            }
        }
    }
}

// event handler
impl LocalBarrierWorker {
    fn on_epoch_completed(&mut self, epoch: u64) {
        if let Some(sender) = self.epoch_result_sender.remove(&epoch) {
            let result = self
                .state
                .pop_completed_epoch(epoch)
                .expect("should exist")
                .expect("should have completed");
            if sender.send(result).is_err() {
                warn!(epoch, "fail to send epoch complete result");
            }
        }
    }

    /// Register sender for source actors, used to send barriers.
    fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        tracing::debug!(
            target: "events::stream::barrier::manager",
            actor_id = actor_id,
            "register sender"
        );
        self.barrier_senders
            .entry(actor_id)
            .or_default()
            .push(sender);
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    fn send_barrier(
        &mut self,
        barrier: &Barrier,
        to_send: HashSet<ActorId>,
        to_collect: HashSet<ActorId>,
    ) -> StreamResult<()> {
        #[cfg(not(test))]
        {
            use itertools::Itertools;
            // The barrier might be outdated and been injected after recovery in some certain extreme
            // scenarios. So some newly creating actors in the barrier are possibly not rebuilt during
            // recovery. Check it here and return an error here if some actors are not found to
            // avoid collection hang. We need some refine in meta side to remove this workaround since
            // it will cause another round of unnecessary recovery.
            let missing_actor_ids = to_collect
                .iter()
                .filter(|id| !self.actor_manager_state.handles.contains_key(id))
                .collect_vec();
            if !missing_actor_ids.is_empty() {
                tracing::warn!(
                    "to collect actors not found, they should be cleaned when recovering: {:?}",
                    missing_actor_ids
                );
                return Err(anyhow!("to collect actors not found: {:?}", to_collect).into());
            }
        }

        if barrier.kind == BarrierKind::Initial {
            self.actor_manager
                .watermark_epoch
                .store(barrier.epoch.curr, std::sync::atomic::Ordering::SeqCst);
        }
        debug!(
            target: "events::stream::barrier::manager::send",
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            to_send,
            to_collect
        );

        // There must be some actors to collect from.
        assert!(!to_collect.is_empty());

        for actor_id in &to_collect {
            if let Some(e) = self.failure_actors.get(actor_id) {
                // The failure actors could exit before the barrier is issued, while their
                // up-downstream actors could be stuck somehow. Return error directly to trigger the
                // recovery.
                // try_find_root_failure is not used merely because it requires async.
                return Err(self.root_failure.clone().unwrap_or(e.clone()));
            }
        }

        self.state.transform_to_issued(barrier, to_collect);

        for actor_id in to_send {
            match self.barrier_senders.get(&actor_id) {
                Some(senders) => {
                    for sender in senders {
                        if let Err(_err) = sender.send(barrier.clone()) {
                            // return err to trigger recovery.
                            return Err(StreamError::barrier_send(
                                barrier.clone(),
                                actor_id,
                                "channel closed",
                            ));
                        }
                    }
                }
                None => {
                    return Err(StreamError::barrier_send(
                        barrier.clone(),
                        actor_id,
                        "sender not found",
                    ));
                }
            }
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(actors) = barrier.all_stop_actors() {
            debug!(
                target: "events::stream::barrier::manager",
                "remove actors {:?} from senders",
                actors
            );
            for actor in actors {
                self.barrier_senders.remove(actor);
            }
        }
        Ok(())
    }

    /// Use `prev_epoch` to remove collect rx and return rx.
    fn await_epoch_completed(
        &mut self,
        prev_epoch: u64,
        result_sender: oneshot::Sender<StreamResult<BarrierCompleteResult>>,
    ) {
        match self.state.pop_completed_epoch(prev_epoch) {
            Err(e) => {
                let _ = result_sender.send(Err(e));
            }
            Ok(Some(result)) => {
                if result_sender.send(result).is_err() {
                    warn!(prev_epoch, "failed to send completed epoch result");
                }
            }
            Ok(None) => {
                if let Some(prev_sender) =
                    self.epoch_result_sender.insert(prev_epoch, result_sender)
                {
                    warn!(?prev_epoch, "duplicate await_collect_barrier on epoch");
                    let _ = prev_sender.send(Err(anyhow!(
                        "duplicate await_collect_barrier on epoch {}",
                        prev_epoch
                    )
                    .into()));
                }
            }
        }
    }

    /// Reset all internal states.
    pub(super) fn reset_state(&mut self) {
        *self = Self::new(self.actor_manager.clone());
    }

    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        self.state.collect(actor_id, barrier)
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    async fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        self.add_failure(actor_id, err.clone());
        let root_err = self.try_find_root_failure(err).await;
        for fail_epoch in self.state.epochs_await_on_actor(actor_id) {
            if let Some(result_sender) = self.epoch_result_sender.remove(&fail_epoch) {
                if result_sender.send(Err(root_err.clone())).is_err() {
                    warn!(fail_epoch, actor_id, err = %root_err.as_report(), "fail to notify actor failure");
                }
            }
        }
    }

    fn add_failure(&mut self, actor_id: ActorId, err: StreamError) {
        let err = err.into_unexpected_exit(actor_id);
        if let Some(prev_err) = self.failure_actors.insert(actor_id, err) {
            warn!(
                actor_id,
                prev_err = %prev_err.as_report(),
                "actor error overwritten"
            );
        }
    }

    async fn try_find_root_failure(&mut self, default_err: StreamError) -> StreamError {
        if let Some(root_failure) = &self.root_failure {
            return root_failure.clone();
        }
        // fetch more actor errors within a timeout
        let _ = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some((actor_id, error)) = self.actor_failure_rx.recv().await {
                self.add_failure(actor_id, error);
            }
        })
        .await;
        self.root_failure = try_find_root_actor_failure(self.failure_actors.values());
        self.root_failure.clone().unwrap_or(default_err)
    }
}

#[derive(Clone)]
pub struct LocalBarrierManager {
    barrier_event_sender: UnboundedSender<LocalBarrierEvent>,
    actor_failure_sender: UnboundedSender<(ActorId, StreamError)>,
}

impl LocalBarrierWorker {
    /// Create a [`LocalBarrierWorker`] with managed mode.
    pub fn spawn(
        env: StreamEnvironment,
        streaming_metrics: Arc<StreamingMetrics>,
        await_tree_reg: Option<Arc<Mutex<await_tree::Registry<ActorId>>>>,
        watermark_epoch: AtomicU64Ref,
        actor_op_rx: UnboundedReceiver<LocalActorOperation>,
    ) -> JoinHandle<()> {
        let runtime = {
            let mut builder = tokio::runtime::Builder::new_multi_thread();
            if let Some(worker_threads_num) = env.config().actor_runtime_worker_threads_num {
                builder.worker_threads(worker_threads_num);
            }
            builder
                .thread_name("rw-streaming")
                .enable_all()
                .build()
                .unwrap()
        };

        let actor_manager = Arc::new(StreamActorManager {
            env: env.clone(),
            streaming_metrics,
            watermark_epoch,
            await_tree_reg,
            runtime: runtime.into(),
        });
        let worker = LocalBarrierWorker::new(actor_manager);
        tokio::spawn(worker.run(actor_op_rx))
    }
}

pub(super) struct EventSender<T>(pub(super) UnboundedSender<T>);

impl<T> Clone for EventSender<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> EventSender<T> {
    pub(super) fn send_event(&self, event: T) {
        self.0.send(event).expect("should be able to send event")
    }

    pub(super) async fn send_and_await<RSP>(
        &self,
        make_event: impl FnOnce(oneshot::Sender<RSP>) -> T,
    ) -> StreamResult<RSP> {
        let (tx, rx) = oneshot::channel();
        let event = make_event(tx);
        self.send_event(event);
        rx.await
            .map_err(|_| anyhow!("barrier manager maybe reset").into())
    }
}

impl LocalBarrierManager {
    fn send_event(&self, event: LocalBarrierEvent) {
        // ignore error, because the current barrier manager maybe a stale one
        let _ = self.barrier_event_sender.send(event);
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        self.send_event(LocalBarrierEvent::RegisterSender { actor_id, sender });
    }
}

impl EventSender<LocalActorOperation> {
    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    pub(super) async fn send_barrier(
        &self,
        barrier: Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> StreamResult<()> {
        self.send_and_await(move |result_sender| LocalActorOperation::InjectBarrier {
            barrier,
            actor_ids_to_send: actor_ids_to_send.into_iter().collect(),
            actor_ids_to_collect: actor_ids_to_collect.into_iter().collect(),
            result_sender,
        })
        .await?
    }

    /// Use `prev_epoch` to remove collect rx and return rx.
    pub(super) async fn await_epoch_completed(
        &self,
        prev_epoch: u64,
    ) -> StreamResult<BarrierCompleteResult> {
        self.send_and_await(|result_sender| LocalActorOperation::AwaitEpochCompleted {
            epoch: prev_epoch,
            result_sender,
        })
        .await?
    }
}

impl LocalBarrierManager {
    /// When a [`crate::executor::StreamConsumer`] (typically [`crate::executor::DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&self, actor_id: ActorId, barrier: &Barrier) {
        self.send_event(LocalBarrierEvent::ReportActorCollected {
            actor_id,
            barrier: barrier.clone(),
        })
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&self, actor_id: ActorId, err: StreamError) {
        let _ = self.actor_failure_sender.send((actor_id, err));
    }
}

/// Tries to find the root cause of actor failures, based on hard-coded rules.
pub fn try_find_root_actor_failure<'a>(
    actor_errors: impl IntoIterator<Item = &'a StreamError>,
) -> Option<StreamError> {
    use crate::executor::StreamExecutorError;
    let stream_executor_error_score = |e: &StreamExecutorError| {
        use crate::executor::error::ErrorKind;
        match e.inner() {
            ErrorKind::ChannelClosed(_) => 0,
            ErrorKind::Internal(_) => 1,
            _ => 999,
        }
    };
    let stream_error_score = |e: &&StreamError| {
        use crate::error::ErrorKind;
        match e.inner() {
            ErrorKind::Internal(_) => 1000,
            ErrorKind::Executor(ee) => 2000 + stream_executor_error_score(ee),
            _ => 3000,
        }
    };
    actor_errors
        .into_iter()
        .max_by_key(stream_error_score)
        .cloned()
}

#[cfg(test)]
impl LocalBarrierManager {
    pub(super) async fn spawn_for_test() -> (EventSender<LocalActorOperation>, Self) {
        use std::sync::atomic::AtomicU64;
        let (tx, rx) = unbounded_channel();
        let _join_handle = LocalBarrierWorker::spawn(
            StreamEnvironment::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
            Arc::new(AtomicU64::new(0)),
            rx,
        );
        let sender = EventSender(tx);
        let context = sender
            .send_and_await(LocalActorOperation::GetCurrentSharedContext)
            .await
            .unwrap();

        (sender, context.local_barrier_manager.clone())
    }

    pub fn for_test() -> Self {
        let (tx, mut rx) = unbounded_channel();
        let (failure_tx, failure_rx) = unbounded_channel();
        let _join_handle = tokio::spawn(async move {
            let _failure_rx = failure_rx;
            while rx.recv().await.is_some() {}
        });
        Self {
            barrier_event_sender: tx,
            actor_failure_sender: failure_tx,
        }
    }

    pub async fn flush_all_events(&self) {
        let (tx, rx) = oneshot::channel();
        self.send_event(LocalBarrierEvent::Flush(tx));
        rx.await.unwrap()
    }
}
