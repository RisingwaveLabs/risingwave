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

use std::assert_matches::assert_matches;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::{poll_fn, Future};
use std::mem::replace;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, StreamExt, TryFutureExt};
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::SyncResult;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::PartialGraphInfo;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::progress::BackfillState;
use super::{BarrierCompleteResult, SubscribeMutationItem};
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, Mutation};
use crate::task::barrier_manager::managed_state::actor_status::InflightActorState;
use crate::task::{await_tree_key, ActorId, PartialGraphId};

struct IssuedState {
    pub mutation: Option<Arc<Mutation>>,
    /// Actor ids remaining to be collected.
    pub remaining_actors: BTreeSet<ActorId>,

    pub barrier_inflight_latency: HistogramTimer,

    /// Only be `Some(_)` when `kind` is `Checkpoint`
    pub table_ids: Option<HashSet<TableId>>,

    pub kind: BarrierKind,
}

impl Debug for IssuedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedState")
            .field("mutation", &self.mutation)
            .field("remaining_actors", &self.remaining_actors)
            .field("table_ids", &self.table_ids)
            .field("kind", &self.kind)
            .finish()
    }
}

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued(IssuedState),

    /// The barrier has been collected by all remaining actors
    AllCollected,

    /// The barrier has been completed, which means the barrier has been collected by all actors and
    /// synced in state store
    Completed(StreamResult<BarrierCompleteResult>),
}

#[derive(Debug)]
pub(super) struct BarrierState {
    curr_epoch: u64,
    inner: ManagedBarrierStateInner,
}

type AwaitEpochCompletedFuture =
    impl Future<Output = (u64, StreamResult<BarrierCompleteResult>)> + 'static;

fn sync_epoch<S: StateStore>(
    state_store: &S,
    streaming_metrics: &StreamingMetrics,
    prev_epoch: u64,
    table_ids: HashSet<TableId>,
) -> BoxFuture<'static, StreamResult<SyncResult>> {
    let timer = streaming_metrics.barrier_sync_latency.start_timer();
    let future = state_store.sync(prev_epoch, table_ids);
    future
        .instrument_await(format!("sync_epoch (epoch {})", prev_epoch))
        .inspect_ok(move |_| {
            timer.observe_duration();
        })
        .map_err(move |e| {
            tracing::error!(
                prev_epoch,
                error = %e.as_report(),
                "Failed to sync state store",
            );
            e.into()
        })
        .boxed()
}

pub(super) struct ManagedBarrierStateDebugInfo<'a> {
    graph_states: &'a HashMap<PartialGraphId, PartialGraphManagedBarrierState>,
}

impl Display for ManagedBarrierStateDebugInfo<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (partial_graph_id, graph_states) in self.graph_states {
            writeln!(f, "--- Partial Group {}", partial_graph_id.0)?;
            write!(f, "{}", graph_states)?;
        }
        Ok(())
    }
}

impl Display for &'_ PartialGraphManagedBarrierState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut prev_epoch = 0u64;
        for (epoch, barrier_state) in &self.epoch_barrier_state_map {
            write!(f, "> Epoch {}: ", epoch)?;
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(state) => {
                    write!(f, "Issued [{:?}]. Remaining actors: [", state.kind)?;
                    let mut is_prev_epoch_issued = false;
                    if prev_epoch != 0 {
                        let bs = &self.epoch_barrier_state_map[&prev_epoch];
                        if let ManagedBarrierStateInner::Issued(IssuedState {
                            remaining_actors: remaining_actors_prev,
                            ..
                        }) = &bs.inner
                        {
                            // Only show the actors that are not in the previous epoch.
                            is_prev_epoch_issued = true;
                            let mut duplicates = 0usize;
                            for actor_id in &state.remaining_actors {
                                if !remaining_actors_prev.contains(actor_id) {
                                    write!(f, "{}, ", actor_id)?;
                                } else {
                                    duplicates += 1;
                                }
                            }
                            if duplicates > 0 {
                                write!(f, "...and {} actors in prev epoch", duplicates)?;
                            }
                        }
                    }
                    if !is_prev_epoch_issued {
                        for actor_id in &state.remaining_actors {
                            write!(f, "{}, ", actor_id)?;
                        }
                    }
                    write!(f, "]")?;
                }
                ManagedBarrierStateInner::AllCollected => {
                    write!(f, "AllCollected")?;
                }
                ManagedBarrierStateInner::Completed(_) => {
                    write!(f, "Completed")?;
                }
            }
            prev_epoch = *epoch;
            writeln!(f)?;
        }

        if !self.create_mview_progress.is_empty() {
            writeln!(f, "Create MView Progress:")?;
            for (epoch, progress) in &self.create_mview_progress {
                write!(f, "> Epoch {}:", epoch)?;
                for (actor_id, state) in progress {
                    write!(f, ">> Actor {}: {}, ", actor_id, state)?;
                }
            }
        }

        Ok(())
    }
}

mod actor_status {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use risingwave_common::must_match;
    use risingwave_common::util::epoch::EpochPair;
    use tokio::sync::mpsc;

    use crate::executor::{Barrier, Mutation};
    use crate::task::{PartialGraphId, SubscribeMutationItem};

    enum InflightActorStatus {
        /// The actor has not been issued any barrier yet
        NotStarted,
        /// The actor has been issued some barriers, but has collected all the barrier.
        /// Waiting for new barrier to issue.
        Pending {
            /// The latest `partial_graph_id` before entering `Pending` status.
            /// The actor should be in the `inflight_actors` of the graph.
            prev_partial_graph_id: PartialGraphId,
            /// The `prev_epoch` of the previous barrier
            prev_epoch: u64,
        },
        /// The actor has been issued with some barriers, and waiting for collecting some barriers.
        Running {
            /// `prev_epoch` -> partial graph id
            /// Store the barriers that has been issued but not collected.
            /// Must be non-empty when in this variant, or transit to `Pending`, or the states gets removed when stopped.
            ///
            /// The actor should be in the `inflight_actors` of graph whose `partial_graph_id` of the first graph id.
            inflight_barriers: BTreeMap<u64, (PartialGraphId, Option<Arc<Mutation>>)>,
            /// Whether the actor has been issued a stop barrier
            is_stopping: bool,
        },
    }

    pub(crate) struct InflightActorState {
        pending_subscribers: BTreeMap<u64, Vec<mpsc::UnboundedSender<SubscribeMutationItem>>>,
        started_subscribers: Vec<mpsc::UnboundedSender<SubscribeMutationItem>>,
        status: InflightActorStatus,
    }

    impl InflightActorState {
        pub(super) fn not_started() -> Self {
            Self {
                pending_subscribers: Default::default(),
                started_subscribers: vec![],
                status: InflightActorStatus::NotStarted,
            }
        }

        #[expect(clippy::type_complexity)]
        pub(crate) fn inflight_barriers(
            &self,
        ) -> Option<&BTreeMap<u64, (PartialGraphId, Option<Arc<Mutation>>)>> {
            if let InflightActorStatus::Running {
                inflight_barriers, ..
            } = &self.status
            {
                Some(inflight_barriers)
            } else {
                None
            }
        }

        pub(super) fn subscribe_actor_mutation(
            &mut self,
            start_prev_epoch: u64,
            tx: mpsc::UnboundedSender<SubscribeMutationItem>,
        ) {
            match &self.status {
                InflightActorStatus::NotStarted => {
                    self.pending_subscribers
                        .entry(start_prev_epoch)
                        .or_default()
                        .push(tx);
                }
                InflightActorStatus::Pending { prev_epoch, .. } => {
                    assert!(*prev_epoch < start_prev_epoch);
                    self.pending_subscribers
                        .entry(start_prev_epoch)
                        .or_default()
                        .push(tx);
                }
                InflightActorStatus::Running {
                    inflight_barriers,
                    is_stopping,
                    ..
                } => {
                    if inflight_barriers.contains_key(&start_prev_epoch) {
                        for (prev_epoch, (_, mutation)) in
                            inflight_barriers.range(start_prev_epoch..)
                        {
                            if tx.send((*prev_epoch, mutation.clone())).is_err() {
                                // No more subscribe on the mutation. Simply return.
                                return;
                            }
                        }
                        if !*is_stopping {
                            self.started_subscribers.push(tx);
                        }
                    } else {
                        // Barrier has not issued yet. Store the pending tx
                        if let Some((last_epoch, _)) = inflight_barriers.last_key_value() {
                            assert!(
                                *last_epoch < start_prev_epoch,
                                "later barrier {} has been issued, but skip the start epoch {:?}",
                                last_epoch,
                                start_prev_epoch
                            );
                        }
                        self.pending_subscribers
                            .entry(start_prev_epoch)
                            .or_default()
                            .push(tx);
                    }
                }
            }
        }

        #[must_use]
        pub(super) fn issue_barrier(
            &mut self,
            partial_graph_id: PartialGraphId,
            barrier: &Barrier,
            is_stop: bool,
        ) -> (
            // Some(prev_partial_graph_id) when the actor was in status
            // InflightActorStatus::Pending { .. } with a different graph id
            Option<PartialGraphId>,
            // whether the actor is new to the `partial_graph_id`
            bool,
        ) {
            let (prev_partial_graph_id, is_new_in_graph) = match &self.status {
                InflightActorStatus::NotStarted => {
                    self.status = InflightActorStatus::Running {
                        inflight_barriers: Default::default(),
                        is_stopping: false,
                    };
                    (None, true)
                }
                InflightActorStatus::Pending {
                    prev_partial_graph_id,
                    prev_epoch,
                } => {
                    assert!(*prev_epoch < barrier.epoch.prev);
                    let prev_partial_graph_id = *prev_partial_graph_id;
                    self.status = InflightActorStatus::Running {
                        inflight_barriers: Default::default(),
                        is_stopping: false,
                    };
                    if prev_partial_graph_id != partial_graph_id {
                        (Some(prev_partial_graph_id), true)
                    } else {
                        (None, false)
                    }
                }
                InflightActorStatus::Running {
                    inflight_barriers, ..
                } => {
                    let (prev_epoch, (prev_partial_graph_id, _)) =
                        inflight_barriers.last_key_value().expect("non-empty");
                    assert!(*prev_epoch < barrier.epoch.prev);
                    (None, *prev_partial_graph_id != partial_graph_id)
                }
            };

            if let Some((first_epoch, _)) = self.pending_subscribers.first_key_value() {
                assert!(
                    *first_epoch >= barrier.epoch.prev,
                    "barrier epoch {:?} skip subscribed epoch {}",
                    barrier.epoch,
                    first_epoch
                );
                if *first_epoch == barrier.epoch.prev {
                    self.started_subscribers.extend(
                        self.pending_subscribers
                            .pop_first()
                            .expect("should exist")
                            .1,
                    );
                }
            }
            self.started_subscribers.retain(|tx| {
                tx.send((barrier.epoch.prev, barrier.mutation.clone()))
                    .is_ok()
            });

            must_match!(&mut self.status, InflightActorStatus::Running {
                inflight_barriers, is_stopping,
            } => {
                inflight_barriers.insert(barrier.epoch.prev, (partial_graph_id, barrier.mutation.clone()));
                *is_stopping = is_stop;
            });

            (prev_partial_graph_id, is_new_in_graph)
        }

        #[must_use]
        pub(super) fn collect(
            &mut self,
            epoch: EpochPair,
        ) -> (
            // The `partial_graph_id` of actor on the collected epoch
            PartialGraphId,
            // None => the partial graph id of this actor is not changed
            // Some(None) => the actor has stopped, and should be removed from the return `partial_graph_id`
            // Some(Some(new_partial_graph_id)) => the actor will move to the `new_partial_graph_id`
            Option<Option<(PartialGraphId, u64)>>,
        ) {
            must_match!(&mut self.status, InflightActorStatus::Running {
                inflight_barriers, is_stopping
            } => {
                let (prev_epoch, (prev_partial_graph_id, _)) = inflight_barriers.pop_first().expect("should exist");
                assert_eq!(prev_epoch, epoch.prev);
                let move_to_graph_id = if let Some((epoch, (graph_id, _))) = inflight_barriers.first_key_value() {
                    if *graph_id != prev_partial_graph_id {
                        Some(Some((prev_partial_graph_id, *epoch)))
                    } else {
                        None
                    }
                } else if *is_stopping {
                    Some(None)
                } else {
                    self.status = InflightActorStatus::Pending {prev_epoch, prev_partial_graph_id};
                    // No need to move to any partial graph when transit to `Pending`. When issuing the next barrier and
                    // the next graph id gets different, the actor will then move to the next graph id
                    None
                };
                (prev_partial_graph_id, move_to_graph_id)
            })
        }
    }
}

pub(super) struct PartialGraphManagedBarrierState {
    /// This is a temporary workaround for the need to still calling `seal_epoch` for storage.
    /// Can be removed after `seal_epoch` is deprecated in storage.
    need_seal_epoch: bool,
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is `prev_epoch`, and the first value is `curr_epoch`
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    inflight_actors: HashSet<ActorId>,

    prev_barrier_table_ids: Option<(EpochPair, HashSet<TableId>)>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    /// Futures will be finished in the order of epoch in ascending order.
    await_epoch_completed_futures: FuturesOrdered<AwaitEpochCompletedFuture>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Manages the await-trees of all barriers.
    barrier_await_tree_reg: Option<await_tree::Registry>,
}

impl PartialGraphManagedBarrierState {
    fn new(
        need_seal_epoch: bool,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        barrier_await_tree_reg: Option<await_tree::Registry>,
    ) -> Self {
        Self {
            need_seal_epoch,
            epoch_barrier_state_map: Default::default(),
            inflight_actors: Default::default(),
            prev_barrier_table_ids: None,
            create_mview_progress: Default::default(),
            await_epoch_completed_futures: Default::default(),
            state_store,
            streaming_metrics,
            barrier_await_tree_reg,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            true,
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
        )
    }
}

pub(super) struct ManagedBarrierState {
    pub(super) actor_states: HashMap<ActorId, InflightActorState>,

    pub(super) graph_states: HashMap<PartialGraphId, PartialGraphManagedBarrierState>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Manages the await-trees of all barriers.
    barrier_await_tree_reg: Option<await_tree::Registry>,
}

impl ManagedBarrierState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        barrier_await_tree_reg: Option<await_tree::Registry>,
    ) -> Self {
        Self {
            actor_states: Default::default(),
            graph_states: Default::default(),
            state_store,
            streaming_metrics,
            barrier_await_tree_reg,
        }
    }

    pub(super) fn to_debug_info(&self) -> ManagedBarrierStateDebugInfo<'_> {
        ManagedBarrierStateDebugInfo {
            graph_states: &self.graph_states,
        }
    }

    pub(super) fn subscribe_actor_mutation(
        &mut self,
        actor_id: ActorId,
        start_prev_epoch: u64,
        tx: mpsc::UnboundedSender<SubscribeMutationItem>,
    ) {
        self.actor_states
            .entry(actor_id)
            .or_insert_with(InflightActorState::not_started)
            .subscribe_actor_mutation(start_prev_epoch, tx);
    }

    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        graph_infos: &HashMap<u32, PartialGraphInfo>,
    ) {
        let actor_to_stop = barrier.all_stop_actors();
        let mut graph_actors_to_clean: HashMap<PartialGraphId, Vec<ActorId>> = HashMap::new();
        for (partial_graph_id, graph_info) in graph_infos {
            let partial_graph_id = PartialGraphId::new(*partial_graph_id);

            let graph_state = self
                .graph_states
                .entry(partial_graph_id)
                .or_insert_with(|| {
                    PartialGraphManagedBarrierState::new(
                        partial_graph_id.0 == u32::MAX,
                        self.state_store.clone(),
                        self.streaming_metrics.clone(),
                        self.barrier_await_tree_reg.clone(),
                    )
                });

            graph_state.transform_to_issued(
                barrier,
                graph_info.actor_ids_to_collect.iter().cloned().collect(),
                graph_info
                    .table_ids_to_sync
                    .iter()
                    .map(|table_id| TableId::new(*table_id))
                    .collect(),
            );

            for actor_id in &graph_info.actor_ids_to_collect {
                let (prev_partial_graph_id, is_new_in_graph) = self
                    .actor_states
                    .entry(*actor_id)
                    .or_insert_with(InflightActorState::not_started)
                    .issue_barrier(
                        partial_graph_id,
                        barrier,
                        actor_to_stop
                            .map(|actors| actors.contains(actor_id))
                            .unwrap_or(false),
                    );
                if is_new_in_graph {
                    graph_state.add_inflight_actor(*actor_id, barrier.epoch.prev);
                }
                if let Some(prev_partial_graph_id) = prev_partial_graph_id {
                    graph_actors_to_clean
                        .entry(prev_partial_graph_id)
                        .or_default()
                        .push(*actor_id);
                }
            }
        }

        for (graph_id, actors_to_clean) in graph_actors_to_clean {
            let graph_state = self.graph_states.get_mut(&graph_id).expect("should exist");
            if graph_state.remove_inflight_actors(actors_to_clean) {
                self.graph_states.remove(&graph_id);
            }
        }
    }

    pub(super) fn next_completed_epoch(
        &mut self,
    ) -> impl Future<Output = (PartialGraphId, u64)> + '_ {
        poll_fn(|cx| {
            for (partial_graph_id, graph_state) in &mut self.graph_states {
                if let Poll::Ready(epoch) = graph_state.poll_next_completed_epoch(cx) {
                    let partial_graph_id = *partial_graph_id;
                    if graph_state.is_empty() {
                        self.graph_states.remove(&partial_graph_id);
                    }
                    return Poll::Ready((partial_graph_id, epoch));
                }
            }
            Poll::Pending
        })
    }

    pub(super) fn collect(&mut self, actor_id: ActorId, epoch: EpochPair) {
        let (prev_partial_graph_id, move_to_partial_graph_id) = self
            .actor_states
            .get_mut(&actor_id)
            .expect("should exist")
            .collect(epoch);
        let prev_graph_state = self
            .graph_states
            .get_mut(&prev_partial_graph_id)
            .expect("should exist");
        prev_graph_state.collect(actor_id, epoch);
        if let Some(move_to_partial_graph_id) = move_to_partial_graph_id {
            if prev_graph_state.remove_inflight_actors([actor_id]) {
                self.graph_states.remove(&prev_partial_graph_id);
            }
            if let Some((move_to_partial_graph_id, start_epoch)) = move_to_partial_graph_id {
                self.graph_states
                    .get_mut(&move_to_partial_graph_id)
                    .expect("should exist")
                    .add_inflight_actor(actor_id, start_epoch);
            } else {
                self.actor_states.remove(&actor_id);
            }
        }
    }
}

impl PartialGraphManagedBarrierState {
    fn is_empty(&self) -> bool {
        self.inflight_actors.is_empty()
            && self.epoch_barrier_state_map.is_empty()
            && self.await_epoch_completed_futures.is_empty()
    }

    fn add_inflight_actor(&mut self, actor_id: ActorId, start_epoch: u64) {
        assert!(self.inflight_actors.insert(actor_id));
        must_match!(&self.epoch_barrier_state_map.get(&start_epoch).expect("should exist").inner, ManagedBarrierStateInner::Issued(state) => {
            state.remaining_actors.contains(&actor_id);
        });
        if cfg!(debug_assertions) {
            for (_, state) in self.epoch_barrier_state_map.range(..start_epoch) {
                if let ManagedBarrierStateInner::Issued(state) = &state.inner {
                    // ensure that start_epoch is the first epoch to collect the barrier
                    assert!(!state.remaining_actors.contains(&actor_id));
                }
            }
        }
    }

    #[must_use]
    fn remove_inflight_actors(&mut self, actor_ids: impl IntoIterator<Item = ActorId>) -> bool {
        for actor_id in actor_ids {
            assert!(self.inflight_actors.remove(&actor_id));
        }
        self.is_empty()
    }

    /// This method is called when barrier state is modified in either `Issued` or `Stashed`
    /// to transform the state to `AllCollected` and start state store `sync` when the barrier
    /// has been collected from all actors for an `Issued` barrier.
    fn may_have_collected_all(&mut self, prev_epoch: u64) {
        // Report if there's progress on the earliest in-flight barrier.
        if self.epoch_barrier_state_map.keys().next() == Some(&prev_epoch) {
            self.streaming_metrics.barrier_manager_progress.inc();
        }

        for (prev_epoch, barrier_state) in &mut self.epoch_barrier_state_map {
            let prev_epoch = *prev_epoch;
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors, ..
                }) if remaining_actors.is_empty() => {}
                ManagedBarrierStateInner::AllCollected | ManagedBarrierStateInner::Completed(_) => {
                    continue;
                }
                ManagedBarrierStateInner::Issued(_) => {
                    break;
                }
            }

            let prev_state = replace(
                &mut barrier_state.inner,
                ManagedBarrierStateInner::AllCollected,
            );

            let (kind, table_ids) = must_match!(prev_state, ManagedBarrierStateInner::Issued(IssuedState {
                barrier_inflight_latency: timer,
                kind,
                table_ids,
                ..
            }) => {
                timer.observe_duration();
                (kind, table_ids)
            });

            let create_mview_progress = self
                .create_mview_progress
                .remove(&barrier_state.curr_epoch)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| CreateMviewProgress {
                    backfill_actor_id: actor,
                    done: matches!(state, BackfillState::Done(_)),
                    consumed_epoch: match state {
                        BackfillState::ConsumingUpstream(consumed_epoch, _) => consumed_epoch,
                        BackfillState::Done(_) => barrier_state.curr_epoch,
                    },
                    consumed_rows: match state {
                        BackfillState::ConsumingUpstream(_, consumed_rows) => consumed_rows,
                        BackfillState::Done(consumed_rows) => consumed_rows,
                    },
                })
                .collect();

            let complete_barrier_future = match kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => {
                    tracing::info!(
                        epoch = prev_epoch,
                        "ignore sealing data for the first barrier"
                    );
                    if let Some(hummock) = self.state_store.as_hummock() {
                        let mce = hummock.get_pinned_version().max_committed_epoch();
                        assert_eq!(
                            mce, prev_epoch,
                            "first epoch should match with the current version",
                        );
                    }
                    tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                    None
                }
                BarrierKind::Barrier => {
                    if self.need_seal_epoch {
                        dispatch_state_store!(&self.state_store, state_store, {
                            state_store.seal_epoch(prev_epoch, kind.is_checkpoint());
                        });
                    }
                    None
                }
                BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.state_store, state_store, {
                        if self.need_seal_epoch {
                            state_store.seal_epoch(prev_epoch, kind.is_checkpoint());
                        }
                        Some(sync_epoch(
                            state_store,
                            &self.streaming_metrics,
                            prev_epoch,
                            table_ids.expect("should be Some on BarrierKind::Checkpoint"),
                        ))
                    })
                }
            };

            self.await_epoch_completed_futures.push_back({
                let future = async move {
                    if let Some(future) = complete_barrier_future {
                        let result = future.await;
                        result.map(Some)
                    } else {
                        Ok(None)
                    }
                }
                .map(move |result| {
                    (
                        prev_epoch,
                        result.map(|sync_result| BarrierCompleteResult {
                            sync_result,
                            create_mview_progress,
                        }),
                    )
                });
                if let Some(reg) = &self.barrier_await_tree_reg {
                    reg.register(
                        await_tree_key::BarrierAwait { prev_epoch },
                        format!("SyncEpoch({})", prev_epoch),
                    )
                    .instrument(future)
                    .left_future()
                } else {
                    future.right_future()
                }
            });
        }
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, epoch: EpochPair) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            ?epoch, actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&epoch.prev) {
            None => {
                // If the barrier's state is stashed, this occurs exclusively in scenarios where the barrier has not been
                // injected by the barrier manager, or the barrier message is blocked at the `RemoteInput` side waiting for injection.
                // Given these conditions, it's inconceivable for an actor to attempt collect at this point.
                panic!(
                    "cannot collect new actor barrier {:?} at current state: None",
                    epoch,
                )
            }
            Some(&mut BarrierState {
                curr_epoch,
                inner:
                    ManagedBarrierStateInner::Issued(IssuedState {
                        ref mut remaining_actors,
                        ..
                    }),
                ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                assert!(
                    exist,
                    "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                    actor_id, epoch.curr
                );
                assert_eq!(curr_epoch, epoch.curr);
                self.may_have_collected_all(epoch.prev);
            }
            Some(BarrierState { inner, .. }) => {
                panic!(
                    "cannot collect new actor barrier {:?} at current state: {:?}",
                    epoch, inner
                )
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: HashSet<ActorId>,
        table_ids: HashSet<TableId>,
    ) {
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();

        if let Some(hummock) = self.state_store.as_hummock() {
            hummock.start_epoch(barrier.epoch.curr, table_ids.clone());
        }

        let table_ids = match barrier.kind {
            BarrierKind::Unspecified => {
                unreachable!()
            }
            BarrierKind::Initial => {
                assert!(
                    self.prev_barrier_table_ids.is_none(),
                    "non empty table_ids at initial barrier: {:?}",
                    self.prev_barrier_table_ids
                );
                debug!(epoch = ?barrier.epoch, "initialize at Initial barrier");
                self.prev_barrier_table_ids = Some((barrier.epoch, table_ids));
                None
            }
            BarrierKind::Barrier => {
                if let Some((prev_epoch, prev_table_ids)) = self.prev_barrier_table_ids.as_mut()
                    && prev_epoch.curr == barrier.epoch.prev
                {
                    assert_eq!(prev_table_ids, &table_ids);
                    *prev_epoch = barrier.epoch;
                } else {
                    debug!(epoch = ?barrier.epoch, "reinitialize at non-checkpoint barrier");
                    self.prev_barrier_table_ids = Some((barrier.epoch, table_ids));
                }
                None
            }
            BarrierKind::Checkpoint => Some(
                if let Some((prev_epoch, prev_table_ids)) = self
                    .prev_barrier_table_ids
                    .replace((barrier.epoch, table_ids))
                    && prev_epoch.curr == barrier.epoch.prev
                {
                    prev_table_ids
                } else {
                    debug!(epoch = ?barrier.epoch, "reinitialize at Checkpoint barrier");
                    HashSet::new()
                },
            ),
        };

        if let Some(BarrierState { ref inner, .. }) =
            self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev)
        {
            {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`. Current state: {:?}",
                    barrier.epoch, inner
                );
            }
        };

        self.epoch_barrier_state_map.insert(
            barrier.epoch.prev,
            BarrierState {
                curr_epoch: barrier.epoch.curr,
                inner: ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors: BTreeSet::from_iter(actor_ids_to_collect),
                    mutation: barrier.mutation.clone(),
                    barrier_inflight_latency: timer,
                    kind: barrier.kind,
                    table_ids,
                }),
            },
        );
        self.may_have_collected_all(barrier.epoch.prev);
    }

    /// Return a future that yields the next completed epoch. The future is cancellation safe.
    pub(crate) fn poll_next_completed_epoch(&mut self, cx: &mut Context<'_>) -> Poll<u64> {
        ready!(self.await_epoch_completed_futures.next().poll_unpin(cx))
            .map(|(prev_epoch, result)| {
                let state = self
                    .epoch_barrier_state_map
                    .get_mut(&prev_epoch)
                    .expect("should exist");
                // sanity check on barrier state
                assert_matches!(&state.inner, ManagedBarrierStateInner::AllCollected);
                state.inner = ManagedBarrierStateInner::Completed(result);
                prev_epoch
            })
            .map(Poll::Ready)
            .unwrap_or(Poll::Pending)
    }

    /// Pop the completion result of an completed epoch.
    /// Return:
    /// - `Err(_)` `prev_epoch` is not an epoch to be collected.
    /// - `Ok(None)` when `prev_epoch` exists but has not completed.
    /// - `Ok(Some(_))` when `prev_epoch` has completed but not been reclaimed yet.
    ///    The `BarrierCompleteResult` will be popped out.
    pub(crate) fn pop_completed_epoch(
        &mut self,
        prev_epoch: u64,
    ) -> StreamResult<Option<StreamResult<BarrierCompleteResult>>> {
        let state = self
            .epoch_barrier_state_map
            .get(&prev_epoch)
            .ok_or_else(|| {
                // It's still possible that `collect_complete_receiver` does not contain the target epoch
                // when receiving collect_barrier request. Because `collect_complete_receiver` could
                // be cleared when CN is under recovering. We should return error rather than panic.
                anyhow!(
                    "barrier collect complete receiver for prev epoch {} not exists",
                    prev_epoch
                )
            })?;
        match &state.inner {
            ManagedBarrierStateInner::Completed(_) => {
                match self
                    .epoch_barrier_state_map
                    .remove(&prev_epoch)
                    .expect("should exists")
                    .inner
                {
                    ManagedBarrierStateInner::Completed(result) => Ok(Some(result)),
                    _ => unreachable!(),
                }
            }
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    async fn pop_next_completed_epoch(&mut self) -> u64 {
        let epoch = poll_fn(|cx| self.poll_next_completed_epoch(cx)).await;
        let _ = self.pop_completed_epoch(epoch).unwrap().unwrap();
        epoch
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::util::epoch::test_epoch;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::PartialGraphManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());
        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &test_epoch(1)
        );
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            { &test_epoch(2) }
        );
        managed_barrier_state.collect(2, barrier3.epoch);
        managed_barrier_state.collect(3, barrier3.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());

        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        managed_barrier_state.collect(2, barrier3.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(3, barrier1.epoch);
        managed_barrier_state.collect(3, barrier2.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(4, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
