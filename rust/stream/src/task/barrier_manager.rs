use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::error::Result;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::executor::*;

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

struct ManagedBarrierState {
    epoch: u64,

    /// Notify that the collection is finished.
    collect_notifier: oneshot::Sender<()>,

    /// Actor ids remaining to be collected.
    remaining_actors: HashSet<u32>,
}

impl ManagedBarrierState {
    fn notify(self) {
        // Notify about barrier finishing.
        if self.collect_notifier.send(()).is_err() {
            warn!(
                "failed to notify barrier collection with epoch {}: rx is dropped",
                self.epoch
            )
        }
    }
}

enum BarrierState {
    /// `Local` mode should be only used for tests. In this mode, barriers are not managed or
    /// collected, and there's no way to know whether or when a barrier is finished.
    #[allow(dead_code)]
    Local,

    /// In `Managed` mode, barriers are sent and collected according to the request from meta
    /// service. When the barrier is finished, the caller can be notified about this.
    Managed(Option<ManagedBarrierState>),
}

/// [`LocalBarrierManager`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierManager`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub struct LocalBarrierManager {
    /// Stores all materialized view source sender.
    senders: HashMap<u32, UnboundedSender<Message>>,

    /// Span of the current epoch.
    #[allow(dead_code)]
    span: tracing::Span,

    /// Current barrier collection state.
    state: BarrierState,

    /// Last epoch of barriers.
    last_epoch: Option<u64>,

    /// Blacklist of failed senders.
    failed_senders: HashSet<u32>,
}

impl Default for LocalBarrierManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBarrierManager {
    fn with_state(state: BarrierState) -> Self {
        Self {
            senders: HashMap::new(),
            span: tracing::Span::none(),
            state,
            last_epoch: None,
            failed_senders: HashSet::new(),
        }
    }

    /// Create a [`LocalBarrierManager`] with managed mode.
    pub fn new() -> Self {
        Self::with_state(BarrierState::Managed(None))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_state(BarrierState::Local)
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&mut self, actor_id: u32, sender: UnboundedSender<Message>) {
        debug!("register sender: {}", actor_id);
        self.senders.insert(actor_id, sender);
    }

    /// When actor is cancelled or aborted, it should call this method to withdraw itself.
    /// For example, a source actor might be cancelled because of Kafka stream timeout,
    /// which would cause a chain of actors (itself and the downstream actors to abort).
    /// TODO(#742): Should let global stream manager know the cancelled actors and decide what
    /// to do.
    pub fn withdraw_actor(&mut self, actor_id: u32) {
        debug!("withdraw actor: {}", actor_id);
        // Add it to local black list.
        self.failed_senders.insert(actor_id);

        if let BarrierState::Managed(managed_state) = &mut self.state {
            let mut need_to_notify: bool = false;

            // If the barrier state has been initialized, which means `send_barrier` has been
            // called.
            if let Some(barrier_state) = managed_state.as_mut() {
                if barrier_state.remaining_actors.remove(&actor_id) {
                    // If the actor is the last one to be withdrawn.
                    if barrier_state.remaining_actors.is_empty() {
                        need_to_notify = true;
                    }
                }
            }

            if need_to_notify {
                // Reset this round of barrier and notify local stream manager.
                let state = managed_state.take().unwrap();
                state.notify();
            }
        }
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    /// Returns `Ok(None)` is receiver is not available.
    /// TODO: async collect barrier flush state from hummock.
    pub fn send_barrier(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = u32>,
        actor_ids_to_collect: impl IntoIterator<Item = u32>,
    ) -> Result<Option<oneshot::Receiver<()>>> {
        let to_send = {
            let mut to_send: HashSet<u32> = actor_ids_to_send.into_iter().collect();
            // For failed actors that have been locally observed
            // and not tracked by global stream manager, untrack them.
            // TODO(#742): Global stream manager should know failed actors and untrack them.
            to_send.retain(|actor_id| !self.failed_senders.contains(actor_id));

            match &self.state {
                BarrierState::Local => {
                    if to_send.is_empty() {
                        to_send = self.senders.keys().cloned().collect()
                    }
                }
                BarrierState::Managed(_) => {
                    // If there's no actor to send, return.
                    if to_send.is_empty() {
                        return Ok(None);
                    }
                }
            }
            to_send
        };
        let mut to_collect: HashSet<u32> = actor_ids_to_collect.into_iter().collect();
        trace!(
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            to_send,
            to_collect
        );
        // For failed actors that have been locally observed
        // and not tracked by global stream manager, untrack them.
        // TODO(#742): Global stream manager should know failed actors and untrack them.
        to_collect.retain(|actor_id| !self.failed_senders.contains(actor_id));

        info!(
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier, to_send, to_collect
        );

        let rx = match &mut self.state {
            BarrierState::Local => None,

            BarrierState::Managed(state) => {
                // There should be only one epoch / barrier at a time.
                assert!(state.is_none());
                // There must be some actors to collect from.
                assert!(!to_collect.is_empty());

                let (tx, rx) = oneshot::channel();
                *state = Some(ManagedBarrierState {
                    epoch: barrier.epoch.curr,
                    collect_notifier: tx,
                    remaining_actors: to_collect,
                });

                Some(rx)
            }
        };

        for actor_id in to_send {
            let sender = self
                .senders
                .get(&actor_id)
                .unwrap_or_else(|| panic!("sender for actor {} does not exist", actor_id));

            match sender.send(Message::Barrier(barrier.clone())) {
                Ok(it) => it,
                Err(_) => {
                    // Fail to send barrier to this actor (e.g. Kafka source timeout).
                    error!("[LocalBarrierManager] Send barrier to actor {} failed. Make sure target actor is still alive.", actor_id);

                    // // Remove this actor from the list of senders.
                    // self.senders.remove(&actor_id);
                }
            }
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
            for actor in actors {
                trace!("remove actor {} from senders", actor);
                self.senders.remove(actor);
            }
        }

        Ok(rx)
    }

    /// When a [`StreamConsumer`] (typically [`DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&mut self, actor_id: u32, barrier: &Barrier) -> Result<()> {
        match &mut self.state {
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                let current_epoch = managed_state.as_ref().map(|s| s.epoch);
                if current_epoch != Some(barrier.epoch.curr) {
                    panic!(
                        "bad barrier with epoch {} from actor {}, while current epoch is {:?}, last epoch is {:?}",
                        barrier.epoch.curr, actor_id, current_epoch, self.last_epoch
                    );
                }

                let state = managed_state.as_mut().unwrap();
                state.remaining_actors.remove(&actor_id);

                tracing::trace!(
                    target: "events::stream::barrier::collect_barrier",
                    "collect_barrier: epoch = {}, actor_id = {}, remaining_actors = {:?}",
                    barrier.epoch.curr,
                    actor_id,
                    state
                        .remaining_actors
                        .clone()
                        .into_iter()
                        .sorted()
                        .collect_vec()
                );

                if state.remaining_actors.is_empty() {
                    let state = managed_state.take().unwrap();
                    self.last_epoch = Some(state.epoch);
                    // Notify about barrier finishing.
                    state.notify();
                }
            }
        }

        Ok(())
    }

    /// Returns whether [`BarrierState`] is `Local`.
    pub fn is_local_mode(&self) -> bool {
        matches!(self.state, BarrierState::Local)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[tokio::test]
    async fn test_managed_barrier_collection() -> Result<()> {
        let mut manager = LocalBarrierManager::new();
        assert!(!manager.is_local_mode());

        let register_sender = |actor_id: u32| {
            let (barrier_tx, barrier_rx) = unbounded_channel();
            manager.register_sender(actor_id, barrier_tx);
            (actor_id, barrier_rx)
        };

        // Register actors
        let actor_ids = vec![233, 234, 235];
        let count = actor_ids.len();
        let mut rxs = actor_ids
            .clone()
            .into_iter()
            .map(register_sender)
            .collect_vec();

        // Send a barrier to all actors
        let epoch = 114514;
        let barrier = Barrier::new_test_barrier(epoch);
        let mut collect_rx = manager
            .send_barrier(&barrier, actor_ids.clone(), actor_ids)
            .unwrap()
            .unwrap();

        // Collect barriers from actors
        let collected_barriers = rxs
            .iter_mut()
            .map(|(actor_id, rx)| {
                let msg = rx.try_recv().unwrap();
                let barrier = match msg {
                    Message::Barrier(b) => {
                        assert_eq!(b.epoch.curr, epoch);
                        b
                    }
                    _ => unreachable!(),
                };
                (*actor_id, barrier)
            })
            .collect_vec();

        // Report to local barrier manager
        for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
            manager.collect(actor_id, &barrier).unwrap();
            let notified = collect_rx.try_recv().is_ok();
            assert_eq!(notified, i == count - 1);
        }

        Ok(())
    }
}
