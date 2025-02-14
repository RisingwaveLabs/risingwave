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

use std::collections::{HashMap, VecDeque};
use std::iter::once;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use assert_matches::assert_matches;
use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::HistogramTimer;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_hummock_sdk::HummockVersionId;
use tokio::select;
use tokio::sync::{oneshot, watch};
use tokio::time::Interval;

use super::notifier::Notifier;
use super::{Command, Scheduled};
use crate::barrier::context::GlobalBarrierWorkerContext;
use crate::hummock::HummockManagerRef;
use crate::rpc::metrics::MetaMetrics;
use crate::{MetaError, MetaResult};

pub(super) struct NewBarrier {
    pub command: Option<(DatabaseId, Command, Vec<Notifier>)>,
    pub span: tracing::Span,
    pub checkpoint: bool,
}

/// A queue for scheduling barriers.
///
/// We manually implement one here instead of using channels since we may need to update the front
/// of the queue to add some notifiers for instant flushes.
struct Inner {
    queue: Mutex<ScheduledQueue>,

    /// When `queue` is not empty anymore, all subscribers of this watcher will be notified.
    changed_tx: watch::Sender<()>,

    /// Used for recording send latency of each barrier.
    metrics: Arc<MetaMetrics>,
}

#[derive(Debug)]
enum QueueStatus {
    /// The queue is ready to accept new command.
    Ready,
    /// The queue is blocked to accept new command with the given reason.
    Blocked(String),
}

impl QueueStatus {
    fn is_blocked(&self) -> bool {
        matches!(self, Self::Blocked(_))
    }
}

struct ScheduledQueueItem {
    command: Command,
    notifiers: Vec<Notifier>,
    send_latency_timer: HistogramTimer,
    span: tracing::Span,
}

struct StatusQueue<T> {
    queue: T,
    status: QueueStatus,
}

type DatabaseScheduledQueue = StatusQueue<VecDeque<ScheduledQueueItem>>;
type ScheduledQueue = StatusQueue<HashMap<DatabaseId, DatabaseScheduledQueue>>;

impl<T> StatusQueue<T> {
    fn new() -> Self
    where
        T: Default,
    {
        Self {
            queue: T::default(),
            status: QueueStatus::Ready,
        }
    }

    fn mark_blocked(&mut self, reason: String) {
        self.status = QueueStatus::Blocked(reason);
    }

    fn mark_ready(&mut self) -> bool {
        let prev_blocked = self.status.is_blocked();
        self.status = QueueStatus::Ready;
        prev_blocked
    }

    fn validate_item(&mut self, scheduled: &ScheduledQueueItem) -> MetaResult<()> {
        // We don't allow any command to be scheduled when the queue is blocked, except for dropping streaming jobs.
        // Because we allow dropping streaming jobs when the cluster is under recovery, so we have to buffer the drop
        // command and execute it when the cluster is ready to clean up it.
        // TODO: this is just a workaround to allow dropping streaming jobs when the cluster is under recovery,
        // we need to refine it when catalog and streaming metadata can be handled in a transactional way.
        if let QueueStatus::Blocked(reason) = &self.status
            && !matches!(
                scheduled.command,
                Command::DropStreamingJobs { .. } | Command::DropSubscription { .. }
            )
        {
            return Err(MetaError::unavailable(reason));
        }
        Ok(())
    }
}

fn tracing_span() -> tracing::Span {
    if tracing::Span::current().is_none() {
        tracing::Span::none()
    } else {
        tracing::info_span!(
            "barrier",
            checkpoint = tracing::field::Empty,
            epoch = tracing::field::Empty
        )
    }
}

impl Inner {
    /// Create a new scheduled barrier with the given `checkpoint`, `command` and `notifiers`.
    fn new_scheduled(
        &self,
        command: Command,
        notifiers: impl IntoIterator<Item = Notifier>,
    ) -> ScheduledQueueItem {
        // Create a span only if we're being traced, instead of for every periodic barrier.
        let span = tracing_span();

        ScheduledQueueItem {
            command,
            notifiers: notifiers.into_iter().collect(),
            send_latency_timer: self.metrics.barrier_send_latency.start_timer(),
            span,
        }
    }
}

/// The sender side of the barrier scheduling queue.
/// Can be cloned and held by other managers to schedule and run barriers.
#[derive(Clone)]
pub struct BarrierScheduler {
    inner: Arc<Inner>,

    /// Used for getting the latest snapshot after `FLUSH`.
    hummock_manager: HummockManagerRef,
}

impl BarrierScheduler {
    /// Create a pair of [`BarrierScheduler`] and [`ScheduledBarriers`], for scheduling barriers
    /// from different managers, and executing them in the barrier manager, respectively.
    pub fn new_pair(
        hummock_manager: HummockManagerRef,
        metrics: Arc<MetaMetrics>,
    ) -> (Self, ScheduledBarriers) {
        let inner = Arc::new(Inner {
            queue: Mutex::new(ScheduledQueue::new()),
            changed_tx: watch::channel(()).0,
            metrics,
        });

        (
            Self {
                inner: inner.clone(),
                hummock_manager,
            },
            ScheduledBarriers { inner },
        )
    }

    /// Push a scheduled barrier into the queue.
    fn push(
        &self,
        database_id: DatabaseId,
        scheduleds: impl IntoIterator<Item = ScheduledQueueItem>,
    ) -> MetaResult<()> {
        let mut queue = self.inner.queue.lock();
        let scheduleds = scheduleds.into_iter().collect_vec();
        scheduleds
            .iter()
            .try_for_each(|scheduled| queue.validate_item(scheduled))?;
        let queue = queue
            .queue
            .entry(database_id)
            .or_insert_with(DatabaseScheduledQueue::new);
        scheduleds
            .iter()
            .try_for_each(|scheduled| queue.validate_item(scheduled))?;
        for scheduled in scheduleds {
            queue.queue.push_back(scheduled);
            if queue.queue.len() == 1 {
                self.inner.changed_tx.send(()).ok();
            }
        }
        Ok(())
    }

    /// Try to cancel scheduled cmd for create streaming job, return true if the command exists previously and get cancelled.
    pub fn try_cancel_scheduled_create(&self, database_id: DatabaseId, table_id: TableId) -> bool {
        let queue = &mut self.inner.queue.lock();
        let Some(queue) = queue.queue.get_mut(&database_id) else {
            return false;
        };

        if let Some(idx) = queue.queue.iter().position(|scheduled| {
            if let Command::CreateStreamingJob { info, .. } = &scheduled.command
                && info.stream_job_fragments.stream_job_id() == table_id
            {
                true
            } else {
                false
            }
        }) {
            queue.queue.remove(idx).unwrap();
            true
        } else {
            false
        }
    }

    /// Run multiple commands and return when they're all completely finished (i.e., collected). It's ensured that
    /// multiple commands are executed continuously.
    ///
    /// Returns the barrier info of each command.
    ///
    /// TODO: atomicity of multiple commands is not guaranteed.
    async fn run_multiple_commands(
        &self,
        database_id: DatabaseId,
        commands: Vec<Command>,
    ) -> MetaResult<()> {
        let mut contexts = Vec::with_capacity(commands.len());
        let mut scheduleds = Vec::with_capacity(commands.len());

        for command in commands {
            let (started_tx, started_rx) = oneshot::channel();
            let (collect_tx, collect_rx) = oneshot::channel();

            contexts.push((started_rx, collect_rx));
            scheduleds.push(self.inner.new_scheduled(
                command,
                once(Notifier {
                    started: Some(started_tx),
                    collected: Some(collect_tx),
                }),
            ));
        }

        self.push(database_id, scheduleds)?;

        for (injected_rx, collect_rx) in contexts {
            // Wait for this command to be injected, and record the result.
            tracing::trace!("waiting for injected_rx");
            injected_rx
                .await
                .ok()
                .context("failed to inject barrier")??;

            tracing::trace!("waiting for collect_rx");
            // Throw the error if it occurs when collecting this barrier.
            collect_rx
                .await
                .ok()
                .context("failed to collect barrier")??;
        }

        Ok(())
    }

    /// Run a command and return when it's completely finished (i.e., collected).
    ///
    /// Returns the barrier info of the actual command.
    pub async fn run_command(&self, database_id: DatabaseId, command: Command) -> MetaResult<()> {
        tracing::trace!("run_command: {:?}", command);
        let ret = self.run_multiple_commands(database_id, vec![command]).await;
        tracing::trace!("run_command finished");
        ret
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self, database_id: DatabaseId) -> MetaResult<HummockVersionId> {
        let start = Instant::now();

        tracing::debug!("start barrier flush");
        self.run_multiple_commands(database_id, vec![Command::Flush])
            .await?;

        let elapsed = Instant::now().duration_since(start);
        tracing::debug!("barrier flushed in {:?}", elapsed);

        let version_id = self.hummock_manager.get_version_id().await;
        Ok(version_id)
    }
}

/// The receiver side of the barrier scheduling queue.
pub struct ScheduledBarriers {
    inner: Arc<Inner>,
}

/// Held by the [`crate::barrier::worker::GlobalBarrierWorker`] to execute these commands.
pub(super) struct PeriodicBarriers {
    min_interval: Interval,

    /// Force checkpoint in next barrier.
    force_checkpoint: bool,

    /// The numbers of barrier (checkpoint = false) since the last barrier (checkpoint = true)
    num_uncheckpointed_barrier: usize,
    checkpoint_frequency: usize,
}

impl PeriodicBarriers {
    pub(super) fn new(min_interval: Duration, checkpoint_frequency: usize) -> PeriodicBarriers {
        Self {
            min_interval: tokio::time::interval(min_interval),
            force_checkpoint: false,
            num_uncheckpointed_barrier: 0,
            checkpoint_frequency,
        }
    }

    pub(super) fn set_min_interval(&mut self, min_interval: Duration) {
        let set_new_interval = min_interval != self.min_interval.period();
        if set_new_interval {
            let mut min_interval = tokio::time::interval(min_interval);
            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            self.min_interval = min_interval;
        }
    }

    pub(super) async fn next_barrier(
        &mut self,
        context: &impl GlobalBarrierWorkerContext,
    ) -> NewBarrier {
        let checkpoint = self.try_get_checkpoint();
        let scheduled = select! {
            biased;
            scheduled = context.next_scheduled() => {
                self.min_interval.reset();
                let checkpoint = scheduled.command.need_checkpoint() || checkpoint;
                NewBarrier {
                    command: Some((scheduled.database_id, scheduled.command, scheduled.notifiers)),
                    span: scheduled.span,
                    checkpoint,
                }
            },
            _ = self.min_interval.tick() => {
                NewBarrier {
                    command: None,
                    span: tracing_span(),
                    checkpoint,
                }
            }
        };
        self.update_num_uncheckpointed_barrier(scheduled.checkpoint);
        scheduled
    }
}

impl ScheduledBarriers {
    pub(super) async fn next_scheduled(&self) -> Scheduled {
        'outer: loop {
            let mut rx = self.inner.changed_tx.subscribe();
            {
                let mut queue = self.inner.queue.lock();
                if queue.status.is_blocked() {
                    continue;
                }
                for (database_id, queue) in &mut queue.queue {
                    if queue.status.is_blocked() {
                        continue;
                    }
                    if let Some(item) = queue.queue.pop_front() {
                        item.send_latency_timer.observe_duration();
                        break 'outer Scheduled {
                            database_id: *database_id,
                            command: item.command,
                            notifiers: item.notifiers,
                            span: item.span,
                        };
                    }
                }
            }
            rx.changed().await.unwrap();
        }
    }
}

impl ScheduledBarriers {
    /// Pre buffered drop and cancel command, return true if any.
    pub(super) fn pre_apply_drop_cancel(&self, database_id: Option<DatabaseId>) -> bool {
        self.pre_apply_drop_cancel_scheduled(database_id)
    }

    /// Mark command scheduler as blocked and abort all queued scheduled command and notify with
    /// specific reason.
    pub(super) fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        reason: impl Into<String> + Copy,
    ) {
        let mut queue = self.inner.queue.lock();
        let mark_blocked_and_notify_failed = |queue: &mut DatabaseScheduledQueue| {
            queue.mark_blocked(reason.into());
            while let Some(ScheduledQueueItem { notifiers, .. }) = queue.queue.pop_front() {
                notifiers.into_iter().for_each(|notify| {
                    notify.notify_collection_failed(anyhow!(reason.into()).into())
                })
            }
        };
        if let Some(database_id) = database_id {
            let queue = queue
                .queue
                .entry(database_id)
                .or_insert_with(DatabaseScheduledQueue::new);
            mark_blocked_and_notify_failed(queue);
        } else {
            queue.mark_blocked(reason.into());
            for queue in queue.queue.values_mut() {
                mark_blocked_and_notify_failed(queue);
            }
        }
    }

    /// Mark command scheduler as ready to accept new command.
    pub(super) fn mark_ready(&self, database_id: Option<DatabaseId>) {
        let mut queue = self.inner.queue.lock();
        if let Some(database_id) = database_id {
            let database_queue = queue
                .queue
                .entry(database_id)
                .or_insert_with(DatabaseScheduledQueue::new);
            if database_queue.mark_ready() && !database_queue.queue.is_empty() {
                self.inner.changed_tx.send(()).ok();
            }
        } else {
            let prev_blocked = queue.mark_ready();
            for queue in queue.queue.values_mut() {
                queue.mark_ready();
            }
            if prev_blocked
                && queue
                    .queue
                    .values()
                    .any(|database_queue| !database_queue.queue.is_empty())
            {
                self.inner.changed_tx.send(()).ok();
            }
        }
    }

    /// Try to pre apply drop and cancel scheduled command and return them if any.
    /// It should only be called in recovery.
    pub(super) fn pre_apply_drop_cancel_scheduled(&self, database_id: Option<DatabaseId>) -> bool {
        let mut queue = self.inner.queue.lock();
        let mut applied = false;

        let mut pre_apply_drop_cancel = |queue: &mut DatabaseScheduledQueue| {
            while let Some(ScheduledQueueItem {
                notifiers, command, ..
            }) = queue.queue.pop_front()
            {
                match command {
                    Command::DropStreamingJobs { .. } => {
                        applied = true;
                    }
                    Command::DropSubscription { .. } => {}
                    _ => {
                        unreachable!("only drop and cancel streaming jobs should be buffered");
                    }
                }
                notifiers.into_iter().for_each(|notify| {
                    notify.notify_collected();
                });
            }
        };

        if let Some(database_id) = database_id {
            assert_matches!(queue.status, QueueStatus::Ready);
            if let Some(queue) = queue.queue.get_mut(&database_id) {
                assert_matches!(queue.status, QueueStatus::Blocked(_));
                pre_apply_drop_cancel(queue);
            }
        } else {
            assert_matches!(queue.status, QueueStatus::Blocked(_));
            for queue in queue.queue.values_mut() {
                pre_apply_drop_cancel(queue);
            }
        }

        applied
    }
}

impl PeriodicBarriers {
    /// Whether the barrier(checkpoint = true) should be injected.
    fn try_get_checkpoint(&self) -> bool {
        self.num_uncheckpointed_barrier + 1 >= self.checkpoint_frequency || self.force_checkpoint
    }

    /// Make the `checkpoint` of the next barrier must be true
    pub fn force_checkpoint_in_next_barrier(&mut self) {
        self.force_checkpoint = true;
    }

    /// Update the `checkpoint_frequency`
    pub fn set_checkpoint_frequency(&mut self, frequency: usize) {
        self.checkpoint_frequency = frequency;
    }

    /// Update the `num_uncheckpointed_barrier`
    fn update_num_uncheckpointed_barrier(&mut self, checkpoint: bool) {
        if checkpoint {
            self.num_uncheckpointed_barrier = 0;
            self.force_checkpoint = false;
        } else {
            self.num_uncheckpointed_barrier += 1;
        }
    }
}
