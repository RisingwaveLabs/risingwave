// Copyright 2023 RisingWave Labs
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::compact_task::{self, TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::CompactTask;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

use super::Compactor;
use crate::hummock::compaction::{
    DynamicLevelSelector, LevelSelector, SpaceReclaimCompactionSelector, TtlCompactionSelector,
};
use crate::hummock::error::Error;
use crate::hummock::{CompactorManagerRef, HummockManagerRef};
use crate::manager::{LocalNotification, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::util::GlobalEventManager;

pub type CompactionSchedulerRef<S> = Arc<CompactionScheduler<S>>;
pub type CompactionRequestChannelRef = Arc<CompactionRequestChannel>;

type CompactionRequestChannelItem = (CompactionGroupId, compact_task::TaskType);

/// [`CompactionRequestChannel`] wrappers a mpsc channel and deduplicate requests from same
/// compaction groups.
pub struct CompactionRequestChannel {
    request_tx: UnboundedSender<CompactionRequestChannelItem>,
    scheduled: Mutex<HashSet<(CompactionGroupId, compact_task::TaskType)>>,
}

#[derive(Debug, PartialEq)]
pub enum ScheduleStatus {
    Ok,
    NoTask,
    PickFailure,
    AssignFailure(CompactTask),
    SendFailure(CompactTask),
}

impl CompactionRequestChannel {
    pub fn new(request_tx: UnboundedSender<CompactionRequestChannelItem>) -> Self {
        Self {
            request_tx,
            scheduled: Default::default(),
        }
    }

    /// Enqueues only if the target is not yet in queue.
    pub fn try_sched_compaction(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) -> Result<bool, SendError<CompactionRequestChannelItem>> {
        let mut guard = self.scheduled.lock();
        let key = (compaction_group, task_type);
        if guard.contains(&key) {
            return Ok(false);
        }
        self.request_tx.send(key)?;
        guard.insert(key);
        Ok(true)
    }

    pub fn unschedule(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
    ) {
        self.scheduled.lock().remove(&(compaction_group, task_type));
    }
}

/// Schedules compaction task picking and assignment.
///
/// When no idle compactor is available, the scheduling will be paused until
/// `compaction_resume_notifier` is `notified`. Compaction should only be resumed by calling
/// `HummockManager::try_resume_compaction`. See [`CompactionResumeTrigger`] for all cases that can
/// resume compaction.
pub struct CompactionScheduler<S>
where
    S: MetaStore,
{
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
    compaction_resume_notifier: Arc<Notify>,
    stopped: AtomicBool,
}

impl<S> CompactionScheduler<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            compactor_manager,
            compaction_resume_notifier: Arc::new(Notify::new()),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn start(self: Arc<Self>, global_event_manager: &mut GlobalEventManager) {
        let (sched_tx, mut sched_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let sched_channel = Arc::new(CompactionRequestChannel::new(sched_tx));

        self.hummock_manager.init_compaction_scheduler(
            sched_channel.clone(),
            Some(self.compaction_resume_notifier.clone()),
        );

        tracing::info!("Start compaction scheduler.");
        self.register_interval_task(
            &sched_channel,
            global_event_manager,
            self.env.opts.periodic_compaction_interval_sec,
            compact_task::TaskType::Dynamic,
        );
        self.register_interval_task(
            &sched_channel,
            global_event_manager,
            self.env.opts.periodic_space_reclaim_compaction_interval_sec,
            compact_task::TaskType::SpaceReclaim,
        );
        self.register_interval_task(
            &sched_channel,
            global_event_manager,
            self.env.opts.periodic_ttl_reclaim_compaction_interval_sec,
            compact_task::TaskType::Ttl,
        );
        let mut selectors = Self::init_selectors();
        let sched_channel_closed = sched_channel.clone();
        let scheduler = self.clone();
        let handle = tokio::spawn(async move {
            while let Some((compaction_group, task_type)) = sched_rx.recv().await {
                if !scheduler
                    .on_handle_compact(compaction_group, &mut selectors, task_type, &sched_channel)
                    .await
                {
                    break;
                }
            }
        });
        global_event_manager.register_shutdown_task(
            async move {
                // we must assign true to `stopped` before waking up `compaction_resume_notifier`.
                self.stopped.store(true, Ordering::Release);
                let _ = sched_channel_closed
                    .request_tx
                    .send((1, TaskType::TypeUnspecified));
                self.compaction_resume_notifier.notify_waiters();
            },
            handle,
        );
    }

    fn register_interval_task(
        &self,
        sched_channel: &Arc<CompactionRequestChannel>,
        global_event_manager: &GlobalEventManager,
        event_interval: u64,
        task: compact_task::TaskType,
    ) {
        let sched_channel_tick = sched_channel.clone();
        let hummock_manager = self.hummock_manager.clone();
        global_event_manager.register_interval_task(event_interval, move || {
            let manager = hummock_manager.clone();
            let sched_channel_0 = sched_channel_tick.clone();
            async move {
                for cg_id in manager.compaction_group_ids().await {
                    if let Err(e) = sched_channel_0.try_sched_compaction(cg_id, task) {
                        tracing::warn!(
                            "Failed to schedule {:?} for compaction group {}. {}",
                            task,
                            cg_id,
                            e
                        );
                    }
                }
            }
        });
    }

    fn init_selectors() -> HashMap<compact_task::TaskType, Box<dyn LevelSelector>> {
        let mut compaction_selectors: HashMap<compact_task::TaskType, Box<dyn LevelSelector>> =
            HashMap::default();
        compaction_selectors.insert(
            compact_task::TaskType::Dynamic,
            Box::<DynamicLevelSelector>::default(),
        );
        compaction_selectors.insert(
            compact_task::TaskType::SpaceReclaim,
            Box::<SpaceReclaimCompactionSelector>::default(),
        );
        compaction_selectors.insert(
            compact_task::TaskType::Ttl,
            Box::<TtlCompactionSelector>::default(),
        );
        compaction_selectors
    }

    /// Tries to pick a compaction task, schedule it to a compactor.
    ///
    /// Returns true if a task is successfully picked and sent.
    async fn pick_and_assign(
        &self,
        compaction_group: CompactionGroupId,
        compactor: Arc<Compactor>,
        sched_channel: Arc<CompactionRequestChannel>,
        selector: &mut Box<dyn LevelSelector>,
    ) -> ScheduleStatus {
        let schedule_status = self
            .pick_and_assign_impl(compaction_group, compactor, sched_channel, selector)
            .await;

        let cancel_state = match &schedule_status {
            ScheduleStatus::Ok => None,
            ScheduleStatus::NoTask | ScheduleStatus::PickFailure => None,
            ScheduleStatus::AssignFailure(task) => {
                Some((task.clone(), TaskStatus::AssignFailCanceled))
            }
            ScheduleStatus::SendFailure(task) => Some((task.clone(), TaskStatus::SendFailCanceled)),
        };

        if let Some((mut compact_task, task_state)) = cancel_state {
            // Try to cancel task immediately.
            if let Err(err) = self
                .hummock_manager
                .cancel_compact_task(&mut compact_task, task_state)
                .await
            {
                // Cancel task asynchronously.
                tracing::warn!(
                    "Failed to cancel task {}. {}. {:?} It will be cancelled asynchronously.",
                    compact_task.task_id,
                    err,
                    task_state
                );
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::CompactionTaskNeedCancel(
                        compact_task,
                    ))
                    .await;
            }
        }
        schedule_status
    }

    async fn pick_and_assign_impl(
        &self,
        compaction_group: CompactionGroupId,
        compactor: Arc<Compactor>,
        sched_channel: Arc<CompactionRequestChannel>,
        selector: &mut Box<dyn LevelSelector>,
    ) -> ScheduleStatus {
        // 1. Pick a compaction task.
        let compact_task = self
            .hummock_manager
            .get_compact_task(compaction_group, selector)
            .await;

        let compact_task = match compact_task {
            Ok(Some(compact_task)) => compact_task,
            Ok(None) => {
                return ScheduleStatus::NoTask;
            }
            Err(err) => {
                tracing::warn!("Failed to get compaction task: {:#?}.", err);
                return ScheduleStatus::PickFailure;
            }
        };
        tracing::trace!(
            "Picked compaction task. {}",
            compact_task_to_string(&compact_task)
        );

        // 2. Assign the compaction task to a compactor.
        match self
            .hummock_manager
            .assign_compaction_task(&compact_task, compactor.context_id())
            .await
        {
            Ok(_) => {
                tracing::trace!(
                    "Assigned compaction task. {}",
                    compact_task_to_string(&compact_task)
                );
            }
            Err(err) => {
                tracing::warn!(
                    "Failed to assign {:?} compaction task to compactor {} : {:#?}",
                    compact_task.task_type().as_str_name(),
                    compactor.context_id(),
                    err
                );
                match err {
                    Error::CompactionTaskAlreadyAssigned(_, _) => {
                        panic!("Compaction scheduler is the only tokio task that can assign task.");
                    }
                    Error::InvalidContext(context_id) => {
                        self.compactor_manager.remove_compactor(context_id);
                        return ScheduleStatus::AssignFailure(compact_task);
                    }
                    _ => {
                        return ScheduleStatus::AssignFailure(compact_task);
                    }
                }
            }
        };

        // 3. Send the compaction task.
        if let Err(e) = compactor
            .send_task(Task::CompactTask(compact_task.clone()))
            .await
        {
            tracing::warn!(
                "Failed to send task {} to {}. {:#?}",
                compact_task.task_id,
                compactor.context_id(),
                e
            );
            self.compactor_manager
                .pause_compactor(compactor.context_id());
            return ScheduleStatus::SendFailure(compact_task);
        }

        // Bypass reschedule if we want compaction scheduling in a deterministic way
        if self.env.opts.compaction_deterministic_test {
            return ScheduleStatus::Ok;
        }

        // 4. Reschedule it with best effort, in case there are more tasks.
        if let Err(e) =
            sched_channel.try_sched_compaction(compaction_group, compact_task.task_type())
        {
            tracing::error!(
                "Failed to reschedule compaction group {} after sending new task {}. {:#?}",
                compaction_group,
                compact_task.task_id,
                e
            );
        }
        ScheduleStatus::Ok
    }

    async fn on_handle_compact(
        &self,
        compaction_group: CompactionGroupId,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        task_type: compact_task::TaskType,
        sched_channel: &Arc<CompactionRequestChannel>,
    ) -> bool {
        if task_type == TaskType::TypeUnspecified {
            return false;
        }
        sync_point::sync_point!("BEFORE_SCHEDULE_COMPACTION_TASK");
        sched_channel.unschedule(compaction_group, task_type);

        self.task_dispatch(
            compaction_group,
            task_type,
            compaction_selectors,
            sched_channel,
        )
        .await
    }

    async fn task_dispatch(
        &self,
        compaction_group: CompactionGroupId,
        task_type: compact_task::TaskType,
        compaction_selectors: &mut HashMap<compact_task::TaskType, Box<dyn LevelSelector>>,
        sched_channel: &Arc<CompactionRequestChannel>,
    ) -> bool {
        // Wait for a compactor to become available.
        let compactor = loop {
            if let Some(compactor) = self.hummock_manager.get_idle_compactor().await {
                break compactor;
            } else {
                self.compaction_resume_notifier.notified().await;
                if self.stopped.load(Ordering::Acquire) {
                    return false;
                }
            }
        };
        let selector = compaction_selectors.get_mut(&task_type).unwrap();
        self.pick_and_assign(compaction_group, compactor, sched_channel.clone(), selector)
            .await;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;

    use crate::hummock::compaction::default_level_selector;
    use crate::hummock::compaction_scheduler::{
        CompactionRequestChannel, CompactionRequestChannelItem, ScheduleStatus,
    };
    use crate::hummock::test_utils::{add_ssts, setup_compute_env};
    use crate::hummock::CompactionScheduler;

    #[tokio::test]
    async fn test_pick_and_assign() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let compaction_scheduler =
            CompactionScheduler::new(env, hummock_manager.clone(), compactor_manager.clone());

        let (request_tx, _request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));

        // Add a compactor with invalid context_id.
        let _receiver = compactor_manager.add_compactor(1234, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);

        // No task
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::NoTask,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );

        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        // Cannot assign because of invalid compactor
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        assert_eq!(compactor_manager.compactor_num(), 0);

        // Add a valid compactor and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 1);
        assert_eq!(compactor_manager.compactor_num(), 1);
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::Ok,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );

        // Add more SSTs for compaction.
        let _sst_infos = add_ssts(2, hummock_manager.as_ref(), context_id).await;

        // No idle compactor
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            1
        );
        assert_eq!(compactor_manager.compactor_num(), 1);
        assert_matches!(hummock_manager.get_idle_compactor().await, None);

        // Increase compactor concurrency and succeed
        let _receiver = compactor_manager.add_compactor(context_id, 10);
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            1
        );
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::Ok,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );
        assert_eq!(
            hummock_manager.get_assigned_tasks_number(context_id).await,
            2
        );
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints() {
        use risingwave_pb::hummock::compact_task::TaskStatus;

        use crate::manager::LocalNotification;

        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let compaction_scheduler = CompactionScheduler::new(
            env.clone(),
            hummock_manager.clone(),
            compactor_manager.clone(),
        );

        let (request_tx, _request_rx) =
            tokio::sync::mpsc::unbounded_channel::<CompactionRequestChannelItem>();
        let request_channel = Arc::new(CompactionRequestChannel::new(request_tx));

        let _sst_infos = add_ssts(1, hummock_manager.as_ref(), context_id).await;
        let _receiver = compactor_manager.add_compactor(context_id, 1);

        // Pick failure
        let fp_get_compact_task = "fp_get_compact_task";
        fail::cfg(fp_get_compact_task, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_eq!(
            ScheduleStatus::PickFailure,
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await
        );
        fail::remove(fp_get_compact_task);

        // Assign failed and task cancelled.
        let fp_assign_compaction_task_fail = "assign_compaction_task_fail";
        fail::cfg(fp_assign_compaction_task_fail, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        fail::remove(fp_assign_compaction_task_fail);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // Send failed and task cancelled.
        let fp_compaction_send_task_fail = "compaction_send_task_fail";
        fail::cfg(fp_compaction_send_task_fail, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::SendFailure(_)
        );
        fail::remove(fp_compaction_send_task_fail);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // There is no idle compactor, because the compactor is paused after send failure.
        assert_matches!(hummock_manager.get_idle_compactor().await, None);
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());
        let _receiver = compactor_manager.add_compactor(context_id, 1);

        // Assign failed and task cancellation failed.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        env.notification_manager().insert_local_sender(tx).await;
        let fp_cancel_compact_task = "fp_cancel_compact_task";
        fail::cfg(fp_assign_compaction_task_fail, "return").unwrap();
        fail::cfg(fp_cancel_compact_task, "return").unwrap();
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::AssignFailure(_)
        );
        fail::remove(fp_assign_compaction_task_fail);
        fail::remove(fp_cancel_compact_task);
        assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);
        // Notified to retry cancellation.
        let mut task_to_cancel = match rx.recv().await.unwrap() {
            LocalNotification::WorkerNodeIsDeleted(_) => {
                panic!()
            }
            LocalNotification::CompactionTaskNeedCancel(task_to_cancel) => task_to_cancel,
        };
        hummock_manager
            .cancel_compact_task(&mut task_to_cancel, TaskStatus::ManualCanceled)
            .await
            .unwrap();
        assert!(hummock_manager.list_all_tasks_ids().await.is_empty());

        // Succeeded.
        let compactor = hummock_manager.get_idle_compactor().await.unwrap();
        assert_matches!(
            compaction_scheduler
                .pick_and_assign(
                    StaticCompactionGroupId::StateDefault.into(),
                    compactor,
                    request_channel.clone(),
                    &mut default_level_selector(),
                )
                .await,
            ScheduleStatus::Ok
        );
        assert_eq!(hummock_manager.list_all_tasks_ids().await.len(), 1);
    }
}
