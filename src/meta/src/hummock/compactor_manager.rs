// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::{Mutex, MutexGuard, RwLock};
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTask, CompactTaskAssignment, CompactTaskProgress,
    SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::{Receiver, Sender};

use super::compaction_schedule_policy::{CompactionSchedulePolicy, RoundRobinPolicy, ScoredPolicy};
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

pub type CompactorManagerRef = Arc<CompactorManager>;
type TaskId = u64;

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    max_concurrent_task_number: u64,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    expire_at: u64,
}

impl Compactor {
    pub fn new(
        context_id: HummockContextId,
        sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
        max_concurrent_task_number: u64,
    ) -> Self {
        Self {
            context_id,
            sender,
            max_concurrent_task_number,
        }
    }

    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                task: Some(Task::CancelCompactTask(CancelCompactTask {
                    context_id: self.context_id,
                    task_id,
                })),
            }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn max_concurrent_task_number(&self) -> u64 {
        self.max_concurrent_task_number
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`.
///
/// A compact task can be in one of these states:
/// - 1. Pending: a compact task is picked but not assigned to a compactor.
///   Pending-->Success/Failed/Canceled.
/// - 2. Success: an assigned task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 3. Failed: an Failed task is reported as success via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 4. Cancelled: a task is reported as cancelled via `CompactStatus::report_compact_task`. It's
///   the final state.
/// We omit Assigned state because there's nothing to be done about this state currently.
///
/// Furthermore, the compactor for a compaction task must be picked with `CompactorManager`,
/// or its internal states might not be correctly maintained.
pub struct CompactorManager {
    // `policy` must be locked before `compactor_assigned_task_num`.
    policy: RwLock<Box<dyn CompactionSchedulePolicy>>,
    // Map compactor to the number of assigned tasks.
    compactor_assigned_task_num: Mutex<HashMap<HummockContextId, u64>>,

    pub task_expiry_seconds: u64,
    // A map: { context_id -> { task_id -> heartbeat } }
    task_heartbeats: RwLock<HashMap<HummockContextId, HashMap<TaskId, TaskHeartbeat>>>,
}

impl CompactorManager {
    pub async fn new_with_meta<S: MetaStore>(
        env: MetaSrvEnv<S>,
        task_expiry_seconds: u64,
    ) -> MetaResult<Self> {
        let manager = Self::new(task_expiry_seconds);
        // Initialize the existing task assignments from metastore
        CompactTaskAssignment::list(env.meta_store())
            .await?
            .into_iter()
            .for_each(|assignment| {
                manager.initiate_task_heartbeat(
                    assignment.context_id,
                    assignment.compact_task.unwrap(),
                );
            });
        Ok(manager)
    }

    pub fn new(task_expiry_seconds: u64) -> Self {
        Self {
            policy: RwLock::new(Box::new(ScoredPolicy::new())),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
            task_expiry_seconds,
            task_heartbeats: Default::default(),
        }
    }

    /// Only used for unit test.
    pub fn new_for_test() -> Self {
        Self {
            policy: RwLock::new(Box::new(RoundRobinPolicy::new())),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Only used for unit test.
    pub fn new_with_policy(policy: Box<dyn CompactionSchedulePolicy>) -> Self {
        Self {
            policy: RwLock::new(policy),
            compactor_assigned_task_num: Mutex::new(HashMap::new()),
            task_expiry_seconds: 1,
            task_heartbeats: Default::default(),
        }
    }

    /// Gets next idle compactor to assign task.
    // TODO: If a compactor is returned and `compact_task` is `Some`, then it is considered to be
    // already assigned to it. This may cause inconsistency between `CompactorManager` and
    // `HummockManager, if `HummockManager::assign_compaction_task` and `next_idle_compactor` are
    // not called together. So we might need to put `HummockManager::assign_compaction_task` in this
    // function.
    pub fn next_idle_compactor(
        &self,
        compact_task: Option<&CompactTask>,
    ) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.next_idle_compactor(&compactor_assigned_task_num, compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    /// Gets next compactor to assign task.
    // TODO: If a compactor is returned and `compact_task` is `Some`, then it is considered to be
    // already assigned to it. This may cause inconsistency between `CompactorManager` and
    // `HummockManager, if `HummockManager::assign_compaction_task` and `next_compactor` are
    // not called together. So we might need to put `HummockManager::assign_compaction_task` in this
    // function.
    pub fn next_compactor(&self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.next_compactor(compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    /// Gets next compactor to assign task.
    // TODO: If a compactor is returned and `compact_task` is `Some`, then it is considered to be
    // already assigned to it. This may cause inconsistency between `CompactorManager` and
    // `HummockManager, if `HummockManager::assign_compaction_task` and `random_compactor` are
    // not called together. So we might need to put `HummockManager::assign_compaction_task` in this
    // function.
    pub fn random_compactor(&self, compact_task: Option<&CompactTask>) -> Option<Arc<Compactor>> {
        let need_update_task_num = compact_task.is_some();
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        let compactor = policy.random_compactor(compact_task);
        if let Some(compactor) = compactor {
            if need_update_task_num {
                Self::update_task_num(&mut compactor_assigned_task_num, compactor.context_id, true);
            }
            Some(compactor)
        } else {
            None
        }
    }

    /// Retrieve a receiver of tasks for the compactor identified by `context_id`. The sender should
    /// be obtained by calling one of the compactor getters.
    ///
    ///  If `add_compactor` is called with the same `context_id` more than once, the only cause
    /// would be compactor re-subscription, as `context_id` is a monotonically increasing
    /// sequence.
    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        compactor_assigned_task_num.insert(context_id, 0);
        let rx = policy.add_compactor(context_id, max_concurrent_task_number);
        tracing::info!("Added compactor session {}", context_id);
        rx
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        compactor_assigned_task_num.remove(&context_id);
        policy.remove_compactor(context_id);

        // To remove the heartbeats, they need to be forcefully purged,
        // which is only safe when the context has been completely removed from meta.
        tracing::info!("Removed compactor session {}", context_id);
    }

    pub fn get_compactor(&self, context_id: HummockContextId) -> Option<Arc<Compactor>> {
        self.policy.read().get_compactor(context_id)
    }

    pub fn report_compact_task(&self, context_id: HummockContextId, task: &CompactTask) {
        let mut policy = self.policy.write();
        let mut compactor_assigned_task_num = self.compactor_assigned_task_num.lock();
        policy.report_compact_task(context_id, task);
        Self::update_task_num(&mut compactor_assigned_task_num, context_id, false);
    }

    /// Forcefully purging the heartbeats for a task is only safe when the
    /// context has been completely removed from meta.
    /// Returns true if there were remaining heartbeats for the task.
    pub fn purge_heartbeats_for_context(&self, context_id: HummockContextId) -> bool {
        self.task_heartbeats.write().remove(&context_id).is_some()
    }

    pub fn get_expired_tasks(&self) -> Vec<(HummockContextId, CompactTask)> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut cancellable_tasks = vec![];
        {
            let guard = self.task_heartbeats.read();
            for (context_id, heartbeats) in guard.iter() {
                {
                    for TaskHeartbeat {
                        expire_at, task, ..
                    } in heartbeats.values()
                    {
                        if *expire_at < now {
                            cancellable_tasks.push((*context_id, task.clone()));
                        }
                    }
                }
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&self, context_id: HummockContextId, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        let entry = guard.entry(context_id).or_insert_with(HashMap::new);
        entry.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                expire_at: now + self.task_expiry_seconds,
            },
        );
    }

    pub fn remove_task_heartbeat(&self, context_id: HummockContextId, task_id: u64) {
        let mut guard = self.task_heartbeats.write();
        let mut garbage_collect = false;
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            heartbeats.remove(&task_id);
            if heartbeats.is_empty() {
                garbage_collect = true;
            }
        }
        if garbage_collect {
            guard.remove(&context_id);
        }
    }

    pub fn update_task_heartbeats(
        &self,
        context_id: HummockContextId,
        progress_list: &Vec<CompactTaskProgress>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            for progress in progress_list {
                if let Some(task_ref) = heartbeats.get_mut(&progress.task_id) {
                    if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                        || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                    {
                        // Refresh the expiry of the task as it is showing progress.
                        task_ref.expire_at = now + self.task_expiry_seconds;
                        // Update the task state to the latest state.
                        task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                        task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                    }
                }
            }
        }
    }

    fn update_task_num(
        compactor_assigned_task_num: &mut MutexGuard<HashMap<HummockContextId, u64>>,
        context_id: HummockContextId,
        inc: bool,
    ) {
        let task_num = compactor_assigned_task_num.get_mut(&context_id).unwrap();
        if inc {
            *task_num += 1;
        } else {
            debug_assert!(*task_num > 0);
            *task_num -= 1;
        }
    }
}
