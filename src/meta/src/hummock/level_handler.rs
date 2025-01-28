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

use itertools::Itertools;
use risingwave_hummock_sdk::level::Level;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockCompactionTaskId, HummockSstableId};
use risingwave_pb::hummock::level_handler::RunningCompactTask;

#[derive(Clone, Debug, PartialEq)]
pub struct LevelHandler {
    level: u32,
    compacting_files: HashMap<HummockSstableId, HummockCompactionTaskId>,
    pending_tasks: Vec<RunningCompactTask>,
}

impl LevelHandler {
    pub fn new(level: u32) -> Self {
        Self {
            level,
            compacting_files: HashMap::default(),
            pending_tasks: vec![],
        }
    }

    pub fn get_level(&self) -> u32 {
        self.level
    }

    pub fn remove_task(&mut self, target_task_id: u64) {
        for task in &self.pending_tasks {
            if task.task_id == target_task_id {
                for sst in &task.ssts {
                    self.compacting_files.remove(sst);
                }
            }
        }
        self.pending_tasks
            .retain(|task| task.task_id != target_task_id);
    }

    pub fn is_pending_compact(&self, sst_id: &HummockSstableId) -> bool {
        self.compacting_files.contains_key(sst_id)
    }

    pub fn pending_task_id_by_sst(
        &self,
        sst_id: &HummockSstableId,
    ) -> Option<HummockCompactionTaskId> {
        self.compacting_files.get(sst_id).cloned()
    }

    pub fn is_level_pending_compact(&self, level: &Level) -> bool {
        level
            .table_infos
            .iter()
            .any(|table| self.compacting_files.contains_key(&table.sst_id))
    }

    pub fn is_level_all_pending_compact(&self, level: &Level) -> bool {
        if level.table_infos.is_empty() {
            return false;
        }

        level
            .table_infos
            .iter()
            .all(|table| self.compacting_files.contains_key(&table.sst_id))
    }

    pub fn add_pending_task<'a>(
        &mut self,
        task_id: u64,
        target_level: usize,
        ssts: impl IntoIterator<Item = &'a SstableInfo>,
    ) {
        let target_level = target_level as u32;
        let mut table_ids = vec![];
        let mut total_file_size = 0;
        for sst in ssts {
            self.compacting_files.insert(sst.sst_id, task_id);
            total_file_size += sst.sst_size;
            table_ids.push(sst.sst_id);
        }

        self.pending_tasks.push(RunningCompactTask {
            task_id,
            target_level,
            total_file_size,
            ssts: table_ids,
        });
    }

    pub fn pending_file_count(&self) -> usize {
        self.compacting_files.len()
    }

    pub fn pending_file_size(&self) -> u64 {
        self.pending_tasks
            .iter()
            .map(|task| task.total_file_size)
            .sum::<u64>()
    }

    pub fn pending_output_file_size(&self, target_level: u32) -> u64 {
        self.pending_tasks
            .iter()
            .filter(|task| task.target_level == target_level)
            .map(|task| task.total_file_size)
            .sum::<u64>()
    }

    pub fn pending_tasks_ids(&self) -> Vec<u64> {
        self.pending_tasks
            .iter()
            .map(|task| task.task_id)
            .collect_vec()
    }

    pub fn pending_tasks(&self) -> &[RunningCompactTask] {
        &self.pending_tasks
    }

    pub fn compacting_files(&self) -> &HashMap<HummockSstableId, HummockCompactionTaskId> {
        &self.compacting_files
    }

    #[cfg(test)]
    pub(crate) fn test_add_pending_sst(&mut self, sst_id: HummockSstableId, task_id: u64) {
        self.compacting_files.insert(sst_id, task_id);
    }
}

impl From<&LevelHandler> for risingwave_pb::hummock::LevelHandler {
    fn from(lh: &LevelHandler) -> Self {
        risingwave_pb::hummock::LevelHandler {
            level: lh.level,
            tasks: lh.pending_tasks.clone(),
        }
    }
}

impl From<&risingwave_pb::hummock::LevelHandler> for LevelHandler {
    fn from(lh: &risingwave_pb::hummock::LevelHandler) -> Self {
        let mut pending_tasks = vec![];
        let mut compacting_files = HashMap::new();
        for task in &lh.tasks {
            pending_tasks.push(task.clone());
            for s in &task.ssts {
                compacting_files.insert(*s, task.task_id);
            }
        }
        Self {
            pending_tasks,
            compacting_files,
            level: lh.level,
        }
    }
}
