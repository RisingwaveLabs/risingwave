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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;

use itertools::Itertools;
use risingwave_pb::hummock::compact_task::{PbTaskStatus, PbTaskType, TaskStatus, TaskType};
use risingwave_pb::hummock::subscribe_compaction_event_request::PbReportTask;
use risingwave_pb::hummock::{
    LevelType, PbCompactTask, PbKeyRange, PbTableOption, PbTableSchema, PbTableStats,
    PbValidationTask,
};

use crate::key_range::KeyRange;
use crate::level::InputLevel;
use crate::sstable_info::SstableInfo;
use crate::table_watermark::TableWatermarks;
use crate::HummockSstableObjectId;

#[derive(Clone, PartialEq, Default, Debug)]
pub struct CompactTask {
    /// SSTs to be compacted, which will be removed from LSM after compaction
    pub input_ssts: Vec<InputLevel>,
    /// In ideal case, the compaction will generate `splits.len()` tables which have key range
    /// corresponding to that in `splits`, respectively
    pub splits: Vec<KeyRange>,
    /// compaction output, which will be added to `target_level` of LSM after compaction
    pub sorted_output_ssts: Vec<SstableInfo>,
    /// task id assigned by hummock storage service
    pub task_id: u64,
    /// compaction output will be added to `target_level` of LSM after compaction
    pub target_level: u32,
    pub gc_delete_keys: bool,
    /// Lbase in LSM
    pub base_level: u32,
    pub task_status: PbTaskStatus,
    /// compaction group the task belongs to
    pub compaction_group_id: u64,
    /// `existing_table_ids` for compaction drop key
    pub existing_table_ids: Vec<u32>,
    pub compression_algorithm: u32,
    pub target_file_size: u64,
    pub compaction_filter_mask: u32,
    pub table_options: BTreeMap<u32, PbTableOption>,
    pub current_epoch_time: u64,
    pub target_sub_level_id: u64,
    /// Identifies whether the task is `space_reclaim`, if the `compact_task_type` increases, it will be refactored to enum
    pub task_type: PbTaskType,
    /// Deprecated. use `table_vnode_partition` instead;
    pub split_by_state_table: bool,
    /// Compaction needs to cut the state table every time 1/weight of vnodes in the table have been processed.
    /// Deprecated. use `table_vnode_partition` instead;
    pub split_weight_by_vnode: u32,
    pub table_vnode_partition: BTreeMap<u32, u32>,
    /// The table watermark of any table id. In compaction we only use the table watermarks on safe epoch,
    /// so we only need to include the table watermarks on safe epoch to reduce the size of metadata.
    pub table_watermarks: BTreeMap<u32, TableWatermarks>,

    pub table_schemas: BTreeMap<u32, PbTableSchema>,

    pub max_sub_compaction: u32,
}

impl CompactTask {
    pub fn estimated_encode_len(&self) -> usize {
        self.input_ssts
            .iter()
            .map(|input_level| input_level.estimated_encode_len())
            .sum::<usize>()
            + self
                .splits
                .iter()
                .map(|split| split.left.len() + split.right.len() + size_of::<bool>())
                .sum::<usize>()
            + size_of::<u64>()
            + self
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u64>()
            + size_of::<u32>()
            + size_of::<bool>()
            + size_of::<u32>()
            + size_of::<i32>()
            + size_of::<u64>()
            + self.existing_table_ids.len() * size_of::<u32>()
            + size_of::<u32>()
            + size_of::<u64>()
            + size_of::<u32>()
            + self.table_options.len() * size_of::<u64>()
            + size_of::<u64>()
            + size_of::<u64>()
            + size_of::<i32>()
            + size_of::<bool>()
            + size_of::<u32>()
            + self.table_vnode_partition.len() * size_of::<u64>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| size_of::<u32>() + table_watermark.estimated_encode_len())
                .sum::<usize>()
    }

    pub fn is_trivial_move_task(&self) -> bool {
        if self.task_type != TaskType::Dynamic && self.task_type != TaskType::Emergency {
            return false;
        }

        if self.input_ssts.len() != 2 || self.input_ssts[0].level_type != LevelType::Nonoverlapping
        {
            return false;
        }

        // it may be a manual compaction task
        if self.input_ssts[0].level_idx == self.input_ssts[1].level_idx
            && self.input_ssts[0].level_idx > 0
        {
            return false;
        }

        if self.input_ssts[1].level_idx == self.target_level
            && self.input_ssts[1].table_infos.is_empty()
        {
            return true;
        }

        false
    }

    pub fn is_trivial_reclaim(&self) -> bool {
        // Currently all VnodeWatermark tasks are trivial reclaim.
        if self.task_type == TaskType::VnodeWatermark {
            return true;
        }
        let exist_table_ids = HashSet::<u32>::from_iter(self.existing_table_ids.clone());
        self.input_ssts.iter().all(|level| {
            level.table_infos.iter().all(|sst| {
                sst.table_ids
                    .iter()
                    .all(|table_id| !exist_table_ids.contains(table_id))
            })
        })
    }
}

impl From<PbCompactTask> for CompactTask {
    fn from(pb_compact_task: PbCompactTask) -> Self {
        #[expect(deprecated)]
        Self {
            input_ssts: pb_compact_task
                .input_ssts
                .into_iter()
                .map(InputLevel::from)
                .collect_vec(),
            splits: pb_compact_task
                .splits
                .into_iter()
                .map(|pb_keyrange| KeyRange {
                    left: pb_keyrange.left.into(),
                    right: pb_keyrange.right.into(),
                    right_exclusive: pb_keyrange.right_exclusive,
                })
                .collect_vec(),
            sorted_output_ssts: pb_compact_task
                .sorted_output_ssts
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            task_id: pb_compact_task.task_id,
            target_level: pb_compact_task.target_level,
            gc_delete_keys: pb_compact_task.gc_delete_keys,
            base_level: pb_compact_task.base_level,
            task_status: TaskStatus::try_from(pb_compact_task.task_status).unwrap(),
            compaction_group_id: pb_compact_task.compaction_group_id,
            existing_table_ids: pb_compact_task.existing_table_ids.clone(),
            compression_algorithm: pb_compact_task.compression_algorithm,
            target_file_size: pb_compact_task.target_file_size,
            compaction_filter_mask: pb_compact_task.compaction_filter_mask,
            table_options: pb_compact_task.table_options.clone(),
            current_epoch_time: pb_compact_task.current_epoch_time,
            target_sub_level_id: pb_compact_task.target_sub_level_id,
            task_type: PbTaskType::try_from(pb_compact_task.task_type).unwrap(),
            split_by_state_table: pb_compact_task.split_by_state_table,
            split_weight_by_vnode: pb_compact_task.split_weight_by_vnode,
            table_vnode_partition: pb_compact_task.table_vnode_partition.clone(),
            table_watermarks: pb_compact_task
                .table_watermarks
                .into_iter()
                .map(|(table_id, pb_table_watermark)| {
                    (table_id, TableWatermarks::from(pb_table_watermark))
                })
                .collect(),
            table_schemas: pb_compact_task.table_schemas,
            max_sub_compaction: pb_compact_task.max_sub_compaction,
        }
    }
}

impl From<&PbCompactTask> for CompactTask {
    fn from(pb_compact_task: &PbCompactTask) -> Self {
        #[expect(deprecated)]
        Self {
            input_ssts: pb_compact_task
                .input_ssts
                .iter()
                .map(InputLevel::from)
                .collect_vec(),
            splits: pb_compact_task
                .splits
                .iter()
                .map(|pb_keyrange| KeyRange {
                    left: pb_keyrange.left.clone().into(),
                    right: pb_keyrange.right.clone().into(),
                    right_exclusive: pb_keyrange.right_exclusive,
                })
                .collect_vec(),
            sorted_output_ssts: pb_compact_task
                .sorted_output_ssts
                .iter()
                .map(SstableInfo::from)
                .collect_vec(),
            task_id: pb_compact_task.task_id,
            target_level: pb_compact_task.target_level,
            gc_delete_keys: pb_compact_task.gc_delete_keys,
            base_level: pb_compact_task.base_level,
            task_status: TaskStatus::try_from(pb_compact_task.task_status).unwrap(),
            compaction_group_id: pb_compact_task.compaction_group_id,
            existing_table_ids: pb_compact_task.existing_table_ids.clone(),
            compression_algorithm: pb_compact_task.compression_algorithm,
            target_file_size: pb_compact_task.target_file_size,
            compaction_filter_mask: pb_compact_task.compaction_filter_mask,
            table_options: pb_compact_task.table_options.clone(),
            current_epoch_time: pb_compact_task.current_epoch_time,
            target_sub_level_id: pb_compact_task.target_sub_level_id,
            task_type: PbTaskType::try_from(pb_compact_task.task_type).unwrap(),
            split_by_state_table: pb_compact_task.split_by_state_table,
            split_weight_by_vnode: pb_compact_task.split_weight_by_vnode,
            table_vnode_partition: pb_compact_task.table_vnode_partition.clone(),
            table_watermarks: pb_compact_task
                .table_watermarks
                .iter()
                .map(|(table_id, pb_table_watermark)| {
                    (*table_id, TableWatermarks::from(pb_table_watermark))
                })
                .collect(),
            table_schemas: pb_compact_task.table_schemas.clone(),
            max_sub_compaction: pb_compact_task.max_sub_compaction,
        }
    }
}

impl From<CompactTask> for PbCompactTask {
    fn from(compact_task: CompactTask) -> Self {
        #[expect(deprecated)]
        Self {
            input_ssts: compact_task
                .input_ssts
                .into_iter()
                .map(|input_level| input_level.into())
                .collect_vec(),
            splits: compact_task
                .splits
                .into_iter()
                .map(|keyrange| PbKeyRange {
                    left: keyrange.left.into(),
                    right: keyrange.right.into(),
                    right_exclusive: keyrange.right_exclusive,
                })
                .collect_vec(),
            sorted_output_ssts: compact_task
                .sorted_output_ssts
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
            task_id: compact_task.task_id,
            target_level: compact_task.target_level,
            gc_delete_keys: compact_task.gc_delete_keys,
            base_level: compact_task.base_level,
            task_status: compact_task.task_status.into(),
            compaction_group_id: compact_task.compaction_group_id,
            existing_table_ids: compact_task.existing_table_ids.clone(),
            compression_algorithm: compact_task.compression_algorithm,
            target_file_size: compact_task.target_file_size,
            compaction_filter_mask: compact_task.compaction_filter_mask,
            table_options: compact_task.table_options.clone(),
            current_epoch_time: compact_task.current_epoch_time,
            target_sub_level_id: compact_task.target_sub_level_id,
            task_type: compact_task.task_type.into(),
            split_weight_by_vnode: compact_task.split_weight_by_vnode,
            table_vnode_partition: compact_task.table_vnode_partition.clone(),
            table_watermarks: compact_task
                .table_watermarks
                .into_iter()
                .map(|(table_id, table_watermark)| (table_id, table_watermark.into()))
                .collect(),
            split_by_state_table: compact_task.split_by_state_table,
            table_schemas: compact_task.table_schemas.clone(),
            max_sub_compaction: compact_task.max_sub_compaction,
        }
    }
}

impl From<&CompactTask> for PbCompactTask {
    fn from(compact_task: &CompactTask) -> Self {
        #[expect(deprecated)]
        Self {
            input_ssts: compact_task
                .input_ssts
                .iter()
                .map(|input_level| input_level.into())
                .collect_vec(),
            splits: compact_task
                .splits
                .iter()
                .map(|keyrange| PbKeyRange {
                    left: keyrange.left.to_vec(),
                    right: keyrange.right.to_vec(),
                    right_exclusive: keyrange.right_exclusive,
                })
                .collect_vec(),
            sorted_output_ssts: compact_task
                .sorted_output_ssts
                .iter()
                .map(|sst| sst.into())
                .collect_vec(),
            task_id: compact_task.task_id,
            target_level: compact_task.target_level,
            gc_delete_keys: compact_task.gc_delete_keys,
            base_level: compact_task.base_level,
            task_status: compact_task.task_status.into(),
            compaction_group_id: compact_task.compaction_group_id,
            existing_table_ids: compact_task.existing_table_ids.clone(),
            compression_algorithm: compact_task.compression_algorithm,
            target_file_size: compact_task.target_file_size,
            compaction_filter_mask: compact_task.compaction_filter_mask,
            table_options: compact_task.table_options.clone(),
            current_epoch_time: compact_task.current_epoch_time,
            target_sub_level_id: compact_task.target_sub_level_id,
            task_type: compact_task.task_type.into(),
            split_weight_by_vnode: compact_task.split_weight_by_vnode,
            table_vnode_partition: compact_task.table_vnode_partition.clone(),
            table_watermarks: compact_task
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| (*table_id, table_watermark.into()))
                .collect(),
            split_by_state_table: compact_task.split_by_state_table,
            table_schemas: compact_task.table_schemas.clone(),
            max_sub_compaction: compact_task.max_sub_compaction,
        }
    }
}

#[derive(Clone, PartialEq, Default)]
pub struct ValidationTask {
    pub sst_infos: Vec<SstableInfo>,
    pub sst_id_to_worker_id: HashMap<u64, u32>,
}

impl From<PbValidationTask> for ValidationTask {
    fn from(pb_validation_task: PbValidationTask) -> Self {
        Self {
            sst_infos: pb_validation_task
                .sst_infos
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            sst_id_to_worker_id: pb_validation_task.sst_id_to_worker_id.clone(),
        }
    }
}

impl From<ValidationTask> for PbValidationTask {
    fn from(validation_task: ValidationTask) -> Self {
        Self {
            sst_infos: validation_task
                .sst_infos
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
            sst_id_to_worker_id: validation_task.sst_id_to_worker_id.clone(),
        }
    }
}

impl ValidationTask {
    pub fn estimated_encode_len(&self) -> usize {
        self.sst_infos
            .iter()
            .map(|sst| sst.estimated_encode_len())
            .sum::<usize>()
            + self.sst_id_to_worker_id.len() * (size_of::<u64>() + size_of::<u32>())
            + size_of::<u64>()
    }
}

#[derive(Clone, PartialEq, Default, Debug)]
pub struct ReportTask {
    pub table_stats_change: HashMap<u32, PbTableStats>,
    pub task_id: u64,
    pub task_status: TaskStatus,
    pub sorted_output_ssts: Vec<SstableInfo>,
    pub object_timestamps: HashMap<HummockSstableObjectId, u64>,
}

impl From<PbReportTask> for ReportTask {
    fn from(value: PbReportTask) -> Self {
        Self {
            table_stats_change: value.table_stats_change.clone(),
            task_id: value.task_id,
            task_status: PbTaskStatus::try_from(value.task_status).unwrap(),
            sorted_output_ssts: value
                .sorted_output_ssts
                .into_iter()
                .map(SstableInfo::from)
                .collect_vec(),
            object_timestamps: value.object_timestamps,
        }
    }
}

impl From<ReportTask> for PbReportTask {
    fn from(value: ReportTask) -> Self {
        Self {
            table_stats_change: value.table_stats_change.clone(),
            task_id: value.task_id,
            task_status: value.task_status.into(),
            sorted_output_ssts: value
                .sorted_output_ssts
                .into_iter()
                .map(|sst| sst.into())
                .collect_vec(),
            object_timestamps: value.object_timestamps,
        }
    }
}
