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

mod base_level_compaction_picker;
mod manual_compaction_picker;
mod min_overlap_compaction_picker;
mod space_reclaim_compaction_picker;
mod tier_compaction_picker;
mod tombstone_reclaim_compaction_picker;
mod trivial_move_compaction_picker;
mod ttl_reclaim_compaction_picker;

use std::collections::BTreeSet;
use std::sync::Arc;

pub use base_level_compaction_picker::LevelCompactionPicker;
pub use manual_compaction_picker::ManualCompactionPicker;
pub use min_overlap_compaction_picker::MinOverlappingPicker;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel};
pub use space_reclaim_compaction_picker::{SpaceReclaimCompactionPicker, SpaceReclaimPickerState};
pub use tier_compaction_picker::TierCompactionPicker;
pub use tombstone_reclaim_compaction_picker::{
    TombstoneReclaimCompactionPicker, TombstoneReclaimPickerState,
};
pub use trivial_move_compaction_picker::TrivialMovePicker;
pub use ttl_reclaim_compaction_picker::{TtlPickerState, TtlReclaimCompactionPicker};

use crate::hummock::level_handler::LevelHandler;

pub const MAX_COMPACT_LEVEL_COUNT: usize = 42;

#[derive(Default)]
pub struct LocalPickerStatistic {
    pub skip_by_write_amp_limit: u64,
    pub skip_by_count_limit: u64,
    pub skip_by_pending_files: u64,
    pub skip_by_overlapping: u64,
}
pub struct CompactionInput {
    pub input_levels: Vec<InputLevel>,
    pub target_level: usize,
    pub target_sub_level_id: u64,
}

impl CompactionInput {
    pub fn add_pending_task(&self, task_id: u64, level_handlers: &mut [LevelHandler]) {
        let mut has_l0 = false;
        for level in &self.input_levels {
            if level.level_idx != 0 {
                level_handlers[level.level_idx as usize].add_pending_task(
                    task_id,
                    self.target_level,
                    &level.table_infos,
                );
            } else {
                has_l0 = true;
            }
        }
        if has_l0 {
            let table_infos = self
                .input_levels
                .iter()
                .filter(|level| level.level_idx == 0)
                .flat_map(|level| level.table_infos.iter());
            level_handlers[0].add_pending_task(task_id, self.target_level, table_infos);
        }
    }
}

pub trait CompactionPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput>;
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionTaskOptimizeRule {
    Tier = 0,
    Intra = 1,
    ToBase = 2,
}

pub struct CompactionTaskValidator {
    config: Arc<CompactionConfig>,
    optimize_rules: BTreeSet<CompactionTaskOptimizeRule>,
}

impl CompactionTaskValidator {
    fn new(config: Arc<CompactionConfig>) -> Self {
        CompactionTaskValidator {
            config,
            optimize_rules: BTreeSet::from_iter(
                vec![
                    CompactionTaskOptimizeRule::Tier,
                    CompactionTaskOptimizeRule::Intra,
                    CompactionTaskOptimizeRule::ToBase,
                ]
                .into_iter(),
            ),
        }
    }

    fn valid_tier_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        // so the design here wants to merge multiple overlapping-levels in one compaction
        let max_compaction_bytes = std::cmp::min(
            self.config.max_compaction_bytes,
            self.config.sub_level_max_compaction_bytes
                * self.config.level0_overlapping_sub_level_compact_level_count as u64,
        );

        let compaction_bytes: u64 = input
            .input_levels
            .iter()
            .map(|overlapping_sub_level| {
                overlapping_sub_level
                    .table_infos
                    .iter()
                    .map(|sst| sst.file_size)
                    .sum::<u64>()
            })
            .sum();

        let compact_file_count: u64 = input
            .input_levels
            .iter()
            .map(|overlapping_sub_level| overlapping_sub_level.table_infos.len() as u64)
            .sum();

        // Limit sstable file count to avoid using too much memory.
        let overlapping_max_compact_file_numer = std::cmp::min(
            self.config.level0_max_compact_file_number,
            MAX_COMPACT_LEVEL_COUNT as u64,
        );

        let waiting_enough_files = {
            if compaction_bytes > max_compaction_bytes {
                false
            } else {
                compact_file_count <= overlapping_max_compact_file_numer
            }
        };

        // If waiting_enough_files is not satisfied, we will raise the priority of the number of
        // levels to ensure that we can merge as many sub_levels as possible
        let tier_sub_level_compact_level_count =
            self.config.level0_overlapping_sub_level_compact_level_count as usize;
        if input.input_levels.len() < tier_sub_level_compact_level_count && waiting_enough_files {
            stats.skip_by_count_limit += 1;
            return false;
        }

        true
    }

    fn valid_intra_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        let intra_sub_level_compact_level_count =
            self.config.level0_sub_level_compact_level_count as usize;

        if input.input_levels.len() < intra_sub_level_compact_level_count {
            return false;
        }

        let mut max_level_size = 0;
        let mut total_file_size = 0;
        let mut total_file_count = 0;
        for select_level in &input.input_levels {
            let level_select_size = select_level
                .table_infos
                .iter()
                .map(|sst| sst.file_size)
                .sum::<u64>();

            max_level_size = std::cmp::max(max_level_size, level_select_size);

            total_file_size += level_select_size;
            total_file_count += select_level.table_infos.len();
        }

        // This limitation would keep our write-amplification no more than
        // ln(max_compaction_bytes/flush_level_bytes) /
        // ln(self.config.level0_sub_level_compact_level_count/2) Here we only use half
        // of level0_sub_level_compact_level_count just for convenient.
        let is_write_amp_large =
            max_level_size * self.config.level0_sub_level_compact_level_count as u64 / 2
                >= total_file_size;

        if is_write_amp_large
            && total_file_count < self.config.level0_max_compact_file_number as usize
        {
            stats.skip_by_write_amp_limit += 1;
            return false;
        }

        if input.input_levels.len() < intra_sub_level_compact_level_count
            && total_file_count < self.config.level0_max_compact_file_number as usize
        {
            stats.skip_by_count_limit += 1;
            return false;
        }

        true
    }

    fn valid_base_level_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        let target_level_size: u64 = input
            .input_levels
            .last()
            .as_ref()
            .unwrap()
            .table_infos
            .iter()
            .map(|sst| sst.file_size)
            .sum();

        let select_file_size: u64 = input
            .input_levels
            .iter()
            .map(|level| {
                level
                    .table_infos
                    .iter()
                    .map(|sst| sst.file_size)
                    .sum::<u64>()
            })
            .sum::<u64>()
            - target_level_size;

        // The size of target level may be too large, we shall skip this compact task and wait
        //  the data in base level compact to lower level.
        if target_level_size > self.config.max_compaction_bytes {
            stats.skip_by_count_limit += 1;
            return false;
        }

        if select_file_size < target_level_size {
            stats.skip_by_write_amp_limit += 1;
            return false;
        }

        true
    }

    fn valid_compact_task(
        &self,
        input: &CompactionInput,
        picker_type: CompactionTaskOptimizeRule,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        if !self.optimize_rules.contains(&picker_type) {
            return true;
        }

        match picker_type {
            CompactionTaskOptimizeRule::Tier => self.valid_tier_compaction(input, stats),
            CompactionTaskOptimizeRule::Intra => self.valid_intra_compaction(input, stats),
            CompactionTaskOptimizeRule::ToBase => self.valid_base_level_compaction(input, stats),
        }
    }
}
