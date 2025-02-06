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

use std::sync::Arc;

use risingwave_common::config::default::compaction_config;
use risingwave_hummock_sdk::level::{InputLevel, Levels, OverlappingLevel};
use risingwave_hummock_sdk::KeyComparator;
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, LocalPickerStatistic,
    ValidationRuleType,
};
use crate::hummock::compaction::picker::TrivialMovePicker;
use crate::hummock::compaction::{create_overlap_strategy, CompactionDeveloperConfig};
use crate::hummock::level_handler::LevelHandler;

pub struct IntraCompactionPicker {
    config: Arc<CompactionConfig>,
    compaction_task_validator: Arc<CompactionTaskValidator>,

    developer_config: Arc<CompactionDeveloperConfig>,
}

impl CompactionPicker for IntraCompactionPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = &levels.l0;
        if l0.sub_levels.is_empty() {
            return None;
        }

        if let Some(ret) = self.pick_l0_trivial_move_file(l0, level_handlers, stats) {
            return Some(ret);
        }

        let vnode_partition_count = self.config.split_weight_by_vnode;

        if let Some(ret) =
            self.pick_whole_level(l0, &level_handlers[0], vnode_partition_count, stats)
        {
            if ret.input_levels.len() < 2 {
                tracing::error!(
                    ?ret,
                    vnode_partition_count,
                    "pick_whole_level failed to pick enough levels"
                );
                return None;
            }

            return Some(ret);
        }

        if let Some(ret) = self.pick_l0_intra(l0, &level_handlers[0], vnode_partition_count, stats)
        {
            if ret.input_levels.len() < 2 {
                tracing::error!(
                    ?ret,
                    vnode_partition_count,
                    "pick_l0_intra failed to pick enough levels"
                );
                return None;
            }

            return Some(ret);
        }

        None
    }
}

impl IntraCompactionPicker {
    #[cfg(test)]
    pub fn for_test(
        config: Arc<CompactionConfig>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> IntraCompactionPicker {
        IntraCompactionPicker {
            compaction_task_validator: Arc::new(CompactionTaskValidator::new(config.clone())),
            config,
            developer_config,
        }
    }

    pub fn new_with_validator(
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> IntraCompactionPicker {
        assert!(config.level0_sub_level_compact_level_count > 1);
        IntraCompactionPicker {
            config,
            compaction_task_validator,
            developer_config,
        }
    }

    fn pick_whole_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        partition_count: u32,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let picker = WholeLevelCompactionPicker::new(
            self.config.clone(),
            self.compaction_task_validator.clone(),
        );
        picker.pick_whole_level(l0, level_handler, partition_count, stats)
    }

    fn pick_l0_intra(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        vnode_partition_count: u32,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());
        let mut max_vnode_partition_idx = 0;
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.vnode_partition_count < vnode_partition_count {
                break;
            }
            max_vnode_partition_idx = idx;
        }

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type != LevelType::Nonoverlapping
                || level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
                continue;
            }

            if idx > max_vnode_partition_idx {
                break;
            }

            if level_handler.is_level_all_pending_compact(level) {
                continue;
            }

            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
                self.config.sub_level_max_compaction_bytes / 2,
                max_compaction_bytes,
                self.config.level0_sub_level_compact_level_count as usize,
                self.config.level0_max_compact_file_number,
                overlap_strategy.clone(),
                self.developer_config.enable_check_task_level_overlap,
                self.config
                    .max_l0_compact_level_count
                    .unwrap_or(compaction_config::max_l0_compact_level_count())
                    as usize,
            );

            let l0_select_tables_vec = non_overlap_sub_level_picker
                .pick_l0_multi_non_overlap_level(
                    &l0.sub_levels[idx..=max_vnode_partition_idx],
                    level_handler,
                );

            if l0_select_tables_vec.is_empty() {
                continue;
            }

            let mut select_input_size = 0;
            let mut total_file_count = 0;
            for input in l0_select_tables_vec {
                let mut max_level_size = 0;
                for level_select_table in &input.sstable_infos {
                    let level_select_size = level_select_table
                        .iter()
                        .map(|sst| sst.sst_size)
                        .sum::<u64>();

                    max_level_size = std::cmp::max(max_level_size, level_select_size);
                }

                let mut select_level_inputs = Vec::with_capacity(input.sstable_infos.len());
                for level_select_sst in input.sstable_infos {
                    if level_select_sst.is_empty() {
                        continue;
                    }
                    select_level_inputs.push(InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: level_select_sst,
                    });

                    select_input_size += input.total_file_size;
                    total_file_count += input.total_file_count;
                }
                select_level_inputs.reverse();

                let result = CompactionInput {
                    input_levels: select_level_inputs,
                    target_sub_level_id: level.sub_level_id,
                    select_input_size,
                    total_file_count: total_file_count as u64,
                    ..Default::default()
                };

                if !self.compaction_task_validator.valid_compact_task(
                    &result,
                    ValidationRuleType::Intra,
                    stats,
                ) {
                    continue;
                }

                return Some(result);
            }
        }

        None
    }

    fn pick_l0_trivial_move_file(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        if !self.developer_config.enable_trivial_move {
            return None;
        }

        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping || idx + 1 >= l0.sub_levels.len() {
                continue;
            }

            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping {
                continue;
            }

            if level_handlers[0].is_level_pending_compact(level) {
                continue;
            }

            if l0.sub_levels[idx + 1].vnode_partition_count
                != l0.sub_levels[idx].vnode_partition_count
            {
                continue;
            }

            let trivial_move_picker = TrivialMovePicker::new(
                0,
                0,
                overlap_strategy.clone(),
                0,
                self.config
                    .sst_allowed_trivial_move_max_count
                    .unwrap_or(compaction_config::sst_allowed_trivial_move_max_count())
                    as usize,
            );

            if let Some(select_ssts) = trivial_move_picker.pick_multi_trivial_move_ssts(
                &l0.sub_levels[idx + 1].table_infos,
                &level.table_infos,
                level_handlers,
                stats,
            ) {
                let select_input_size: u64 = select_ssts.iter().map(|sst| sst.sst_size).sum();
                let total_file_count = select_ssts.len() as u64;
                let mut target_sub_level_id = level.sub_level_id;
                // This optimisation is only enabled for ssts at the end of the level. See https://github.com/risingwavelabs/risingwave/pull/15659
                if select_ssts.last().unwrap().sst_id
                    == l0.sub_levels[idx + 1].table_infos.last().unwrap().sst_id
                    && idx != 0
                {
                    // Try to trivial move ssts cross multiple sub levels. It's very useful in append only scenarios.
                    let left_bound = &select_ssts.first().unwrap().key_range.left;
                    for probe_level in l0.sub_levels[0..idx].iter().rev() {
                        let last_sst = probe_level.table_infos.last().unwrap();
                        let level_max_bound = &last_sst.key_range.right;
                        // compare the left bound of the first sst with the max bound of the level, if the left bound is greater than the max bound, we can trivial move to the next level
                        if last_sst.key_range.right_exclusive {
                            if !KeyComparator::compare_encoded_full_key(level_max_bound, left_bound)
                                .is_le()
                            {
                                break;
                            }
                        } else if !KeyComparator::compare_encoded_full_key(
                            level_max_bound,
                            left_bound,
                        )
                        .is_lt()
                        {
                            break;
                        }

                        target_sub_level_id = probe_level.sub_level_id;
                    }

                    tracing::debug!(
                        "optimize trivial move origin sub_level_id: {}, target_sub_level_id: {} sst_count: {}",
                        level.sub_level_id, target_sub_level_id, select_ssts.len()
                    );
                }

                let mut overlap = overlap_strategy.create_overlap_info();
                select_ssts.iter().for_each(|ssts| overlap.update(ssts));

                assert!(overlap
                    .check_multiple_overlap(&l0.sub_levels[idx].table_infos)
                    .is_empty());

                let input_levels = vec![
                    InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: select_ssts,
                    },
                    InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping,
                        table_infos: vec![],
                    },
                ];
                return Some(CompactionInput {
                    input_levels,
                    target_level: 0,
                    target_sub_level_id,
                    select_input_size,
                    total_file_count,
                    ..Default::default()
                });
            }
        }

        None
    }
}

pub struct WholeLevelCompactionPicker {
    config: Arc<CompactionConfig>,
    compaction_task_validator: Arc<CompactionTaskValidator>,
}

impl WholeLevelCompactionPicker {
    pub fn new(
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
    ) -> Self {
        Self {
            config,
            compaction_task_validator,
        }
    }

    pub fn pick_whole_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        partition_count: u32,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        if partition_count == 0 {
            return None;
        }
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type != LevelType::Nonoverlapping
                || level.vnode_partition_count == partition_count
            {
                continue;
            }

            let max_compaction_bytes = std::cmp::max(
                self.config.max_bytes_for_level_base,
                self.config.sub_level_max_compaction_bytes
                    * (self.config.level0_sub_level_compact_level_count as u64),
            );

            let mut select_input_size = 0;

            let mut select_level_inputs = vec![];
            let mut total_file_count = 0;
            let mut wait_enough = false;
            for next_level in l0.sub_levels.iter().skip(idx) {
                if (select_input_size > max_compaction_bytes
                    || total_file_count > self.config.level0_max_compact_file_number
                    || next_level.vnode_partition_count == partition_count)
                    && select_level_inputs.len() > 1
                {
                    wait_enough = true;
                    break;
                }

                if level_handler.is_level_pending_compact(next_level) {
                    break;
                }

                select_input_size += next_level.total_file_size;
                total_file_count += next_level.table_infos.len() as u64;

                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: next_level.level_type,
                    table_infos: next_level.table_infos.clone(),
                });
            }
            if select_level_inputs.len() > 1 {
                let vnode_partition_count =
                    if select_input_size > self.config.sub_level_max_compaction_bytes / 2 {
                        partition_count
                    } else {
                        0
                    };
                let result = CompactionInput {
                    input_levels: select_level_inputs,
                    target_sub_level_id: level.sub_level_id,
                    select_input_size,
                    total_file_count,
                    vnode_partition_count,
                    ..Default::default()
                };
                if wait_enough
                    || self.compaction_task_validator.valid_compact_task(
                        &result,
                        ValidationRuleType::Intra,
                        stats,
                    )
                {
                    return Some(result);
                }
            }
        }

        None
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_hummock_sdk::level::Level;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_multi_sublevels, generate_l0_nonoverlapping_sublevels,
        generate_l0_overlapping_sublevels, generate_level, generate_table,
        push_table_level0_overlapping, push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::TierCompactionPicker;

    fn create_compaction_picker_for_test() -> IntraCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        IntraCompactionPicker::for_test(config, Arc::new(CompactionDeveloperConfig::default()))
    }

    #[test]
    fn test_l0_to_l1_compact_conflict() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping,
            table_infos: vec![],
            ..Default::default()
        }];
        let mut levels = Levels {
            levels,
            l0: OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            },
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(1, 1, 100, 300, 2),
                generate_table(2, 1, 350, 500, 2),
            ],
        );
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(3)
            .build();
        let mut picker = TierCompactionPicker::new(Arc::new(config));

        let ret: Option<CompactionInput> =
            picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
    }

    #[test]
    fn test_compacting_key_range_overlap_intra_l0() {
        // When picking L0->L0, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let mut picker = create_compaction_picker_for_test();

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
                ..Default::default()
            }],
            l0: generate_l0_nonoverlapping_sublevels(vec![
                generate_table(1, 1, 100, 210, 2),
                generate_table(2, 1, 200, 250, 2),
            ]),
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        ret.add_pending_task(0, &mut levels_handler);

        push_table_level0_overlapping(&mut levels, generate_table(4, 1, 170, 180, 3));
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());
    }

    #[test]
    fn test_pick_l0_intra() {
        {
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(6, 1, 50, 99, 1),
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 250, 300, 1),
                ],
                vec![
                    generate_table(3, 1, 10, 90, 1),
                    generate_table(6, 1, 100, 110, 1),
                ],
                vec![
                    generate_table(4, 1, 50, 99, 1),
                    generate_table(5, 1, 100, 200, 1),
                ],
            ]);
            let levels = Levels {
                l0,
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, &levels.levels[0].table_infos);
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .level0_overlapping_sub_level_compact_level_count(4)
                    .build(),
            );
            let mut picker = IntraCompactionPicker::for_test(
                config,
                Arc::new(CompactionDeveloperConfig::default()),
            );
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );
        }

        {
            // Suppose keyguard [100, 200] [300, 400]
            // will pick sst [1, 3, 4]
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(3, 1, 100, 200, 1),
                    generate_table(6, 1, 300, 500, 1),
                ],
                vec![
                    generate_table(4, 1, 100, 200, 1),
                    generate_table(5, 1, 300, 400, 1),
                ],
            ]);
            let levels = Levels {
                l0,
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, &levels.levels[0].table_infos);
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .sub_level_max_compaction_bytes(300)
                    .build(),
            );
            let mut picker = IntraCompactionPicker::for_test(
                config,
                Arc::new(CompactionDeveloperConfig::default()),
            );
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(4, ret.input_levels[0].table_infos[0].sst_id);
            assert_eq!(3, ret.input_levels[1].table_infos[0].sst_id);
            assert_eq!(1, ret.input_levels[2].table_infos[0].sst_id);

            // will pick sst [2, 6]
            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(
                ret2.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                2
            );

            assert_eq!(6, ret2.input_levels[0].table_infos[0].sst_id);
            assert_eq!(2, ret2.input_levels[1].table_infos[0].sst_id);
        }

        {
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 100, 149, 1),
                    generate_table(6, 1, 150, 199, 1),
                    generate_table(7, 1, 200, 250, 1),
                    generate_table(2, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(3, 1, 100, 149, 1),
                    generate_table(8, 1, 150, 199, 1),
                    generate_table(9, 1, 200, 250, 1),
                    generate_table(10, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(4, 1, 100, 199, 1),
                    generate_table(11, 1, 200, 250, 1),
                    generate_table(5, 1, 300, 350, 1),
                ],
            ]);
            let levels = Levels {
                l0,
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, &levels.levels[0].table_infos);
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .sub_level_max_compaction_bytes(300)
                    .build(),
            );
            let mut picker = IntraCompactionPicker::for_test(
                config,
                Arc::new(CompactionDeveloperConfig::default()),
            );
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(11, ret.input_levels[0].table_infos[0].sst_id);
            assert_eq!(9, ret.input_levels[1].table_infos[0].sst_id);
            assert_eq!(7, ret.input_levels[2].table_infos[0].sst_id);

            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(
                ret2.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(5, ret2.input_levels[0].table_infos[0].sst_id);
            assert_eq!(10, ret2.input_levels[1].table_infos[0].sst_id);
            assert_eq!(2, ret2.input_levels[2].table_infos[0].sst_id);
        }
    }

    fn is_l0_trivial_move(compaction_input: &CompactionInput) -> bool {
        compaction_input.input_levels.len() == 2
            && !compaction_input.input_levels[0].table_infos.is_empty()
            && compaction_input.input_levels[1].table_infos.is_empty()
    }

    #[test]
    fn test_trivial_move() {
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .target_file_size_base(30)
                .level0_sub_level_compact_level_count(20) // reject intra
                .build(),
        );
        let mut picker =
            IntraCompactionPicker::for_test(config, Arc::new(CompactionDeveloperConfig::default()));

        // Cannot trivial move because there is only 1 sub-level.
        let l0 = generate_l0_overlapping_sublevels(vec![vec![
            generate_table(1, 1, 100, 110, 1),
            generate_table(2, 1, 150, 250, 1),
        ]]);
        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            ..Default::default()
        };
        levels_handler[1].add_pending_task(100, 1, &levels.levels[0].table_infos);
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because sub-levels are overlapping
        let l0: OverlappingLevel = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(1, 1, 100, 110, 1),
                generate_table(2, 1, 150, 250, 1),
            ],
            vec![generate_table(3, 1, 10, 90, 1)],
            vec![generate_table(4, 1, 10, 90, 1)],
            vec![generate_table(5, 1, 10, 90, 1)],
        ]);
        let mut levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            ..Default::default()
        };
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());

        // Cannot trivial move because latter sub-level is overlapping
        levels.l0.sub_levels[0].level_type = LevelType::Nonoverlapping;
        levels.l0.sub_levels[1].level_type = LevelType::Overlapping;
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because former sub-level is overlapping
        levels.l0.sub_levels[0].level_type = LevelType::Overlapping;
        levels.l0.sub_levels[1].level_type = LevelType::Nonoverlapping;
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // trivial move
        levels.l0.sub_levels[0].level_type = LevelType::Nonoverlapping;
        levels.l0.sub_levels[1].level_type = LevelType::Nonoverlapping;
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(is_l0_trivial_move(&ret));
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
    }
    #[test]
    fn test_pick_whole_level() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_max_compact_file_number(20)
                .build(),
        );
        let mut table_infos = vec![];
        for epoch in 1..3 {
            let base = epoch * 100;
            let mut ssts = vec![];
            for i in 1..50 {
                let left = (i as usize) * 100;
                let right = left + 100;
                ssts.push(generate_table(base + i, 1, left, right, epoch));
            }
            table_infos.push(ssts);
        }

        let l0 = generate_l0_nonoverlapping_multi_sublevels(table_infos);
        let compaction_task_validator = Arc::new(CompactionTaskValidator::new(config.clone()));
        let picker = WholeLevelCompactionPicker::new(config, compaction_task_validator);
        let level_handler = LevelHandler::new(0);
        let ret = picker
            .pick_whole_level(&l0, &level_handler, 4, &mut LocalPickerStatistic::default())
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
    }

    #[test]
    fn test_priority() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_max_compact_file_number(20)
                .sub_level_max_compaction_bytes(1)
                .level0_sub_level_compact_level_count(2)
                .build(),
        );
        let mut table_infos = vec![];
        for epoch in 1..3 {
            let base = epoch * 100;
            let mut ssts = vec![];
            for i in 1..50 {
                let left = (i as usize) * 100;
                let right = left + 100;
                ssts.push(generate_table(base + i, 1, left, right, epoch));
            }
            table_infos.push(ssts);
        }

        let mut l0 = generate_l0_nonoverlapping_multi_sublevels(table_infos);
        // trivial-move
        l0.sub_levels[1]
            .table_infos
            .push(generate_table(9999, 900000000, 0, 100, 1));

        l0.sub_levels[0].total_file_size = 1;
        l0.sub_levels[1].total_file_size = 1;

        let mut picker = IntraCompactionPicker::new_with_validator(
            config,
            Arc::new(CompactionTaskValidator::unused()),
            Arc::new(CompactionDeveloperConfig::default()),
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            ..Default::default()
        };

        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(is_l0_trivial_move(ret.as_ref().unwrap()));
        ret.as_ref()
            .unwrap()
            .add_pending_task(1, &mut levels_handler);
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_some());
        let input = ret.as_ref().unwrap();
        assert_eq!(input.input_levels.len(), 2);
        assert_ne!(
            levels.l0.sub_levels[0].table_infos.len(),
            input.input_levels[0].table_infos.len()
        );
        assert_ne!(
            levels.l0.sub_levels[1].table_infos.len(),
            input.input_levels[1].table_infos.len()
        );
    }

    #[test]
    fn test_trivial_move_multi_level() {
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .target_file_size_base(30)
                .level0_sub_level_compact_level_count(20) // reject intra
                .build(),
        );
        let mut picker =
            IntraCompactionPicker::for_test(config, Arc::new(CompactionDeveloperConfig::default()));

        // Cannot trivial move because there is only 1 sub-level.
        let l0 = generate_l0_overlapping_sublevels(vec![vec![
            generate_table(1, 1, 100, 110, 1),
            generate_table(2, 1, 150, 250, 1),
        ]]);
        let levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            ..Default::default()
        };
        levels_handler[1].add_pending_task(100, 1, &levels.levels[0].table_infos);
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because sub-levels are overlapping
        let l0: OverlappingLevel = generate_l0_overlapping_sublevels(vec![
            vec![generate_table(1, 1, 100, 110, 1)],
            vec![generate_table(3, 1, 10, 120, 1)],
            vec![generate_table(4, 1, 10, 120, 1)],
            vec![
                generate_table(5, 1, 10, 120, 1),
                generate_table(2, 1, 1500, 2500, 1),
            ],
        ]);
        let mut levels = Levels {
            l0,
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            ..Default::default()
        };
        // trivial move
        levels.l0.sub_levels[0].level_type = LevelType::Nonoverlapping;
        levels.l0.sub_levels[1].level_type = LevelType::Nonoverlapping;
        levels.l0.sub_levels[2].level_type = LevelType::Nonoverlapping;
        levels.l0.sub_levels[3].level_type = LevelType::Nonoverlapping;
        let input = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(is_l0_trivial_move(&input));
        assert_eq!(input.target_sub_level_id, 0);
    }
}
