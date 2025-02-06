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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::{enumerate, Itertools};
use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::IntGauge;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::object_size_map;
use risingwave_hummock_sdk::level::Levels;
use risingwave_hummock_sdk::table_stats::PbTableStatsMap;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId, HummockVersionId};
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{
    CompactionConfig, HummockPinnedVersion, HummockVersionStats, LevelType,
};

use super::compaction::selector::DynamicLevelSelectorCore;
use super::compaction::{get_compression_algorithm, CompactionDeveloperConfig};
use crate::hummock::checkpoint::HummockVersionCheckpoint;
use crate::hummock::compaction::CompactStatus;
use crate::rpc::metrics::MetaMetrics;

pub struct LocalTableMetrics {
    total_key_count: IntGauge,
    total_key_size: IntGauge,
    total_value_size: IntGauge,
    write_throughput: GenericCounter<AtomicU64>,
    cal_count: usize,
    write_size: u64,
}

const MIN_FLUSH_COUNT: usize = 16;
const MIN_FLUSH_DATA_SIZE: u64 = 128 * 1024 * 1024;

impl LocalTableMetrics {
    pub fn inc_write_throughput(&mut self, val: u64) {
        self.write_size += val;
        self.cal_count += 1;
        if self.write_size > MIN_FLUSH_DATA_SIZE || self.cal_count > MIN_FLUSH_COUNT {
            self.write_throughput.inc_by(self.write_size / 1024 / 1024);
            self.write_size = 0;
            self.cal_count = 0;
        }
    }
}

pub fn get_or_create_local_table_stat<'a>(
    metrics: &MetaMetrics,
    table_id: u32,
    local_metrics: &'a mut HashMap<u32, LocalTableMetrics>,
) -> &'a mut LocalTableMetrics {
    local_metrics.entry(table_id).or_insert_with(|| {
        let table_label = format!("{}", table_id);
        LocalTableMetrics {
            total_key_count: metrics
                .version_stats
                .with_label_values(&[&table_label, "total_key_count"]),
            total_key_size: metrics
                .version_stats
                .with_label_values(&[&table_label, "total_key_size"]),
            total_value_size: metrics
                .version_stats
                .with_label_values(&[&table_label, "total_value_size"]),
            write_throughput: metrics
                .table_write_throughput
                .with_label_values(&[&table_label]),
            cal_count: 0,
            write_size: 0,
        }
    })
}

pub fn trigger_local_table_stat(
    metrics: &MetaMetrics,
    local_metrics: &mut HashMap<u32, LocalTableMetrics>,
    version_stats: &HummockVersionStats,
    table_stats_change: &PbTableStatsMap,
) {
    for (table_id, stats) in table_stats_change {
        if stats.total_key_size == 0 && stats.total_value_size == 0 && stats.total_key_count == 0 {
            continue;
        }
        let table_metrics = get_or_create_local_table_stat(metrics, *table_id, local_metrics);
        if let Some(table_stats) = version_stats.table_stats.get(table_id) {
            table_metrics
                .total_key_count
                .set(table_stats.total_key_count);
            table_metrics
                .total_value_size
                .set(table_stats.total_value_size);
            table_metrics.total_key_size.set(table_stats.total_key_size);
        }
    }
}

pub fn trigger_mv_stat(
    metrics: &MetaMetrics,
    version_stats: &HummockVersionStats,
    mv_id_to_all_table_ids: Vec<(u32, Vec<u32>)>,
) {
    metrics.materialized_view_stats.reset();
    for (mv_id, all_table_ids) in mv_id_to_all_table_ids {
        let total_size = all_table_ids
            .iter()
            .filter_map(|&table_id| version_stats.table_stats.get(&table_id))
            .map(|stats| stats.total_key_size + stats.total_value_size)
            .sum();

        metrics
            .materialized_view_stats
            .with_label_values(&[&mv_id.to_string(), "materialized_view_total_size"])
            .set(total_size);
    }
}

pub fn trigger_sst_stat(
    metrics: &MetaMetrics,
    compact_status: Option<&CompactStatus>,
    current_version: &HummockVersion,
    compaction_group_id: CompactionGroupId,
) {
    let level_sst_cnt = |level_idx: usize| {
        let mut sst_num = 0;
        current_version.level_iter(compaction_group_id, |level| {
            if level.level_idx == level_idx as u32 {
                sst_num += level.table_infos.len();
            }
            true
        });
        sst_num
    };
    let level_sst_size = |level_idx: usize| {
        let mut level_sst_size = 0;
        current_version.level_iter(compaction_group_id, |level| {
            if level.level_idx == level_idx as u32 {
                level_sst_size += level.total_file_size;
            }
            true
        });
        level_sst_size / 1024
    };

    let mut compacting_task_stat: BTreeMap<(usize, usize), usize> = BTreeMap::default();
    for idx in 0..current_version.num_levels(compaction_group_id) {
        let sst_num = level_sst_cnt(idx);
        let level_label = build_level_metrics_label(compaction_group_id, idx);
        metrics
            .level_sst_num
            .with_label_values(&[&level_label])
            .set(sst_num as i64);
        metrics
            .level_file_size
            .with_label_values(&[&level_label])
            .set(level_sst_size(idx) as i64);
        if let Some(compact_status) = compact_status {
            let compact_cnt = compact_status.level_handlers[idx].pending_file_count();
            metrics
                .level_compact_cnt
                .with_label_values(&[&level_label])
                .set(compact_cnt as i64);

            let compacting_task = compact_status.level_handlers[idx].pending_tasks();
            let mut pending_task_ids: HashSet<u64> = HashSet::default();
            for task in compacting_task {
                if pending_task_ids.contains(&task.task_id) {
                    continue;
                }

                if idx != 0 && idx == task.target_level as usize {
                    continue;
                }

                let key = (idx, task.target_level as usize);
                let count = compacting_task_stat.entry(key).or_insert(0);
                *count += 1;

                pending_task_ids.insert(task.task_id);
            }
        }
    }

    tracing::debug!("LSM Compacting STAT {:?}", compacting_task_stat);
    for ((select, target), compacting_task_count) in &compacting_task_stat {
        let label_str =
            build_compact_task_stat_metrics_label(compaction_group_id, *select, *target);
        metrics
            .level_compact_task_cnt
            .with_label_values(&[&label_str])
            .set(*compacting_task_count as _);
    }

    if compacting_task_stat.is_empty() {
        if let Some(levels) = current_version.levels.get(&compaction_group_id) {
            let max_level = levels.levels.len();
            remove_compacting_task_stat(metrics, compaction_group_id, max_level);
        }
    }

    {
        // sub level stat
        let overlapping_level_label =
            build_compact_task_l0_stat_metrics_label(compaction_group_id, true, false);
        let non_overlap_level_label =
            build_compact_task_l0_stat_metrics_label(compaction_group_id, false, false);
        let partition_level_label =
            build_compact_task_l0_stat_metrics_label(compaction_group_id, true, true);

        let overlapping_sst_num = current_version
            .levels
            .get(&compaction_group_id)
            .map(|level| {
                level
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|sub_level| sub_level.level_type == LevelType::Overlapping)
                    .count()
            })
            .unwrap_or(0);

        let non_overlap_sst_num = current_version
            .levels
            .get(&compaction_group_id)
            .map(|level| {
                level
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|sub_level| sub_level.level_type == LevelType::Nonoverlapping)
                    .count()
            })
            .unwrap_or(0);

        let partition_level_num = current_version
            .levels
            .get(&compaction_group_id)
            .map(|level| {
                level
                    .l0
                    .sub_levels
                    .iter()
                    .filter(|sub_level| {
                        sub_level.level_type == LevelType::Nonoverlapping
                            && sub_level.vnode_partition_count > 0
                    })
                    .count()
            })
            .unwrap_or(0);
        metrics
            .level_sst_num
            .with_label_values(&[&overlapping_level_label])
            .set(overlapping_sst_num as i64);

        metrics
            .level_sst_num
            .with_label_values(&[&non_overlap_level_label])
            .set(non_overlap_sst_num as i64);

        metrics
            .level_sst_num
            .with_label_values(&[&partition_level_label])
            .set(partition_level_num as i64);
    }

    let previous_time = metrics.time_after_last_observation.load(Ordering::Relaxed);
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    if current_time > 600 + previous_time
        && metrics
            .time_after_last_observation
            .compare_exchange(
                previous_time,
                current_time,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_ok()
    {
        if let Some(compact_status) = compact_status {
            for (idx, level_handler) in enumerate(compact_status.level_handlers.iter()) {
                let sst_num = level_sst_cnt(idx);
                let sst_size = level_sst_size(idx);
                let compact_cnt = level_handler.pending_file_count();
                tracing::info!(
                    "Level {} has {} SSTs, the total size of which is {}KB, while {} of those are being compacted to bottom levels",
                    idx,
                    sst_num,
                    sst_size,
                    compact_cnt,
                );
            }
        }
    }
}

pub fn trigger_epoch_stat(metrics: &MetaMetrics, version: &HummockVersion) {
    metrics.max_committed_epoch.set(
        version
            .state_table_info
            .info()
            .values()
            .map(|i| i.committed_epoch)
            .max()
            .unwrap_or(0) as _,
    );
    metrics.min_committed_epoch.set(
        version
            .state_table_info
            .info()
            .values()
            .map(|i| i.committed_epoch)
            .min()
            .unwrap_or(0) as _,
    );
}

pub fn remove_compaction_group_in_sst_stat(
    metrics: &MetaMetrics,
    compaction_group_id: CompactionGroupId,
    max_level: usize,
) {
    let mut idx = 0;
    loop {
        let level_label = build_level_metrics_label(compaction_group_id, idx);
        let should_continue = metrics
            .level_sst_num
            .remove_label_values(&[&level_label])
            .is_ok();

        metrics
            .level_file_size
            .remove_label_values(&[&level_label])
            .ok();

        metrics
            .level_compact_cnt
            .remove_label_values(&[&level_label])
            .ok();
        if !should_continue {
            break;
        }
        idx += 1;
    }

    let overlapping_level_label = build_level_l0_metrics_label(compaction_group_id, true);
    let non_overlap_level_label = build_level_l0_metrics_label(compaction_group_id, false);
    metrics
        .level_sst_num
        .remove_label_values(&[&overlapping_level_label])
        .ok();
    metrics
        .level_sst_num
        .remove_label_values(&[&non_overlap_level_label])
        .ok();

    remove_compacting_task_stat(metrics, compaction_group_id, max_level);
    remove_split_stat(metrics, compaction_group_id);
    remove_compact_task_metrics(metrics, compaction_group_id, max_level);
    remove_compaction_group_stat(metrics, compaction_group_id);
}

pub fn remove_compacting_task_stat(
    metrics: &MetaMetrics,
    compaction_group_id: CompactionGroupId,
    max_level: usize,
) {
    for select_level in 0..=max_level {
        for target_level in 0..=max_level {
            let label_str = build_compact_task_stat_metrics_label(
                compaction_group_id,
                select_level,
                target_level,
            );
            metrics
                .level_compact_task_cnt
                .remove_label_values(&[&label_str])
                .ok();
        }
    }
}

pub fn remove_split_stat(metrics: &MetaMetrics, compaction_group_id: CompactionGroupId) {
    let label_str = compaction_group_id.to_string();
    metrics
        .state_table_count
        .remove_label_values(&[&label_str])
        .ok();

    metrics
        .branched_sst_count
        .remove_label_values(&[&label_str])
        .ok();
}

pub fn remove_compaction_group_stat(metrics: &MetaMetrics, compaction_group_id: CompactionGroupId) {
    let label_str = compaction_group_id.to_string();
    metrics
        .compaction_group_size
        .remove_label_values(&[&label_str])
        .ok();
    metrics
        .compaction_group_file_count
        .remove_label_values(&[&label_str])
        .ok();
    metrics
        .compaction_group_throughput
        .remove_label_values(&[&label_str])
        .ok();

    metrics
        .split_compaction_group_count
        .remove_label_values(&[&label_str])
        .ok();

    metrics
        .merge_compaction_group_count
        .remove_label_values(&[&label_str])
        .ok();
}

pub fn trigger_pin_unpin_version_state(
    metrics: &MetaMetrics,
    pinned_versions: &BTreeMap<HummockContextId, HummockPinnedVersion>,
) {
    if let Some(m) = pinned_versions.values().map(|v| v.min_pinned_id).min() {
        metrics.min_pinned_version_id.set(m as i64);
    } else {
        metrics
            .min_pinned_version_id
            .set(HummockVersionId::MAX.to_u64() as _);
    }
}

pub fn trigger_gc_stat(
    metrics: &MetaMetrics,
    checkpoint: &HummockVersionCheckpoint,
    min_pinned_version_id: HummockVersionId,
) {
    let current_version_object_size_map = object_size_map(&checkpoint.version);
    let current_version_object_size = current_version_object_size_map.values().sum::<u64>();
    let current_version_object_count = current_version_object_size_map.len();
    let mut old_version_object_size = 0;
    let mut old_version_object_count = 0;
    let mut stale_object_size = 0;
    let mut stale_object_count = 0;
    checkpoint.stale_objects.iter().for_each(|(id, objects)| {
        if *id <= min_pinned_version_id {
            stale_object_size += objects.total_file_size;
            stale_object_count += objects.id.len() as u64;
        } else {
            old_version_object_size += objects.total_file_size;
            old_version_object_count += objects.id.len() as u64;
        }
    });
    metrics
        .current_version_object_size
        .set(current_version_object_size as _);
    metrics
        .current_version_object_count
        .set(current_version_object_count as _);
    metrics
        .old_version_object_size
        .set(old_version_object_size as _);
    metrics
        .old_version_object_count
        .set(old_version_object_count as _);
    metrics.stale_object_size.set(stale_object_size as _);
    metrics.stale_object_count.set(stale_object_count as _);
}

// Triggers a report on compact_pending_bytes_needed
pub fn trigger_lsm_stat(
    metrics: &MetaMetrics,
    compaction_config: Arc<CompactionConfig>,
    levels: &Levels,
    compaction_group_id: CompactionGroupId,
) {
    let group_label = compaction_group_id.to_string();
    // compact_pending_bytes
    // we don't actually generate a compaction task here so developer config can be ignored.
    let dynamic_level_core = DynamicLevelSelectorCore::new(
        compaction_config.clone(),
        Arc::new(CompactionDeveloperConfig::default()),
    );
    let ctx = dynamic_level_core.calculate_level_base_size(levels);
    {
        let compact_pending_bytes_needed =
            dynamic_level_core.compact_pending_bytes_needed_with_ctx(levels, &ctx);

        metrics
            .compact_pending_bytes
            .with_label_values(&[&group_label])
            .set(compact_pending_bytes_needed as _);
    }

    {
        // compact_level_compression_ratio
        let level_compression_ratio = levels
            .levels
            .iter()
            .map(|level| {
                let ratio = if level.uncompressed_file_size == 0 {
                    0.0
                } else {
                    level.total_file_size as f64 / level.uncompressed_file_size as f64
                };

                (level.level_idx, ratio)
            })
            .collect_vec();

        for (level_index, compression_ratio) in level_compression_ratio {
            let compression_algorithm_label = get_compression_algorithm(
                compaction_config.as_ref(),
                ctx.base_level,
                level_index as usize,
            );

            metrics
                .compact_level_compression_ratio
                .with_label_values(&[
                    &group_label,
                    &level_index.to_string(),
                    &compression_algorithm_label,
                ])
                .set(compression_ratio);
        }
    }
}

pub fn trigger_write_stop_stats(
    metrics: &MetaMetrics,
    write_limit: &HashMap<CompactionGroupId, WriteLimit>,
) {
    metrics.write_stop_compaction_groups.reset();
    for cg in write_limit.keys() {
        metrics
            .write_stop_compaction_groups
            .with_label_values(&[&cg.to_string()])
            .set(1);
    }
}

pub fn trigger_split_stat(metrics: &MetaMetrics, version: &HummockVersion) {
    let branched_ssts = version.build_branched_sst_info();

    for compaction_group_id in version.levels.keys() {
        let group_label = compaction_group_id.to_string();
        metrics
            .state_table_count
            .with_label_values(&[&group_label])
            .set(
                version
                    .state_table_info
                    .compaction_group_member_table_ids(*compaction_group_id)
                    .len() as _,
            );

        let branched_sst_count: usize = branched_ssts
            .values()
            .map(|branched_map| {
                branched_map
                    .keys()
                    .filter(|group_id| *group_id == compaction_group_id)
                    .count()
            })
            .sum();

        metrics
            .branched_sst_count
            .with_label_values(&[&group_label])
            .set(branched_sst_count as _);
    }
}

pub fn build_level_metrics_label(compaction_group_id: u64, level_idx: usize) -> String {
    format!("cg{}_L{}", compaction_group_id, level_idx)
}

pub fn build_level_l0_metrics_label(compaction_group_id: u64, overlapping: bool) -> String {
    if overlapping {
        format!("cg{}_l0_sub_overlapping", compaction_group_id)
    } else {
        format!("cg{}_l0_sub_non_overlap", compaction_group_id)
    }
}

pub fn build_compact_task_stat_metrics_label(
    compaction_group_id: u64,
    select_level: usize,
    target_level: usize,
) -> String {
    format!(
        "cg{} L{} -> L{}",
        compaction_group_id, select_level, target_level
    )
}

pub fn build_compact_task_l0_stat_metrics_label(
    compaction_group_id: u64,
    overlapping: bool,
    partition: bool,
) -> String {
    if partition {
        format!("cg{}_l0_sub_partition", compaction_group_id)
    } else if overlapping {
        format!("cg{}_l0_sub_overlapping", compaction_group_id)
    } else {
        format!("cg{}_l0_sub_non_overlap", compaction_group_id)
    }
}

pub fn build_compact_task_level_type_metrics_label(
    select_level: usize,
    target_level: usize,
) -> String {
    format!("L{}->L{}", select_level, target_level)
}

pub fn remove_compact_task_metrics(
    metrics: &MetaMetrics,
    compaction_group_id: CompactionGroupId,
    max_level: usize,
) {
    for select_level in 0..=max_level {
        for target_level in 0..=max_level {
            let level_type_label =
                build_compact_task_level_type_metrics_label(select_level, target_level);
            let should_continue = metrics
                .l0_compact_level_count
                .remove_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .is_ok();

            metrics
                .compact_task_size
                .remove_label_values(&[&compaction_group_id.to_string(), &level_type_label])
                .ok();

            metrics
                .compact_task_size
                .remove_label_values(&[
                    &compaction_group_id.to_string(),
                    &format!("{} uncompressed", level_type_label),
                ])
                .ok();
            if !should_continue {
                break;
            }
        }
    }
}

pub fn trigger_compact_tasks_stat(
    metrics: &MetaMetrics,
    compact_tasks: &[CompactTask],
    compact_status: &BTreeMap<CompactionGroupId, CompactStatus>,
    current_version: &HummockVersion,
) {
    let mut task_status_label_map = HashMap::new();
    let mut task_type_label_map = HashMap::new();
    let mut group_label_map = HashMap::new();

    for task in compact_tasks {
        let task_status_label = task_status_label_map
            .entry(task.task_status)
            .or_insert_with(|| task.task_status.as_str_name().to_owned());

        let task_type_label = task_type_label_map
            .entry(task.task_type)
            .or_insert_with(|| task.task_type.as_str_name().to_owned());

        let group_label = group_label_map
            .entry(task.compaction_group_id)
            .or_insert_with(|| task.compaction_group_id.to_string());

        metrics
            .compact_frequency
            .with_label_values(&["normal", group_label, task_type_label, task_status_label])
            .inc();
    }

    group_label_map.keys().for_each(|group_id| {
        trigger_sst_stat(
            metrics,
            compact_status.get(group_id),
            current_version,
            *group_id,
        );
    });
}
