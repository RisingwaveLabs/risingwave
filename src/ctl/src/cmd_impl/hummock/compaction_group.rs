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

use std::collections::{HashMap, HashSet};

use comfy_table::{Row, Table};
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::{CompactionGroupId, HummockContextId};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::CompressionAlgorithm;

use crate::CtlContext;

pub async fn list_compaction_group(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let result = meta_client.risectl_list_compaction_group().await?;
    println!("{:#?}", result);
    Ok(())
}

pub async fn update_compaction_config(
    context: &CtlContext,
    ids: Vec<CompactionGroupId>,
    configs: Vec<MutableConfig>,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client
        .risectl_update_compaction_config(ids.as_slice(), configs.as_slice())
        .await?;
    println!(
        "Succeed: update compaction groups {:#?} with configs {:#?}.",
        ids, configs
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn build_compaction_config_vec(
    max_bytes_for_level_base: Option<u64>,
    max_bytes_for_level_multiplier: Option<u64>,
    max_compaction_bytes: Option<u64>,
    sub_level_max_compaction_bytes: Option<u64>,
    level0_tier_compact_file_number: Option<u64>,
    target_file_size_base: Option<u64>,
    compaction_filter_mask: Option<u32>,
    max_sub_compaction: Option<u32>,
    level0_stop_write_threshold_sub_level_number: Option<u64>,
    level0_sub_level_compact_level_count: Option<u32>,
    max_space_reclaim_bytes: Option<u64>,
    level0_max_compact_file_number: Option<u64>,
    level0_overlapping_sub_level_compact_level_count: Option<u32>,
    enable_emergency_picker: Option<bool>,
    tombstone_reclaim_ratio: Option<u32>,
    compress_algorithm: Option<CompressionAlgorithm>,
    max_l0_compact_level: Option<u32>,
    sst_allowed_trivial_move_min_size: Option<u64>,
    disable_auto_group_scheduling: Option<bool>,
    max_overlapping_level_size: Option<u64>,
    sst_allowed_trivial_move_max_count: Option<u32>,
) -> Vec<MutableConfig> {
    let mut configs = vec![];
    if let Some(c) = max_bytes_for_level_base {
        configs.push(MutableConfig::MaxBytesForLevelBase(c));
    }
    if let Some(c) = max_bytes_for_level_multiplier {
        configs.push(MutableConfig::MaxBytesForLevelMultiplier(c));
    }
    if let Some(c) = max_compaction_bytes {
        configs.push(MutableConfig::MaxCompactionBytes(c));
    }
    if let Some(c) = sub_level_max_compaction_bytes {
        configs.push(MutableConfig::SubLevelMaxCompactionBytes(c));
    }
    if let Some(c) = level0_tier_compact_file_number {
        configs.push(MutableConfig::Level0TierCompactFileNumber(c));
    }
    if let Some(c) = target_file_size_base {
        configs.push(MutableConfig::TargetFileSizeBase(c));
    }
    if let Some(c) = compaction_filter_mask {
        configs.push(MutableConfig::CompactionFilterMask(c));
    }
    if let Some(c) = max_sub_compaction {
        configs.push(MutableConfig::MaxSubCompaction(c));
    }
    if let Some(c) = level0_stop_write_threshold_sub_level_number {
        configs.push(MutableConfig::Level0StopWriteThresholdSubLevelNumber(c));
    }
    if let Some(c) = level0_sub_level_compact_level_count {
        configs.push(MutableConfig::Level0SubLevelCompactLevelCount(c));
    }
    if let Some(c) = max_space_reclaim_bytes {
        configs.push(MutableConfig::MaxSpaceReclaimBytes(c))
    }
    if let Some(c) = level0_max_compact_file_number {
        configs.push(MutableConfig::Level0MaxCompactFileNumber(c))
    }
    if let Some(c) = level0_overlapping_sub_level_compact_level_count {
        configs.push(MutableConfig::Level0OverlappingSubLevelCompactLevelCount(c))
    }
    if let Some(c) = enable_emergency_picker {
        configs.push(MutableConfig::EnableEmergencyPicker(c))
    }
    if let Some(c) = tombstone_reclaim_ratio {
        configs.push(MutableConfig::TombstoneReclaimRatio(c))
    }
    if let Some(c) = compress_algorithm {
        configs.push(MutableConfig::CompressionAlgorithm(c))
    }
    if let Some(c) = max_l0_compact_level {
        configs.push(MutableConfig::MaxL0CompactLevelCount(c))
    }
    if let Some(c) = sst_allowed_trivial_move_min_size {
        configs.push(MutableConfig::SstAllowedTrivialMoveMinSize(c))
    }
    if let Some(c) = disable_auto_group_scheduling {
        configs.push(MutableConfig::DisableAutoGroupScheduling(c))
    }
    if let Some(c) = max_overlapping_level_size {
        configs.push(MutableConfig::MaxOverlappingLevelSize(c))
    }
    if let Some(c) = sst_allowed_trivial_move_max_count {
        configs.push(MutableConfig::SstAllowedTrivialMoveMaxCount(c))
    }

    configs
}

pub async fn split_compaction_group(
    context: &CtlContext,
    group_id: CompactionGroupId,
    table_ids_to_new_group: &[StateTableId],
    partition_vnode_count: u32,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let new_group_id = meta_client
        .split_compaction_group(group_id, table_ids_to_new_group, partition_vnode_count)
        .await?;
    println!(
        "Succeed: split compaction group {}. tables {:#?} are moved to new group {}.",
        group_id, table_ids_to_new_group, new_group_id
    );
    Ok(())
}

pub async fn list_compaction_status(context: &CtlContext, verbose: bool) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let (status, assignment, progress) = meta_client.risectl_list_compaction_status().await?;
    if !verbose {
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("Compaction Group".into());
            row.add_cell("Level".into());
            row.add_cell("Task Count".into());
            row.add_cell("Tasks".into());
            row
        });
        for s in status {
            let cg_id = s.compaction_group_id;
            for l in s.level_handlers {
                let level = l.level;
                let mut task_ids = HashSet::new();
                for t in l.tasks {
                    task_ids.insert(t.task_id);
                }
                let mut row = Row::new();
                row.add_cell(cg_id.into());
                row.add_cell(level.into());
                row.add_cell(task_ids.len().into());
                row.add_cell(
                    task_ids
                        .into_iter()
                        .sorted()
                        .map(|t| t.to_string())
                        .join(",")
                        .into(),
                );
                table.add_row(row);
            }
        }
        println!("{table}");

        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("Hummock Context".into());
            row.add_cell("Task Count".into());
            row.add_cell("Tasks".into());
            row
        });
        let mut assignment_lite: HashMap<HummockContextId, Vec<u64>> = HashMap::new();
        for a in assignment {
            assignment_lite
                .entry(a.context_id)
                .or_default()
                .push(a.compact_task.unwrap().task_id);
        }
        for (k, v) in assignment_lite {
            let mut row = Row::new();
            row.add_cell(k.into());
            row.add_cell(v.len().into());
            row.add_cell(
                v.into_iter()
                    .sorted()
                    .map(|t| t.to_string())
                    .join(",")
                    .into(),
            );
            table.add_row(row);
        }
        println!("{table}");

        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("Task".into());
            row.add_cell("Num SSTs Sealed".into());
            row.add_cell("Num SSTs Uploaded".into());
            row.add_cell("Num Progress Key".into());
            row.add_cell("Num Pending Read IO".into());
            row.add_cell("Num Pending Write IO".into());
            row
        });
        for p in progress {
            let mut row = Row::new();
            row.add_cell(p.task_id.into());
            row.add_cell(p.num_ssts_sealed.into());
            row.add_cell(p.num_ssts_uploaded.into());
            row.add_cell(p.num_progress_key.into());
            row.add_cell(p.num_pending_read_io.into());
            row.add_cell(p.num_pending_write_io.into());
            table.add_row(row);
        }
        println!("{table}");
    } else {
        println!("--- LSMtree Status ---");
        println!("{:#?}", status);
        println!("--- Task Assignment ---");
        println!("{:#?}", assignment);
        println!("--- Task Progress ---");
        println!("{:#?}", progress);
    }
    Ok(())
}

pub async fn get_compaction_score(
    context: &CtlContext,
    id: CompactionGroupId,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let scores = meta_client.get_compaction_score(id).await?;
    let mut table = Table::new();
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Select Level".into());
        row.add_cell("Target Level".into());
        row.add_cell("Type".into());
        row.add_cell("Score".into());
        row
    });
    for s in scores.into_iter().sorted_by(|a, b| {
        a.select_level
            .cmp(&b.select_level)
            .then_with(|| a.target_level.cmp(&b.target_level))
    }) {
        let mut row = Row::new();
        row.add_cell(s.select_level.into());
        row.add_cell(s.target_level.into());
        row.add_cell(s.picker_type.into());
        row.add_cell(s.score.into());
        table.add_row(row);
    }
    println!("{table}");
    Ok(())
}

pub async fn cancel_compact_task(context: &CtlContext, task_id: u64) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let ret = meta_client
        .cancel_compact_task(task_id, TaskStatus::ManualCanceled)
        .await?;
    println!("cancel_compact_task {} ret {:?}", task_id, ret);

    Ok(())
}

pub async fn merge_compaction_group(
    context: &CtlContext,
    left_group_id: CompactionGroupId,
    right_group_id: CompactionGroupId,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client
        .merge_compaction_group(left_group_id, right_group_id)
        .await?;
    Ok(())
}
