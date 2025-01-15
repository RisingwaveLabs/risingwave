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

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use futures::{stream, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::util::value_encoding::column_aware_row_encoding::try_drop_invalid_columns;
use risingwave_hummock_sdk::compact::{
    compact_task_to_string, estimate_memory_for_compact_task, statistics_compact_task,
};
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{FullKey, FullKeyTracker};
use risingwave_hummock_sdk::key_range::{KeyRange, KeyRangeCommon};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::{add_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::{
    can_concat, compact_task_output_to_string, full_key_can_concat, HummockSstableObjectId,
    KeyComparator,
};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::LevelType;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;

use super::compaction_utils::split_watermark_from_task;
use super::iterator::MonitoredCompactorIterator;
use super::task_progress::TaskProgress;
use super::{CompactionStatistics, TaskConfig};
use crate::compaction_catalog_manager::{CompactionCatalogAgentRef, CompactionCatalogManagerRef};
use crate::hummock::compactor::compaction_utils::{
    build_multi_compaction_filter, estimate_task_output_capacity, generate_splits_for_task,
    metrics_report_for_task, optimize_by_copy_block,
};
use crate::hummock::compactor::iterator::ConcatSstableIterator;
use crate::hummock::compactor::task_progress::TaskProgressGuard;
use crate::hummock::compactor::{
    await_tree_key, fast_compactor_runner, CompactOutput, CompactionFilter, Compactor,
    CompactorContext,
};
use crate::hummock::iterator::{
    Forward, HummockIterator, MergeIterator, NonPkPrefixSkipWatermarkIterator,
    SkipWatermarkIterator, ValueMeta,
};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    BlockedXor16FilterBuilder, CachePolicy, CompressionAlgorithm, GetObjectId, HummockResult,
    SstableBuilderOptions, SstableStoreRef,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};
pub struct CompactorRunner {
    compact_task: CompactTask,
    compactor: Compactor,
    sstable_store: SstableStoreRef,
    key_range: KeyRange,
    split_index: usize,
}

impl CompactorRunner {
    pub fn new(
        split_index: usize,
        context: CompactorContext,
        task: CompactTask,
        object_id_getter: Box<dyn GetObjectId>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        options.compression_algorithm = match task.compression_algorithm {
            0 => CompressionAlgorithm::None,
            1 => CompressionAlgorithm::Lz4,
            _ => CompressionAlgorithm::Zstd,
        };

        options.capacity = estimate_task_output_capacity(context.clone(), &task);
        let kv_count = task
            .input_ssts
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .map(|sst| sst.total_key_count)
            .sum::<u64>() as usize;
        let use_block_based_filter =
            BlockedXor16FilterBuilder::is_kv_count_too_large(kv_count) || task.target_level > 0;

        let key_range = KeyRange {
            left: task.splits[split_index].left.clone(),
            right: task.splits[split_index].right.clone(),
            right_exclusive: true,
        };

        let compactor = Compactor::new(
            context.clone(),
            options,
            TaskConfig {
                key_range: key_range.clone(),
                cache_policy: CachePolicy::NotFill,
                gc_delete_keys: task.gc_delete_keys,
                retain_multiple_version: false,
                stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
                task_type: task.task_type,
                use_block_based_filter,
                table_vnode_partition: task.table_vnode_partition.clone(),
                table_schemas: task
                    .table_schemas
                    .iter()
                    .map(|(k, v)| (*k, v.clone()))
                    .collect(),
                disable_drop_column_optimization: false,
            },
            object_id_getter,
        );

        Self {
            compactor,
            compact_task: task,
            sstable_store: context.sstable_store,
            key_range,
            split_index,
        }
    }

    pub async fn run(
        &self,
        compaction_filter: impl CompactionFilter,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        task_progress: Arc<TaskProgress>,
    ) -> HummockResult<CompactOutput> {
        let iter =
            self.build_sst_iter(task_progress.clone(), compaction_catalog_agent_ref.clone())?;
        let (ssts, compaction_stat) = self
            .compactor
            .compact_key_range(
                iter,
                compaction_filter,
                compaction_catalog_agent_ref,
                Some(task_progress),
                Some(self.compact_task.task_id),
                Some(self.split_index),
            )
            .await?;
        Ok((self.split_index, ssts, compaction_stat))
    }

    /// Build the merge iterator based on the given input ssts.
    fn build_sst_iter(
        &self,
        task_progress: Arc<TaskProgress>,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    ) -> HummockResult<impl HummockIterator<Direction = Forward>> {
        let compactor_iter_max_io_retry_times = self
            .compactor
            .context
            .storage_opts
            .compactor_iter_max_io_retry_times;
        let mut table_iters = Vec::new();
        for level in &self.compact_task.input_ssts {
            if level.table_infos.is_empty() {
                continue;
            }

            let tables = level
                .table_infos
                .iter()
                .filter(|table_info| {
                    let table_ids = &table_info.table_ids;
                    let exist_table = table_ids
                        .iter()
                        .any(|table_id| self.compact_task.existing_table_ids.contains(table_id));

                    self.key_range.full_key_overlap(&table_info.key_range) && exist_table
                })
                .cloned()
                .collect_vec();
            // Do not need to filter the table because manager has done it.
            if level.level_type == LevelType::Nonoverlapping {
                debug_assert!(can_concat(&level.table_infos));
                table_iters.push(ConcatSstableIterator::new(
                    self.compact_task.existing_table_ids.clone(),
                    tables,
                    self.compactor.task_config.key_range.clone(),
                    self.sstable_store.clone(),
                    task_progress.clone(),
                    compactor_iter_max_io_retry_times,
                ));
            } else if tables.len()
                > self
                    .compactor
                    .context
                    .storage_opts
                    .compactor_max_overlap_sst_count
            {
                let sst_groups = partition_overlapping_sstable_infos(tables);
                tracing::warn!(
                    "COMPACT A LARGE OVERLAPPING LEVEL: try to partition {} ssts with {} groups",
                    level.table_infos.len(),
                    sst_groups.len()
                );
                for (idx, table_infos) in sst_groups.into_iter().enumerate() {
                    // Overlapping sstables may contains ssts with same user key (generated by spilled), so we need to check concat with full key.
                    assert!(
                        full_key_can_concat(&table_infos),
                        "sst_group idx {:?} table_infos: {:?}",
                        idx,
                        table_infos
                    );
                    table_iters.push(ConcatSstableIterator::new(
                        self.compact_task.existing_table_ids.clone(),
                        table_infos,
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                        task_progress.clone(),
                        compactor_iter_max_io_retry_times,
                    ));
                }
            } else {
                for table_info in tables {
                    table_iters.push(ConcatSstableIterator::new(
                        self.compact_task.existing_table_ids.clone(),
                        vec![table_info],
                        self.compactor.task_config.key_range.clone(),
                        self.sstable_store.clone(),
                        task_progress.clone(),
                        compactor_iter_max_io_retry_times,
                    ));
                }
            }
        }

        // The `SkipWatermarkIterator` is used to handle the table watermark state cleaning introduced
        // in https://github.com/risingwavelabs/risingwave/issues/13148
        let combine_iter = {
            let (pk_watermarks, non_pk_prefix_watermarks) =
                split_watermark_from_task(&self.compact_task);

            let skip_watermark_iter = SkipWatermarkIterator::from_safe_epoch_watermarks(
                MonitoredCompactorIterator::new(
                    MergeIterator::for_compactor(table_iters),
                    task_progress.clone(),
                ),
                pk_watermarks,
            );

            NonPkPrefixSkipWatermarkIterator::from_safe_epoch_watermarks(
                skip_watermark_iter,
                non_pk_prefix_watermarks,
                compaction_catalog_agent_ref,
            )
        };

        Ok(combine_iter)
    }
}

pub fn partition_overlapping_sstable_infos(
    mut origin_infos: Vec<SstableInfo>,
) -> Vec<Vec<SstableInfo>> {
    pub struct SstableGroup {
        ssts: Vec<SstableInfo>,
        max_right_bound: Bytes,
    }

    impl PartialEq for SstableGroup {
        fn eq(&self, other: &SstableGroup) -> bool {
            self.max_right_bound == other.max_right_bound
        }
    }
    impl PartialOrd for SstableGroup {
        fn partial_cmp(&self, other: &SstableGroup) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }
    impl Eq for SstableGroup {}
    impl Ord for SstableGroup {
        fn cmp(&self, other: &SstableGroup) -> std::cmp::Ordering {
            // Pick group with the smallest right bound for every new sstable.
            KeyComparator::compare_encoded_full_key(&other.max_right_bound, &self.max_right_bound)
        }
    }
    let mut groups: BinaryHeap<SstableGroup> = BinaryHeap::default();
    origin_infos.sort_by(|a, b| {
        let x = &a.key_range;
        let y = &b.key_range;
        KeyComparator::compare_encoded_full_key(&x.left, &y.left)
    });
    for sst in origin_infos {
        // Pick group with the smallest right bound for every new sstable. So do not check the larger one if the smallest one does not meet condition.
        if let Some(mut prev_group) = groups.peek_mut() {
            if KeyComparator::encoded_full_key_less_than(
                &prev_group.max_right_bound,
                &sst.key_range.left,
            ) {
                prev_group.max_right_bound.clone_from(&sst.key_range.right);
                prev_group.ssts.push(sst);
                continue;
            }
        }
        groups.push(SstableGroup {
            max_right_bound: sst.key_range.right.clone(),
            ssts: vec![sst],
        });
    }
    assert!(!groups.is_empty());
    groups.into_iter().map(|group| group.ssts).collect_vec()
}

/// Handles a compaction task and reports its status to hummock manager.
/// Always return `Ok` and let hummock manager handle errors.
pub async fn compact_with_agent(
    compactor_context: CompactorContext,
    mut compact_task: CompactTask,
    mut shutdown_rx: Receiver<()>,
    object_id_getter: Box<dyn GetObjectId>,
    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
) -> (
    (
        CompactTask,
        HashMap<u32, TableStats>,
        HashMap<HummockSstableObjectId, u64>,
    ),
    Option<MemoryTracker>,
) {
    let context = compactor_context.clone();
    let group_label = compact_task.compaction_group_id.to_string();
    metrics_report_for_task(&compact_task, &context);

    let timer = context
        .compactor_metrics
        .compact_task_duration
        .with_label_values(&[
            &group_label,
            &compact_task.input_ssts[0].level_idx.to_string(),
        ])
        .start_timer();

    let multi_filter = build_multi_compaction_filter(&compact_task);
    let mut task_status = TaskStatus::Success;
    let optimize_by_copy_block = optimize_by_copy_block(&compact_task, &context);

    if let Err(e) =
        generate_splits_for_task(&mut compact_task, &context, optimize_by_copy_block).await
    {
        tracing::warn!(error = %e.as_report(), "Failed to generate_splits");
        task_status = TaskStatus::ExecuteFailed;
        return (
            compact_done(compact_task, context.clone(), vec![], task_status),
            None,
        );
    }

    let compact_task_statistics = statistics_compact_task(&compact_task);
    // Number of splits (key ranges) is equal to number of compaction tasks
    let parallelism = compact_task.splits.len();
    assert_ne!(parallelism, 0, "splits cannot be empty");
    let mut output_ssts = Vec::with_capacity(parallelism);
    let mut compaction_futures = vec![];
    let mut abort_handles = vec![];
    let task_progress_guard =
        TaskProgressGuard::new(compact_task.task_id, context.task_progress_manager.clone());

    let capacity = estimate_task_output_capacity(context.clone(), &compact_task);

    let task_memory_capacity_with_parallelism = estimate_memory_for_compact_task(
        &compact_task,
        (context.storage_opts.block_size_kb as u64) * (1 << 10),
        context
            .storage_opts
            .object_store_config
            .s3
            .recv_buffer_size
            .unwrap_or(6 * 1024 * 1024) as u64,
        capacity as u64,
    ) * compact_task.splits.len() as u64;

    tracing::info!(
        "Ready to handle task: {} compact_task_statistics {:?} compression_algorithm {:?}  parallelism {} task_memory_capacity_with_parallelism {}, enable fast runner: {}, {}",
            compact_task.task_id,
            compact_task_statistics,
            compact_task.compression_algorithm,
            parallelism,
            task_memory_capacity_with_parallelism,
            optimize_by_copy_block,
            compact_task_to_string(&compact_task),
    );

    // If the task does not have enough memory, it should cancel the task and let the meta
    // reschedule it, so that it does not occupy the compactor's resources.
    let memory_detector = context
        .memory_limiter
        .try_require_memory(task_memory_capacity_with_parallelism);
    if memory_detector.is_none() {
        tracing::warn!(
                "Not enough memory to serve the task {} task_memory_capacity_with_parallelism {}  memory_usage {} memory_quota {}",
                compact_task.task_id,
                task_memory_capacity_with_parallelism,
                context.memory_limiter.get_memory_usage(),
                context.memory_limiter.quota()
            );
        task_status = TaskStatus::NoAvailMemoryResourceCanceled;
        return (
            compact_done(compact_task, context.clone(), output_ssts, task_status),
            memory_detector,
        );
    }

    context.compactor_metrics.compact_task_pending_num.inc();
    context
        .compactor_metrics
        .compact_task_pending_parallelism
        .add(parallelism as _);
    let _release_metrics_guard =
        scopeguard::guard((parallelism, context.clone()), |(parallelism, context)| {
            context.compactor_metrics.compact_task_pending_num.dec();
            context
                .compactor_metrics
                .compact_task_pending_parallelism
                .sub(parallelism as _);
        });

    if optimize_by_copy_block {
        let runner = fast_compactor_runner::CompactorRunner::new(
            context.clone(),
            compact_task.clone(),
            compaction_catalog_agent_ref.clone(),
            object_id_getter.clone(),
            task_progress_guard.progress.clone(),
        );

        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::warn!("Compaction task cancelled externally:\n{}", compact_task_to_string(&compact_task));
                task_status = TaskStatus::ManualCanceled;
            },

            ret = runner.run() => {
                match ret {
                    Ok((ssts, statistics)) => {
                        output_ssts.push((0, ssts, statistics));
                    }
                    Err(e) => {
                        task_status = TaskStatus::ExecuteFailed;
                        tracing::warn!(
                            error = %e.as_report(),
                            "Compaction task {} failed with error",
                            compact_task.task_id,
                        );
                    }
                }
            }
        }

        // After a compaction is done, mutate the compaction task.
        let (compact_task, table_stats, object_timestamps) =
            compact_done(compact_task, context.clone(), output_ssts, task_status);
        let cost_time = timer.stop_and_record() * 1000.0;
        tracing::info!(
            "Finished fast compaction task in {:?}ms: {}",
            cost_time,
            compact_task_to_string(&compact_task)
        );
        return (
            (compact_task, table_stats, object_timestamps),
            memory_detector,
        );
    }
    for (split_index, _) in compact_task.splits.iter().enumerate() {
        let filter = multi_filter.clone();
        let compaction_catalog_agent_ref = compaction_catalog_agent_ref.clone();
        let compactor_runner = CompactorRunner::new(
            split_index,
            compactor_context.clone(),
            compact_task.clone(),
            object_id_getter.clone(),
        );
        let task_progress = task_progress_guard.progress.clone();
        let runner = async move {
            compactor_runner
                .run(filter, compaction_catalog_agent_ref, task_progress)
                .await
        };
        let traced = match context.await_tree_reg.as_ref() {
            None => runner.right_future(),
            Some(await_tree_reg) => await_tree_reg
                .register(
                    await_tree_key::CompactRunner {
                        task_id: compact_task.task_id,
                        split_index,
                    },
                    format!(
                        "Compaction Task {} Split {} ",
                        compact_task.task_id, split_index
                    ),
                )
                .instrument(runner)
                .left_future(),
        };
        let handle = tokio::spawn(traced);
        abort_handles.push(handle.abort_handle());
        compaction_futures.push(handle);
    }

    let mut buffered = stream::iter(compaction_futures).buffer_unordered(parallelism);
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                tracing::warn!("Compaction task cancelled externally:\n{}", compact_task_to_string(&compact_task));
                task_status = TaskStatus::ManualCanceled;
                break;
            }
            future_result = buffered.next() => {
                match future_result {
                    Some(Ok(Ok((split_index, ssts, compact_stat)))) => {
                        output_ssts.push((split_index, ssts, compact_stat));
                    }
                    Some(Ok(Err(e))) => {
                        task_status = TaskStatus::ExecuteFailed;
                        tracing::warn!(
                            error = %e.as_report(),
                            "Compaction task {} failed with error",
                            compact_task.task_id,
                        );
                        break;
                    }
                    Some(Err(e)) => {
                        task_status = TaskStatus::JoinHandleFailed;
                        tracing::warn!(
                            error = %e.as_report(),
                            "Compaction task {} failed with join handle error",
                            compact_task.task_id,
                        );
                        break;
                    }
                    None => break,
                }
            }
        }
    }

    if task_status != TaskStatus::Success {
        for abort_handle in abort_handles {
            abort_handle.abort();
        }
        output_ssts.clear();
    }
    // Sort by split/key range index.
    if !output_ssts.is_empty() {
        output_ssts.sort_by_key(|(split_index, ..)| *split_index);
    }

    // After a compaction is done, mutate the compaction task.
    let (compact_task, table_stats, object_timestamps) =
        compact_done(compact_task, context.clone(), output_ssts, task_status);
    let cost_time = timer.stop_and_record() * 1000.0;
    tracing::info!(
        "Finished compaction task in {:?}ms: {}",
        cost_time,
        compact_task_output_to_string(&compact_task)
    );
    (
        (compact_task, table_stats, object_timestamps),
        memory_detector,
    )
}

/// Handles a compaction task and reports its status to hummock manager.
/// Always return `Ok` and let hummock manager handle errors.
pub async fn compact(
    compactor_context: CompactorContext,
    compact_task: CompactTask,
    shutdown_rx: Receiver<()>,
    object_id_getter: Box<dyn GetObjectId>,
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
) -> (
    (
        CompactTask,
        HashMap<u32, TableStats>,
        HashMap<HummockSstableObjectId, u64>,
    ),
    Option<MemoryTracker>,
) {
    let compact_table_ids = compact_task.build_compact_table_ids();
    let compaction_catalog_agent_ref = match compaction_catalog_manager_ref
        .acquire(compact_table_ids.clone())
        .await
    {
        Ok(compaction_catalog_agent_ref) => {
            let acquire_table_ids: HashSet<StateTableId> =
                compaction_catalog_agent_ref.table_ids().collect();
            if acquire_table_ids.len() != compact_table_ids.len() {
                let diff = compact_table_ids
                    .into_iter()
                    .collect::<HashSet<_>>()
                    .symmetric_difference(&acquire_table_ids)
                    .cloned()
                    .collect::<Vec<_>>();
                tracing::warn!(
                    dif= ?diff,
                    "Some table ids are not acquired."
                );
                return (
                    compact_done(
                        compact_task,
                        compactor_context.clone(),
                        vec![],
                        TaskStatus::ExecuteFailed,
                    ),
                    None,
                );
            }

            compaction_catalog_agent_ref
        }
        Err(e) => {
            tracing::warn!(
                error = %e.as_report(),
                "Failed to acquire compaction catalog agent"
            );
            return (
                compact_done(
                    compact_task,
                    compactor_context.clone(),
                    vec![],
                    TaskStatus::ExecuteFailed,
                ),
                None,
            );
        }
    };

    compact_with_agent(
        compactor_context,
        compact_task,
        shutdown_rx,
        object_id_getter,
        compaction_catalog_agent_ref,
    )
    .await
}

/// Fills in the compact task and tries to report the task result to meta node.
pub(crate) fn compact_done(
    mut compact_task: CompactTask,
    context: CompactorContext,
    output_ssts: Vec<CompactOutput>,
    task_status: TaskStatus,
) -> (
    CompactTask,
    HashMap<u32, TableStats>,
    HashMap<HummockSstableObjectId, u64>,
) {
    let mut table_stats_map = TableStatsMap::default();
    let mut object_timestamps = HashMap::default();
    compact_task.task_status = task_status;
    compact_task
        .sorted_output_ssts
        .reserve(compact_task.splits.len());
    let mut compaction_write_bytes = 0;
    for (
        _,
        ssts,
        CompactionStatistics {
            delta_drop_stat, ..
        },
    ) in output_ssts
    {
        add_table_stats_map(&mut table_stats_map, &delta_drop_stat);
        for sst_info in ssts {
            compaction_write_bytes += sst_info.file_size();
            object_timestamps.insert(sst_info.sst_info.object_id, sst_info.created_at);
            compact_task.sorted_output_ssts.push(sst_info.sst_info);
        }
    }

    let group_label = compact_task.compaction_group_id.to_string();
    let level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_write_bytes
        .with_label_values(&[&group_label, level_label.as_str()])
        .inc_by(compaction_write_bytes);
    context
        .compactor_metrics
        .compact_write_sstn
        .with_label_values(&[&group_label, level_label.as_str()])
        .inc_by(compact_task.sorted_output_ssts.len() as u64);

    (compact_task, table_stats_map, object_timestamps)
}

pub async fn compact_and_build_sst<F>(
    sst_builder: &mut CapacitySplitTableBuilder<F>,
    task_config: &TaskConfig,
    compactor_metrics: Arc<CompactorMetrics>,
    mut iter: impl HummockIterator<Direction = Forward>,
    mut compaction_filter: impl CompactionFilter,
) -> HummockResult<CompactionStatistics>
where
    F: TableBuilderFactory,
{
    if !task_config.key_range.left.is_empty() {
        let full_key = FullKey::decode(&task_config.key_range.left);
        iter.seek(full_key)
            .verbose_instrument_await("iter_seek")
            .await?;
    } else {
        iter.rewind().verbose_instrument_await("rewind").await?;
    };

    let end_key = if task_config.key_range.right.is_empty() {
        FullKey::default()
    } else {
        FullKey::decode(&task_config.key_range.right).to_vec()
    };
    let max_key = end_key.to_ref();

    let mut full_key_tracker = FullKeyTracker::<Vec<u8>>::new(FullKey::default());
    let mut local_stats = StoreLocalStatistic::default();

    // Keep table stats changes due to dropping KV.
    let mut table_stats_drop = TableStatsMap::default();
    let mut last_table_stats = TableStats::default();
    let mut last_table_id = None;
    let mut compaction_statistics = CompactionStatistics::default();
    // object id -> block id. For an object id, block id is updated in a monotonically increasing manner.
    let mut skip_schema_check: HashMap<HummockSstableObjectId, u64> = HashMap::default();
    let schemas: HashMap<u32, HashSet<i32>> = task_config
        .table_schemas
        .iter()
        .map(|(table_id, schema)| (*table_id, schema.column_ids.iter().copied().collect()))
        .collect();
    while iter.is_valid() {
        let iter_key = iter.key();
        compaction_statistics.iter_total_key_counts += 1;

        let is_new_user_key = full_key_tracker.observe(iter.key());
        let mut drop = false;

        // CRITICAL WARN: Because of memtable spill, there may be several versions of the same user-key share the same `pure_epoch`. Do not change this code unless necessary.
        let value = iter.value();
        let ValueMeta {
            object_id,
            block_id,
        } = iter.value_meta();
        if is_new_user_key {
            if !max_key.is_empty() && iter_key >= max_key {
                break;
            }
            if value.is_delete() {
                local_stats.skip_delete_key_count += 1;
            }
        } else {
            local_stats.skip_multi_version_key_count += 1;
        }

        if last_table_id.map_or(true, |last_table_id| {
            last_table_id != iter_key.user_key.table_id.table_id
        }) {
            if let Some(last_table_id) = last_table_id.take() {
                table_stats_drop.insert(last_table_id, std::mem::take(&mut last_table_stats));
            }
            last_table_id = Some(iter_key.user_key.table_id.table_id);
        }

        // Among keys with same user key, only keep the latest key unless retain_multiple_version is true.
        // In our design, frontend avoid to access keys which had be deleted, so we don't
        // need to consider the epoch when the compaction_filter match (it
        // means that mv had drop)
        // Because of memtable spill, there may be a PUT key share the same `pure_epoch` with DELETE key.
        // Do not assume that "the epoch of keys behind must be smaller than the current key."
        if (!task_config.retain_multiple_version && task_config.gc_delete_keys && value.is_delete())
            || (!task_config.retain_multiple_version && !is_new_user_key)
        {
            drop = true;
        }

        if !drop && compaction_filter.should_delete(iter_key) {
            drop = true;
        }

        if drop {
            compaction_statistics.iter_drop_key_counts += 1;

            let should_count = match task_config.stats_target_table_ids.as_ref() {
                Some(target_table_ids) => {
                    target_table_ids.contains(&iter_key.user_key.table_id.table_id)
                }
                None => true,
            };
            if should_count {
                last_table_stats.total_key_count -= 1;
                last_table_stats.total_key_size -= iter_key.encoded_len() as i64;
                last_table_stats.total_value_size -= iter.value().encoded_len() as i64;
            }
            iter.next()
                .verbose_instrument_await("iter_next_in_drop")
                .await?;
            continue;
        }

        // May drop stale columns
        let check_table_id = iter_key.user_key.table_id.table_id;
        let mut is_value_rewritten = false;
        if let HummockValue::Put(v) = value
            && let Some(object_id) = object_id
            && let Some(block_id) = block_id
            && !skip_schema_check
                .get(&object_id)
                .map(|prev_block_id| {
                    assert!(*prev_block_id <= block_id);
                    *prev_block_id == block_id
                })
                .unwrap_or(false)
            && let Some(schema) = schemas.get(&check_table_id)
        {
            let value_size = v.len();
            match try_drop_invalid_columns(v, schema) {
                None => {
                    if !task_config.disable_drop_column_optimization {
                        // Under the assumption that all values in the same (object, block) group should share the same schema,
                        // if one value drops no columns during a compaction, no need to check other values in the same group.
                        skip_schema_check.insert(object_id, block_id);
                    }
                }
                Some(new_value) => {
                    is_value_rewritten = true;
                    let new_put = HummockValue::put(new_value.as_slice());
                    sst_builder
                        .add_full_key(iter_key, new_put, is_new_user_key)
                        .verbose_instrument_await("add_rewritten_full_key")
                        .await?;
                    let value_size_change = value_size as i64 - new_value.len() as i64;
                    assert!(value_size_change >= 0);
                    last_table_stats.total_value_size -= value_size_change;
                }
            }
        }

        if !is_value_rewritten {
            // Don't allow two SSTs to share same user key
            sst_builder
                .add_full_key(iter_key, value, is_new_user_key)
                .verbose_instrument_await("add_full_key")
                .await?;
        }

        iter.next().verbose_instrument_await("iter_next").await?;
    }

    if let Some(last_table_id) = last_table_id.take() {
        table_stats_drop.insert(last_table_id, std::mem::take(&mut last_table_stats));
    }
    iter.collect_local_statistic(&mut local_stats);
    add_table_stats_map(
        &mut table_stats_drop,
        &local_stats.skipped_by_watermark_table_stats,
    );
    local_stats.report_compactor(compactor_metrics.as_ref());
    compaction_statistics.delta_drop_stat = table_stats_drop;

    Ok(compaction_statistics)
}

#[cfg(test)]
pub mod tests {
    use risingwave_hummock_sdk::can_concat;

    use crate::hummock::compactor::compactor_runner::partition_overlapping_sstable_infos;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_info, test_key_of, test_value_of,
    };
    use crate::hummock::value::HummockValue;

    #[tokio::test]
    async fn test_partition_overlapping_level() {
        const TEST_KEYS_COUNT: usize = 10;
        let sstable_store = mock_sstable_store().await;
        let mut table_infos = vec![];
        for object_id in 0..10 {
            let start_index = object_id * TEST_KEYS_COUNT;
            let end_index = start_index + 2 * TEST_KEYS_COUNT;
            let table_info = gen_test_sstable_info(
                default_builder_opt_for_test(),
                object_id as u64,
                (start_index..end_index)
                    .map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
                sstable_store.clone(),
            )
            .await;
            table_infos.push(table_info);
        }
        let table_infos = partition_overlapping_sstable_infos(table_infos);
        assert_eq!(table_infos.len(), 2);
        for ssts in table_infos {
            assert!(can_concat(&ssts));
        }
    }
}
