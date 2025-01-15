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
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::table_watermark::{TableWatermarks, WatermarkSerdeType};
use risingwave_hummock_sdk::{can_concat, EpochWithGap, KeyComparator};
use risingwave_pb::hummock::compact_task::PbTaskType;
use risingwave_pb::hummock::{BloomFilterType, PbLevelType, PbTableSchema};
use tokio::time::Instant;

pub use super::context::CompactorContext;
use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::compactor::{
    ConcatSstableIterator, MultiCompactionFilter, StateCleanUpCompactionFilter, TaskProgress,
    TtlCompactionFilter,
};
use crate::hummock::iterator::{
    Forward, HummockIterator, MergeIterator, NonPkPrefixSkipWatermarkIterator,
    SkipWatermarkIterator, UserIterator,
};
use crate::hummock::multi_builder::TableBuilderFactory;
use crate::hummock::sstable::DEFAULT_ENTRY_SIZE;
use crate::hummock::{
    CachePolicy, FilterBuilder, GetObjectId, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableWriterFactory, SstableWriterOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct RemoteBuilderFactory<W: SstableWriterFactory, F: FilterBuilder> {
    pub object_id_getter: Box<dyn GetObjectId>,
    pub limiter: Arc<MemoryLimiter>,
    pub options: SstableBuilderOptions,
    pub policy: CachePolicy,
    pub remote_rpc_cost: Arc<AtomicU64>,
    pub compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    pub sstable_writer_factory: W,
    pub _phantom: PhantomData<F>,
}

#[async_trait::async_trait]
impl<W: SstableWriterFactory, F: FilterBuilder> TableBuilderFactory for RemoteBuilderFactory<W, F> {
    type Filter = F;
    type Writer = W::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        let timer = Instant::now();
        let table_id = self.object_id_getter.get_new_sst_object_id().await?;
        let cost = (timer.elapsed().as_secs_f64() * 1000000.0).round() as u64;
        self.remote_rpc_cost.fetch_add(cost, Ordering::Relaxed);
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity + self.options.block_capacity),
            tracker: None,
            policy: self.policy,
        };
        let writer = self
            .sstable_writer_factory
            .create_sst_writer(table_id, writer_options)
            .await?;
        let builder = SstableBuilder::new(
            table_id,
            writer,
            Self::Filter::create(
                self.options.bloom_false_positive,
                self.options.capacity / DEFAULT_ENTRY_SIZE + 1,
            ),
            self.options.clone(),
            self.compaction_catalog_agent_ref.clone(),
            Some(self.limiter.clone()),
        );
        Ok(builder)
    }
}

/// `CompactionStatistics` will count the results of each compact split
#[derive(Default, Debug)]
pub struct CompactionStatistics {
    // to report per-table metrics
    pub delta_drop_stat: TableStatsMap,

    // to calculate delete ratio
    pub iter_total_key_counts: u64,
    pub iter_drop_key_counts: u64,
}

impl CompactionStatistics {
    #[allow(dead_code)]
    fn delete_ratio(&self) -> Option<u64> {
        if self.iter_total_key_counts == 0 {
            return None;
        }

        Some(self.iter_drop_key_counts / self.iter_total_key_counts)
    }
}

#[derive(Clone, Default)]
pub struct TaskConfig {
    pub key_range: KeyRange,
    pub cache_policy: CachePolicy,
    pub gc_delete_keys: bool,
    pub retain_multiple_version: bool,
    /// `stats_target_table_ids` decides whether a dropped key should be counted as table stats
    /// change. For an divided SST as input, a dropped key shouldn't be counted if its table id
    /// doesn't belong to this divided SST. See `Compactor::compact_and_build_sst`.
    pub stats_target_table_ids: Option<HashSet<u32>>,
    pub task_type: PbTaskType,
    pub use_block_based_filter: bool,

    pub table_vnode_partition: BTreeMap<u32, u32>,
    /// `TableId` -> `TableSchema`
    /// Schemas in `table_schemas` are at least as new as the one used to create `input_ssts`.
    /// For a table with schema existing in `table_schemas`, its columns not in `table_schemas` but in `input_ssts` can be safely dropped.
    pub table_schemas: HashMap<u32, PbTableSchema>,
    /// `disable_drop_column_optimization` should only be set in benchmark.
    pub disable_drop_column_optimization: bool,
}

pub fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
    let mut multi_filter = MultiCompactionFilter::default();
    let compaction_filter_flag =
        CompactionFilterFlag::from_bits(compact_task.compaction_filter_mask).unwrap_or_default();
    if compaction_filter_flag.contains(CompactionFilterFlag::STATE_CLEAN) {
        let state_clean_up_filter = Box::new(StateCleanUpCompactionFilter::new(
            HashSet::from_iter(compact_task.existing_table_ids.clone()),
        ));

        multi_filter.register(state_clean_up_filter);
    }

    if compaction_filter_flag.contains(CompactionFilterFlag::TTL) {
        let id_to_ttl = compact_task
            .table_options
            .iter()
            .filter_map(|(id, option)| {
                option
                    .retention_seconds
                    .and_then(|ttl| if ttl > 0 { Some((*id, ttl)) } else { None })
            })
            .collect();

        let ttl_filter = Box::new(TtlCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

fn generate_splits_fast(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: &CompactorContext,
    max_sub_compaction: u32,
) -> Vec<KeyRange> {
    let worker_num = context.compaction_executor.worker_num();
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;

    let parallelism = calculate_task_parallelism_impl(
        worker_num,
        parallel_compact_size,
        compaction_size,
        max_sub_compaction,
    );
    let mut indexes = vec![];
    for sst in sstable_infos {
        let key_range = &sst.key_range;
        indexes.push(
            FullKey {
                user_key: FullKey::decode(&key_range.left).user_key,
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            }
            .encode(),
        );
        indexes.push(
            FullKey {
                user_key: FullKey::decode(&key_range.right).user_key,
                epoch_with_gap: EpochWithGap::new_max_epoch(),
            }
            .encode(),
        );
    }
    indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.as_ref(), b.as_ref()));
    indexes.dedup();
    if indexes.len() <= parallelism {
        return vec![];
    }

    let mut splits = vec![];
    splits.push(KeyRange::default());
    let parallel_key_count = indexes.len() / parallelism;
    let mut last_split_key_count = 0;
    for key in indexes {
        if last_split_key_count >= parallel_key_count {
            splits.last_mut().unwrap().right = Bytes::from(key.clone());
            splits.push(KeyRange::new(Bytes::from(key.clone()), Bytes::default()));
            last_split_key_count = 0;
        }
        last_split_key_count += 1;
    }

    splits
}

pub async fn generate_splits(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: &CompactorContext,
    max_sub_compaction: u32,
) -> HummockResult<Vec<KeyRange>> {
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    if compaction_size > parallel_compact_size {
        if sstable_infos.len() > context.storage_opts.compactor_max_preload_meta_file_count {
            return Ok(generate_splits_fast(
                sstable_infos,
                compaction_size,
                context,
                max_sub_compaction,
            ));
        }
        let mut indexes = vec![];
        // preload the meta and get the smallest key to split sub_compaction
        for sstable_info in sstable_infos {
            indexes.extend(
                context
                    .sstable_store
                    .sstable(sstable_info, &mut StoreLocalStatistic::default())
                    .await?
                    .meta
                    .block_metas
                    .iter()
                    .map(|block| {
                        let data_size = block.len;
                        let full_key = FullKey {
                            user_key: FullKey::decode(&block.smallest_key).user_key,
                            epoch_with_gap: EpochWithGap::new_max_epoch(),
                        }
                        .encode();
                        (data_size as u64, full_key)
                    })
                    .collect_vec(),
            );
        }
        // sort by key, as for every data block has the same size;
        indexes.sort_by(|a, b| KeyComparator::compare_encoded_full_key(a.1.as_ref(), b.1.as_ref()));
        let mut splits = vec![];
        splits.push(KeyRange::default());

        let parallelism = calculate_task_parallelism_impl(
            context.compaction_executor.worker_num(),
            parallel_compact_size,
            compaction_size,
            max_sub_compaction,
        );

        let sub_compaction_data_size =
            std::cmp::max(compaction_size / parallelism as u64, parallel_compact_size);

        if parallelism > 1 {
            let mut last_buffer_size = 0;
            let mut last_key: Vec<u8> = vec![];
            let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
            for (data_size, key) in indexes {
                if last_buffer_size >= sub_compaction_data_size
                    && !last_key.eq(&key)
                    && remaining_size > parallel_compact_size
                {
                    splits.last_mut().unwrap().right = Bytes::from(key.clone());
                    splits.push(KeyRange::new(Bytes::from(key.clone()), Bytes::default()));
                    last_buffer_size = data_size;
                } else {
                    last_buffer_size += data_size;
                }
                remaining_size -= data_size;
                last_key = key;
            }
            return Ok(splits);
        }
    }

    Ok(vec![])
}

pub fn estimate_task_output_capacity(context: CompactorContext, task: &CompactTask) -> usize {
    let max_target_file_size = context.storage_opts.sstable_size_mb as usize * (1 << 20);
    let total_input_uncompressed_file_size = task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .map(|table| table.uncompressed_file_size)
        .sum::<u64>();

    let capacity = std::cmp::min(task.target_file_size as usize, max_target_file_size);
    std::cmp::min(capacity, total_input_uncompressed_file_size as usize)
}

/// Compare result of compaction task and input. The data saw by user shall not change after applying compaction result.
pub async fn check_compaction_result(
    compact_task: &CompactTask,
    context: CompactorContext,
    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
) -> HummockResult<bool> {
    let mut table_ids_from_input_ssts = compact_task.get_table_ids_from_input_ssts();
    let need_clean_state_table = table_ids_from_input_ssts
        .any(|table_id| !compact_task.existing_table_ids.contains(&table_id));
    // This check method does not consider dropped keys by compaction filter.
    if compact_task.contains_ttl() || need_clean_state_table {
        return Ok(true);
    }

    let mut table_iters = Vec::new();
    for level in &compact_task.input_ssts {
        if level.table_infos.is_empty() {
            continue;
        }

        // Do not need to filter the table because manager has done it.
        if level.level_type == PbLevelType::Nonoverlapping {
            debug_assert!(can_concat(&level.table_infos));

            table_iters.push(ConcatSstableIterator::new(
                compact_task.existing_table_ids.clone(),
                level.table_infos.clone(),
                KeyRange::inf(),
                context.sstable_store.clone(),
                Arc::new(TaskProgress::default()),
                context.storage_opts.compactor_iter_max_io_retry_times,
            ));
        } else {
            for table_info in &level.table_infos {
                table_iters.push(ConcatSstableIterator::new(
                    compact_task.existing_table_ids.clone(),
                    vec![table_info.clone()],
                    KeyRange::inf(),
                    context.sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    context.storage_opts.compactor_iter_max_io_retry_times,
                ));
            }
        }
    }

    let iter = MergeIterator::for_compactor(table_iters);
    let (pk_watermarks, non_pk_prefix_watermarks) = split_watermark_from_task(compact_task);
    let left_iter = {
        let skip_watermark_iter =
            SkipWatermarkIterator::from_safe_epoch_watermarks(iter, pk_watermarks.clone());

        let combine_iter = NonPkPrefixSkipWatermarkIterator::from_safe_epoch_watermarks(
            skip_watermark_iter,
            non_pk_prefix_watermarks.clone(),
            compaction_catalog_agent_ref.clone(),
        );

        UserIterator::new(
            combine_iter,
            (Bound::Unbounded, Bound::Unbounded),
            u64::MAX,
            0,
            None,
        )
    };
    let iter = ConcatSstableIterator::new(
        compact_task.existing_table_ids.clone(),
        compact_task.sorted_output_ssts.clone(),
        KeyRange::inf(),
        context.sstable_store.clone(),
        Arc::new(TaskProgress::default()),
        context.storage_opts.compactor_iter_max_io_retry_times,
    );
    let right_iter = {
        let skip_watermark_iter =
            SkipWatermarkIterator::from_safe_epoch_watermarks(iter, pk_watermarks);

        let combine_iter = NonPkPrefixSkipWatermarkIterator::from_safe_epoch_watermarks(
            skip_watermark_iter,
            non_pk_prefix_watermarks,
            compaction_catalog_agent_ref,
        );

        UserIterator::new(
            combine_iter,
            (Bound::Unbounded, Bound::Unbounded),
            u64::MAX,
            0,
            None,
        )
    };

    check_result(left_iter, right_iter).await
}

pub async fn check_flush_result<I: HummockIterator<Direction = Forward>>(
    left_iter: UserIterator<I>,
    existing_table_ids: Vec<StateTableId>,
    sort_ssts: Vec<SstableInfo>,
    context: CompactorContext,
) -> HummockResult<bool> {
    let iter = ConcatSstableIterator::new(
        existing_table_ids.clone(),
        sort_ssts.clone(),
        KeyRange::inf(),
        context.sstable_store.clone(),
        Arc::new(TaskProgress::default()),
        0,
    );
    let right_iter = UserIterator::new(
        iter,
        (Bound::Unbounded, Bound::Unbounded),
        u64::MAX,
        0,
        None,
    );
    check_result(left_iter, right_iter).await
}

async fn check_result<
    I1: HummockIterator<Direction = Forward>,
    I2: HummockIterator<Direction = Forward>,
>(
    mut left_iter: UserIterator<I1>,
    mut right_iter: UserIterator<I2>,
) -> HummockResult<bool> {
    left_iter.rewind().await?;
    right_iter.rewind().await?;
    let mut right_count = 0;
    let mut left_count = 0;
    while left_iter.is_valid() && right_iter.is_valid() {
        if left_iter.key() != right_iter.key() {
            tracing::error!(
                "The key of input and output not equal. key: {:?} vs {:?}",
                left_iter.key(),
                right_iter.key()
            );
            return Ok(false);
        }
        if left_iter.value() != right_iter.value() {
            tracing::error!(
                "The value of input and output not equal. key: {:?}, value: {:?} vs {:?}",
                left_iter.key(),
                left_iter.value(),
                right_iter.value()
            );
            return Ok(false);
        }
        left_iter.next().await?;
        right_iter.next().await?;
        left_count += 1;
        right_count += 1;
    }
    while left_iter.is_valid() {
        left_count += 1;
        left_iter.next().await?;
    }
    while right_iter.is_valid() {
        right_count += 1;
        right_iter.next().await?;
    }
    if left_count != right_count {
        tracing::error!(
            "The key count of input and output not equal: {} vs {}",
            left_count,
            right_count
        );
        return Ok(false);
    }
    Ok(true)
}

pub fn optimize_by_copy_block(compact_task: &CompactTask, context: &CompactorContext) -> bool {
    let sstable_infos = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .filter(|table_info| {
            let table_ids = &table_info.table_ids;
            table_ids
                .iter()
                .any(|table_id| compact_task.existing_table_ids.contains(table_id))
        })
        .cloned()
        .collect_vec();
    let compaction_size = sstable_infos
        .iter()
        .map(|table_info| table_info.sst_size)
        .sum::<u64>();

    let all_ssts_are_blocked_filter = sstable_infos
        .iter()
        .all(|table_info| table_info.bloom_filter_kind == BloomFilterType::Blocked);

    let delete_key_count = sstable_infos
        .iter()
        .map(|table_info| table_info.stale_key_count + table_info.range_tombstone_count)
        .sum::<u64>();
    let total_key_count = sstable_infos
        .iter()
        .map(|table_info| table_info.total_key_count)
        .sum::<u64>();

    let single_table = compact_task.build_compact_table_ids().len() == 1;
    context.storage_opts.enable_fast_compaction
        && all_ssts_are_blocked_filter
        && !compact_task.contains_range_tombstone()
        && !compact_task.contains_ttl()
        && !compact_task.contains_split_sst()
        && single_table
        && compact_task.target_level > 0
        && compact_task.input_ssts.len() == 2
        && compaction_size < context.storage_opts.compactor_fast_max_compact_task_size
        && delete_key_count * 100
            < context.storage_opts.compactor_fast_max_compact_delete_ratio as u64 * total_key_count
        && compact_task.task_type == PbTaskType::Dynamic
}

pub async fn generate_splits_for_task(
    compact_task: &mut CompactTask,
    context: &CompactorContext,
    optimize_by_copy_block: bool,
) -> HummockResult<()> {
    let sstable_infos = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .filter(|table_info| {
            let table_ids = &table_info.table_ids;
            table_ids
                .iter()
                .any(|table_id| compact_task.existing_table_ids.contains(table_id))
        })
        .cloned()
        .collect_vec();
    let compaction_size = sstable_infos
        .iter()
        .map(|table_info| table_info.sst_size)
        .sum::<u64>();

    if !optimize_by_copy_block {
        let splits = generate_splits(
            &sstable_infos,
            compaction_size,
            context,
            compact_task.max_sub_compaction,
        )
        .await?;
        if !splits.is_empty() {
            compact_task.splits = splits;
        }
        return Ok(());
    }

    Ok(())
}

pub fn metrics_report_for_task(compact_task: &CompactTask, context: &CompactorContext) {
    let group_label = compact_task.compaction_group_id.to_string();
    let cur_level_label = compact_task.input_ssts[0].level_idx.to_string();
    let select_table_infos = compact_task
        .input_ssts
        .iter()
        .filter(|level| level.level_idx != compact_task.target_level)
        .flat_map(|level| level.table_infos.iter())
        .collect_vec();
    let target_table_infos = compact_task
        .input_ssts
        .iter()
        .filter(|level| level.level_idx == compact_task.target_level)
        .flat_map(|level| level.table_infos.iter())
        .collect_vec();
    let select_size = select_table_infos
        .iter()
        .map(|table| table.sst_size)
        .sum::<u64>();
    context
        .compactor_metrics
        .compact_read_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_size);
    context
        .compactor_metrics
        .compact_read_sstn_current_level
        .with_label_values(&[&group_label, &cur_level_label])
        .inc_by(select_table_infos.len() as u64);

    let target_level_read_bytes = target_table_infos.iter().map(|t| t.sst_size).sum::<u64>();
    let next_level_label = compact_task.target_level.to_string();
    context
        .compactor_metrics
        .compact_read_next_level
        .with_label_values(&[&group_label, next_level_label.as_str()])
        .inc_by(target_level_read_bytes);
    context
        .compactor_metrics
        .compact_read_sstn_next_level
        .with_label_values(&[&group_label, next_level_label.as_str()])
        .inc_by(target_table_infos.len() as u64);
}

pub fn calculate_task_parallelism(compact_task: &CompactTask, context: &CompactorContext) -> usize {
    let optimize_by_copy_block = optimize_by_copy_block(compact_task, context);

    if optimize_by_copy_block {
        return 1;
    }

    let sstable_infos = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .filter(|table_info| {
            let table_ids = &table_info.table_ids;
            table_ids
                .iter()
                .any(|table_id| compact_task.existing_table_ids.contains(table_id))
        })
        .cloned()
        .collect_vec();
    let compaction_size = sstable_infos
        .iter()
        .map(|table_info| table_info.sst_size)
        .sum::<u64>();
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    calculate_task_parallelism_impl(
        context.compaction_executor.worker_num(),
        parallel_compact_size,
        compaction_size,
        compact_task.max_sub_compaction,
    )
}

pub fn calculate_task_parallelism_impl(
    worker_num: usize,
    parallel_compact_size: u64,
    compaction_size: u64,
    max_sub_compaction: u32,
) -> usize {
    let parallelism = compaction_size.div_ceil(parallel_compact_size);
    worker_num.min(parallelism.min(max_sub_compaction as u64) as usize)
}

pub fn split_watermark_from_task(
    compact_task: &CompactTask,
) -> (
    BTreeMap<u32, TableWatermarks>,
    BTreeMap<u32, TableWatermarks>,
) {
    let mut pk_watermarks = BTreeMap::default();
    let mut non_pk_prefix_watermarks = BTreeMap::default();

    for (table_id, watermark) in &compact_task.table_watermarks {
        if let WatermarkSerdeType::PkPrefix = watermark.watermark_type {
            pk_watermarks.insert(*table_id, watermark.clone());
        } else {
            non_pk_prefix_watermarks.insert(*table_id, watermark.clone());
        }
    }

    (pk_watermarks, non_pk_prefix_watermarks)
}
