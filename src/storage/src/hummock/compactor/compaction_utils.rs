// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::prost_key_range::KeyRangeExt;
use risingwave_hummock_sdk::table_stats::TableStatsMap;
use risingwave_hummock_sdk::{can_concat, EpochWithGap, KeyComparator};
use risingwave_pb::hummock::{
    compact_task, CompactTask, KeyRange as KeyRange_vec, LevelType, SstableInfo,
};
use tokio::time::Instant;

pub use super::context::CompactorContext;
use crate::filter_key_extractor::FilterKeyExtractorImpl;
use crate::hummock::compactor::{
    ConcatSstableIterator, MultiCompactionFilter, StateCleanUpCompactionFilter, TaskProgress,
    TtlCompactionFilter,
};
use crate::hummock::iterator::{
    Forward, ForwardMergeRangeIterator, HummockIterator, SkipWatermarkIterator,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::multi_builder::TableBuilderFactory;
use crate::hummock::sstable::DEFAULT_ENTRY_SIZE;
use crate::hummock::{
    CachePolicy, FilterBuilder, GetObjectId, HummockResult, MemoryLimiter, SstableBuilder,
    SstableBuilderOptions, SstableDeleteRangeIterator, SstableWriterFactory, SstableWriterOptions,
};
use crate::monitor::StoreLocalStatistic;

pub struct RemoteBuilderFactory<W: SstableWriterFactory, F: FilterBuilder> {
    pub object_id_getter: Box<dyn GetObjectId>,
    pub limiter: Arc<MemoryLimiter>,
    pub options: SstableBuilderOptions,
    pub policy: CachePolicy,
    pub remote_rpc_cost: Arc<AtomicU64>,
    pub filter_key_extractor: Arc<FilterKeyExtractorImpl>,
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
            self.filter_key_extractor.clone(),
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
    pub watermark: u64,
    /// `stats_target_table_ids` decides whether a dropped key should be counted as table stats
    /// change. For an divided SST as input, a dropped key shouldn't be counted if its table id
    /// doesn't belong to this divided SST. See `Compactor::compact_and_build_sst`.
    pub stats_target_table_ids: Option<HashSet<u32>>,
    pub task_type: compact_task::TaskType,
    pub is_target_l0_or_lbase: bool,
    pub use_block_based_filter: bool,

    pub table_vnode_partition: BTreeMap<u32, u32>,
}

pub fn build_multi_compaction_filter(compact_task: &CompactTask) -> MultiCompactionFilter {
    use risingwave_common::catalog::TableOption;
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
            .filter(|id_to_option| {
                let table_option: TableOption = id_to_option.1.into();
                table_option.retention_seconds.is_some()
            })
            .map(|id_to_option| (*id_to_option.0, id_to_option.1.retention_seconds))
            .collect();

        let ttl_filter = Box::new(TtlCompactionFilter::new(
            id_to_ttl,
            compact_task.current_epoch_time,
        ));
        multi_filter.register(ttl_filter);
    }

    multi_filter
}

const MAX_FILE_COUNT: usize = 32;

fn generate_splits_fast(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: CompactorContext,
) -> HummockResult<Vec<KeyRange_vec>> {
    let worker_num = context.compaction_executor.worker_num();
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;

    let parallelism = (compaction_size + parallel_compact_size - 1) / parallel_compact_size;

    let parallelism = std::cmp::min(
        worker_num,
        std::cmp::min(
            parallelism as usize,
            context.storage_opts.max_sub_compaction as usize,
        ),
    );
    let mut indexes = vec![];
    for sst in sstable_infos {
        let key_range = sst.key_range.as_ref().unwrap();
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
        return Ok(vec![]);
    }
    let mut splits = vec![];
    splits.push(KeyRange_vec::new(vec![], vec![]));
    let parallel_key_count = indexes.len() / parallelism;
    let mut last_split_key_count = 0;
    for key in indexes {
        if last_split_key_count >= parallel_key_count {
            splits.last_mut().unwrap().right = key.clone();
            splits.push(KeyRange_vec::new(key.clone(), vec![]));
            last_split_key_count = 0;
        }
        last_split_key_count += 1;
    }
    Ok(splits)
}

pub async fn generate_splits(
    sstable_infos: &Vec<SstableInfo>,
    compaction_size: u64,
    context: CompactorContext,
) -> HummockResult<Vec<KeyRange_vec>> {
    let parallel_compact_size = (context.storage_opts.parallel_compact_size_mb as u64) << 20;
    if compaction_size > parallel_compact_size {
        if sstable_infos.len() > MAX_FILE_COUNT {
            return generate_splits_fast(sstable_infos, compaction_size, context);
        }
        let mut indexes = vec![];
        // preload the meta and get the smallest key to split sub_compaction
        for sstable_info in sstable_infos {
            indexes.extend(
                context
                    .sstable_store
                    .sstable(sstable_info, &mut StoreLocalStatistic::default())
                    .await?
                    .value()
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
        splits.push(KeyRange_vec::new(vec![], vec![]));

        let worker_num = context.compaction_executor.worker_num();

        let parallelism = std::cmp::min(
            worker_num as u64,
            std::cmp::min(
                indexes.len() as u64,
                context.storage_opts.max_sub_compaction as u64,
            ),
        );
        let sub_compaction_data_size =
            std::cmp::max(compaction_size / parallelism, parallel_compact_size);
        let parallelism = compaction_size / sub_compaction_data_size;

        if parallelism > 1 {
            let mut last_buffer_size = 0;
            let mut last_key: Vec<u8> = vec![];
            let mut remaining_size = indexes.iter().map(|block| block.0).sum::<u64>();
            for (data_size, key) in indexes {
                if last_buffer_size >= sub_compaction_data_size
                    && !last_key.eq(&key)
                    && remaining_size > parallel_compact_size
                {
                    splits.last_mut().unwrap().right = key.clone();
                    splits.push(KeyRange_vec::new(key.clone(), vec![]));
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
) -> HummockResult<bool> {
    let has_ttl = compact_task
        .table_options
        .iter()
        .any(|(_, table_option)| table_option.retention_seconds > 0);

    let mut compact_table_ids = compact_task
        .input_ssts
        .iter()
        .flat_map(|level| level.table_infos.iter())
        .flat_map(|sst| sst.table_ids.clone())
        .collect_vec();
    compact_table_ids.sort();
    compact_table_ids.dedup();
    let existing_table_ids: HashSet<u32> =
        HashSet::from_iter(compact_task.existing_table_ids.clone());
    let need_clean_state_table = compact_table_ids
        .iter()
        .any(|table_id| !existing_table_ids.contains(table_id));
    // This check method does not consider dropped keys by compaction filter.
    if has_ttl || need_clean_state_table {
        return Ok(true);
    }

    let mut table_iters = Vec::new();
    let mut del_iter = ForwardMergeRangeIterator::default();
    let compact_io_retry_time = context.storage_opts.compact_iter_recreate_timeout_ms;
    for level in &compact_task.input_ssts {
        if level.table_infos.is_empty() {
            continue;
        }

        // Do not need to filter the table because manager has done it.
        if level.level_type == LevelType::Nonoverlapping as i32 {
            debug_assert!(can_concat(&level.table_infos));
            del_iter.add_concat_iter(level.table_infos.clone(), context.sstable_store.clone());

            table_iters.push(ConcatSstableIterator::new(
                compact_task.existing_table_ids.clone(),
                level.table_infos.clone(),
                KeyRange::inf(),
                context.sstable_store.clone(),
                Arc::new(TaskProgress::default()),
                compact_io_retry_time,
            ));
        } else {
            let mut stats = StoreLocalStatistic::default();
            for table_info in &level.table_infos {
                let table = context
                    .sstable_store
                    .sstable(table_info, &mut stats)
                    .await?;
                del_iter.add_sst_iter(SstableDeleteRangeIterator::new(table));
                table_iters.push(ConcatSstableIterator::new(
                    compact_task.existing_table_ids.clone(),
                    vec![table_info.clone()],
                    KeyRange::inf(),
                    context.sstable_store.clone(),
                    Arc::new(TaskProgress::default()),
                    compact_io_retry_time,
                ));
            }
        }
    }
    let iter = UnorderedMergeIteratorInner::for_compactor(table_iters);
    let left_iter = UserIterator::new(
        SkipWatermarkIterator::from_safe_epoch_watermarks(iter, &compact_task.table_watermarks),
        (Bound::Unbounded, Bound::Unbounded),
        u64::MAX,
        0,
        None,
        del_iter,
    );
    let mut del_iter = ForwardMergeRangeIterator::default();
    del_iter.add_concat_iter(
        compact_task.sorted_output_ssts.clone(),
        context.sstable_store.clone(),
    );
    let iter = ConcatSstableIterator::new(
        compact_task.existing_table_ids.clone(),
        compact_task.sorted_output_ssts.clone(),
        KeyRange::inf(),
        context.sstable_store.clone(),
        Arc::new(TaskProgress::default()),
        compact_io_retry_time,
    );
    let right_iter = UserIterator::new(
        SkipWatermarkIterator::from_safe_epoch_watermarks(iter, &compact_task.table_watermarks),
        (Bound::Unbounded, Bound::Unbounded),
        u64::MAX,
        0,
        None,
        del_iter,
    );

    check_result(left_iter, right_iter).await
}

pub async fn check_flush_result<I: HummockIterator<Direction = Forward>>(
    left_iter: UserIterator<I>,
    existing_table_ids: Vec<StateTableId>,
    sort_ssts: Vec<SstableInfo>,
    context: CompactorContext,
) -> HummockResult<bool> {
    let mut del_iter = ForwardMergeRangeIterator::default();
    del_iter.add_concat_iter(sort_ssts.clone(), context.sstable_store.clone());
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
        del_iter,
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
    }
    if left_iter.is_valid() || right_iter.is_valid() {
        tracing::error!("The key count of input and output not equal");
        return Ok(false);
    }
    Ok(true)
}
