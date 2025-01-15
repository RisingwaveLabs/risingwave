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

use std::cmp::Ordering;
use std::collections::HashSet;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::time::Instant;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use fail::fail_point;
use itertools::Itertools;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::TableStats;
use risingwave_hummock_sdk::{can_concat, compact_task_to_string, EpochWithGap, LocalSstableInfo};

use crate::compaction_catalog_manager::CompactionCatalogAgentRef;
use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::compactor::compaction_utils::split_watermark_from_task;
use crate::hummock::compactor::task_progress::TaskProgress;
use crate::hummock::compactor::{
    CompactionStatistics, Compactor, CompactorContext, RemoteBuilderFactory, TaskConfig,
};
use crate::hummock::iterator::{NonPkPrefixSkipWatermarkState, SkipWatermarkState};
use crate::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockBuilder, BlockHolder, BlockIterator, BlockMeta, BlockedXor16FilterBuilder,
    CachePolicy, CompressionAlgorithm, GetObjectId, HummockResult, SstableBuilderOptions,
    TableHolder, UnifiedSstableWriterFactory,
};
use crate::monitor::{CompactorMetrics, StoreLocalStatistic};

/// Iterates over the KV-pairs of an SST while downloading it.
pub struct BlockStreamIterator {
    /// The downloading stream.
    block_stream: Option<BlockDataStream>,

    next_block_index: usize,

    /// For key sanity check of divided SST and debugging
    sstable: TableHolder,
    iter: Option<BlockIterator>,
    task_progress: Arc<TaskProgress>,

    // For block stream recreate
    sstable_store: SstableStoreRef,
    sstable_info: SstableInfo,
    io_retry_times: usize,
    max_io_retry_times: usize,
    stats_ptr: Arc<AtomicU64>,
}

impl BlockStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`BlockStreamIterator`] which iterates over the given [`BlockDataStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        sstable: TableHolder,
        task_progress: Arc<TaskProgress>,
        sstable_store: SstableStoreRef,
        sstable_info: SstableInfo,
        max_io_retry_times: usize,
        stats_ptr: Arc<AtomicU64>,
    ) -> Self {
        Self {
            block_stream: None,
            next_block_index: 0,
            sstable,
            iter: None,
            task_progress,
            sstable_store,
            sstable_info,
            io_retry_times: 0,
            max_io_retry_times,
            stats_ptr,
        }
    }

    async fn create_stream(&mut self) -> HummockResult<()> {
        // Fast compact only support the single table compaction.(not split sst)
        // So we don't need to filter the block_metas with table_id and key_range
        let block_stream = self
            .sstable_store
            .get_stream_for_blocks(
                self.sstable_info.object_id,
                &self.sstable.meta.block_metas[self.next_block_index..],
            )
            .verbose_instrument_await("stream_iter_get_stream")
            .await?;
        self.block_stream = Some(block_stream);
        Ok(())
    }

    /// Wrapper function for `self.block_stream.next()` which allows us to measure the time needed.
    pub(crate) async fn download_next_block(
        &mut self,
    ) -> HummockResult<Option<(Bytes, Vec<u8>, BlockMeta)>> {
        let now = Instant::now();
        let _time_stat = scopeguard::guard(self.stats_ptr.clone(), |stats_ptr: Arc<AtomicU64>| {
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);
        });
        loop {
            let ret = match &mut self.block_stream {
                Some(block_stream) => block_stream.next_block_impl().await,
                None => {
                    self.create_stream().await?;
                    continue;
                }
            };
            match ret {
                Ok(Some((data, _))) => {
                    let meta = self.sstable.meta.block_metas[self.next_block_index].clone();
                    let filter_block = self
                        .sstable
                        .filter_reader
                        .get_block_raw_filter(self.next_block_index);
                    self.next_block_index += 1;
                    return Ok(Some((data, filter_block, meta)));
                }

                Ok(None) => break,

                Err(e) => {
                    if !e.is_object_error() || self.io_retry_times >= self.max_io_retry_times {
                        return Err(e);
                    }

                    self.block_stream.take();
                    self.io_retry_times += 1;
                    fail_point!("create_stream_err");

                    tracing::warn!(
                        "fast compact retry create stream for sstable {} times, sstinfo={}",
                        self.io_retry_times,
                        format!(
                            "object_id={}, sst_id={}, meta_offset={}, table_ids={:?}",
                            self.sstable_info.object_id,
                            self.sstable_info.sst_id,
                            self.sstable_info.meta_offset,
                            self.sstable_info.table_ids
                        )
                    );
                }
            }
        }

        self.next_block_index = self.sstable.meta.block_metas.len();
        self.iter.take();
        Ok(None)
    }

    pub(crate) fn init_block_iter(
        &mut self,
        buf: Bytes,
        uncompressed_capacity: usize,
    ) -> HummockResult<()> {
        let block = Block::decode(buf, uncompressed_capacity)?;
        let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
        iter.seek_to_first();
        self.iter = Some(iter);
        Ok(())
    }

    fn next_block_smallest(&self) -> &[u8] {
        self.sstable.meta.block_metas[self.next_block_index]
            .smallest_key
            .as_ref()
    }

    fn next_block_largest(&self) -> &[u8] {
        if self.next_block_index + 1 < self.sstable.meta.block_metas.len() {
            self.sstable.meta.block_metas[self.next_block_index + 1]
                .smallest_key
                .as_ref()
        } else {
            self.sstable.meta.largest_key.as_ref()
        }
    }

    fn current_block_largest(&self) -> Vec<u8> {
        if self.next_block_index < self.sstable.meta.block_metas.len() {
            let mut largest_key = FullKey::decode(
                self.sstable.meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            );
            // do not include this key because it is the smallest key of next block.
            largest_key.epoch_with_gap = EpochWithGap::new_max_epoch();
            largest_key.encode()
        } else {
            self.sstable.meta.largest_key.clone()
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        match self.iter.as_ref() {
            Some(iter) => iter.key(),
            None => FullKey::decode(
                self.sstable.meta.block_metas[self.next_block_index]
                    .smallest_key
                    .as_ref(),
            ),
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.iter.is_some() || self.next_block_index < self.sstable.meta.block_metas.len()
    }

    #[cfg(test)]
    #[cfg(feature = "failpoints")]
    pub(crate) fn iter_mut(&mut self) -> &mut BlockIterator {
        self.iter.as_mut().unwrap()
    }
}

impl Drop for BlockStreamIterator {
    fn drop(&mut self) {
        self.task_progress.dec_num_pending_read_io();
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<BlockStreamIterator>,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    sstables: Vec<SstableInfo>,

    sstable_store: SstableStoreRef,

    stats: StoreLocalStatistic,
    task_progress: Arc<TaskProgress>,

    max_io_retry_times: usize,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        sst_infos: Vec<SstableInfo>,
        sstable_store: SstableStoreRef,
        task_progress: Arc<TaskProgress>,
        max_io_retry_times: usize,
    ) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            sstables: sst_infos,
            sstable_store,
            task_progress,
            stats: StoreLocalStatistic::default(),
            max_io_retry_times,
        }
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0).await
    }

    pub async fn next_sstable(&mut self) -> HummockResult<()> {
        self.seek_idx(self.cur_idx + 1).await
    }

    pub fn current_sstable(&mut self) -> &mut BlockStreamIterator {
        self.sstable_iter.as_mut().unwrap()
    }

    pub async fn init_block_iter(&mut self) -> HummockResult<()> {
        if let Some(sstable) = self.sstable_iter.as_mut() {
            if sstable.iter.is_some() {
                return Ok(());
            }
            let (buf, _, meta) = sstable.download_next_block().await?.unwrap();
            sstable.init_block_iter(buf, meta.uncompressed_size as usize)?;
        }
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.cur_idx < self.sstables.len()
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize) -> HummockResult<()> {
        self.sstable_iter.take();
        self.cur_idx = idx;
        if self.cur_idx < self.sstables.len() {
            let sstable_info = &self.sstables[self.cur_idx];
            let sstable = self
                .sstable_store
                .sstable(sstable_info, &mut self.stats)
                .verbose_instrument_await("stream_iter_sstable")
                .await?;
            self.task_progress.inc_num_pending_read_io();

            let sstable_iter = BlockStreamIterator::new(
                sstable,
                self.task_progress.clone(),
                self.sstable_store.clone(),
                sstable_info.clone(),
                self.max_io_retry_times,
                self.stats.remote_io_time.clone(),
            );
            self.sstable_iter = Some(sstable_iter);
        }
        Ok(())
    }
}

pub struct CompactorRunner {
    left: Box<ConcatSstableIterator>,
    right: Box<ConcatSstableIterator>,
    task_id: u64,
    executor: CompactTaskExecutor<
        RemoteBuilderFactory<UnifiedSstableWriterFactory, BlockedXor16FilterBuilder>,
    >,
    compression_algorithm: CompressionAlgorithm,
    metrics: Arc<CompactorMetrics>,
}

impl CompactorRunner {
    pub fn new(
        context: CompactorContext,
        task: CompactTask,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        object_id_getter: Box<dyn GetObjectId>,
        task_progress: Arc<TaskProgress>,
    ) -> Self {
        let mut options: SstableBuilderOptions = context.storage_opts.as_ref().into();
        let compression_algorithm: CompressionAlgorithm = task.compression_algorithm.into();
        options.compression_algorithm = compression_algorithm;
        options.capacity = task.target_file_size as usize;
        let get_id_time = Arc::new(AtomicU64::new(0));

        let key_range = KeyRange::inf();

        let task_config = TaskConfig {
            key_range,
            cache_policy: CachePolicy::NotFill,
            gc_delete_keys: task.gc_delete_keys,
            retain_multiple_version: false,
            stats_target_table_ids: Some(HashSet::from_iter(task.existing_table_ids.clone())),
            task_type: task.task_type,
            table_vnode_partition: task.table_vnode_partition.clone(),
            use_block_based_filter: true,
            table_schemas: Default::default(),
            disable_drop_column_optimization: false,
        };
        let factory = UnifiedSstableWriterFactory::new(context.sstable_store.clone());

        let builder_factory = RemoteBuilderFactory::<_, BlockedXor16FilterBuilder> {
            object_id_getter,
            limiter: context.memory_limiter.clone(),
            options,
            policy: task_config.cache_policy,
            remote_rpc_cost: get_id_time,
            compaction_catalog_agent_ref: compaction_catalog_agent_ref.clone(),
            sstable_writer_factory: factory,
            _phantom: PhantomData,
        };
        let sst_builder = CapacitySplitTableBuilder::new(
            builder_factory,
            context.compactor_metrics.clone(),
            Some(task_progress.clone()),
            task_config.table_vnode_partition.clone(),
            context
                .storage_opts
                .compactor_concurrent_uploading_sst_count,
            compaction_catalog_agent_ref.clone(),
        );
        assert_eq!(
            task.input_ssts.len(),
            2,
            "TaskId {} target_level {:?} task {:?}",
            task.task_id,
            task.target_level,
            compact_task_to_string(&task)
        );
        let left = Box::new(ConcatSstableIterator::new(
            task.input_ssts[0].table_infos.clone(),
            context.sstable_store.clone(),
            task_progress.clone(),
            context.storage_opts.compactor_iter_max_io_retry_times,
        ));
        let right = Box::new(ConcatSstableIterator::new(
            task.input_ssts[1].table_infos.clone(),
            context.sstable_store,
            task_progress.clone(),
            context.storage_opts.compactor_iter_max_io_retry_times,
        ));

        let (pk_watermarks, non_pk_prefix_watermarks) = split_watermark_from_task(&task);

        let state = SkipWatermarkState::from_safe_epoch_watermarks(pk_watermarks);
        let non_pk_prefix_state = NonPkPrefixSkipWatermarkState::from_safe_epoch_watermarks(
            non_pk_prefix_watermarks,
            compaction_catalog_agent_ref,
        );

        Self {
            executor: CompactTaskExecutor::new(
                sst_builder,
                task_config,
                task_progress,
                state,
                non_pk_prefix_state,
            ),
            left,
            right,
            task_id: task.task_id,
            metrics: context.compactor_metrics.clone(),
            compression_algorithm,
        }
    }

    pub async fn run(mut self) -> HummockResult<(Vec<LocalSstableInfo>, CompactionStatistics)> {
        self.left.rewind().await?;
        self.right.rewind().await?;
        let mut skip_raw_block_count = 0;
        let mut skip_raw_block_size = 0;
        while self.left.is_valid() && self.right.is_valid() {
            let ret = self
                .left
                .current_sstable()
                .key()
                .cmp(&self.right.current_sstable().key());
            let (first, second) = if ret == Ordering::Less {
                (&mut self.left, &mut self.right)
            } else {
                (&mut self.right, &mut self.left)
            };
            assert!(
                ret != Ordering::Equal,
                "sst range overlap equal_key {:?}",
                self.left.current_sstable().key()
            );
            if first.current_sstable().iter.is_none() {
                let right_key = second.current_sstable().key();
                while first.current_sstable().is_valid() && !self.executor.builder.need_flush() {
                    let full_key = FullKey::decode(first.current_sstable().next_block_largest());
                    // the full key may be either Excluded key or Included key, so we do not allow
                    // they equals.
                    if full_key.user_key.ge(&right_key.user_key) {
                        break;
                    }
                    let smallest_key =
                        FullKey::decode(first.current_sstable().next_block_smallest());
                    if !self.executor.shall_copy_raw_block(&smallest_key) {
                        break;
                    }
                    let smallest_key = smallest_key.to_vec();

                    let (mut block, filter_data, mut meta) = first
                        .current_sstable()
                        .download_next_block()
                        .await?
                        .unwrap();
                    let algorithm = Block::get_algorithm(&block)?;
                    if algorithm == CompressionAlgorithm::None
                        && algorithm != self.compression_algorithm
                    {
                        block = BlockBuilder::compress_block(block, self.compression_algorithm)?;
                        meta.len = block.len() as u32;
                    }

                    let largest_key = first.current_sstable().current_block_largest();
                    let block_len = block.len() as u64;
                    let block_key_count = meta.total_key_count;

                    if self
                        .executor
                        .builder
                        .add_raw_block(block, filter_data, smallest_key, largest_key, meta)
                        .await?
                    {
                        skip_raw_block_size += block_len;
                        skip_raw_block_count += 1;
                    }
                    self.executor.may_report_process_key(block_key_count);
                    self.executor.clear();
                }
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                    continue;
                }
                first.init_block_iter().await?;
            }

            let target_key = second.current_sstable().key();
            let iter = first.sstable_iter.as_mut().unwrap().iter.as_mut().unwrap();
            self.executor.run(iter, target_key).await?;
            if !iter.is_valid() {
                first.sstable_iter.as_mut().unwrap().iter.take();
                if !first.current_sstable().is_valid() {
                    first.next_sstable().await?;
                }
            }
        }
        let rest_data = if !self.left.is_valid() {
            &mut self.right
        } else {
            &mut self.left
        };
        if rest_data.is_valid() {
            // compact rest keys of the current block.
            let sstable_iter = rest_data.sstable_iter.as_mut().unwrap();
            let target_key = FullKey::decode(&sstable_iter.sstable.meta.largest_key);
            if let Some(iter) = sstable_iter.iter.as_mut() {
                self.executor.run(iter, target_key).await?;
                assert!(
                    !iter.is_valid(),
                    "iter should not be valid key {:?}",
                    iter.key()
                );
            }
            sstable_iter.iter.take();
        }

        while rest_data.is_valid() {
            let mut sstable_iter = rest_data.sstable_iter.take().unwrap();
            while sstable_iter.is_valid() {
                let smallest_key = FullKey::decode(sstable_iter.next_block_smallest()).to_vec();
                let (block, filter_data, block_meta) =
                    sstable_iter.download_next_block().await?.unwrap();
                // If the last key is tombstone and it was deleted, the first key of this block must be deleted. So we can not move this block directly.
                let need_deleted = self.executor.last_key.user_key.eq(&smallest_key.user_key)
                    && self.executor.last_key_is_delete;
                if self.executor.builder.need_flush()
                    || need_deleted
                    || !self.executor.shall_copy_raw_block(&smallest_key.to_ref())
                {
                    let largest_key = sstable_iter.sstable.meta.largest_key.clone();
                    let target_key = FullKey::decode(&largest_key);
                    sstable_iter.init_block_iter(block, block_meta.uncompressed_size as usize)?;
                    let mut iter = sstable_iter.iter.take().unwrap();
                    self.executor.run(&mut iter, target_key).await?;
                } else {
                    let largest_key = sstable_iter.current_block_largest();
                    let block_len = block.len() as u64;
                    let block_key_count = block_meta.total_key_count;
                    if self
                        .executor
                        .builder
                        .add_raw_block(block, filter_data, smallest_key, largest_key, block_meta)
                        .await?
                    {
                        skip_raw_block_count += 1;
                        skip_raw_block_size += block_len;
                    }
                    self.executor.may_report_process_key(block_key_count);
                    self.executor.clear();
                }
            }
            rest_data.next_sstable().await?;
        }
        let mut total_read_bytes = 0;
        for sst in &self.left.sstables {
            total_read_bytes += sst.sst_size;
        }
        for sst in &self.right.sstables {
            total_read_bytes += sst.sst_size;
        }
        self.metrics
            .compact_fast_runner_bytes
            .inc_by(skip_raw_block_size);
        tracing::info!(
            "OPTIMIZATION: skip {} blocks for task-{}, optimize {}% data compression",
            skip_raw_block_count,
            self.task_id,
            skip_raw_block_size * 100 / total_read_bytes,
        );

        let statistic = self.executor.take_statistics();
        let output_ssts = self.executor.builder.finish().await?;
        Compactor::report_progress(
            self.metrics.clone(),
            Some(self.executor.task_progress.clone()),
            &output_ssts,
            false,
        );
        let sst_infos = output_ssts
            .iter()
            .map(|sst| sst.sst_info.clone())
            .collect_vec();
        assert!(can_concat(&sst_infos));
        Ok((output_ssts, statistic))
    }
}

pub struct CompactTaskExecutor<F: TableBuilderFactory> {
    last_key: FullKey<Vec<u8>>,
    compaction_statistics: CompactionStatistics,
    last_table_id: Option<u32>,
    last_table_stats: TableStats,
    builder: CapacitySplitTableBuilder<F>,
    task_config: TaskConfig,
    task_progress: Arc<TaskProgress>,
    skip_watermark_state: SkipWatermarkState,
    last_key_is_delete: bool,
    progress_key_num: u32,
    non_pk_prefix_skip_watermark_state: NonPkPrefixSkipWatermarkState,
}

impl<F: TableBuilderFactory> CompactTaskExecutor<F> {
    pub fn new(
        builder: CapacitySplitTableBuilder<F>,
        task_config: TaskConfig,
        task_progress: Arc<TaskProgress>,
        skip_watermark_state: SkipWatermarkState,
        non_pk_prefix_skip_watermark_state: NonPkPrefixSkipWatermarkState,
    ) -> Self {
        Self {
            builder,
            task_config,
            last_key: FullKey::default(),
            last_key_is_delete: false,
            compaction_statistics: CompactionStatistics::default(),
            last_table_id: None,
            last_table_stats: TableStats::default(),
            task_progress,
            skip_watermark_state,
            progress_key_num: 0,
            non_pk_prefix_skip_watermark_state,
        }
    }

    fn take_statistics(&mut self) -> CompactionStatistics {
        if let Some(last_table_id) = self.last_table_id.take() {
            self.compaction_statistics
                .delta_drop_stat
                .insert(last_table_id, std::mem::take(&mut self.last_table_stats));
        }
        std::mem::take(&mut self.compaction_statistics)
    }

    fn clear(&mut self) {
        if !self.last_key.is_empty() {
            self.last_key = FullKey::default();
        }
        self.last_key_is_delete = false;
    }

    #[inline(always)]
    fn may_report_process_key(&mut self, key_count: u32) {
        const PROGRESS_KEY_INTERVAL: u32 = 100;
        self.progress_key_num += key_count;
        if self.progress_key_num > PROGRESS_KEY_INTERVAL {
            self.task_progress
                .inc_progress_key(self.progress_key_num as u64);
            self.progress_key_num = 0;
        }
    }

    pub async fn run(
        &mut self,
        iter: &mut BlockIterator,
        target_key: FullKey<&[u8]>,
    ) -> HummockResult<()> {
        self.skip_watermark_state.reset_watermark();
        self.non_pk_prefix_skip_watermark_state.reset_watermark();

        while iter.is_valid() && iter.key().le(&target_key) {
            let is_new_user_key =
                !self.last_key.is_empty() && iter.key().user_key != self.last_key.user_key.as_ref();
            self.compaction_statistics.iter_total_key_counts += 1;
            self.may_report_process_key(1);

            let mut drop = false;
            let value = HummockValue::from_slice(iter.value()).unwrap();
            let is_first_or_new_user_key = is_new_user_key || self.last_key.is_empty();
            if is_first_or_new_user_key {
                self.last_key.set(iter.key());
                self.last_key_is_delete = false;
            }

            // See note in `compactor_runner.rs`.
            if !self.task_config.retain_multiple_version
                && self.task_config.gc_delete_keys
                && value.is_delete()
            {
                drop = true;
                self.last_key_is_delete = true;
            } else if !self.task_config.retain_multiple_version && !is_first_or_new_user_key {
                drop = true;
            }

            if self.watermark_should_delete(&iter.key()) {
                drop = true;
                self.last_key_is_delete = true;
            }

            if self.last_table_id.map_or(true, |last_table_id| {
                last_table_id != self.last_key.user_key.table_id.table_id
            }) {
                if let Some(last_table_id) = self.last_table_id.take() {
                    self.compaction_statistics
                        .delta_drop_stat
                        .insert(last_table_id, std::mem::take(&mut self.last_table_stats));
                }
                self.last_table_id = Some(self.last_key.user_key.table_id.table_id);
            }

            if drop {
                self.compaction_statistics.iter_drop_key_counts += 1;

                let should_count = match self.task_config.stats_target_table_ids.as_ref() {
                    Some(target_table_ids) => {
                        target_table_ids.contains(&self.last_key.user_key.table_id.table_id)
                    }
                    None => true,
                };
                if should_count {
                    self.last_table_stats.total_key_count -= 1;
                    self.last_table_stats.total_key_size -= self.last_key.encoded_len() as i64;
                    self.last_table_stats.total_value_size -= value.encoded_len() as i64;
                }
                iter.next();
                continue;
            }
            self.builder
                .add_full_key(iter.key(), value, is_new_user_key)
                .await?;
            iter.next();
        }
        Ok(())
    }

    pub fn shall_copy_raw_block(&mut self, smallest_key: &FullKey<&[u8]>) -> bool {
        if self.last_key_is_delete && self.last_key.user_key.as_ref().eq(&smallest_key.user_key) {
            // If the last key is delete tombstone, we can not append the origin block
            // because it would cause a deleted key could be see by user again.
            return false;
        }

        if self.watermark_should_delete(smallest_key) {
            return false;
        }

        true
    }

    fn watermark_should_delete(&mut self, key: &FullKey<&[u8]>) -> bool {
        (self.skip_watermark_state.has_watermark() && self.skip_watermark_state.should_delete(key))
            || (self.non_pk_prefix_skip_watermark_state.has_watermark()
                && self.non_pk_prefix_skip_watermark_state.should_delete(key))
    }
}
