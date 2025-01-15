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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::{Bytes, BytesMut};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{user_key, FullKey, MAX_KEY_LEN};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::table_stats::{TableStats, TableStatsMap};
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::BloomFilterType;

use super::utils::CompressionAlgorithm;
use super::{
    BlockBuilder, BlockBuilderOptions, BlockMeta, SstableMeta, SstableWriter, DEFAULT_BLOCK_SIZE,
    DEFAULT_ENTRY_SIZE, DEFAULT_RESTART_INTERVAL, VERSION,
};
use crate::compaction_catalog_manager::{
    CompactionCatalogAgent, CompactionCatalogAgentRef, FilterKeyExtractorImpl,
    FullKeyFilterKeyExtractor,
};
use crate::hummock::sstable::{utils, FilterBuilder};
use crate::hummock::value::HummockValue;
use crate::hummock::{
    Block, BlockHolder, BlockIterator, HummockResult, MemoryLimiter, Xor16FilterBuilder,
};
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

pub const DEFAULT_SSTABLE_SIZE: usize = 4 * 1024 * 1024;
pub const DEFAULT_BLOOM_FALSE_POSITIVE: f64 = 0.001;
pub const DEFAULT_MAX_SST_SIZE: u64 = 512 * 1024 * 1024;
pub const MIN_BLOCK_SIZE: usize = 8 * 1024;

#[derive(Clone, Debug)]
pub struct SstableBuilderOptions {
    /// Approximate sstable capacity.
    pub capacity: usize,
    /// Approximate block capacity.
    pub block_capacity: usize,
    /// Restart point interval.
    pub restart_interval: usize,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// Compression algorithm.
    pub compression_algorithm: CompressionAlgorithm,
    pub max_sst_size: u64,
}

impl From<&StorageOpts> for SstableBuilderOptions {
    fn from(options: &StorageOpts) -> SstableBuilderOptions {
        let capacity: usize = (options.sstable_size_mb as usize) * (1 << 20);
        SstableBuilderOptions {
            capacity,
            block_capacity: (options.block_size_kb as usize) * (1 << 10),
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: options.compactor_max_sst_size,
        }
    }
}

impl Default for SstableBuilderOptions {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SSTABLE_SIZE,
            block_capacity: DEFAULT_BLOCK_SIZE,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: DEFAULT_BLOOM_FALSE_POSITIVE,
            compression_algorithm: CompressionAlgorithm::None,
            max_sst_size: DEFAULT_MAX_SST_SIZE,
        }
    }
}

pub struct SstableBuilderOutput<WO> {
    pub sst_info: LocalSstableInfo,
    pub writer_output: WO,
    pub stats: SstableBuilderOutputStats,
}

pub struct SstableBuilder<W: SstableWriter, F: FilterBuilder> {
    /// Options.
    options: SstableBuilderOptions,
    /// Data writer.
    writer: W,
    /// Current block builder.
    block_builder: BlockBuilder,

    compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    /// Block metadata vec.
    block_metas: Vec<BlockMeta>,

    /// `table_id` of added keys.
    table_ids: BTreeSet<u32>,
    last_full_key: Vec<u8>,
    /// Buffer for encoded key and value to avoid allocation.
    raw_key: BytesMut,
    raw_value: BytesMut,
    last_table_id: Option<u32>,
    sstable_id: u64,

    /// Per table stats.
    table_stats: TableStatsMap,
    /// `last_table_stats` accumulates stats for `last_table_id` and finalizes it in `table_stats`
    /// by `finalize_last_table_stats`
    last_table_stats: TableStats,

    filter_builder: F,

    epoch_set: BTreeSet<u64>,
    memory_limiter: Option<Arc<MemoryLimiter>>,

    block_size_vec: Vec<usize>, // for statistics
}

impl<W: SstableWriter> SstableBuilder<W, Xor16FilterBuilder> {
    pub fn for_test(
        sstable_id: u64,
        writer: W,
        options: SstableBuilderOptions,
        table_id_to_vnode: HashMap<StateTableId, usize>,
        table_id_to_watermark_serde: HashMap<
            u32,
            Option<(OrderedRowSerde, OrderedRowSerde, usize)>,
        >,
    ) -> Self {
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        ));

        Self::new(
            sstable_id,
            writer,
            Xor16FilterBuilder::new(options.capacity / DEFAULT_ENTRY_SIZE + 1),
            options,
            compaction_catalog_agent_ref,
            None,
        )
    }
}

impl<W: SstableWriter, F: FilterBuilder> SstableBuilder<W, F> {
    pub fn new(
        sstable_id: u64,
        writer: W,
        filter_builder: F,
        options: SstableBuilderOptions,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
        memory_limiter: Option<Arc<MemoryLimiter>>,
    ) -> Self {
        Self {
            options: options.clone(),
            writer,
            block_builder: BlockBuilder::new(BlockBuilderOptions {
                capacity: options.block_capacity,
                restart_interval: options.restart_interval,
                compression_algorithm: options.compression_algorithm,
            }),
            filter_builder,
            block_metas: Vec::with_capacity(options.capacity / options.block_capacity + 1),
            table_ids: BTreeSet::new(),
            last_table_id: None,
            raw_key: BytesMut::new(),
            raw_value: BytesMut::new(),
            last_full_key: vec![],
            sstable_id,
            compaction_catalog_agent_ref,
            table_stats: Default::default(),
            last_table_stats: Default::default(),
            epoch_set: BTreeSet::default(),
            memory_limiter,
            block_size_vec: Vec::new(),
        }
    }

    /// Add kv pair to sstable.
    pub async fn add_for_test(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add(full_key, value).await
    }

    pub fn current_block_size(&self) -> usize {
        self.block_builder.approximate_len()
    }

    /// Add raw data of block to sstable. return false means fallback
    pub async fn add_raw_block(
        &mut self,
        buf: Bytes,
        filter_data: Vec<u8>,
        smallest_key: FullKey<Vec<u8>>,
        largest_key: Vec<u8>,
        mut meta: BlockMeta,
    ) -> HummockResult<bool> {
        let table_id = smallest_key.user_key.table_id.table_id;
        if self.last_table_id.is_none() || self.last_table_id.unwrap() != table_id {
            if !self.block_builder.is_empty() {
                // Try to finish the previous `Block`` when the `table_id` is switched, making sure that the data in the `Block` doesn't span two `table_ids`.
                self.build_block().await?;
            }

            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
        }

        if !self.block_builder.is_empty() {
            let min_block_size = std::cmp::min(MIN_BLOCK_SIZE, self.options.block_capacity / 4);

            // If the previous block is too small, we should merge it into the previous block.
            if self.block_builder.approximate_len() < min_block_size {
                let block = Block::decode(buf, meta.uncompressed_size as usize)?;
                let mut iter = BlockIterator::new(BlockHolder::from_owned_block(Box::new(block)));
                iter.seek_to_first();
                while iter.is_valid() {
                    let value = HummockValue::from_slice(iter.value()).unwrap_or_else(|_| {
                        panic!(
                            "decode failed for fast compact sst_id {} block_idx {} last_table_id {:?}",
                            self.sstable_id, self.block_metas.len(), self.last_table_id
                        )
                    });
                    self.add_impl(iter.key(), value, false).await?;
                    iter.next();
                }
                return Ok(false);
            }

            self.build_block().await?;
        }
        self.last_full_key = largest_key;
        assert_eq!(
            meta.len as usize,
            buf.len(),
            "meta {} buf {} last_table_id {:?}",
            meta.len,
            buf.len(),
            self.last_table_id
        );
        meta.offset = self.writer.data_len() as u32;
        self.block_metas.push(meta);
        self.filter_builder.add_raw_data(filter_data);
        let block_meta = self.block_metas.last_mut().unwrap();
        self.writer.write_block_bytes(buf, block_meta).await?;

        Ok(true)
    }

    /// Add kv pair to sstable.
    pub async fn add(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
    ) -> HummockResult<()> {
        self.add_impl(full_key, value, true).await
    }

    /// Add kv pair to sstable.
    async fn add_impl(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        could_switch_block: bool,
    ) -> HummockResult<()> {
        const LARGE_KEY_LEN: usize = MAX_KEY_LEN >> 1;

        let table_key_len = full_key.user_key.table_key.as_ref().len();
        let table_value_len = match &value {
            HummockValue::Put(t) => t.len(),
            HummockValue::Delete => 0,
        };
        let large_value_len = self.options.max_sst_size as usize / 10;
        let large_key_value_len = self.options.max_sst_size as usize / 2;
        if table_key_len >= LARGE_KEY_LEN
            || table_value_len > large_value_len
            || table_key_len + table_value_len > large_key_value_len
        {
            let table_id = full_key.user_key.table_id.table_id();
            tracing::warn!(
                "A large key/value (table_id={}, key len={}, value len={}, epoch={}, spill offset={}) is added to block",
                table_id,
                table_key_len,
                table_value_len,
                full_key.epoch_with_gap.pure_epoch(),
                full_key.epoch_with_gap.offset(),
            );
        }

        // TODO: refine me
        full_key.encode_into(&mut self.raw_key);
        value.encode(&mut self.raw_value);
        let is_new_user_key = self.last_full_key.is_empty()
            || !user_key(&self.raw_key).eq(user_key(&self.last_full_key));
        let table_id = full_key.user_key.table_id.table_id();
        let is_new_table = self.last_table_id.is_none() || self.last_table_id.unwrap() != table_id;
        let current_block_size = self.current_block_size();
        let is_block_full = current_block_size >= self.options.block_capacity
            || (current_block_size > self.options.block_capacity / 4 * 3
                && current_block_size + self.raw_value.len() + self.raw_key.len()
                    > self.options.block_capacity);

        if is_new_table {
            assert!(
                could_switch_block,
                "is_new_user_key {} sst_id {} block_idx {} table_id {} last_table_id {:?} full_key {:?}",
                is_new_user_key, self.sstable_id, self.block_metas.len(), table_id, self.last_table_id, full_key
            );
            self.table_ids.insert(table_id);
            self.finalize_last_table_stats();
            self.last_table_id = Some(table_id);
            if !self.block_builder.is_empty() {
                self.build_block().await?;
            }
        } else if is_block_full && could_switch_block {
            self.build_block().await?;
        }
        self.last_table_stats.total_key_count += 1;
        self.epoch_set.insert(full_key.epoch_with_gap.pure_epoch());

        // Rotate block builder if the previous one has been built.
        if self.block_builder.is_empty() {
            self.block_metas.push(BlockMeta {
                offset: utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
                    panic!(
                        "WARN overflow can't convert writer_data_len {} into u32 sst_id {} block_idx {} tables {:?}",
                        self.writer.data_len(),
                        self.sstable_id,
                        self.block_metas.len(),
                        self.table_ids,
                    )
                }),
                len: 0,
                smallest_key: full_key.encode(),
                uncompressed_size: 0,
                total_key_count: 0,
                stale_key_count: 0,
            });
        }

        let table_id = full_key.user_key.table_id.table_id();
        let mut extract_key = user_key(&self.raw_key);
        extract_key = self.compaction_catalog_agent_ref.extract(extract_key);
        // add bloom_filter check
        if !extract_key.is_empty() {
            self.filter_builder.add_key(extract_key, table_id);
        }
        self.block_builder.add(full_key, self.raw_value.as_ref());
        self.block_metas.last_mut().unwrap().total_key_count += 1;
        if !is_new_user_key || value.is_delete() {
            self.block_metas.last_mut().unwrap().stale_key_count += 1;
        }
        self.last_table_stats.total_key_size += full_key.encoded_len() as i64;
        self.last_table_stats.total_value_size += value.encoded_len() as i64;

        self.last_full_key.clear();
        self.last_full_key.extend_from_slice(&self.raw_key);

        self.raw_key.clear();
        self.raw_value.clear();
        Ok(())
    }

    /// Finish building sst.
    ///
    /// # Format
    ///
    /// data:
    ///
    /// ```plain
    /// | Block 0 | ... | Block N-1 | N (4B) |
    /// ```
    pub async fn finish(mut self) -> HummockResult<SstableBuilderOutput<W::Output>> {
        let smallest_key = if self.block_metas.is_empty() {
            vec![]
        } else {
            self.block_metas[0].smallest_key.clone()
        };
        let largest_key = self.last_full_key.clone();
        self.finalize_last_table_stats();

        self.build_block().await?;
        let right_exclusive = false;
        let meta_offset = self.writer.data_len() as u64;

        let bloom_filter_kind = if self.filter_builder.support_blocked_raw_data() {
            BloomFilterType::Blocked
        } else {
            BloomFilterType::Sstable
        };
        let bloom_filter = if self.options.bloom_false_positive > 0.0 {
            self.filter_builder.finish(self.memory_limiter.clone())
        } else {
            vec![]
        };

        let total_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.total_key_count as u64)
            .sum::<u64>();
        let stale_key_count = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.stale_key_count as u64)
            .sum::<u64>();
        let uncompressed_file_size = self
            .block_metas
            .iter()
            .map(|block_meta| block_meta.uncompressed_size as u64)
            .sum::<u64>();

        #[expect(deprecated)]
        let mut meta = SstableMeta {
            block_metas: self.block_metas,
            bloom_filter,
            estimated_size: 0,
            key_count: utils::checked_into_u32(total_key_count).unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert total_key_count {} into u32 tables {:?}",
                    total_key_count, self.table_ids,
                )
            }),
            smallest_key,
            largest_key,
            version: VERSION,
            meta_offset,
            monotonic_tombstone_events: vec![],
        };

        let meta_encode_size = meta.encoded_size();
        let encoded_size_u32 = utils::checked_into_u32(meta_encode_size).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert meta_encoded_size {} into u32 tables {:?}",
                meta_encode_size, self.table_ids,
            )
        });
        let meta_offset_u32 = utils::checked_into_u32(meta_offset).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert meta_offset {} into u32 tables {:?}",
                meta_offset, self.table_ids,
            )
        });
        meta.estimated_size = encoded_size_u32
            .checked_add(meta_offset_u32)
            .unwrap_or_else(|| {
                panic!(
                    "WARN overflow encoded_size_u32 {} meta_offset_u32 {} table_id {:?} table_ids {:?}",
                    encoded_size_u32, meta_offset_u32, self.last_table_id, self.table_ids
                )
            });

        let (avg_key_size, avg_value_size) = if self.table_stats.is_empty() {
            (0, 0)
        } else {
            let total_key_count: usize = self
                .table_stats
                .values()
                .map(|s| s.total_key_count as usize)
                .sum();

            if total_key_count == 0 {
                (0, 0)
            } else {
                let total_key_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_key_size as usize)
                    .sum();

                let total_value_size: usize = self
                    .table_stats
                    .values()
                    .map(|s| s.total_value_size as usize)
                    .sum();

                (
                    total_key_size / total_key_count,
                    total_value_size / total_key_count,
                )
            }
        };

        let (min_epoch, max_epoch) = {
            if self.epoch_set.is_empty() {
                (HummockEpoch::MAX, u64::MIN)
            } else {
                (
                    *self.epoch_set.first().unwrap(),
                    *self.epoch_set.last().unwrap(),
                )
            }
        };

        let sst_info: SstableInfo = SstableInfoInner {
            object_id: self.sstable_id,
            sst_id: self.sstable_id,
            bloom_filter_kind,
            key_range: KeyRange {
                left: Bytes::from(meta.smallest_key.clone()),
                right: Bytes::from(meta.largest_key.clone()),
                right_exclusive,
            },
            file_size: meta.estimated_size as u64,
            table_ids: self.table_ids.into_iter().collect(),
            meta_offset: meta.meta_offset,
            stale_key_count,
            total_key_count,
            uncompressed_file_size: uncompressed_file_size + meta.encoded_size() as u64,
            min_epoch,
            max_epoch,
            range_tombstone_count: 0,
            sst_size: meta.estimated_size as u64,
        }
        .into();
        tracing::trace!(
            "meta_size {} bloom_filter_size {}  add_key_counts {} stale_key_count {} min_epoch {} max_epoch {} epoch_count {}",
            meta.encoded_size(),
            meta.bloom_filter.len(),
            total_key_count,
            stale_key_count,
            min_epoch,
            max_epoch,
            self.epoch_set.len()
        );
        let bloom_filter_size = meta.bloom_filter.len();
        let sstable_file_size = sst_info.file_size as usize;

        if !meta.block_metas.is_empty() {
            // fill total_compressed_size
            let mut last_table_id = meta.block_metas[0].table_id().table_id();
            let mut last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
            for block_meta in &meta.block_metas {
                let block_table_id = block_meta.table_id();
                if last_table_id != block_table_id.table_id() {
                    last_table_id = block_table_id.table_id();
                    last_table_stats = self.table_stats.get_mut(&last_table_id).unwrap();
                }

                last_table_stats.total_compressed_size += block_meta.len as u64;
            }
        }

        let writer_output = self.writer.finish(meta).await?;
        // The timestamp is only used during full GC.
        //
        // Ideally object store object's last_modified should be used.
        // However, it'll incur additional IO overhead since S3 lacks an interface to retrieve the last_modified timestamp after the PUT operation on an object.
        //
        // The local timestamp below is expected to precede the last_modified of object store object, given that the object store object is created afterward.
        // It should help alleviate the clock drift issue.

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        Ok(SstableBuilderOutput::<W::Output> {
            sst_info: LocalSstableInfo::new(sst_info, self.table_stats, now),
            writer_output,
            stats: SstableBuilderOutputStats {
                bloom_filter_size,
                avg_key_size,
                avg_value_size,
                epoch_count: self.epoch_set.len(),
                block_size_vec: self.block_size_vec,
                sstable_file_size,
            },
        })
    }

    pub fn approximate_len(&self) -> usize {
        self.writer.data_len()
            + self.block_builder.approximate_len()
            + self.filter_builder.approximate_len()
    }

    pub async fn build_block(&mut self) -> HummockResult<()> {
        // Skip empty block.
        if self.block_builder.is_empty() {
            return Ok(());
        }

        let block_meta = self.block_metas.last_mut().unwrap();
        let uncompressed_block_size = self.block_builder.uncompressed_block_size();
        block_meta.uncompressed_size = utils::checked_into_u32(uncompressed_block_size)
            .unwrap_or_else(|_| {
                panic!(
                    "WARN overflow can't convert uncompressed_block_size {} into u32 table {:?}",
                    uncompressed_block_size,
                    self.block_builder.table_id(),
                )
            });
        let block = self.block_builder.build();
        self.writer.write_block(block, block_meta).await?;
        self.block_size_vec.push(block.len());
        self.filter_builder
            .switch_block(self.memory_limiter.clone());
        let data_len = utils::checked_into_u32(self.writer.data_len()).unwrap_or_else(|_| {
            panic!(
                "WARN overflow can't convert writer_data_len {} into u32 table {:?}",
                self.writer.data_len(),
                self.block_builder.table_id(),
            )
        });
        block_meta.len = data_len.checked_sub(block_meta.offset).unwrap_or_else(|| {
            panic!(
                "data_len should >= meta_offset, found data_len={}, meta_offset={}",
                data_len, block_meta.offset
            )
        });

        if data_len as usize > self.options.capacity * 2 {
            tracing::warn!(
                "WARN unexpected block size {} table {:?}",
                data_len,
                self.block_builder.table_id()
            );
        }

        self.block_builder.clear();
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.writer.data_len() > 0
    }

    /// Returns true if we roughly reached capacity
    pub fn reach_capacity(&self) -> bool {
        self.approximate_len() >= self.options.capacity
    }

    fn finalize_last_table_stats(&mut self) {
        if self.table_ids.is_empty() || self.last_table_id.is_none() {
            return;
        }
        self.table_stats.insert(
            self.last_table_id.unwrap(),
            std::mem::take(&mut self.last_table_stats),
        );
    }
}

pub struct SstableBuilderOutputStats {
    bloom_filter_size: usize,
    avg_key_size: usize,
    avg_value_size: usize,
    epoch_count: usize,
    block_size_vec: Vec<usize>, // for statistics
    sstable_file_size: usize,
}

impl SstableBuilderOutputStats {
    pub fn report_stats(&self, metrics: &Arc<CompactorMetrics>) {
        if self.bloom_filter_size != 0 {
            metrics
                .sstable_bloom_filter_size
                .observe(self.bloom_filter_size as _);
        }

        if self.sstable_file_size != 0 {
            metrics
                .sstable_file_size
                .observe(self.sstable_file_size as _);
        }

        if self.avg_key_size != 0 {
            metrics.sstable_avg_key_size.observe(self.avg_key_size as _);
        }

        if self.avg_value_size != 0 {
            metrics
                .sstable_avg_value_size
                .observe(self.avg_value_size as _);
        }

        if self.epoch_count != 0 {
            metrics
                .sstable_distinct_epoch_count
                .observe(self.epoch_count as _);
        }

        if !self.block_size_vec.is_empty() {
            for block_size in &self.block_size_vec {
                metrics.sstable_block_size.observe(*block_size as _);
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::collections::{Bound, HashMap};

    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::UserKey;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::compaction_catalog_manager::{
        CompactionCatalogAgent, DummyFilterKeyExtractor, MultiFilterKeyExtractor,
    };
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::xor_filter::BlockedXor16FilterBuilder;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_impl, mock_sst_writer, test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::{CachePolicy, Sstable, SstableWriterOptions, Xor8FilterBuilder};
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    async fn test_empty() {
        let opt = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: 0.001,
            ..Default::default()
        };

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        b.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_basic() {
        let opt = default_builder_opt_for_test();

        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let mut b = SstableBuilder::for_test(
            0,
            mock_sst_writer(&opt),
            opt,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        );

        for i in 0..TEST_KEYS_COUNT {
            b.add_for_test(
                test_key_of(i).to_ref(),
                HummockValue::put(&test_value_of(i)),
            )
            .await
            .unwrap();
        }

        let output = b.finish().await.unwrap();
        let info = output.sst_info.sst_info;

        assert_bytes_eq!(test_key_of(0).encode(), info.key_range.left);
        assert_bytes_eq!(
            test_key_of(TEST_KEYS_COUNT - 1).encode(),
            info.key_range.right
        );
        let (data, meta) = output.writer_output;
        assert_eq!(info.file_size, meta.estimated_size as u64);
        let offset = info.meta_offset as usize;
        let meta2 = SstableMeta::decode(&data[offset..]).unwrap();
        assert_eq!(meta2, meta);
    }

    async fn test_with_bloom_filter<F: FilterBuilder>(with_blooms: bool) {
        let key_count = 1000;

        let opts = SstableBuilderOptions {
            capacity: 0,
            block_capacity: 4096,
            restart_interval: 16,
            bloom_false_positive: if with_blooms { 0.01 } else { 0.0 },
            ..Default::default()
        };

        // build remote table
        let sstable_store = mock_sstable_store().await;
        let table_id_to_vnode = HashMap::from_iter(vec![(0, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let sst_info = gen_test_sstable_impl::<Vec<u8>, F>(
            opts,
            0,
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
            CachePolicy::NotFill,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )
        .await;
        let table = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();

        assert_eq!(table.has_bloom_filter(), with_blooms);
        for i in 0..key_count {
            let full_key = test_key_of(i);
            if table.has_bloom_filter() {
                let hash = Sstable::hash_for_bloom_filter(full_key.user_key.encode().as_slice(), 0);
                let key_ref = full_key.user_key.as_ref();
                assert!(
                    table.may_match_hash(
                        &(Bound::Included(key_ref), Bound::Included(key_ref)),
                        hash
                    ),
                    "failed at {}",
                    i
                );
            }
        }
    }

    #[tokio::test]
    async fn test_bloom_filter() {
        test_with_bloom_filter::<Xor16FilterBuilder>(false).await;
        test_with_bloom_filter::<Xor16FilterBuilder>(true).await;
        test_with_bloom_filter::<Xor8FilterBuilder>(true).await;
        test_with_bloom_filter::<BlockedXor16FilterBuilder>(true).await;
    }

    #[tokio::test]
    async fn test_no_bloom_filter_block() {
        let opts = SstableBuilderOptions::default();
        // build remote table
        let sstable_store = mock_sstable_store().await;
        let writer_opts = SstableWriterOptions::default();
        let object_id = 1;
        let writer = sstable_store
            .clone()
            .create_sst_writer(object_id, writer_opts);
        let mut filter = MultiFilterKeyExtractor::default();
        filter.register(1, FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor));
        filter.register(
            2,
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor),
        );
        filter.register(3, FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor));

        let table_id_to_vnode = HashMap::from_iter(vec![
            (1, VirtualNode::COUNT_FOR_TEST),
            (2, VirtualNode::COUNT_FOR_TEST),
            (3, VirtualNode::COUNT_FOR_TEST),
        ]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(1, None), (2, None), (3, None)]);

        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(filter),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        ));

        let mut builder = SstableBuilder::new(
            object_id,
            writer,
            BlockedXor16FilterBuilder::new(1024),
            opts,
            compaction_catalog_agent_ref,
            None,
        );

        let key_count: usize = 10000;
        for table_id in 1..4 {
            let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
            for idx in 0..key_count {
                table_key.resize(VirtualNode::SIZE, 0);
                table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
                let k = UserKey::for_test(TableId::new(table_id), table_key.as_ref());
                let v = test_value_of(idx);
                builder
                    .add(
                        FullKey::from_user_key(k, test_epoch(1)),
                        HummockValue::put(v.as_ref()),
                    )
                    .await
                    .unwrap();
            }
        }
        let ret = builder.finish().await.unwrap();
        let sst_info = ret.sst_info.sst_info.clone();
        ret.writer_output.await.unwrap().unwrap();
        let table = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
        for idx in 0..key_count {
            table_key.resize(VirtualNode::SIZE, 0);
            table_key.extend_from_slice(format!("key_test_{:05}", idx * 2).as_bytes());
            let k = UserKey::for_test(TableId::new(2), table_key.as_slice());
            let hash = Sstable::hash_for_bloom_filter(&k.encode(), 2);
            let key_ref = k.as_ref();
            assert!(
                table.may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            );
        }
    }
}
