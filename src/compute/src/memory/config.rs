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

use foyer::{LfuConfig, LruConfig, S3FifoConfig};
use risingwave_common::config::{
    CacheEvictionConfig, EvictionConfig, StorageConfig, StorageMemoryConfig,
    MAX_BLOCK_CACHE_SHARD_BITS, MAX_META_CACHE_SHARD_BITS, MIN_BUFFER_SIZE_PER_SHARD,
};
use risingwave_common::util::pretty_bytes::convert;

use crate::ComputeNodeOpts;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation
/// overhead, network buffer, etc.) in megabytes.
pub const MIN_SYSTEM_RESERVED_MEMORY_MB: usize = 512;

const RESERVED_MEMORY_LEVELS: [usize; 2] = [16 << 30, usize::MAX];

const RESERVED_MEMORY_PROPORTIONS: [f64; 2] = [0.3, 0.2];

const STORAGE_MEMORY_PROPORTION: f64 = 0.3;

const COMPACTOR_MEMORY_PROPORTION: f64 = 0.1;

const STORAGE_BLOCK_CACHE_MEMORY_PROPORTION: f64 = 0.3;

const STORAGE_SHARED_BUFFER_MAX_MEMORY_MB: usize = 4096;
const STORAGE_META_CACHE_MEMORY_PROPORTION: f64 = 0.35;
const STORAGE_SHARED_BUFFER_MEMORY_PROPORTION: f64 = 0.3;

/// The proportion of compute memory used for batch processing.
const COMPUTE_BATCH_MEMORY_PROPORTION_FOR_STREAMING: f64 = 0.3;
const COMPUTE_BATCH_MEMORY_PROPORTION_FOR_SERVING: f64 = 0.6;

/// Each compute node reserves some memory for stack and code segment of processes, allocation
/// overhead, network buffer, etc. based on gradient reserve memory proportion. The reserve memory
/// size must be larger than `MIN_SYSTEM_RESERVED_MEMORY_MB`
pub fn reserve_memory_bytes(opts: &ComputeNodeOpts) -> (usize, usize) {
    if opts.total_memory_bytes < MIN_COMPUTE_MEMORY_MB << 20 {
        panic!(
            "The total memory size ({}) is too small. It must be at least {} MB.",
            convert(opts.total_memory_bytes as _),
            MIN_COMPUTE_MEMORY_MB
        );
    }

    // If `reserved_memory_bytes` is not set, calculate total_memory_bytes based on gradient reserve memory proportion.
    let reserved = opts
        .reserved_memory_bytes
        .unwrap_or_else(|| gradient_reserve_memory_bytes(opts.total_memory_bytes));

    // Should have at least `MIN_SYSTEM_RESERVED_MEMORY_MB` for reserved memory.
    let reserved = std::cmp::max(reserved, MIN_SYSTEM_RESERVED_MEMORY_MB << 20);

    if reserved >= opts.total_memory_bytes {
        panic!(
            "reserved memory ({}) >= total memory ({}).",
            convert(reserved as _),
            convert(opts.total_memory_bytes as _)
        );
    }

    (reserved, opts.total_memory_bytes - reserved)
}

/// Calculate the reserved memory based on the total memory size.
/// The reserved memory size is calculated based on the following gradient:
/// - 30% of the first 16GB
/// - 20% of the rest
pub fn gradient_reserve_memory_bytes(total_memory_bytes: usize) -> usize {
    let mut total_memory_bytes = total_memory_bytes;
    let mut reserved = 0;
    for i in 0..RESERVED_MEMORY_LEVELS.len() {
        let level_diff = if i == 0 {
            RESERVED_MEMORY_LEVELS[0]
        } else {
            RESERVED_MEMORY_LEVELS[i] - RESERVED_MEMORY_LEVELS[i - 1]
        };
        if total_memory_bytes <= level_diff {
            reserved += (total_memory_bytes as f64 * RESERVED_MEMORY_PROPORTIONS[i]) as usize;
            break;
        } else {
            reserved += (level_diff as f64 * RESERVED_MEMORY_PROPORTIONS[i]) as usize;
            total_memory_bytes -= level_diff;
        }
    }

    reserved
}

/// Decide the memory limit for each storage cache. If not specified in `StorageConfig`, memory
/// limits are calculated based on the proportions to total `non_reserved_memory_bytes`.
pub fn storage_memory_config(
    non_reserved_memory_bytes: usize,
    embedded_compactor_enabled: bool,
    storage_config: &StorageConfig,
    is_serving: bool,
) -> StorageMemoryConfig {
    let (storage_memory_proportion, compactor_memory_proportion) = if embedded_compactor_enabled {
        (STORAGE_MEMORY_PROPORTION, COMPACTOR_MEMORY_PROPORTION)
    } else {
        (STORAGE_MEMORY_PROPORTION + COMPACTOR_MEMORY_PROPORTION, 0.0)
    };

    let storage_memory_bytes = non_reserved_memory_bytes as f64 * storage_memory_proportion;

    // Only if the all cache capacities are specified and their sum doesn't exceed the max allowed storage_memory_bytes, will we use them.
    // Other invalid combination of the cache capacities config will be ignored.
    let (
        config_block_cache_capacity_mb,
        config_meta_cache_capacity_mb,
        config_shared_buffer_capacity_mb,
    ) = match (
        storage_config.cache.block_cache_capacity_mb,
        storage_config.cache.meta_cache_capacity_mb,
        storage_config.shared_buffer_capacity_mb,
    ) {
        (
            Some(block_cache_capacity_mb),
            Some(meta_cache_capacity_mb),
            Some(shared_buffer_capacity_mb),
        ) => {
            let config_storage_memory_bytes =
                (block_cache_capacity_mb + meta_cache_capacity_mb + shared_buffer_capacity_mb)
                    << 20;
            if config_storage_memory_bytes as f64 > storage_memory_bytes {
                tracing::warn!(
                    "config block_cache_capacity_mb {} + meta_cache_capacity_mb {} + shared_buffer_capacity_mb {} = {} exceeds allowed storage_memory_bytes {}. These configs will be ignored.",
                    block_cache_capacity_mb, meta_cache_capacity_mb, shared_buffer_capacity_mb, convert(config_storage_memory_bytes as _), convert(storage_memory_bytes as _)
                );
                (None, None, None)
            } else {
                (
                    Some(block_cache_capacity_mb),
                    Some(meta_cache_capacity_mb),
                    Some(shared_buffer_capacity_mb),
                )
            }
        }
        c => {
            tracing::warn!(
                "config (block_cache_capacity_mb, meta_cache_capacity_mb, shared_buffer_capacity_mb): {:?} should be set altogether. These configs will be ignored.",
                c
            );
            (None, None, None)
        }
    };

    let mut default_block_cache_capacity_mb =
        ((storage_memory_bytes * STORAGE_BLOCK_CACHE_MEMORY_PROPORTION).ceil() as usize) >> 20;
    let default_meta_cache_capacity_mb =
        ((storage_memory_bytes * STORAGE_META_CACHE_MEMORY_PROPORTION).ceil() as usize) >> 20;
    let meta_cache_capacity_mb = config_meta_cache_capacity_mb.unwrap_or(
        // adapt to old version
        storage_config
            .meta_cache_capacity_mb
            .unwrap_or(default_meta_cache_capacity_mb),
    );

    let prefetch_buffer_capacity_mb = storage_config
        .prefetch_buffer_capacity_mb
        .unwrap_or(default_block_cache_capacity_mb);

    if meta_cache_capacity_mb != default_meta_cache_capacity_mb {
        default_block_cache_capacity_mb += default_meta_cache_capacity_mb;
        default_block_cache_capacity_mb =
            default_block_cache_capacity_mb.saturating_sub(meta_cache_capacity_mb);
    }

    let default_shared_buffer_capacity_mb =
        ((storage_memory_bytes * STORAGE_SHARED_BUFFER_MEMORY_PROPORTION).ceil() as usize) >> 20;
    let mut shared_buffer_capacity_mb = config_shared_buffer_capacity_mb.unwrap_or(std::cmp::min(
        default_shared_buffer_capacity_mb,
        STORAGE_SHARED_BUFFER_MAX_MEMORY_MB,
    ));
    if is_serving {
        default_block_cache_capacity_mb += default_shared_buffer_capacity_mb;
        // set 1 to pass internal check
        shared_buffer_capacity_mb = 1;
    } else if shared_buffer_capacity_mb != default_shared_buffer_capacity_mb {
        default_block_cache_capacity_mb += default_shared_buffer_capacity_mb;
        default_block_cache_capacity_mb =
            default_block_cache_capacity_mb.saturating_sub(shared_buffer_capacity_mb);
    }
    let block_cache_capacity_mb = config_block_cache_capacity_mb.unwrap_or(
        // adapt to old version
        storage_config
            .block_cache_capacity_mb
            .unwrap_or(default_block_cache_capacity_mb),
    );

    let compactor_memory_limit_mb = if embedded_compactor_enabled {
        storage_config.compactor_memory_limit_mb.unwrap_or(
            ((non_reserved_memory_bytes as f64 * compactor_memory_proportion).ceil() as usize)
                >> 20,
        )
    } else {
        0
    };

    // The file cache flush buffer threshold is used as a emergency limitation.
    // On most cases the flush buffer is not supposed to be as large as the threshold.
    // So, the file cache flush buffer threshold size is not calculated in the memory usage.
    let block_file_cache_flush_buffer_threshold_mb = storage_config
        .data_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(
            risingwave_common::config::default::storage::block_file_cache_flush_buffer_threshold_mb(
            ),
        );
    let meta_file_cache_flush_buffer_threshold_mb = storage_config
        .meta_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(
            risingwave_common::config::default::storage::meta_file_cache_flush_buffer_threshold_mb(
            ),
        );

    let total_calculated_mb = block_cache_capacity_mb
        + meta_cache_capacity_mb
        + shared_buffer_capacity_mb
        + compactor_memory_limit_mb;
    let soft_limit_mb = (non_reserved_memory_bytes as f64
        * (storage_memory_proportion + compactor_memory_proportion).ceil())
        as usize
        >> 20;
    // + 5 because ceil is used when calculating `total_bytes`.
    if total_calculated_mb > soft_limit_mb + 5 {
        tracing::warn!(
            "The storage memory ({}) exceeds soft limit ({}).",
            convert((total_calculated_mb << 20) as _),
            convert((soft_limit_mb << 20) as _)
        );
    }

    let meta_cache_shard_num = storage_config
        .cache
        .meta_cache_shard_num
        .unwrap_or_else(|| {
            let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
            while (meta_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD
                && shard_bits > 0
            {
                shard_bits -= 1;
            }
            1 << shard_bits
        });
    let block_cache_shard_num = storage_config
        .cache
        .block_cache_shard_num
        .unwrap_or_else(|| {
            let mut shard_bits = MAX_BLOCK_CACHE_SHARD_BITS;
            while (block_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD
                && shard_bits > 0
            {
                shard_bits -= 1;
            }
            1 << shard_bits
        });

    let get_eviction_config = |c: &CacheEvictionConfig| match c {
        CacheEvictionConfig::Lru {
            high_priority_ratio_in_percent,
        } => EvictionConfig::Lru(LruConfig {
            high_priority_pool_ratio: high_priority_ratio_in_percent.unwrap_or(
                // adapt to old version
                storage_config.high_priority_ratio_in_percent.unwrap_or(
                    risingwave_common::config::default::storage::high_priority_ratio_in_percent(),
                ),
            ) as f64
                / 100.0,
        }),
        CacheEvictionConfig::Lfu {
            window_capacity_ratio_in_percent,
            protected_capacity_ratio_in_percent,
            cmsketch_eps,
            cmsketch_confidence,
        } => EvictionConfig::Lfu(LfuConfig {
            window_capacity_ratio: window_capacity_ratio_in_percent.unwrap_or(
                risingwave_common::config::default::storage::window_capacity_ratio_in_percent(),
            ) as f64
                / 100.0,
            protected_capacity_ratio: protected_capacity_ratio_in_percent.unwrap_or(
                risingwave_common::config::default::storage::protected_capacity_ratio_in_percent(),
            ) as f64
                / 100.0,
            cmsketch_eps: cmsketch_eps
                .unwrap_or(risingwave_common::config::default::storage::cmsketch_eps()),
            cmsketch_confidence: cmsketch_confidence
                .unwrap_or(risingwave_common::config::default::storage::cmsketch_confidence()),
        }),
        CacheEvictionConfig::S3Fifo {
            small_queue_capacity_ratio_in_percent,
            ghost_queue_capacity_ratio_in_percent,
            small_to_main_freq_threshold,
        } => EvictionConfig::S3Fifo(S3FifoConfig {
            small_queue_capacity_ratio: small_queue_capacity_ratio_in_percent.unwrap_or(
                risingwave_common::config::default::storage::small_queue_capacity_ratio_in_percent(
                ),
            ) as f64
                / 100.0,
            ghost_queue_capacity_ratio: ghost_queue_capacity_ratio_in_percent.unwrap_or(
                risingwave_common::config::default::storage::ghost_queue_capacity_ratio_in_percent(
                ),
            ) as f64
                / 100.0,
            small_to_main_freq_threshold: small_to_main_freq_threshold.unwrap_or(
                risingwave_common::config::default::storage::small_to_main_freq_threshold(),
            ),
        }),
    };

    let block_cache_eviction_config =
        get_eviction_config(&storage_config.cache.block_cache_eviction);
    let meta_cache_eviction_config = get_eviction_config(&storage_config.cache.meta_cache_eviction);

    StorageMemoryConfig {
        block_cache_capacity_mb,
        block_cache_shard_num,
        meta_cache_capacity_mb,
        meta_cache_shard_num,
        shared_buffer_capacity_mb,
        compactor_memory_limit_mb: if embedded_compactor_enabled {
            compactor_memory_limit_mb
        } else {
            0 // unused
        },
        prefetch_buffer_capacity_mb,
        block_cache_eviction_config,
        meta_cache_eviction_config,
        block_file_cache_flush_buffer_threshold_mb,
        meta_file_cache_flush_buffer_threshold_mb,
    }
}

pub fn batch_mem_limit(compute_memory_bytes: usize, is_serving_node: bool) -> u64 {
    if is_serving_node {
        (compute_memory_bytes as f64 * COMPUTE_BATCH_MEMORY_PROPORTION_FOR_SERVING) as u64
    } else {
        (compute_memory_bytes as f64 * COMPUTE_BATCH_MEMORY_PROPORTION_FOR_STREAMING) as u64
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use risingwave_common::config::StorageConfig;

    use super::{reserve_memory_bytes, storage_memory_config};
    use crate::ComputeNodeOpts;

    #[test]
    fn test_reserve_memory_bytes() {
        // at least 512 MB
        {
            let mut opts = ComputeNodeOpts::parse_from(vec![] as Vec<String>);
            opts.total_memory_bytes = 1536 << 20;
            let (reserved, non_reserved) = reserve_memory_bytes(&opts);
            assert_eq!(reserved, 512 << 20);
            assert_eq!(non_reserved, 1024 << 20);
        }

        // reserve based on proportion
        {
            let mut opts = ComputeNodeOpts::parse_from(vec![] as Vec<String>);
            opts.total_memory_bytes = 10 << 30;
            let (reserved, non_reserved) = reserve_memory_bytes(&opts);
            assert_eq!(reserved, 3 << 30);
            assert_eq!(non_reserved, 7 << 30);
        }

        // reserve based on opts
        {
            let mut opts = ComputeNodeOpts::parse_from(vec![] as Vec<String>);
            opts.total_memory_bytes = 10 << 30;
            opts.reserved_memory_bytes = Some(2 << 30);
            let (reserved, non_reserved) = reserve_memory_bytes(&opts);
            assert_eq!(reserved, 2 << 30);
            assert_eq!(non_reserved, 8 << 30);
        }
    }

    #[test]
    fn test_storage_memory_config() {
        let mut storage_config = StorageConfig::default();

        let total_non_reserved_memory_bytes = 8 << 30;

        // Embedded compactor enabled, streaming node
        let memory_config = storage_memory_config(
            total_non_reserved_memory_bytes,
            true,
            &storage_config,
            false,
        );
        assert_eq!(memory_config.block_cache_capacity_mb, 737);
        assert_eq!(memory_config.meta_cache_capacity_mb, 860);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 737);
        assert_eq!(memory_config.compactor_memory_limit_mb, 819);

        // Embedded compactor disabled, serving node
        let memory_config = storage_memory_config(
            total_non_reserved_memory_bytes,
            false,
            &storage_config,
            true,
        );
        assert_eq!(memory_config.block_cache_capacity_mb, 1966);
        assert_eq!(memory_config.meta_cache_capacity_mb, 1146);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 1);
        assert_eq!(memory_config.compactor_memory_limit_mb, 0);

        // Embedded compactor enabled, streaming node, file cache
        storage_config.data_file_cache.dir = "data".to_owned();
        storage_config.meta_file_cache.dir = "meta".to_owned();
        let memory_config = storage_memory_config(
            total_non_reserved_memory_bytes,
            true,
            &storage_config,
            false,
        );
        assert_eq!(memory_config.block_cache_capacity_mb, 737);
        assert_eq!(memory_config.meta_cache_capacity_mb, 860);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 737);
        assert_eq!(memory_config.compactor_memory_limit_mb, 819);

        // Embedded compactor enabled, streaming node, file cache, cache capacities specified
        storage_config.cache.block_cache_capacity_mb = Some(512);
        storage_config.cache.meta_cache_capacity_mb = Some(128);
        storage_config.shared_buffer_capacity_mb = Some(1024);
        storage_config.compactor_memory_limit_mb = Some(512);
        let memory_config = storage_memory_config(
            total_non_reserved_memory_bytes,
            true,
            &storage_config,
            false,
        );
        assert_eq!(memory_config.block_cache_capacity_mb, 512);
        assert_eq!(memory_config.meta_cache_capacity_mb, 128);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 1024);
        assert_eq!(memory_config.compactor_memory_limit_mb, 512);
    }

    #[test]
    fn test_gradient_reserve_memory_bytes() {
        assert_eq!(super::gradient_reserve_memory_bytes(4 << 30), 1288490188);
        assert_eq!(super::gradient_reserve_memory_bytes(8 << 30), 2576980377);
        assert_eq!(super::gradient_reserve_memory_bytes(16 << 30), 5153960755);
        assert_eq!(super::gradient_reserve_memory_bytes(24 << 30), 6871947673);
        assert_eq!(super::gradient_reserve_memory_bytes(32 << 30), 8589934591);
        assert_eq!(super::gradient_reserve_memory_bytes(54 << 30), 13314398617);
        assert_eq!(super::gradient_reserve_memory_bytes(64 << 30), 15461882265);
        assert_eq!(super::gradient_reserve_memory_bytes(100 << 30), 23192823398);
        assert_eq!(super::gradient_reserve_memory_bytes(128 << 30), 29205777612);
    }
}
