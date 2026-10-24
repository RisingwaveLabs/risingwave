// Copyright 2022 RisingWave Labs
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

//! Hummock is the state store of the streaming system.

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKeyRangeRef};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockEpoch, *};

pub mod block_cache;
pub use block_cache::*;

pub mod sstable;
pub use sstable::*;

pub mod compactor;
mod error;
pub mod hummock_meta_client;
pub mod iterator;
pub mod shared_buffer;
pub mod sstable_store;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
pub mod utils;
pub use utils::MemoryLimiter;
pub mod backup_reader;
pub mod event_handler;
pub mod local_version;
pub mod observer_manager;
// The backend is exercised by component tests before production wiring in the final stack layer.
pub(crate) mod pin_cache;
pub(crate) mod pin_cache_refill;
pub(crate) mod refill_locality;
pub mod store;
pub(crate) mod table_cache_metadata;
pub use store::*;
mod validator;
pub mod value;
pub mod write_limiter;

pub mod recent_filter;
pub use recent_filter::*;

pub mod block_stream;
mod time_travel_version_cache;

pub(crate) mod vector;

mod object_id_manager;
pub use error::*;
pub use object_id_manager::*;
pub use risingwave_common::cache::{CacheableEntry, LookupResult, LruCache};
pub use validator::*;
use value::*;

use self::iterator::HummockIterator;
pub use self::sstable_store::*;
use crate::mem_table::ImmutableMemtable;
use crate::monitor::StoreLocalStatistic;
use crate::store::ReadOptions;

pub(in crate::hummock) struct IteratorStatsGuard<'a, TI: HummockIterator> {
    iter: Option<TI>,
    parent_stats: &'a mut StoreLocalStatistic,
}

impl<'a, TI: HummockIterator> IteratorStatsGuard<'a, TI> {
    pub(in crate::hummock) fn new(iter: TI, parent_stats: &'a mut StoreLocalStatistic) -> Self {
        Self {
            iter: Some(iter),
            parent_stats,
        }
    }

    pub(in crate::hummock) fn iter(&self) -> &TI {
        self.iter.as_ref().expect("iterator must be present")
    }

    pub(in crate::hummock) fn iter_mut(&mut self) -> &mut TI {
        self.iter.as_mut().expect("iterator must be present")
    }

    fn collect(&mut self) {
        if let Some(iter) = &self.iter {
            iter.collect_local_statistic(self.parent_stats);
        }
    }

    pub(in crate::hummock) fn handoff(mut self) -> TI {
        self.iter.take().expect("iterator must be present")
    }

    pub(in crate::hummock) fn collect_and_take(mut self) -> TI {
        self.collect();
        self.iter.take().expect("iterator must be present")
    }
}

impl<TI: HummockIterator> Drop for IteratorStatsGuard<'_, TI> {
    fn drop(&mut self) {
        self.collect();
    }
}

pub async fn get_from_sstable_info(
    sstable_store_ref: SstableStoreRef,
    sstable_info: &SstableInfo,
    full_key: FullKey<&[u8]>,
    read_options: &ReadOptions,
    dist_key_hash: Option<u64>,
    local_stats: &mut StoreLocalStatistic,
) -> HummockResult<Option<impl HummockIterator>> {
    let sstable = sstable_store_ref.sstable(sstable_info, local_stats).await?;

    // SST filter key is the distribution key, which does not need to be the prefix of pk, and does not
    // contain `TablePrefix` and `VnodePrefix`.
    if let Some(hash) = dist_key_hash
        && !hit_sstable_filter(
            &sstable,
            &(
                Bound::Included(full_key.user_key),
                Bound::Included(full_key.user_key),
            ),
            hash,
            local_stats,
        )
    {
        return Ok(None);
    }

    let mut iterator_read_options = SstableIteratorReadOptions::from_read_options(read_options);
    iterator_read_options.read_table_id = Some(full_key.user_key.table_id);

    let mut iter = IteratorStatsGuard::new(
        SstableIterator::create(
            sstable,
            sstable_store_ref.clone(),
            Arc::new(iterator_read_options),
            sstable_info,
        ),
        local_stats,
    );
    iter.iter_mut().seek(full_key).await?;
    // Iterator has sought passed the borders.
    if !iter.iter().is_valid() {
        return Ok(None);
    }

    // Iterator gets us the key, we tell if it's the key we want
    // or key next to it.
    let value = if iter.iter().key().user_key == full_key.user_key {
        Some(iter.collect_and_take())
    } else {
        None
    };

    Ok(value)
}

pub fn hit_sstable_filter(
    sstable_ref: &Sstable,
    user_key_range: &UserKeyRangeRef<'_>,
    prefix_hash: u64,
    local_stats: &mut StoreLocalStatistic,
) -> bool {
    local_stats.bloom_filter_check_counts += 1;
    let may_exist = sstable_ref.may_match_hash(user_key_range, prefix_hash);
    if !may_exist {
        local_stats.bloom_filter_true_negative_counts += 1;
    }
    may_exist
}

/// Get `user_value` from `ImmutableMemtable`
pub fn get_from_batch<'a>(
    imm: &'a ImmutableMemtable,
    table_key: TableKey<&[u8]>,
    read_epoch: HummockEpoch,
    read_options: &ReadOptions,
    local_stats: &mut StoreLocalStatistic,
) -> Option<(HummockValue<&'a Bytes>, EpochWithGap)> {
    imm.get(table_key, read_epoch, read_options).inspect(|_| {
        local_stats.get_shared_buffer_hit_counts += 1;
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey};

    use super::{CachePolicy, get_from_sstable_info};
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_info, gen_test_sstable_with_table_ids,
        test_value_of,
    };
    use crate::hummock::value::HummockValue;
    use crate::monitor::StoreLocalStatistic;
    use crate::store::ReadOptions;

    #[tokio::test]
    async fn test_get_collects_stats_when_seek_passes_sst_end() {
        let sstable_store = mock_sstable_store().await;
        let sstable_info = gen_test_sstable_info(
            default_builder_opt_for_test(),
            1,
            (0..10).map(|idx| {
                (
                    iterator_test_key_of(idx),
                    HummockValue::put(format!("value_{idx}").into_bytes()),
                )
            }),
            sstable_store.clone(),
        )
        .await;
        let mut stats = StoreLocalStatistic::default();
        let key = iterator_test_key_of(10);
        let read_options = ReadOptions::default();

        let result = get_from_sstable_info(
            sstable_store,
            &sstable_info,
            key.to_ref(),
            &read_options,
            None,
            &mut stats,
        )
        .await
        .unwrap();

        assert!(result.is_none());
        drop(result);
        assert_eq!(stats.cache_data_block_total, 1);
    }

    #[tokio::test]
    async fn test_point_get_reads_only_requested_table_blocks() {
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 128;

        let test_user_key = |table_id, key: &str| {
            UserKey::new(
                TableId::new(table_id),
                TableKey(Bytes::from(
                    [VirtualNode::ZERO.to_be_bytes().as_slice(), key.as_bytes()].concat(),
                )),
            )
        };
        let test_key = |table_id, key: &str| FullKey {
            user_key: test_user_key(table_id, key),
            epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
        };
        let kv_pairs = (1..=2).flat_map(|table_id| {
            (0..8).map(move |idx| {
                (
                    test_key(table_id, &format!("key_{idx:05}")),
                    HummockValue::put(Bytes::from(test_value_of(idx))),
                )
            })
        });
        let (sstable, sstable_info) = gen_test_sstable_with_table_ids(
            builder_options,
            10,
            kv_pairs,
            sstable_store.clone(),
            vec![1, 2],
        )
        .await;
        let table_2_block_start = sstable
            .meta
            .block_metas
            .partition_point(|block_meta| block_meta.table_id() < TableId::new(2));
        assert!(table_2_block_start > 0);

        // The queried table-2 key sorts before the first table-2 block. Without a point-get table
        // id filter, the SST iterator seeks to the previous table-1 block first.
        let full_key = test_key(2, "key");
        let mut local_stats = StoreLocalStatistic::default();
        let read_options = ReadOptions {
            cache_policy: CachePolicy::Disable,
            ..Default::default()
        };

        {
            let result = get_from_sstable_info(
                sstable_store,
                &sstable_info,
                full_key.to_ref(),
                &read_options,
                None,
                &mut local_stats,
            )
            .await
            .unwrap();
            assert!(result.is_none());
        }
        assert_eq!(local_stats.cache_data_block_total, 1);
    }
}
