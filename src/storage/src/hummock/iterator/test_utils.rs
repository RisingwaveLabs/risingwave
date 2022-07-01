// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::iter::Iterator;
use std::sync::Arc;

use futures::executor::block_on;
use itertools::Itertools;
use risingwave_hummock_sdk::key::{key_with_epoch, Epoch};
use risingwave_hummock_sdk::HummockSSTableId;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
};

use crate::hummock::iterator::{BoxedForwardHummockIterator, ReadOptions};
use crate::hummock::sstable_store::SstableStore;
pub use crate::hummock::test_utils::default_builder_opt_for_test;
use crate::hummock::test_utils::{create_small_table_cache, gen_test_sstable};
use crate::hummock::{
    HummockValue, SSTableBuilderOptions, SSTableIterator, SSTableIteratorType, Sstable,
    SstableStoreRef,
};
use crate::monitor::ObjectStoreMetrics;

/// `assert_eq` two `Vec<u8>` with human-readable format.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {{
        use bytes::Bytes;
        assert_eq!(
            Bytes::copy_from_slice(&$left),
            Bytes::copy_from_slice(&$right)
        )
    }};
}

pub const TEST_KEYS_COUNT: usize = 10;

pub fn mock_sstable_store() -> SstableStoreRef {
    mock_sstable_store_with_object_store(Arc::new(ObjectStoreImpl::Hybrid {
        local: Box::new(ObjectStoreImpl::InMem(
            InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
        )),
        remote: Box::new(ObjectStoreImpl::InMem(
            InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
        )),
    }))
}

pub fn mock_sstable_store_with_object_store(store: ObjectStoreRef) -> SstableStoreRef {
    let path = "test".to_string();
    Arc::new(SstableStore::new(store, path, 64 << 20, 64 << 20))
}

/// Generates keys like `key_test_00002` with epoch 233.
pub fn iterator_test_key_of(idx: usize) -> Vec<u8> {
    key_with_epoch(format!("key_test_{:05}", idx).as_bytes().to_vec(), 233)
}

/// Generates keys like `key_test_00002` with epoch `epoch` .
pub fn iterator_test_key_of_epoch(idx: usize, epoch: Epoch) -> Vec<u8> {
    key_with_epoch(format!("key_test_{:05}", idx).as_bytes().to_vec(), epoch)
}

/// The value of an index, like `value_test_00002` without value meta
pub fn iterator_test_value_of(idx: usize) -> Vec<u8> {
    format!("value_test_{:05}", idx).as_bytes().to_vec()
}

/// Generates a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_iterator_test_sstable_base(
    sst_id: HummockSSTableId,
    opts: SSTableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
) -> Sstable {
    gen_test_sstable(
        opts,
        sst_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of(idx_mapping(i)),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}

// key=[idx, epoch], value
pub async fn gen_iterator_test_sstable_from_kv_pair(
    sst_id: HummockSSTableId,
    kv_pairs: Vec<(usize, u64, HummockValue<Vec<u8>>)>,
    sstable_store: SstableStoreRef,
) -> Sstable {
    gen_test_sstable(
        default_builder_opt_for_test(),
        sst_id,
        kv_pairs
            .into_iter()
            .map(|kv| (iterator_test_key_of_epoch(kv.0, kv.1), kv.2)),
        sstable_store,
    )
    .await
}

pub fn gen_merge_iterator_interleave_test_sstable_iters(
    key_count: usize,
    count: usize,
) -> Vec<BoxedForwardHummockIterator> {
    let sstable_store = mock_sstable_store();
    let cache = create_small_table_cache();
    (0..count)
        .map(|i: usize| {
            let table = block_on(gen_iterator_test_sstable_base(
                i as HummockSSTableId,
                default_builder_opt_for_test(),
                |x| x * count + i,
                sstable_store.clone(),
                key_count,
            ));
            let handle = cache.insert(table.id, table.id, 1, Box::new(table));
            Box::new(SSTableIterator::create(
                handle,
                sstable_store.clone(),
                Arc::new(ReadOptions::default()),
            )) as BoxedForwardHummockIterator
        })
        .collect_vec()
}

pub async fn gen_iterator_test_sstable_with_incr_epoch(
    sst_id: HummockSSTableId,
    opts: SSTableBuilderOptions,
    idx_mapping: impl Fn(usize) -> usize,
    sstable_store: SstableStoreRef,
    total: usize,
    epoch_base: u64,
) -> Sstable {
    gen_test_sstable(
        opts,
        sst_id,
        (0..total).map(|i| {
            (
                iterator_test_key_of_epoch(idx_mapping(i), epoch_base + i as u64),
                HummockValue::put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}
