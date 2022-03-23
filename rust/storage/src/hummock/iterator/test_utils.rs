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
//
#![allow(dead_code)]

// TODO: Cleanup this file, see issue #547 .

use std::iter::Iterator;
use std::sync::Arc;

use sstable_store::{SstableStore, SstableStoreRef};

use crate::hummock::key::{key_with_epoch, Epoch};
pub use crate::hummock::test_utils::default_builder_opt_for_test;
use crate::hummock::test_utils::gen_test_sstable;
use crate::hummock::{sstable_store, HummockValue, SSTableBuilderOptions, Sstable};
use crate::monitor::StateStoreMetrics;
use crate::object::{InMemObjectStore, ObjectStoreRef};

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
    let object_store = Arc::new(InMemObjectStore::new());
    mock_sstable_store_with_object_store(object_store)
}

pub fn mock_sstable_store_with_object_store(object_store: ObjectStoreRef) -> SstableStoreRef {
    let path = "test".to_string();
    Arc::new(SstableStore::new(
        object_store,
        path,
        Arc::new(StateStoreMetrics::unused()),
    ))
}

/// Generate keys like `key_test_00002` with epoch 233.
pub fn iterator_test_key_of(idx: usize) -> Vec<u8> {
    key_with_epoch(format!("key_test_{:05}", idx).as_bytes().to_vec(), 233)
}

/// Generate keys like `key_test_00002` with epoch `epoch` .
pub fn iterator_test_key_of_epoch(idx: usize, epoch: Epoch) -> Vec<u8> {
    key_with_epoch(format!("key_test_{:05}", idx).as_bytes().to_vec(), epoch)
}

/// The value of an index,like `value_test_00002`
pub fn iterator_test_value_of(idx: usize) -> Vec<u8> {
    format!("value_test_{:05}", idx).as_bytes().to_vec()
}

/// Generate a test table used in almost all table-related tests. Developers may verify the
/// correctness of their implementations by comparing the got value and the expected value
/// generated by `test_key_of` and `test_value_of`.
pub async fn gen_iterator_test_sstable_base(
    sst_id: u64,
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
                HummockValue::Put(iterator_test_value_of(idx_mapping(i))),
            )
        }),
        sstable_store,
    )
    .await
}

// key=[idx, epoch], value
pub async fn gen_iterator_test_sstable_from_kv_pair(
    sst_id: u64,
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
