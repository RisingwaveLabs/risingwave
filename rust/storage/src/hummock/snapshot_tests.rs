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

#[cfg(test)]
use std::sync::Arc;

use risingwave_pb::hummock::SstableInfo;

use super::*;
use crate::hummock::iterator::test_utils::{iterator_test_key_of, iterator_test_key_of_epoch};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
use crate::hummock::test_utils::{
    default_builder_opt_for_test, default_config_for_test, gen_test_sstable,
};
use crate::hummock::value::HummockValue;
use crate::object::{InMemObjectStore, ObjectStore};

async fn gen_and_upload_table(
    object_store: Arc<dyn ObjectStore>,
    remote_dir: &str,
    vm: Arc<LocalVersionManager>,
    hummock_meta_client: &dyn HummockMetaClient,
    kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
    epoch: u64,
) {
    if kv_pairs.is_empty() {
        return;
    }
    let table_id = hummock_meta_client.get_new_table_id().await.unwrap();

    // Get remote table
    let sstable_store = Arc::new(SstableStore::new(
        object_store,
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let sst = gen_test_sstable(
        default_builder_opt_for_test(),
        table_id,
        kv_pairs
            .into_iter()
            .map(|(key, value)| (iterator_test_key_of_epoch(key, epoch), value)),
        sstable_store,
    )
    .await;

    let version = hummock_meta_client
        .add_tables(
            epoch,
            vec![SstableInfo {
                id: table_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key,
                    right: sst.meta.largest_key,
                    inf: false,
                }),
            }],
        )
        .await
        .unwrap();
    vm.try_set_version(version);
    hummock_meta_client.commit_epoch(epoch).await.ok();
}

async fn gen_and_upload_table_with_sstable_store(
    sstable_store: SstableStoreRef,
    vm: Arc<LocalVersionManager>,
    hummock_meta_client: &dyn HummockMetaClient,
    kv_pairs: Vec<(usize, HummockValue<Vec<u8>>)>,
    epoch: u64,
) {
    if kv_pairs.is_empty() {
        return;
    }
    let table_id = hummock_meta_client.get_new_table_id().await.unwrap();

    let sst = gen_test_sstable(
        default_builder_opt_for_test(),
        table_id,
        kv_pairs
            .into_iter()
            .map(|(key, value)| (iterator_test_key_of_epoch(key, epoch), value)),
        sstable_store,
    )
    .await;

    let version = hummock_meta_client
        .add_tables(
            epoch,
            vec![SstableInfo {
                id: table_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: sst.meta.smallest_key,
                    right: sst.meta.largest_key,
                    inf: false,
                }),
            }],
        )
        .await
        .unwrap();
    vm.try_set_version(version);
    hummock_meta_client.commit_epoch(epoch).await.ok();
}

macro_rules! assert_count_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage.iter::<_, Vec<u8>>($range, $epoch).await.unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

macro_rules! assert_count_reverse_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        let mut it = $storage
            .reverse_iter::<_, Vec<u8>>($range, $epoch)
            .await
            .unwrap();
        let mut count = 0;
        loop {
            match it.next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

#[tokio::test]
async fn test_snapshot() {
    let remote_dir = "hummock_001";
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));

    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch1: u64 = 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
        ],
        epoch1,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch2 = epoch1 + 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Delete(Default::default())),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch2,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);

    let epoch3 = epoch2 + 1;
    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (2, HummockValue::Delete(Default::default())),
            (3, HummockValue::Delete(Default::default())),
            (4, HummockValue::Delete(Default::default())),
        ],
        epoch3,
    )
    .await;
    assert_count_range_scan!(hummock_storage, .., 0, epoch3);
    assert_count_range_scan!(hummock_storage, .., 3, epoch2);
    assert_count_range_scan!(hummock_storage, .., 2, epoch1);
}

#[tokio::test]
async fn test_snapshot_range_scan() {
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "hummock_001";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch: u64 = 1;

    gen_and_upload_table(
        object_store.clone(),
        remote_dir,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;

    macro_rules! key {
        ($idx:expr) => {
            user_key(&iterator_test_key_of($idx)).to_vec()
        };
    }

    assert_count_range_scan!(hummock_storage, key!(2)..=key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, key!(2)..key!(3), 1, epoch);
    assert_count_range_scan!(hummock_storage, key!(2).., 3, epoch);
    assert_count_range_scan!(hummock_storage, ..=key!(3), 3, epoch);
    assert_count_range_scan!(hummock_storage, ..key!(3), 2, epoch);
    assert_count_range_scan!(hummock_storage, .., 4, epoch);
}

#[tokio::test]
async fn test_snapshot_reverse_range_scan() {
    let object_store = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
    let remote_dir = "/test";
    let sstable_store = Arc::new(SstableStore::new(
        object_store.clone(),
        remote_dir.to_string(),
        Arc::new(StateStoreMetrics::unused()),
        64 << 20,
        64 << 20,
    ));
    let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
    let mock_hummock_meta_service = Arc::new(MockHummockMetaService::new());
    let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        mock_hummock_meta_service.clone(),
    ));
    let hummock_options = Arc::new(default_config_for_test());
    let hummock_storage = HummockStorage::with_default_stats(
        hummock_options,
        sstable_store.clone(),
        vm.clone(),
        mock_hummock_meta_client.clone(),
        Arc::new(StateStoreMetrics::unused()),
    )
    .await
    .unwrap();

    let epoch = 1;

    gen_and_upload_table_with_sstable_store(
        sstable_store,
        vm.clone(),
        mock_hummock_meta_client.as_ref(),
        vec![
            (1, HummockValue::Put(b"test".to_vec())),
            (2, HummockValue::Put(b"test".to_vec())),
            (3, HummockValue::Put(b"test".to_vec())),
            (4, HummockValue::Put(b"test".to_vec())),
        ],
        epoch,
    )
    .await;

    macro_rules! key {
        ($idx:expr) => {
            user_key(&iterator_test_key_of($idx)).to_vec()
        };
    }

    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(2), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(2), 1, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(1), 2, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..=key!(1), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, key!(3)..key!(0), 3, epoch);
    assert_count_reverse_range_scan!(hummock_storage, .., 4, epoch);
}
