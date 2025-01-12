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

use std::collections::{HashMap, HashSet};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use bytes::Bytes;
use expect_test::expect;
use foyer::CacheHint;
use futures::{pin_mut, FutureExt, StreamExt};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{test_epoch, EpochExt, INVALID_EPOCH, MAX_EPOCH};
use risingwave_hummock_sdk::key::{prefixed_range_with_vnode, FullKey, TableKeyRange};
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo, SyncResult};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::CommitEpochInfo;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::iterator::change_log::test_utils::{
    apply_test_log_data, gen_test_data,
};
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;
use risingwave_storage::store_impl::verify::VerifyStateStore;

use crate::get_notification_client_for_test;
use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::{gen_key_from_str, with_hummock_storage, TestIngestBatch};

#[tokio::test]
async fn test_empty_read() {
    let (hummock_storage, _meta_client) = with_hummock_storage(Default::default()).await;
    assert!(hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "test_key"),
            u64::MAX,
            ReadOptions {
                table_id: TableId { table_id: 2333 },
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .is_none());
    let stream = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                VirtualNode::ZERO,
            ),
            u64::MAX,
            ReadOptions {
                table_id: TableId { table_id: 2333 },
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    pin_mut!(stream);
    assert!(stream.try_next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_basic() {
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;
    let anchor = gen_key_from_str(VirtualNode::ZERO, "aa");

    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "cc"),
            StorageValue::new_put("333"),
        ),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Third batch deletes the anchor
    let mut batch3 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "dd"),
            StorageValue::new_put("444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "ee"),
            StorageValue::new_put("555"),
        ),
        (anchor.clone(), StorageValue::new_delete()),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(TableId::default()))
        .await;

    // epoch 0 is reserved by storage service
    let epoch1 = test_epoch(1);
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch1).await.unwrap();

    // try to write an empty batch, and hummock should write nothing
    let size = local
        .ingest_batch(
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    assert_eq!(size, 0);

    // Write the first batch.
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    hummock_storage.start_epoch(epoch2, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ab"),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2.next_epoch();
    hummock_storage.start_epoch(epoch3, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.

    local
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch3,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ff"),
            epoch3,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let iter = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (
                    Bound::<Bytes>::Unbounded,
                    Bound::Included(Bytes::from("ee")),
                ),
                VirtualNode::ZERO,
            ),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (
                    Bound::<Bytes>::Unbounded,
                    Bound::Included(Bytes::from("ee")),
                ),
                VirtualNode::ZERO,
            ),
            epoch2,
            ReadOptions {
                prefetch_options: PrefetchOptions::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 3);

    // Delete aa, write dd,ee
    let iter = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (
                    Bound::<Bytes>::Unbounded,
                    Bound::Included(Bytes::from("ee")),
                ),
                VirtualNode::ZERO,
            ),
            epoch3,
            ReadOptions {
                prefetch_options: PrefetchOptions::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 4);
    let res = hummock_storage
        .seal_and_sync_epoch(epoch1, HashSet::from_iter([local.table_id()]))
        .await
        .unwrap();
    meta_client.commit_epoch(epoch1, res).await.unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(epoch1),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "dd"),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(value.is_none());
}

#[tokio::test]
async fn test_state_store_sync() {
    let (hummock_storage, _meta_client) = with_hummock_storage(Default::default()).await;

    let mut epoch = INVALID_EPOCH.next_epoch();

    // ingest 16B batch
    let mut batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "\0\0aaaa"),
            StorageValue::new_put("1111"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "\0\0bbbb"),
            StorageValue::new_put("2222"),
        ),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;
    hummock_storage.start_epoch(epoch, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch).await.unwrap();
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // ingest 24B batch
    let mut batch2 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "\0\0cccc"),
            StorageValue::new_put("3333"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "\0\0dddd"),
            StorageValue::new_put("4444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "\0\0eeee"),
            StorageValue::new_put("5555"),
        ),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // shared buffer threshold size should have been reached and will trigger a flush
    // then ingest the batch
    // assert_eq!(
    //     (24 + 8 * 3) as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    epoch.inc_epoch();
    hummock_storage.start_epoch(epoch, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch, SealCurrentEpochOptions::for_test());

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "\0\0eeee"),
        StorageValue::new_put("5555"),
    )];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // 16B in total with 8B epoch appended to the key
    // assert_eq!(
    //     16 as u64,
    //     hummock_storage.shared_buffer_manager().size() as u64
    // );

    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());

    // trigger a sync
    let table_id_set = HashSet::from_iter([local.table_id()]);
    hummock_storage
        .seal_and_sync_epoch(epoch.prev_epoch(), table_id_set.clone())
        .await
        .unwrap();
    hummock_storage
        .seal_and_sync_epoch(epoch, table_id_set)
        .await
        .unwrap();

    // TODO: Uncomment the following lines after flushed sstable can be accessed.
    // FYI: https://github.com/risingwavelabs/risingwave/pull/1928#discussion_r852698719
    // assert_eq!(0, hummock_storage.shared_buffer_manager().size());
}

#[tokio::test]
/// Fix this when we finished epoch management.
#[ignore]
async fn test_reload_storage() {
    let sstable_store = mock_sstable_store().await;
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;
    let anchor = gen_key_from_str(VirtualNode::ZERO, "aa");

    // First batch inserts the anchor and others.
    let mut batch1 = [
        (anchor.clone(), StorageValue::new_put("111")),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];

    // Make sure the batch is sorted.
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // Second batch modifies the anchor.
    let mut batch2 = [
        (
            gen_key_from_str(VirtualNode::ZERO, "cc"),
            StorageValue::new_put("333"),
        ),
        (anchor.clone(), StorageValue::new_put("111111")),
    ];

    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1 = test_epoch(1);

    // Un-comment it when the unit test is re-enabled.
    // // Write the first batch.
    // hummock_storage
    //     .ingest_batch(
    //         batch1,
    //         WriteOptions {
    //             epoch: epoch1,
    //             table_id: Default::default(),
    //         },
    //     )
    //     .await
    //     .unwrap();

    // Mock something happened to storage internal, and storage is reloaded.
    drop(hummock_storage);
    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store.clone(),
        meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref, cluster_ctl_ref, worker_id)
            .await,
    )
    .await
    .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ab"),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write the second batch.
    let epoch2 = epoch1.next_epoch();
    // TODO: recover the comment if the test is needed
    // hummock_storage
    //     .ingest_batch(
    //         batch2,
    //         WriteOptions {
    //             epoch: epoch2,
    //             table_id: Default::default(),
    //         },
    //     )
    //     .await
    //     .unwrap();

    // Get the value after flushing to remote.
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write aa bb
    let iter = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (
                    Bound::<Bytes>::Unbounded,
                    Bound::Included(Bytes::from("ee")),
                ),
                VirtualNode::ZERO,
            ),
            epoch1,
            ReadOptions {
                prefetch_options: PrefetchOptions::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 2);

    // Get the anchor value at the first snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = hummock_storage
        .get(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = hummock_storage
        .iter(
            prefixed_range_with_vnode(
                (
                    Bound::<Bytes>::Unbounded,
                    Bound::Included(Bytes::from("ee")),
                ),
                VirtualNode::ZERO,
            ),
            epoch2,
            ReadOptions {
                prefetch_options: PrefetchOptions::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iter).await;
    assert_eq!(len, 3);
}

// Keep this test case's codes for future reference
// #[tokio::test]
// async fn test_write_anytime() {
//     let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;
//     test_write_anytime_inner(hummock_storage, meta_client).await;
// }

// async fn test_write_anytime_inner(
//     hummock_storage: impl HummockStateStoreTestTrait,
//     _meta_client: Arc<MockHummockMetaClient>,
// ) {
//     let initial_epoch = hummock_storage.get_pinned_version().max_committed_epoch();

//     let epoch1 = initial_epoch + 1;

//     let assert_old_value = |epoch| {
//         let hummock_storage = &hummock_storage;
//         async move {
//             // check point get
//             assert_eq!(
//                 "111".as_bytes(),
//                 hummock_storage
//                     .get(
//                         gen_key_from_str(VirtualNode::ZERO, "aa"),
//                         epoch,
//                         ReadOptions {
//                             cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                             ..Default::default()
//                         }
//                     )
//                     .await
//                     .unwrap()
//                     .unwrap()
//             );
//             assert_eq!(
//                 "222".as_bytes(),
//                 hummock_storage
//                     .get(
//                         gen_key_from_str(VirtualNode::ZERO, "bb"),
//                         epoch,
//                         ReadOptions {
//                             cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                             ..Default::default()
//                         }
//                     )
//                     .await
//                     .unwrap()
//                     .unwrap()
//             );
//             assert_eq!(
//                 "333".as_bytes(),
//                 hummock_storage
//                     .get(
//                         gen_key_from_str(VirtualNode::ZERO, "cc"),
//                         epoch,
//                         ReadOptions {
//                             cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                             ..Default::default()
//                         }
//                     )
//                     .await
//                     .unwrap()
//                     .unwrap()
//             );
//             // check iter
//             let iter = hummock_storage
//                 .iter(
//                     (
//                         Bound::Included(gen_key_from_str(VirtualNode::ZERO, "aa")),
//                         Bound::Included(gen_key_from_str(VirtualNode::ZERO, "cc")),
//                     ),
//                     epoch,
//                     ReadOptions {
//                         cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                         ..Default::default()
//                     },
//                 )
//                 .await
//                 .unwrap();
//             futures::pin_mut!(iter);
//             assert_eq!(
//                 (
//                     FullKey::new(
//                         TableId::default(),
//                         gen_key_from_str(VirtualNode::ZERO, "aa"),
//                         epoch
//                     ),
//                     Bytes::from("111")
//                 ),
//                 iter.try_next().await.unwrap().unwrap()
//             );
//             assert_eq!(
//                 (
//                     FullKey::new(
//                         TableId::default(),
//                         gen_key_from_str(VirtualNode::ZERO, "bb"),
//                         epoch
//                     ),
//                     Bytes::from("222")
//                 ),
//                 iter.try_next().await.unwrap().unwrap()
//             );
//             assert_eq!(
//                 (
//                     FullKey::new(
//                         TableId::default(),
//                         gen_key_from_str(VirtualNode::ZERO, "cc"),
//                         epoch
//                     ),
//                     Bytes::from("333")
//                 ),
//                 iter.try_next().await.unwrap().unwrap()
//             );
//             assert!(iter.try_next().await.unwrap().is_none());
//         }
//     };

//     let batch1 = vec![
//         (
//             gen_key_from_str(VirtualNode::ZERO, "aa"),
//             StorageValue::new_put("111"),
//         ),
//         (
//             gen_key_from_str(VirtualNode::ZERO, "bb"),
//             StorageValue::new_put("222"),
//         ),
//         (
//             gen_key_from_str(VirtualNode::ZERO, "cc"),
//             StorageValue::new_put("333"),
//         ),
//     ];

//     let mut local = hummock_storage.new_local(NewLocalOptions::default()).await;
//     local.init_for_test(epoch1).await.unwrap();

//     local
//         .ingest_batch(
//             batch1.clone(),
//             WriteOptions {
//                 epoch: epoch1,
//                 table_id: Default::default(),
//             },
//         )
//         .await
//         .unwrap();
//     assert_old_value(epoch1).await;

//     let assert_new_value = |epoch| {
//         let hummock_storage = &hummock_storage;
//         async move {
//             // check point get
//             assert_eq!(
//                 "111_new".as_bytes(),
//                 hummock_storage
//                     .get(
//                         gen_key_from_str(VirtualNode::ZERO, "aa"),
//                         epoch,
//                         ReadOptions {
//                             cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                             ..Default::default()
//                         }
//                     )
//                     .await
//                     .unwrap()
//                     .unwrap()
//             );

//             assert!(hummock_storage
//                 .get(
//                     gen_key_from_str(VirtualNode::ZERO, "bb"),
//                     epoch,
//                     ReadOptions {
//                         cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                         ..Default::default()
//                     }
//                 )
//                 .await
//                 .unwrap()
//                 .is_none());
//             assert_eq!(
//                 "333".as_bytes(),
//                 hummock_storage
//                     .get(
//                         gen_key_from_str(VirtualNode::ZERO, "cc"),
//                         epoch,
//                         ReadOptions {
//                             cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                             ..Default::default()
//                         }
//                     )
//                     .await
//                     .unwrap()
//                     .unwrap()
//             );
//             let iter = hummock_storage
//                 .iter(
//                     (
//                         Bound::Included(gen_key_from_str(VirtualNode::ZERO, "aa")),
//                         Bound::Included(gen_key_from_str(VirtualNode::ZERO, "cc")),
//                     ),
//                     epoch,
//                     ReadOptions {
//                         cache_policy: CachePolicy::Fill(CacheHint::Normal),
//                         ..Default::default()
//                     },
//                 )
//                 .await
//                 .unwrap();
//             futures::pin_mut!(iter);
//             assert_eq!(
//                 (
//                     FullKey::new(
//                         TableId::default(),
//                         gen_key_from_str(VirtualNode::ZERO, "aa"),
//                         epoch
//                     ),
//                     Bytes::from("111_new")
//                 ),
//                 iter.try_next().await.unwrap().unwrap()
//             );
//             assert_eq!(
//                 (
//                     FullKey::new(
//                         TableId::default(),
//                         gen_key_from_str(VirtualNode::ZERO, "cc"),
//                         epoch
//                     ),
//                     Bytes::from("333")
//                 ),
//                 iter.try_next().await.unwrap().unwrap()
//             );
//             assert!(iter.try_next().await.unwrap().is_none());
//         }
//     };

//     // Update aa, delete bb, cc unchanged
//     let batch2 = vec![
//         (
//             gen_key_from_str(VirtualNode::ZERO, "aa"),
//             StorageValue::new_put("111_new"),
//         ),
//         (
//             gen_key_from_str(VirtualNode::ZERO, "bb"),
//             StorageValue::new_delete(),
//         ),
//     ];

//     local
//         .ingest_batch(
//             batch2,
//             WriteOptions {
//                 epoch: epoch1,
//                 table_id: Default::default(),
//             },
//         )
//         .await
//         .unwrap();

//     assert_new_value(epoch1).await;

//     let epoch2 = epoch1 + 1;
//     local.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

//     // Write to epoch2
//     local
//         .ingest_batch(
//             batch1,
//             WriteOptions {
//                 epoch: epoch2,
//                 table_id: Default::default(),
//             },
//         )
//         .await
//         .unwrap();
//     local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
//     // Assert epoch 1 unchanged
//     assert_new_value(epoch1).await;
//     // Assert epoch 2 correctness
//     assert_old_value(epoch2).await;

//     let ssts1 = hummock_storage
//         .seal_and_sync_epoch(epoch1)
//         .await
//         .unwrap()
//         .uncommitted_ssts;
//     assert_new_value(epoch1).await;
//     assert_old_value(epoch2).await;

//     let ssts2 = hummock_storage
//         .seal_and_sync_epoch(epoch2)
//         .await
//         .unwrap()
//         .uncommitted_ssts;
//     assert_new_value(epoch1).await;
//     assert_old_value(epoch2).await;

//     assert!(!ssts1.is_empty());
//     assert!(!ssts2.is_empty());
// }

#[tokio::test]
async fn test_delete_get() {
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;

    let initial_epoch = INVALID_EPOCH;
    let epoch1 = initial_epoch.next_epoch();
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    let batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("111"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];
    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;
    local.init_for_test(epoch1).await.unwrap();
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    let epoch2 = epoch1.next_epoch();
    hummock_storage.start_epoch(epoch2, HashSet::from_iter([Default::default()]));

    local.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    let table_id_set = HashSet::from_iter([local.table_id()]);
    let res = hummock_storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    meta_client.commit_epoch(epoch1, res).await.unwrap();

    let batch2 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "bb"),
        StorageValue::new_delete(),
    )];
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    let res = hummock_storage
        .seal_and_sync_epoch(epoch2, table_id_set)
        .await
        .unwrap();
    meta_client.commit_epoch(epoch2, res).await.unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(epoch2),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();
    assert!(hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            }
        )
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn test_multiple_epoch_sync() {
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;

    let initial_epoch = INVALID_EPOCH;
    let epoch1 = initial_epoch.next_epoch();
    let batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("111"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(TableId::default()))
        .await;
    let table_id_set = HashSet::from_iter([local.table_id()]);
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch1).await.unwrap();
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    hummock_storage.start_epoch(epoch2, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    let batch2 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "bb"),
        StorageValue::new_delete(),
    )];
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2.next_epoch();
    hummock_storage.start_epoch(epoch3, HashSet::from_iter([Default::default()]));
    let batch3 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("555"),
        ),
    ];
    local.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());
    local
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    let test_get = |read_committed: bool| {
        let hummock_storage_clone = &hummock_storage;
        async move {
            assert_eq!(
                hummock_storage_clone
                    .get(
                        gen_key_from_str(VirtualNode::ZERO, "bb"),
                        epoch1,
                        ReadOptions {
                            read_committed,
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "222".as_bytes()
            );
            assert!(hummock_storage_clone
                .get(
                    gen_key_from_str(VirtualNode::ZERO, "bb"),
                    epoch2,
                    ReadOptions {
                        read_committed,
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    }
                )
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                hummock_storage_clone
                    .get(
                        gen_key_from_str(VirtualNode::ZERO, "bb"),
                        epoch3,
                        ReadOptions {
                            read_committed,
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        }
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "555".as_bytes()
            );
        }
    };

    test_get(false).await;
    let sync_result1 = hummock_storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    let sync_result2 = hummock_storage
        .seal_and_sync_epoch(epoch2, table_id_set.clone())
        .await
        .unwrap();
    let sync_result3 = hummock_storage
        .seal_and_sync_epoch(epoch3, table_id_set)
        .await
        .unwrap();
    test_get(false).await;

    meta_client
        .commit_epoch(epoch1, sync_result1)
        .await
        .unwrap();
    meta_client
        .commit_epoch(epoch2, sync_result2)
        .await
        .unwrap();
    meta_client
        .commit_epoch(epoch3, sync_result3)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(epoch3),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();
    test_get(true).await;
}

#[tokio::test]
async fn test_clear_shared_buffer() {
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;
    let mut local_hummock_storage = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;
    let table_id_set = HashSet::from_iter([local_hummock_storage.table_id()]);

    let initial_epoch = INVALID_EPOCH;
    let epoch1 = initial_epoch.next_epoch();
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    local_hummock_storage.init_for_test(epoch1).await.unwrap();
    local_hummock_storage
        .insert(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            Bytes::from("111"),
            None,
        )
        .unwrap();
    local_hummock_storage
        .insert(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            Bytes::from("222"),
            None,
        )
        .unwrap();
    local_hummock_storage.flush().await.unwrap();

    let epoch2 = epoch1.next_epoch();
    hummock_storage.start_epoch(epoch2, HashSet::from_iter([Default::default()]));
    local_hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    local_hummock_storage
        .delete(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            Bytes::from("222"),
        )
        .unwrap();
    local_hummock_storage.flush().await.unwrap();

    let _min_object_id = |sync_result: &SyncResult| {
        sync_result
            .uncommitted_ssts
            .iter()
            .map(|LocalSstableInfo { sst_info, .. }| sst_info.object_id)
            .min()
            .unwrap()
    };
    local_hummock_storage.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    let sync_result1 = hummock_storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();

    let _sync_result2 = hummock_storage
        .seal_and_sync_epoch(epoch2, table_id_set)
        .await
        .unwrap();

    meta_client
        .commit_epoch(epoch1, sync_result1)
        .await
        .unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(epoch1),
            TryWaitEpochOptions::for_test(local_hummock_storage.table_id()),
        )
        .await
        .unwrap();

    drop(local_hummock_storage);

    hummock_storage.clear_shared_buffer().await;
}

/// Test the following behaviours:
/// 1. LocalStateStore can read replicated ReadVersion.
/// 2. GlobalStateStore cannot read replicated ReadVersion.
#[tokio::test]
async fn test_replicated_local_hummock_storage() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };

    let (hummock_storage, meta_client) = with_hummock_storage(TEST_TABLE_ID).await;

    let epoch0 = meta_client
        .hummock_manager_ref()
        .on_current_version(|version| version.table_committed_epoch(TEST_TABLE_ID).unwrap())
        .await;

    let epoch0 = epoch0.next_epoch();

    meta_client
        .hummock_manager_ref()
        .commit_epoch(CommitEpochInfo {
            sstables: vec![],
            new_table_watermarks: Default::default(),
            sst_to_context: Default::default(),
            new_table_fragment_infos: vec![],
            change_log_delta: Default::default(),
            tables_to_commit: HashMap::from_iter([(TEST_TABLE_ID, epoch0)]),
        })
        .await
        .unwrap();

    let read_options = ReadOptions {
        table_id: TableId {
            table_id: TEST_TABLE_ID.table_id,
        },
        cache_policy: CachePolicy::Fill(CacheHint::Normal),
        ..Default::default()
    };

    let mut local_hummock_storage = hummock_storage
        .new_local(NewLocalOptions::new_replicated(
            TEST_TABLE_ID,
            OpConsistencyLevel::Inconsistent,
            TableOption {
                retention_seconds: None,
            },
            Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        ))
        .await;

    let epoch1 = epoch0.next_epoch();

    local_hummock_storage.init_for_test(epoch1).await.unwrap();
    // ingest 16B batch
    let mut batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aaaa"),
            StorageValue::new_put("1111"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bbbb"),
            StorageValue::new_put("2222"),
        ),
    ];

    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local_hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Test local state store read for replicated data.
    {
        assert!(local_hummock_storage.read_version().read().is_replicated());

        let actual = risingwave_storage::store::LocalStateStore::iter(
            &local_hummock_storage,
            prefixed_range_with_vnode(
                (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                VirtualNode::ZERO,
            ),
            read_options.clone(),
        )
        .await
        .unwrap()
        .into_stream(to_owned_item)
        .collect::<Vec<_>>()
        .await;

        let expected = expect![[r#"
            [
                Ok(
                    (
                        FullKey { UserKey { 233, TableKey { 000061616161 } }, epoch: 131072, epoch_with_gap: 131072, spill_offset: 0},
                        b"1111",
                    ),
                ),
                Ok(
                    (
                        FullKey { UserKey { 233, TableKey { 000062626262 } }, epoch: 131072, epoch_with_gap: 131072, spill_offset: 0},
                        b"2222",
                    ),
                ),
            ]
        "#]];
        expected.assert_debug_eq(&actual);
    }

    let epoch2 = epoch1.next_epoch();

    let mut local_hummock_storage_2 = hummock_storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    local_hummock_storage_2.init_for_test(epoch1).await.unwrap();
    local_hummock_storage_2.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

    // ingest 16B batch
    let mut batch2 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "cccc"),
            StorageValue::new_put("3333"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "dddd"),
            StorageValue::new_put("4444"),
        ),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    local_hummock_storage_2
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Test Global State Store iter, epoch2
    {
        let actual = hummock_storage
            .iter(
                prefixed_range_with_vnode(
                    (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                    VirtualNode::ZERO,
                ),
                epoch2,
                read_options.clone(),
            )
            .await
            .unwrap()
            .into_stream(to_owned_item)
            .collect::<Vec<_>>()
            .await;

        let expected = expect![[r#"
            [
                Ok(
                    (
                        FullKey { UserKey { 233, TableKey { 000063636363 } }, epoch: 196608, epoch_with_gap: 196608, spill_offset: 0},
                        b"3333",
                    ),
                ),
                Ok(
                    (
                        FullKey { UserKey { 233, TableKey { 000064646464 } }, epoch: 196608, epoch_with_gap: 196608, spill_offset: 0},
                        b"4444",
                    ),
                ),
            ]
        "#]];
        expected.assert_debug_eq(&actual);
    }

    // Test Global State Store iter, epoch1
    {
        let actual = hummock_storage
            .iter(
                prefixed_range_with_vnode(
                    (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                    VirtualNode::ZERO,
                ),
                epoch1,
                read_options,
            )
            .await
            .unwrap()
            .into_stream(to_owned_item)
            .collect::<Vec<_>>()
            .await;

        let expected = expect![[r#"
            []
        "#]];
        expected.assert_debug_eq(&actual);
    }
}

#[tokio::test]
async fn test_iter_log() {
    let table_id = TableId::new(233);
    let (hummock_storage, meta_client) = with_hummock_storage(table_id).await;
    let checkpoint_frequency = 3;
    let epoch_count = 10;
    let key_count = 10000;

    let test_log_data = gen_test_data(epoch_count, key_count, 0.05, 0.2);
    for (epoch, _) in &test_log_data {
        hummock_storage.start_epoch(*epoch, HashSet::from_iter([table_id]));
    }
    hummock_storage.start_epoch(MAX_EPOCH, HashSet::from_iter([table_id]));
    let in_memory_state_store = MemoryStateStore::new();

    let mut in_memory_local = in_memory_state_store
        .new_local(NewLocalOptions {
            table_id,
            op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                check_old_value: CHECK_BYTES_EQUAL.clone(),
                is_log_store: true,
            },
            table_option: Default::default(),
            is_replicated: false,
            vnodes: Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into(),
        })
        .await;

    apply_test_log_data(test_log_data.clone(), &mut in_memory_local, 0.0).await;

    let mut hummock_local = hummock_storage
        .new_local(NewLocalOptions {
            table_id,
            op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                check_old_value: CHECK_BYTES_EQUAL.clone(),
                is_log_store: true,
            },
            table_option: Default::default(),
            is_replicated: false,
            vnodes: Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into(),
        })
        .await;
    // flush for about 10 times per epoch
    apply_test_log_data(test_log_data.clone(), &mut hummock_local, 0.001).await;

    let table_id_set = HashSet::from_iter([table_id]);
    let mut pending_non_checkpoint_epochs = vec![];
    for (i, (epoch, _)) in test_log_data.iter().enumerate() {
        pending_non_checkpoint_epochs.push(*epoch);
        if i != 0 && i != test_log_data.len() - 1 && i % checkpoint_frequency != 0 {
            continue;
        }
        let res = hummock_storage
            .seal_and_sync_epoch(*epoch, table_id_set.clone())
            .await
            .unwrap();
        if i != 0 {
            assert!(!res.old_value_ssts.is_empty());
        }
        assert!(!res.uncommitted_ssts.is_empty());
        meta_client
            .commit_epoch_with_change_log(
                *epoch,
                res,
                Some(std::mem::take(&mut pending_non_checkpoint_epochs)),
            )
            .await
            .unwrap();
    }
    assert!(pending_non_checkpoint_epochs.is_empty());

    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(test_log_data.last().unwrap().0),
            TryWaitEpochOptions { table_id },
        )
        .await
        .unwrap();

    let verify_state_store = VerifyStateStore {
        actual: hummock_storage,
        expected: Some(in_memory_state_store),
        _phantom: Default::default(),
    };

    let verify_iter_log = |key_range: TableKeyRange| {
        let state_store = &verify_state_store;
        let test_log_data = &test_log_data;
        async move {
            for start_epoch_idx in 0..epoch_count {
                for end_epoch_idx in start_epoch_idx..epoch_count {
                    let min_epoch = test_log_data[start_epoch_idx].0;
                    let max_epoch = test_log_data[end_epoch_idx].0;
                    let mut iter = state_store
                        .iter_log(
                            (min_epoch, max_epoch),
                            key_range.clone(),
                            ReadLogOptions { table_id },
                        )
                        .await
                        .unwrap();
                    while iter.try_next().await.unwrap().is_some() {}
                }
            }
        }
    };

    verify_iter_log((Unbounded, Unbounded)).await;

    if cfg!(madsim) {
        // avoid timeout in madsim
        return;
    }

    let keys = test_log_data
        .iter()
        .flat_map(|(_, logs)| logs.iter().map(|(key, _)| key.clone()))
        .sorted()
        .dedup()
        .collect_vec();
    let test_key_count = 5;
    let step = keys.len() / test_key_count;

    for i in 0..test_key_count {
        let start_key_idx = i * step;
        let start_key = keys[start_key_idx].clone();
        let start_bound = Included(start_key);
        for j in i..test_key_count {
            let end_key_idx = j * step;
            let end_key = keys[end_key_idx].clone();
            let end_bound = if j % 2 == 0 {
                Included(end_key)
            } else {
                Excluded(end_key)
            };
            verify_iter_log((start_bound.clone(), end_bound)).await;
        }
        verify_iter_log((start_bound, Unbounded)).await;
    }
}

#[tokio::test]
async fn test_read_log_next_epoch() {
    let table_id = TableId::new(233);
    let (hummock_storage, meta_client) = with_hummock_storage(table_id).await;
    let checkpoint_frequency = 10;
    let epoch_count = 100;
    let key_count = 100;

    let test_log_data = gen_test_data(epoch_count, key_count, 0.05, 0.2);
    for (epoch, _) in &test_log_data {
        hummock_storage.start_epoch(*epoch, HashSet::from_iter([table_id]));
    }
    hummock_storage.start_epoch(MAX_EPOCH, HashSet::from_iter([table_id]));

    let mut hummock_local = hummock_storage
        .new_local(NewLocalOptions {
            table_id,
            op_consistency_level: OpConsistencyLevel::ConsistentOldValue {
                check_old_value: CHECK_BYTES_EQUAL.clone(),
                is_log_store: true,
            },
            table_option: Default::default(),
            is_replicated: false,
            vnodes: Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into(),
        })
        .await;
    // flush for about 10 times per epoch
    apply_test_log_data(test_log_data.clone(), &mut hummock_local, 0.001).await;

    let table_id_set = HashSet::from_iter([table_id]);
    let later_commit_epoch_count = 5;
    let mut pending_non_checkpoint_epochs = vec![];
    let first_commit_log_data = &test_log_data[..test_log_data.len() - later_commit_epoch_count];
    for (i, (epoch, _)) in first_commit_log_data.iter().enumerate() {
        pending_non_checkpoint_epochs.push(*epoch);
        if i != 0 && i != first_commit_log_data.len() - 1 && i % checkpoint_frequency != 0 {
            continue;
        }
        let res = hummock_storage
            .seal_and_sync_epoch(*epoch, table_id_set.clone())
            .await
            .unwrap();
        if *epoch != first_commit_log_data[0].0 {
            assert!(!res.old_value_ssts.is_empty());
        }
        assert!(!res.uncommitted_ssts.is_empty());
        meta_client
            .commit_epoch_with_change_log(
                *epoch,
                res,
                Some(std::mem::take(&mut pending_non_checkpoint_epochs)),
            )
            .await
            .unwrap();
    }
    assert!(pending_non_checkpoint_epochs.is_empty());
    for i in 0..(first_commit_log_data.len() - 1) {
        let epoch = first_commit_log_data[i].0;
        let next_epoch = first_commit_log_data[i + 1].0;
        assert_eq!(
            hummock_storage
                .next_epoch(epoch, NextEpochOptions { table_id })
                .await
                .unwrap(),
            next_epoch
        );
    }
    let first_latest_commit_epoch = first_commit_log_data.last().unwrap().0;
    let first_commit_epoch_next_epoch_future =
        hummock_storage.next_epoch(first_latest_commit_epoch, NextEpochOptions { table_id });
    pin_mut!(first_commit_epoch_next_epoch_future);
    assert!(first_commit_epoch_next_epoch_future
        .as_mut()
        .now_or_never()
        .is_none());
    let second_commit_log_data = &test_log_data[test_log_data.len() - later_commit_epoch_count..];
    let commit_epoch = second_commit_log_data.last().unwrap().0;
    let res = hummock_storage
        .seal_and_sync_epoch(commit_epoch, table_id_set.clone())
        .await
        .unwrap();
    assert!(!res.old_value_ssts.is_empty());
    assert!(!res.uncommitted_ssts.is_empty());
    meta_client
        .commit_epoch_with_change_log(
            commit_epoch,
            res,
            Some(
                second_commit_log_data
                    .iter()
                    .map(|(epoch, _)| *epoch)
                    .collect(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        first_commit_epoch_next_epoch_future.await.unwrap(),
        second_commit_log_data.first().unwrap().0
    );
    for (i, epoch) in [first_latest_commit_epoch]
        .into_iter()
        .chain(
            second_commit_log_data[..second_commit_log_data.len() - 1]
                .iter()
                .map(|(epoch, _)| *epoch),
        )
        .enumerate()
    {
        let next_epoch = second_commit_log_data[i].0;
        assert_eq!(
            hummock_storage
                .next_epoch(epoch, NextEpochOptions { table_id })
                .await
                .unwrap(),
            next_epoch
        );
    }
}

#[tokio::test]
async fn test_get_keyed_row() {
    let (hummock_storage, meta_client) = with_hummock_storage(Default::default()).await;
    let table_id = TableId::default();
    let anchor = gen_key_from_str(VirtualNode::ZERO, "aa");

    // First batch inserts the anchor and others.
    let batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];

    // Second batch modifies the anchor.
    let batch2 = vec![
        (anchor.clone(), StorageValue::new_put("111111")),
        (
            gen_key_from_str(VirtualNode::ZERO, "cc"),
            StorageValue::new_put("333"),
        ),
    ];

    // Third batch deletes the anchor
    let batch3 = vec![
        (anchor.clone(), StorageValue::new_delete()),
        (
            gen_key_from_str(VirtualNode::ZERO, "dd"),
            StorageValue::new_put("444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "ee"),
            StorageValue::new_put("555"),
        ),
    ];

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(TableId::default()))
        .await;

    // epoch 0 is reserved by storage service
    let epoch1 = test_epoch(1);
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch1).await.unwrap();

    // try to write an empty batch, and hummock should write nothing
    let size = local
        .ingest_batch(
            vec![],
            WriteOptions {
                epoch: epoch1,
                table_id,
            },
        )
        .await
        .unwrap();
    assert_eq!(size, 0);

    // Write the first batch.
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id,
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    hummock_storage.start_epoch(epoch2, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let (key, value) = hummock_storage
        .get_keyed_row(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key, FullKey::new(table_id, anchor.clone(), epoch1));
    assert_eq!(value, Bytes::from("111"));
    let (key, value) = hummock_storage
        .get_keyed_row(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        key,
        FullKey::new(table_id, gen_key_from_str(VirtualNode::ZERO, "bb"), epoch1)
    );
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let res = hummock_storage
        .get_keyed_row(
            gen_key_from_str(VirtualNode::ZERO, "ab"),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(res, None);

    // Write the second batch.
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id,
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2.next_epoch();
    hummock_storage.start_epoch(epoch3, HashSet::from_iter([Default::default()]));
    local.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let (key, value) = hummock_storage
        .get_keyed_row(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key, FullKey::new(table_id, anchor.clone(), epoch2));
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    local
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id,
            },
        )
        .await
        .unwrap();

    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());

    // Get the value after flushing to remote.
    let res = hummock_storage
        .get_keyed_row(
            anchor.clone(),
            epoch3,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(res, None);

    // Get the anchor value at the first snapshot
    let (key, value) = hummock_storage
        .get_keyed_row(
            anchor.clone(),
            epoch1,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key, FullKey::new(table_id, anchor.clone(), epoch1));
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let (key, value) = hummock_storage
        .get_keyed_row(
            anchor.clone(),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key, FullKey::new(table_id, anchor.clone(), epoch2));
    assert_eq!(value, Bytes::from("111111"));

    let res = hummock_storage
        .seal_and_sync_epoch(epoch1, HashSet::from_iter([local.table_id()]))
        .await
        .unwrap();
    meta_client.commit_epoch(epoch1, res).await.unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(epoch1),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();
    let (key, value) = hummock_storage
        .get_keyed_row(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        key,
        FullKey::new(table_id, gen_key_from_str(VirtualNode::ZERO, "bb"), epoch1)
    );
    assert_eq!(value, Bytes::from("222"));
    let res = hummock_storage
        .get_keyed_row(
            gen_key_from_str(VirtualNode::ZERO, "dd"),
            epoch2,
            ReadOptions {
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(res.is_none());
}
