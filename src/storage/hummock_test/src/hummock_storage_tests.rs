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
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::Range;
use std::sync::Arc;

use bytes::{BufMut, Bytes};
use foyer::CacheHint;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::range::RangeBoundsExt;
use risingwave_common::util::epoch::{test_epoch, EpochExt, INVALID_EPOCH};
use risingwave_hummock_sdk::key::{
    gen_key_from_bytes, prefixed_range_with_vnode, FullKey, TableKey, UserKey, TABLE_PREFIX_LEN,
};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::table_stats::TableStats;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarksIndex, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::{EpochWithGap, LocalSstableInfo};
use risingwave_meta::hummock::test_utils::get_compaction_group_id_by_table_id;
use risingwave_meta::hummock::{CommitEpochInfo, NewTableFragmentInfo};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::store::version::read_filter_for_version;
use risingwave_storage::hummock::{CachePolicy, HummockStorage, LocalHummockStorage};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;

use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::{gen_key_from_str, prepare_hummock_test_env, TestIngestBatch};

#[tokio::test]
async fn test_storage_basic() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;
    // First batch inserts the anchor and others.
    let mut batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("111"),
        ),
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
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("111111"),
        ),
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
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_delete(),
        ),
    ];

    // Make sure the batch is sorted.
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    // epoch 0 is reserved by storage service
    let epoch1 = test_epoch(1);
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.init_for_test(epoch1).await.unwrap();

    // Write the first batch.
    hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            epoch1,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch1,
            ReadOptions {
                table_id: TEST_TABLE_ID,
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("222"));

    // Test looking for a nonexistent key. `next()` would return the next key.
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ab"),
            epoch1,
            ReadOptions {
                table_id: TEST_TABLE_ID,
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    let epoch2 = epoch1.next_epoch();
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    hummock_storage
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            epoch2,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));

    // Write the third batch.
    let epoch3 = epoch2.next_epoch();
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());
    hummock_storage
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // Get the value after flushing to remote.
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            epoch3,
            ReadOptions {
                table_id: TEST_TABLE_ID,
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Get non-existent maximum key.
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ff"),
            epoch3,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(value, None);

    // Write aa bb
    let iter = test_env
        .storage
        .iter(
            (
                Unbounded,
                Included(gen_key_from_str(VirtualNode::ZERO, "ee")),
            ),
            epoch1,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_stream(to_owned_item);
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "aa"),
                epoch1
            ),
            Bytes::copy_from_slice(&b"111"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "bb"),
                epoch1
            ),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // Get the anchor value at the first snapshot
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            epoch1,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));

    // Get the anchor value at the second snapshot
    let value = test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            epoch2,
            ReadOptions {
                table_id: TEST_TABLE_ID,

                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111111"));
    // Update aa, write cc
    let iter = test_env
        .storage
        .iter(
            (
                Unbounded,
                Included(gen_key_from_str(VirtualNode::ZERO, "ee")),
            ),
            epoch2,
            ReadOptions {
                table_id: TEST_TABLE_ID,
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_stream(to_owned_item);
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "aa"),
                epoch2
            ),
            Bytes::copy_from_slice(&b"111111"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "bb"),
                epoch1
            ),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "cc"),
                epoch2
            ),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // Delete aa, write dd,ee
    let iter = test_env
        .storage
        .iter(
            (
                Unbounded,
                Included(gen_key_from_str(VirtualNode::ZERO, "ee")),
            ),
            epoch3,
            ReadOptions {
                table_id: TEST_TABLE_ID,
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .into_stream(to_owned_item);
    futures::pin_mut!(iter);
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "bb"),
                epoch1
            ),
            Bytes::copy_from_slice(&b"222"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "cc"),
                epoch2
            ),
            Bytes::copy_from_slice(&b"333"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "dd"),
                epoch3
            ),
            Bytes::copy_from_slice(&b"444"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(
        Some((
            FullKey::new(
                TEST_TABLE_ID,
                gen_key_from_str(VirtualNode::ZERO, "ee"),
                epoch3
            ),
            Bytes::copy_from_slice(&b"555"[..])
        )),
        iter.try_next().await.unwrap()
    );
    assert_eq!(None, iter.try_next().await.unwrap());

    // TODO: add more test cases after sync is supported
}

#[tokio::test]
async fn test_state_store_sync() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let read_version = hummock_storage.read_version();

    let base_epoch = read_version
        .read()
        .committed()
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();
    let epoch1 = base_epoch.next_epoch();
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.init_for_test(epoch1).await.unwrap();

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
    hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    // ingest 24B batch
    let mut batch2 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "cccc"),
            StorageValue::new_put("3333"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "dddd"),
            StorageValue::new_put("4444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "eeee"),
            StorageValue::new_put("5555"),
        ),
    ];
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());

    // ingest more 8B then will trigger a sync behind the scene
    let mut batch3 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "eeee"),
        StorageValue::new_put("6666"),
    )];
    batch3.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    hummock_storage
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2.next_epoch();
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());

    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, res)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;
    {
        // after sync 1 epoch
        let read_version = hummock_storage.read_version();
        assert_eq!(1, read_version.read().staging().imm.len());
        assert!(read_version.read().staging().sst.is_empty());
    }

    {
        let kv_map = [
            (gen_key_from_str(VirtualNode::ZERO, "aaaa"), "1111"),
            (gen_key_from_str(VirtualNode::ZERO, "bbbb"), "2222"),
            (gen_key_from_str(VirtualNode::ZERO, "cccc"), "3333"),
            (gen_key_from_str(VirtualNode::ZERO, "dddd"), "4444"),
            (gen_key_from_str(VirtualNode::ZERO, "eeee"), "5555"),
        ];

        for (k, v) in kv_map {
            let value = test_env
                .storage
                .get(
                    k,
                    epoch1,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes::from(v));
        }
    }

    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch2, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, res)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;
    {
        // after sync all epoch
        let read_version = hummock_storage.read_version();
        assert!(read_version.read().staging().imm.is_empty());
        assert!(read_version.read().staging().sst.is_empty());
    }

    {
        let kv_map = [
            (gen_key_from_str(VirtualNode::ZERO, "aaaa"), "1111"),
            (gen_key_from_str(VirtualNode::ZERO, "bbbb"), "2222"),
            (gen_key_from_str(VirtualNode::ZERO, "cccc"), "3333"),
            (gen_key_from_str(VirtualNode::ZERO, "dddd"), "4444"),
            (gen_key_from_str(VirtualNode::ZERO, "eeee"), "6666"),
        ];

        for (k, v) in kv_map {
            let value = test_env
                .storage
                .get(
                    k,
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(value, Bytes::from(v));
        }
    }

    // test iter
    {
        let iter = test_env
            .storage
            .iter(
                (
                    Unbounded,
                    Included(gen_key_from_str(VirtualNode::ZERO, "eeee")),
                ),
                epoch1,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    read_committed: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream(to_owned_item);
        futures::pin_mut!(iter);

        let rev_iter = test_env
            .storage
            .rev_iter(
                (
                    Unbounded,
                    Included(gen_key_from_str(VirtualNode::ZERO, "eeee")),
                ),
                epoch1,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    read_committed: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream(to_owned_item);
        futures::pin_mut!(rev_iter);
        let mut rev_results = vec![];
        while let Some(result) = rev_iter.try_next().await.unwrap() {
            rev_results.push(result);
        }
        let kv_map_batch_1 = [
            (gen_key_from_str(VirtualNode::ZERO, "aaaa"), "1111", epoch1),
            (gen_key_from_str(VirtualNode::ZERO, "bbbb"), "2222", epoch1),
        ];
        for (k, v, e) in kv_map_batch_1 {
            let result = iter.try_next().await.unwrap();
            let rev_result = rev_results.pop();
            assert_eq!(result, rev_result);
            assert_eq!(
                result,
                Some((
                    FullKey::new_with_gap_epoch(TEST_TABLE_ID, k, EpochWithGap::new(e, 0)),
                    Bytes::from(v)
                ))
            );
        }
        let kv_map_batch_2 = [
            (gen_key_from_str(VirtualNode::ZERO, "cccc"), "3333", epoch1),
            (gen_key_from_str(VirtualNode::ZERO, "dddd"), "4444", epoch1),
            (gen_key_from_str(VirtualNode::ZERO, "eeee"), "5555", epoch1),
        ];

        for (k, v, e) in kv_map_batch_2 {
            let result = iter.try_next().await.unwrap();
            let rev_result = rev_results.pop();
            assert_eq!(result, rev_result);
            assert_eq!(
                result,
                Some((
                    FullKey::new_with_gap_epoch(TEST_TABLE_ID, k, EpochWithGap::new(e, 1)),
                    Bytes::from(v)
                ))
            );
        }

        assert!(iter.try_next().await.unwrap().is_none());
    }

    {
        let iter = test_env
            .storage
            .iter(
                (
                    Unbounded,
                    Included(gen_key_from_str(VirtualNode::ZERO, "eeee")),
                ),
                epoch2,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream(to_owned_item);

        futures::pin_mut!(iter);

        let rev_iter = test_env
            .storage
            .rev_iter(
                (
                    Unbounded,
                    Included(gen_key_from_str(VirtualNode::ZERO, "eeee")),
                ),
                epoch2,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream(to_owned_item);
        futures::pin_mut!(rev_iter);
        let mut rev_results = vec![];
        while let Some(result) = rev_iter.try_next().await.unwrap() {
            rev_results.push(result);
        }

        let kv_map_batch_1 = [("aaaa", "1111", epoch1), ("bbbb", "2222", epoch1)];

        let kv_map_batch_2 = [("cccc", "3333", epoch1), ("dddd", "4444", epoch1)];
        let kv_map_batch_3 = [("eeee", "6666", epoch2)];
        for (k, v, e) in kv_map_batch_1 {
            let result = iter.try_next().await.unwrap();
            let rev_result = rev_results.pop();
            assert_eq!(result, rev_result);
            assert_eq!(
                result,
                Some((
                    FullKey::new(TEST_TABLE_ID, gen_key_from_str(VirtualNode::ZERO, k), e),
                    Bytes::from(v)
                ))
            );
        }

        for (k, v, e) in kv_map_batch_2 {
            let result = iter.try_next().await.unwrap();
            let rev_result = rev_results.pop();
            assert_eq!(result, rev_result);
            assert_eq!(
                result,
                Some((
                    FullKey::new_with_gap_epoch(
                        TEST_TABLE_ID,
                        gen_key_from_str(VirtualNode::ZERO, k),
                        EpochWithGap::new(e, 1)
                    ),
                    Bytes::from(v)
                ))
            );
        }
        for (k, v, e) in kv_map_batch_3 {
            let result = iter.try_next().await.unwrap();
            assert_eq!(
                result,
                Some((
                    FullKey::new_with_gap_epoch(
                        TEST_TABLE_ID,
                        gen_key_from_str(VirtualNode::ZERO, k),
                        EpochWithGap::new(e, 0)
                    ),
                    Bytes::from(v)
                ))
            );
        }
    }
}

#[tokio::test]
async fn test_delete_get() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let initial_epoch = hummock_storage
        .read_version()
        .read()
        .committed()
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();

    let epoch1 = initial_epoch.next_epoch();
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));

    hummock_storage.init_for_test(epoch1).await.unwrap();
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
    hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, res)
        .await
        .unwrap();

    let batch2 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "bb"),
        StorageValue::new_delete(),
    )];
    hummock_storage
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();
    hummock_storage.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    let res = test_env
        .storage
        .seal_and_sync_epoch(epoch2, table_id_set)
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, res)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;
    assert!(test_env
        .storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            epoch2,
            ReadOptions {
                prefix_hint: None,
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
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let initial_epoch = hummock_storage
        .read_version()
        .read()
        .committed()
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();

    let epoch1 = initial_epoch.next_epoch();
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.init_for_test(epoch1).await.unwrap();
    let batch1 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("111"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("222"),
        ),
    ];
    hummock_storage
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = epoch1.next_epoch();
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    let batch2 = vec![(
        gen_key_from_str(VirtualNode::ZERO, "bb"),
        StorageValue::new_delete(),
    )];
    hummock_storage
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch3 = epoch2.next_epoch();
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());
    let batch3 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("444"),
        ),
        (
            gen_key_from_str(VirtualNode::ZERO, "bb"),
            StorageValue::new_put("555"),
        ),
    ];
    hummock_storage
        .ingest_batch(
            batch3,
            WriteOptions {
                epoch: epoch3,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();
    let test_get = |read_committed: bool| {
        let hummock_storage_clone = &test_env.storage;
        async move {
            assert_eq!(
                hummock_storage_clone
                    .get(
                        gen_key_from_str(VirtualNode::ZERO, "bb"),
                        epoch1,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            read_committed,
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
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
                        table_id: TEST_TABLE_ID,
                        read_committed,
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
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
                            table_id: TEST_TABLE_ID,
                            read_committed,
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .unwrap(),
                "555".as_bytes()
            );
        }
    };
    test_get(false).await;

    let epoch4 = epoch3.next_epoch();
    test_env
        .storage
        .start_epoch(epoch4, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch4, SealCurrentEpochOptions::for_test());

    let sync_result1 = test_env
        .storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    let sync_result2 = test_env
        .storage
        .seal_and_sync_epoch(epoch2, table_id_set.clone())
        .await
        .unwrap();
    let sync_result3 = test_env
        .storage
        .seal_and_sync_epoch(epoch3, table_id_set)
        .await
        .unwrap();
    test_get(false).await;

    test_env
        .meta_client
        .commit_epoch(epoch1, sync_result1)
        .await
        .unwrap();

    test_env
        .meta_client
        .commit_epoch(epoch2, sync_result2)
        .await
        .unwrap();

    test_env
        .meta_client
        .commit_epoch(epoch3, sync_result3)
        .await
        .unwrap();
    test_env.wait_sync_committed_version().await;
    test_get(true).await;
}

#[tokio::test]
async fn test_iter_with_min_epoch() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let prev_epoch = test_env
        .manager
        .get_current_version()
        .await
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();

    let epoch1 = (31 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));

    let gen_key = |index: usize| -> TableKey<Bytes> {
        gen_key_from_str(VirtualNode::ZERO, format!("\0\0key_{}", index).as_str())
    };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(TableKey<Bytes>, StorageValue)> = (0..10)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    hummock_storage
        .init_for_test_with_prev_epoch(epoch1, prev_epoch)
        .await
        .unwrap();

    hummock_storage
        .ingest_batch(
            batch_epoch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = (32 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    // epoch 2 write
    let batch_epoch2: Vec<(TableKey<Bytes>, StorageValue)> = (20..30)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch3 = (33 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());

    {
        // test before sync
        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch1,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(20, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(0),
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }
    }

    {
        // test after sync

        let sync_result1 = test_env
            .storage
            .seal_and_sync_epoch(epoch1, table_id_set.clone())
            .await
            .unwrap();
        let sync_result2 = test_env
            .storage
            .seal_and_sync_epoch(epoch2, table_id_set)
            .await
            .unwrap();
        test_env
            .meta_client
            .commit_epoch(epoch1, sync_result1)
            .await
            .unwrap();
        test_env
            .meta_client
            .commit_epoch(epoch2, sync_result2)
            .await
            .unwrap();
        test_env.wait_sync_committed_version().await;

        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch1,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        read_committed: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(20, result.len());
        }

        {
            let iter = test_env
                .storage
                .iter(
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(0),
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item);

            futures::pin_mut!(iter);

            let result: Vec<_> = iter.try_collect().await.unwrap();
            assert_eq!(10, result.len());
        }
    }
}

#[tokio::test]
async fn test_hummock_version_reader() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;
    let hummock_version_reader = test_env.storage.version_reader();

    let epoch1 = (31 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));

    let gen_key = |index: usize| -> TableKey<Bytes> {
        gen_key_from_str(VirtualNode::ZERO, format!("\0\0key_{}", index).as_str())
    };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(TableKey<Bytes>, StorageValue)> = (0..10)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    let epoch2 = (32 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    // epoch 2 write
    let batch_epoch2: Vec<(TableKey<Bytes>, StorageValue)> = (20..30)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    let epoch3 = (33 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));
    // epoch 3 write
    let batch_epoch3: Vec<(TableKey<Bytes>, StorageValue)> = (40..50)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();
    {
        let prev_epoch = test_env
            .manager
            .get_current_version()
            .await
            .table_committed_epoch(TEST_TABLE_ID)
            .unwrap();
        hummock_storage
            .init_for_test_with_prev_epoch(epoch1, prev_epoch)
            .await
            .unwrap();
        hummock_storage
            .ingest_batch(
                batch_epoch1,
                WriteOptions {
                    epoch: epoch1,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
        hummock_storage
            .ingest_batch(
                batch_epoch2,
                WriteOptions {
                    epoch: epoch2,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        hummock_storage.seal_current_epoch(epoch3, SealCurrentEpochOptions::for_test());
        hummock_storage
            .ingest_batch(
                batch_epoch3,
                WriteOptions {
                    epoch: epoch3,
                    table_id: TEST_TABLE_ID,
                },
            )
            .await
            .unwrap();

        let epoch4 = (34 * 1000) << 16;
        test_env
            .storage
            .start_epoch(epoch4, HashSet::from_iter([TEST_TABLE_ID]));
        hummock_storage.seal_current_epoch(epoch4, SealCurrentEpochOptions::for_test());

        {
            // test before sync
            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch1,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch1,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch2,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch2,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(20, result.len());
            }

            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch2,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch2,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            retention_seconds: Some(0),
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }
        }

        {
            let sync_result1 = test_env
                .storage
                .seal_and_sync_epoch(epoch1, table_id_set.clone())
                .await
                .unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch1, sync_result1)
                .await
                .unwrap();
            test_env.wait_sync_committed_version().await;

            let sync_result2 = test_env
                .storage
                .seal_and_sync_epoch(epoch2, table_id_set.clone())
                .await
                .unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch2, sync_result2)
                .await
                .unwrap();
            test_env.wait_sync_committed_version().await;

            let sync_result3 = test_env
                .storage
                .seal_and_sync_epoch(epoch3, table_id_set)
                .await
                .unwrap();
            test_env
                .meta_client
                .commit_epoch(epoch3, sync_result3)
                .await
                .unwrap();
            test_env.wait_sync_committed_version().await;
            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch1,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch1,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch2,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                assert_eq!(
                    hummock_storage
                        .read_version()
                        .read()
                        .committed()
                        .table_committed_epoch(TEST_TABLE_ID)
                        .unwrap(),
                    read_snapshot
                        .2
                        .table_committed_epoch(TEST_TABLE_ID)
                        .unwrap()
                );

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch2,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(20, result.len());
            }

            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch2,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch2,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            retention_seconds: Some(0),
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(10, result.len());
            }

            {
                let (_, read_snapshot) = read_filter_for_version(
                    epoch2,
                    TEST_TABLE_ID,
                    prefixed_range_with_vnode(
                        (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                        VirtualNode::ZERO,
                    ),
                    &hummock_storage.read_version(),
                )
                .unwrap();

                let iter = hummock_version_reader
                    .iter(
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        epoch3,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            prefetch_options: PrefetchOptions::default(),
                            cache_policy: CachePolicy::Fill(CacheHint::Normal),
                            ..Default::default()
                        },
                        read_snapshot,
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item);

                let result: Vec<_> = iter.try_collect().await.unwrap();
                assert_eq!(30, result.len());
            }

            {
                let start_key = gen_key(25);
                let end_key = gen_key(50);

                let key_range = (Included(start_key), Excluded(end_key));

                {
                    let (_, read_snapshot) = read_filter_for_version(
                        epoch2,
                        TEST_TABLE_ID,
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        &hummock_storage.read_version(),
                    )
                    .unwrap();

                    let iter = hummock_version_reader
                        .iter(
                            key_range.clone(),
                            epoch2,
                            ReadOptions {
                                table_id: TEST_TABLE_ID,
                                prefetch_options: PrefetchOptions::default(),
                                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                                ..Default::default()
                            },
                            read_snapshot,
                        )
                        .await
                        .unwrap()
                        .into_stream(to_owned_item);

                    let result: Vec<_> = iter.try_collect().await.unwrap();
                    assert_eq!(8, result.len());
                }

                {
                    let (_, read_snapshot) = read_filter_for_version(
                        epoch2,
                        TEST_TABLE_ID,
                        prefixed_range_with_vnode(
                            (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                            VirtualNode::ZERO,
                        ),
                        &hummock_storage.read_version(),
                    )
                    .unwrap();

                    let iter = hummock_version_reader
                        .iter(
                            key_range.clone(),
                            epoch3,
                            ReadOptions {
                                table_id: TEST_TABLE_ID,
                                prefetch_options: PrefetchOptions::default(),
                                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                                ..Default::default()
                            },
                            read_snapshot,
                        )
                        .await
                        .unwrap()
                        .into_stream(to_owned_item);

                    let result: Vec<_> = iter.try_collect().await.unwrap();
                    assert_eq!(18, result.len());
                }
            }
        }
    }
}

#[tokio::test]
async fn test_get_with_min_epoch() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let table_id_set = HashSet::from_iter([TEST_TABLE_ID]);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut hummock_storage = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let epoch1 = (31 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    let prev_epoch = test_env
        .manager
        .get_current_version()
        .await
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();
    hummock_storage
        .init_for_test_with_prev_epoch(epoch1, prev_epoch)
        .await
        .unwrap();

    let gen_key = |index: usize| -> TableKey<Bytes> {
        gen_key_from_str(VirtualNode::ZERO, format!("key_{}", index).as_str())
    };

    let gen_val = |index: usize| -> String { format!("val_{}", index) };

    // epoch 1 write
    let batch_epoch1: Vec<(TableKey<Bytes>, StorageValue)> = (0..10)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch1,
            WriteOptions {
                epoch: epoch1,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    let epoch2 = (32 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    hummock_storage.seal_current_epoch(epoch2, SealCurrentEpochOptions::for_test());
    // epoch 2 write
    let batch_epoch2: Vec<(TableKey<Bytes>, StorageValue)> = (20..30)
        .map(|index| (gen_key(index), StorageValue::new_put(gen_val(index))))
        .collect();

    hummock_storage
        .ingest_batch(
            batch_epoch2,
            WriteOptions {
                epoch: epoch2,
                table_id: TEST_TABLE_ID,
            },
        )
        .await
        .unwrap();

    hummock_storage.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());

    {
        // test before sync
        let k = gen_key(0);
        let prefix_hint = {
            let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + k.len());
            ret.put_u32(TEST_TABLE_ID.table_id());
            ret.put_slice(k.as_ref());
            ret
        };
        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch1,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch1,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(v.is_some());
        }

        {
            let v = test_env
                .storage
                .get(
                    k.clone(),
                    epoch2,
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        retention_seconds: Some(0),
                        prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                        prefetch_options: Default::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(v.is_none());
        }
    }

    // test after sync

    let sync_result1 = test_env
        .storage
        .seal_and_sync_epoch(epoch1, table_id_set.clone())
        .await
        .unwrap();
    let sync_result2 = test_env
        .storage
        .seal_and_sync_epoch(epoch2, table_id_set)
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch1, sync_result1)
        .await
        .unwrap();
    test_env
        .meta_client
        .commit_epoch(epoch2, sync_result2)
        .await
        .unwrap();

    test_env.wait_sync_committed_version().await;
    let k = gen_key(0);
    let prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + k.len());
        ret.put_u32(TEST_TABLE_ID.table_id());
        ret.put_slice(k.as_ref());
        ret
    };

    {
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch1,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    read_committed: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(v.is_some());
    }

    {
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch1,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    read_committed: true,
                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(v.is_some());
    }

    {
        let k = gen_key(0);
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch2,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(v.is_some());
    }

    {
        let k = gen_key(0);
        let v = test_env
            .storage
            .get(
                k.clone(),
                epoch2,
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    retention_seconds: Some(0),

                    prefix_hint: Some(Bytes::from(prefix_hint.clone())),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(v.is_none());
    }
}

#[tokio::test]
async fn test_table_watermark() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table_id(TEST_TABLE_ID).await;
    let mut local1 = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let mut local2 = test_env
        .storage
        .new_local(NewLocalOptions::for_test(TEST_TABLE_ID))
        .await;

    let vnode1 = VirtualNode::from_index(1);
    let vnode_bitmap1 = Arc::new({
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        builder.set(1, true);
        builder.finish()
    });
    let vnode2 = VirtualNode::from_index(2);
    let vnode_bitmap2 = Arc::new({
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        builder.set(2, true);
        builder.finish()
    });

    let epoch1 = (31 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch1, HashSet::from_iter([TEST_TABLE_ID]));
    let prev_epoch = test_env
        .manager
        .get_current_version()
        .await
        .table_committed_epoch(TEST_TABLE_ID)
        .unwrap();
    local1
        .init_for_test_with_prev_epoch(epoch1, prev_epoch)
        .await
        .unwrap();
    local1
        .update_vnode_bitmap(vnode_bitmap1.clone())
        .await
        .unwrap();
    local2
        .init_for_test_with_prev_epoch(epoch1, prev_epoch)
        .await
        .unwrap();
    local2
        .update_vnode_bitmap(vnode_bitmap2.clone())
        .await
        .unwrap();

    fn gen_inner_key(index: usize) -> Bytes {
        Bytes::copy_from_slice(format!("key_{:05}", index).as_bytes())
    }

    fn gen_key(vnode: VirtualNode, index: usize) -> TableKey<Bytes> {
        gen_key_from_bytes(vnode, &gen_inner_key(index))
    }

    fn gen_val(index: usize) -> Bytes {
        Bytes::copy_from_slice(format!("val_{}", index).as_bytes())
    }

    fn gen_range() -> Range<usize> {
        0..30
    }

    fn gen_batch(
        vnode: VirtualNode,
        index: impl Iterator<Item = usize>,
    ) -> Vec<(TableKey<Bytes>, Bytes)> {
        index
            .map(|index| (gen_key(vnode, index), gen_val(index)))
            .collect_vec()
    }

    let epoch1_indexes = || gen_range().filter(|index| index % 3 == 0);

    // epoch 1 write
    let batch1_epoch1 = gen_batch(vnode1, epoch1_indexes());
    let batch2_epoch1 = gen_batch(vnode2, epoch1_indexes());

    for (local, batch) in [(&mut local1, batch1_epoch1), (&mut local2, batch2_epoch1)] {
        for (key, value) in batch {
            local.insert(key, value, None).unwrap();
        }
    }

    // test read after write
    {
        for (local, vnode) in [(&local1, vnode1), (&local2, vnode2)] {
            for index in epoch1_indexes() {
                let value = risingwave_storage::store::LocalStateStore::get(
                    local,
                    gen_key(vnode, index),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                assert_eq!(value.unwrap(), gen_val(index));
            }
            let result = risingwave_storage::store::LocalStateStore::iter(
                local,
                RangeBoundsExt::map(&gen_range(), |index| gen_key(vnode, *index)),
                ReadOptions {
                    table_id: TEST_TABLE_ID,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .into_stream(to_owned_item)
            .map_ok(|(full_key, value)| (full_key.user_key, value))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            let expected = epoch1_indexes()
                .map(|index| {
                    (
                        UserKey::new(TEST_TABLE_ID, gen_key(vnode, index)),
                        gen_val(index),
                    )
                })
                .collect_vec();
            assert_eq!(expected, result);
        }
    }

    let watermark1 = 10;

    let epoch2 = (32 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch2, HashSet::from_iter([TEST_TABLE_ID]));
    for (local, vnode_bitmap) in [
        (&mut local1, vnode_bitmap1.clone()),
        (&mut local2, vnode_bitmap2.clone()),
    ] {
        local.flush().await.unwrap();
        local.seal_current_epoch(
            epoch2,
            SealCurrentEpochOptions {
                table_watermarks: Some((
                    WatermarkDirection::Ascending,
                    vec![VnodeWatermark::new(vnode_bitmap, gen_inner_key(watermark1))],
                    WatermarkSerdeType::PkPrefix,
                )),
                switch_op_consistency_level: None,
            },
        );
    }

    // test read after seal with watermark1
    {
        for (local, vnode) in [(&local1, vnode1), (&local2, vnode2)] {
            for index in epoch1_indexes() {
                let value = risingwave_storage::store::LocalStateStore::get(
                    local,
                    gen_key(vnode, index),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                if index < watermark1 {
                    assert!(value.is_none());
                } else {
                    assert_eq!(value.unwrap(), gen_val(index));
                }
            }

            // iter full range
            {
                let result = risingwave_storage::store::LocalStateStore::iter(
                    local,
                    RangeBoundsExt::map(&gen_range(), |index| gen_key(vnode, *index)),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item)
                .map_ok(|(full_key, value)| (full_key.user_key, value))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
                let expected = epoch1_indexes()
                    .filter(|index| index >= &watermark1)
                    .map(|index| {
                        (
                            UserKey::new(TEST_TABLE_ID, gen_key(vnode, index)),
                            gen_val(index),
                        )
                    })
                    .collect_vec();
                assert_eq!(expected, result);
            }

            // iter below watermark
            {
                let result = risingwave_storage::store::LocalStateStore::iter(
                    local,
                    (
                        Included(gen_key(vnode, 0)),
                        Included(gen_key(vnode, watermark1 - 1)),
                    ),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
                assert!(result.is_empty());
            }
        }
    }

    let epoch2_indexes = || {
        gen_range()
            .filter(|index| index % 3 == 1)
            .filter(|index| index >= &watermark1)
    };

    // epoch 2 write
    let batch1_epoch2 = gen_batch(vnode1, epoch2_indexes());
    let batch2_epoch2 = gen_batch(vnode2, epoch2_indexes());

    let epoch3 = (33 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch3, HashSet::from_iter([TEST_TABLE_ID]));

    for (local, batch) in [(&mut local1, batch1_epoch2), (&mut local2, batch2_epoch2)] {
        for (key, value) in batch {
            local.insert(key, value, None).unwrap();
        }
        local.flush().await.unwrap();
        local.seal_current_epoch(
            epoch3,
            SealCurrentEpochOptions {
                table_watermarks: None,
                switch_op_consistency_level: None,
            },
        );
    }

    let indexes_after_epoch2 = || gen_range().filter(|index| index % 3 == 0 || index % 3 == 1);

    let test_after_epoch2 = |local1: LocalHummockStorage, local2: LocalHummockStorage| async {
        for (local, vnode) in [(&local1, vnode1), (&local2, vnode2)] {
            for index in indexes_after_epoch2() {
                let value = risingwave_storage::store::LocalStateStore::get(
                    local,
                    gen_key(vnode, index),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                if index < watermark1 {
                    assert!(value.is_none());
                } else {
                    assert_eq!(value.unwrap(), gen_val(index));
                }
            }

            // iter full range
            {
                let result = risingwave_storage::store::LocalStateStore::iter(
                    local,
                    RangeBoundsExt::map(&gen_range(), |index| gen_key(vnode, *index)),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item)
                .map_ok(|(full_key, value)| (full_key.user_key, value))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
                let expected = indexes_after_epoch2()
                    .filter(|index| index >= &watermark1)
                    .map(|index| {
                        (
                            UserKey::new(TEST_TABLE_ID, gen_key(vnode, index)),
                            gen_val(index),
                        )
                    })
                    .collect_vec();
                assert_eq!(expected, result);
            }

            // iter below watermark
            {
                let result = risingwave_storage::store::LocalStateStore::iter(
                    local,
                    (
                        Included(gen_key(vnode, 0)),
                        Included(gen_key(vnode, watermark1 - 1)),
                    ),
                    ReadOptions {
                        table_id: TEST_TABLE_ID,
                        ..Default::default()
                    },
                )
                .await
                .unwrap()
                .into_stream(to_owned_item)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
                assert!(result.is_empty());
            }
        }
        (local1, local2)
    };

    let (local1, local2) = test_after_epoch2(local1, local2).await;

    let check_version_table_watermark = |version: PinnedVersion| {
        let epoch = version
            .state_table_info
            .info()
            .get(&TEST_TABLE_ID)
            .unwrap()
            .committed_epoch;
        let table_watermarks = TableWatermarksIndex::new_committed(
            version
                .table_watermarks
                .get(&TEST_TABLE_ID)
                .unwrap()
                .clone(),
            epoch,
        );
        assert_eq!(WatermarkDirection::Ascending, table_watermarks.direction());
        assert_eq!(
            gen_inner_key(watermark1),
            table_watermarks.read_watermark(vnode1, epoch).unwrap()
        );
        assert_eq!(
            gen_inner_key(watermark1),
            table_watermarks.read_watermark(vnode2, epoch).unwrap()
        );
    };

    test_env.commit_epoch(epoch1).await;
    test_env.wait_sync_committed_version().await;

    let (local1, local2) = test_after_epoch2(local1, local2).await;

    let test_global_read = |storage: HummockStorage, epoch: u64| async move {
        // inner vnode read
        for vnode in [vnode1, vnode2] {
            for index in indexes_after_epoch2() {
                let value = storage
                    .get(
                        gen_key(vnode, index),
                        epoch,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                if index < watermark1 {
                    assert!(value.is_none());
                } else {
                    println!("index {} vnode {}", index, vnode);
                    assert_eq!(value.unwrap(), gen_val(index));
                }
            }

            // iter full range
            {
                let result = storage
                    .iter(
                        RangeBoundsExt::map(&gen_range(), |index| gen_key(vnode, *index)),
                        epoch,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item)
                    .map_ok(|(full_key, value)| (full_key.user_key, value))
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let expected = indexes_after_epoch2()
                    .filter(|index| index >= &watermark1)
                    .map(|index| {
                        (
                            UserKey::new(TEST_TABLE_ID, gen_key(vnode, index)),
                            gen_val(index),
                        )
                    })
                    .collect_vec();
                assert_eq!(expected, result);
            }

            // iter below watermark
            {
                let result = storage
                    .iter(
                        (
                            Included(gen_key(vnode, 0)),
                            Included(gen_key(vnode, watermark1 - 1)),
                        ),
                        epoch,
                        ReadOptions {
                            table_id: TEST_TABLE_ID,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap()
                    .into_stream(to_owned_item)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                assert!(result.is_empty());
            }
        }
    };

    test_global_read(test_env.storage.clone(), epoch2).await;

    check_version_table_watermark(test_env.storage.get_pinned_version());

    let (local1, local2) = test_after_epoch2(local1, local2).await;

    test_env.commit_epoch(epoch2).await;
    test_env.wait_sync_committed_version().await;

    test_global_read(test_env.storage.clone(), epoch2).await;

    check_version_table_watermark(test_env.storage.get_pinned_version());

    let (mut local1, mut local2) = test_after_epoch2(local1, local2).await;

    let epoch4 = (34 * 1000) << 16;
    test_env
        .storage
        .start_epoch(epoch4, HashSet::from_iter([TEST_TABLE_ID]));

    for (local, vnode_bitmap) in [
        (&mut local1, vnode_bitmap1.clone()),
        (&mut local2, vnode_bitmap2.clone()),
    ] {
        // regress watermark
        local.seal_current_epoch(
            epoch4,
            SealCurrentEpochOptions {
                table_watermarks: Some((
                    WatermarkDirection::Ascending,
                    vec![VnodeWatermark::new(vnode_bitmap, gen_inner_key(5))],
                    WatermarkSerdeType::PkPrefix,
                )),
                switch_op_consistency_level: None,
            },
        );
    }

    test_global_read(test_env.storage.clone(), epoch3).await;

    let (local1, local2) = test_after_epoch2(local1, local2).await;

    test_env.commit_epoch(epoch3).await;
    test_env.wait_sync_committed_version().await;

    check_version_table_watermark(test_env.storage.get_pinned_version());

    let (_local1, _local2) = test_after_epoch2(local1, local2).await;

    test_global_read(test_env.storage.clone(), epoch3).await;
}

#[tokio::test]
async fn test_commit_multi_epoch() {
    let test_env = prepare_hummock_test_env().await;
    let context_id = test_env.meta_client.context_id();
    let context_id_map = |object_ids: &[_]| {
        HashMap::from_iter(object_ids.iter().map(|object_id| (*object_id, context_id)))
    };
    let existing_table_id = TableId::new(1);
    let initial_epoch = INVALID_EPOCH;

    let commit_epoch =
        |epoch, sst: SstableInfo, new_table_fragment_infos, tables_to_commit: &[TableId]| {
            let manager = &test_env.manager;
            let tables_to_commit = tables_to_commit
                .iter()
                .map(|table_id| (*table_id, epoch))
                .collect();
            async move {
                manager
                    .commit_epoch(CommitEpochInfo {
                        new_table_watermarks: Default::default(),
                        sst_to_context: context_id_map(&[sst.object_id]),
                        sstables: vec![LocalSstableInfo {
                            table_stats: sst
                                .table_ids
                                .iter()
                                .map(|&table_id| {
                                    (
                                        table_id,
                                        TableStats {
                                            total_compressed_size: 10,
                                            ..Default::default()
                                        },
                                    )
                                })
                                .collect(),
                            sst_info: sst,
                            created_at: u64::MAX,
                        }],
                        new_table_fragment_infos,
                        change_log_delta: Default::default(),
                        tables_to_commit,
                    })
                    .await
                    .unwrap();
            }
        };

    let epoch1 = initial_epoch.next_epoch();
    let sst1_epoch1: SstableInfo = {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "key_1");
        let table_key_r = gen_key_from_str(VirtualNode::ZERO, "key_2");
        SstableInfoInner {
            sst_id: 11,
            object_id: 1,
            table_ids: vec![existing_table_id.table_id],
            file_size: 100,
            sst_size: 100,
            key_range: KeyRange {
                left: FullKey::for_test(existing_table_id, table_key_l, epoch1)
                    .encode()
                    .into(),
                right: FullKey::for_test(existing_table_id, table_key_r, epoch1)
                    .encode()
                    .into(),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    };

    commit_epoch(
        epoch1,
        sst1_epoch1.clone(),
        vec![NewTableFragmentInfo {
            table_ids: HashSet::from_iter([existing_table_id]),
        }],
        &[existing_table_id],
    )
    .await;

    let cg_id =
        get_compaction_group_id_by_table_id(test_env.manager.clone(), existing_table_id.table_id())
            .await;

    {
        let version = test_env.manager.get_current_version().await;
        let cg = version.levels.get(&(cg_id)).unwrap();
        let sub_levels = &cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 1);
        let sub_level = &sub_levels[0];
        assert_eq!(sub_level.table_infos.len(), 1);
        assert_eq!(sub_level.table_infos[0].object_id, sst1_epoch1.object_id);

        let info = version
            .state_table_info
            .info()
            .get(&existing_table_id)
            .unwrap();
        assert_eq!(epoch1, info.committed_epoch);
        assert_eq!(cg_id, info.compaction_group_id);
    }

    let epoch2 = epoch1.next_epoch();
    let sst1_epoch2: SstableInfo = {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "key_1");
        let table_key_r = gen_key_from_str(VirtualNode::ZERO, "key_2");
        SstableInfoInner {
            sst_id: 22,
            object_id: 2,
            table_ids: vec![existing_table_id.table_id],
            file_size: 100,
            sst_size: 100,
            key_range: KeyRange {
                left: FullKey::for_test(existing_table_id, table_key_l, epoch2)
                    .encode()
                    .into(),
                right: FullKey::for_test(existing_table_id, table_key_r, epoch2)
                    .encode()
                    .into(),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    };

    commit_epoch(epoch2, sst1_epoch2.clone(), vec![], &[existing_table_id]).await;

    {
        let version = test_env.manager.get_current_version().await;
        let cg = version.levels.get(&(cg_id)).unwrap();
        let sub_levels = &cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 2);
        let sub_level = &sub_levels[0];
        assert_eq!(sub_level.table_infos.len(), 1);
        assert_eq!(sub_level.table_infos[0].object_id, sst1_epoch1.object_id);
        let sub_level = &sub_levels[1];
        assert_eq!(sub_level.table_infos.len(), 1);
        assert_eq!(sub_level.table_infos[0].object_id, sst1_epoch2.object_id);

        let info = version
            .state_table_info
            .info()
            .get(&existing_table_id)
            .unwrap();
        assert_eq!(epoch2, info.committed_epoch);
        assert_eq!(cg_id, info.compaction_group_id);
    };

    let new_table_id = TableId::new(2);

    let sst2_epoch1: SstableInfo = {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "key_1");
        let table_key_r = gen_key_from_str(VirtualNode::ZERO, "key_2");
        SstableInfoInner {
            sst_id: 33,
            object_id: 3,
            table_ids: vec![new_table_id.table_id],
            file_size: 100,
            sst_size: 100,
            key_range: KeyRange {
                left: FullKey::for_test(new_table_id, table_key_l, epoch1)
                    .encode()
                    .into(),
                right: FullKey::for_test(new_table_id, table_key_r, epoch1)
                    .encode()
                    .into(),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    };

    commit_epoch(
        epoch1,
        sst2_epoch1.clone(),
        vec![NewTableFragmentInfo {
            table_ids: HashSet::from_iter([new_table_id]),
        }],
        &[new_table_id],
    )
    .await;

    let new_cg_id = {
        let version = test_env.manager.get_current_version().await;
        let new_cg_id =
            get_compaction_group_id_by_table_id(test_env.manager.clone(), new_table_id.table_id())
                .await;

        let new_cg = version.levels.get(&new_cg_id).unwrap();
        let sub_levels = &new_cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 1);
        let sub_level1 = &sub_levels[0];
        assert_eq!(sub_level1.table_infos.len(), 1);
        assert_eq!(sub_level1.table_infos[0].object_id, sst2_epoch1.object_id);

        let info = version.state_table_info.info().get(&new_table_id).unwrap();
        assert_eq!(info.committed_epoch, epoch1);
        assert_eq!(info.compaction_group_id, new_cg_id);

        new_cg_id
    };

    let sst2_epoch2: SstableInfo = {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "key_1");
        let table_key_r = gen_key_from_str(VirtualNode::ZERO, "key_2");
        SstableInfoInner {
            sst_id: 44,
            object_id: 4,
            table_ids: vec![new_table_id.table_id],
            file_size: 100,
            sst_size: 100,
            key_range: KeyRange {
                left: FullKey::for_test(new_table_id, table_key_l, epoch2)
                    .encode()
                    .into(),
                right: FullKey::for_test(new_table_id, table_key_r, epoch2)
                    .encode()
                    .into(),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    };

    commit_epoch(epoch2, sst2_epoch2.clone(), vec![], &[new_table_id]).await;

    {
        let version = test_env.manager.get_current_version().await;

        let new_cg = version.levels.get(&new_cg_id).unwrap();
        let sub_levels = &new_cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 2);
        let sub_level1 = &sub_levels[0];
        assert_eq!(sub_level1.table_infos.len(), 1);
        assert_eq!(sub_level1.table_infos[0].object_id, sst2_epoch1.object_id);
        let sub_level2 = &sub_levels[1];
        assert_eq!(sub_level2.table_infos.len(), 1);
        assert_eq!(sub_level2.table_infos[0].object_id, sst2_epoch2.object_id);

        let info = version.state_table_info.info().get(&new_table_id).unwrap();
        assert_eq!(info.committed_epoch, epoch2);
        assert_eq!(info.compaction_group_id, new_cg_id);
    }

    let epoch3 = epoch2.next_epoch();

    let sst_epoch3: SstableInfo = {
        let table_key_l = gen_key_from_str(VirtualNode::ZERO, "key_1");
        let table_key_r = gen_key_from_str(VirtualNode::ZERO, "key_2");
        SstableInfoInner {
            sst_id: 55,
            object_id: 5,
            table_ids: vec![existing_table_id.table_id, new_table_id.table_id],
            file_size: 100,
            sst_size: 100,
            key_range: KeyRange {
                left: FullKey::for_test(existing_table_id, table_key_l, epoch3)
                    .encode()
                    .into(),
                right: FullKey::for_test(new_table_id, table_key_r, epoch3)
                    .encode()
                    .into(),
                right_exclusive: false,
            },
            ..Default::default()
        }
        .into()
    };

    commit_epoch(
        epoch3,
        sst_epoch3.clone(),
        vec![],
        &[existing_table_id, new_table_id],
    )
    .await;

    {
        let version = test_env.manager.get_current_version().await;
        let old_cg = version.levels.get(&cg_id).unwrap();
        let sub_levels = &old_cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 3);
        let sub_level1 = &sub_levels[0];
        assert_eq!(sub_level1.table_infos.len(), 1);
        assert_eq!(sub_level1.table_infos[0].object_id, sst1_epoch1.object_id);
        let sub_level2 = &sub_levels[1];
        assert_eq!(sub_level2.table_infos.len(), 1);
        assert_eq!(sub_level2.table_infos[0].object_id, sst1_epoch2.object_id);
        let sub_level3 = &sub_levels[2];
        assert_eq!(sub_level3.table_infos.len(), 1);
        assert_eq!(sub_level3.table_infos[0].object_id, sst_epoch3.object_id);

        let new_cg = version.levels.get(&new_cg_id).unwrap();
        let sub_levels = &new_cg.l0.sub_levels;
        assert_eq!(sub_levels.len(), 3);
        let sub_level1 = &sub_levels[0];
        assert_eq!(sub_level1.table_infos.len(), 1);
        assert_eq!(sub_level1.table_infos[0].object_id, sst2_epoch1.object_id);
        let sub_level2 = &sub_levels[1];
        assert_eq!(sub_level2.table_infos.len(), 1);
        assert_eq!(sub_level2.table_infos[0].object_id, sst2_epoch2.object_id);
        let sub_level3 = &sub_levels[1];
        assert_eq!(sub_level3.table_infos.len(), 1);
        assert_eq!(sub_level3.table_infos[0].object_id, sst2_epoch2.object_id);

        let info = version.state_table_info.info().get(&new_table_id).unwrap();
        assert_eq!(info.committed_epoch, epoch3);

        let info = version
            .state_table_info
            .info()
            .get(&existing_table_id)
            .unwrap();
        assert_eq!(info.committed_epoch, epoch3);
    }
}

#[tokio::test]
async fn test_commit_with_large_size() {
    let test_env = prepare_hummock_test_env().await;
    let context_id = test_env.meta_client.context_id();
    let existing_table_id = TableId::new(1);
    let initial_epoch = INVALID_EPOCH;

    let commit_epoch =
        |epoch, ssts: Vec<SstableInfo>, new_table_fragment_infos, tables_to_commit: &[TableId]| {
            let manager = &test_env.manager;
            let tables_to_commit = tables_to_commit
                .iter()
                .map(|table_id| (*table_id, epoch))
                .collect();
            let sst_to_context = ssts.iter().map(|sst| (sst.object_id, context_id)).collect();

            let sstables = ssts
                .into_iter()
                .map(|sst| LocalSstableInfo {
                    table_stats: sst
                        .table_ids
                        .iter()
                        .map(|&table_id| {
                            (
                                table_id,
                                TableStats {
                                    total_compressed_size: 10,
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                    sst_info: sst,
                    created_at: u64::MAX,
                })
                .collect_vec();

            async move {
                manager
                    .commit_epoch(CommitEpochInfo {
                        new_table_watermarks: Default::default(),
                        sst_to_context,
                        sstables,
                        new_table_fragment_infos,
                        change_log_delta: Default::default(),
                        tables_to_commit,
                    })
                    .await
                    .unwrap();
            }
        };

    let epoch1 = initial_epoch.next_epoch();
    let sst1_epoch1: SstableInfo = SstableInfoInner {
        sst_id: 11,
        object_id: 1,
        table_ids: vec![existing_table_id.table_id],
        file_size: 512 << 20,
        sst_size: 512 << 20,
        ..Default::default()
    }
    .into();

    let sst1_epoch2: SstableInfo = SstableInfoInner {
        sst_id: 12,
        object_id: 2,
        table_ids: vec![existing_table_id.table_id],
        file_size: 512 << 20,
        sst_size: 512 << 20,
        ..Default::default()
    }
    .into();

    let sst1_epoch3: SstableInfo = SstableInfoInner {
        sst_id: 13,
        object_id: 3,
        table_ids: vec![existing_table_id.table_id],
        file_size: 512 << 20,
        sst_size: 512 << 20,
        ..Default::default()
    }
    .into();

    commit_epoch(
        epoch1,
        vec![
            sst1_epoch3.clone(),
            sst1_epoch2.clone(),
            sst1_epoch1.clone(),
        ],
        vec![NewTableFragmentInfo {
            table_ids: HashSet::from_iter([existing_table_id]),
        }],
        &[existing_table_id],
    )
    .await;

    let cg_id =
        get_compaction_group_id_by_table_id(test_env.manager.clone(), existing_table_id.table_id())
            .await;

    let l0_sub_levels = test_env
        .manager
        .get_current_version()
        .await
        .levels
        .get(&cg_id)
        .unwrap()
        .l0
        .clone();

    println!("l0_sub_levels {:?}", l0_sub_levels.sub_levels);
    assert_eq!(3, l0_sub_levels.sub_levels.len());
    assert_eq!(1, l0_sub_levels.sub_levels[0].table_infos.len());
    assert_eq!(
        sst1_epoch1.object_id,
        l0_sub_levels.sub_levels[0].table_infos[0].object_id
    );
    assert_eq!(1, l0_sub_levels.sub_levels[1].table_infos.len());
    assert_eq!(
        sst1_epoch2.object_id,
        l0_sub_levels.sub_levels[1].table_infos[0].object_id
    );
    assert_eq!(1, l0_sub_levels.sub_levels[2].table_infos.len());
    assert_eq!(
        sst1_epoch3.object_id,
        l0_sub_levels.sub_levels[2].table_infos[0].object_id
    );
}
