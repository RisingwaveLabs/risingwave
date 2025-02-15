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

use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use foyer::CacheHint;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use risingwave_hummock_sdk::key::prefixed_range_with_vnode;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, PrefetchOptions, ReadOptions, SealCurrentEpochOptions,
    TryWaitEpochOptions, WriteOptions,
};
use risingwave_storage::StateStore;

use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::{gen_key_from_bytes, with_hummock_storage, TestIngestBatch};

macro_rules! assert_count_range_scan {
    (
        $storage:expr,
        $vnode:expr,
        $range:expr,
        $expect_count:expr,
        $epoch:expr,
        $read_committed:expr
    ) => {{
        use std::ops::RangeBounds;

        use risingwave_storage::StateStoreIter;
        let range = $range;
        let bounds: (Bound<Bytes>, Bound<Bytes>) = (
            range.start_bound().map(|x: &Bytes| x.clone()),
            range.end_bound().map(|x: &Bytes| x.clone()),
        );
        let vnode = $vnode;
        let table_key_range = prefixed_range_with_vnode(bounds, vnode);
        let mut it = $storage
            .iter(
                table_key_range,
                $epoch,
                ReadOptions {
                    prefetch_options: PrefetchOptions::prefetch_for_large_range_scan(),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    read_committed: $read_committed,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut count = 0;
        loop {
            match it.try_next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

#[allow(unused_macros)]
macro_rules! assert_count_backward_range_scan {
    ($storage:expr, $range:expr, $expect_count:expr, $epoch:expr) => {{
        use std::ops::RangeBounds;
        let range = $range;
        let bounds: (Bound<Vec<u8>>, Bound<Vec<u8>>) = (
            range.start_bound().map(|x: &Bytes| x.to_vec()),
            range.end_bound().map(|x: &Bytes| x.to_vec()),
        );
        let it = $storage
            .backward_iter(
                bounds,
                ReadOptions {
                    ignore_range_tombstone: false,
                    epoch: $epoch,
                    table_id: Default::default(),
                    retention_seconds: None,
                    read_version_from_backup: false,
                },
            )
            .await
            .unwrap();
        futures::pin_mut!(it);
        let mut count = 0;
        loop {
            match it.try_next().await.unwrap() {
                Some(_) => count += 1,
                None => break,
            }
        }
        assert_eq!(count, $expect_count);
    }};
}

async fn test_snapshot_inner(
    hummock_storage: HummockStorage,
    mock_hummock_meta_client: Arc<MockHummockMetaClient>,
    enable_sync: bool,
    enable_commit: bool,
) {
    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;

    let epoch1 = test_epoch(1);
    hummock_storage.start_epoch(epoch1, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch1).await.unwrap();
    local
        .ingest_batch(
            vec![
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("1")),
                    StorageValue::new_put("test"),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("2")),
                    StorageValue::new_put("test"),
                ),
            ],
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
    if enable_sync {
        let res = hummock_storage
            .seal_and_sync_epoch(epoch1, HashSet::from_iter([local.table_id()]))
            .await
            .unwrap();
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch1, res)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(
                    HummockReadEpoch::Committed(epoch1),
                    TryWaitEpochOptions::for_test(Default::default()),
                )
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(hummock_storage, VirtualNode::ZERO, .., 2, epoch1, false);

    local
        .ingest_batch(
            vec![
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("1")),
                    StorageValue::new_delete(),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("3")),
                    StorageValue::new_put("test"),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("4")),
                    StorageValue::new_put("test"),
                ),
            ],
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
    if enable_sync {
        let res = hummock_storage
            .seal_and_sync_epoch(epoch2, HashSet::from_iter([local.table_id()]))
            .await
            .unwrap();
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch2, res)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(
                    HummockReadEpoch::Committed(epoch2),
                    TryWaitEpochOptions::for_test(Default::default()),
                )
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(hummock_storage, VirtualNode::ZERO, .., 3, epoch2, false);
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..,
        2,
        epoch1,
        enable_commit
    );

    local
        .ingest_batch(
            vec![
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("2")),
                    StorageValue::new_delete(),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("3")),
                    StorageValue::new_delete(),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("4")),
                    StorageValue::new_delete(),
                ),
            ],
            WriteOptions {
                epoch: epoch3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    if enable_sync {
        let res = hummock_storage
            .seal_and_sync_epoch(epoch3, HashSet::from_iter([local.table_id()]))
            .await
            .unwrap();
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch3, res)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(
                    HummockReadEpoch::Committed(epoch3),
                    TryWaitEpochOptions::for_test(Default::default()),
                )
                .await
                .unwrap();
        }
    }
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..,
        0,
        epoch3,
        enable_commit
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..,
        3,
        epoch2,
        enable_commit
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..,
        2,
        epoch1,
        enable_commit
    );
}

async fn test_snapshot_range_scan_inner(
    hummock_storage: HummockStorage,
    mock_hummock_meta_client: Arc<MockHummockMetaClient>,
    enable_sync: bool,
    enable_commit: bool,
) {
    let epoch = test_epoch(1);
    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(Default::default()))
        .await;
    hummock_storage.start_epoch(epoch, HashSet::from_iter([Default::default()]));
    local.init_for_test(epoch).await.unwrap();

    local
        .ingest_batch(
            vec![
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("1")),
                    StorageValue::new_put("test"),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("2")),
                    StorageValue::new_put("test"),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("3")),
                    StorageValue::new_put("test"),
                ),
                (
                    gen_key_from_bytes(VirtualNode::ZERO, &Bytes::from("4")),
                    StorageValue::new_put("test"),
                ),
            ],
            WriteOptions {
                epoch,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();
    local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
    if enable_sync {
        let res = hummock_storage
            .seal_and_sync_epoch(epoch, HashSet::from_iter([local.table_id()]))
            .await
            .unwrap();
        if enable_commit {
            mock_hummock_meta_client
                .commit_epoch(epoch, res)
                .await
                .unwrap();
            hummock_storage
                .try_wait_epoch(
                    HummockReadEpoch::Committed(epoch),
                    TryWaitEpochOptions::for_test(local.table_id()),
                )
                .await
                .unwrap();
        }
    }
    macro_rules! key {
        ($idx:expr) => {
            Bytes::from(stringify!($idx))
        };
    }

    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        key!(2)..=key!(3),
        2,
        epoch,
        false
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        key!(2)..key!(3),
        1,
        epoch,
        false
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        key!(2)..,
        3,
        epoch,
        false
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..=key!(3),
        3,
        epoch,
        false
    );
    assert_count_range_scan!(
        hummock_storage,
        VirtualNode::ZERO,
        ..key!(3),
        2,
        epoch,
        false
    );
    assert_count_range_scan!(hummock_storage, VirtualNode::ZERO, .., 4, epoch, false);
}

#[tokio::test]
async fn test_snapshot_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_with_sync_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_with_commit_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_inner(storage, meta_client, true, true).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_range_scan_inner(storage, meta_client, false, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_sync_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_range_scan_inner(storage, meta_client, true, false).await;
}

#[tokio::test]
async fn test_snapshot_range_scan_with_commit_v2() {
    let (storage, meta_client) = with_hummock_storage(Default::default()).await;
    test_snapshot_range_scan_inner(storage, meta_client, true, true).await;
}
