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

use bytes::{BufMut, Bytes};
use foyer::CacheHint;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::TABLE_PREFIX_LEN;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, PrefetchOptions, ReadOptions, TryWaitEpochOptions,
    WriteOptions,
};
use risingwave_storage::StateStore;

use crate::get_notification_client_for_test;
use crate::local_state_store_test_utils::LocalStateStoreTestExt;
use crate::test_utils::{gen_key_from_str, TestIngestBatch};

#[tokio::test]
#[ignore]
#[cfg(all(test, feature = "failpoints"))]
async fn test_failpoints_state_store_read_upload() {
    let mem_upload_err = "mem_upload_err";
    let mem_read_err = "mem_read_err";
    let sstable_store = mock_sstable_store().await;
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store.clone(),
        meta_client.clone(),
        get_notification_client_for_test(env, hummock_manager_ref, cluster_ctl_ref, worker_id)
            .await,
    )
    .await
    .unwrap();

    let mut local = hummock_storage
        .new_local(NewLocalOptions::for_test(TableId::default()))
        .await;

    let anchor = gen_key_from_str(VirtualNode::ZERO, "aa");
    let mut batch1 = vec![
        (anchor.clone(), StorageValue::new_put("111")),
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("222"),
        ),
    ];
    batch1.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

    let mut batch2 = vec![
        (
            gen_key_from_str(VirtualNode::ZERO, "aa"),
            StorageValue::new_put("333"),
        ),
        (anchor.clone(), StorageValue::new_delete()),
    ];
    // Make sure the batch is sorted.
    batch2.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
    local.init_for_test(1).await.unwrap();
    local
        .ingest_batch(
            batch1,
            WriteOptions {
                epoch: 1,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    local.seal_current_epoch(
        3,
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );

    // Get the value after flushing to remote.
    let anchor_prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + anchor.len());
        ret.put_u32(TableId::default().table_id());
        ret.put_slice(anchor.as_ref());
        ret
    };
    let value = hummock_storage
        .get(
            anchor.clone(),
            1,
            ReadOptions {
                prefix_hint: Some(Bytes::from(anchor_prefix_hint)),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    // // Write second batch.
    local
        .ingest_batch(
            batch2,
            WriteOptions {
                epoch: 3,
                table_id: Default::default(),
            },
        )
        .await
        .unwrap();

    local.seal_current_epoch(
        u64::MAX,
        risingwave_storage::store::SealCurrentEpochOptions::for_test(),
    );

    // sync epoch1 test the read_error
    let table_id_set = HashSet::from_iter([local.table_id()]);
    let res = hummock_storage
        .seal_and_sync_epoch(1, table_id_set.clone())
        .await
        .unwrap();
    meta_client.commit_epoch(1, res).await.unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(1),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();
    // clear block cache
    sstable_store.clear_block_cache().await.unwrap();
    sstable_store.clear_meta_cache().await.unwrap();
    fail::cfg(mem_read_err, "return").unwrap();

    let anchor_prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + anchor.len());
        ret.put_u32(TableId::default().table_id());
        ret.put_slice(anchor.as_ref());
        ret
    };
    let result = hummock_storage
        .get(
            anchor.clone(),
            2,
            ReadOptions {
                prefix_hint: Some(Bytes::from(anchor_prefix_hint)),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err());
    let result = hummock_storage
        .iter(
            (
                Bound::Unbounded,
                Bound::Included(gen_key_from_str(VirtualNode::ZERO, "ee")),
            ),
            2,
            ReadOptions {
                table_id: Default::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err());

    let bee_prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + b"ee".as_ref().len());
        ret.put_u32(TableId::default().table_id());
        ret.put_slice(b"ee".as_ref().as_ref());
        ret
    };
    let value = hummock_storage
        .get(
            gen_key_from_str(VirtualNode::ZERO, "ee"),
            2,
            ReadOptions {
                prefix_hint: Some(Bytes::from(bee_prefix_hint)),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(value.is_none());
    fail::remove(mem_read_err);
    // test the upload_error
    fail::cfg(mem_upload_err, "return").unwrap();

    let result = hummock_storage
        .seal_and_sync_epoch(3, table_id_set.clone())
        .await;
    assert!(result.is_err());
    fail::remove(mem_upload_err);

    let res = hummock_storage
        .seal_and_sync_epoch(3, table_id_set)
        .await
        .unwrap();
    meta_client.commit_epoch(3, res).await.unwrap();
    hummock_storage
        .try_wait_epoch(
            HummockReadEpoch::Committed(3),
            TryWaitEpochOptions::for_test(local.table_id()),
        )
        .await
        .unwrap();

    let anchor_prefix_hint = {
        let mut ret = Vec::with_capacity(TABLE_PREFIX_LEN + anchor.len());
        ret.put_u32(TableId::default().table_id());
        ret.put_slice(anchor.as_ref());
        ret
    };
    let value = hummock_storage
        .get(
            anchor.clone(),
            5,
            ReadOptions {
                prefix_hint: Some(Bytes::from(anchor_prefix_hint)),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(value, Bytes::from("111"));
    let iters = hummock_storage
        .iter(
            (
                Bound::Unbounded,
                Bound::Included(gen_key_from_str(VirtualNode::ZERO, "ee")),
            ),
            5,
            ReadOptions {
                prefetch_options: PrefetchOptions::default(),
                cache_policy: CachePolicy::Fill(CacheHint::Normal),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let len = count_stream(iters).await;
    assert_eq!(len, 2);
}
