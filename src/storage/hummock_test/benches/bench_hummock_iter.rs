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

use std::ops::Bound::Unbounded;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use foyer::CacheHint;
use futures::pin_mut;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::key::TableKey;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_hummock_test::get_notification_client_for_test;
use risingwave_hummock_test::local_state_store_test_utils::LocalStateStoreTestExt;
use risingwave_hummock_test::test_utils::TestIngestBatch;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::hummock::MockHummockMetaClient;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::{CachePolicy, HummockStorage};
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;

fn gen_interleave_shared_buffer_batch_iter(
    batch_size: usize,
    batch_count: usize,
) -> Vec<Vec<(TableKey<Bytes>, StorageValue)>> {
    let mut ret = Vec::new();
    for i in 0..batch_count {
        let mut batch_data = vec![];
        for j in 0..batch_size {
            batch_data.push((
                TableKey(Bytes::copy_from_slice(
                    format!("test_key_{:08}", j * batch_count + i).as_bytes(),
                )),
                StorageValue::new_put(Bytes::copy_from_slice("value".as_bytes())),
            ));
        }
        ret.push(batch_data);
    }
    ret
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let batches = gen_interleave_shared_buffer_batch_iter(10000, 100);
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) =
        runtime.block_on(setup_compute_env(8080));
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));

    let global_hummock_storage = runtime.block_on(async {
        let sstable_store = mock_sstable_store().await;
        HummockStorage::for_test(
            hummock_options,
            sstable_store,
            meta_client.clone(),
            get_notification_client_for_test(env, hummock_manager_ref, cluster_ctl_ref, worker_id)
                .await,
        )
        .await
        .unwrap()
    });

    let mut hummock_storage = runtime.block_on(async {
        global_hummock_storage
            .new_local(NewLocalOptions::for_test(Default::default()))
            .await
    });

    let epoch = test_epoch(100);
    runtime
        .block_on(hummock_storage.init_for_test(epoch))
        .unwrap();

    for batch in batches {
        runtime
            .block_on(hummock_storage.ingest_batch(
                batch,
                WriteOptions {
                    epoch,
                    table_id: Default::default(),
                },
            ))
            .unwrap();
    }
    hummock_storage.seal_current_epoch(HummockEpoch::MAX, SealCurrentEpochOptions::for_test());

    c.bench_function("bench-hummock-iter", move |b| {
        b.iter(|| {
            let iter = runtime
                .block_on(global_hummock_storage.iter(
                    (Unbounded, Unbounded),
                    epoch,
                    ReadOptions {
                        prefetch_options: PrefetchOptions::default(),
                        cache_policy: CachePolicy::Fill(CacheHint::Normal),
                        ..Default::default()
                    },
                ))
                .unwrap();
            runtime.block_on(async move {
                let mut count = 0;
                pin_mut!(iter);
                while iter.try_next().await.unwrap().is_some() {
                    count += 1;
                }
                assert_eq!(count, 1000000);
            });
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
