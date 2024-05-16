// Copyright 2024 RisingWave Labs
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

use std::env;
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use foyer::HybridCacheBuilder;
use futures::future::try_join_all;
use itertools::Itertools;
use rand::random;
use risingwave_common::catalog::TableId;
use risingwave_common::config::{MetricLevel, ObjectStoreConfig};
use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreImpl, S3ObjectStore,
};
use risingwave_pb::hummock::SstableInfo;
use risingwave_storage::hummock::iterator::{ConcatIterator, ConcatIteratorInner, HummockIterator};
use risingwave_storage::hummock::multi_builder::{CapacitySplitTableBuilder, TableBuilderFactory};
use risingwave_storage::hummock::value::HummockValue;
use risingwave_storage::hummock::{
    BackwardSstableIterator, BatchSstableWriterFactory, CachePolicy, FileCache, HummockResult,
    MemoryLimiter, SstableBuilder, SstableBuilderOptions, SstableIteratorReadOptions, SstableStore,
    SstableStoreConfig, SstableWriterFactory, SstableWriterOptions, StreamingSstableWriterFactory,
    Xor16FilterBuilder,
};
use risingwave_storage::monitor::{global_hummock_state_store_metrics, ObjectStoreMetrics};

const RANGE: Range<u64> = 0..1500000;
const VALUE: &[u8] = &[0; 400];
const SAMPLE_COUNT: usize = 10;
const ESTIMATED_MEASUREMENT_TIME: Duration = Duration::from_secs(60);

struct LocalTableBuilderFactory<F: SstableWriterFactory> {
    next_id: AtomicU64,
    writer_factory: F,
    options: SstableBuilderOptions,
    policy: CachePolicy,
    limiter: MemoryLimiter,
}

impl<F: SstableWriterFactory> LocalTableBuilderFactory<F> {
    pub fn new(next_id: u64, writer_factory: F, options: SstableBuilderOptions) -> Self {
        Self {
            next_id: AtomicU64::new(next_id),
            writer_factory,
            options,
            policy: CachePolicy::NotFill,
            limiter: MemoryLimiter::new(1000000),
        }
    }
}

#[async_trait::async_trait]
impl<F: SstableWriterFactory> TableBuilderFactory for LocalTableBuilderFactory<F> {
    type Filter = Xor16FilterBuilder;
    type Writer = <F as SstableWriterFactory>::Writer;

    async fn open_builder(&mut self) -> HummockResult<SstableBuilder<Self::Writer, Self::Filter>> {
        let id = self.next_id.fetch_add(1, SeqCst);
        let tracker = self.limiter.require_memory(1).await;
        let writer_options = SstableWriterOptions {
            capacity_hint: Some(self.options.capacity),
            tracker: Some(tracker),
            policy: self.policy,
        };
        let writer = self
            .writer_factory
            .create_sst_writer(id, writer_options)
            .await
            .unwrap();
        let builder = SstableBuilder::for_test(id, writer, self.options.clone());

        Ok(builder)
    }
}

fn get_builder_options(capacity_mb: usize) -> SstableBuilderOptions {
    SstableBuilderOptions {
        capacity: capacity_mb * 1024 * 1024,
        block_capacity: 1024 * 1024,
        restart_interval: 16,
        bloom_false_positive: 0.001,
        ..Default::default()
    }
}

fn test_user_key_of(idx: u64) -> UserKey<Vec<u8>> {
    UserKey::for_test(TableId::default(), idx.to_be_bytes().to_vec())
}

async fn build_tables<F: SstableWriterFactory>(
    mut builder: CapacitySplitTableBuilder<LocalTableBuilderFactory<F>>,
) -> Vec<SstableInfo> {
    for i in RANGE {
        builder
            .add_full_key_for_test(
                FullKey::from_user_key(test_user_key_of(i).as_ref(), 1),
                HummockValue::put(VALUE),
                true,
            )
            .await
            .unwrap();
    }
    let split_table_outputs = builder.finish().await.unwrap();
    let ssts = split_table_outputs
        .iter()
        .map(|handle| handle.sst_info.sst_info.clone())
        .collect_vec();
    let join_handles = split_table_outputs
        .into_iter()
        .map(|o| o.upload_join_handle)
        .collect_vec();
    try_join_all(join_handles).await.unwrap();
    ssts
}

fn generate_sstable_store(object_store: Arc<ObjectStoreImpl>) -> SstableStore {
    SstableStore::new(SstableStoreConfig {
        store: object_store,
        path: "test".to_string(),
        block_cache_capacity: 64 << 20,
        meta_cache_capacity: 128 << 20,
        meta_cache_shard_num: 2,
        block_cache_shard_num: 2,
        block_cache_eviction: EvictionConfig::for_test(),
        meta_cache_eviction: EvictionConfig::for_test(),
        prefetch_buffer_capacity: 64 << 20,
        max_prefetch_block_number: 16,
        data_file_cache: FileCache::none(),
        meta_file_cache: FileCache::none(),
        recent_filter: None,
        state_store_metrics: Arc::new(global_hummock_state_store_metrics(MetricLevel::Disabled)),
    })
}

fn bench_builder(
    c: &mut Criterion,
    bucket: &str,
    capacity_mb: usize,
    enable_streaming_upload: bool,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let metrics = Arc::new(ObjectStoreMetrics::unused());

    let default_config = Arc::new(ObjectStoreConfig::default());
    let object_store = runtime.block_on(async {
        S3ObjectStore::new_with_config(bucket.to_string(), metrics.clone(), default_config.clone())
            .await
            .monitored(metrics, default_config)
    });
    let object_store = Arc::new(ObjectStoreImpl::S3(object_store));


    let sstable_store = runtime.block_on(async {
        let meta_cache_v2 = HybridCacheBuilder::new()
            .memory(64 << 20)
            .with_shards(2)
            .storage()
            .build()
            .await
            .unwrap();
        let block_cache_v2 = HybridCacheBuilder::new()
            .memory(128 << 20)
            .with_shards(2)
            .storage()
            .build()
            .await
            .unwrap();
        Arc::new(SstableStore::new(SstableStoreConfig {
            store: object_store,
            path: "test".to_string(),
            prefetch_buffer_capacity: 64 << 20,
            max_prefetch_block_number: 16,
            recent_filter: None,
            state_store_metrics: Arc::new(global_hummock_state_store_metrics(
                MetricLevel::Disabled,
            )),
            meta_cache_v2,
            block_cache_v2,
        }))
    });

    let mut group = c.benchmark_group("bench_multi_builder");
    group
        .sample_size(SAMPLE_COUNT)
        .measurement_time(ESTIMATED_MEASUREMENT_TIME);
    if enable_streaming_upload {
        group.bench_function(format!("bench_streaming_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(CapacitySplitTableBuilder::for_test(
                    LocalTableBuilderFactory::new(
                        1,
                        StreamingSstableWriterFactory::new(sstable_store.clone()),
                        get_builder_options(capacity_mb),
                    ),
                ))
            })
        });
    } else {
        group.bench_function(format!("bench_batch_upload_{}mb", capacity_mb), |b| {
            b.to_async(&runtime).iter(|| {
                build_tables(CapacitySplitTableBuilder::for_test(
                    LocalTableBuilderFactory::new(
                        1,
                        BatchSstableWriterFactory::new(sstable_store.clone()),
                        get_builder_options(capacity_mb),
                    ),
                ))
            })
        });
    }
    group.finish();
}

// SST size: 4, 32, 64, 128, 256MiB
fn bench_multi_builder(c: &mut Criterion) {
    let sst_capacities = vec![4, 32, 64, 128, 256];
    let bucket = env::var("S3_BUCKET").unwrap();
    for capacity in sst_capacities {
        bench_builder(c, &bucket, capacity, false);
        bench_builder(c, &bucket, capacity, true);
    }
}

fn bench_table_scan(c: &mut Criterion) {
    let capacity_mb: usize = 32;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let store = InMemObjectStore::new().monitored(
        Arc::new(ObjectStoreMetrics::unused()),
        Arc::new(ObjectStoreConfig::default()),
    );
    let object_store = Arc::new(ObjectStoreImpl::InMem(store));
    let sstable_store = Arc::new(generate_sstable_store(object_store));

    let ssts = runtime.block_on(async {
        build_tables(CapacitySplitTableBuilder::for_test(
            LocalTableBuilderFactory::new(
                1,
                BatchSstableWriterFactory::new(sstable_store.clone()),
                get_builder_options(capacity_mb),
            ),
        ))
        .await
    });
    println!("sst count: {}", ssts.len());
    let mut group = c.benchmark_group("bench_multi_builder");
    group
        .sample_size(SAMPLE_COUNT)
        .measurement_time(ESTIMATED_MEASUREMENT_TIME);
    let read_options = Arc::new(SstableIteratorReadOptions::default());
    group.bench_function("bench_table_scan", |b| {
        let sstable_ssts = ssts.clone();
        b.to_async(&runtime).iter(|| {
            let sstable_ssts = sstable_ssts.clone();
            let sstable_store = sstable_store.clone();
            let read_options = read_options.clone();
            async move {
                let mut iter = ConcatIterator::new(
                    sstable_ssts.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                );
                iter.rewind().await.unwrap();
                let mut count = 0;
                while iter.is_valid() {
                    count += 1;
                    iter.next().await.unwrap();
                }
                assert_eq!(count, RANGE.end - RANGE.start);
            }
        });
    });
    group.bench_function("bench_table_reverse_scan", |b| {
        let mut sstable_ssts = ssts.clone();
        sstable_ssts.reverse();
        b.to_async(&runtime).iter(|| {
            let sstable_ssts = sstable_ssts.clone();
            let sstable_store = sstable_store.clone();
            let read_options = read_options.clone();
            async move {
                let mut iter = ConcatIteratorInner::<BackwardSstableIterator>::new(
                    sstable_ssts.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                );
                iter.rewind().await.unwrap();
                let mut count = 0;
                while iter.is_valid() {
                    count += 1;
                    iter.next().await.unwrap();
                }
                assert_eq!(count, RANGE.end - RANGE.start);
            }
        });
    });
    group.bench_function("bench_point_scan", |b| {
        let sstable_ssts = ssts.clone();
        b.to_async(&runtime).iter(|| {
            let sstable_ssts = sstable_ssts.clone();
            let sstable_store = sstable_store.clone();
            let read_options = read_options.clone();
            let idx = random::<u64>() % (RANGE.end - RANGE.start);
            let key = FullKey::from_user_key(test_user_key_of(idx), 1);
            async move {
                let mut iter = ConcatIterator::new(
                    sstable_ssts.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                );
                iter.seek(key.to_ref()).await.unwrap();
                if iter.is_valid() {
                    iter.next().await.unwrap();
                }
            }
        });
    });
    group.bench_function("bench_point_reverse_scan", |b| {
        let mut sstable_ssts = ssts.clone();
        sstable_ssts.reverse();
        b.to_async(&runtime).iter(|| {
            let sstable_ssts = sstable_ssts.clone();
            let sstable_store = sstable_store.clone();
            let read_options = read_options.clone();
            let idx = random::<u64>() % (RANGE.end - RANGE.start);
            let key = FullKey::from_user_key(test_user_key_of(idx), 1);
            async move {
                let mut iter = ConcatIteratorInner::<BackwardSstableIterator>::new(
                    sstable_ssts.clone(),
                    sstable_store.clone(),
                    read_options.clone(),
                );
                iter.seek(key.to_ref()).await.unwrap();
                if iter.is_valid() {
                    iter.next().await.unwrap();
                }
            }
        });
    });
}

criterion_group!(benches, bench_multi_builder, bench_table_scan);
criterion_main!(benches);
