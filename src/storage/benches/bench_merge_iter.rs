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

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures::executor::block_on;
use risingwave_hummock_sdk::key::TableKey;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogAgent;
use risingwave_storage::hummock::iterator::{
    Forward, HummockIterator, HummockIteratorUnion, MergeIterator,
    NonPkPrefixSkipWatermarkIterator, SkipWatermarkIterator,
};
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator, SharedBufferValue,
};

fn gen_interleave_shared_buffer_batch_iter(
    batch_size: usize,
    batch_count: usize,
) -> Vec<SharedBufferBatchIterator<Forward>> {
    let mut iterators = Vec::new();
    for i in 0..batch_count {
        let mut batch_data = vec![];
        for j in 0..batch_size {
            batch_data.push((
                TableKey(Bytes::copy_from_slice(
                    format!("test_key_{:08}", j * batch_count + i).as_bytes(),
                )),
                SharedBufferValue::Insert(Bytes::copy_from_slice("value".as_bytes())),
            ));
        }
        let batch = SharedBufferBatch::for_test(batch_data, 2333, Default::default());
        iterators.push(batch.into_forward_iter());
    }
    iterators
}

#[allow(clippy::type_complexity)]
fn gen_interleave_shared_buffer_batch_enum_iter(
    batch_size: usize,
    batch_count: usize,
) -> Vec<
    HummockIteratorUnion<
        Forward,
        SharedBufferBatchIterator<Forward>,
        SharedBufferBatchIterator<Forward>,
        SharedBufferBatchIterator<Forward>,
        SharedBufferBatchIterator<Forward>,
    >,
> {
    let mut iterators = Vec::new();
    for i in 0..batch_count {
        let mut batch_data = vec![];
        for j in 0..batch_size {
            batch_data.push((
                TableKey(Bytes::copy_from_slice(
                    format!("test_key_{:08}", j * batch_count + i).as_bytes(),
                )),
                SharedBufferValue::Insert(Bytes::copy_from_slice("value".as_bytes())),
            ));
        }
        let batch = SharedBufferBatch::for_test(batch_data, 2333, Default::default());
        match i % 4 {
            0 => iterators.push(HummockIteratorUnion::First(batch.into_forward_iter())),
            1 => iterators.push(HummockIteratorUnion::Second(batch.into_forward_iter())),
            2 => iterators.push(HummockIteratorUnion::Third(batch.into_forward_iter())),
            3 => iterators.push(HummockIteratorUnion::Fourth(batch.into_forward_iter())),
            _ => unreachable!(),
        };
    }
    iterators
}

fn run_iter<I: HummockIterator<Direction = Forward>>(iter_ref: &RefCell<I>, total_count: usize) {
    let mut iter = iter_ref.borrow_mut();
    block_on(iter.rewind()).unwrap();
    let mut count = 0;
    while iter.is_valid() {
        count += 1;
        block_on(iter.next()).unwrap();
    }
    assert_eq!(total_count, count);
}

fn criterion_benchmark(c: &mut Criterion) {
    let merge_iter = RefCell::new(MergeIterator::new(gen_interleave_shared_buffer_batch_iter(
        10000, 100,
    )));
    c.bench_with_input(
        BenchmarkId::new("bench-merge-iter", "unordered"),
        &merge_iter,
        |b, iter_ref| {
            b.iter(|| {
                run_iter(iter_ref, 100 * 10000);
            });
        },
    );

    let combine_iter = {
        let iter = SkipWatermarkIterator::new(
            MergeIterator::new(gen_interleave_shared_buffer_batch_iter(10000, 100)),
            BTreeMap::new(),
        );

        NonPkPrefixSkipWatermarkIterator::new(
            iter,
            BTreeMap::new(),
            Arc::new(CompactionCatalogAgent::dummy()),
        )
    };

    let merge_iter = RefCell::new(combine_iter);
    c.bench_with_input(
        BenchmarkId::new("bench-merge-iter-skip-empty-watermark", "unordered"),
        &merge_iter,
        |b, iter_ref| {
            b.iter(|| {
                run_iter(iter_ref, 100 * 10000);
            });
        },
    );

    let merge_iter = RefCell::new(MergeIterator::new(
        gen_interleave_shared_buffer_batch_enum_iter(10000, 100),
    ));
    c.bench_with_input(
        BenchmarkId::new("bench-enum-merge-iter", "unordered"),
        &merge_iter,
        |b, iter_ref| {
            b.iter(|| {
                run_iter(iter_ref, 100 * 10000);
            });
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
