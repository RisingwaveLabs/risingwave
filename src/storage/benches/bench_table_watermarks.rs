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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use itertools::Itertools;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::test_epoch;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::table_watermark::{
    TableWatermarks, TableWatermarksIndex, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionStateTableInfo};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::{PbHummockVersion, StateTableInfoDelta};
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use spin::Mutex;
use tokio::sync::mpsc::unbounded_channel;

fn vnode_bitmaps(part_count: usize) -> impl Iterator<Item = Arc<Bitmap>> {
    static BITMAP_CACHE: LazyLock<Mutex<HashMap<usize, Vec<Arc<Bitmap>>>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    assert_eq!(VirtualNode::COUNT_FOR_TEST % part_count, 0);
    let mut cache = BITMAP_CACHE.lock();
    match cache.entry(part_count) {
        Entry::Occupied(entry) => entry.get().clone().into_iter(),
        Entry::Vacant(entry) => entry
            .insert({
                let part_size = VirtualNode::COUNT_FOR_TEST / part_count;
                (0..part_count)
                    .map(move |part_idx| {
                        let start = part_idx * part_size;
                        let end = part_idx * part_size + part_size;
                        let mut bitmap = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
                        for i in start..end {
                            bitmap.set(i, true);
                        }
                        Arc::new(bitmap.finish())
                    })
                    .collect()
            })
            .clone()
            .into_iter(),
    }
}

fn gen_watermark(epoch_idx: usize) -> Bytes {
    static WATERMARK_CACHE: LazyLock<Mutex<HashMap<usize, Bytes>>> =
        LazyLock::new(|| Mutex::new(HashMap::new()));
    let mut cache = WATERMARK_CACHE.lock();
    match cache.entry(epoch_idx) {
        Entry::Occupied(entry) => entry.get().clone(),
        Entry::Vacant(entry) => entry
            .insert(Bytes::copy_from_slice(
                format!("key_test_{:010}", epoch_idx).as_bytes(),
            ))
            .clone(),
    }
}

fn gen_epoch_watermarks(
    epoch_idx: usize,
    vnode_part_count: usize,
) -> (HummockEpoch, Vec<VnodeWatermark>) {
    (
        test_epoch(epoch_idx as _),
        vnode_bitmaps(vnode_part_count)
            .map(|bitmap| VnodeWatermark::new(bitmap, gen_watermark(epoch_idx)))
            .collect_vec(),
    )
}

fn gen_committed_table_watermarks(
    old_epoch_idx: usize,
    new_epoch_idx: usize,
    vnode_part_count: usize,
) -> TableWatermarks {
    assert!(old_epoch_idx <= new_epoch_idx);
    TableWatermarks {
        watermarks: (old_epoch_idx..=new_epoch_idx)
            .map(|epoch_idx| {
                let (epoch, watermarks) = gen_epoch_watermarks(epoch_idx, vnode_part_count);
                (epoch, watermarks.into())
            })
            .collect(),
        direction: WatermarkDirection::Ascending,
        watermark_type: WatermarkSerdeType::PkPrefix,
    }
}

fn gen_version(
    old_epoch_idx: usize,
    new_epoch_idx: usize,
    table_count: usize,
    vnode_part_count: usize,
) -> HummockVersion {
    let table_watermarks = Arc::new(gen_committed_table_watermarks(
        old_epoch_idx,
        new_epoch_idx,
        vnode_part_count,
    ));
    let committed_epoch = test_epoch(new_epoch_idx as _);
    let mut version = HummockVersion::from_persisted_protobuf(&PbHummockVersion {
        id: new_epoch_idx as _,
        ..Default::default()
    });
    version.table_watermarks = (0..table_count)
        .map(|table_id| (TableId::new(table_id as _), table_watermarks.clone()))
        .collect();
    let mut state_table_info = HummockVersionStateTableInfo::empty();
    state_table_info.apply_delta(
        &(0..table_count)
            .map(|table_id| {
                (
                    TableId::new(table_id as _),
                    StateTableInfoDelta {
                        committed_epoch,
                        compaction_group_id: StaticCompactionGroupId::StateDefault as _,
                    },
                )
            })
            .collect(),
        &HashSet::new(),
    );
    version.state_table_info = state_table_info;
    version
}

fn bench_table_watermarks(c: &mut Criterion) {
    let version_count = 500;
    let epoch_count = 1000;
    let table_count = 1;
    let vnode_part_count = 16;
    let pre_generated_versions: VecDeque<_> = (1..version_count + 1)
        .map(|epoch_idx| {
            gen_version(
                epoch_idx,
                epoch_idx + epoch_count,
                table_count,
                vnode_part_count,
            )
        })
        .collect();
    c.bench_function("new pinned version", |b| {
        b.iter_batched(
            || pre_generated_versions.clone(),
            |mut versions| {
                let mut pinned_version =
                    PinnedVersion::new(versions.pop_front().unwrap(), unbounded_channel().0);
                while let Some(version) = versions.pop_front() {
                    pinned_version = pinned_version.new_pin_version(version).unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });

    let safe_epoch_idx: usize = 9500;
    let committed_epoch_idx: usize = 10000;
    let staging_epoch_count: usize = 500;
    let vnode_part_count = 16;

    let mut table_watermarks = TableWatermarksIndex::new_committed(
        gen_committed_table_watermarks(safe_epoch_idx, committed_epoch_idx, vnode_part_count)
            .into(),
        test_epoch(committed_epoch_idx as u64),
    );
    for i in 0..staging_epoch_count {
        let (epoch, watermarks) =
            gen_epoch_watermarks(committed_epoch_idx + i + 1, vnode_part_count);
        table_watermarks.add_epoch_watermark(
            epoch,
            watermarks.into(),
            WatermarkDirection::Ascending,
        );
    }
    let table_watermarks = table_watermarks;
    let committed_watermarks = gen_committed_table_watermarks(
        safe_epoch_idx + 1,
        committed_epoch_idx + 1,
        vnode_part_count,
    );
    let batch_size = 100;
    c.bench_function("apply committed watermark", |b| {
        b.iter_batched(
            || {
                (0..batch_size)
                    .map(|_| (table_watermarks.clone(), committed_watermarks.clone()))
                    .collect_vec()
            },
            |list| {
                for (mut table_watermarks, committed_watermarks) in list {
                    table_watermarks.apply_committed_watermarks(
                        committed_watermarks.into(),
                        test_epoch((committed_epoch_idx + 1) as _),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });

    // Code for the original table watermark index
    // let mut table_watermarks =
    //     gen_committed_table_watermarks(safe_epoch_idx, committed_epoch_idx, vnode_part_count)
    //         .build_index(test_epoch(committed_epoch_idx as u64));
    // for i in 0..staging_epoch_count {
    //     let (epoch, watermarks) =
    //         gen_epoch_watermarks(committed_epoch_idx + i + 1, vnode_part_count);
    //     table_watermarks.add_epoch_watermark(epoch, &watermarks, WatermarkDirection::Ascending);
    // }
    // let table_watermarks = table_watermarks;
    // let committed_watermarks = gen_committed_table_watermarks(
    //     safe_epoch_idx + 1,
    //     committed_epoch_idx + 1,
    //     vnode_part_count,
    // )
    // .build_index(test_epoch((committed_epoch_idx + 1) as _));
    // let batch_size = 100;
    // c.bench_function("apply committed watermark", |b| {
    //     b.iter_batched(
    //         || {
    //             (0..batch_size)
    //                 .map(|_| (table_watermarks.clone(), committed_watermarks.clone()))
    //                 .collect_vec()
    //         },
    //         |list| {
    //             for (mut table_watermarks, committed_watermarks) in list {
    //                 table_watermarks.apply_committed_watermarks(&committed_watermarks);
    //             }
    //         },
    //         BatchSize::SmallInput,
    //     )
    // });

    c.bench_function("read latest watermark", |b| {
        b.iter(|| {
            for i in 0..VirtualNode::COUNT_FOR_TEST {
                let _ = table_watermarks.latest_watermark(VirtualNode::from_index(i));
            }
        })
    });

    c.bench_function("read committed watermark", |b| {
        b.iter(|| {
            for i in 0..VirtualNode::COUNT_FOR_TEST {
                let _ = table_watermarks.read_watermark(
                    VirtualNode::from_index(i),
                    test_epoch(committed_epoch_idx as u64),
                );
            }
        })
    });
}

criterion_group!(benches, bench_table_watermarks);
criterion_main!(benches);
