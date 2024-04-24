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

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use risingwave_hummock_sdk::key::{key_with_epoch, map_table_key_range};
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use risingwave_storage::hummock::iterator::test_utils::{
    iterator_test_table_key_of, iterator_test_user_key_of,
};
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::store::version::{
    read_filter_for_version, HummockReadVersion, StagingData, StagingSstableInfo, VersionUpdate,
};
use risingwave_storage::hummock::test_utils::gen_dummy_batch;

use crate::test_utils::prepare_first_valid_version;

#[tokio::test]
async fn test_read_version_basic() {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let (pinned_version, _, _) =
        prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

    let mut epoch = test_epoch(1);
    let table_id = 0;
    let vnodes = Arc::new(Bitmap::ones(VirtualNode::COUNT));
    let mut read_version = HummockReadVersion::new(TableId::from(table_id), pinned_version, vnodes);

    {
        // single imm
        let sorted_items = gen_dummy_batch(1);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items, None);
        let imm = SharedBufferBatch::build_shared_buffer_batch_for_test(
            epoch,
            0,
            sorted_items,
            size,
            TableId::from(table_id),
        );

        read_version.update(VersionUpdate::Staging(StagingData::ImmMem(imm)));

        let key = iterator_test_table_key_of(1_usize);
        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key.to_vec())),
            Bound::Included(Bytes::from(key.to_vec())),
        ));

        let staging_data_iter =
            read_version
                .staging()
                .prune_overlap(epoch, TableId::default(), &key_range);

        let staging_imm = staging_data_iter.cloned().collect_vec();

        assert_eq!(1, staging_imm.len());
        assert!(staging_imm.iter().any(|imm| imm.min_epoch() <= epoch));
    }

    {
        // several epoch
        for i in 0..5 {
            epoch.inc_epoch();
            let sorted_items = gen_dummy_batch(i + 2);
            let size = SharedBufferBatch::measure_batch_size(&sorted_items, None);
            let imm = SharedBufferBatch::build_shared_buffer_batch_for_test(
                epoch,
                0,
                sorted_items,
                size,
                TableId::from(table_id),
            );

            read_version.update(VersionUpdate::Staging(StagingData::ImmMem(imm)));
        }

        for e in 1..6 {
            let epoch = test_epoch(e);
            let key = iterator_test_table_key_of(e as usize);
            let key_range = map_table_key_range((
                Bound::Included(Bytes::from(key.to_vec())),
                Bound::Included(Bytes::from(key.to_vec())),
            ));
            let staging_data_iter =
                read_version
                    .staging()
                    .prune_overlap(epoch, TableId::default(), &key_range);

            let staging_imm = staging_data_iter.cloned().collect_vec();

            assert_eq!(1, staging_imm.len() as u64);
            assert!(staging_imm.iter().any(|imm| imm.min_epoch() <= epoch));
        }
    }

    {
        // test clean imm with sst update info
        let staging = read_version.staging();
        assert_eq!(6, staging.data.len());
        let batch_id_vec_for_clear = staging
            .data
            .iter()
            .flat_map(|data| data.to_imm().map(|imm| imm.batch_id()))
            .take(3)
            .collect::<Vec<_>>();

        let epoch_id_vec_for_clear = staging
            .data
            .iter()
            .flat_map(|data| data.to_imm())
            .map(|imm| imm.min_epoch())
            .take(3)
            .collect::<Vec<_>>();

        let dummy_sst = Arc::new(StagingSstableInfo::new(
            vec![
                LocalSstableInfo::for_test(SstableInfo {
                    object_id: 1,
                    sst_id: 1,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(iterator_test_user_key_of(1).encode(), test_epoch(1)),
                        right: key_with_epoch(iterator_test_user_key_of(2).encode(), test_epoch(2)),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    uncompressed_file_size: 1,
                    ..Default::default()
                }),
                LocalSstableInfo::for_test(SstableInfo {
                    object_id: 2,
                    sst_id: 2,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(iterator_test_user_key_of(3).encode(), test_epoch(3)),
                        right: key_with_epoch(iterator_test_user_key_of(3).encode(), test_epoch(3)),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    uncompressed_file_size: 1,
                    ..Default::default()
                }),
            ],
            vec![],
            epoch_id_vec_for_clear,
            batch_id_vec_for_clear,
            1,
        ));

        {
            read_version.update(VersionUpdate::Staging(StagingData::Sst(dummy_sst)));
        }
    }

    {
        // test clear related batch after update sst

        // after update sst
        // imm(0, 1, 2) => sst{sst_object_id: 1}
        // staging => {imm(3, 4, 5), sst[{sst_object_id: 1}, {sst_object_id: 2}]}
        let staging = read_version.staging();
        assert_eq!(4, read_version.staging().data.len());
        let remain_batch_id_vec = staging
            .data
            .iter()
            .flat_map(|data| data.to_imm().map(|imm| imm.batch_id()))
            .collect::<Vec<_>>();
        assert!(remain_batch_id_vec.iter().any(|batch_id| *batch_id > 2));
    }

    {
        let key_range_left = iterator_test_table_key_of(0);
        let key_range_right = iterator_test_table_key_of(4_usize);

        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key_range_left)),
            Bound::Included(Bytes::from(key_range_right)),
        ));

        let staging_data_iter =
            read_version
                .staging()
                .prune_overlap(epoch, TableId::default(), &key_range);

        let staging_data = staging_data_iter.cloned().collect_vec();
        assert_eq!(2, staging_data.len());

        assert_eq!(test_epoch(4), staging_data[0].min_epoch());

        match &staging_data[1] {
            StagingData::Sst(sst) => {
                assert_eq!(1, sst.sstable_infos()[0].sst_info.get_object_id());
                assert_eq!(2, sst.sstable_infos()[1].sst_info.get_object_id());
            }
            StagingData::ImmMem(_) => {
                unreachable!("can not be immemtable");
            }
        }
    }

    {
        let key_range_left = iterator_test_table_key_of(3);
        let key_range_right = iterator_test_table_key_of(4);

        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key_range_left)),
            Bound::Included(Bytes::from(key_range_right)),
        ));

        let staging_data_iter =
            read_version
                .staging()
                .prune_overlap(epoch, TableId::default(), &key_range);

        let staging_data = staging_data_iter.cloned().collect_vec();
        assert_eq!(2, staging_data.len());
        assert_eq!(test_epoch(4), staging_data[0].min_epoch());

        // let staging_ssts = staging_sst_iter.cloned().collect_vec();
        // assert_eq!(1, staging_ssts.len());
        // assert_eq!(2, staging_ssts[0].get_object_id());
    }
}

#[tokio::test]
async fn test_read_filter_basic() {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let (pinned_version, _, _) =
        prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

    let epoch = test_epoch(1);
    let table_id = 0;
    let vnodes = Arc::new(Bitmap::ones(VirtualNode::COUNT));
    let read_version = Arc::new(RwLock::new(HummockReadVersion::new(
        TableId::from(table_id),
        pinned_version,
        vnodes.clone(),
    )));
    read_version.write().update_vnode_bitmap(vnodes);

    {
        // single imm
        let sorted_items = gen_dummy_batch(epoch);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items, None);
        let imm = SharedBufferBatch::build_shared_buffer_batch_for_test(
            epoch,
            0,
            sorted_items,
            size,
            TableId::from(table_id),
        );

        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm)));

        // directly prune_overlap
        let key = Bytes::from(iterator_test_table_key_of(epoch as usize));
        let key_range = map_table_key_range((Bound::Included(key.clone()), Bound::Included(key)));

        let staging_data = {
            let read_guard = read_version.read();
            read_guard
                .staging()
                .prune_overlap(epoch, TableId::default(), &key_range)
                .cloned()
                .collect_vec()
        };

        assert_eq!(1, staging_data.len());
        assert!(staging_data.iter().any(|data| {
            match data {
                StagingData::ImmMem(imm) => imm.min_epoch() <= epoch,
                StagingData::Sst(_) => unreachable!("can not be sstable"),
            }
        }));

        // test read_filter_for_version
        {
            let key_range = key_range.clone();
            let (_, hummock_read_snapshot) =
                read_filter_for_version(epoch, TableId::from(table_id), key_range, &read_version)
                    .unwrap();

            assert_eq!(1, hummock_read_snapshot.0.len());
            assert_eq!(
                read_version.read().committed().max_committed_epoch(),
                hummock_read_snapshot.1.max_committed_epoch()
            );
        }
    }
}

// #[tokio::test]
// async fn test_read_filter_for_batch_issue_14659() {
//     use std::ops::Bound::Unbounded;

//     let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
//         setup_compute_env(8080).await;

//     let (pinned_version, _, _) =
//         prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

//     const NUM_SHARDS: u64 = 2;
//     let table_id = TableId::from(2);
//     let epoch = test_epoch(1);
//     let mut read_version_vec = vec![];
//     let mut imms = vec![];

//     // Populate IMMs
//     for i in 0..NUM_SHARDS {
//         let read_version = Arc::new(RwLock::new(HummockReadVersion::new(
//             table_id,
//             pinned_version.clone(),
//         )));

//         let items = SharedBufferBatch::build_shared_buffer_item_batches(gen_dummy_batch(i));
//         let size = SharedBufferBatch::measure_batch_size(&items);
//         let imm =
//             SharedBufferBatch::build_shared_buffer_batch_for_test(epoch, 0, items, size, table_id);

//         imms.push(imm.clone());

//         read_version
//             .write()
//             .update(VersionUpdate::Staging(StagingData::ImmMem(imm)));

//         read_version_vec.push(read_version);
//     }

//     // Update read version via staging SSTs
//     let sst_id = 233;
//     let staging_sst = Arc::new(gen_dummy_sst_info(sst_id, imms.clone(), table_id, epoch));
//     read_version_vec.iter().for_each(|v| {
//         v.write().update(VersionUpdate::Staging(StagingData::Sst(
//             StagingSstableInfo::new(
//                 vec![LocalSstableInfo::for_test(staging_sst.clone())],
//                 vec![epoch],
//                 imms.iter().map(|imm| imm.batch_id()).collect_vec(),
//                 imms.iter().map(|imm| imm.size()).sum(),
//             ),
//         )));
//     });

//     // build for batch with max epoch
//     let (_, hummock_read_snapshot) = read_filter_for_batch(
//         HummockEpoch::MAX,
//         table_id,
//         (Unbounded, Unbounded),
//         read_version_vec,
//     )
//     .unwrap();

//     // No imms should be proivided
//     assert_eq!(0, hummock_read_snapshot.0.len());
//     // Only 1 staging sst is provided
//     assert_eq!(1, hummock_read_snapshot.1.len());
// }
