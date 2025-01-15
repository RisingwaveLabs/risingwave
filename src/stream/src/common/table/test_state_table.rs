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
use std::ops::Bound::{self, *};

use futures::{pin_mut, StreamExt};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::{self, OwnedRow};
use risingwave_common::types::{DataType, Scalar, ScalarImpl, Timestamptz};
use risingwave_common::util::epoch::{test_epoch, EpochPair};
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::SINGLETON_VNODE;

use crate::common::table::state_table::{
    ReplicatedStateTable, StateTable, WatermarkCacheStateTable,
};
use crate::common::table::test_utils::{gen_pbtable, gen_pbtable_with_value_indices};

#[tokio::test]
async fn test_state_table_update_insert() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(4), DataType::Int32),
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    state_table.delete(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
        Some(6666_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        None,
        None,
        Some(6666_i32.into()),
    ]));

    state_table.delete(OwnedRow::new(vec![
        Some(7_i32.into()),
        None,
        Some(777_i32.into()),
        None,
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(7777_i32.into()),
        None,
    ]));
    let row6 = state_table
        .get_row(&OwnedRow::new(vec![Some(6_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row6,
        Some(OwnedRow::new(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );

    let row7 = state_table
        .get_row(&OwnedRow::new(vec![Some(7_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row7,
        Some(OwnedRow::new(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    let row6_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(6_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row6_commit,
        Some(OwnedRow::new(vec![
            Some(6_i32.into()),
            None,
            None,
            Some(6666_i32.into())
        ]))
    );
    let row7_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(7_i32.into())]))
        .await
        .unwrap();
    assert_eq!(
        row7_commit,
        Some(OwnedRow::new(vec![
            Some(7_i32.into()),
            Some(77_i32.into()),
            Some(7777_i32.into()),
            None
        ]))
    );

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    // one epoch: delete (1, 2, 3, 4), insert (5, 6, 7, None), delete(5, 6, 7, None)
    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(2_i32.into()),
        Some(3_i32.into()),
        Some(4_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));
    state_table.delete(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(6_i32.into()),
        Some(7_i32.into()),
        None,
    ]));

    let row1 = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1, None);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    let row1_commit = state_table
        .get_row(&OwnedRow::new(vec![Some(1_i32.into())]))
        .await
        .unwrap();
    assert_eq!(row1_commit, None);
}

#[tokio::test]
async fn test_state_table_iter_with_prefix() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::ascending(), OrderType::descending()];

    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
    let iter = state_table
        .iter_with_prefix(&pk_prefix, sub_range, Default::default())
        .await
        .unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(55_i32.into()),
            Some(5555_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into())
        ]),
        res.as_ref()
    );
    // this row exists in shared_storage
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.as_ref()
    );
    // pk without the prefix the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_iter_with_pk_range() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::ascending(), OrderType::descending()];

    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32), // This is the range prefix key
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(55_i32.into()),
        Some(5555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    let pk_range = (
        std::ops::Bound::Excluded(OwnedRow::new(vec![Some(1_i32.into())])),
        std::ops::Bound::Included(OwnedRow::new(vec![Some(4_i32.into())])),
    );
    let iter = state_table
        .iter_with_vnode(SINGLETON_VNODE, &pk_range, Default::default())
        .await
        .unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]),
        res.as_ref()
    );

    // pk without the prefix the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());

    let pk_range = (
        std::ops::Bound::Included(OwnedRow::new(vec![Some(2_i32.into())])),
        std::ops::Bound::<row::Empty>::Unbounded,
    );
    let iter = state_table
        .iter_with_vnode(SINGLETON_VNODE, &pk_range, Default::default())
        .await
        .unwrap();
    pin_mut!(iter);

    // this row exists in both mem_table and cell_based_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(6_i32.into()),
            Some(66_i32.into()),
            Some(666_i32.into()),
        ]),
        res.as_ref()
    );

    // pk without the prefix the range will not be scan
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
#[should_panic]
async fn test_mem_table_assertion() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog(&table, test_env.storage.clone(), None).await;

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    // duplicate key: 1
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
}

#[tokio::test]
async fn test_state_table_iter_with_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state_table.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    {
        let iter = state_table
            .iter_with_prefix(row::empty(), sub_range, Default::default())
            .await
            .unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(111_i32.into())]), res.as_ref());

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(3333_i32.into())]), res.as_ref());

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(&OwnedRow::new(vec![Some(666_i32.into())]), res.as_ref());
    }

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state_table
        .iter_with_prefix(row::empty(), sub_range, Default::default())
        .await
        .unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    // this pk exist in both shared_storage and mem_table
    assert_eq!(&OwnedRow::new(vec![Some(333_i32.into())]), res.as_ref());

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(&OwnedRow::new(vec![Some(444_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(555_i32.into())]), res.as_ref());
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(&OwnedRow::new(vec![Some(666_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(777.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(&OwnedRow::new(vec![Some(888_i32.into())]), res.as_ref());

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(&OwnedRow::new(vec![Some(999_i32.into())]), res.as_ref());

    // there is no row in both shared_storage and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_iter_with_shuffle_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2, 1, 0],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state_table.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(3333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(6_i32.into()),
        Some(66_i32.into()),
        Some(666_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(9_i32.into()),
        Some(99_i32.into()),
        Some(999_i32.into()),
    ]));
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    {
        let iter = state_table
            .iter_with_prefix(row::empty(), sub_range, Default::default())
            .await
            .unwrap();
        pin_mut!(iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(111_i32.into()),
                Some(11_i32.into()),
                Some(1_i32.into())
            ]),
            res.as_ref()
        );

        // will not get [2, 22, 222]
        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(3333_i32.into()),
                Some(33_i32.into()),
                Some(3_i32.into())
            ]),
            res.as_ref()
        );

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(666_i32.into()),
                Some(66_i32.into()),
                Some(6_i32.into())
            ]),
            res.as_ref()
        );
    }

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    // write [3, 33, 333], [4, 44, 444], [5, 55, 555], [7, 77, 777], [8, 88, 888]into mem_table,
    // [3, 33, 3333], [6, 66, 666], [9, 99, 999] exists in
    // shared_storage

    state_table.delete(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(7_i32.into()),
        Some(77_i32.into()),
        Some(777_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(8_i32.into()),
        Some(88_i32.into()),
        Some(888_i32.into()),
    ]));

    let iter = state_table
        .iter_with_prefix(row::empty(), sub_range, Default::default())
        .await
        .unwrap();
    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(333_i32.into()),
            Some(33_i32.into()),
            Some(3_i32.into())
        ]),
        res.as_ref()
    );

    // this row exists in mem_table
    let res = iter.next().await.unwrap().unwrap();
    assert_eq!(
        &OwnedRow::new(vec![
            Some(444_i32.into()),
            Some(44_i32.into()),
            Some(4_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(555_i32.into()),
            Some(55_i32.into()),
            Some(5_i32.into())
        ]),
        res.as_ref()
    );
    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(666_i32.into()),
            Some(66_i32.into()),
            Some(6_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();
    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(777.into()),
            Some(77_i32.into()),
            Some(7_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in mem_table
    assert_eq!(
        &OwnedRow::new(vec![
            Some(888_i32.into()),
            Some(88_i32.into()),
            Some(8_i32.into())
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    // this row exists in shared_storage
    assert_eq!(
        &OwnedRow::new(vec![
            Some(999_i32.into()),
            Some(99_i32.into()),
            Some(9_i32.into())
        ]),
        res.as_ref()
    );

    // there is no row in both shared_storage and mem_table
    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_state_table_write_chunk() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 1
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 2
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
            (
                Op::UpdateDelete, // update the row with `mark 1`
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::UpdateInsert,
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4999i64.into()),
                    Some(false.into()),
                    Some(0.123456f32.into()),
                ]),
            ),
            (
                Op::Delete, // delete the row with `mark 2`
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );

    state_table.write_chunk(chunk);
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    let rows: Vec<_> = state_table
        .iter_with_prefix(row::empty(), sub_range, PrefetchOptions::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![
            Some(123i32.into()),
            Some(456i64.into()),
            Some(true.into()),
            Some(3.15f32.into()),
        ])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![
            Some(365i32.into()),
            Some(4999i64.into()),
            Some(false.into()),
            Some(0.123456f32.into()),
        ])
    );
}

#[tokio::test]
async fn test_state_table_write_chunk_visibility() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert, // mark 1
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
            (
                Op::Delete, // delete the row with `mark 1`, but hidden
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );
    let (ops, columns, _) = chunk.into_inner();
    let chunk =
        StreamChunk::with_visibility(ops, columns, Bitmap::from_iter([true, true, true, false]));

    state_table.write_chunk(chunk);
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    let rows: Vec<_> = state_table
        .iter_with_prefix(row::empty(), sub_range, PrefetchOptions::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![
            Some(8i32.into()),
            Some(1000i64.into()),
            Some(true.into()),
            Some(123.987f32.into()),
        ])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![
            Some(123i32.into()),
            Some(456i64.into()),
            Some(true.into()),
            Some(3.15f32.into()),
        ])
    );
    assert_eq!(
        rows[2].as_ref(),
        &OwnedRow::new(vec![
            Some(365i32.into()),
            Some(4888i64.into()),
            Some(false.into()),
            Some(0.123f32.into()),
        ])
    );
}

#[tokio::test]
async fn test_state_table_write_chunk_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
        ColumnDesc::unnamed(ColumnId::from(3), DataType::Float32),
    ];
    let data_types = [
        DataType::Int32,
        DataType::Int64,
        DataType::Boolean,
        DataType::Float32,
    ];
    let order_types = vec![OrderType::ascending()];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
        vec![2, 1],
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let chunk = StreamChunk::from_rows(
        &[
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(123i32.into()),
                    Some(456i64.into()),
                    Some(true.into()),
                    Some(3.15f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(365i32.into()),
                    Some(4888i64.into()),
                    Some(false.into()),
                    Some(0.123f32.into()),
                ]),
            ),
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(8i32.into()),
                    Some(1000i64.into()),
                    Some(true.into()),
                    Some(123.987f32.into()),
                ]),
            ),
        ],
        &data_types,
    );

    state_table.write_chunk(chunk);
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    let rows: Vec<_> = state_table
        .iter_with_prefix(row::empty(), sub_range, PrefetchOptions::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].as_ref(),
        &OwnedRow::new(vec![Some(true.into()), Some(1000i64.into()),])
    );
    assert_eq!(
        rows[1].as_ref(),
        &OwnedRow::new(vec![Some(true.into()), Some(456i64.into()),])
    );
    assert_eq!(
        rows[2].as_ref(),
        &OwnedRow::new(vec![Some(false.into()), Some(4888i64.into()),])
    );
}

// After NULL watermark col values are inserted & deleted, they should not appear in the state table
// cache. Test for apply_batch.
#[tokio::test]
async fn test_state_table_watermark_cache_ignore_null() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Timestamptz),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
    ];
    let data_types = column_descs
        .iter()
        .map(|c| c.data_type.clone())
        .collect::<Vec<_>>();

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        WatermarkCacheStateTable::from_table_catalog(&table, test_env.storage.clone(), None).await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let rows = vec![
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
                Some(456i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(4000).unwrap().to_scalar_value()),
                Some(4888i64.into()),
            ]),
        ),
        (Op::Insert, OwnedRow::new(vec![None, Some(4888i64.into())])),
        (Op::Insert, OwnedRow::new(vec![None, Some(1000i64.into())])),
    ];

    let chunk = StreamChunk::from_rows(&rows, &data_types);

    state_table.write_chunk(chunk);
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    let inserted_rows: Vec<_> = state_table
        .iter_with_prefix(row::empty(), sub_range, PrefetchOptions::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(inserted_rows.len(), 4);
    assert_eq!(inserted_rows[0], rows[0].1);
    assert_eq!(inserted_rows[1], rows[1].1);
    assert_eq!(inserted_rows[2], rows[2].1);
    assert_eq!(inserted_rows[3], rows[3].1);
    let cache = state_table.get_watermark_cache();
    // Before the first barrier, watermark cache won't be filled.
    assert_eq!(cache.len(), 0);

    let watermark = Timestamptz::from_secs(2500).unwrap().to_scalar_value();
    state_table.update_watermark(watermark);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 1);
    assert_eq!(
        cache.lowest_key().unwrap(),
        Timestamptz::from_secs(4000)
            .unwrap()
            .to_scalar_value()
            .as_scalar_ref_impl()
    );

    let rows = vec![
        (Op::Delete, OwnedRow::new(vec![None, Some(4888i64.into())])),
        // Watermark Partition here later.
        (Op::Delete, OwnedRow::new(vec![None, Some(1000i64.into())])),
    ];

    let chunk = StreamChunk::from_rows(&rows, &data_types);
    state_table.write_chunk(chunk);

    // Cache contents should not be impacted.
    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 1);
    assert_eq!(
        cache.lowest_key().unwrap(),
        Timestamptz::from_secs(4000)
            .unwrap()
            .to_scalar_value()
            .as_scalar_ref_impl()
    );
}

// Commit state table to init cache.
// Insert a chunk which:
// 1. Insert some values into state table.
// 2. Delete some values from the state table.
// 3. Insert some values into the state table.
// At each step, check the cache invariants.
// Commit state table to trigger watermark state cleaning.
// Bump watermark, watermark value should partition the state table.
// Check the partitioned values (larger than watermark) should be retained in cache.
#[tokio::test]
async fn test_state_table_watermark_cache_write_chunk() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Timestamptz),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
    ];
    let data_types = column_descs
        .iter()
        .map(|c| c.data_type.clone())
        .collect::<Vec<_>>();

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        WatermarkCacheStateTable::from_table_catalog(&table, test_env.storage.clone(), None).await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 0);

    let watermark = Timestamptz::from_secs(0).unwrap().to_scalar_value();
    state_table.update_watermark(watermark);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    let inserts_1 = vec![
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
                Some(456i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
                Some(4888i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
                Some(1000i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(4000).unwrap().to_scalar_value()),
                Some(4888i64.into()),
            ]),
        ),
    ];

    let chunk = StreamChunk::from_rows(&inserts_1, &data_types);
    state_table.write_chunk(chunk);

    // We know the row_count of state table if row count matches.
    // So new rows inserted can be inserted into the cache.
    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 4);

    let deletes_1 = vec![
        (
            Op::Delete,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
                Some(456i64.into()),
            ]),
        ),
        (
            Op::Delete,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
                Some(4888i64.into()),
            ]),
        ),
        (
            Op::Delete,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
                Some(1000i64.into()),
            ]),
        ),
        // Leave 4000 in the state_table.
    ];

    let chunk = StreamChunk::from_rows(&deletes_1, &data_types);
    state_table.write_chunk(chunk);

    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.get_table_row_count().unwrap(), 1);

    let inserts_2 = vec![
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(5000).unwrap().to_scalar_value()),
                Some(456i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(6000).unwrap().to_scalar_value()),
                Some(4888i64.into()),
            ]),
        ),
        (
            Op::Insert,
            OwnedRow::new(vec![
                Some(Timestamptz::from_secs(7000).unwrap().to_scalar_value()),
                Some(1000i64.into()),
            ]),
        ),
    ];

    let chunk = StreamChunk::from_rows(&inserts_2, &data_types);
    state_table.write_chunk(chunk);

    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 4);
    assert_eq!(cache.get_table_row_count().unwrap(), 4);

    // Should not cleanup anything.
    let watermark = Timestamptz::from_secs(2500).unwrap().to_scalar_value();
    state_table.update_watermark(watermark);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    // After sync, we should scan all rows into watermark cache.
    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 4);
    assert_eq!(
        cache.lowest_key().unwrap(),
        Timestamptz::from_secs(4000)
            .unwrap()
            .to_scalar_value()
            .as_scalar_ref_impl()
    )
}

// Insert some values into the state table.
// Bump watermark, watermark value should partition the state table.
// Commit state table to trigger watermark state cleaning.
// Check the partitioned values (larger than watermark) should be retained in cache.
#[tokio::test]
async fn test_state_table_watermark_cache_refill() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), DataType::Timestamptz),
        ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
    ];
    let _data_types = column_descs
        .iter()
        .map(|c| c.data_type.clone())
        .collect::<Vec<_>>();

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        WatermarkCacheStateTable::from_table_catalog(&table, test_env.storage.clone(), None).await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    let rows = vec![
        OwnedRow::new(vec![
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(456i64.into()),
        ]),
        OwnedRow::new(vec![
            Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
            Some(4888i64.into()),
        ]),
        // Watermark Partition here later.
        OwnedRow::new(vec![
            Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
            Some(1000i64.into()),
        ]),
        OwnedRow::new(vec![
            Some(Timestamptz::from_secs(4000).unwrap().to_scalar_value()),
            Some(4888i64.into()),
        ]),
    ];

    for row in &rows {
        state_table.insert(row);
    }
    let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Unbounded, Unbounded);
    let inserted_rows: Vec<_> = state_table
        .iter_with_prefix(row::empty(), sub_range, PrefetchOptions::default())
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|row| row.unwrap())
        .collect();

    assert_eq!(inserted_rows.len(), 4);
    assert_eq!(inserted_rows[0], rows[0]);
    assert_eq!(inserted_rows[1], rows[1]);
    assert_eq!(inserted_rows[2], rows[2]);
    assert_eq!(inserted_rows[3], rows[3]);
    let cache = state_table.get_watermark_cache();
    // Before the first barrier, watermark cache won't be filled.
    assert_eq!(cache.len(), 0);

    let watermark = Timestamptz::from_secs(2500).unwrap().to_scalar_value();
    state_table.update_watermark(watermark);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    // After the first barrier, watermark cache won't be filled.
    let cache = state_table.get_watermark_cache();
    assert_eq!(cache.len(), 2);
    assert_eq!(
        cache.lowest_key().unwrap(),
        Timestamptz::from_secs(3000)
            .unwrap()
            .to_scalar_value()
            .as_scalar_ref_impl()
    )
}

#[tokio::test]
async fn test_state_table_iter_prefix_and_sub_range() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![OrderType::ascending(), OrderType::ascending()];
    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]));
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]));

    state_table.insert(OwnedRow::new(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);

    let sub_range1 = (
        std::ops::Bound::Included(OwnedRow::new(vec![Some(11_i32.into())])),
        std::ops::Bound::Excluded(OwnedRow::new(vec![Some(33_i32.into())])),
    );

    let iter = state_table
        .iter_with_prefix(pk_prefix, &sub_range1, Default::default())
        .await
        .unwrap();

    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await;
    assert!(res.is_none());

    let sub_range2: (Bound<OwnedRow>, Bound<OwnedRow>) = (
        std::ops::Bound::Excluded(OwnedRow::new(vec![Some(11_i32.into())])),
        std::ops::Bound::Unbounded,
    );

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);
    let iter = state_table
        .iter_with_prefix(pk_prefix, &sub_range2, Default::default())
        .await
        .unwrap();

    pin_mut!(iter);

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await;
    assert!(res.is_none());

    let sub_range3: (Bound<OwnedRow>, Bound<OwnedRow>) = (
        std::ops::Bound::Unbounded,
        std::ops::Bound::Included(OwnedRow::new(vec![Some(33_i32.into())])),
    );

    let pk_prefix = OwnedRow::new(vec![Some(1_i32.into())]);
    let iter = state_table
        .iter_with_prefix(pk_prefix, &sub_range3, Default::default())
        .await
        .unwrap();

    pin_mut!(iter);
    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await.unwrap().unwrap();

    assert_eq!(
        &OwnedRow::new(vec![
            Some(1_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ]),
        res.as_ref()
    );

    let res = iter.next().await;
    assert!(res.is_none());
}

#[tokio::test]
async fn test_replicated_state_table_replication() {
    type TestReplicatedStateTable = ReplicatedStateTable<HummockStorage, BasicSerde>;
    // Define the base table to replicate
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let order_types = vec![OrderType::ascending()];
    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_index = vec![0_usize];
    let read_prefix_len_hint = 1;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_index,
        read_prefix_len_hint,
    );

    let test_env = prepare_hummock_test_env().await;
    test_env.register_table(table.clone()).await;

    // Create the base state table
    let mut state_table =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    // Create a replicated state table
    let output_column_ids = vec![ColumnId::from(2), ColumnId::from(0)];
    let mut replicated_state_table =
        TestReplicatedStateTable::from_table_catalog_with_output_column_ids(
            &table,
            test_env.storage.clone(),
            None,
            output_column_ids,
        )
        .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();
    replicated_state_table.init_epoch(epoch).await.unwrap();

    // Insert first record into base state table
    state_table.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();
    replicated_state_table.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    {
        let range_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (
            std::ops::Bound::Included(OwnedRow::new(vec![Some(1_i32.into())])),
            std::ops::Bound::Included(OwnedRow::new(vec![Some(2_i32.into())])),
        );
        let iter = state_table
            .iter_with_vnode(SINGLETON_VNODE, &range_bounds, Default::default())
            .await
            .unwrap();
        let replicated_iter = replicated_state_table
            .iter_with_vnode_and_output_indices(SINGLETON_VNODE, &range_bounds, Default::default())
            .await
            .unwrap();
        pin_mut!(iter);
        pin_mut!(replicated_iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into()),
            ]),
            res.as_ref()
        );

        let res = replicated_iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![Some(111_i32.into()), Some(1_i32.into()),]),
            res.as_ref()
        );
    }

    // Test replication
    let state_table_chunk = [(
        Op::Insert,
        OwnedRow::new(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    )];
    let state_table_chunk = StreamChunk::from_rows(
        &state_table_chunk,
        &[DataType::Int32, DataType::Int32, DataType::Int32],
    );
    state_table.write_chunk(state_table_chunk);
    let replicate_chunk = [(
        Op::Insert,
        OwnedRow::new(vec![Some(222_i32.into()), Some(2_i32.into())]),
    )];
    let replicate_chunk =
        StreamChunk::from_rows(&replicate_chunk, &[DataType::Int32, DataType::Int32]);
    replicated_state_table.write_chunk(replicate_chunk);

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();
    replicated_state_table.commit(epoch).await.unwrap();

    {
        let range_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (
            std::ops::Bound::Excluded(OwnedRow::new(vec![Some(1_i32.into())])),
            std::ops::Bound::Included(OwnedRow::new(vec![Some(2_i32.into())])),
        );

        let iter = state_table
            .iter_with_vnode(SINGLETON_VNODE, &range_bounds, Default::default())
            .await
            .unwrap();

        let range_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) = (
            std::ops::Bound::Excluded(OwnedRow::new(vec![Some(1_i32.into())])),
            std::ops::Bound::Unbounded,
        );
        let replicated_iter = replicated_state_table
            .iter_with_vnode_and_output_indices(SINGLETON_VNODE, &range_bounds, Default::default())
            .await
            .unwrap();
        pin_mut!(iter);
        pin_mut!(replicated_iter);

        let res = iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into()),
            ]),
            res.as_ref()
        );

        let res = replicated_iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![Some(222_i32.into()), Some(2_i32.into()),]),
            res.as_ref()
        );
    }

    // Test that after commit, the local state store can
    // merge the replicated records with the local records
    test_env.commit_epoch(epoch.prev).await;

    {
        let range_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) =
            (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded);
        let replicated_iter = replicated_state_table
            .iter_with_vnode_and_output_indices(SINGLETON_VNODE, &range_bounds, Default::default())
            .await
            .unwrap();
        pin_mut!(replicated_iter);

        let res = replicated_iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![Some(111_i32.into()), Some(1_i32.into()),]),
            res.as_ref()
        );

        let res = replicated_iter.next().await.unwrap().unwrap();
        assert_eq!(
            &OwnedRow::new(vec![Some(222_i32.into()), Some(2_i32.into()),]),
            res.as_ref()
        );
    }
}

#[tokio::test]
async fn test_non_pk_prefix_watermark_read() {
    // Define the base table to replicate
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let order_types = vec![OrderType::ascending(), OrderType::ascending()];
    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 1;
    let mut table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs,
        order_types,
        pk_indices,
        read_prefix_len_hint,
    );

    // non-pk-prefix watermark
    let watermark_col_idx = 1;
    table.watermark_indices = vec![watermark_col_idx];
    table.clean_watermark_index_in_pk = Some(1);
    let test_env = prepare_hummock_test_env().await;
    test_env.register_table(table.clone()).await;

    // Create the base state table
    let mut state_table: crate::common::table::state_table::StateTableInner<HummockStorage> =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.init_epoch(epoch).await.unwrap();

    // Insert first record into base state table
    let r1 = OwnedRow::new(vec![
        Some(0_i32.into()),
        Some(1_i32.into()),
        Some(1_i32.into()),
    ]);
    state_table.insert(r1.clone());

    let r2 = OwnedRow::new(vec![
        Some(0_i32.into()),
        Some(2_i32.into()),
        Some(2_i32.into()),
    ]);
    state_table.insert(r2.clone());

    let r3 = OwnedRow::new(vec![
        Some(0_i32.into()),
        Some(3_i32.into()),
        Some(3_i32.into()),
    ]);
    state_table.insert(r3.clone());

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state_table.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    {
        // test read
        let item_1 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(1_i32.into())]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1, item_1);

        let item_2 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(2_i32.into())]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(r2, item_2);

        let item_3 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(3_i32.into())]))
            .await
            .unwrap()
            .unwrap();

        assert_eq!(r3, item_3);
    }

    {
        // update watermark
        let watermark = ScalarImpl::Int32(1);
        state_table.update_watermark(watermark);

        epoch.inc_for_test();
        test_env
            .storage
            .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
        state_table.commit(epoch).await.unwrap();
        test_env.commit_epoch(epoch.prev).await;

        // do not rewrite key-range or filter data for non-pk-prefix watermark
        let item_1 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(1_i32.into())]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1, item_1);

        let item_2 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(2_i32.into())]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r2, item_2);

        let item_3 = state_table
            .get_row(OwnedRow::new(vec![Some(0_i32.into()), Some(3_i32.into())]))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r3, item_3);
    }
}
