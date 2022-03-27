// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::Keyspace;

use crate::executor::ManagedMViewState;

#[tokio::test]
async fn test_mview_table() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let epoch = u64::MAX;
    let cell_1_0 = table
        .get_for_test(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 0, epoch)
        .await
        .unwrap();
    assert!(cell_1_0.is_some());
    assert_eq!(*cell_1_0.unwrap().unwrap().as_int32(), 1);
    let cell_1_1 = table
        .get_for_test(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 1, epoch)
        .await
        .unwrap();
    assert!(cell_1_1.is_some());
    assert_eq!(*cell_1_1.unwrap().unwrap().as_int32(), 11);
    let cell_1_2 = table
        .get_for_test(Row(vec![Some(1_i32.into()), Some(11_i32.into())]), 2, epoch)
        .await
        .unwrap();
    assert!(cell_1_2.is_some());
    assert_eq!(*cell_1_2.unwrap().unwrap().as_int32(), 111);

    let cell_2_0 = table
        .get_for_test(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 0, epoch)
        .await
        .unwrap();
    assert!(cell_2_0.is_none());
    let cell_2_1 = table
        .get_for_test(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 1, epoch)
        .await
        .unwrap();
    assert!(cell_2_1.is_none());
    let cell_2_2 = table
        .get_for_test(Row(vec![Some(2_i32.into()), Some(22_i32.into())]), 2, epoch)
        .await
        .unwrap();
    assert!(cell_2_2.is_none());
}

#[tokio::test]
async fn test_mview_table_for_string() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];
    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
        ]),
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]),
    );
    state.put(
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
        ]),
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]),
    );
    state.delete(Row(vec![
        Some("2".to_string().into()),
        Some("22".to_string().into()),
    ]));
    state.flush(epoch).await.unwrap();

    let epoch = u64::MAX;
    let cell_1_0 = table
        .get_for_test(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            0,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_1_0.is_some());
    assert_eq!(
        Some(cell_1_0.unwrap().unwrap().as_utf8().to_string()),
        Some("1".to_string())
    );
    let cell_1_1 = table
        .get_for_test(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            1,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_1_1.is_some());
    assert_eq!(
        Some(cell_1_1.unwrap().unwrap().as_utf8().to_string()),
        Some("11".to_string())
    );
    let cell_1_2 = table
        .get_for_test(
            Row(vec![
                Some("1".to_string().into()),
                Some("11".to_string().into()),
            ]),
            2,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_1_2.is_some());
    assert_eq!(
        Some(cell_1_2.unwrap().unwrap().as_utf8().to_string()),
        Some("111".to_string())
    );

    let cell_2_0 = table
        .get_for_test(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            0,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_2_0.is_none());
    let cell_2_1 = table
        .get_for_test(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            1,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_2_1.is_none());
    let cell_2_2 = table
        .get_for_test(
            Row(vec![
                Some("2".to_string().into()),
                Some("22".to_string().into()),
            ]),
            2,
            epoch,
        )
        .await
        .unwrap();
    assert!(cell_2_2.is_none());
}

#[tokio::test]
async fn test_mview_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let epoch = u64::MAX;
    let mut iter = table.iter(epoch).await.unwrap();

    let res = iter.next().await.unwrap();
    assert!(res.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into())
        ]),
        res.unwrap()
    );

    let res = iter.next().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_multi_mview_table_iter() {
    let state_store = MemoryStateStore::new();
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];

    let keyspace_1 = Keyspace::executor_root(state_store.clone(), 0x1111);
    let keyspace_2 = Keyspace::executor_root(state_store.clone(), 0x2222);
    let epoch: u64 = 0;

    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs_1 = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let column_descs_2 = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[1], DataType::Varchar),
        ColumnDesc::unnamed(column_ids[2], DataType::Varchar),
    ];

    let mut state_1 =
        ManagedMViewState::new(keyspace_1.clone(), column_ids.clone(), order_types.clone());
    let mut state_2 = ManagedMViewState::new(keyspace_2.clone(), column_ids, order_types.clone());

    let table_1 =
        CellBasedTable::new_for_test(keyspace_1.clone(), column_descs_1, order_types.clone());
    let table_2 = CellBasedTable::new_for_test(keyspace_2.clone(), column_descs_2, order_types);

    state_1.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state_1.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state_1.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));

    state_2.put(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
        ]),
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into()),
        ]),
    );
    state_2.put(
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
        ]),
        Row(vec![
            Some("2".to_string().into()),
            Some("22".to_string().into()),
            Some("222".to_string().into()),
        ]),
    );
    state_2.delete(Row(vec![
        Some("2".to_string().into()),
        Some("22".to_string().into()),
    ]));

    state_1.flush(epoch).await.unwrap();
    state_2.flush(epoch).await.unwrap();

    let mut iter_1 = table_1.iter(epoch).await.unwrap();
    let mut iter_2 = table_2.iter(epoch).await.unwrap();

    let res_1_1 = iter_1.next().await.unwrap();
    assert!(res_1_1.is_some());
    assert_eq!(
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
        res_1_1.unwrap()
    );
    let res_1_2 = iter_1.next().await.unwrap();
    assert!(res_1_2.is_none());

    let res_2_1 = iter_2.next().await.unwrap();
    assert!(res_2_1.is_some());
    assert_eq!(
        Row(vec![
            Some("1".to_string().into()),
            Some("11".to_string().into()),
            Some("111".to_string().into())
        ]),
        res_2_1.unwrap()
    );
    let res_2_2 = iter_2.next().await.unwrap();
    assert!(res_2_2.is_none());
}

#[tokio::test]
async fn test_mview_scan_empty_column_ids_cardinality() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    // let pk_columns = vec![0, 1]; leave a message to indicate pk columns
    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);

    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![
            Some(1_i32.into()),
            Some(11_i32.into()),
            Some(111_i32.into()),
        ]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![
            Some(2_i32.into()),
            Some(22_i32.into()),
            Some(222_i32.into()),
        ]),
    );
    state.flush(epoch).await.unwrap();

    let chunk = {
        let mut iter = table.iter(u64::MAX).await.unwrap();
        iter.collect_data_chunk(&table, None)
            .await
            .unwrap()
            .unwrap()
    };
    assert_eq!(chunk.cardinality(), 2);
}

#[tokio::test]
async fn test_get_row_by_scan() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![Some(1_i32.into()), None, None]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
    );
    state.put(
        Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
        Row(vec![Some(3_i32.into()), None, None]),
    );

    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row_by_scan(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row_by_scan(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row_by_scan(&Row(vec![Some(3_i32.into()), Some(33_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row_by_scan(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_get_row_by_muti_get() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut state = ManagedMViewState::new(keyspace.clone(), column_ids, order_types.clone());
    let table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;

    state.put(
        Row(vec![Some(1_i32.into()), Some(11_i32.into())]),
        Row(vec![None, None, None]),
    );
    state.put(
        Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
        Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]),
    );
    state.put(
        Row(vec![Some(3_i32.into()), Some(33_i32.into())]),
        Row(vec![None, None, Some(3_i32.into())]),
    );

    state.delete(Row(vec![Some(2_i32.into()), Some(22_i32.into())]));
    state.flush(epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row1_res, Some(Row(vec![None, None, None])));

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(&Row(vec![Some(3_i32.into()), Some(33_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(Row(vec![None, None, Some(3_i32.into())]))
    );

    let get_no_exist_res = table
        .get_row(&Row(vec![Some(0_i32.into()), Some(00_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_cell_based_write() {
    let state_store = MemoryStateStore::new();
    let column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::Ascending, OrderType::Descending];
    let keyspace = Keyspace::executor_root(state_store, 0x42);
    let mut table = CellBasedTable::new_for_test(keyspace.clone(), column_descs, order_types);
    let epoch: u64 = 0;
    let pk1 = Row(vec![Some(1_i32.into()), Some(11_i32.into())]);
    let value1 = Row(vec![Some(1_i32.into()), None, Some(111_i32.into())]);

    let pk2 = Row(vec![Some(2_i32.into()), Some(22_i32.into())]);
    let value2 = Row(vec![None, Some(22_i32.into()), Some(222_i32.into())]);

    let batch1 = vec![(pk1, Some(value1)), (pk2, Some(value2))];

    // cell_based insert row
    table.batch_write_rows(batch1, epoch).await.unwrap();

    let epoch = u64::MAX;

    let get_row1_res = table
        .get_row(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(Row(vec![Some(1_i32.into()), None, Some(111_i32.into()),])),
    );

    let get_row2_res = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row2_res,
        Some(Row(vec![None, Some(22_i32.into()), Some(222_i32.into()),])),
    );

    // cell_based delete row
    let delete_pk1 = Row(vec![Some(2_i32.into()), Some(22_i32.into())]);
    let delete_value1 = Row(vec![Some(2_i32.into()), None, Some(222_i32.into())]);
    table
        .delete_row(&delete_pk1, Some(delete_value1), epoch)
        .await
        .unwrap();
    let get_delete_row2_res1 = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_delete_row2_res1,
        Some(Row(vec![None, Some(22_i32.into()), None]))
    );

    let delete_pk2 = Row(vec![Some(2_i32.into()), Some(22_i32.into())]);
    let delete_value2 = Row(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
    ]);
    table
        .delete_row(&delete_pk2, Some(delete_value2), epoch)
        .await
        .unwrap();

    let get_delete_row2_res2 = table
        .get_row(&Row(vec![Some(2_i32.into()), Some(22_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(get_delete_row2_res2, None);

    // cell_based update row
    let update_pk = Row(vec![Some(1_i32.into()), Some(11_i32.into())]);
    let old_value = Row(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
    ]);
    let update_value = Row(vec![
        Some(3_i32.into()),
        Some(33_i32.into()),
        Some(333_i32.into()),
    ]);
    table
        .update_row(&update_pk, Some(old_value), Some(update_value), epoch)
        .await
        .unwrap();

    let get_update_row3_res = table
        .get_row(&Row(vec![Some(1_i32.into()), Some(11_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_update_row3_res,
        Some(Row(vec![
            Some(3_i32.into()),
            Some(33_i32.into()),
            Some(333_i32.into()),
        ])),
    );

    // cell_based batch_insert row
    let batch_pk1 = Row(vec![Some(4_i32.into()), Some(44_i32.into())]);
    let batch_value1 = Row(vec![
        Some(4_i32.into()),
        Some(44_i32.into()),
        Some(444_i32.into()),
    ]);
    let batch_pk2 = Row(vec![Some(5_i32.into()), Some(55_i32.into())]);
    let batch_value2 = Row(vec![
        Some(5_i32.into()),
        Some(55_i32.into()),
        Some(555_i32.into()),
    ]);

    let batch2 = vec![
        (batch_pk1, Some(batch_value1)),
        (batch_pk2, Some(batch_value2)),
    ];

    table.batch_write_rows(batch2, epoch).await.unwrap();

    let get_row4_res = table
        .get_row(&Row(vec![Some(4_i32.into()), Some(44_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row4_res,
        Some(Row(vec![
            Some(4_i32.into()),
            Some(44_i32.into()),
            Some(444_i32.into()),
        ])),
    );

    let get_row5_res = table
        .get_row(&Row(vec![Some(5_i32.into()), Some(55_i32.into())]), epoch)
        .await
        .unwrap();
    assert_eq!(
        get_row5_res,
        Some(Row(vec![
            Some(5_i32.into()),
            Some(55_i32.into()),
            Some(555_i32.into()),
        ])),
    );
}
