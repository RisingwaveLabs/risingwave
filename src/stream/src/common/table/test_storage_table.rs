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

use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
use risingwave_common::row::{self, OwnedRow, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::{test_epoch, EpochPair};
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::table::TableIter;

use crate::common::table::state_table::StateTable;
use crate::common::table::test_utils::{gen_pbtable, gen_pbtable_with_value_indices};

/// There are three struct in relational layer, StateTable, MemTable and StorageTable.
/// `StateTable` provides read/write interfaces to the upper layer streaming operator.
/// `MemTable` is an in-memory buffer used to cache operator operations.
#[tokio::test]
async fn test_storage_table_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_ids = [
        ColumnId::from(0),
        ColumnId::from(1),
        ColumnId::from(2),
        ColumnId::from(3),
        ColumnId::from(4),
    ];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ColumnDesc::unnamed(column_ids[3], DataType::Int32),
        ColumnDesc::unnamed(column_ids[4], DataType::Varchar),
    ];
    let pk_indices = vec![0_usize, 2_usize];
    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let value_indices = vec![1, 3, 4];
    let read_prefix_len_hint = 2;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        read_prefix_len_hint,
        value_indices.clone(),
    );

    test_env.register_table(table.clone()).await;
    let mut state =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let table = BatchTable::for_test(
        test_env.storage.clone(),
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices,
        value_indices,
    );
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.init_epoch(epoch).await.unwrap();

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        None,
        Some(11_i32.into()),
        Some(111_i32.into()),
        Some("1111".to_owned().into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some("2222".to_owned().into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(3_i32.into()),
        None,
        Some(33_i32.into()),
        Some(333_i32.into()),
        Some("3333".to_owned().into()),
    ]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some("2222".to_owned().into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), Some(11_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![
            Some(1_i32.into()),
            None,
            Some(11_i32.into()),
            Some(111_i32.into()),
            Some("1111".to_owned().into())
        ]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), Some(22_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), Some(33_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(OwnedRow::new(vec![
            Some(3_i32.into()),
            None,
            Some(33_i32.into()),
            Some(333_i32.into()),
            Some("3333".to_owned().into())
        ]))
    );

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_shuffled_column_id_for_storage_table_get_row() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_ids = [ColumnId::from(3), ColumnId::from(2), ColumnId::from(1)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let pk_indices = vec![0_usize, 1_usize];
    let read_prefix_len_hint = 2;
    let table = gen_pbtable(
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        read_prefix_len_hint,
    );

    test_env.register_table(table.clone()).await;
    let mut state =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.init_epoch(epoch).await.unwrap();

    let table = BatchTable::for_test(
        test_env.storage.clone(),
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        vec![0, 1, 2],
    );

    state.insert(OwnedRow::new(vec![Some(1_i32.into()), None, None]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![Some(3_i32.into()), None, None]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row1_res,
        Some(OwnedRow::new(vec![Some(1_i32.into()), None, None,]))
    );

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(
        get_row3_res,
        Some(OwnedRow::new(vec![Some(3_i32.into()), None, None]))
    );

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

// test row-based encoding in batch mode
#[tokio::test]
async fn test_row_based_storage_table_point_get_in_batch_mode() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let value_indices: Vec<usize> = vec![0, 1, 2];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        read_prefix_len_hint,
        value_indices.clone(),
    );

    test_env.register_table(table.clone()).await;
    let mut state =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];
    let table = BatchTable::for_test_with_partial_columns(
        test_env.storage.clone(),
        TEST_TABLE_ID,
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        value_indices,
    );
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.init_epoch(epoch).await.unwrap();

    state.insert(OwnedRow::new(vec![Some(1_i32.into()), None, None]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![Some(3_i32.into()), None, None]));

    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        None,
        Some(222_i32.into()),
    ]));
    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    let get_row1_res = table
        .get_row(
            &OwnedRow::new(vec![Some(1_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();

    // Here only column_ids_partial will be get
    assert_eq!(get_row1_res, Some(OwnedRow::new(vec![None, None,])));

    let get_row2_res = table
        .get_row(
            &OwnedRow::new(vec![Some(2_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_row2_res, None);

    let get_row3_res = table
        .get_row(
            &OwnedRow::new(vec![Some(3_i32.into()), None]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_row3_res, Some(OwnedRow::new(vec![None, None])));

    let get_no_exist_res = table
        .get_row(
            &OwnedRow::new(vec![Some(0_i32.into()), Some(00_i32.into())]),
            HummockReadEpoch::Committed(epoch.prev),
        )
        .await
        .unwrap();
    assert_eq!(get_no_exist_res, None);
}

#[tokio::test]
async fn test_batch_scan_with_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let column_ids = [
        ColumnId::from(0),
        ColumnId::from(1),
        ColumnId::from(2),
        ColumnId::from(3),
    ];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ColumnDesc::unnamed(column_ids[3], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 2_usize];
    let value_indices: Vec<usize> = vec![1, 3];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        read_prefix_len_hint,
        value_indices.clone(),
    );

    test_env.register_table(table.clone()).await;
    let mut state =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let column_ids_partial = vec![ColumnId::from(1), ColumnId::from(2)];

    let table = BatchTable::for_test_with_partial_columns(
        test_env.storage.clone(),
        TEST_TABLE_ID,
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        value_indices,
    );
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.init_epoch(epoch).await.unwrap();

    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(11_i32.into()),
        Some(111_i32.into()),
        Some(1111_i32.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));
    state.delete(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(22_i32.into()),
        Some(222_i32.into()),
        Some(2222_i32.into()),
    ]));

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    let iter = table
        .batch_iter(
            HummockReadEpoch::Committed(epoch.prev),
            false,
            Default::default(),
        )
        .await
        .unwrap();
    pin_mut!(iter);

    let res = iter.next_row().await.unwrap();

    // only scan two columns
    assert_eq!(
        OwnedRow::new(vec![Some(11_i32.into()), Some(111_i32.into())]),
        res.unwrap()
    );

    let res = iter.next_row().await.unwrap();
    assert!(res.is_none());
}

#[tokio::test]
async fn test_batch_scan_chunk_with_value_indices() {
    const TEST_TABLE_ID: TableId = TableId { table_id: 233 };
    let test_env = prepare_hummock_test_env().await;

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let column_ids = [
        ColumnId::from(0),
        ColumnId::from(1),
        ColumnId::from(2),
        ColumnId::from(3),
    ];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
        ColumnDesc::unnamed(column_ids[3], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 2_usize];
    let value_indices: Vec<usize> = vec![1, 3];
    let read_prefix_len_hint = 0;
    let table = gen_pbtable_with_value_indices(
        TEST_TABLE_ID,
        column_descs.clone(),
        order_types.clone(),
        pk_indices.clone(),
        read_prefix_len_hint,
        value_indices.clone(),
    );

    test_env.register_table(table.clone()).await;
    let mut state =
        StateTable::from_table_catalog_inconsistent_op(&table, test_env.storage.clone(), None)
            .await;

    let output_column_idx: Vec<usize> = vec![1, 2];
    let column_ids_partial = output_column_idx
        .iter()
        .map(|i| ColumnId::from(*i as i32))
        .collect_vec();

    let table = BatchTable::for_test_with_partial_columns(
        test_env.storage.clone(),
        TEST_TABLE_ID,
        column_descs.clone(),
        column_ids_partial,
        order_types.clone(),
        pk_indices,
        value_indices.clone(),
    );
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.init_epoch(epoch).await.unwrap();

    let gen_row = |i: i32, is_update: bool| {
        let scale = if is_update { 10 } else { 1 };
        OwnedRow::new(vec![
            Some(i.into()),
            Some((i * 10 * scale).into()),
            Some((i * 100).into()),
            Some((i * 1000 * scale).into()),
        ])
    };

    let mut rows = vec![];
    let insert_row_idx = (0..20).collect_vec();
    let delete_row_idx = (0..5).map(|i| i * 2).collect_vec();
    let updated_row_idx = (0..5).map(|i| i * 2 + 1).collect_vec();
    for i in &insert_row_idx {
        let row = gen_row(*i, false);
        state.insert(row.clone());
        rows.push(row);
    }

    for i in &updated_row_idx {
        let row = gen_row(*i, true);
        state.update(rows[*i as usize].clone(), row.clone());
        rows[*i as usize] = row;
    }

    for i in &delete_row_idx {
        let row = gen_row(*i, false);
        state.delete(row);
    }

    let mut rows = rows
        .into_iter()
        .enumerate()
        .filter(|(idx, _)| !delete_row_idx.contains(&(*idx as i32)))
        .map(|(_, row)| row)
        .collect_vec();

    epoch.inc_for_test();
    test_env
        .storage
        .start_epoch(epoch.curr, HashSet::from_iter([TEST_TABLE_ID]));
    state.commit(epoch).await.unwrap();
    test_env.commit_epoch(epoch.prev).await;

    let chunk_size = 2;
    let iter = table
        .batch_chunk_iter_with_pk_bounds(
            HummockReadEpoch::Committed(epoch.prev),
            row::empty(),
            ..,
            false,
            chunk_size,
            Default::default(),
        )
        .await
        .unwrap();
    pin_mut!(iter);

    let chunks: Vec<_> = iter.collect().await;
    for (chunk, expected_rows) in chunks.into_iter().zip_eq(rows.chunks_mut(chunk_size)) {
        let mut builder =
            DataChunkBuilder::new(vec![DataType::Int32, DataType::Int32], 2 * chunk_size);
        for row in expected_rows {
            let _ = builder.append_one_row(row.clone().project(&output_column_idx));
        }
        assert_eq!(builder.consume_all().unwrap(), chunk.unwrap());
    }
}
