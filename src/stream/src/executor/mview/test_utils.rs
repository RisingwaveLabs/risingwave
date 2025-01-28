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

use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::{test_epoch, EpochPair};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::BatchTable;

use crate::common::table::state_table::StateTable;
use crate::common::table::test_utils::gen_pbtable;

pub async fn gen_basic_table(row_count: usize) -> BatchTable<MemoryStateStore> {
    let state_store = MemoryStateStore::new();

    let order_types = vec![OrderType::ascending(), OrderType::descending()];
    let column_ids = [0.into(), 1.into(), 2.into()];
    let column_descs = vec![
        ColumnDesc::unnamed(column_ids[0], DataType::Int32),
        ColumnDesc::unnamed(column_ids[1], DataType::Int32),
        ColumnDesc::unnamed(column_ids[2], DataType::Int32),
    ];
    let pk_indices = vec![0_usize, 1_usize];
    let mut state = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::from(0x42),
            column_descs.clone(),
            order_types,
            pk_indices,
            0,
        ),
        state_store.clone(),
        None,
    )
    .await;
    let table = BatchTable::for_test(
        state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        vec![0, 1, 2],
    );
    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    state.init_epoch(epoch).await.unwrap();

    for idx in 0..row_count {
        let idx = idx as i32;
        state.insert(OwnedRow::new(vec![
            Some(idx.into()),
            Some(idx.into()),
            Some(idx.into()),
        ]));
    }

    epoch.inc_for_test();
    state.commit(epoch).await.unwrap();

    table
}
