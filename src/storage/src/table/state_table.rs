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
#![allow(dead_code)]
use std::sync::Arc;

use risingwave_common::array::{Op, Row};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::util::sort_util::OrderType;

use super::cell_based_table::{CellBasedTable, CellBasedTableRowIter};
use super::mem_table::{MemTable, RowOp};
use crate::error::StorageResult;
use crate::monitor::StateStoreMetrics;
use crate::{Keyspace, StateStore};

/// `StateTable` is the interface accessing relational data in KV(`StateStore`) with encoding
#[derive(Clone)]
pub struct StateTable<S: StateStore> {
    // /// Ordering of primary key (for assertion)
    order_types: Vec<OrderType>,

    /// buffer key/values
    mem_table: MemTable,

    /// Relation layer
    cell_based_table: CellBasedTable<S>,
}
impl<S: StateStore> StateTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        Self {
            order_types: order_types.clone(),
            mem_table: MemTable::new(),
            cell_based_table: CellBasedTable::new_for_test(keyspace, column_descs, order_types),
        }
    }

    /// read methods
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let mem_table_res = self.mem_table.get_row(pk).unwrap();
        if let Some(res) = mem_table_res {
            match res {
                RowOp::Insert(row) => Ok(Some(row)),
                RowOp::Delete(_) => Ok(None),
                RowOp::Update((_, new_row)) => Ok(Some(new_row)),
            }
        } else {
            self.cell_based_table.get_row(pk, epoch).await
        }
    }

    /// write methods
    pub fn insert(&mut self, pk: Row, value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        self.mem_table.insert(pk, value)?;
        Ok(())
    }

    pub fn delete(&mut self, pk: Row, old_value: Row) -> StorageResult<()> {
        assert_eq!(self.order_types.len(), pk.size());
        self.mem_table.delete(pk, old_value)?;
        Ok(())
    }

    pub fn update(&mut self, _pk: Row, _old_value: Row, _new_value: Row) -> StorageResult<()> {
        todo!()
    }

    pub async fn commit(&mut self, new_epoch: u64) -> StorageResult<()> {
        let mut rows = Vec::new();
        let mem_table = std::mem::take(&mut self.mem_table).into_parts();
        for (pk, row_op) in mem_table {
            match row_op {
                RowOp::Insert(row) => rows.push((Op::Insert, pk, row)),
                RowOp::Delete(row) => rows.push((Op::Delete, pk, row)),
                RowOp::Update((old_row, new_row)) => {
                    rows.push((Op::UpdateDelete, pk.clone(), old_row));
                    rows.push((Op::UpdateInsert, pk, new_row));
                }
            }
        }
        self.cell_based_table
            .batch_write_rows(rows, new_epoch)
            .await?;
        Ok(())
    }

    pub async fn iter(&self, _pk: Row) -> StorageResult<StateTableRowIter<S>> {
        todo!()
    }
}

pub struct StateTableRowIter<S: StateStore> {
    cell_based_iter: CellBasedTableRowIter<S>,
    mem_table: MemTable,
}

impl<S: StateStore> StateTableRowIter<S> {
    async fn new(
        _keyspace: Keyspace<S>,
        _table_descs: Vec<ColumnDesc>,
        _epoch: u64,
        _stats: Arc<StateStoreMetrics>,
    ) -> StorageResult<Self> {
        todo!()
    }

    async fn next(&mut self) -> StorageResult<Option<Row>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::memory::MemoryStateStore;

    #[tokio::test]
    async fn test_state_table() -> StorageResult<()> {
        let state_store = MemoryStateStore::new();
        let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ];
        let order_types = vec![OrderType::Ascending];
        let mut state_table = StateTable::new(keyspace.clone(), column_descs, order_types);
        let mut epoch: u64 = 0;
        state_table
            .insert(
                Row(vec![Some(1_i32.into())]),
                Row(vec![
                    Some(1_i32.into()),
                    Some(11_i32.into()),
                    Some(111_i32.into()),
                ]),
            )
            .unwrap();
        state_table
            .insert(
                Row(vec![Some(2_i32.into())]),
                Row(vec![
                    Some(2_i32.into()),
                    Some(22_i32.into()),
                    Some(222_i32.into()),
                ]),
            )
            .unwrap();
        state_table
            .insert(
                Row(vec![Some(3_i32.into())]),
                Row(vec![
                    Some(3_i32.into()),
                    Some(33_i32.into()),
                    Some(333_i32.into()),
                ]),
            )
            .unwrap();

        // test read visibility
        let row1 = state_table
            .get_row(&Row(vec![Some(1_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(
            row1,
            Some(Row(vec![
                Some(1_i32.into()),
                Some(11_i32.into()),
                Some(111_i32.into())
            ]))
        );

        let row2 = state_table
            .get_row(&Row(vec![Some(2_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(
            row2,
            Some(Row(vec![
                Some(2_i32.into()),
                Some(22_i32.into()),
                Some(222_i32.into())
            ]))
        );

        state_table
            .delete(
                Row(vec![Some(2_i32.into())]),
                Row(vec![Some(2_i32.into()), Some(22_i32.into())]),
            )
            .unwrap();

        let row2_delete = state_table
            .get_row(&Row(vec![Some(2_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row2_delete, None);

        state_table.commit(epoch).await.unwrap();

        let row2_delete_commit = state_table
            .get_row(&Row(vec![Some(2_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row2_delete_commit, None);

        epoch += 1;
        state_table
            .delete(
                Row(vec![Some(3_i32.into())]),
                Row(vec![
                    Some(3_i32.into()),
                    Some(33_i32.into()),
                    Some(333_i32.into()),
                ]),
            )
            .unwrap();

        state_table
            .insert(
                Row(vec![Some(4_i32.into())]),
                Row(vec![Some(4_i32.into()), None, None]),
            )
            .unwrap();
        state_table
            .insert(Row(vec![Some(5_i32.into())]), Row(vec![None, None, None]))
            .unwrap();

        let row4 = state_table
            .get_row(&Row(vec![Some(4_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row4, Some(Row(vec![Some(4_i32.into()), None, None])));

        let row5 = state_table
            .get_row(&Row(vec![Some(5_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row5, Some(Row(vec![None, None, None])));

        let non_exist_row = state_table
            .get_row(&Row(vec![Some(0_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(non_exist_row, None);

        state_table
            .delete(
                Row(vec![Some(4_i32.into())]),
                Row(vec![Some(4_i32.into()), None, None]),
            )
            .unwrap();

        state_table.commit(epoch).await.unwrap();

        let row3_delete = state_table
            .get_row(&Row(vec![Some(3_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row3_delete, None);

        let row4_delete = state_table
            .get_row(&Row(vec![Some(4_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row4_delete, None);

        Ok(())
    }

    #[tokio::test]
    async fn test_state_table_update_insert() -> StorageResult<()> {
        let state_store = MemoryStateStore::new();
        let keyspace = Keyspace::executor_root(state_store.clone(), 0x42);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::from(0), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::from(1), DataType::Int32),
            ColumnDesc::unnamed(ColumnId::from(2), DataType::Int32),
        ];
        let order_types = vec![OrderType::Ascending];
        let mut state_table = StateTable::new(keyspace.clone(), column_descs, order_types);
        let mut epoch: u64 = 0;
        state_table
            .insert(
                Row(vec![Some(6_i32.into())]),
                Row(vec![
                    Some(6_i32.into()),
                    Some(66_i32.into()),
                    Some(666_i32.into()),
                ]),
            )
            .unwrap();

        state_table
            .insert(
                Row(vec![Some(7_i32.into())]),
                Row(vec![Some(7_i32.into()), None, None]),
            )
            .unwrap();
        state_table.commit(epoch).await.unwrap();

        epoch += 1;
        state_table
            .delete(
                Row(vec![Some(6_i32.into())]),
                Row(vec![
                    Some(6_i32.into()),
                    Some(66_i32.into()),
                    Some(666_i32.into()),
                ]),
            )
            .unwrap();
        state_table
            .insert(
                Row(vec![Some(6_i32.into())]),
                Row(vec![Some(6666_i32.into()), None, None]),
            )
            .unwrap();

        state_table
            .delete(
                Row(vec![Some(7_i32.into())]),
                Row(vec![Some(7_i32.into()), None, None]),
            )
            .unwrap();
        state_table
            .insert(
                Row(vec![Some(7_i32.into())]),
                Row(vec![Some(7777_i32.into()), Some(7777_i32.into()), None]),
            )
            .unwrap();
        let row6 = state_table
            .get_row(&Row(vec![Some(6_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(row6, Some(Row(vec![Some(6666_i32.into()), None, None])));

        let row7 = state_table
            .get_row(&Row(vec![Some(7_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(
            row7,
            Some(Row(vec![
                Some(7777_i32.into()),
                Some(7777_i32.into()),
                None
            ]))
        );

        state_table.commit(epoch).await.unwrap();

        let row6_commit = state_table
            .get_row(&Row(vec![Some(6_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(
            row6_commit,
            Some(Row(vec![Some(6666_i32.into()), None, None]))
        );
        let row7_commit = state_table
            .get_row(&Row(vec![Some(7_i32.into())]), epoch)
            .await
            .unwrap();
        assert_eq!(
            row7_commit,
            Some(Row(vec![
                Some(7777_i32.into()),
                Some(7777_i32.into()),
                None
            ]))
        );

        Ok(())
    }
}
