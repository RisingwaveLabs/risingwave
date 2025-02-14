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

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use risingwave_common::catalog::{ColumnDesc, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::{TxnId, TxnIdGenerator};
use risingwave_common::util::worker_util::WorkerNodeId;

use crate::error::{DmlError, Result};
use crate::{TableDmlHandle, TableDmlHandleRef};

pub type DmlManagerRef = Arc<DmlManager>;

#[derive(Debug)]
pub struct TableReader {
    version_id: TableVersionId,
    pub handle: Weak<TableDmlHandle>,
}

/// [`DmlManager`] manages the communication between batch data manipulation and streaming
/// processing.
/// NOTE: `TableDmlHandle` is used here as an out-of-the-box solution. We should further optimize
/// its implementation (e.g. directly expose a channel instead of offering a `write_chunk`
/// interface).
#[derive(Debug)]
pub struct DmlManager {
    table_readers: RwLock<HashMap<TableId, TableReader>>,
    txn_id_generator: TxnIdGenerator,
    dml_channel_initial_permits: usize,
}

impl DmlManager {
    pub fn new(worker_node_id: WorkerNodeId, dml_channel_initial_permits: usize) -> Self {
        Self {
            table_readers: RwLock::new(HashMap::new()),
            txn_id_generator: TxnIdGenerator::new(worker_node_id),
            dml_channel_initial_permits,
        }
    }

    pub fn for_test() -> Self {
        const TEST_DML_CHANNEL_INIT_PERMITS: usize = 32768;
        Self::new(WorkerNodeId::default(), TEST_DML_CHANNEL_INIT_PERMITS)
    }

    /// Register a new DML reader for a table. If the reader for this version of the table already
    /// exists, returns a reference to the existing reader.
    pub fn register_reader(
        &self,
        table_id: TableId,
        table_version_id: TableVersionId,
        column_descs: &[ColumnDesc],
    ) -> Result<TableDmlHandleRef> {
        let mut table_readers = self.table_readers.write();
        // Clear invalid table readers.
        table_readers.retain(|_, r| r.handle.strong_count() > 0);

        macro_rules! new_handle {
            ($entry:ident) => {{
                let handle = Arc::new(TableDmlHandle::new(
                    column_descs.to_vec(),
                    self.dml_channel_initial_permits,
                ));
                $entry.insert(TableReader {
                    version_id: table_version_id,
                    handle: Arc::downgrade(&handle),
                });
                handle
            }};
        }

        let handle = match table_readers.entry(table_id) {
            // Create a new reader. This happens when the first `DmlExecutor` of this table is
            // activated on this compute node.
            Entry::Vacant(v) => new_handle!(v),

            Entry::Occupied(mut o) => {
                let TableReader { version_id, handle } = o.get();

                match table_version_id.cmp(version_id) {
                    // This should never happen as the schema change is guaranteed to happen after a
                    // table is successfully created and all the readers are registered.
                    Ordering::Less => unreachable!("table version `{table_version_id}` expired"),

                    // Register with the correct version. This happens when the following
                    // `DmlExecutor`s of this table is activated on this compute
                    // node.
                    Ordering::Equal => handle
                        .upgrade()
                        .inspect(|handle| {
                            assert_eq!(
                                handle.column_descs(),
                                column_descs,
                                "dml handler registers with same version but different schema"
                            )
                        })
                        .expect("the first dml executor is gone"), // this should never happen

                    // A new version of the table is activated, overwrite the old reader.
                    Ordering::Greater => new_handle!(o),
                }
            }
        };

        Ok(handle)
    }

    pub fn table_dml_handle(
        &self,
        table_id: TableId,
        table_version_id: TableVersionId,
    ) -> Result<TableDmlHandleRef> {
        let table_dml_handle = {
            let table_readers = self.table_readers.read();

            match table_readers.get(&table_id) {
                Some(TableReader { version_id, handle }) => {
                    match table_version_id.cmp(version_id) {
                        // A new version of the table is activated, but the DML request is still on
                        // the old version.
                        Ordering::Less => {
                            return Err(DmlError::SchemaChanged);
                        }

                        // Write the chunk of correct version to the table.
                        Ordering::Equal => handle.upgrade(),

                        // This should never happen as the notification of the new version is
                        // guaranteed to happen after all new readers are activated.
                        Ordering::Greater => {
                            unreachable!("table version `{table_version_id} not registered")
                        }
                    }
                }
                None => None,
            }
        }
        .ok_or(DmlError::NoReader)?;

        Ok(table_dml_handle)
    }

    pub fn clear(&self) {
        self.table_readers.write().clear()
    }

    pub fn gen_txn_id(&self) -> TxnId {
        self.txn_id_generator.gen_txn_id()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::INITIAL_TABLE_VERSION_ID;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;

    use super::*;

    const TEST_TRANSACTION_ID: TxnId = 0;
    const TEST_SESSION_ID: u32 = 0;

    #[tokio::test]
    async fn test_register_and_drop() {
        let dml_manager = DmlManager::for_test();
        let table_id = TableId::new(1);
        let table_version_id = INITIAL_TABLE_VERSION_ID;
        let column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let chunk = || StreamChunk::from_pretty("F\n+ 1");

        let h1 = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();
        let h2 = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();

        // They should be the same handle.
        assert!(Arc::ptr_eq(&h1, &h2));

        // Start reading.
        let r1 = h1.stream_reader();
        let r2 = h2.stream_reader();

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, table_version_id)
            .unwrap();
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();
        write_handle.begin().unwrap();

        // Should be able to write to the table.
        write_handle.write_chunk(chunk()).await.unwrap();

        // After dropping the corresponding reader, the write handle should be not allowed to write.
        // This is to simulate the scale-in of DML executors.
        drop(r1);

        write_handle.write_chunk(chunk()).await.unwrap_err();

        // Unless we create a new write handle.
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();
        write_handle.begin().unwrap();
        write_handle.write_chunk(chunk()).await.unwrap();

        // After dropping the last reader, no more writes are allowed.
        // This is to simulate the dropping of the table.
        drop(r2);
        write_handle.write_chunk(chunk()).await.unwrap_err();
    }

    #[tokio::test]
    async fn test_versioned() {
        let dml_manager = DmlManager::for_test();
        let table_id = TableId::new(1);

        let old_version_id = INITIAL_TABLE_VERSION_ID;
        let old_column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let old_chunk = || StreamChunk::from_pretty("F\n+ 1");

        let new_version_id = old_version_id + 1;
        let new_column_descs = vec![
            ColumnDesc::unnamed(100.into(), DataType::Float64),
            ColumnDesc::unnamed(101.into(), DataType::Float64),
        ];
        let new_chunk = || StreamChunk::from_pretty("F F\n+ 1 2");

        // Start reading.
        let old_h = dml_manager
            .register_reader(table_id, old_version_id, &old_column_descs)
            .unwrap();
        let _old_r = old_h.stream_reader();

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, old_version_id)
            .unwrap();
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();
        write_handle.begin().unwrap();

        // Should be able to write to the table.
        write_handle.write_chunk(old_chunk()).await.unwrap();

        // Start reading the new version.
        let new_h = dml_manager
            .register_reader(table_id, new_version_id, &new_column_descs)
            .unwrap();
        let _new_r = new_h.stream_reader();

        // Still be able to write to the old write handle, if the channel is not closed.
        write_handle.write_chunk(old_chunk()).await.unwrap();

        // However, it is no longer possible to create a `table_dml_handle` with the old version;
        dml_manager
            .table_dml_handle(table_id, old_version_id)
            .unwrap_err();

        // Should be able to write to the new version.
        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, new_version_id)
            .unwrap();
        let mut write_handle = table_dml_handle
            .write_handle(TEST_SESSION_ID, TEST_TRANSACTION_ID)
            .unwrap();
        write_handle.begin().unwrap();
        write_handle.write_chunk(new_chunk()).await.unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_schema() {
        let dml_manager = DmlManager::for_test();
        let table_id = TableId::new(1);
        let table_version_id = INITIAL_TABLE_VERSION_ID;

        let column_descs = vec![ColumnDesc::unnamed(100.into(), DataType::Float64)];
        let other_column_descs = vec![ColumnDesc::unnamed(101.into(), DataType::Float64)];

        let _h = dml_manager
            .register_reader(table_id, table_version_id, &column_descs)
            .unwrap();

        // Should panic as the schema is different.
        let _h = dml_manager
            .register_reader(table_id, table_version_id, &other_column_descs)
            .unwrap();
    }
}
