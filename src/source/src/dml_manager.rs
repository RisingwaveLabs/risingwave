// Copyright 2023 RisingWave Labs
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

use anyhow::Context;
use parking_lot::RwLock;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, TableId, TableVersionId};
use risingwave_common::error::Result;
use risingwave_common::hash::ActorId;
use risingwave_common::transaction::transaction_id::{TxnId, TxnIdGenerator};
use risingwave_common::util::worker_util::WorkerNodeId;

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
    pub table_readers: RwLock<HashMap<TableId, TableReader>>,
    txn_id_generator: TxnIdGenerator,
}

impl DmlManager {
    pub fn new(worker_node_id: WorkerNodeId) -> Self {
        Self {
            table_readers: RwLock::new(HashMap::new()),
            txn_id_generator: TxnIdGenerator::new(worker_node_id),
        }
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
        table_readers.drain_filter(|_, r| r.handle.strong_count() == 0);

        macro_rules! new_handle {
            ($entry:ident) => {{
                let handle = Arc::new(TableDmlHandle::new(column_descs.to_vec()));
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
                        .with_context(|| {
                            format!("fail to register reader for table with key `{table_id:?}`")
                        })?,

                    // A new version of the table is activated, overwrite the old reader.
                    Ordering::Greater => new_handle!(o),
                }
            }
        };

        Ok(handle)
    }

    pub fn unregister_changes_sender(&self, table_id: TableId, actor_id: ActorId) {
        let table_readers = self.table_readers.write();
        let table_reader = table_readers.get(&table_id).unwrap();
        let table_dml_handle = table_reader
            .handle
            .upgrade()
            .expect("should be able to upgrade");
        let mut guard = table_dml_handle.core.write();
        guard
            .changes_txs
            .retain(|sender| sender.actor_id != actor_id);
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
                            bail!("schema changed for table `{table_id:?}`, please retry later")
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
        .with_context(|| format!("no reader for dml in table `{table_id:?}`"))?;

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
    use futures::FutureExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::INITIAL_TABLE_VERSION_ID;
    use risingwave_common::hash::ActorId;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::transaction::transaction_id::TxnId;
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::types::DataType;
    use tokio::sync::oneshot;

    use super::*;
    use crate::WriteHandle;

    const TEST_TRANSACTION_ID: TxnId = 0;
    const ACTOR_ID1: ActorId = 1;
    const ACTOR_ID2: ActorId = 2;

    #[easy_ext::ext(WriteHandleTestExt)]
    impl WriteHandle {
        pub fn write_chunk_ready(&self, chunk: StreamChunk) -> Result<oneshot::Receiver<usize>> {
            self.write_txn_msg(TxnMsg::Begin(TEST_TRANSACTION_ID))
                .now_or_never()
                .unwrap()?;
            let result = self
                .write_txn_msg(TxnMsg::Data(TEST_TRANSACTION_ID, chunk))
                .now_or_never()
                .unwrap();
            self.write_txn_msg(TxnMsg::End(TEST_TRANSACTION_ID))
                .now_or_never()
                .unwrap()?;
            result
        }
    }

    #[test]
    fn test_register_and_drop() {
        let dml_manager = DmlManager::new(WorkerNodeId::default());
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
        let r1 = h1.stream_reader(ACTOR_ID1);
        let r2 = h2.stream_reader(ACTOR_ID2);

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, table_version_id)
            .unwrap();
        let write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();

        // Should be able to write to the table.
        write_handle.write_chunk_ready(chunk()).unwrap();

        // After dropping the corresponding reader, the write handle should be not allowed to write.
        // This is to simulate the scale-in of DML executors.
        {
            dml_manager.unregister_changes_sender(table_id, ACTOR_ID1);
            drop(r1);
        }

        write_handle.write_chunk_ready(chunk()).unwrap_err();

        // Unless we create a new write handle.
        let write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();
        write_handle.write_chunk_ready(chunk()).unwrap();

        // After dropping the last reader, no more writes are allowed.
        // This is to simulate the dropping of the table.
        {
            dml_manager.unregister_changes_sender(table_id, ACTOR_ID2);
            drop(r2);
        }
        write_handle.write_chunk_ready(chunk()).unwrap_err();
    }

    #[test]
    fn test_versioned() {
        let dml_manager = DmlManager::new(WorkerNodeId::default());
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
        let _old_r = old_h.stream_reader(ACTOR_ID1);

        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, old_version_id)
            .unwrap();
        let write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();

        // Should be able to write to the table.
        write_handle.write_chunk_ready(old_chunk()).unwrap();

        // Start reading the new version.
        let new_h = dml_manager
            .register_reader(table_id, new_version_id, &new_column_descs)
            .unwrap();
        let _new_r = new_h.stream_reader(ACTOR_ID2);

        // Still be able to write to the old write handle, if the channel is not closed.
        write_handle.write_chunk_ready(old_chunk()).unwrap();

        // However, it is no longer possible to create a `table_dml_handle` with the old version;
        dml_manager
            .table_dml_handle(table_id, old_version_id)
            .unwrap_err();

        // Should be able to write to the new version.
        let table_dml_handle = dml_manager
            .table_dml_handle(table_id, new_version_id)
            .unwrap();
        let write_handle = table_dml_handle.write_handle(TEST_TRANSACTION_ID).unwrap();
        write_handle.write_chunk_ready(new_chunk()).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_bad_schema() {
        let dml_manager = DmlManager::new(WorkerNodeId::default());
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
