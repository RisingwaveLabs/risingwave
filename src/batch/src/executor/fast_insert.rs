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

use std::iter::repeat;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op, SerialArray, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId, TableVersionId};
use risingwave_common::transaction::transaction_id::TxnId;
use risingwave_common::types::DataType;
use risingwave_common::util::epoch::Epoch;
use risingwave_dml::dml_manager::DmlManagerRef;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::FastInsertNode;
use risingwave_pb::plan_common::IndexAndExpr;

use crate::error::Result;

pub struct FastInsertExecutor {
    /// Target table id.
    table_id: TableId,
    table_version_id: TableVersionId,
    dml_manager: DmlManagerRef,
    // TODO(Kexiang): get rid of it?
    input_schema: Schema,
    column_indices: Vec<usize>,

    // TODO(Kexiang): get rid of it?
    sorted_default_columns: Vec<(usize, BoxedExpression)>,

    row_id_index: Option<usize>,
    txn_id: TxnId,
    session_id: u32,
}

impl FastInsertExecutor {
    pub fn build(
        dml_manager: DmlManagerRef,
        insert_node: FastInsertNode,
    ) -> Result<(FastInsertExecutor, DataChunk)> {
        let table_id = TableId::new(insert_node.table_id);
        let column_indices = insert_node
            .column_indices
            .iter()
            .map(|&i| i as usize)
            .collect();
        let sorted_default_columns = if let Some(default_columns) = &insert_node.default_columns {
            let mut default_columns = default_columns
                .get_default_columns()
                .iter()
                .cloned()
                .map(|IndexAndExpr { index: i, expr: e }| {
                    Ok((
                        i as usize,
                        build_from_prost(&e.context("expression is None")?)
                            .context("failed to build expression")?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            default_columns.sort_unstable_by_key(|(i, _)| *i);
            default_columns
        } else {
            vec![]
        };
        let mut schema = Schema::new(vec![Field::unnamed(DataType::Jsonb)]);
        schema.fields.push(Field::unnamed(DataType::Serial)); // row_id column
        let data_chunk_pb = insert_node
            .data_chunk
            .expect("no data_chunk found in fast insert node");

        Ok((
            FastInsertExecutor::new(
                table_id,
                insert_node.table_version_id,
                dml_manager,
                schema,
                column_indices,
                sorted_default_columns,
                insert_node.row_id_index.as_ref().map(|index| *index as _),
                insert_node.session_id,
            ),
            DataChunk::from_protobuf(&data_chunk_pb)?,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        table_version_id: TableVersionId,
        dml_manager: DmlManagerRef,
        input_schema: Schema,
        column_indices: Vec<usize>,
        sorted_default_columns: Vec<(usize, BoxedExpression)>,
        row_id_index: Option<usize>,
        session_id: u32,
    ) -> Self {
        let txn_id = dml_manager.gen_txn_id();
        Self {
            table_id,
            table_version_id,
            dml_manager,
            input_schema,
            column_indices,
            sorted_default_columns,
            row_id_index,
            txn_id,
            session_id,
        }
    }
}

impl FastInsertExecutor {
    pub async fn do_execute(self, data_chunk_to_insert: DataChunk) -> Result<Epoch> {
        let table_dml_handle = self
            .dml_manager
            .table_dml_handle(self.table_id, self.table_version_id)?;
        let mut write_handle = table_dml_handle.write_handle(self.session_id, self.txn_id)?;

        write_handle.begin()?;

        // Transform the data chunk to a stream chunk, then write to the source.
        // Return the returning chunk.
        let write_txn_data = |chunk: DataChunk| async {
            let cap = chunk.capacity();
            let (mut columns, vis) = chunk.into_parts();

            let mut ordered_columns = self
                .column_indices
                .iter()
                .enumerate()
                .map(|(i, idx)| (*idx, columns[i].clone()))
                .collect_vec();
            ordered_columns.reserve(ordered_columns.len() + self.sorted_default_columns.len());

            ordered_columns.sort_unstable_by_key(|(idx, _)| *idx);
            columns = ordered_columns
                .into_iter()
                .map(|(_, column)| column)
                .collect_vec();

            // If the user does not specify the primary key, then we need to add a column as the
            // primary key.
            if let Some(row_id_index) = self.row_id_index {
                let row_id_col = SerialArray::from_iter(repeat(None).take(cap));
                columns.insert(row_id_index, Arc::new(row_id_col.into()))
            }

            let stream_chunk = StreamChunk::with_visibility(vec![Op::Insert; cap], columns, vis);

            #[cfg(debug_assertions)]
            table_dml_handle.check_chunk_schema(&stream_chunk);

            write_handle.write_chunk(stream_chunk).await?;

            Result::Ok(())
        };
        write_txn_data(data_chunk_to_insert).await?;
        let epoch = write_handle.end_returning_epoch().await?;
        return Ok(epoch);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Bound;

    use assert_matches::assert_matches;
    use foyer::CacheHint;
    use futures::StreamExt;
    use risingwave_common::array::{Array, JsonbArrayBuilder};
    use risingwave_common::catalog::{ColumnDesc, ColumnId, INITIAL_TABLE_VERSION_ID};
    use risingwave_common::transaction::transaction_message::TxnMsg;
    use risingwave_common::types::JsonbVal;
    use risingwave_dml::dml_manager::DmlManager;
    use risingwave_storage::hummock::CachePolicy;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::store::{ReadOptions, StateStoreReadExt};
    use serde_json::json;

    use super::*;
    use crate::risingwave_common::types::Scalar;
    use crate::*;

    #[tokio::test]
    async fn test_insert_executor() -> Result<()> {
        let dml_manager = Arc::new(DmlManager::for_test());
        let store = MemoryStateStore::new();
        // Schema of the table
        let mut schema = Schema::new(vec![Field::unnamed(DataType::Jsonb)]);
        schema.fields.push(Field::unnamed(DataType::Serial)); // row_id column

        let row_id_index = Some(1);

        let mut builder = JsonbArrayBuilder::with_type(1, DataType::Jsonb);

        let mut header_map = HashMap::new();
        header_map.insert("data".to_string(), "value1".to_string());

        let json_value = json!(header_map);
        let jsonb_val = JsonbVal::from(json_value);
        // Add 4 ListValues to ArrayBuilder
        builder.append(Some(jsonb_val.as_scalar_ref()));

        // Use builder to obtain a single (List) column DataChunk
        let data_chunk = DataChunk::new(vec![builder.finish().into_ref()], 1);

        // Create the table.
        let table_id = TableId::new(0);

        // Create reader
        let column_descs = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| ColumnDesc::unnamed(ColumnId::new(i as _), field.data_type.clone()))
            .collect_vec();
        // We must create a variable to hold this `Arc<TableDmlHandle>` here, or it will be dropped
        // due to the `Weak` reference in `DmlManager`.
        let reader = dml_manager
            .register_reader(table_id, INITIAL_TABLE_VERSION_ID, &column_descs)
            .unwrap();
        let mut reader = reader.stream_reader().into_stream();

        // Insert
        let insert_executor = Box::new(FastInsertExecutor::new(
            table_id,
            INITIAL_TABLE_VERSION_ID,
            dml_manager,
            schema,
            vec![0], // Ignoring insertion order
            vec![],
            row_id_index,
            0,
        ));
        let handle = tokio::spawn(async move {
            insert_executor.do_execute(data_chunk).await.unwrap();
        });

        // Read
        assert_matches!(reader.next().await.unwrap()?, (TxnMsg::Begin(_), _));

        assert_matches!(reader.next().await.unwrap()?, (TxnMsg::Data(_, chunk),_) => {
            // assert_eq!(
            //     chunk.columns()[0].as_int32().iter().collect::<Vec<_>>(),
            //     vec![Some(1), Some(3), Some(5), Some(7), Some(9)]
            // );

            assert_eq!(chunk.columns().len(),2);
            let array = chunk.columns()[0].as_jsonb().iter().collect::<Vec<_>>();
            println!("WKXLOG arrar: {:?}",array);
            // assert_eq!(*array,2);
            // assert_eq!(*chunk.columns()[2], array);
        });

        assert_matches!(reader.next().await.unwrap()?, (TxnMsg::End(_), _));
        let epoch = u64::MAX;
        let full_range = (Bound::Unbounded, Bound::Unbounded);
        let store_content = store
            .scan(
                full_range,
                epoch,
                None,
                ReadOptions {
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await?;
        assert!(store_content.is_empty());

        handle.await.unwrap();

        Ok(())
    }
}
