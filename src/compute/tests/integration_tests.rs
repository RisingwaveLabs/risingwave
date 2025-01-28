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

#![feature(coroutines)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use maplit::{btreemap, convert_args};
use risingwave_batch::error::BatchError;
use risingwave_batch_executors::{
    BoxedDataChunkStream, BoxedExecutor, DeleteExecutor, Executor as BatchExecutor, InsertExecutor,
    RowSeqScanExecutor, ScanRange,
};
use risingwave_common::array::{Array, DataChunk, F64Array, SerialArray};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId, INITIAL_TABLE_VERSION_ID,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::test_prelude::DataChunkTestExt;
use risingwave_common::types::{DataType, IntoOrdered};
use risingwave_common::util::epoch::{test_epoch, EpochExt, EpochPair};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_common_rate_limit::RateLimit;
use risingwave_connector::source::reader::desc::test_utils::create_source_desc_builder;
use risingwave_dml::dml_manager::DmlManager;
use risingwave_hummock_sdk::test_batch_query_epoch;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::PbRowFormatType;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::panic_store::PanicStateStore;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::dml::DmlExecutor;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::row_id_gen::RowIdGenExecutor;
use risingwave_stream::executor::source::SourceExecutor;
use risingwave_stream::executor::{
    ActorContext, Barrier, Execute, Executor, ExecutorInfo, MaterializeExecutor, Message, PkIndices,
};
use tokio::sync::mpsc::unbounded_channel;

struct SingleChunkExecutor {
    chunk: Option<DataChunk>,
    schema: Schema,
    identity: String,
}

impl SingleChunkExecutor {
    pub fn new(chunk: DataChunk, schema: Schema) -> Self {
        Self {
            chunk: Some(chunk),
            schema,
            identity: "SingleChunkExecutor".to_owned(),
        }
    }
}

impl BatchExecutor for SingleChunkExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl SingleChunkExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        yield self.chunk.unwrap()
    }
}

/// This test checks whether batch task and streaming task work together for `Table` creation,
/// insertion, deletion, and materialization.
#[tokio::test]
async fn test_table_materialize() -> StreamResult<()> {
    use risingwave_common::types::DataType;

    let memory_state_store = MemoryStateStore::new();
    let dml_manager = Arc::new(DmlManager::for_test());
    let table_id = TableId::default();
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Serial),
            Field::unnamed(DataType::Float64),
        ],
    };
    let source_info = StreamSourceInfo {
        row_format: PbRowFormatType::Json as i32,
        ..Default::default()
    };
    let properties = convert_args!(btreemap!(
        "connector" => "datagen",
        "fields.v1.min" => "1",
        "fields.v1.max" => "1000",
        "fields.v1.seed" => "12345",
    ));
    let row_id_index: usize = 0;
    let source_builder = create_source_desc_builder(
        &schema,
        Some(row_id_index),
        source_info,
        properties,
        vec![row_id_index],
    );

    // Ensure the source exists.
    let source_desc = source_builder.build().unwrap();
    let get_schema = |column_ids: &[ColumnId]| {
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::unnamed(column_desc.data_type.clone()));
        }
        Schema::new(fields)
    };

    let all_column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
    let all_schema = get_schema(&all_column_ids);
    let pk_indices = PkIndices::from([0]);
    let column_descs = all_column_ids
        .iter()
        .zip_eq_fast(all_schema.fields.iter().cloned())
        .map(|(column_id, field)| ColumnDesc::named(field.name, *column_id, field.data_type))
        .collect_vec();
    let (barrier_tx, barrier_rx) = unbounded_channel();
    let barrier_tx = Arc::new(barrier_tx);
    let vnodes = Bitmap::from_bytes(&[0b11111111]);

    let actor_ctx = ActorContext::for_test(0x3f3f3f);
    let system_params_manager = LocalSystemParamsManager::for_test();

    // Create a `SourceExecutor` to read the changes.
    let source_executor = Executor::new(
        ExecutorInfo {
            schema: all_schema.clone(),
            pk_indices: pk_indices.clone(),
            identity: format!("SourceExecutor {:X}", 1),
        },
        SourceExecutor::<PanicStateStore>::new(
            actor_ctx.clone(),
            None, // There is no external stream source.
            Arc::new(StreamingMetrics::unused()),
            barrier_rx,
            system_params_manager.get_params(),
            None,
            false,
        )
        .boxed(),
    );

    // Create a `DmlExecutor` to accept data change from users.
    let dml_executor = Executor::new(
        ExecutorInfo {
            schema: all_schema.clone(),
            pk_indices: pk_indices.clone(),
            identity: format!("DmlExecutor {:X}", 2),
        },
        DmlExecutor::new(
            ActorContext::for_test(0),
            source_executor,
            dml_manager.clone(),
            table_id,
            INITIAL_TABLE_VERSION_ID,
            column_descs.clone(),
            1024,
            RateLimit::Disabled,
        )
        .boxed(),
    );

    let row_id_gen_executor = Executor::new(
        ExecutorInfo {
            schema: all_schema.clone(),
            pk_indices: pk_indices.clone(),
            identity: format!("RowIdGenExecutor {:X}", 3),
        },
        RowIdGenExecutor::new(actor_ctx, dml_executor, row_id_index, vnodes).boxed(),
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let mut materialize = MaterializeExecutor::for_test(
        row_id_gen_executor,
        memory_state_store.clone(),
        table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        all_column_ids.clone(),
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::NoCheck,
    )
    .await
    .boxed()
    .execute();

    // 1.
    // Test insertion
    //

    // Add some data using `InsertExecutor`. Assume we are inserting into the "mv".
    let chunk = DataChunk::from_pretty(
        "F
         1.14
         5.14",
    );
    let insert_inner: BoxedExecutor = Box::new(SingleChunkExecutor::new(
        chunk,
        get_schema(&[ColumnId::from(1)]),
    ));
    let insert = Box::new(InsertExecutor::new(
        table_id,
        INITIAL_TABLE_VERSION_ID,
        dml_manager.clone(),
        insert_inner,
        1024,
        "InsertExecutor".to_owned(),
        vec![0], // ignore insertion order
        vec![],
        Some(row_id_index),
        false,
        0,
    ));

    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = BatchTable::for_test(
        memory_state_store.clone(),
        table_id,
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        value_indices,
    );

    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1024,
        "RowSeqExecutor2".to_owned(),
        None,
        None,
        None,
    ));
    let mut stream = scan.execute();
    let result = stream.next().await;
    assert!(result.is_none());

    // Send a barrier to start materialized view.
    let mut curr_epoch = test_epoch(1919);
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch))
        .unwrap();

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    curr_epoch.inc_epoch();
    let barrier_tx_clone = barrier_tx.clone();
    tokio::spawn(async move {
        let mut stream = insert.execute();
        let _ = stream.next().await.unwrap()?;
        // Send a barrier and poll again, should write changes to storage.
        barrier_tx_clone
            .send(Barrier::new_test_barrier(curr_epoch))
            .unwrap();
        anyhow::Ok(())
    });

    // Poll `Materialize`, should output the same insertion stream chunk.
    let message = materialize.next().await.unwrap()?;
    let mut col_row_ids = vec![];
    match message {
        Message::Watermark(_) => {
            // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
        }
        Message::Chunk(c) => {
            let col_row_id = c.columns()[0].as_serial();
            col_row_ids.push(col_row_id.value_at(0).unwrap());
            col_row_ids.push(col_row_id.value_at(1).unwrap());

            let col_data = c.columns()[1].as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
            assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());
        }
        Message::Barrier(_) => panic!(),
    }

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    // Scan the table again, we are able to get the data now!
    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1024,
        "RowSeqScanExecutor2".to_owned(),
        None,
        None,
        None,
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();

    let col_data = result.columns()[1].as_float64();
    assert_eq!(col_data.len(), 2);
    assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
    assert_eq!(col_data.value_at(1).unwrap(), 5.14.into_ordered());

    // 2.
    // Test deletion
    //

    // Delete some data using `DeleteExecutor`, assuming we are inserting into the "mv".
    let columns = vec![
        SerialArray::from_iter([col_row_ids[0]]).into_ref(), // row id column
        F64Array::from_iter([1.14]).into_ref(),
    ];
    let chunk = DataChunk::new(columns.clone(), 1);
    let delete_inner: BoxedExecutor = Box::new(SingleChunkExecutor::new(chunk, all_schema.clone()));
    let delete = Box::new(DeleteExecutor::new(
        table_id,
        INITIAL_TABLE_VERSION_ID,
        dml_manager.clone(),
        delete_inner,
        1024,
        "DeleteExecutor".to_owned(),
        false,
        0,
    ));

    curr_epoch.inc_epoch();
    let barrier_tx_clone = barrier_tx.clone();
    tokio::spawn(async move {
        let mut stream = delete.execute();
        let _ = stream.next().await.unwrap()?;
        // Send a barrier and poll again, should write changes to storage.
        barrier_tx_clone
            .send(Barrier::new_test_barrier(curr_epoch))
            .unwrap();
        anyhow::Ok(())
    });

    // Poll `Materialize`, should output the same deletion stream chunk.
    let message = materialize.next().await.unwrap()?;
    match message {
        Message::Watermark(_) => {
            // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
        }
        Message::Chunk(c) => {
            let col_row_id = c.columns()[0].as_serial();
            assert_eq!(col_row_id.value_at(0).unwrap(), col_row_ids[0]);

            let col_data = c.columns()[1].as_float64();
            assert_eq!(col_data.value_at(0).unwrap(), 1.14.into_ordered());
        }
        Message::Barrier(_) => panic!(),
    }

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    // Scan the table again, we are able to see the deletion now!
    let scan = Box::new(RowSeqScanExecutor::new(
        table,
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1024,
        "RowSeqScanExecutor2".to_owned(),
        None,
        None,
        None,
    ));

    let mut stream = scan.execute();
    let result = stream.next().await.unwrap().unwrap();
    let col_data = result.columns()[1].as_float64();
    assert_eq!(col_data.len(), 1);
    assert_eq!(col_data.value_at(0).unwrap(), 5.14.into_ordered());

    Ok(())
}

#[tokio::test]
async fn test_row_seq_scan() -> StreamResult<()> {
    // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
    let memory_state_store = MemoryStateStore::new();

    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int32), // pk
        Field::unnamed(DataType::Int32),
        Field::unnamed(DataType::Int64),
    ]);
    let _column_ids = [ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), schema[2].data_type.clone()),
    ];

    let mut state = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::from(0x42),
            column_descs.clone(),
            vec![OrderType::ascending()],
            vec![0],
            0,
        ),
        memory_state_store.clone(),
        None,
    )
    .await;
    let table = BatchTable::for_test(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        vec![0, 1, 2],
    );

    let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
    state.init_epoch(epoch).await?;
    state.insert(OwnedRow::new(vec![
        Some(1_i32.into()),
        Some(4_i32.into()),
        Some(7_i64.into()),
    ]));
    state.insert(OwnedRow::new(vec![
        Some(2_i32.into()),
        Some(5_i32.into()),
        Some(8_i64.into()),
    ]));

    epoch.inc_for_test();
    state.commit(epoch).await.unwrap();

    let executor = Box::new(RowSeqScanExecutor::new(
        table,
        vec![ScanRange::full()],
        true,
        test_batch_query_epoch(),
        1,
        "RowSeqScanExecutor2".to_owned(),
        None,
        None,
        None,
    ));

    assert_eq!(executor.schema().fields().len(), 3);

    let mut stream = executor.execute();
    let res_chunk = stream.next().await.unwrap().unwrap();

    assert_eq!(res_chunk.dimension(), 3);
    assert_eq!(
        res_chunk.column_at(0).as_int32().iter().collect::<Vec<_>>(),
        vec![Some(1)]
    );
    assert_eq!(
        res_chunk.column_at(1).as_int32().iter().collect::<Vec<_>>(),
        vec![Some(4)]
    );

    let res_chunk2 = stream.next().await.unwrap().unwrap();
    assert_eq!(res_chunk2.dimension(), 3);
    assert_eq!(
        res_chunk2
            .column_at(0)
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2)]
    );
    assert_eq!(
        res_chunk2
            .column_at(1)
            .as_int32()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(5)]
    );
    Ok(())
}
