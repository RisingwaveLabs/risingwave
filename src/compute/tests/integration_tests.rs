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

#![feature(generators)]
#![feature(proc_macro_hygiene, stmt_expr_attributes)]
#![feature(let_chains)]

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use risingwave_batch::executor::{
    BoxedDataChunkStream, BoxedExecutor, DeleteExecutor, Executor as BatchExecutor, InsertExecutor,
    RowSeqScanExecutor, ScanRange,
};
use risingwave_common::array::stream_chunk::{Offset, StreamChunkMeta};
use risingwave_common::array::{
    Array, DataChunk, F64Array, SerialArray, StreamChunk, StreamChunkTestExt,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{
    ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId, INITIAL_TABLE_VERSION_ID,
};
use risingwave_common::error::{Result, RwError};
use risingwave_common::row::OwnedRow;
use risingwave_common::system_param::local_manager::LocalSystemParamsManager;
use risingwave_common::test_prelude::DataChunkTestExt;
use risingwave_common::types::{DataType, IntoOrdered};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::external::{
    DebeziumOffset, DebeziumSourceOffset, ExternalTableReaderImpl, MySqlOffset, SchemaTableName,
};
use risingwave_connector::source::{MockExternalTableReader, SourceCtrlOpts};
use risingwave_connector::ConnectorParams;
use risingwave_hummock_sdk::to_committed_batch_query_epoch;
use risingwave_pb::catalog::StreamSourceInfo;
use risingwave_pb::plan_common::PbRowFormatType;
use risingwave_source::connector_test_utils::create_source_desc_builder;
use risingwave_source::dml_manager::DmlManager;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::panic_store::PanicStateStore;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::dml::DmlExecutor;
use risingwave_stream::executor::external::ExternalStorageTable;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::row_id_gen::RowIdGenExecutor;
use risingwave_stream::executor::source_executor::SourceExecutor;
use risingwave_stream::executor::test_utils::MockSource;
use risingwave_stream::executor::{
    expect_first_barrier, ActorContext, Barrier, BoxedExecutor as StreamBoxedExecutor,
    BoxedMessageStream, CdcBackfillExecutor, Executor, MaterializeExecutor, Message, Mutation,
    PkIndices, PkIndicesRef, StreamExecutorError,
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
            identity: "SingleChunkExecutor".to_string(),
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
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        yield self.chunk.unwrap()
    }
}

// mock upstream binlog offset starting from "1.binlog, pos=0"
pub struct MockOffsetGenExecutor {
    upstream: Option<StreamBoxedExecutor>,

    schema: Schema,

    pk_indices: PkIndices,

    identity: String,

    start_offset: u32,
}

impl MockOffsetGenExecutor {
    pub fn new(upstream: StreamBoxedExecutor, schema: Schema, pk_indices: PkIndices) -> Self {
        Self {
            upstream: Some(upstream),
            schema,
            pk_indices,
            identity: "MockOffsetGenExecutor".to_string(),
            start_offset: 0,
        }
    }

    fn next_offset(&mut self) -> anyhow::Result<String> {
        let start_offset = self.start_offset;
        let dbz_offset = DebeziumOffset {
            source_partition: Default::default(),
            source_offset: DebeziumSourceOffset {
                last_snapshot_record: None,
                snapshot: None,
                file: Some("1.binlog".to_string()),
                pos: Some(start_offset as _),
                lsn: None,
                txid: None,
                tx_usec: None,
            },
        };

        self.start_offset += 1;
        let out = serde_json::to_string(&dbz_offset)?;
        Ok(out)
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // fake offset in stream chunk
        let mut upstream = self.upstream.take().unwrap().execute();

        // The first barrier mush propagated.
        let barrier = expect_first_barrier(&mut upstream).await?;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Chunk(chunk) => {
                    let mut offsets = vec![];
                    let (ops, columns, vis) = chunk.into_inner();
                    assert!(vis.as_visibility().is_none());

                    for _ in 0..ops.len() {
                        offsets.push(Offset::new(self.next_offset()?));
                    }
                    yield Message::Chunk(StreamChunk::new_with_meta(
                        ops,
                        columns,
                        vis.into_visibility(),
                        Some(StreamChunkMeta { offsets }),
                    ));
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
            }
        }
    }
}

impl Executor for MockOffsetGenExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[tokio::test]
async fn test_cdc_backfill() -> StreamResult<()> {
    use risingwave_common::types::DataType;
    let memory_state_store = MemoryStateStore::new();

    let table_id = TableId::new(1002);
    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int64), // primary key
        Field::unnamed(DataType::Float64),
    ]);
    let column_ids = vec![0.into(), 1.into()];

    let pk_indices = vec![0];

    let (mut tx, source) = MockSource::channel(schema.clone(), pk_indices.clone());
    let _actor_ctx = ActorContext::create(0x3a3a3a);

    // mock upstream offset (start from "1.binlog, pos=0") for ingested chunks
    let mock_offset_executor =
        MockOffsetGenExecutor::new(Box::new(source), schema.clone(), pk_indices.clone());

    let binlog_file = String::from("1.binlog");

    // mock binlog watermarks for backfill
    // initial low watermark: 1.binlog, pos=2 and expected behaviors:
    // - ignore events before (1.binlog, pos=2);
    // - apply events in the range of (1.binlog, pos=2, 1.binlog, pos=4) to the snapshot
    let binlog_watermarks = vec![
        MySqlOffset::new(binlog_file.clone(), 2), // binlog low watermark
        MySqlOffset::new(binlog_file.clone(), 4),
        MySqlOffset::new(binlog_file.clone(), 6),
    ];

    let table_name = SchemaTableName::new("mock_table".to_string(), "public".to_string());
    let external_table = ExternalStorageTable::new(
        table_id,
        table_name,
        ExternalTableReaderImpl::Mock(MockExternalTableReader::new(binlog_watermarks)),
        schema.clone(),
        vec![OrderType::ascending()],
        pk_indices,
        vec![0, 1],
    );

    let cdc_backfill = CdcBackfillExecutor::new(
        ActorContext::create(0x1a),
        external_table,
        Box::new(mock_offset_executor),
        vec![0, 1],
        None,
        schema.clone(),
        vec![0],
        Arc::new(StreamingMetrics::unused()),
        4, // 4 rows in a snapshot chunk
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let mut materialize = MaterializeExecutor::for_test(
        Box::new(cdc_backfill),
        memory_state_store.clone(),
        table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        column_ids.clone(),
        4,
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();

    // push upstream chunks
    let stream_chunk1 = StreamChunk::from_pretty(
        " I F
            + 1 10.01
            + 2 2.02
            + 3 3.03 // binlog pos=2
            + 4 4.04
            + 5 5.05 // binlog pos=4
            + 6 6.06",
    );
    let stream_chunk2 = StreamChunk::from_pretty(
        " I F
            + 6 10.08
            + 199 40.5
            + 978 72.6
            + 134 41.7",
    );

    // The first barrier
    let curr_epoch = 11;
    tx.push_barrier(curr_epoch, false);

    // assert barrier is forwarded to mview
    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch
    ));

    // start the stream pipeline src -> backfill -> mview
    let mview_handle = tokio::spawn(consume_message_stream(materialize));

    // ingest data and barrier
    let interval = Duration::from_millis(10);
    tx.push_chunk(stream_chunk1);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 1, false);

    tx.push_chunk(stream_chunk2);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 2, false);

    tokio::time::sleep(interval).await;
    tx.push_barrier(curr_epoch + 3, true);

    // scan the final result of the mv table
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = StorageTable::for_test(
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
        to_committed_batch_query_epoch(u64::MAX),
        1024,
        "RowSeqExecutor2".to_string(),
        None,
    ));
    let mut stream = scan.execute();
    while let Some(message) = stream.next().await {
        println!("[scan] chunk: {:#?}", message.unwrap());
    }

    mview_handle.await.unwrap()?;

    Ok(())
}

async fn consume_message_stream(mut stream: BoxedMessageStream) -> StreamResult<()> {
    while let Some(message) = stream.next().await {
        let message = message?;
        match message {
                Message::Watermark(_) => {
                    break;
                }
                Message::Chunk(c) => {
                    println!("[mv] chunk: {:#?}", c);
                }
                Message::Barrier(b) => {
                    if let Some(m) = b.mutation && matches!(*m, Mutation::Stop(_)) {
                        println!("encounter stop barrier");
                        break
                    }
                }
            }
    }
    Ok(())
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
    let properties = convert_args!(hashmap!(
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
        .map(|(column_id, field)| ColumnDesc {
            data_type: field.data_type,
            column_id: *column_id,
            name: field.name,
            field_descs: vec![],
            type_name: "".to_string(),
            generated_or_default_column: None,
        })
        .collect_vec();
    let (barrier_tx, barrier_rx) = unbounded_channel();
    let vnodes = Bitmap::from_bytes(&[0b11111111]);

    let actor_ctx = ActorContext::create(0x3f3f3f);
    let system_params_manager = LocalSystemParamsManager::for_test();

    // Create a `SourceExecutor` to read the changes.
    let source_executor = SourceExecutor::<PanicStateStore>::new(
        actor_ctx.clone(),
        all_schema.clone(),
        pk_indices.clone(),
        None, // There is no external stream source.
        Arc::new(StreamingMetrics::unused()),
        barrier_rx,
        system_params_manager.get_params(),
        1,
        SourceCtrlOpts::default(),
        ConnectorParams::default(),
    );

    // Create a `DmlExecutor` to accept data change from users.
    let dml_executor = DmlExecutor::new(
        Box::new(source_executor),
        all_schema.clone(),
        pk_indices.clone(),
        2,
        dml_manager.clone(),
        table_id,
        INITIAL_TABLE_VERSION_ID,
        column_descs.clone(),
    );

    let row_id_gen_executor = RowIdGenExecutor::new(
        actor_ctx,
        Box::new(dml_executor),
        all_schema.clone(),
        pk_indices.clone(),
        3,
        row_id_index,
        vnodes,
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let mut materialize = MaterializeExecutor::for_test(
        Box::new(row_id_gen_executor),
        memory_state_store.clone(),
        table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        all_column_ids.clone(),
        4,
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
        "InsertExecutor".to_string(),
        vec![0], // ignore insertion order
        vec![],
        Some(row_id_index),
        false,
    ));

    tokio::spawn(async move {
        let mut stream = insert.execute();
        let _ = stream.next().await.unwrap()?;
        Ok::<_, RwError>(())
    });

    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = StorageTable::for_test(
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
        to_committed_batch_query_epoch(u64::MAX),
        1024,
        "RowSeqExecutor2".to_string(),
        None,
    ));
    let mut stream = scan.execute();
    let result = stream.next().await;
    assert!(result.is_none());

    // Send a barrier to start materialized view.
    let curr_epoch = 1919;
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

    // Send a barrier and poll again, should write changes to storage.
    let curr_epoch = 1920;
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

    // Scan the table again, we are able to get the data now!
    let scan = Box::new(RowSeqScanExecutor::new(
        table.clone(),
        vec![ScanRange::full()],
        true,
        to_committed_batch_query_epoch(u64::MAX),
        1024,
        "RowSeqScanExecutor2".to_string(),
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
        "DeleteExecutor".to_string(),
        false,
    ));

    tokio::spawn(async move {
        let mut stream = delete.execute();
        let _ = stream.next().await.unwrap()?;
        Ok::<_, RwError>(())
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

    // Send a barrier and poll again, should write changes to storage.
    barrier_tx
        .send(Barrier::new_test_barrier(curr_epoch + 1))
        .unwrap();

    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: None,
            ..
        }) if epoch.curr == curr_epoch + 1
    ));

    // Scan the table again, we are able to see the deletion now!
    let scan = Box::new(RowSeqScanExecutor::new(
        table,
        vec![ScanRange::full()],
        true,
        to_committed_batch_query_epoch(u64::MAX),
        1024,
        "RowSeqScanExecutor2".to_string(),
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
async fn test_row_seq_scan() -> Result<()> {
    // In this test we test if the memtable can be correctly scanned for K-V pair insertions.
    let memory_state_store = MemoryStateStore::new();

    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int32), // pk
        Field::unnamed(DataType::Int32),
        Field::unnamed(DataType::Int64),
    ]);
    let _column_ids = vec![ColumnId::from(0), ColumnId::from(1), ColumnId::from(2)];

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), schema[2].data_type.clone()),
    ];

    let mut state = StateTable::new_without_distribution(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0_usize],
    )
    .await;
    let table = StorageTable::for_test(
        memory_state_store.clone(),
        TableId::from(0x42),
        column_descs.clone(),
        vec![OrderType::ascending()],
        vec![0],
        vec![0, 1, 2],
    );

    let mut epoch = EpochPair::new_test_epoch(1);
    state.init_epoch(epoch);
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

    epoch.inc();
    state.commit(epoch).await.unwrap();

    let executor = Box::new(RowSeqScanExecutor::new(
        table,
        vec![ScanRange::full()],
        true,
        to_committed_batch_query_epoch(u64::MAX),
        1,
        "RowSeqScanExecutor2".to_string(),
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
