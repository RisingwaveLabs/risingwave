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

#![feature(let_chains)]
#![feature(coroutines)]

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch_executors::{Executor as BatchExecutor, RowSeqScanExecutor, ScanRange};
use risingwave_common::array::{
    Array, ArrayBuilder, DataChunk, DataChunkTestExt, Op, StreamChunk, Utf8ArrayBuilder,
};
use risingwave_common::catalog::{ColumnDesc, ColumnId, ConflictBehavior, Field, Schema, TableId};
use risingwave_common::types::{Datum, JsonbVal};
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_connector::source::cdc::external::{
    CdcTableType, DebeziumOffset, DebeziumSourceOffset, ExternalTableConfig, SchemaTableName,
};
use risingwave_connector::source::cdc::DebeziumCdcSplit;
use risingwave_connector::source::SplitImpl;
use risingwave_hummock_sdk::test_batch_query_epoch;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable;
use risingwave_stream::error::StreamResult;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::test_utils::MockSource;
use risingwave_stream::executor::{
    expect_first_barrier, ActorContext, AddMutation, Barrier, BoxedMessageStream,
    CdcBackfillExecutor, CdcScanOptions, Execute, Executor as StreamExecutor, ExecutorInfo,
    ExternalStorageTable, MaterializeExecutor, Message, Mutation, StreamExecutorError,
};

// mock upstream binlog offset starting from "1.binlog, pos=0"
pub struct MockOffsetGenExecutor {
    upstream: Option<StreamExecutor>,

    start_offset: u32,
}

impl MockOffsetGenExecutor {
    pub fn new(upstream: StreamExecutor) -> Self {
        Self {
            upstream: Some(upstream),
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
                file: Some("1.binlog".to_owned()),
                pos: Some(start_offset as _),
                lsn: None,
                txid: None,
                tx_usec: None,
                change_lsn: None,
                commit_lsn: None,
            },
            is_heartbeat: false,
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
                    let mut offset_builder = Utf8ArrayBuilder::new(chunk.cardinality());
                    assert!(chunk.is_compacted());
                    let (ops, mut columns, vis) = chunk.into_inner();

                    for _ in 0..ops.len() {
                        let offset_str = self.next_offset()?;
                        offset_builder.append(Some(&offset_str));
                    }

                    let offsets = offset_builder.finish();
                    columns.push(offsets.into_ref());
                    yield Message::Chunk(StreamChunk::with_visibility(ops, columns, vis));
                }
                Message::Barrier(barrier) => {
                    yield Message::Barrier(barrier);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
            }
        }
    }
}

impl Execute for MockOffsetGenExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[tokio::test]
async fn test_cdc_backfill() -> StreamResult<()> {
    use risingwave_common::types::DataType;
    let memory_state_store = MemoryStateStore::new();

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(
        Schema::new(vec![
            Field::unnamed(DataType::Jsonb), // payload
        ]),
        vec![0],
    );

    // mock upstream offset (start from "1.binlog, pos=0") for ingested chunks
    let mock_offset_executor = StreamExecutor::new(
        ExecutorInfo {
            schema: Schema::new(vec![
                Field::unnamed(DataType::Jsonb),   // payload
                Field::unnamed(DataType::Varchar), // _rw_offset
            ]),
            pk_indices: vec![0],
            identity: "MockOffsetGenExecutor".to_owned(),
        },
        MockOffsetGenExecutor::new(source).boxed(),
    );

    let table_name = SchemaTableName {
        schema_name: "public".to_owned(),
        table_name: "mock_table".to_owned(),
    };
    let table_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "id"), // primary key
        Field::with_name(DataType::Float64, "price"),
    ]);
    let table_pk_indices = vec![0];
    let table_pk_order_types = vec![OrderType::ascending()];
    let config = ExternalTableConfig::default();

    let external_table = ExternalStorageTable::new(
        TableId::new(1234),
        table_name,
        "mydb".to_owned(),
        config,
        CdcTableType::Mock,
        table_schema.clone(),
        table_pk_order_types,
        table_pk_indices.clone(),
    );

    let actor_id = 0x1a;

    // create state table
    let state_schema = Schema::new(vec![
        Field::with_name(DataType::Varchar, "split_id"),
        Field::with_name(DataType::Int64, "id"), // pk
        Field::with_name(DataType::Boolean, "backfill_finished"),
        Field::with_name(DataType::Int64, "row_count"),
        Field::with_name(DataType::Jsonb, "cdc_offset"),
    ]);

    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), state_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), state_schema[1].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(2), state_schema[2].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(3), state_schema[3].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(4), state_schema[4].data_type.clone()),
    ];

    let state_table = StateTable::from_table_catalog(
        &gen_pbtable(
            TableId::from(0x42),
            column_descs,
            vec![OrderType::ascending()],
            vec![0],
            0,
        ),
        memory_state_store.clone(),
        None,
    )
    .await;

    let output_columns = vec![
        ColumnDesc::named("id", ColumnId::new(1), DataType::Int64), // primary key
        ColumnDesc::named("price", ColumnId::new(2), DataType::Float64),
    ];

    let cdc_backfill = StreamExecutor::new(
        ExecutorInfo {
            schema: table_schema.clone(),
            pk_indices: table_pk_indices,
            identity: "CdcBackfillExecutor".to_owned(),
        },
        CdcBackfillExecutor::new(
            ActorContext::for_test(actor_id),
            external_table,
            mock_offset_executor,
            vec![0, 1],
            output_columns,
            None,
            Arc::new(StreamingMetrics::unused()),
            state_table,
            Some(4), // limit a snapshot chunk to have <= 4 rows by rate limit
            CdcScanOptions {
                disable_backfill: false,
                snapshot_interval: 1,
                snapshot_batch_size: 4,
            },
        )
        .boxed(),
    );

    // Create a `MaterializeExecutor` to write the changes to storage.
    let materialize_table_id = TableId::new(5678);
    let mut materialize = MaterializeExecutor::for_test(
        cdc_backfill,
        memory_state_store.clone(),
        materialize_table_id,
        vec![ColumnOrder::new(0, OrderType::ascending())],
        vec![0.into(), 1.into()],
        Arc::new(AtomicU64::new(0)),
        ConflictBehavior::Overwrite,
    )
    .await
    .boxed()
    .execute();

    // construct upstream chunks
    let chunk1_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 10.01}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 2, "price": 22.22}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 3, "price": 3.03}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 4, "price": 4.04}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 5, "price": 5.05}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 6.06}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk2_payload = vec![
        r#"{ "payload": { "before": null, "after": { "id": 1, "price": 11.11}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 6, "price": 10.08}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 199, "price": 40.5}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 978, "price": 72.6}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
        r#"{ "payload": { "before": null, "after": { "id": 134, "price": 41.7}, "source": { "version": "1.9.7.Final", "connector": "mysql", "name": "RW_CDC_1002"}, "op": "r", "ts_ms": 1695277757017, "transaction": null } }"#,
    ];

    let chunk1_datums: Vec<Datum> = chunk1_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk2_datums: Vec<Datum> = chunk2_payload
        .into_iter()
        .map(|s| Some(JsonbVal::from_str(s).unwrap().into()))
        .collect_vec();

    let chunk_schema = Schema::new(vec![
        Field::unnamed(DataType::Jsonb), // payload
    ]);

    let stream_chunk1 = create_stream_chunk(chunk1_datums, &chunk_schema);
    let stream_chunk2 = create_stream_chunk(chunk2_datums, &chunk_schema);

    // The first barrier
    let mut curr_epoch = test_epoch(11);
    let mut splits = HashMap::new();
    splits.insert(
        actor_id,
        vec![SplitImpl::MysqlCdc(DebeziumCdcSplit::new(0, None, None))],
    );
    let init_barrier =
        Barrier::new_test_barrier(curr_epoch).with_mutation(Mutation::Add(AddMutation {
            adds: HashMap::new(),
            added_actors: HashSet::new(),
            splits,
            pause: false,
            subscriptions_to_add: vec![],
        }));

    tx.send_barrier(init_barrier);

    // assert barrier is forwarded to mview
    assert!(matches!(
        materialize.next().await.unwrap()?,
        Message::Barrier(Barrier {
            epoch,
            mutation: Some(_),
            ..
        }) if epoch.curr == curr_epoch
    ));

    // start the stream pipeline src -> backfill -> mview
    let mview_handle = tokio::spawn(consume_message_stream(materialize));

    // ingest data and barrier
    let interval = Duration::from_millis(10);

    // send a dummy barrier to trigger the backfill, since
    // cdc backfill will wait for a barrier before start
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    // first chunk
    tx.push_chunk(stream_chunk1);

    tokio::time::sleep(interval).await;

    // barrier to trigger emit buffered events
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    // second chunk
    tx.push_chunk(stream_chunk2);

    tokio::time::sleep(interval).await;
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, false);

    tokio::time::sleep(interval).await;
    curr_epoch.inc_epoch();
    tx.push_barrier(curr_epoch, true);

    // scan the final result of the mv table
    let column_descs = vec![
        ColumnDesc::unnamed(ColumnId::from(0), table_schema[0].data_type.clone()),
        ColumnDesc::unnamed(ColumnId::from(1), table_schema[1].data_type.clone()),
    ];
    let value_indices = (0..column_descs.len()).collect_vec();
    // Since we have not polled `Materialize`, we cannot scan anything from this table
    let table = BatchTable::for_test(
        memory_state_store.clone(),
        materialize_table_id,
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

    // check result
    let mut stream = scan.execute();
    while let Some(message) = stream.next().await {
        let chunk = message.expect("scan a chunk");
        let expect = DataChunk::from_pretty(
            "I F
                1 11.11
                2 22.22
                3 3.03
                4 4.04
                5 5.05
                6 10.08
                8 1.0008
                134 41.7
                199 40.5
                978 72.6",
        );
        assert_eq!(expect, chunk);
    }

    mview_handle.await.unwrap()?;

    Ok(())
}

fn create_stream_chunk(datums: Vec<Datum>, schema: &Schema) -> StreamChunk {
    let mut builders = schema.create_array_builders(8);
    for datum in &datums {
        builders[0].append(datum);
    }
    let columns = builders
        .into_iter()
        .map(|builder| builder.finish().into())
        .collect();

    let ops = vec![Op::Insert; datums.len()];
    StreamChunk::from_parts(ops, DataChunk::new(columns, datums.len()))
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
                if let Some(m) = b.mutation
                    && matches!(*m, Mutation::Stop(_))
                {
                    println!("encounter stop barrier");
                    break;
                }
            }
        }
    }
    Ok(())
}
