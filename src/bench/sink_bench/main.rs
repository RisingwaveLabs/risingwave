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
#![feature(coroutines)]
#![feature(proc_macro_hygiene)]
#![feature(stmt_expr_attributes)]
#![feature(let_chains)]

use core::pin::Pin;
use core::str::FromStr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use clap::Parser;
use futures::prelude::stream::PollNext;
use futures::stream::select_with_strategy;
use futures_async_stream::try_stream;
use futures::{Stream, StreamExt, TryStreamExt, FutureExt};

use risingwave_connector::dispatch_sink;
use futures::prelude::future::Either;
use risingwave_common::catalog::ColumnId;
use risingwave_common::error::anyhow_error;
use risingwave_connector::sink::catalog::{SinkType, SinkId, SinkFormatDesc};
use risingwave_connector::sink::iceberg::{IcebergSink, RemoteIcebergSink};
use risingwave_connector::sink::log_store::{TruncateOffset, LogStoreResult, LogStoreReadItem};
use risingwave_connector::sink::remote::{ElasticSearchSink, CassandraSink, JdbcSink, DeltaLakeSink, HttpJavaSink};
use risingwave_connector::{sink::{build_sink, Sink, SinkWriterParam, log_store::LogReader, SinkParam, LogSinker}, source::{datagen::{DatagenSplitEnumerator, DatagenProperties, DatagenSplitReader}, SplitEnumerator, SplitReader, Column, DataType}, parser::{ParserConfig, SpecificParserConfig, EncodingProperties, ProtocolProperties}};
use risingwave_pb::connector_service::SinkPayloadFormat;
use risingwave_stream::executor::test_utils::prelude::ColumnDesc;
use risingwave_stream::executor::{Message, StreamExecutorError, Barrier};
use serde::{Deserialize,Deserializer};
use tokio::sync::RwLock;
use tokio::time::{sleep, Instant};

const CHECKPOINT_INTERVAL: u64 = 1000;
const THROUGHPUT_METRIC_RECORD_INTERVAL: u128 = 500;
const BENCH_TIME: u64 = 20;
const BENCH_TEST: &str = "bench_test";

pub struct MockRangeLogReader{
    up_stream: Pin<Box<dyn Stream<Item = Result<Message, StreamExecutorError>> + Send>>,
    current_epoch: u64,
    chunk_id: usize,
    throughput_metric: std::sync::Arc<RwLock<ThroughputMetric>>,
}

impl LogReader for MockRangeLogReader {
    async fn init(&mut self) -> LogStoreResult<()> {
        self.throughput_metric.write().await.add_metric(0);
        Ok(())
    }

    async fn next_item(
        &mut self,
    ) -> LogStoreResult<(u64, LogStoreReadItem)>{
        match self.up_stream.next().await.unwrap().unwrap(){
            Message::Barrier(barrier) => {
                let prev_epoch = self.current_epoch;
                self.current_epoch = barrier.epoch.curr;
                return Ok((prev_epoch, LogStoreReadItem::Barrier{is_checkpoint: true}));
            },
            Message::Chunk(chunk) => {
                self.throughput_metric.write().await.add_metric(chunk.capacity());
                self.chunk_id += 1;
                return Ok((self.current_epoch, LogStoreReadItem::StreamChunk{chunk, chunk_id: self.chunk_id }));
            },
            _ => return Err(anyhow_error!("Can't assert message type".to_string())),
        }
    }

    async fn truncate(
        &mut self,
        _offset: TruncateOffset,
    ) -> LogStoreResult<()>{
        Ok(())
    }
    
}

impl MockRangeLogReader{
    fn new(mock_source:  MockDatagenSource,throughput_metric: std::sync::Arc<RwLock<ThroughputMetric>>) -> MockRangeLogReader{
        MockRangeLogReader{ up_stream: mock_source.into_stream().boxed(), current_epoch: 0 ,chunk_id: 0, throughput_metric}
    }
}

struct ThroughputMetric{
    chunk_size_list: Vec<(u64,Instant)>,
    accumulate_chunk_size: u64,
    // Record every `record_interval` ms
    record_interval: u128,
    last_record_time: Instant,
}

impl ThroughputMetric {
    pub fn new() -> Self{
        Self{
            chunk_size_list: vec![],
            accumulate_chunk_size: 0,
            record_interval: THROUGHPUT_METRIC_RECORD_INTERVAL,
            last_record_time: Instant::now(),
        }
    }

    pub fn add_metric(&mut self, chunk_size: usize){
        self.accumulate_chunk_size += chunk_size as u64;
        if Instant::now().duration_since(self.last_record_time).as_millis() > self.record_interval{
            self.chunk_size_list.push((self.accumulate_chunk_size, Instant::now()));
            self.last_record_time = Instant::now();
        }
    }

    pub fn get_throughput(&self) -> Vec<u64>{
        self.chunk_size_list.iter().zip(self.chunk_size_list.iter().skip(1))
        .map(|(current, next)| (next.0 - current.0) * 1000 /(next.1.duration_since(current.1).as_millis() as u64))
        .collect()
    }
}

pub struct MockDatagenSource{
    datagen_split_readers: RwLock<Vec<DatagenSplitReader>>,
    epoch: AtomicU64,
}
impl MockDatagenSource{
    pub async fn new(rows_per_second: u64, source_schema: Vec<Column>, split_num: String) -> MockDatagenSource{
        let properties= DatagenProperties{
            split_num: Some(split_num),
            rows_per_second,
            fields: HashMap::default(),
        };
        let mut datagen_enumerator = DatagenSplitEnumerator::new(properties.clone(), Default::default()).await.unwrap();
        let parser_config = ParserConfig {
            specific: SpecificParserConfig {
                key_encoding_config: None,
                encoding_config: EncodingProperties::Native,
                protocol_config: ProtocolProperties::Native,
            },
            ..Default::default()
        };
        let mut datagen_split_readers = vec![];
        let mut datagen_splits = datagen_enumerator.list_splits().await.unwrap();
        while let Some(splits) = datagen_splits.pop(){
            datagen_split_readers.push(DatagenSplitReader::new(properties.clone(), vec![splits], parser_config.clone(), Default::default(), Some(source_schema.clone())).await.unwrap());
        }
        MockDatagenSource{ epoch: AtomicU64::new(0), datagen_split_readers: RwLock::new(datagen_split_readers)}
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_data_stream(&self) {
        let mut readers = vec![];
        while let Some(reader) = self.datagen_split_readers.write().await.pop(){
            readers.push(reader.into_stream());
        }
        loop {
            for i in &mut readers {
                let item = i.next().await.unwrap().unwrap();
                yield Message::Chunk(item.chunk);
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream(self){
        let stream = select_with_strategy(self.barrier_to_message_stream().map_ok(Either::Left), self.into_data_stream().map_ok(Either::Right), |_: &mut PollNext| PollNext::Left);
        #[for_await]
        for message in stream{
            match message.unwrap() {
                Either::Left(Message::Barrier(barrier)) => {
                    yield Message::Barrier(barrier);
                },
                Either::Right(Message::Chunk(chunk)) => yield Message::Chunk(chunk),
                _ => return Err(StreamExecutorError::from("Can't assert message type".to_string())),
            }
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn barrier_to_message_stream(&self) {
        loop {
            let prev_epoch = self.epoch.fetch_add(1, Ordering::Relaxed);
            let barrier = Barrier::with_prev_epoch_for_test(self.epoch.load(Ordering::Relaxed),prev_epoch);
            yield Message::Barrier(barrier);
            sleep(tokio::time::Duration::from_millis(CHECKPOINT_INTERVAL)).await;
        }
    }
}

async fn consume_log_stream<S: Sink> (sink: S, log_reader: MockRangeLogReader, sink_writer_param: SinkWriterParam,connector: &str) -> Result<(),String>{
    let log_sinker = match connector {
        IcebergSink::SINK_NAME | RemoteIcebergSink::SINK_NAME => {
            let coordinator = sink.new_coordinator().await.unwrap();
            sink.mock_log_sinker_with_corrdinator(sink_writer_param, coordinator).await.unwrap()
        },
        _=>{
            sink.new_log_sinker(sink_writer_param).await.unwrap()
        }
    };
    
    if let Err(e) = log_sinker.consume_log_and_sink(log_reader).await{
        return Err(e.to_string());
    }
    return Err("Stream closed".to_string());
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct TableSchemaFromYml{
    table_name: String,
    pk_indexs: Vec<usize>,
    columns: Vec<ColumnDescFromYml>,
}

impl TableSchemaFromYml{
    pub fn into_source_schema(&self) -> Vec<Column>{
        self.columns.iter().map(|column| Column{
            name: column.name.clone(),
            data_type: column.r#type.clone(),
            is_visible: true,
        }).collect()
    }

    pub fn into_sink_schema(&self) -> Vec<ColumnDesc>{
        self.columns.iter().map(|column| ColumnDesc::named(column.name.clone(), ColumnId::new(1), column.r#type.clone())).collect()
    }
}
#[derive(Debug, Deserialize)]
struct ColumnDescFromYml{
    name: String,
    #[serde(deserialize_with = "deserialize_datatype")]
    r#type: DataType,
}

fn deserialize_datatype<'de, D>(deserializer: D) -> Result<DataType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    DataType::from_str(s).map_err(serde::de::Error::custom)
}


fn read_table_schema_from_yml(path: &str) -> TableSchemaFromYml{
    let data = std::fs::read_to_string(path).unwrap();
    let table: TableSchemaFromYml = serde_yaml::from_str(&data).unwrap();
    table
}

fn read_sink_option_from_yml(path: &str) -> HashMap<String, HashMap<String, String>>{
    let data = std::fs::read_to_string(path).unwrap();
    let sink_option: HashMap<String, HashMap<String, String>> = serde_yaml::from_str(&data).unwrap();
    sink_option
}

#[derive(Parser, Debug)]
pub struct Config {
    #[clap(long , default_value = "./sink_bench/schema.yml")]
    schema_path: String,

    #[clap(short, long , default_value = "./sink_bench/sink_option.yml")]
    option_path: String,

    #[clap(short, long, default_value = BENCH_TEST)]
    sink: String,

    #[clap(short, long)]
    rows_per_second: u64,

    #[clap(long, default_value = "10")]
    split_num: String,
}

#[tokio::main]
async fn main() {
    let cfg = Config::parse();
    let table_schema = read_table_schema_from_yml(&cfg.schema_path);
    let mock_datagen_source = MockDatagenSource::new(cfg.rows_per_second, table_schema.into_source_schema(),cfg.split_num).await;
    let throughput_metric = std::sync::Arc::new(RwLock::new(ThroughputMetric::new()));
    let mut mock_range_log_reader = MockRangeLogReader::new(mock_datagen_source,throughput_metric.clone());
    if cfg.sink.eq(&BENCH_TEST.to_string()){
        tokio::spawn(async move {
            mock_range_log_reader.init().await.unwrap();
            loop {
                mock_range_log_reader.next_item().await.unwrap();
            }
        });
        sleep(tokio::time::Duration::from_secs(BENCH_TIME)).await;
        println!("Throughput Test: {:?}", throughput_metric.read().await.get_throughput());
    }else{

        let properties = read_sink_option_from_yml(&cfg.option_path).get(&cfg.sink).expect("Sink type error").clone();

        let connector = properties.get("connector").unwrap().clone();
        let format_desc = SinkFormatDesc::mock_from_legacy_type(&connector.clone(), properties.get("type").unwrap_or(&"append-only".to_string())).unwrap();
        let sink_param
        = SinkParam {
            sink_id: SinkId::new(1),
            properties,
            columns: table_schema.into_sink_schema(),
            downstream_pk: table_schema.pk_indexs,
            sink_type: SinkType::AppendOnly,
            format_desc,
            db_name: "not_need_set".to_string(),
            sink_from_name: "not_need_set".to_string(),
            target_table:None,
        };
        let sink = build_sink(sink_param).unwrap();
        let mut sink_writer_param = SinkWriterParam::for_test();
        sink_writer_param.connector_params.sink_payload_format = SinkPayloadFormat::StreamChunk;
        tokio::spawn(async move {
            dispatch_sink!(sink, sink, {
                consume_log_stream(
                    sink,
                    mock_range_log_reader,
                    sink_writer_param,
                    &connector,
                ).boxed()
            }).await.unwrap();
        });
        sleep(tokio::time::Duration::from_secs(BENCH_TIME)).await;
        println!("Throughput Sink: {:?}", throughput_metric.read().await.get_throughput());
    }
}
