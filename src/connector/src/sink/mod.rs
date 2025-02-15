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
pub mod big_query;
pub mod boxed;
pub mod catalog;
pub mod clickhouse;
pub mod coordinate;
pub mod decouple_checkpoint_log_sink;
pub mod deltalake;
pub mod doris;
pub mod doris_starrocks_connector;
pub mod dynamodb;
pub mod elasticsearch_opensearch;
pub mod encoder;
pub mod file_sink;
pub mod formatter;
pub mod google_pubsub;
pub mod iceberg;
pub mod kafka;
pub mod kinesis;
pub mod log_store;
pub mod mock_coordination_client;
pub mod mongodb;
pub mod mqtt;
pub mod nats;
pub mod postgres;
pub mod pulsar;
pub mod redis;
pub mod remote;
pub mod sqlserver;
pub mod starrocks;
pub mod test_sink;
pub mod trivial;
pub mod utils;
pub mod writer;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::LazyLock;

use ::clickhouse::error::Error as ClickHouseError;
use ::deltalake::DeltaTableError;
use ::redis::RedisError;
use anyhow::anyhow;
use async_trait::async_trait;
use clickhouse::CLICKHOUSE_SINK;
use decouple_checkpoint_log_sink::{
    COMMIT_CHECKPOINT_INTERVAL, DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITHOUT_SINK_DECOUPLE,
    DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE,
};
use deltalake::DELTALAKE_SINK;
use iceberg::ICEBERG_SINK;
use opendal::Error as OpendalError;
use prometheus::Registry;
use risingwave_common::array::ArrayError;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::hash::ActorId;
use risingwave_common::metrics::{
    LabelGuardedHistogram, LabelGuardedHistogramVec, LabelGuardedIntCounter,
    LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::secret::{LocalSecretManager, SecretError};
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};
use risingwave_pb::catalog::PbSinkType;
use risingwave_pb::connector_service::{PbSinkParam, SinkMetadata, TableSchema};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::MetaClient;
use starrocks::STARROCKS_SINK;
use thiserror::Error;
use thiserror_ext::AsReport;
pub use tracing;

use self::catalog::{SinkFormatDesc, SinkType};
use self::mock_coordination_client::{MockMetaClient, SinkCoordinationRpcClientEnum};
use crate::error::ConnectorError;
use crate::sink::catalog::desc::SinkDesc;
use crate::sink::catalog::{SinkCatalog, SinkId};
use crate::sink::file_sink::fs::FsSink;
use crate::sink::log_store::{LogReader, LogStoreReadItem, LogStoreResult, TruncateOffset};
use crate::sink::writer::SinkWriter;

const BOUNDED_CHANNEL_SIZE: usize = 16;
#[macro_export]
macro_rules! for_all_sinks {
    ($macro:path $(, $arg:tt)*) => {
        $macro! {
            {
                { Redis, $crate::sink::redis::RedisSink },
                { Kafka, $crate::sink::kafka::KafkaSink },
                { Pulsar, $crate::sink::pulsar::PulsarSink },
                { BlackHole, $crate::sink::trivial::BlackHoleSink },
                { Kinesis, $crate::sink::kinesis::KinesisSink },
                { ClickHouse, $crate::sink::clickhouse::ClickHouseSink },
                { Iceberg, $crate::sink::iceberg::IcebergSink },
                { Mqtt, $crate::sink::mqtt::MqttSink },
                { GooglePubSub, $crate::sink::google_pubsub::GooglePubSubSink },
                { Nats, $crate::sink::nats::NatsSink },
                { Jdbc, $crate::sink::remote::JdbcSink },
                // { ElasticSearchJava, $crate::sink::remote::ElasticSearchJavaSink },
                // { OpensearchJava, $crate::sink::remote::OpenSearchJavaSink },
                { ElasticSearch, $crate::sink::elasticsearch_opensearch::elasticsearch::ElasticSearchSink },
                { Opensearch, $crate::sink::elasticsearch_opensearch::opensearch::OpenSearchSink },
                { Cassandra, $crate::sink::remote::CassandraSink },
                { HttpJava, $crate::sink::remote::HttpJavaSink },
                { Doris, $crate::sink::doris::DorisSink },
                { Starrocks, $crate::sink::starrocks::StarrocksSink },
                { S3, $crate::sink::file_sink::opendal_sink::FileSink<$crate::sink::file_sink::s3::S3Sink>},

                { Gcs, $crate::sink::file_sink::opendal_sink::FileSink<$crate::sink::file_sink::gcs::GcsSink>  },
                { Azblob, $crate::sink::file_sink::opendal_sink::FileSink<$crate::sink::file_sink::azblob::AzblobSink>},
                { Webhdfs, $crate::sink::file_sink::opendal_sink::FileSink<$crate::sink::file_sink::webhdfs::WebhdfsSink>},

                { Fs, $crate::sink::file_sink::opendal_sink::FileSink<FsSink>  },
                { Snowflake, $crate::sink::file_sink::opendal_sink::FileSink<$crate::sink::file_sink::s3::SnowflakeSink>},
                { DeltaLake, $crate::sink::deltalake::DeltaLakeSink },
                { BigQuery, $crate::sink::big_query::BigQuerySink },
                { DynamoDb, $crate::sink::dynamodb::DynamoDbSink },
                { Mongodb, $crate::sink::mongodb::MongodbSink },
                { SqlServer, $crate::sink::sqlserver::SqlServerSink },
                { Postgres, $crate::sink::postgres::PostgresSink },

                { Test, $crate::sink::test_sink::TestSink },
                { Table, $crate::sink::trivial::TableSink }
            }
            $(,$arg)*
        }
    };
}

#[macro_export]
macro_rules! dispatch_sink {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $impl:tt, $sink:tt, $body:tt) => {{
        use $crate::sink::SinkImpl;

        match $impl {
            $(
                SinkImpl::$variant_name($sink) => $body,
            )*
        }
    }};
    ($impl:expr, $sink:ident, $body:expr) => {{
        $crate::for_all_sinks! {$crate::dispatch_sink, {$impl}, $sink, {$body}}
    }};
}

#[macro_export]
macro_rules! match_sink_name_str {
    ({$({$variant_name:ident, $sink_type:ty}),*}, $name_str:tt, $type_name:ident, $body:tt, $on_other_closure:tt) => {{
        use $crate::sink::Sink;
        match $name_str {
            $(
                <$sink_type>::SINK_NAME => {
                    type $type_name = $sink_type;
                    {
                        $body
                    }
                },
            )*
            other => ($on_other_closure)(other),
        }
    }};
    ($name_str:expr, $type_name:ident, $body:expr, $on_other_closure:expr) => {{
        $crate::for_all_sinks! {$crate::match_sink_name_str, {$name_str}, $type_name, {$body}, {$on_other_closure}}
    }};
}

pub const CONNECTOR_TYPE_KEY: &str = "connector";
pub const SINK_TYPE_OPTION: &str = "type";
pub const SINK_WITHOUT_BACKFILL: &str = "snapshot";
pub const SINK_TYPE_APPEND_ONLY: &str = "append-only";
pub const SINK_TYPE_DEBEZIUM: &str = "debezium";
pub const SINK_TYPE_UPSERT: &str = "upsert";
pub const SINK_USER_FORCE_APPEND_ONLY_OPTION: &str = "force_append_only";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkParam {
    pub sink_id: SinkId,
    pub sink_name: String,
    pub properties: BTreeMap<String, String>,
    pub columns: Vec<ColumnDesc>,
    pub downstream_pk: Vec<usize>,
    pub sink_type: SinkType,
    pub format_desc: Option<SinkFormatDesc>,
    pub db_name: String,

    /// - For `CREATE SINK ... FROM ...`, the name of the source table.
    /// - For `CREATE SINK ... AS <query>`, the name of the sink itself.
    ///
    /// See also `gen_sink_plan`.
    // TODO(eric): Why need these 2 fields (db_name and sink_from_name)?
    pub sink_from_name: String,
}

impl SinkParam {
    pub fn from_proto(pb_param: PbSinkParam) -> Self {
        let table_schema = pb_param.table_schema.expect("should contain table schema");
        let format_desc = match pb_param.format_desc {
            Some(f) => f.try_into().ok(),
            None => {
                let connector = pb_param.properties.get(CONNECTOR_TYPE_KEY);
                let r#type = pb_param.properties.get(SINK_TYPE_OPTION);
                match (connector, r#type) {
                    (Some(c), Some(t)) => SinkFormatDesc::from_legacy_type(c, t).ok().flatten(),
                    _ => None,
                }
            }
        };
        Self {
            sink_id: SinkId::from(pb_param.sink_id),
            sink_name: pb_param.sink_name,
            properties: pb_param.properties,
            columns: table_schema.columns.iter().map(ColumnDesc::from).collect(),
            downstream_pk: table_schema
                .pk_indices
                .iter()
                .map(|i| *i as usize)
                .collect(),
            sink_type: SinkType::from_proto(
                PbSinkType::try_from(pb_param.sink_type).expect("should be able to convert"),
            ),
            format_desc,
            db_name: pb_param.db_name,
            sink_from_name: pb_param.sink_from_name,
        }
    }

    pub fn to_proto(&self) -> PbSinkParam {
        PbSinkParam {
            sink_id: self.sink_id.sink_id,
            sink_name: self.sink_name.clone(),
            properties: self.properties.clone(),
            table_schema: Some(TableSchema {
                columns: self.columns.iter().map(|col| col.to_protobuf()).collect(),
                pk_indices: self.downstream_pk.iter().map(|i| *i as u32).collect(),
            }),
            sink_type: self.sink_type.to_proto().into(),
            format_desc: self.format_desc.as_ref().map(|f| f.to_proto()),
            db_name: self.db_name.clone(),
            sink_from_name: self.sink_from_name.clone(),
        }
    }

    pub fn schema(&self) -> Schema {
        Schema {
            fields: self.columns.iter().map(Field::from).collect(),
        }
    }

    // `SinkParams` should only be used when there is a secret context.
    // FIXME: Use a new type for `SinkFormatDesc` with properties contain filled secrets.
    pub fn fill_secret_for_format_desc(
        format_desc: Option<SinkFormatDesc>,
    ) -> Result<Option<SinkFormatDesc>> {
        match format_desc {
            Some(mut format_desc) => {
                format_desc.options = LocalSecretManager::global()
                    .fill_secrets(format_desc.options, format_desc.secret_refs.clone())?;
                Ok(Some(format_desc))
            }
            None => Ok(None),
        }
    }

    /// Try to convert a `SinkCatalog` to a `SinkParam` and fill the secrets to properties.
    pub fn try_from_sink_catalog(sink_catalog: SinkCatalog) -> Result<Self> {
        let columns = sink_catalog
            .visible_columns()
            .map(|col| col.column_desc.clone())
            .collect();
        let properties_with_secret = LocalSecretManager::global()
            .fill_secrets(sink_catalog.properties, sink_catalog.secret_refs)?;
        let format_desc_with_secret = Self::fill_secret_for_format_desc(sink_catalog.format_desc)?;
        Ok(Self {
            sink_id: sink_catalog.id,
            sink_name: sink_catalog.name,
            properties: properties_with_secret,
            columns,
            downstream_pk: sink_catalog.downstream_pk,
            sink_type: sink_catalog.sink_type,
            format_desc: format_desc_with_secret,
            db_name: sink_catalog.db_name,
            sink_from_name: sink_catalog.sink_from_name,
        })
    }
}

pub static GLOBAL_SINK_METRICS: LazyLock<SinkMetrics> =
    LazyLock::new(|| SinkMetrics::new(&GLOBAL_METRICS_REGISTRY));

#[derive(Clone)]
pub struct SinkMetrics {
    pub sink_commit_duration: LabelGuardedHistogramVec<4>,
    pub connector_sink_rows_received: LabelGuardedIntCounterVec<4>,

    // Log store writer metrics
    pub log_store_first_write_epoch: LabelGuardedIntGaugeVec<3>,
    pub log_store_latest_write_epoch: LabelGuardedIntGaugeVec<3>,
    pub log_store_write_rows: LabelGuardedIntCounterVec<3>,

    // Log store reader metrics
    pub log_store_latest_read_epoch: LabelGuardedIntGaugeVec<4>,
    pub log_store_read_rows: LabelGuardedIntCounterVec<4>,
    pub log_store_read_bytes: LabelGuardedIntCounterVec<4>,
    pub log_store_reader_wait_new_future_duration_ns: LabelGuardedIntCounterVec<4>,

    // Iceberg metrics
    pub iceberg_write_qps: LabelGuardedIntCounterVec<3>,
    pub iceberg_write_latency: LabelGuardedHistogramVec<3>,
    pub iceberg_rolling_unflushed_data_file: LabelGuardedIntGaugeVec<3>,
    pub iceberg_position_delete_cache_num: LabelGuardedIntGaugeVec<3>,
    pub iceberg_partition_num: LabelGuardedIntGaugeVec<3>,
    pub iceberg_write_bytes: LabelGuardedIntCounterVec<3>,
}

impl SinkMetrics {
    pub fn new(registry: &Registry) -> Self {
        let sink_commit_duration = register_guarded_histogram_vec_with_registry!(
            "sink_commit_duration",
            "Duration of commit op in sink",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let connector_sink_rows_received = register_guarded_int_counter_vec_with_registry!(
            "connector_sink_rows_received",
            "Number of rows received by sink",
            &["actor_id", "connector_type", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_first_write_epoch = register_guarded_int_gauge_vec_with_registry!(
            "log_store_first_write_epoch",
            "The first write epoch of log store",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_latest_write_epoch = register_guarded_int_gauge_vec_with_registry!(
            "log_store_latest_write_epoch",
            "The latest write epoch of log store",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_write_rows = register_guarded_int_counter_vec_with_registry!(
            "log_store_write_rows",
            "The write rate of rows",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_latest_read_epoch = register_guarded_int_gauge_vec_with_registry!(
            "log_store_latest_read_epoch",
            "The latest read epoch of log store",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_read_rows = register_guarded_int_counter_vec_with_registry!(
            "log_store_read_rows",
            "The read rate of rows",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_read_bytes = register_guarded_int_counter_vec_with_registry!(
            "log_store_read_bytes",
            "Total size of chunks read by log reader",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let log_store_reader_wait_new_future_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "log_store_reader_wait_new_future_duration_ns",
                "Accumulated duration of LogReader to wait for next call to create future",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap();

        let iceberg_write_qps = register_guarded_int_counter_vec_with_registry!(
            "iceberg_write_qps",
            "The qps of iceberg writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let iceberg_write_latency = register_guarded_histogram_vec_with_registry!(
            "iceberg_write_latency",
            "The latency of iceberg writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let iceberg_rolling_unflushed_data_file = register_guarded_int_gauge_vec_with_registry!(
            "iceberg_rolling_unflushed_data_file",
            "The unflushed data file count of iceberg rolling writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let iceberg_position_delete_cache_num = register_guarded_int_gauge_vec_with_registry!(
            "iceberg_position_delete_cache_num",
            "The delete cache num of iceberg position delete writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let iceberg_partition_num = register_guarded_int_gauge_vec_with_registry!(
            "iceberg_partition_num",
            "The partition num of iceberg partition writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let iceberg_write_bytes = register_guarded_int_counter_vec_with_registry!(
            "iceberg_write_bytes",
            "The write bytes of iceberg writer",
            &["actor_id", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        Self {
            sink_commit_duration,
            connector_sink_rows_received,
            log_store_first_write_epoch,
            log_store_latest_write_epoch,
            log_store_write_rows,
            log_store_latest_read_epoch,
            log_store_read_rows,
            log_store_read_bytes,
            log_store_reader_wait_new_future_duration_ns,
            iceberg_write_qps,
            iceberg_write_latency,
            iceberg_rolling_unflushed_data_file,
            iceberg_position_delete_cache_num,
            iceberg_partition_num,
            iceberg_write_bytes,
        }
    }
}

#[derive(Clone)]
pub struct SinkWriterParam {
    // TODO(eric): deprecate executor_id
    pub executor_id: u64,
    pub vnode_bitmap: Option<Bitmap>,
    pub meta_client: Option<SinkMetaClient>,
    // The val has two effect:
    // 1. Indicates that the sink will accpect the data chunk with extra partition value column.
    // 2. The index of the extra partition value column.
    // More detail of partition value column, see `PartitionComputeInfo`
    pub extra_partition_col_idx: Option<usize>,

    pub actor_id: ActorId,
    pub sink_id: SinkId,
    pub sink_name: String,
    pub connector: String,
}

#[derive(Clone)]
pub struct SinkWriterMetrics {
    pub sink_commit_duration: LabelGuardedHistogram<4>,
    pub connector_sink_rows_received: LabelGuardedIntCounter<4>,
}

impl SinkWriterMetrics {
    pub fn new(writer_param: &SinkWriterParam) -> Self {
        let labels = [
            &writer_param.actor_id.to_string(),
            writer_param.connector.as_str(),
            &writer_param.sink_id.to_string(),
            writer_param.sink_name.as_str(),
        ];
        let sink_commit_duration = GLOBAL_SINK_METRICS
            .sink_commit_duration
            .with_guarded_label_values(&labels);
        let connector_sink_rows_received = GLOBAL_SINK_METRICS
            .connector_sink_rows_received
            .with_guarded_label_values(&labels);
        Self {
            sink_commit_duration,
            connector_sink_rows_received,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self {
            sink_commit_duration: LabelGuardedHistogram::test_histogram(),
            connector_sink_rows_received: LabelGuardedIntCounter::test_int_counter(),
        }
    }
}

#[derive(Clone)]
pub enum SinkMetaClient {
    MetaClient(MetaClient),
    MockMetaClient(MockMetaClient),
}

impl SinkMetaClient {
    pub async fn sink_coordinate_client(&self) -> SinkCoordinationRpcClientEnum {
        match self {
            SinkMetaClient::MetaClient(meta_client) => {
                SinkCoordinationRpcClientEnum::SinkCoordinationRpcClient(
                    meta_client.sink_coordinate_client().await,
                )
            }
            SinkMetaClient::MockMetaClient(mock_meta_client) => {
                SinkCoordinationRpcClientEnum::MockSinkCoordinationRpcClient(
                    mock_meta_client.sink_coordinate_client(),
                )
            }
        }
    }

    pub async fn add_sink_fail_evet_log(
        &self,
        sink_id: u32,
        sink_name: String,
        connector: String,
        error: String,
    ) {
        match self {
            SinkMetaClient::MetaClient(meta_client) => {
                match meta_client
                    .add_sink_fail_evet(sink_id, sink_name, connector, error)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e.as_report(), sink_id = sink_id, "Fialed to add sink fail event to event log.");
                    }
                }
            }
            SinkMetaClient::MockMetaClient(_) => {}
        }
    }
}

impl SinkWriterParam {
    pub fn for_test() -> Self {
        SinkWriterParam {
            executor_id: Default::default(),
            vnode_bitmap: Default::default(),
            meta_client: Default::default(),
            extra_partition_col_idx: Default::default(),

            actor_id: 1,
            sink_id: SinkId::new(1),
            sink_name: "test_sink".to_owned(),
            connector: "test_connector".to_owned(),
        }
    }
}

fn is_sink_support_commit_checkpoint_interval(sink_name: &str) -> bool {
    matches!(
        sink_name,
        ICEBERG_SINK | CLICKHOUSE_SINK | STARROCKS_SINK | DELTALAKE_SINK
    )
}
pub trait Sink: TryFrom<SinkParam, Error = SinkError> {
    const SINK_NAME: &'static str;
    type LogSinker: LogSinker;
    type Coordinator: SinkCommitCoordinator;

    fn set_default_commit_checkpoint_interval(
        desc: &mut SinkDesc,
        user_specified: &SinkDecouple,
    ) -> Result<()> {
        if is_sink_support_commit_checkpoint_interval(Self::SINK_NAME) {
            match desc.properties.get(COMMIT_CHECKPOINT_INTERVAL) {
                Some(commit_checkpoint_interval) => {
                    let commit_checkpoint_interval = commit_checkpoint_interval
                        .parse::<u64>()
                        .map_err(|e| SinkError::Config(anyhow!(e)))?;
                    if matches!(user_specified, SinkDecouple::Disable)
                        && commit_checkpoint_interval > 1
                    {
                        return Err(SinkError::Config(anyhow!("config conflict: `commit_checkpoint_interval` larger than 1 means that sink decouple must be enabled, but session config sink_decouple is disabled")));
                    }
                }
                None => match user_specified {
                    SinkDecouple::Default | SinkDecouple::Enable => {
                        desc.properties.insert(
                            COMMIT_CHECKPOINT_INTERVAL.to_owned(),
                            DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITH_SINK_DECOUPLE.to_string(),
                        );
                    }
                    SinkDecouple::Disable => {
                        desc.properties.insert(
                            COMMIT_CHECKPOINT_INTERVAL.to_owned(),
                            DEFAULT_COMMIT_CHECKPOINT_INTERVAL_WITHOUT_SINK_DECOUPLE.to_string(),
                        );
                    }
                },
            }
        }
        Ok(())
    }

    /// `user_specified` is the value of `sink_decouple` config.
    fn is_sink_decouple(user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default | SinkDecouple::Enable => Ok(true),
            SinkDecouple::Disable => Ok(false),
        }
    }

    async fn validate(&self) -> Result<()>;
    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker>;
    #[expect(clippy::unused_async)]
    async fn new_coordinator(&self) -> Result<Self::Coordinator> {
        Err(SinkError::Coordinator(anyhow!("no coordinator")))
    }
}

pub trait SinkLogReader: Send + Sized + 'static {
    /// Emit the next item.
    ///
    /// The implementation should ensure that the future is cancellation safe.
    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_;

    /// Mark that all items emitted so far have been consumed and it is safe to truncate the log
    /// from the current offset.
    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()>;
}

impl<R: LogReader> SinkLogReader for R {
    fn next_item(
        &mut self,
    ) -> impl Future<Output = LogStoreResult<(u64, LogStoreReadItem)>> + Send + '_ {
        <Self as LogReader>::next_item(self)
    }

    fn truncate(&mut self, offset: TruncateOffset) -> LogStoreResult<()> {
        <Self as LogReader>::truncate(self, offset)
    }
}

#[async_trait]
pub trait LogSinker: 'static {
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<!>;
}

#[async_trait]
pub trait SinkCommitCoordinator {
    /// Initialize the sink committer coordinator
    async fn init(&mut self) -> Result<()>;
    /// After collecting the metadata from each sink writer, a coordinator will call `commit` with
    /// the set of metadata. The metadata is serialized into bytes, because the metadata is expected
    /// to be passed between different gRPC node, so in this general trait, the metadata is
    /// serialized bytes.
    async fn commit(&mut self, epoch: u64, metadata: Vec<SinkMetadata>) -> Result<()>;
}

pub struct DummySinkCommitCoordinator;

#[async_trait]
impl SinkCommitCoordinator for DummySinkCommitCoordinator {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self, _epoch: u64, _metadata: Vec<SinkMetadata>) -> Result<()> {
        Ok(())
    }
}

impl SinkImpl {
    pub fn new(mut param: SinkParam) -> Result<Self> {
        const PRIVATE_LINK_TARGET_KEY: &str = "privatelink.targets";

        // remove privatelink related properties if any
        param.properties.remove(PRIVATE_LINK_TARGET_KEY);

        let sink_type = param
            .properties
            .get(CONNECTOR_TYPE_KEY)
            .ok_or_else(|| SinkError::Config(anyhow!("missing config: {}", CONNECTOR_TYPE_KEY)))?;

        match_sink_name_str!(
            sink_type.to_lowercase().as_str(),
            SinkType,
            Ok(SinkType::try_from(param)?.into()),
            |other| {
                Err(SinkError::Config(anyhow!(
                    "unsupported sink connector {}",
                    other
                )))
            }
        )
    }

    pub fn is_sink_into_table(&self) -> bool {
        matches!(self, SinkImpl::Table(_))
    }

    pub fn is_blackhole(&self) -> bool {
        matches!(self, SinkImpl::BlackHole(_))
    }
}

pub fn build_sink(param: SinkParam) -> Result<SinkImpl> {
    SinkImpl::new(param)
}

macro_rules! def_sink_impl {
    () => {
        $crate::for_all_sinks! { def_sink_impl }
    };
    ({ $({ $variant_name:ident, $sink_type:ty }),* }) => {
        #[derive(Debug)]
        pub enum SinkImpl {
            $(
                $variant_name(Box<$sink_type>),
            )*
        }

        $(
            impl From<$sink_type> for SinkImpl {
                fn from(sink: $sink_type) -> SinkImpl {
                    SinkImpl::$variant_name(Box::new(sink))
                }
            }
        )*
    };
}

def_sink_impl!();

pub type Result<T> = std::result::Result<T, SinkError>;

#[derive(Error, Debug)]
pub enum SinkError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("Kinesis error: {0}")]
    Kinesis(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Remote sink error: {0}")]
    Remote(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Encode error: {0}")]
    Encode(String),
    #[error("Avro error: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("Iceberg error: {0}")]
    Iceberg(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("config error: {0}")]
    Config(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("coordinator error: {0}")]
    Coordinator(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("ClickHouse error: {0}")]
    ClickHouse(String),
    #[error("Redis error: {0}")]
    Redis(String),
    #[error("Mqtt error: {0}")]
    Mqtt(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Nats error: {0}")]
    Nats(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Google Pub/Sub error: {0}")]
    GooglePubSub(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Doris/Starrocks connect error: {0}")]
    DorisStarrocksConnect(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Doris error: {0}")]
    Doris(String),
    #[error("DeltaLake error: {0}")]
    DeltaLake(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("ElasticSearch/OpenSearch error: {0}")]
    ElasticSearchOpenSearch(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Starrocks error: {0}")]
    Starrocks(String),
    #[error("File error: {0}")]
    File(String),
    #[error("Pulsar error: {0}")]
    Pulsar(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("BigQuery error: {0}")]
    BigQuery(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("DynamoDB error: {0}")]
    DynamoDb(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("SQL Server error: {0}")]
    SqlServer(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error("Postgres error: {0}")]
    Postgres(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
    #[error(transparent)]
    Connector(
        #[from]
        #[backtrace]
        ConnectorError,
    ),
    #[error("Secret error: {0}")]
    Secret(
        #[from]
        #[backtrace]
        SecretError,
    ),
    #[error("Mongodb error: {0}")]
    Mongodb(
        #[source]
        #[backtrace]
        anyhow::Error,
    ),
}

impl From<icelake::Error> for SinkError {
    fn from(value: icelake::Error) -> Self {
        SinkError::Iceberg(anyhow!(value))
    }
}

impl From<OpendalError> for SinkError {
    fn from(error: OpendalError) -> Self {
        SinkError::File(error.to_report_string())
    }
}

impl From<parquet::errors::ParquetError> for SinkError {
    fn from(error: parquet::errors::ParquetError) -> Self {
        SinkError::File(error.to_report_string())
    }
}

impl From<ArrayError> for SinkError {
    fn from(error: ArrayError) -> Self {
        SinkError::File(error.to_report_string())
    }
}

impl From<RpcError> for SinkError {
    fn from(value: RpcError) -> Self {
        SinkError::Remote(anyhow!(value))
    }
}

impl From<ClickHouseError> for SinkError {
    fn from(value: ClickHouseError) -> Self {
        SinkError::ClickHouse(value.to_report_string())
    }
}

impl From<DeltaTableError> for SinkError {
    fn from(value: DeltaTableError) -> Self {
        SinkError::DeltaLake(anyhow!(value))
    }
}

impl From<RedisError> for SinkError {
    fn from(value: RedisError) -> Self {
        SinkError::Redis(value.to_report_string())
    }
}

impl From<tiberius::error::Error> for SinkError {
    fn from(err: tiberius::error::Error) -> Self {
        SinkError::SqlServer(anyhow!(err))
    }
}

impl From<::elasticsearch::Error> for SinkError {
    fn from(err: ::elasticsearch::Error) -> Self {
        SinkError::ElasticSearchOpenSearch(anyhow!(err))
    }
}

impl From<::opensearch::Error> for SinkError {
    fn from(err: ::opensearch::Error) -> Self {
        SinkError::ElasticSearchOpenSearch(anyhow!(err))
    }
}

impl From<tokio_postgres::Error> for SinkError {
    fn from(err: tokio_postgres::Error) -> Self {
        SinkError::Postgres(anyhow!(err))
    }
}
