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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use bytes::BytesMut;
use chrono::{TimeZone, Utc};
use opendal::{FuturesAsyncWriter, Operator, Writer as OpendalWriter};
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use risingwave_common::array::arrow::arrow_schema_iceberg::{self, SchemaRef};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_json::Value;
use serde_with::{serde_as, DisplayFromStr};
use strum_macros::{Display, EnumString};
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};
use with_options::WithOptions;

use crate::sink::catalog::SinkEncode;
use crate::sink::encoder::{
    JsonEncoder, JsonbHandlingMode, RowEncoder, TimeHandlingMode, TimestampHandlingMode,
    TimestamptzHandlingMode,
};
use crate::sink::file_sink::batching_log_sink::BatchingLogSinker;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkFormatDesc, SinkParam};
use crate::source::TryFromBTreeMap;
use crate::with_options::WithOptions;

pub const DEFAULT_ROLLOVER_SECONDS: usize = 10;
pub const DEFAULT_MAX_ROW_COUNR: usize = 10240;

pub fn default_rollover_seconds() -> usize {
    DEFAULT_ROLLOVER_SECONDS
}
pub fn default_max_row_count() -> usize {
    DEFAULT_MAX_ROW_COUNR
}
/// The `FileSink` struct represents a file sink that uses the `OpendalSinkBackend` trait for its backend implementation.
///
/// # Type Parameters
///
/// - S: The type parameter S represents the concrete implementation of the `OpendalSinkBackend` trait used by this file sink.
#[derive(Debug, Clone)]
pub struct FileSink<S: OpendalSinkBackend> {
    pub(crate) op: Operator,
    /// The path to the file where the sink writes data.
    pub(crate) path: String,
    /// The schema describing the structure of the data being written to the file sink.
    pub(crate) schema: Schema,
    pub(crate) is_append_only: bool,
    /// The batching strategy for sinking data to files.
    pub(crate) batching_strategy: BatchingStrategy,

    /// The description of the sink's format.
    pub(crate) format_desc: SinkFormatDesc,
    pub(crate) engine_type: EngineType,
    pub(crate) _marker: PhantomData<S>,
}

/// The `OpendalSinkBackend` trait unifies the behavior of various sink backends
/// implemented through `OpenDAL`(`<https://github.com/apache/opendal>`).
///
/// # Type Parameters
///
/// - Properties: Represents the necessary parameters for establishing a backend.
///
/// # Constants
///
/// - `SINK_NAME`: A static string representing the name of the sink.
///
/// # Functions
///
/// - `from_btreemap`: Automatically parse the required parameters from the input create sink statement.
/// - `new_operator`: Creates a new operator using the provided backend properties.
/// - `get_path`: Returns the path of the sink file specified by the user's create sink statement.
pub trait OpendalSinkBackend: Send + Sync + 'static + Clone + PartialEq {
    type Properties: TryFromBTreeMap + Send + Sync + Clone + WithOptions;
    const SINK_NAME: &'static str;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties>;
    fn new_operator(properties: Self::Properties) -> Result<Operator>;
    fn get_path(properties: Self::Properties) -> String;
    fn get_engine_type() -> EngineType;
    fn get_batching_strategy(properties: Self::Properties) -> BatchingStrategy;
}

#[derive(Clone, Debug)]
pub enum EngineType {
    Gcs,
    S3,
    Fs,
    Azblob,
    Webhdfs,
}

impl<S: OpendalSinkBackend> Sink for FileSink<S> {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = BatchingLogSinker;

    const SINK_NAME: &'static str = S::SINK_NAME;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports append-only mode at present. \
                    Please change the query to append-only, and specify it \
                    explicitly after the `FORMAT ... ENCODE ...` statement. \
                    For example, `FORMAT xxx ENCODE xxx(force_append_only='true')`"
            )));
        }

        if self.format_desc.encode != SinkEncode::Parquet
            && self.format_desc.encode != SinkEncode::Json
        {
            return Err(SinkError::Config(anyhow!(
                "File sink only supports `PARQUET` and `JSON` encode at present."
            )));
        }

        match self.op.list(&self.path).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!(e).into()),
        }
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let writer = OpenDalSinkWriter::new(
            self.op.clone(),
            &self.path,
            self.schema.clone(),
            writer_param.executor_id,
            self.format_desc.encode.clone(),
            self.engine_type.clone(),
            self.batching_strategy.clone(),
        )?;
        Ok(BatchingLogSinker::new(writer))
    }
}

impl<S: OpendalSinkBackend> TryFrom<SinkParam> for FileSink<S> {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S::from_btreemap(param.properties)?;
        let path = S::get_path(config.clone()).to_string();
        let op = S::new_operator(config.clone())?;
        let batching_strategy = S::get_batching_strategy(config.clone());
        let engine_type = S::get_engine_type();

        Ok(Self {
            op,
            path,
            schema,
            is_append_only: param.sink_type.is_append_only(),
            batching_strategy,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            engine_type,
            _marker: PhantomData,
        })
    }
}

pub struct OpenDalSinkWriter {
    schema: SchemaRef,
    operator: Operator,
    sink_writer: Option<FileWriterEnum>,
    write_path: String,
    executor_id: u64,
    encode_type: SinkEncode,
    row_encoder: JsonEncoder,
    engine_type: EngineType,
    pub(crate) batching_strategy: BatchingStrategy,
    current_bached_row_num: usize,
    created_time: SystemTime,
    written_chunk_id: Option<usize>,
}

/// The `FileWriterEnum` enum represents different types of file writers used for various sink
/// implementations.
///
/// # Variants
///
/// - `ParquetFileWriter`: Represents a Parquet file writer using the `AsyncArrowWriter<W>`
///   for writing data to a Parquet file. It accepts an implementation of W: `AsyncWrite` + `Unpin` + `Send`
///   as the underlying writer. In this case, the `OpendalWriter` serves as the underlying writer.
/// - `FileWriter`: Represents a basic `OpenDAL` writer, for writing files in encodes other than parquet.
///
/// The choice of writer used during the actual writing process depends on the encode type of the sink.
enum FileWriterEnum {
    ParquetFileWriter(AsyncArrowWriter<Compat<FuturesAsyncWriter>>),
    FileWriter(OpendalWriter),
}

/// Method for writing files into file system.
///
/// # Write Chunk Methods
/// - `write_batch_and_try_finish`: Writes a stream chunk to the sink and tries to close the writer according to the batching strategy.
///
/// # Close Writer Methods
/// - `try_finish`: Attempts to complete the file write using the writer's internal batching strategy.
/// - `should_finish`: Force close writer regardless of whether it trigger batching strategy.
///
/// # Batching Condition Methods
/// - `try_finish_write_via_batched_rows`: Checks if writing can be completed based on the number of batched rows.
/// - `try_finish_write_via_rollover_interval`: Checks if writing can be completed based on the rollover time interval.
impl OpenDalSinkWriter {
    /// This method writes a chunk and attempts to complete the file write
    /// based on the writer's internal batching strategy. If the write is
    /// successful, it returns the last `chunk_id` that was written;
    /// otherwise, it returns None.
    pub async fn write_batch_and_try_finish(
        &mut self,
        chunk: StreamChunk,
        chunk_id: usize,
    ) -> Result<Option<usize>> {
        if self.sink_writer.is_none() {
            self.create_sink_writer().await?;
        };

        // While writing this chunk (via `append_only`), it checks if the maximum row count
        // has been reached to determine if the file should be finalized.
        // Then, it checks the rollover interval to see if the time-based batching threshold
        // has been met. If either condition results in a successful write, the write is marked as complete.
        let finish_write =
            self.append_only(chunk).await? || self.try_finish_write_via_rollover_interval().await?;
        self.written_chunk_id = Some(chunk_id);
        if finish_write {
            Ok(Some(chunk_id))
        } else {
            Ok(None)
        }
    }

    /// Close current writer, finish writing a file.
    pub async fn commit(&mut self) -> Result<bool> {
        if let Some(sink_writer) = self.sink_writer.take() {
            match sink_writer {
                FileWriterEnum::ParquetFileWriter(w) => {
                    if w.bytes_written() > 0 {
                        let metadata = w.close().await?;
                        tracing::info!(
                            "writer {:?}_{:?}finish write file, metadata: {:?}",
                            self.executor_id,
                            self.created_time
                                .duration_since(UNIX_EPOCH)
                                .expect("Time went backwards")
                                .as_secs(),
                            metadata
                        );
                        return Ok(true);
                    }
                }
                FileWriterEnum::FileWriter(mut w) => {
                    w.close().await?;
                }
            };
        }
        Ok(false)
    }

    /// Attempts to complete the file write using the writer's internal batching strategy.
    ///
    /// If the write is completed, returns the last `chunk_id` that was written;
    /// otherwise, returns None.
    ///
    /// For the file sink, currently, the sink decoupling feature is not enabled.
    /// When a checkpoint arrives, the force commit is performed to write the data to the file.
    /// In the future if flush and checkpoint is decoupled, we should enable sink decouple accordingly.
    pub async fn try_finish(&mut self) -> Result<Option<usize>> {
        match self.try_finish_write_via_rollover_interval().await? {
            true => Ok(self.written_chunk_id),
            false => Ok(None),
        }
    }

    /// This asynchronous function checks if the duration since the writer was
    /// created exceeds the configured rollover interval (`rollover_seconds`).
    /// If the duration condition is met and a sink writer is available, it
    /// closes the current writer (e.g., a Parquet file writer) if any bytes
    /// have been written. The function logs the completion message and
    /// increments the writer index for subsequent writes.
    ///
    ///
    /// Returns:
    /// - `Ok(true)` if the write was finalized successfully due to the
    ///   rollover interval being exceeded.
    /// - `Ok(false)` if the rollover interval has not been met, indicating that
    ///   no action was taken.
    async fn try_finish_write_via_rollover_interval(&mut self) -> Result<bool> {
        if self.duration_seconds_since_writer_created()
            >= self.batching_strategy.clone().rollover_seconds
            && self.commit().await?
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// This asynchronous function checks if the number of rows accumulated
    /// (`current_bached_row_num`) meets or exceeds the maximum row count
    /// defined in the `batching_strategy`. If the threshold is met, it
    /// closes the current writer (e.g., a Parquet file writer), logs the
    /// completion message, and resets the row count. It also increments
    /// the writer index for subsequent writes.
    ///
    /// Returns:
    /// - `Ok(true)` if the write was finalized successfully.
    /// - `Ok(false)` if the threshold has not been met, indicating that
    ///   no action was taken.
    async fn try_finish_write_via_batched_rows(&mut self) -> Result<bool> {
        if self.current_bached_row_num >= self.batching_strategy.clone().max_row_count
            && self.commit().await?
        {
            self.current_bached_row_num = 0;
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn should_finish(&mut self) -> Result<Option<usize>> {
        if self.commit().await? {
            return Ok(self.written_chunk_id);
        }

        Ok(None)
    }
}

/// Init methods.
impl OpenDalSinkWriter {
    pub fn new(
        operator: Operator,
        write_path: &str,
        rw_schema: Schema,
        executor_id: u64,
        encode_type: SinkEncode,
        engine_type: EngineType,
        batching_strategy: BatchingStrategy,
    ) -> Result<Self> {
        let arrow_schema = convert_rw_schema_to_arrow_schema(rw_schema.clone())?;
        let row_encoder = JsonEncoder::new(
            rw_schema,
            None,
            crate::sink::encoder::DateHandlingMode::String,
            TimestampHandlingMode::String,
            TimestamptzHandlingMode::UtcString,
            TimeHandlingMode::String,
            JsonbHandlingMode::String,
        );
        Ok(Self {
            schema: Arc::new(arrow_schema),
            write_path: write_path.to_string(),
            operator,
            sink_writer: None,
            executor_id,

            encode_type,
            row_encoder,
            engine_type,
            batching_strategy,
            current_bached_row_num: 0,
            created_time: SystemTime::now(),
            written_chunk_id: None,
        })
    }

    pub fn path_partition_prefix(&self) -> String {
        match self.created_time.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                let datetime = Utc
                    .timestamp_opt(duration.as_secs() as i64, 0)
                    .single()
                    .expect("Failed to convert timestamp to DateTime<Utc>")
                    .with_timezone(&Utc);
                let path_partition_prefix = self
                    .batching_strategy
                    .path_partition_prefix
                    .as_ref()
                    .unwrap_or(&PathPartitionPrefix::None);
                match path_partition_prefix {
                    PathPartitionPrefix::None => "".to_string(),
                    PathPartitionPrefix::Day => datetime.format("%Y-%m-%d/").to_string(),
                    PathPartitionPrefix::Month => datetime.format("/%Y-%m/").to_string(),
                    PathPartitionPrefix::Hour => datetime.format("/%Y-%m-%d %H:00/").to_string(),
                }
            }
            Err(_) => "Invalid time".to_string(),
        }
    }

    fn duration_seconds_since_writer_created(&self) -> usize {
        let now = SystemTime::now();
        now.duration_since(self.created_time)
            .expect("Time went backwards")
            .as_secs() as usize
    }

    async fn create_object_writer(&mut self) -> Result<OpendalWriter> {
        // Todo: specify more file suffixes based on encode_type.
        let suffix = match self.encode_type {
            SinkEncode::Parquet => "parquet",
            SinkEncode::Json => "json",
            _ => unimplemented!(),
        };

        // With batching in place, the file writing process is decoupled from checkpoints.
        // The current file naming convention is as follows:
        // 1. A subdirectory is defined based on `path_partition_prefix` (e.g., by day、hour or month or none.).
        // 2. The file name includes the `executor_id` and the creation time in seconds since the UNIX epoch.
        // If the engine type is `Fs`, the path is automatically handled, and the filename does not include a path prefix.
        let object_name = match self.engine_type {
            // For the local fs sink, the data will be automatically written to the defined path.
            // Therefore, there is no need to specify the path in the file name.
            EngineType::Fs => {
                format!(
                    "{}{}_{}.{}",
                    self.path_partition_prefix(),
                    self.executor_id,
                    self.created_time
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_secs(),
                    suffix
                )
            }
            _ => format!(
                "{}/{}{}_{}.{}",
                self.write_path,
                self.path_partition_prefix(),
                self.executor_id,
                self.created_time
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_secs(),
                suffix,
            ),
        };

        tracing::info!("create new writer, file name: {:?}", object_name);
        Ok(self
            .operator
            .writer_with(&object_name)
            .concurrent(8)
            .await?)
    }

    async fn create_sink_writer(&mut self) -> Result<()> {
        let object_writer = self.create_object_writer().await?;
        match self.encode_type {
            SinkEncode::Parquet => {
                let props = WriterProperties::builder();
                let parquet_writer: tokio_util::compat::Compat<opendal::FuturesAsyncWriter> =
                    object_writer.into_futures_async_write().compat_write();
                self.sink_writer = Some(FileWriterEnum::ParquetFileWriter(
                    AsyncArrowWriter::try_new(
                        parquet_writer,
                        self.schema.clone(),
                        Some(props.build()),
                    )?,
                ));
                self.current_bached_row_num = 0;

                self.created_time = SystemTime::now();
            }
            _ => {
                self.sink_writer = Some(FileWriterEnum::FileWriter(object_writer));
            }
        }

        Ok(())
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<bool> {
        match self
            .sink_writer
            .as_mut()
            .ok_or_else(|| SinkError::File("Sink writer is not created.".to_string()))?
        {
            FileWriterEnum::ParquetFileWriter(w) => {
                let batch =
                    IcebergArrowConvert.to_record_batch(self.schema.clone(), chunk.data_chunk())?;
                let batch_row_nums = batch.num_rows();
                w.write(&batch).await?;
                self.current_bached_row_num += batch_row_nums;
                let res = self.try_finish_write_via_batched_rows().await?;
                Ok(res)
            }
            FileWriterEnum::FileWriter(w) => {
                let mut chunk_buf = BytesMut::new();
                let batch_row_nums = chunk.data_chunk().capacity();
                // write the json representations of the row(s) in current chunk to `chunk_buf`
                for (op, row) in chunk.rows() {
                    assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
                    // to prevent temporary string allocation,
                    // so we directly write to `chunk_buf` implicitly via `write_fmt`.
                    writeln!(
                        chunk_buf,
                        "{}",
                        Value::Object(self.row_encoder.encode(row)?)
                    )
                    .unwrap(); // write to a `BytesMut` should never fail
                }
                w.write(chunk_buf.freeze()).await?;
                self.current_bached_row_num += batch_row_nums;
                let res = self.try_finish_write_via_batched_rows().await?;
                Ok(res)
            }
        }
    }
}

fn convert_rw_schema_to_arrow_schema(
    rw_schema: risingwave_common::catalog::Schema,
) -> anyhow::Result<arrow_schema_iceberg::Schema> {
    let mut schema_fields = HashMap::new();
    rw_schema.fields.iter().for_each(|field| {
        let res = schema_fields.insert(&field.name, &field.data_type);
        // This assert is to make sure there is no duplicate field name in the schema.
        assert!(res.is_none())
    });
    let mut arrow_fields = vec![];
    for rw_field in &rw_schema.fields {
        let arrow_field = IcebergArrowConvert
            .to_arrow_field(&rw_field.name.clone(), &rw_field.data_type.clone())?;

        arrow_fields.push(arrow_field);
    }

    Ok(arrow_schema_iceberg::Schema::new(arrow_fields))
}

/// `BatchingStrategy` represents the strategy for batching data before writing to files.
///
/// This struct contains settings that control how data is collected and
/// partitioned based on specified criteria:
///
/// - `max_row_count`: Optional maximum number of rows to accumulate before writing.
/// - `rollover_seconds`: Optional time interval (in seconds) to trigger a write,
///   regardless of the number of accumulated rows.
/// - `path_partition_prefix`: Specifies how files are organized into directories
///   based on creation time (e.g., by day, month, or hour).

#[serde_as]
#[derive(Default, Deserialize, Debug, Clone, WithOptions)]
pub struct BatchingStrategy {
    #[serde(default = "default_max_row_count")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_row_count: usize,
    #[serde(default = "default_rollover_seconds")]
    #[serde_as(as = "DisplayFromStr")]
    pub rollover_seconds: usize,
    #[serde(default)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub path_partition_prefix: Option<PathPartitionPrefix>,
}

/// `PathPartitionPrefix` defines the granularity of file partitions based on creation time.
///
/// Each variant specifies how files are organized into directories:
/// - `None`: No partitioning.
/// - `Day`: Files are written in a directory for each day.
/// - `Month`: Files are written in a directory for each month.
/// - `Hour`: Files are written in a directory for each hour.
#[derive(Default, Debug, Clone, PartialEq, Display, Deserialize, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum PathPartitionPrefix {
    #[default]
    None = 0,
    #[serde(alias = "day")]
    Day = 1,
    #[serde(alias = "month")]
    Month = 2,
    #[serde(alias = "hour")]
    Hour = 3,
}
