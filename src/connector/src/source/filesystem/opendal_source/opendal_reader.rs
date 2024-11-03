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

use std::future::IntoFuture;
use std::pin::Pin;

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{parquet_to_arrow_schema, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::file::metadata::FileMetaData;
use risingwave_common::array::arrow::arrow_schema_iceberg;
use risingwave_common::array::StreamChunk;
use risingwave_common::util::tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::{ReaderStream, StreamReader};

use super::opendal_enumerator::OpendalEnumerator;
use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, EncodingProperties, ParquetParser, ParserConfig};
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::{nd_streaming, OpendalFsSplit};
use crate::source::{
    BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
    SplitReader,
};

const STREAM_READER_CAPACITY: usize = 4096;

#[derive(Debug, Clone)]
pub struct OpendalReader<Src: OpendalSource> {
    connector: OpendalEnumerator<Src>,
    splits: Vec<OpendalFsSplit<Src>>,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
    columns: Option<Vec<Column>>,
}
#[async_trait]
impl<Src: OpendalSource> SplitReader for OpendalReader<Src> {
    type Properties = Src::Properties;
    type Split = OpendalFsSplit<Src>;

    async fn new(
        properties: Src::Properties,
        splits: Vec<OpendalFsSplit<Src>>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        let connector = Src::new_enumerator(properties)?;
        let opendal_reader = OpendalReader {
            connector,
            splits,
            parser_config,
            source_ctx,
            columns,
        };
        Ok(opendal_reader)
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        self.into_stream_inner()
    }
}

impl<Src: OpendalSource> OpendalReader<Src> {
    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    async fn into_stream_inner(self) {
        for split in self.splits {
            let source_ctx = self.source_ctx.clone();

            let object_name = split.name.clone();

            let msg_stream;

            if let EncodingProperties::Parquet = &self.parser_config.specific.encoding_config {
                // // If the format is "parquet", use `ParquetParser` to convert `record_batch` into stream chunk.
                let mut reader: tokio_util::compat::Compat<opendal::FuturesAsyncReader> = self
                    .connector
                    .op
                    .reader_with(&object_name)
                    .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
                    .await?
                    .into_futures_async_read(..)
                    .await?
                    .compat();
                let parquet_metadata = reader.get_metadata().await.map_err(anyhow::Error::from)?;

                let file_metadata = parquet_metadata.file_metadata();
                let column_indices =
                    extract_valid_column_indices(self.columns.clone(), file_metadata)?;
                let projection_mask =
                    ProjectionMask::leaves(file_metadata.schema_descr(), column_indices);
                // For the Parquet format, we directly convert from a record batch to a stream chunk.
                // Therefore, the offset of the Parquet file represents the current position in terms of the number of rows read from the file.
                let record_batch_stream = ParquetRecordBatchStreamBuilder::new(reader)
                    .await?
                    .with_batch_size(self.source_ctx.source_ctrl_opts.chunk_size)
                    .with_projection(projection_mask)
                    .with_offset(split.offset)
                    .build()?;

                let parquet_parser = ParquetParser::new(
                    self.parser_config.common.rw_columns.clone(),
                    object_name,
                    split.offset,
                )?;
                msg_stream = parquet_parser.into_stream(record_batch_stream);
            } else {
                let data_stream = Self::stream_read_object(
                    self.connector.op.clone(),
                    split,
                    self.source_ctx.clone(),
                    self.connector.compression_format.clone(),
                );

                let parser =
                    ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx)
                        .await?;
                msg_stream = if need_nd_streaming(&self.parser_config.specific.encoding_config) {
                    Box::pin(parser.into_stream(nd_streaming::split_stream(data_stream)))
                } else {
                    Box::pin(parser.into_stream(data_stream))
                };
            }
            #[for_await]
            for msg in msg_stream {
                let msg = msg?;
                yield msg;
            }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_object(
        op: Operator,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
        compression_format: CompressionFormat,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.to_string();
        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let split_id = split.id();
        let object_name = split.name.clone();
        let reader = op
            .read_with(&object_name)
            .range(split.offset as u64..)
            .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
            .await?;
        let stream_reader = StreamReader::new(
            reader.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        let buf_reader: Pin<Box<dyn AsyncRead + Send>> = match compression_format {
            CompressionFormat::Gzip => {
                let gzip_decoder = GzipDecoder::new(stream_reader);
                Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncRead + Send>>
            }
            CompressionFormat::None => {
                // todo: support automatic decompression of more compression types.
                if object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
                    let gzip_decoder = GzipDecoder::new(stream_reader);
                    Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncRead + Send>>
                } else {
                    Box::pin(BufReader::new(stream_reader)) as Pin<Box<dyn AsyncRead + Send>>
                }
            }
        };

        let mut offset: usize = split.offset;
        let mut batch_size: usize = 0;
        let mut batch = Vec::new();
        let partition_input_bytes_metrics = source_ctx
            .metrics
            .partition_input_bytes
            .with_guarded_label_values(&[
                &actor_id,
                &source_id,
                &split_id,
                &source_name,
                &fragment_id,
            ]);
        let stream = ReaderStream::with_capacity(buf_reader, STREAM_READER_CAPACITY);
        #[for_await]
        for read in stream {
            let bytes = read?;
            let len = bytes.len();
            let msg = SourceMessage {
                key: None,
                payload: Some(bytes.as_ref().to_vec()),
                offset: offset.to_string(),
                split_id: split.id(),
                meta: SourceMeta::Empty,
            };
            offset += len;
            batch_size += len;
            batch.push(msg);

            if batch.len() >= max_chunk_size {
                partition_input_bytes_metrics.inc_by(batch_size as u64);
                let yield_batch = std::mem::take(&mut batch);
                batch_size = 0;
                yield yield_batch;
            }
        }
        if !batch.is_empty() {
            partition_input_bytes_metrics.inc_by(batch_size as u64);
            yield batch;
        }
    }
}

/// Extracts valid column indices from a Parquet file schema based on the user's requested schema.
///
/// This function is used for column pruning of Parquet files. It calculates the intersection
/// between the columns in the currently read Parquet file and the schema provided by the user.
/// This is useful for reading a `RecordBatch` with the appropriate `ProjectionMask`, ensuring that
/// only the necessary columns are read.
///
/// # Parameters
/// - `columns`: A vector of `Column` representing the user's requested schema.
/// - `metadata`: A reference to `FileMetaData` containing the schema and metadata of the Parquet file.
///
/// # Returns
/// - A `ConnectorResult<Vec<usize>>`, which contains the indices of the valid columns in the
///   Parquet file schema that match the requested schema. If an error occurs during processing,
///   it returns an appropriate error.
pub fn extract_valid_column_indices(
    columns: Option<Vec<Column>>,
    metadata: &FileMetaData,
) -> ConnectorResult<Vec<usize>> {
    match columns {
        Some(rw_columns) => {
            let parquet_column_names = metadata
                .schema_descr()
                .columns()
                .iter()
                .map(|c| c.name())
                .collect_vec();

            let converted_arrow_schema =
                parquet_to_arrow_schema(metadata.schema_descr(), metadata.key_value_metadata())
                    .map_err(anyhow::Error::from)?;

            let valid_column_indices: Vec<usize> = rw_columns
                .iter()
                .filter_map(|column| {
                    parquet_column_names
                        .iter()
                        .position(|&name| name == column.name)
                        .and_then(|pos| {
                            if is_data_type_matching(
                                &column.data_type,
                                converted_arrow_schema.field(pos).data_type(),
                            ) {
                                Some(pos)
                            } else {
                                None
                            }
                        })
                })
                .collect();
            Ok(valid_column_indices)
        }
        None => Ok(vec![]),
    }
}

/// Checks if the data type in RisingWave matches the data type in a Parquet(arrow) file.
///
/// This function compares the `DataType` from RisingWave with the `DataType` from
/// Parquet file, returning `true` if they are compatible. Specifically, for `Timestamp`
/// types, it ensures that any of the four `TimeUnit` variants from Parquet
/// (i.e., `Second`, `Millisecond`, `Microsecond`, and `Nanosecond`) can be matched
/// with the corresponding `Timestamp` type in RisingWave.
pub fn is_data_type_matching(
    rw_data_type: &risingwave_common::types::DataType,
    arrow_data_type: &arrow_schema_iceberg::DataType,
) -> bool {
    match rw_data_type {
        risingwave_common::types::DataType::Boolean => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Boolean)
        }
        risingwave_common::types::DataType::Int16 => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Int16)
        }
        risingwave_common::types::DataType::Int32 => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Int32)
        }
        risingwave_common::types::DataType::Int64 => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Int64)
        }
        risingwave_common::types::DataType::Float32 => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Float32)
        }
        risingwave_common::types::DataType::Float64 => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Float64)
        }
        risingwave_common::types::DataType::Decimal => {
            matches!(
                arrow_data_type,
                arrow_schema_iceberg::DataType::Decimal128(_, _)
            ) || matches!(
                arrow_data_type,
                arrow_schema_iceberg::DataType::Decimal256(_, _)
            )
        }
        risingwave_common::types::DataType::Date => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Date32)
                || matches!(arrow_data_type, arrow_schema_iceberg::DataType::Date64)
        }
        risingwave_common::types::DataType::Varchar => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Utf8)
        }
        risingwave_common::types::DataType::Time => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Time32(_))
                || matches!(arrow_data_type, arrow_schema_iceberg::DataType::Time64(_))
        }
        risingwave_common::types::DataType::Timestamp => {
            matches!(
                arrow_data_type,
                arrow_schema_iceberg::DataType::Timestamp(_, _)
            )
        }
        risingwave_common::types::DataType::Timestamptz => {
            matches!(
                arrow_data_type,
                arrow_schema_iceberg::DataType::Timestamp(_, _)
            )
        }
        risingwave_common::types::DataType::Interval => {
            matches!(arrow_data_type, arrow_schema_iceberg::DataType::Interval(_))
        }
        risingwave_common::types::DataType::List(inner_type) => {
            if let arrow_schema_iceberg::DataType::List(field_ref) = arrow_data_type {
                let inner_rw_type = inner_type.clone();
                let inner_arrow_type = field_ref.data_type();
                is_data_type_matching(&inner_rw_type, inner_arrow_type)
            } else {
                false
            }
        }
        risingwave_common::types::DataType::Map(map_type) => {
            if let arrow_schema_iceberg::DataType::Map(field_ref, _) = arrow_data_type {
                let key_rw_type = map_type.key();
                let value_rw_type = map_type.value();
                let struct_type = field_ref.data_type();
                if let arrow_schema_iceberg::DataType::Struct(fields) = struct_type {
                    if fields.len() == 2 {
                        let key_arrow_type = fields[0].data_type();
                        let value_arrow_type = fields[1].data_type();
                        return is_data_type_matching(key_rw_type, key_arrow_type)
                            && is_data_type_matching(value_rw_type, value_arrow_type);
                    }
                }
            }
            false
        }
        _ => false, // Handle other data types as necessary
    }
}
