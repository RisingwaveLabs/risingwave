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

use std::future::IntoFuture;
use std::pin::Pin;
use std::sync::Arc;

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, StreamChunk};
use risingwave_common::types::{Datum, ScalarImpl};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::opendal_enumerator::OpendalEnumerator;
use super::OpendalSource;
use crate::error::ConnectorResult;
use crate::parser::{ByteStreamSourceParserImpl, EncodingProperties, ParserConfig};
use crate::source::filesystem::file_common::CompressionFormat;
use crate::source::filesystem::nd_streaming::need_nd_streaming;
use crate::source::filesystem::OpendalFsSplit;
use crate::source::iceberg::read_parquet_file;
use crate::source::{
    BoxChunkSourceStream, Column, SourceColumnDesc, SourceContextRef, SourceMessage, SourceMeta,
    SplitMetaData, SplitReader,
};

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

            let chunk_stream;
            if let EncodingProperties::Parquet = &self.parser_config.specific.encoding_config {
                chunk_stream = read_parquet_file(
                    self.connector.op.clone(),
                    object_name.clone(),
                    self.columns.clone(),
                    Some(self.parser_config.common.rw_columns.clone()),
                    self.source_ctx.source_ctrl_opts.chunk_size,
                    split.offset,
                )
                .await?;
            } else {
                assert!(
                    need_nd_streaming(&self.parser_config.specific.encoding_config),
                    "except for parquet, file source only support split by newline for now"
                );

                let line_stream = Self::stream_read_lines(
                    self.connector.op.clone(),
                    split,
                    self.source_ctx.clone(),
                    self.connector.compression_format.clone(),
                );

                let parser =
                    ByteStreamSourceParserImpl::create(self.parser_config.clone(), source_ctx)
                        .await?;
                chunk_stream = Box::pin(parser.into_stream(line_stream));
            }

            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
            }

            if let EncodingProperties::Parquet = &self.parser_config.specific.encoding_config {
                // We no longer determine whether a file is finished by comparing `offset >= size`, the reader itself can sense whether it has reached the end.
                // Therefore, after a file is read completely, we yield one more chunk marked as EOF, with its offset set to `usize::MAX` to indicate that it is finished.
                // In fetch executor, if `offset = usize::MAX` is encountered, the corresponding file can be deleted from the state table.

                // FIXME(wcy-fdu): The order of hidden columns in parquet encode and other encodes is inconsistent, maybe we can yeild a common eof chunk for both parquet encode and other encodes.
                let eof_chunk = Self::generate_eof_chunk_for_parquet_encode(
                    self.parser_config.common.rw_columns.clone(),
                    object_name.clone(),
                )?;
                yield eof_chunk;
            }
        }
    }

    #[try_stream(boxed, ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    pub async fn stream_read_lines(
        op: Operator,
        split: OpendalFsSplit<Src>,
        source_ctx: SourceContextRef,
        compression_format: CompressionFormat,
    ) {
        let actor_id = source_ctx.actor_id.to_string();
        let fragment_id = source_ctx.fragment_id.to_string();
        let source_id = source_ctx.source_id.to_string();
        let source_name = source_ctx.source_name.clone();
        let object_name = split.name.clone();
        let start_offset = split.offset;
        // After a recovery occurs, for gzip-compressed files, it is necessary to read from the beginning each time,
        // other files can continue reading from the last read `start_offset`.
        let reader = match object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
            true => op.read_with(&object_name).into_future().await?,

            false => {
                op.read_with(&object_name)
                    .range(start_offset as u64..)
                    .into_future()
                    .await?
            }
        };

        let stream_reader = StreamReader::new(
            reader.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        let mut buf_reader: Pin<Box<dyn AsyncBufRead + Send>> = match compression_format {
            CompressionFormat::Gzip => {
                let gzip_decoder = GzipDecoder::new(stream_reader);
                Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncBufRead + Send>>
            }
            CompressionFormat::None => {
                // todo: support automatic decompression of more compression types.
                if object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
                    let gzip_decoder = GzipDecoder::new(stream_reader);
                    Box::pin(BufReader::new(gzip_decoder)) as Pin<Box<dyn AsyncBufRead + Send>>
                } else {
                    Box::pin(BufReader::new(stream_reader)) as Pin<Box<dyn AsyncBufRead + Send>>
                }
            }
        };

        let mut offset = match object_name.ends_with(".gz") || object_name.ends_with(".gzip") {
            true => 0,
            false => start_offset,
        };
        let partition_input_bytes_metrics = source_ctx
            .metrics
            .partition_input_bytes
            .with_guarded_label_values(&[
                &actor_id,
                &source_id,
                split.id().as_ref(),
                &source_name,
                &fragment_id,
            ]);

        let max_chunk_size = source_ctx.source_ctrl_opts.chunk_size;
        let mut batch = Vec::with_capacity(max_chunk_size);
        let mut line_buf = String::new();

        loop {
            let n_read = buf_reader.read_line(&mut line_buf).await?;
            if n_read == 0 {
                // EOF
                break;
            }
            // note that the buffer contains the newline character
            debug_assert_eq!(n_read, line_buf.len());

            // FIXME(rc): Here we have to use `offset + n_read`, i.e. the offset of the next line,
            // as the *message offset*, because we check whether a file is finished by comparing the
            // message offset with the file size in `FsFetchExecutor::into_stream`. However, we must
            // understand that this message offset is not semantically consistent with the offset of
            // other source connectors.
            let msg_offset = (offset + n_read).to_string();
            batch.push(SourceMessage {
                key: None,
                payload: Some(std::mem::take(&mut line_buf).into_bytes()),
                offset: msg_offset,
                split_id: split.id(),
                meta: SourceMeta::Empty,
            });
            offset += n_read;
            partition_input_bytes_metrics.inc_by(n_read as _);

            if batch.len() >= max_chunk_size {
                yield std::mem::replace(&mut batch, Vec::with_capacity(max_chunk_size));
            }
        }

        if !batch.is_empty() {
            batch.shrink_to_fit();
            yield batch;
        }
        // For json and csv encodes, yield an eof message to mark the file has been read.
        let eof_batch = vec![SourceMessage {
            key: None,
            payload: None,
            offset: usize::MAX.to_string(),
            split_id: split.id(),
            meta: SourceMeta::Empty,
        }];
        yield eof_batch;
    }

    // Generate a special chunk to mark the end of reading. Its offset is usize::MAX and other fields are null.
    fn generate_eof_chunk_for_parquet_encode(
        rw_columns: Vec<SourceColumnDesc>,
        object_name: String,
    ) -> Result<StreamChunk, crate::error::ConnectorError> {
        const MAX_HIDDEN_COLUMN_NUMS: usize = 3;
        let column_size = rw_columns.len();
        let mut chunk_columns = Vec::with_capacity(rw_columns.len() + MAX_HIDDEN_COLUMN_NUMS);
        for source_column in rw_columns.clone() {
            match source_column.column_type {
                crate::source::SourceColumnType::Normal => {
                    match source_column.is_hidden_addition_col {
                        false => {
                            let rw_data_type: &risingwave_common::types::DataType =
                                &source_column.data_type;
                            let mut array_builder =
                                ArrayBuilderImpl::with_type(column_size, rw_data_type.clone());

                            array_builder.append_null();
                            let res = array_builder.finish();
                            let column = Arc::new(res);
                            chunk_columns.push(column);
                        }
                        // handle hidden columns, for file source, the hidden columns are only `Offset` and `Filename`
                        true => {
                            if let Some(additional_column_type) =
                                &source_column.additional_column.column_type
                            {
                                match additional_column_type{
                            risingwave_pb::plan_common::additional_column::ColumnType::Offset(_) =>{
                                let mut array_builder =
                                ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                // set the EOF chunk's offset to usize::MAX to mark the end of file.
                                let datum: Datum = Some(ScalarImpl::Utf8((usize::MAX).to_string().into()));
                                array_builder.append(datum);
                                let res = array_builder.finish();
                                let column = Arc::new(res);
                                chunk_columns.push(column);

                            },
                            risingwave_pb::plan_common::additional_column::ColumnType::Filename(_) => {
                                let mut array_builder =
                                ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                let datum: Datum =  Some(ScalarImpl::Utf8(object_name.clone().into()));
                                array_builder.append( datum);
                                let res = array_builder.finish();
                                let column = Arc::new(res);
                                chunk_columns.push(column);
                            },
                            _ => unreachable!()
                        }
                            }
                        }
                    }
                }
                crate::source::SourceColumnType::RowId => {
                    let mut array_builder =
                        ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                    let datum: Datum = None;
                    array_builder.append(datum);
                    let res = array_builder.finish();
                    let column = Arc::new(res);
                    chunk_columns.push(column);
                }
                // The following fields is only used in CDC source
                crate::source::SourceColumnType::Offset | crate::source::SourceColumnType::Meta => {
                    unreachable!()
                }
            }
        }

        let data_chunk = DataChunk::new(chunk_columns.clone(), 1_usize);
        Ok(data_chunk.into())
    }
}
