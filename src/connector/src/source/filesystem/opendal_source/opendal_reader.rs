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

use async_compression::tokio::bufread::GzipDecoder;
use async_trait::async_trait;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use opendal::Operator;
use risingwave_common::array::StreamChunk;
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
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SourceMeta, SplitMetaData,
    SplitReader,
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

    fn into_stream(self) -> BoxSourceChunkStream {
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
                let actor_id = source_ctx.actor_id.to_string();
                let fragment_id = source_ctx.fragment_id.to_string();
                let source_id = source_ctx.source_id.to_string();
                let source_name = source_ctx.source_name.clone();
                let parquet_source_skip_row_count_metrics: risingwave_common::metrics::LabelGuardedMetric<prometheus::core::GenericCounter<prometheus::core::AtomicU64>, 5> = self.source_ctx.metrics
                .parquet_source_skip_row_count
                .with_guarded_label_values(&[
                    &actor_id,
                    &source_id,
                    split.id().as_ref(),
                    &source_name,
                    &fragment_id,
                ]);
                chunk_stream = read_parquet_file(
                    self.connector.op.clone(),
                    object_name,
                    self.columns.clone(),
                    Some(self.parser_config.common.rw_columns.clone()),
                    self.source_ctx.source_ctrl_opts.chunk_size,
                    split.offset,
                    Some(parquet_source_skip_row_count_metrics),
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
                chunk_stream = Box::pin(parser.parse_stream(line_stream));
            }

            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
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
        let reader = op
            .read_with(&object_name)
            .range(start_offset as u64..)
            .into_future() // Unlike `rustc`, `try_stream` seems require manual `into_future`.
            .await?;
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

        let mut offset = start_offset;
        let partition_input_bytes_metrics: risingwave_common::metrics::LabelGuardedMetric<
            prometheus::core::GenericCounter<prometheus::core::AtomicU64>,
            5,
        > = source_ctx
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
    }
}
