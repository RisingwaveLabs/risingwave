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

use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_common::error::{Result, RwError};

use super::simd_json_parser::DebeziumJsonAccessBuilder;
use super::{DebeziumAvroAccessBuilder, DebeziumAvroParserConfig};
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, EncodingType, ParserProperties,
    SourceStreamChunkRowWriter, WriteGuard,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumParser {
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
}

async fn build_accessor_builder(
    config: EncodingProperties,
    encoding_type: EncodingType,
) -> Result<AccessBuilderImpl> {
    match config {
        EncodingProperties::Avro(_) => {
            let config = DebeziumAvroParserConfig::new(config).await?;
            Ok(AccessBuilderImpl::DebeziumAvro(
                DebeziumAvroAccessBuilder::new(config, encoding_type)?,
            ))
        }
        EncodingProperties::Json(_) => Ok(AccessBuilderImpl::DebeziumJson(
            DebeziumJsonAccessBuilder::new()?,
        )),
        EncodingProperties::Protobuf(_) => {
            Ok(AccessBuilderImpl::new_default(config, encoding_type).await?)
        }
        _ => Err(RwError::from(ProtocolError(
            "unsupported encoding for Debezium".to_string(),
        ))),
    }
}

impl DebeziumParser {
    pub async fn new(
        props: ParserProperties,
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> Result<Self> {
        let key_config = props
            .key_encoding_config
            .unwrap_or(props.encoding_config.clone());
        let key_builder = build_accessor_builder(key_config, EncodingType::Key).await?;
        let payload_builder =
            build_accessor_builder(props.encoding_config, EncodingType::Value).await?;
        Ok(Self {
            key_builder,
            payload_builder,
            rw_columns,
            source_ctx,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> Result<WriteGuard> {
        // tombetone messages are handled implicitly by these accessors
        let key_accessor = match key {
            None => None,
            Some(data) => Some(self.key_builder.generate_accessor(data).await?),
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data).await?),
        };
        let row_op = DebeziumChangeEvent::new(key_accessor, payload_accessor);

        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer)
    }
}

impl ByteStreamSourceParser for DebeziumParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    async fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> Result<WriteGuard> {
        self.parse_inner(key, payload, writer).await
    }
}
