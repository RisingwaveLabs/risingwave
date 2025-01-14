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

use std::fmt::Debug;

use anyhow::Context;
use risingwave_common::bail;
use risingwave_common::types::DataType;

use crate::error::ConnectorResult;
use crate::parser::simd_json_parser::DebeziumMongoJsonAccessBuilder;
use crate::parser::unified::debezium::DebeziumChangeEvent;
use crate::parser::unified::util::apply_row_operation_on_stream_chunk_writer;
use crate::parser::{
    AccessBuilderImpl, ByteStreamSourceParser, EncodingProperties, ParserFormat,
    SourceStreamChunkRowWriter,
};
use crate::source::{SourceColumnDesc, SourceContext, SourceContextRef};

#[derive(Debug)]
pub struct DebeziumMongoJsonParser {
    pub(crate) rw_columns: Vec<SourceColumnDesc>,
    source_ctx: SourceContextRef,
    key_builder: AccessBuilderImpl,
    payload_builder: AccessBuilderImpl,
}

fn build_accessor_builder(
    config: EncodingProperties,
    strong_schema: bool,
) -> anyhow::Result<AccessBuilderImpl> {
    match config {
        EncodingProperties::MongoJson => Ok(AccessBuilderImpl::DebeziumMongoJson(
            DebeziumMongoJsonAccessBuilder::new(strong_schema)?,
        )),
        _ => bail!("unsupported encoding for DEBEZIUM_MONGO format"),
    }
}

impl DebeziumMongoJsonParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        source_ctx: SourceContextRef,
    ) -> ConnectorResult<Self> {
        let _id_column = rw_columns
            .iter()
            .find(|desc| {
                desc.name == "_id"
                    && matches!(
                        desc.data_type,
                        DataType::Jsonb
                            | DataType::Varchar
                            | DataType::Int32
                            | DataType::Int64
                    )
            })
            .context("Debezium Mongo needs a `_id` column with supported types (Varchar Jsonb int32 int64) in table")?.clone();

        let strong_schema = source_ctx.connector_props.enable_strong_schema();

        if !strong_schema {
            let _payload_column = rw_columns
                .iter()
                .find(|desc| desc.name == "payload" && matches!(desc.data_type, DataType::Jsonb))
                .context(
                    "Debezium Mongo needs a `payload` column with supported types Jsonb in table",
                )?
                .clone();

            let columns = rw_columns
                .iter()
                .filter(|desc| desc.is_visible() && desc.additional_column.column_type.is_none())
                .count();

            // _rw_{connector}_file/partition & _rw_{connector}_offset are created automatically.
            if columns != 2 || !rw_columns.iter().any(|desc| desc.name == "_id") {
                bail!("Debezium Mongo needs a `_id` column in table");
            }
        }

        // encodings are fixed to MongoJson

        // for key, it doesn't matter if strong schema is enabled or not
        let key_builder = build_accessor_builder(EncodingProperties::MongoJson, false)?;

        let payload_builder = build_accessor_builder(EncodingProperties::MongoJson, strong_schema)?;

        Ok(Self {
            rw_columns,
            source_ctx,
            key_builder,
            payload_builder,
        })
    }

    pub async fn parse_inner(
        &mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        mut writer: SourceStreamChunkRowWriter<'_>,
    ) -> ConnectorResult<()> {
        let key_accessor = match key {
            None => None,
            Some(data) => Some(self.key_builder.generate_accessor(data).await?),
        };
        let payload_accessor = match payload {
            None => None,
            Some(data) => Some(self.payload_builder.generate_accessor(data).await?),
        };

        let row_op = DebeziumChangeEvent::new_mongodb_event(key_accessor, payload_accessor);
        apply_row_operation_on_stream_chunk_writer(row_op, &mut writer).map_err(Into::into)
    }
}

impl DebeziumMongoJsonParser {
    pub fn strong_schema(&self) -> bool {
        self.source_ctx.connector_props.enable_strong_schema()
    }
}

impl ByteStreamSourceParser for DebeziumMongoJsonParser {
    fn columns(&self) -> &[SourceColumnDesc] {
        &self.rw_columns
    }

    fn source_ctx(&self) -> &SourceContext {
        &self.source_ctx
    }

    fn parser_format(&self) -> ParserFormat {
        ParserFormat::DebeziumMongo
    }

    async fn parse_one<'a>(
        &'a mut self,
        key: Option<Vec<u8>>,
        payload: Option<Vec<u8>>,
        writer: SourceStreamChunkRowWriter<'a>,
    ) -> ConnectorResult<()> {
        self.parse_inner(key, payload, writer).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::Op;
    use risingwave_common::catalog::{ColumnDesc, ColumnId};
    use risingwave_common::row::Row;
    use risingwave_common::types::{ScalarImpl, ToOwnedDatum};

    use super::*;
    use crate::parser::unified::debezium::extract_bson_id;
    use crate::parser::SourceStreamChunkBuilder;
    use crate::source::cdc::CDC_STRONG_SCHEMA_KEY;
    use crate::source::{ConnectorProperties, SourceCtrlOpts};
    #[test]
    fn test_parse_bson_value_id_int() {
        let data = r#"{"_id":{"$numberInt":"2345"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();
        let a = extract_bson_id(&DataType::Int32, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Int32(2345)));
    }
    #[test]
    fn test_parse_bson_value_id_long() {
        let data = r#"{"_id":{"$numberLong":"22423434544"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();

        let a = extract_bson_id(&DataType::Int64, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Int64(22423434544)));
    }

    #[test]
    fn test_parse_bson_value_id_oid() {
        let data = r#"{"_id":{"$oid":"5d505646cf6d4fe581014ab2"}}"#;
        let pld: serde_json::Value = serde_json::from_str(data).unwrap();
        let a = extract_bson_id(&DataType::Varchar, &pld).unwrap();
        assert_eq!(a, Some(ScalarImpl::Utf8("5d505646cf6d4fe581014ab2".into())));
    }

    #[tokio::test]
    async fn test_parse_delete_message() {
        let (key, payload) = (
            // key
            br#"{"schema":null,"payload":{"id":"{\"$oid\": \"65bc9fb6c485f419a7a877fe\"}"}}"#.to_vec(),
            // payload
            br#"{"schema":null,"payload":{"before":null,"after":null,"updateDescription":null,"source":{"version":"2.4.2.Final","connector":"mongodb","name":"RW_CDC_3001","ts_ms":1706968217000,"snapshot":"false","db":"dev","sequence":null,"rs":"rs0","collection":"test","ord":2,"lsid":null,"txnNumber":null,"wallTime":null},"op":"d","ts_ms":1706968217377,"transaction":null}}"#.to_vec()
        );

        let columns = vec![
            SourceColumnDesc::simple("_id", DataType::Varchar, ColumnId::from(0)),
            SourceColumnDesc::simple("payload", DataType::Jsonb, ColumnId::from(1)),
        ];
        let mut parser =
            DebeziumMongoJsonParser::new(columns.clone(), SourceContext::dummy().into()).unwrap();
        let mut builder =
            SourceStreamChunkBuilder::new(columns.clone(), SourceCtrlOpts::for_test());
        parser
            .parse_inner(Some(key), Some(payload), builder.row_writer())
            .await
            .unwrap();
        builder.finish_current_chunk();
        let chunk = builder.consume_ready_chunks().next().unwrap();
        let mut rows = chunk.rows();

        let (op, row) = rows.next().unwrap();
        assert_eq!(op, Op::Delete);
        // oid
        assert_eq!(
            row.datum_at(0).to_owned_datum(),
            (Some(ScalarImpl::Utf8("65bc9fb6c485f419a7a877fe".into())))
        );

        // payload should be null
        assert_eq!(row.datum_at(1).to_owned_datum(), None);
    }

    #[tokio::test]
    async fn test_long_id() {
        let input = vec![
            // data with payload and schema field
            br#"{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"before"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"removedFields"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"updatedFields"},{"type":"array","items":{"type":"struct","fields":[{"type":"string","optional":false,"field":"field"},{"type":"int32","optional":false,"field":"size"}],"optional":false,"name":"io.debezium.connector.mongodb.changestream.truncatedarray","version":1},"optional":true,"field":"truncatedArrays"}],"optional":true,"name":"io.debezium.connector.mongodb.changestream.updatedescription","version":1,"field":"updateDescription"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"string","optional":true,"field":"lsid"},{"type":"int64","optional":true,"field":"txnNumber"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"name":"event.block","version":1,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.customers.Envelope"},"payload":{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}}"#.to_vec(),
            // data without payload and schema field
            br#"{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}"#.to_vec()];

        let columns = vec![
            SourceColumnDesc::simple("_id", DataType::Int64, ColumnId::from(0)),
            SourceColumnDesc::simple("payload", DataType::Jsonb, ColumnId::from(1)),
        ];
        for data in input {
            let mut parser =
                DebeziumMongoJsonParser::new(columns.clone(), SourceContext::dummy().into())
                    .unwrap();

            let mut builder =
                SourceStreamChunkBuilder::new(columns.clone(), SourceCtrlOpts::for_test());

            parser
                .parse_inner(None, Some(data), builder.row_writer())
                .await
                .unwrap();
            builder.finish_current_chunk();
            let chunk = builder.consume_ready_chunks().next().unwrap();
            let mut rows = chunk.rows();
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);

            assert_eq!(
            row.datum_at(1).to_owned_datum(),
            (Some(ScalarImpl::Jsonb(
                serde_json::json!({"_id": {"$numberLong": "1004"},"first_name": "Anne","last_name": "Kretchmar","email": "annek@noanswer.org"}).into()
            )))
        );
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int64(1004)))
            );
        }
    }

    #[tokio::test]
    async fn test_strong_schema() {
        let input = vec![
            // data with payload and schema field
            br#"{"schema":{"type":"struct","fields":[{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"before"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"after"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"patch"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"filter"},{"type":"struct","fields":[{"type":"array","items":{"type":"string","optional":false},"optional":true,"field":"removedFields"},{"type":"string","optional":true,"name":"io.debezium.data.Json","version":1,"field":"updatedFields"},{"type":"array","items":{"type":"struct","fields":[{"type":"string","optional":false,"field":"field"},{"type":"int32","optional":false,"field":"size"}],"optional":false,"name":"io.debezium.connector.mongodb.changestream.truncatedarray","version":1},"optional":true,"field":"truncatedArrays"}],"optional":true,"name":"io.debezium.connector.mongodb.changestream.updatedescription","version":1,"field":"updateDescription"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"version"},{"type":"string","optional":false,"field":"connector"},{"type":"string","optional":false,"field":"name"},{"type":"int64","optional":false,"field":"ts_ms"},{"type":"string","optional":true,"name":"io.debezium.data.Enum","version":1,"parameters":{"allowed":"true,last,false,incremental"},"default":"false","field":"snapshot"},{"type":"string","optional":false,"field":"db"},{"type":"string","optional":true,"field":"sequence"},{"type":"string","optional":false,"field":"rs"},{"type":"string","optional":false,"field":"collection"},{"type":"int32","optional":false,"field":"ord"},{"type":"string","optional":true,"field":"lsid"},{"type":"int64","optional":true,"field":"txnNumber"}],"optional":false,"name":"io.debezium.connector.mongo.Source","field":"source"},{"type":"string","optional":true,"field":"op"},{"type":"int64","optional":true,"field":"ts_ms"},{"type":"struct","fields":[{"type":"string","optional":false,"field":"id"},{"type":"int64","optional":false,"field":"total_order"},{"type":"int64","optional":false,"field":"data_collection_order"}],"optional":true,"name":"event.block","version":1,"field":"transaction"}],"optional":false,"name":"dbserver1.inventory.customers.Envelope"},"payload":{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}}"#.to_vec(),
            // data without payload and schema field
            br#"{"before":null,"after":"{\"_id\": {\"$numberLong\": \"1004\"},\"first_name\": \"Anne\",\"last_name\": \"Kretchmar\",\"email\": \"annek@noanswer.org\"}","patch":null,"filter":null,"updateDescription":null,"source":{"version":"2.1.4.Final","connector":"mongodb","name":"dbserver1","ts_ms":1681879044000,"snapshot":"last","db":"inventory","sequence":null,"rs":"rs0","collection":"customers","ord":1,"lsid":null,"txnNumber":null},"op":"r","ts_ms":1681879054736,"transaction":null}"#.to_vec()];

        let columns = vec![
            ColumnDesc::named("_id", ColumnId::new(0), DataType::Int64),
            ColumnDesc::named("first_name", ColumnId::new(1), DataType::Varchar),
            ColumnDesc::named("last_name", ColumnId::new(2), DataType::Varchar),
            ColumnDesc::named("email", ColumnId::new(3), DataType::Varchar),
        ];

        let columns = columns
            .iter()
            .map(SourceColumnDesc::from)
            .collect::<Vec<_>>();

        let ConnectorProperties::MongodbCdc(mut cdc_props) =
            ConnectorProperties::MongodbCdc(Box::default())
        else {
            unreachable!()
        };

        cdc_props
            .properties
            .insert(CDC_STRONG_SCHEMA_KEY.to_string(), "true".to_string());
        let source_ctx: Arc<_> = SourceContext {
            connector_props: ConnectorProperties::MongodbCdc(cdc_props),
            ..SourceContext::dummy()
        }
        .into();

        for data in input {
            let mut parser = DebeziumMongoJsonParser::new(columns.clone(), source_ctx.clone())
                .expect("build parser");

            let mut builder =
                SourceStreamChunkBuilder::new(columns.clone(), SourceCtrlOpts::for_test());

            parser
                .parse_inner(None, Some(data), builder.row_writer())
                .await
                .unwrap();
            builder.finish_current_chunk();

            let chunk = builder.consume_ready_chunks().next().unwrap();
            let mut rows = chunk.rows();
            let (op, row) = rows.next().unwrap();

            assert_eq!(op, Op::Insert);

            let data = vec![
                ScalarImpl::Int64(1004),
                ScalarImpl::Utf8("Anne".into()),
                ScalarImpl::Utf8("Kretchmar".into()),
                ScalarImpl::Utf8("annek@noanswer.org".into()),
            ];

            for (i, datum) in data.iter().enumerate() {
                assert_eq!(row.datum_at(i).to_owned_datum(), Some(datum.clone()));
            }
        }
    }

    #[tokio::test]
    async fn test_strong_schema_datetime() {
        let columns = vec![
            ColumnDesc::named("_id", ColumnId::new(0), DataType::Int64),
            ColumnDesc::named("rocket type", ColumnId::new(1), DataType::Varchar),
            ColumnDesc::named("freezed at", ColumnId::new(2), DataType::Date),
            ColumnDesc::named("launch time", ColumnId::new(3), DataType::Timestamptz),
        ];

        let columns = columns
            .iter()
            .map(SourceColumnDesc::from)
            .collect::<Vec<_>>();

        let data = vec![
            // naked data
 br#"
{
  "before": null,
  "after": "{\"_id\": {\"$numberLong\": \"1004\"}, \"rocket type\": \"Starblazer X-2000\", \"freezed at\": {\"$date\": \"2024-12-01T00:00:00Z\"}, \"launch time\": {\"$timestamp\": {\"t\": 1735689600, \"i\": 0}}}",
  "source": {
    "version": "2.1.4.Final",
    "connector": "mongodb",
    "name": "dbserver1",
    "ts_ms": 1696502096000,
    "snapshot": "false",
    "db": "inventory",
    "collection": "rockets",
    "ord": 1,
    "lsid": null,
    "txnNumber": null
  },
  "op": "c",
  "ts_ms": 1696502096000
}
"#.to_vec(),
            br#"
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "field": "_id",
        "type": "int64",
        "optional": false
      },
      {
        "field": "rocket type",
        "type": "string",
        "optional": true
      },
      {
        "field": "freezed at",
        "type": "string",
        "name": "io.debezium.time.Date",
        "version": 1,
        "optional": true
      },
      {
        "field": "launch time",
        "type": "string",
        "name": "io.debezium.time.ZonedTimestamp",
        "version": 1,
        "optional": true
      }
    ],
    "optional": false,
    "name": "dbserver1.inventory.rockets.Envelope"
  },
  "payload": {
    "before": null,
    "after": "{\"_id\": {\"$numberLong\": \"1004\"}, \"rocket type\": \"Starblazer X-2000\", \"freezed at\": {\"$date\": \"2024-12-01T00:00:00Z\"}, \"launch time\": {\"$timestamp\": {\"t\": 1735689600, \"i\": 0}}}",
    "source": {
      "version": "2.1.4.Final",
      "connector": "mongodb",
      "name": "dbserver1",
      "ts_ms": 1696502096000,
      "snapshot": "false",
      "db": "inventory",
      "collection": "rockets",
      "ord": 1,
      "lsid": null,
      "txnNumber": null
    },
    "op": "c",
    "ts_ms": 1696502096000
  }
}
"#.to_vec()
        ];

        let ConnectorProperties::MongodbCdc(mut cdc_props) =
            ConnectorProperties::MongodbCdc(Box::default())
        else {
            unreachable!()
        };

        cdc_props
            .properties
            .insert(CDC_STRONG_SCHEMA_KEY.to_string(), "true".to_string());
        let source_ctx: Arc<_> = SourceContext {
            connector_props: ConnectorProperties::MongodbCdc(cdc_props),
            ..SourceContext::dummy()
        }
        .into();

        let expected_datetime =
            chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z").unwrap();
        let expected_date = chrono::DateTime::parse_from_rfc3339("2024-12-01T00:00:00Z")
            .unwrap()
            .date_naive();

        for datum in data {
            let mut parser = DebeziumMongoJsonParser::new(columns.clone(), source_ctx.clone())
                .expect("build parser");
            let mut builder =
                SourceStreamChunkBuilder::new(columns.clone(), SourceCtrlOpts::for_test());
            println!("parsing: {:?}", datum);
            parser
                .parse_inner(None, Some(datum), builder.row_writer())
                .await
                .unwrap();
            builder.finish_current_chunk();
            let chunk = builder.consume_ready_chunks().next().unwrap();
            let mut rows = chunk.rows();
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            let data = vec![
                ScalarImpl::Int64(1004),
                ScalarImpl::Utf8("Starblazer X-2000".into()),
                ScalarImpl::Date(expected_date.clone().into()),
                ScalarImpl::Timestamptz(expected_datetime.clone().into()),
            ];
            for (i, datum) in data.iter().enumerate() {
                assert_eq!(row.datum_at(i).to_owned_datum(), Some(datum.clone()));
            }
        }
    }
}
