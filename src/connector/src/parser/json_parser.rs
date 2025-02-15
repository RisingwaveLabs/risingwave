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

// Note on this file:
//
// There's no struct named `JsonParser` anymore since #13707. `ENCODE JSON` will be
// dispatched to `PlainParser` or `UpsertParser` with `JsonAccessBuilder` instead.
//
// This file now only contains utilities and tests for JSON parsing. Also, to avoid
// rely on the internal implementation and allow that to be changed, the tests use
// `ByteStreamSourceParserImpl` to create a parser instance.

use std::collections::BTreeMap;

use anyhow::Context as _;
use risingwave_connector_codec::JsonSchema;
use risingwave_pb::plan_common::ColumnDesc;

use super::utils::{bytes_from_url, get_kafka_topic};
use super::{JsonProperties, SchemaRegistryAuth};
use crate::error::ConnectorResult;
use crate::parser::unified::json::{JsonAccess, JsonParseOptions};
use crate::parser::unified::AccessImpl;
use crate::parser::AccessBuilder;
use crate::schema::schema_registry::{handle_sr_list, Client};

#[derive(Debug)]
pub struct JsonAccessBuilder {
    value: Option<Vec<u8>>,
    payload_start_idx: usize,
    json_parse_options: JsonParseOptions,
}

impl AccessBuilder for JsonAccessBuilder {
    #[allow(clippy::unused_async)]
    async fn generate_accessor(&mut self, payload: Vec<u8>) -> ConnectorResult<AccessImpl<'_>> {
        // XXX: When will we enter this branch?
        if payload.is_empty() {
            self.value = Some("{}".into());
        } else {
            self.value = Some(payload);
        }
        let value = simd_json::to_borrowed_value(
            &mut self.value.as_mut().unwrap()[self.payload_start_idx..],
        )
        .context("failed to parse json payload")?;
        Ok(AccessImpl::Json(JsonAccess::new_with_options(
            value,
            // Debezium and Canal have their special json access builder and will not
            // use this
            &self.json_parse_options,
        )))
    }
}

impl JsonAccessBuilder {
    pub fn new(config: JsonProperties) -> ConnectorResult<Self> {
        let mut json_parse_options = JsonParseOptions::DEFAULT;
        if let Some(mode) = config.timestamptz_handling {
            json_parse_options.timestamptz_handling = mode;
        }
        Ok(Self {
            value: None,
            payload_start_idx: if config.use_schema_registry { 5 } else { 0 },
            json_parse_options,
        })
    }
}

pub async fn fetch_json_schema_and_map_to_columns(
    schema_location: &str,
    schema_registry_auth: Option<SchemaRegistryAuth>,
    props: &BTreeMap<String, String>,
) -> ConnectorResult<Vec<ColumnDesc>> {
    let url = handle_sr_list(schema_location)?;
    let mut json_schema = if let Some(schema_registry_auth) = schema_registry_auth {
        let client = Client::new(url.clone(), &schema_registry_auth)?;
        let topic = get_kafka_topic(props)?;
        let schema = client
            .get_schema_by_subject(&format!("{}-value", topic))
            .await?;
        JsonSchema::parse_str(&schema.content)?
    } else {
        let url = url.first().unwrap();
        let bytes = bytes_from_url(url, None).await?;
        JsonSchema::parse_bytes(&bytes)?
    };
    json_schema
        .json_schema_to_columns(url.first().unwrap().clone())
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::vec;

    use itertools::Itertools;
    use risingwave_common::array::{Op, StructValue};
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::row::Row;
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::{DataType, ScalarImpl, ToOwnedDatum};
    use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
    use risingwave_pb::plan_common::{AdditionalColumn, AdditionalColumnKey};

    use crate::parser::test_utils::ByteStreamSourceParserImplTestExt as _;
    use crate::parser::{
        ByteStreamSourceParserImpl, CommonParserConfig, ParserConfig, ProtocolProperties,
        SourceColumnDesc, SpecificParserConfig,
    };
    use crate::source::SourceColumnType;

    fn make_parser(rw_columns: Vec<SourceColumnDesc>) -> ByteStreamSourceParserImpl {
        ByteStreamSourceParserImpl::create_for_test(ParserConfig {
            common: CommonParserConfig { rw_columns },
            specific: SpecificParserConfig::DEFAULT_PLAIN_JSON,
        })
        .unwrap()
    }

    fn make_upsert_parser(rw_columns: Vec<SourceColumnDesc>) -> ByteStreamSourceParserImpl {
        ByteStreamSourceParserImpl::create_for_test(ParserConfig {
            common: CommonParserConfig { rw_columns },
            specific: SpecificParserConfig {
                protocol_config: ProtocolProperties::Upsert,
                ..SpecificParserConfig::DEFAULT_PLAIN_JSON
            },
        })
        .unwrap()
    }

    fn get_payload() -> Vec<Vec<u8>> {
        vec![
            br#"{"i32":1,"bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar","date":"2021-01-01","timestamp":"2021-01-01 16:06:12.269","decimal":12345.67890,"interval":"P1Y2M3DT0H5M0S"}"#.to_vec(),
            br#"{"i32":1,"f32":12345e+10,"f64":12345,"decimal":12345,"interval":"1 day"}"#.to_vec(),
        ]
    }

    fn get_array_top_level_payload() -> Vec<Vec<u8>> {
        vec![
            br#"[{"i32":1,"bool":true,"i16":1,"i64":12345678,"f32":1.23,"f64":1.2345,"varchar":"varchar","date":"2021-01-01","timestamp":"2021-01-01 16:06:12.269","decimal":12345.67890}, {"i32":1,"f32":12345e+10,"f64":12345,"decimal":12345}]"#.to_vec()
        ]
    }

    async fn test_json_parser(get_payload: fn() -> Vec<Vec<u8>>) {
        let descs = vec![
            SourceColumnDesc::simple("i32", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("bool", DataType::Boolean, 2.into()),
            SourceColumnDesc::simple("i16", DataType::Int16, 3.into()),
            SourceColumnDesc::simple("i64", DataType::Int64, 4.into()),
            SourceColumnDesc::simple("f32", DataType::Float32, 5.into()),
            SourceColumnDesc::simple("f64", DataType::Float64, 6.into()),
            SourceColumnDesc::simple("varchar", DataType::Varchar, 7.into()),
            SourceColumnDesc::simple("date", DataType::Date, 8.into()),
            SourceColumnDesc::simple("timestamp", DataType::Timestamp, 9.into()),
            SourceColumnDesc::simple("decimal", DataType::Decimal, 10.into()),
            SourceColumnDesc::simple("interval", DataType::Interval, 11.into()),
        ];

        let parser = make_parser(descs);
        let chunk = parser.parse(get_payload()).await;

        let mut rows = chunk.rows();

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(row.datum_at(0).to_owned_datum(), Some(ScalarImpl::Int32(1)));
            assert_eq!(
                row.datum_at(1).to_owned_datum(),
                (Some(ScalarImpl::Bool(true)))
            );
            assert_eq!(
                row.datum_at(2).to_owned_datum(),
                (Some(ScalarImpl::Int16(1)))
            );
            assert_eq!(
                row.datum_at(3).to_owned_datum(),
                (Some(ScalarImpl::Int64(12345678)))
            );
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::Float32(1.23.into())))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(1.2345.into())))
            );
            assert_eq!(
                row.datum_at(6).to_owned_datum(),
                (Some(ScalarImpl::Utf8("varchar".into())))
            );
            assert_eq!(
                row.datum_at(7).to_owned_datum(),
                (Some(ScalarImpl::Date("2021-01-01".parse().unwrap())))
            );
            assert_eq!(
                row.datum_at(8).to_owned_datum(),
                (Some(ScalarImpl::Timestamp(
                    "2021-01-01 16:06:12.269".parse().unwrap()
                )))
            );
            assert_eq!(
                row.datum_at(9).to_owned_datum(),
                (Some(ScalarImpl::Decimal("12345.67890".parse().unwrap())))
            );
            assert_eq!(
                row.datum_at(10).to_owned_datum(),
                (Some(ScalarImpl::Interval("P1Y2M3DT0H5M0S".parse().unwrap())))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
            assert_eq!(row.datum_at(1).to_owned_datum(), None);
            assert_eq!(
                row.datum_at(4).to_owned_datum(),
                (Some(ScalarImpl::Float32(12345e+10.into())))
            );
            assert_eq!(
                row.datum_at(5).to_owned_datum(),
                (Some(ScalarImpl::Float64(12345.into())))
            );
            assert_eq!(
                row.datum_at(9).to_owned_datum(),
                (Some(ScalarImpl::Decimal(12345.into())))
            );
            assert_eq!(
                row.datum_at(10).to_owned_datum(),
                (Some(ScalarImpl::Interval("1 day".parse().unwrap())))
            );
        }
    }

    #[tokio::test]
    async fn test_json_parse_object_top_level() {
        test_json_parser(get_payload).await;
    }
    #[ignore]
    #[tokio::test]
    async fn test_json_parse_array_top_level() {
        test_json_parser(get_array_top_level_payload).await;
    }

    #[tokio::test]
    async fn test_json_parser_failed() {
        let descs = vec![
            SourceColumnDesc::simple("v1", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("v2", DataType::Int16, 1.into()),
            SourceColumnDesc::simple("v3", DataType::Varchar, 2.into()),
        ];

        let parser = make_parser(descs);
        let payloads = vec![
            // Parse a correct record.
            br#"{"v1": 1, "v2": 2, "v3": "3"}"#.to_vec(),
            // Parse an incorrect record.
            // `v2` overflowed.
            // ignored the error, and fill None at v2.
            br#"{"v1": 1, "v2": 65536, "v3": "3"}"#.to_vec(),
            // Parse a correct record.
            br#"{"v1": 1, "v2": 2, "v3": "3"}"#.to_vec(),
        ];
        let chunk = parser.parse(payloads).await;

        assert!(chunk.valid());
        assert_eq!(chunk.cardinality(), 3);

        let row_vec = chunk.rows().collect_vec();
        assert_eq!(row_vec[1].1.datum_at(1), None);
    }

    #[tokio::test]
    async fn test_json_parse_struct() {
        let descs = vec![
            ColumnDesc::new_struct(
                "data",
                0,
                "",
                vec![
                    ColumnDesc::new_atomic(DataType::Timestamp, "created_at", 1),
                    ColumnDesc::new_atomic(DataType::Varchar, "id", 2),
                    ColumnDesc::new_atomic(DataType::Varchar, "text", 3),
                    ColumnDesc::new_atomic(DataType::Varchar, "lang", 4),
                ],
            ),
            ColumnDesc::new_struct(
                "author",
                5,
                "",
                vec![
                    ColumnDesc::new_atomic(DataType::Timestamp, "created_at", 6),
                    ColumnDesc::new_atomic(DataType::Varchar, "id", 7),
                    ColumnDesc::new_atomic(DataType::Varchar, "name", 8),
                    ColumnDesc::new_atomic(DataType::Varchar, "username", 9),
                ],
            ),
            ColumnDesc::new_atomic(DataType::Varchar, "I64CastToVarchar", 10),
            ColumnDesc::new_atomic(DataType::Int64, "VarcharCastToI64", 11),
        ]
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();

        let parser = make_parser(descs);
        let payload = br#"
        {
            "data": {
                "created_at": "2022-07-13 20:48:37.07",
                "id": "1732524418112319151",
                "text": "Here man favor ourselves mysteriously most her sigh in straightaway for afterwards.",
                "lang": "English"
            },
            "author": {
                "created_at": "2018-01-29 12:19:11.07",
                "id": "7772634297",
                "name": "Lily Frami yet",
                "username": "Dooley5659"
            },
            "I64CastToVarchar": 1598197865760800768,
            "VarcharCastToI64": "1598197865760800768"
        }
        "#.to_vec();
        let chunk = parser.parse(vec![payload]).await;

        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.into_owned_row().into_inner();

        let expected = vec![
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Timestamp(
                    "2022-07-13 20:48:37.07".parse().unwrap()
                )),
                Some(ScalarImpl::Utf8("1732524418112319151".into())),
                Some(ScalarImpl::Utf8("Here man favor ourselves mysteriously most her sigh in straightaway for afterwards.".into())),
                Some(ScalarImpl::Utf8("English".into())),
            ]))),
            Some(ScalarImpl::Struct(StructValue::new(vec![
                Some(ScalarImpl::Timestamp(
                    "2018-01-29 12:19:11.07".parse().unwrap()
                )),
                Some(ScalarImpl::Utf8("7772634297".into())),
                Some(ScalarImpl::Utf8("Lily Frami yet".into())),
                Some(ScalarImpl::Utf8("Dooley5659".into())),
            ]) )),
            Some(ScalarImpl::Utf8("1598197865760800768".into())),
            Some(ScalarImpl::Int64(1598197865760800768)),
        ];
        assert_eq!(row, expected.into());
    }

    #[tokio::test]
    async fn test_json_parse_struct_from_string() {
        let descs = vec![ColumnDesc::new_struct(
            "struct",
            0,
            "",
            vec![
                ColumnDesc::new_atomic(DataType::Varchar, "varchar", 1),
                ColumnDesc::new_atomic(DataType::Boolean, "boolean", 2),
            ],
        )]
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();

        let parser = make_parser(descs);
        let payload = br#"
        {
            "struct": "{\"varchar\": \"varchar\", \"boolean\": true}"
        }
        "#
        .to_vec();
        let chunk = parser.parse(vec![payload]).await;

        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.into_owned_row().into_inner();

        let expected = vec![Some(ScalarImpl::Struct(StructValue::new(vec![
            Some(ScalarImpl::Utf8("varchar".into())),
            Some(ScalarImpl::Bool(true)),
        ])))];
        assert_eq!(row, expected.into());
    }

    #[cfg(not(madsim))] // Traced test does not work with madsim
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn test_json_parse_struct_missing_field_warning() {
        let descs = vec![ColumnDesc::new_struct(
            "struct",
            0,
            "",
            vec![
                ColumnDesc::new_atomic(DataType::Varchar, "varchar", 1),
                ColumnDesc::new_atomic(DataType::Boolean, "boolean", 2),
            ],
        )]
        .iter()
        .map(SourceColumnDesc::from)
        .collect_vec();

        let parser = make_parser(descs);
        let payload = br#"
        {
            "struct": {
                "varchar": "varchar"
            }
        }
        "#
        .to_vec();
        let chunk = parser.parse(vec![payload]).await;

        let (op, row) = chunk.rows().next().unwrap();
        assert_eq!(op, Op::Insert);
        let row = row.into_owned_row().into_inner();

        let expected = vec![Some(ScalarImpl::Struct(StructValue::new(vec![
            Some(ScalarImpl::Utf8("varchar".into())),
            None,
        ])))];
        assert_eq!(row, expected.into());

        assert!(logs_contain("undefined nested field, padding with `NULL`"));
    }

    #[tokio::test]
    async fn test_json_upsert_parser() {
        let items = [
            (r#"{"a":1}"#, r#"{"a":1,"b":2}"#),
            (r#"{"a":1}"#, r#"{"a":1,"b":3}"#),
            (r#"{"a":2}"#, r#"{"a":2,"b":2}"#),
            (r#"{"a":2}"#, r#""#),
        ]
        .into_iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect_vec();

        let key_column_desc = SourceColumnDesc {
            name: "rw_key".into(),
            data_type: DataType::Bytea,
            column_id: 2.into(),
            fields: vec![],
            column_type: SourceColumnType::Normal,
            is_pk: true,
            is_hidden_addition_col: false,
            additional_column: AdditionalColumn {
                column_type: Some(AdditionalColumnType::Key(AdditionalColumnKey {})),
            },
        };
        let descs = vec![
            SourceColumnDesc::simple("a", DataType::Int32, 0.into()),
            SourceColumnDesc::simple("b", DataType::Int32, 1.into()),
            key_column_desc,
        ];

        let parser = make_upsert_parser(descs);
        let chunk = parser.parse_upsert(items).await;

        // expected chunk
        // +---+---+---+------------------+
        // | + | 1 | 2 | \x7b2261223a317d |
        // | + | 1 | 3 | \x7b2261223a317d |
        // | + | 2 | 2 | \x7b2261223a327d |
        // | - |   |   | \x7b2261223a327d |
        // +---+---+---+------------------+

        let mut rows = chunk.rows();
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
        }

        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(1)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Insert);
            assert_eq!(
                row.datum_at(0).to_owned_datum(),
                (Some(ScalarImpl::Int32(2)))
            );
        }
        {
            let (op, row) = rows.next().unwrap();
            assert_eq!(op, Op::Delete);
            assert_eq!(row.datum_at(0).to_owned_datum(), (None));
        }
    }
}
