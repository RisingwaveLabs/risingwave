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

use std::str::FromStr;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::{
    DataType, Datum, DatumCow, Scalar, ScalarImpl, ScalarRefImpl, Timestamp, Timestamptz,
    ToDatumRef, ToOwnedDatum,
};
use risingwave_connector_codec::decoder::AccessExt;
use risingwave_pb::plan_common::additional_column::ColumnType;
use thiserror_ext::AsReport;
use time::serde::iso8601;

use super::{Access, AccessError, AccessResult, ChangeEvent, ChangeEventOperation};
use crate::parser::debezium::schema_change::{SchemaChangeEnvelope, TableSchemaChange};
use crate::parser::schema_change::TableChangeType;
use crate::parser::TransactionControl;
use crate::source::cdc::build_cdc_table_id;
use crate::source::cdc::external::mysql::{
    mysql_type_to_rw_type, timestamp_val_to_timestamptz, type_name_to_mysql_type,
};
use crate::source::{ConnectorProperties, SourceColumnDesc};

// Example of Debezium JSON value:
// {
//     "payload":
//     {
//         "before": null,
//         "after":
//         {
//             "O_ORDERKEY": 5,
//             "O_CUSTKEY": 44485,
//             "O_ORDERSTATUS": "F",
//             "O_TOTALPRICE": "144659.20",
//             "O_ORDERDATE": "1994-07-30"
//         },
//         "source":
//         {
//             "version": "1.9.7.Final",
//             "connector": "mysql",
//             "name": "RW_CDC_1002",
//             "ts_ms": 1695277757000,
//             "db": "mydb",
//             "sequence": null,
//             "table": "orders",
//             "server_id": 0,
//             "gtid": null,
//             "file": "binlog.000008",
//             "pos": 3693,
//             "row": 0,
//         },
//         "op": "r",
//         "ts_ms": 1695277757017,
//         "transaction": null
//     }
// }
pub struct DebeziumChangeEvent<A> {
    value_accessor: Option<A>,
    key_accessor: Option<A>,
    is_mongodb: bool,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";

const UPSTREAM_DDL: &str = "ddl";
const SOURCE: &str = "source";
const SOURCE_TS_MS: &str = "ts_ms";
const SOURCE_DB: &str = "db";
const SOURCE_SCHEMA: &str = "schema";
const SOURCE_TABLE: &str = "table";
const SOURCE_COLLECTION: &str = "collection";

const OP: &str = "op";
pub const TRANSACTION_STATUS: &str = "status";
pub const TRANSACTION_ID: &str = "id";

pub const TABLE_CHANGES: &str = "tableChanges";

pub const DEBEZIUM_READ_OP: &str = "r";
pub const DEBEZIUM_CREATE_OP: &str = "c";
pub const DEBEZIUM_UPDATE_OP: &str = "u";
pub const DEBEZIUM_DELETE_OP: &str = "d";

pub const DEBEZIUM_TRANSACTION_STATUS_BEGIN: &str = "BEGIN";
pub const DEBEZIUM_TRANSACTION_STATUS_COMMIT: &str = "END";

pub fn parse_transaction_meta(
    accessor: &impl Access,
    connector_props: &ConnectorProperties,
) -> AccessResult<TransactionControl> {
    if let (Some(ScalarRefImpl::Utf8(status)), Some(ScalarRefImpl::Utf8(id))) = (
        accessor
            .access(&[TRANSACTION_STATUS], &DataType::Varchar)?
            .to_datum_ref(),
        accessor
            .access(&[TRANSACTION_ID], &DataType::Varchar)?
            .to_datum_ref(),
    ) {
        // The id field has different meanings for different databases:
        // PG: txID:LSN
        // MySQL: source_id:transaction_id (e.g. 3E11FA47-71CA-11E1-9E33-C80AA9429562:23)
        // SQL Server: commit_lsn (e.g. 00000027:00000ac0:0002)
        match status {
            DEBEZIUM_TRANSACTION_STATUS_BEGIN => match *connector_props {
                ConnectorProperties::PostgresCdc(_) => {
                    let (tx_id, _) = id.split_once(':').unwrap();
                    return Ok(TransactionControl::Begin { id: tx_id.into() });
                }
                ConnectorProperties::MysqlCdc(_) => {
                    return Ok(TransactionControl::Begin { id: id.into() });
                }
                ConnectorProperties::SqlServerCdc(_) => {
                    return Ok(TransactionControl::Begin { id: id.into() });
                }
                _ => {}
            },
            DEBEZIUM_TRANSACTION_STATUS_COMMIT => match *connector_props {
                ConnectorProperties::PostgresCdc(_) => {
                    let (tx_id, _) = id.split_once(':').unwrap();
                    return Ok(TransactionControl::Commit { id: tx_id.into() });
                }
                ConnectorProperties::MysqlCdc(_) => {
                    return Ok(TransactionControl::Commit { id: id.into() });
                }
                ConnectorProperties::SqlServerCdc(_) => {
                    return Ok(TransactionControl::Commit { id: id.into() });
                }
                _ => {}
            },
            _ => {}
        }
    }

    Err(AccessError::Undefined {
        name: "transaction status".into(),
        path: TRANSACTION_STATUS.into(),
    })
}

macro_rules! jsonb_access_field {
    ($col:expr, $field:expr, $as_type:tt) => {
        $crate::paste! {
            $col.access_object_field($field).unwrap().[<as_ $as_type>]().unwrap()
        }
    };
}

/// Parse the schema change message from Debezium.
/// The layout of MySQL schema change message can refer to
/// <https://debezium.io/documentation/reference/2.6/connectors/mysql.html#mysql-schema-change-topic>
pub fn parse_schema_change(
    accessor: &impl Access,
    source_id: u32,
    connector_props: &ConnectorProperties,
) -> AccessResult<SchemaChangeEnvelope> {
    let mut schema_changes = vec![];

    let upstream_ddl: String = accessor
        .access(&[UPSTREAM_DDL], &DataType::Varchar)?
        .to_owned_datum()
        .unwrap()
        .as_utf8()
        .to_string();

    if let Some(ScalarRefImpl::List(table_changes)) = accessor
        .access(&[TABLE_CHANGES], &DataType::List(Box::new(DataType::Jsonb)))?
        .to_datum_ref()
    {
        for datum in table_changes.iter() {
            let jsonb = match datum {
                Some(ScalarRefImpl::Jsonb(jsonb)) => jsonb,
                _ => unreachable!(""),
            };

            let id = jsonb_access_field!(jsonb, "id", string);
            let ty = jsonb_access_field!(jsonb, "type", string);
            let ddl_type: TableChangeType = ty.as_str().into();
            if matches!(ddl_type, TableChangeType::Create | TableChangeType::Drop) {
                tracing::debug!("skip table schema change for create/drop command");
                continue;
            }

            let mut column_descs: Vec<ColumnDesc> = vec![];
            if let Some(table) = jsonb.access_object_field("table")
                && let Some(columns) = table.access_object_field("columns")
            {
                for col in columns.array_elements().unwrap() {
                    let name = jsonb_access_field!(col, "name", string);
                    let type_name = jsonb_access_field!(col, "typeName", string);

                    let data_type = match *connector_props {
                        ConnectorProperties::PostgresCdc(_) => {
                            DataType::from_str(type_name.as_str()).map_err(|err| {
                                tracing::warn!(error=%err.as_report(), "unsupported postgres type in schema change message");
                                AccessError::UnsupportedType {
                                    ty: type_name.clone(),
                                }
                            })?
                        }
                        ConnectorProperties::MysqlCdc(_) => {
                            let ty = type_name_to_mysql_type(type_name.as_str());
                            match ty {
                                Some(ty) => mysql_type_to_rw_type(&ty).map_err(|err| {
                                    tracing::warn!(error=%err.as_report(), "unsupported mysql type in schema change message");
                                    AccessError::UnsupportedType {
                                        ty: type_name.clone(),
                                    }
                                })?,
                                None => {
                                    Err(AccessError::UnsupportedType { ty: type_name })?
                                }
                            }
                        }
                        _ => {
                            unreachable!()
                        }
                    };

                    // handle default value expression, currently we only support constant expression
                    let column_desc = match col.access_object_field("defaultValueExpression") {
                        Some(default_val_expr_str) if !default_val_expr_str.is_jsonb_null() => {
                            let value_text: Option<String>;
                            let default_val_expr_str = default_val_expr_str.as_str().unwrap();
                            match *connector_props {
                                ConnectorProperties::PostgresCdc(_) => {
                                    // default value of non-number data type will be stored as
                                    // "'value'::type"
                                    match default_val_expr_str
                                        .split("::")
                                        .map(|s| s.trim_matches('\''))
                                        .next()
                                    {
                                        None => {
                                            value_text = None;
                                        }
                                        Some(val_text) => {
                                            value_text = Some(val_text.to_owned());
                                        }
                                    }
                                }
                                ConnectorProperties::MysqlCdc(_) => {
                                    // mysql timestamp is mapped to timestamptz, we use UTC timezone to
                                    // interpret its value
                                    if data_type == DataType::Timestamptz {
                                        value_text = Some(timestamp_val_to_timestamptz(default_val_expr_str).map_err(|err| {
                                            tracing::error!(target: "auto_schema_change", error=%err.as_report(), "failed to convert timestamp value to timestamptz");
                                            AccessError::TypeError {
                                                expected: "timestamp in YYYY-MM-DD HH:MM:SS".into(),
                                                got: data_type.to_string(),
                                                value: default_val_expr_str.to_owned(),
                                            }
                                        })?);
                                    } else {
                                        value_text = Some(default_val_expr_str.to_owned());
                                    }
                                }
                                _ => {
                                    unreachable!("connector doesn't support schema change")
                                }
                            }

                            let snapshot_value: Datum = if let Some(value_text) = value_text {
                                Some(ScalarImpl::from_text(value_text.as_str(), &data_type).map_err(
                                    |err| {
                                        tracing::error!(target: "auto_schema_change", error=%err.as_report(), "failed to parse default value expression");
                                        AccessError::TypeError {
                                            expected: "constant expression".into(),
                                            got: data_type.to_string(),
                                            value: value_text,
                                        }
                                    },
                                )?)
                            } else {
                                None
                            };

                            if snapshot_value.is_none() {
                                tracing::warn!(target: "auto_schema_change", "failed to parse default value expression: {}", default_val_expr_str);
                                ColumnDesc::named(name, ColumnId::placeholder(), data_type)
                            } else {
                                ColumnDesc::named_with_default_value(
                                    name,
                                    ColumnId::placeholder(),
                                    data_type,
                                    snapshot_value,
                                )
                            }
                        }
                        _ => ColumnDesc::named(name, ColumnId::placeholder(), data_type),
                    };
                    column_descs.push(column_desc);
                }
            }

            // concatenate the source_id to the cdc_table_id
            let cdc_table_id = build_cdc_table_id(source_id, id.replace('"', "").as_str());
            schema_changes.push(TableSchemaChange {
                cdc_table_id,
                columns: column_descs
                    .into_iter()
                    .map(|column_desc| ColumnCatalog {
                        column_desc,
                        is_hidden: false,
                    })
                    .collect_vec(),
                change_type: ty.as_str().into(),
                upstream_ddl: upstream_ddl.clone(),
            });
        }

        Ok(SchemaChangeEnvelope {
            table_changes: schema_changes,
        })
    } else {
        Err(AccessError::Undefined {
            name: "table schema change".into(),
            path: TABLE_CHANGES.into(),
        })
    }
}

impl<A> DebeziumChangeEvent<A>
where
    A: Access,
{
    /// Panic: one of the `key_accessor` or `value_accessor` must be provided.
    pub fn new(key_accessor: Option<A>, value_accessor: Option<A>) -> Self {
        assert!(key_accessor.is_some() || value_accessor.is_some());
        Self {
            value_accessor,
            key_accessor,
            is_mongodb: false,
        }
    }

    pub fn new_mongodb_event(key_accessor: Option<A>, value_accessor: Option<A>) -> Self {
        assert!(key_accessor.is_some() || value_accessor.is_some());
        Self {
            value_accessor,
            key_accessor,
            is_mongodb: true,
        }
    }

    /// Returns the transaction metadata if exists.
    ///
    /// See the [doc](https://debezium.io/documentation/reference/2.3/connectors/postgresql.html#postgresql-transaction-metadata) of Debezium for more details.
    pub(crate) fn transaction_control(
        &self,
        connector_props: &ConnectorProperties,
    ) -> Option<TransactionControl> {
        // Ignore if `value_accessor` is not provided or there's any error when
        // trying to parse the transaction metadata.
        self.value_accessor
            .as_ref()
            .and_then(|accessor| parse_transaction_meta(accessor, connector_props).ok())
    }
}

impl<A> ChangeEvent for DebeziumChangeEvent<A>
where
    A: Access,
{
    fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult<DatumCow<'_>> {
        match self.op()? {
            ChangeEventOperation::Delete => {
                // For delete events of MongoDB, the "before" and "after" field both are null in the value,
                // we need to extract the _id field from the key.
                if self.is_mongodb && desc.name == "_id" {
                    return self
                        .key_accessor
                        .as_ref()
                        .expect("key_accessor must be provided for delete operation")
                        .access(&[&desc.name], &desc.data_type);
                }

                if let Some(va) = self.value_accessor.as_ref() {
                    va.access(&[BEFORE, &desc.name], &desc.data_type)
                } else {
                    self.key_accessor
                        .as_ref()
                        .unwrap()
                        .access(&[&desc.name], &desc.data_type)
                }
            }

            // value should not be None.
            ChangeEventOperation::Upsert => {
                // For upsert operation, if desc is an additional column, access field in the `SOURCE` field.
                desc.additional_column.column_type.as_ref().map_or_else(
                    || {
                        self.value_accessor
                            .as_ref()
                            .expect("value_accessor must be provided for upsert operation")
                            .access(&[AFTER, &desc.name], &desc.data_type)
                    },
                    |additional_column_type| {
                        match *additional_column_type {
                            ColumnType::Timestamp(_) => {
                                // access payload.source.ts_ms
                                let ts_ms = self
                                    .value_accessor
                                    .as_ref()
                                    .expect("value_accessor must be provided for upsert operation")
                                    .access_owned(&[SOURCE, SOURCE_TS_MS], &DataType::Int64)?;
                                Ok(DatumCow::Owned(ts_ms.map(|scalar| {
                                    Timestamptz::from_millis(scalar.into_int64())
                                        .expect("source.ts_ms must in millisecond")
                                        .to_scalar_value()
                                })))
                            }
                            ColumnType::DatabaseName(_) => self
                                .value_accessor
                                .as_ref()
                                .expect("value_accessor must be provided for upsert operation")
                                .access(&[SOURCE, SOURCE_DB], &desc.data_type),
                            ColumnType::SchemaName(_) => self
                                .value_accessor
                                .as_ref()
                                .expect("value_accessor must be provided for upsert operation")
                                .access(&[SOURCE, SOURCE_SCHEMA], &desc.data_type),
                            ColumnType::TableName(_) => self
                                .value_accessor
                                .as_ref()
                                .expect("value_accessor must be provided for upsert operation")
                                .access(&[SOURCE, SOURCE_TABLE], &desc.data_type),
                            ColumnType::CollectionName(_) => self
                                .value_accessor
                                .as_ref()
                                .expect("value_accessor must be provided for upsert operation")
                                .access(&[SOURCE, SOURCE_COLLECTION], &desc.data_type),
                            _ => Err(AccessError::UnsupportedAdditionalColumn {
                                name: desc.name.clone(),
                            }),
                        }
                    },
                )
            }
        }
    }

    fn op(&self) -> Result<ChangeEventOperation, AccessError> {
        if let Some(accessor) = &self.value_accessor {
            if let Some(ScalarRefImpl::Utf8(op)) =
                accessor.access(&[OP], &DataType::Varchar)?.to_datum_ref()
            {
                match op {
                    DEBEZIUM_READ_OP | DEBEZIUM_CREATE_OP | DEBEZIUM_UPDATE_OP => {
                        return Ok(ChangeEventOperation::Upsert);
                    }
                    DEBEZIUM_DELETE_OP => return Ok(ChangeEventOperation::Delete),
                    _ => (),
                }
            }
            Err(super::AccessError::Undefined {
                name: "op".into(),
                path: Default::default(),
            })
        } else {
            Ok(ChangeEventOperation::Delete)
        }
    }
}

pub struct MongoJsonAccess<A> {
    accessor: A,
    strong_schema: bool,
}

pub fn extract_bson_id(id_type: &DataType, bson_doc: &serde_json::Value) -> AccessResult {
    let id_field = if let Some(value) = bson_doc.get("_id") {
        value
    } else {
        bson_doc
    };

    let type_error = || AccessError::TypeError {
        expected: id_type.to_string(),
        got: match id_field {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
        .to_owned(),
        value: id_field.to_string(),
    };

    let id: Datum = match id_type {
        DataType::Jsonb => ScalarImpl::Jsonb(id_field.clone().into()).into(),
        DataType::Varchar => match id_field {
            serde_json::Value::String(s) => Some(ScalarImpl::Utf8(s.clone().into())),
            serde_json::Value::Object(obj) if obj.contains_key("$oid") => Some(ScalarImpl::Utf8(
                obj["$oid"].as_str().to_owned().unwrap_or_default().into(),
            )),
            _ => return Err(type_error()),
        },
        DataType::Int32 => {
            if let serde_json::Value::Object(ref obj) = id_field
                && obj.contains_key("$numberInt")
            {
                let int_str = obj["$numberInt"].as_str().unwrap_or_default();
                Some(ScalarImpl::Int32(int_str.parse().unwrap_or_default()))
            } else {
                return Err(type_error());
            }
        }
        DataType::Int64 => {
            if let serde_json::Value::Object(ref obj) = id_field
                && obj.contains_key("$numberLong")
            {
                let int_str = obj["$numberLong"].as_str().unwrap_or_default();
                Some(ScalarImpl::Int64(int_str.parse().unwrap_or_default()))
            } else {
                return Err(type_error());
            }
        }
        _ => unreachable!("DebeziumMongoJsonParser::new must ensure _id column datatypes."),
    };
    Ok(id)
}

// similar to extract the "_id" field from the message payload
pub fn extract_bson_field(
    type_expected: &DataType,
    bson_doc: &serde_json::Value,
    field: &str,
) -> AccessResult {
    let datum = if let Some(value) = bson_doc.get(field) {
        value
    } else {
        bson_doc
    };

    let type_error = || AccessError::TypeError {
        expected: type_expected.to_string(),
        got: match bson_doc {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
        .to_owned(),
        value: datum.to_string(),
    };

    let field_datum: Datum = match type_expected {
        DataType::Jsonb => ScalarImpl::Jsonb(datum.clone().into()).into(),
        DataType::Varchar => match datum {
            serde_json::Value::String(s) => Some(ScalarImpl::Utf8(s.clone().into())),
            serde_json::Value::Object(obj) if obj.contains_key("$oid") && field == "_id" => Some(
                ScalarImpl::Utf8(obj["$oid"].as_str().to_owned().unwrap_or_default().into()),
            ),
            _ => return Err(type_error()),
        },
        DataType::Int32 => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$numberInt")
            {
                let int_str = obj["$numberInt"].as_str().unwrap_or_default();
                Some(ScalarImpl::Int32(int_str.parse().unwrap_or_default()))
            } else {
                return Err(type_error());
            }
        }
        DataType::Int64 => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$numberLong")
            {
                let int_str = obj["$numberLong"].as_str().unwrap_or_default();
                Some(ScalarImpl::Int64(int_str.parse().unwrap_or_default()))
            } else {
                return Err(type_error());
            }
        }

        DataType::Float64 => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$numberDouble")
            {
                let float_str = obj["$numberDouble"].as_str().unwrap_or_default();
                let num = match float_str {
                    "Infinity" => f64::INFINITY,
                    "-Infinity" => f64::NEG_INFINITY,
                    "Nan" => f64::NAN,
                    fstr => fstr.parse().unwrap_or_default(),
                };
                Some(ScalarImpl::Float64(num.into()))
            } else {
                return Err(type_error());
            }
        }

        DataType::Float32 => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$numberDouble")
            {
                let float_str = obj["$numberDouble"].as_str().unwrap_or_default();
                let num = match float_str {
                    "Infinity" => f32::INFINITY,
                    "-Infinity" => f32::NEG_INFINITY,
                    "Nan" => f32::NAN,
                    fstr => fstr.parse().unwrap_or_default(),
                };
                Some(ScalarImpl::Float32(num.into()))
            } else {
                return Err(type_error());
            }
        }

        DataType::Date | DataType::Timestamp | DataType::Timestamptz => {
            if let serde_json::Value::Object(ref mp) = datum {
                if mp.contains_key("$timestamp") {
                    // todo: fallback to extract date on error
                    bson_extract_timestamp(datum, field, type_expected)?
                } else if mp.contains_key("$date") {
                    bson_extract_date(datum, field, type_expected)?
                } else {
                    return Err(type_error());
                }
            } else {
                return Err(type_error());
            }
        }
        DataType::Decimal => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$numberDecimal")
            {
                let number = obj["$numberDecimal"].as_str().unwrap_or_default();
                let dec = risingwave_common::types::Decimal::from_str(number).unwrap_or_default();
                Some(ScalarImpl::Decimal(dec))
            } else {
                return Err(type_error());
            }
        }

        DataType::Bytea => {
            if let serde_json::Value::Object(ref obj) = datum
                && obj.contains_key("$binary")
                && obj.contains_key("$type")
            {
                use base64::Engine;

                let binary = obj["$binary"].as_str().unwrap_or_default();
                let _type_str = obj["$type"].as_str().unwrap_or_default();
                let bytes = base64::prelude::BASE64_STANDARD
                    .decode(binary)
                    .unwrap_or_default();
                let bytea = ScalarImpl::Bytea(bytes.into());
                Some(bytea)
            } else {
                return Err(type_error());
            }
        }

        _ => unreachable!("DebeziumMongoJsonParser::new must ensure {field} column datatypes."),
    };
    Ok(field_datum)
}

fn bson_extract_date(
    bson_doc: &serde_json::Value,
    field: &str,
    type_expected: &DataType,
) -> AccessResult {
    // according to mongodb extended json v2
    // the date could be:
    //
    // the timestamp type could be:
    //
    // both Canonical and Relaxed format:
    // {"$timestamp": {"t": 1630454400, "i": 1}}
    //
    // Canonical: {"$date": {"$numberLong": "1630454400000"}}
    // date is encoded as number of milliseconds since the Unix epoch
    //
    // Relaxed: {"$date": "2021-09-01T00:00:00.000Z"}
    // date is encoded as ISO8601 string

    let datum = &bson_doc["$date"];

    let type_error = || AccessError::TypeError {
        expected: type_expected.to_string(),
        got: match bson_doc {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
        .to_owned(),
        value: datum.to_string(),
    };

    let res = if let serde_json::Value::String(ref _iso8601_str) = datum {
        // relaxed format
        let Ok(parsed) = iso8601::deserialize(datum) else {
            return Err(type_error());
        };

        let (millis, nanos) = (parsed.unix_timestamp(), parsed.nanosecond());
        let chrono_datetime = chrono::DateTime::from_timestamp(millis, nanos).unwrap_or_default();
        match type_expected {
            DataType::Date => {
                let naive = chrono_datetime.naive_local();
                let dt = naive.date();
                Some(ScalarImpl::Date(dt.into()))
            }
            DataType::Time => {
                let naive = chrono_datetime.naive_local();
                let dt = naive.time();
                Some(ScalarImpl::Time(dt.into()))
            }
            DataType::Timestamp => {
                let naive = chrono_datetime.naive_local();
                let dt = Timestamp::from(naive);
                Some(ScalarImpl::Timestamp(dt))
            }
            DataType::Timestamptz => {
                let dt = chrono_datetime.into();
                Some(ScalarImpl::Timestamptz(dt))
            }
            _ => unreachable!("DebeziumMongoJsonParser::new must ensure {field} column datatypes."),
        }
    } else if let serde_json::Value::Object(ref obj) = datum
        && obj.contains_key("date")
    {
        // strict format
        let millis = obj["$numberLong"]
            .as_str()
            .unwrap_or_default()
            .parse()
            .unwrap_or_default();
        let datetime = chrono::DateTime::from_timestamp_millis(millis).unwrap_or_default();

        match type_expected {
            DataType::Date => {
                let naive = datetime.naive_local();
                let dt = naive.date();
                Some(ScalarImpl::Date(dt.into()))
            }
            DataType::Time => {
                let naive = datetime.naive_local();
                let dt = naive.time();
                Some(ScalarImpl::Time(dt.into()))
            }
            DataType::Timestamp => {
                let naive = datetime.naive_local();
                let dt = Timestamp::from(naive);
                Some(ScalarImpl::Timestamp(dt))
            }
            DataType::Timestamptz => {
                let dt = datetime.into();
                Some(ScalarImpl::Timestamptz(dt))
            }
            _ => unreachable!("DebeziumMongoJsonParser::new must ensure {field} column datatypes."),
        }
    } else {
        return Err(type_error());
    };
    Ok(res)
}

fn bson_extract_timestamp(
    bson_doc: &serde_json::Value,
    field: &str,
    type_expected: &DataType,
) -> AccessResult {
    // according to mongodb extended json v2
    // the date could be:
    //
    // the timestamp type could be:
    //
    // both Canonical and Relaxed format:
    // {"$timestamp": {"t": 1630454400, "i": 1}}
    // t is the number of seconds since the Unix epoch
    //
    // Canonical: {"$date": {"$numberLong": "1630454400000"}}
    // date is encoded as number of milliseconds since the Unix epoch
    //
    // Relaxed: {"$date": "2021-09-01T00:00:00.000Z"}
    // date is encoded as ISO8601 string

    let Some(obj) = bson_doc["$timestamp"].as_object() else {
        return Err(AccessError::TypeError {
            expected: "timestamp".into(),
            got: bson_doc.to_string(),
            value: field.to_string(),
        });
    };

    let since_epoch = obj["t"].as_i64().unwrap_or_default();
    let nanos = obj["i"].as_i64().unwrap_or_default() as u32;

    let chrono_datetime = chrono::DateTime::from_timestamp(since_epoch, nanos).unwrap_or_default();

    let res = match type_expected {
        DataType::Date => {
            let naive = chrono_datetime.naive_local();
            let dt = naive.date();
            Some(ScalarImpl::Date(dt.into()))
        }
        DataType::Time => {
            let naive = chrono_datetime.naive_local();
            let dt = naive.time();
            Some(ScalarImpl::Time(dt.into()))
        }
        DataType::Timestamp => {
            let naive = chrono_datetime.naive_local();
            let dt = Timestamp::from(naive);
            Some(ScalarImpl::Timestamp(dt))
        }
        DataType::Timestamptz => {
            let dt = chrono_datetime.into();
            Some(ScalarImpl::Timestamptz(dt))
        }
        _ => unreachable!("DebeziumMongoJsonParser::new must ensure {field} column datatypes."),
    };

    Ok(res)
}

impl<A> MongoJsonAccess<A> {
    pub fn new(accessor: A, strong_schema: bool) -> Self {
        Self {
            accessor,
            strong_schema,
        }
    }
}

impl<A> Access for MongoJsonAccess<A>
where
    A: Access,
{
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        match path {
            ["after" | "before", "payload"] if !self.strong_schema => {
                self.access(&[path[0]], &DataType::Jsonb)
            }

            ["after" | "before", "_id"] => {
                let payload = self.access_owned(&[path[0]], &DataType::Jsonb)?;
                if let Some(ScalarImpl::Jsonb(bson_doc)) = payload {
                    return Ok(extract_bson_id(type_expected, &bson_doc.take())?.into());
                } else {
                    // fail to extract the "_id" field from the message payload
                    Err(AccessError::Undefined {
                        name: "_id".to_string(),
                        path: path[0].to_owned(),
                    })?
                }
            }

            ["after" | "before", field] if self.strong_schema => {
                let payload = self.access_owned(&[path[0]], &DataType::Jsonb)?;
                if let Some(ScalarImpl::Jsonb(bson_doc)) = payload {
                    return Ok(extract_bson_field(type_expected, &bson_doc.take(), field)?.into());
                } else {
                    // fail to extract the "_id" field from the message payload
                    Err(AccessError::Undefined {
                        name: field.to_string(),
                        path: path[0].to_owned(),
                    })?
                }
            }

            // To handle a DELETE message, we need to extract the "_id" field from the message key, because it is not in the payload.
            // In addition, the "_id" field is named as "id" in the key. An example of message key:
            // {"schema":null,"payload":{"id":"{\"$oid\": \"65bc9fb6c485f419a7a877fe\"}"}}
            ["_id"] => {
                let ret = self.accessor.access(path, type_expected);
                if matches!(ret, Err(AccessError::Undefined { .. })) {
                    let id_bson = self.accessor.access_owned(&["id"], &DataType::Jsonb)?;
                    if let Some(ScalarImpl::Jsonb(bson_doc)) = id_bson {
                        Ok(extract_bson_id(type_expected, &bson_doc.take())?.into())
                    } else {
                        // fail to extract the "_id" field from the message key
                        Err(AccessError::Undefined {
                            name: "_id".to_owned(),
                            path: "id".to_owned(),
                        })?
                    }
                } else {
                    ret
                }
            }
            _ => self.accessor.access(path, type_expected),
        }
    }
}
