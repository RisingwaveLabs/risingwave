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

use risingwave_common::types::{DataType, Datum, ScalarImpl};

use super::{Access, AccessError, AccessResult, ChangeEvent, ChangeEventOperation};
use crate::parser::unified::uncategorized;
use crate::parser::TransactionControl;
use crate::source::{ConnectorProperties, SourceColumnDesc};

pub struct DebeziumChangeEvent<A> {
    value_accessor: Option<A>,
    key_accessor: Option<A>,
}

const BEFORE: &str = "before";
const AFTER: &str = "after";
const OP: &str = "op";
pub const TRANSACTION_STATUS: &str = "status";
pub const TRANSACTION_ID: &str = "id";

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
    if let (Some(ScalarImpl::Utf8(status)), Some(ScalarImpl::Utf8(id))) = (
        accessor.access(&[TRANSACTION_STATUS], Some(&DataType::Varchar))?,
        accessor.access(&[TRANSACTION_ID], Some(&DataType::Varchar))?,
    ) {
        // The id field has different meanings for different databases:
        // PG: txID:LSN
        // MySQL: source_id:transaction_id (e.g. 3E11FA47-71CA-11E1-9E33-C80AA9429562:23)
        match status.as_ref() {
            DEBEZIUM_TRANSACTION_STATUS_BEGIN => match *connector_props {
                ConnectorProperties::PostgresCdc(_) => {
                    let (tx_id, _) = id.split_once(':').unwrap();
                    return Ok(TransactionControl::Begin { id: tx_id.into() });
                }
                ConnectorProperties::MysqlCdc(_) => return Ok(TransactionControl::Begin { id }),
                _ => {}
            },
            DEBEZIUM_TRANSACTION_STATUS_COMMIT => match *connector_props {
                ConnectorProperties::PostgresCdc(_) => {
                    let (tx_id, _) = id.split_once(':').unwrap();
                    return Ok(TransactionControl::Commit { id: tx_id.into() });
                }
                ConnectorProperties::MysqlCdc(_) => return Ok(TransactionControl::Commit { id }),
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

impl<A> DebeziumChangeEvent<A>
where
    A: Access,
{
    pub fn with_value(value_accessor: A) -> Self {
        Self::new(None, Some(value_accessor))
    }

    pub fn with_key(key_accessor: A) -> Self {
        Self::new(Some(key_accessor), None)
    }

    /// Panic: one of the `key_accessor` or `value_accessor` must be provided.
    pub fn new(key_accessor: Option<A>, value_accessor: Option<A>) -> Self {
        assert!(key_accessor.is_some() || value_accessor.is_some());
        Self {
            value_accessor,
            key_accessor,
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
    fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult {
        match self.op()? {
            ChangeEventOperation::Delete => {
                if let Some(va) = self.value_accessor.as_ref() {
                    va.access(&[BEFORE, &desc.name], Some(&desc.data_type))
                } else {
                    self.key_accessor
                        .as_ref()
                        .unwrap()
                        .access(&[&desc.name], Some(&desc.data_type))
                }
            }

            // value should not be None.
            ChangeEventOperation::Upsert => self
                .value_accessor
                .as_ref()
                .unwrap()
                .access(&[AFTER, &desc.name], Some(&desc.data_type)),
        }
    }

    fn op(&self) -> Result<ChangeEventOperation, AccessError> {
        if let Some(accessor) = &self.value_accessor {
            if let Some(ScalarImpl::Utf8(op)) = accessor.access(&[OP], Some(&DataType::Varchar))? {
                match op.as_ref() {
                    DEBEZIUM_READ_OP | DEBEZIUM_CREATE_OP | DEBEZIUM_UPDATE_OP => {
                        return Ok(ChangeEventOperation::Upsert)
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

pub struct MongoProjection<A> {
    accessor: A,
}

pub fn extract_bson_id(id_type: &DataType, bson_doc: &serde_json::Value) -> AccessResult {
    let id_field = bson_doc
        .get("_id")
        .ok_or_else(|| uncategorized!("Debezium Mongo requires document has a `_id` field"))?;

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
impl<A> MongoProjection<A> {
    pub fn new(accessor: A) -> Self {
        Self { accessor }
    }
}

impl<A> Access for MongoProjection<A>
where
    A: Access,
{
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> super::AccessResult {
        match path {
            ["after" | "before", "_id"] => {
                let payload = self.access(&[path[0]], Some(&DataType::Jsonb))?;
                if let Some(ScalarImpl::Jsonb(bson_doc)) = payload {
                    Ok(extract_bson_id(
                        type_expected.unwrap_or(&DataType::Jsonb),
                        &bson_doc.take(),
                    )?)
                } else {
                    unreachable!("the result of access must match the type_expected")
                }
            }
            ["after" | "before", "payload"] => self.access(&[path[0]], Some(&DataType::Jsonb)),
            _ => self.accessor.access(path, type_expected),
        }
    }
}
