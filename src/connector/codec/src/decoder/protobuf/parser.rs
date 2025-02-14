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

use std::collections::HashSet;

use anyhow::Context;
use itertools::Itertools;
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage, Value};
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{
    DataType, DatumCow, Decimal, JsonbVal, MapType, MapValue, ScalarImpl, StructType, ToOwnedDatum,
    F32, F64,
};
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};
use thiserror::Error;
use thiserror_ext::Macro;

use crate::decoder::{uncategorized, AccessError, AccessResult};

pub const PROTOBUF_MESSAGES_AS_JSONB: &str = "messages_as_jsonb";

pub fn pb_schema_to_column_descs(
    message_descriptor: &MessageDescriptor,
    messages_as_jsonb: &HashSet<String>,
) -> anyhow::Result<Vec<ColumnDesc>> {
    let mut columns = Vec::with_capacity(message_descriptor.fields().len());
    let mut index = 0;
    let mut parse_trace: Vec<String> = vec![];
    for field in message_descriptor.fields() {
        columns.push(pb_field_to_col_desc(
            &field,
            &mut index,
            &mut parse_trace,
            messages_as_jsonb,
        )?);
    }

    Ok(columns)
}

/// Maps a protobuf field to a RW column.
fn pb_field_to_col_desc(
    field_descriptor: &FieldDescriptor,
    index: &mut i32,
    parse_trace: &mut Vec<String>,
    messages_as_jsonb: &HashSet<String>,
) -> anyhow::Result<ColumnDesc> {
    let field_type = protobuf_type_mapping(field_descriptor, parse_trace, messages_as_jsonb)
        .context("failed to map protobuf type")?;
    if let Kind::Message(m) = field_descriptor.kind()
        && !messages_as_jsonb.contains(m.full_name())
    {
        let field_descs = if let DataType::List { .. } = field_type {
            vec![]
        } else {
            m.fields()
                .map(|f| pb_field_to_col_desc(&f, index, parse_trace, messages_as_jsonb))
                .try_collect()?
        };
        *index += 1;
        #[allow(deprecated)]
        Ok(ColumnDesc {
            column_id: *index,
            name: field_descriptor.name().to_owned(),
            column_type: Some(field_type.to_protobuf()), // deprecated
            field_descs,                                 // deprecated
            type_name: m.full_name().to_owned(),
            generated_or_default_column: None,
            description: None,
            additional_column_type: 0, // deprecated
            additional_column: Some(AdditionalColumn { column_type: None }),
            version: ColumnDescVersion::LATEST as _,
        })
    } else {
        *index += 1;
        Ok(ColumnDesc {
            column_id: *index,
            name: field_descriptor.name().to_owned(),
            column_type: Some(field_type.to_protobuf()),
            additional_column: Some(AdditionalColumn { column_type: None }),
            version: ColumnDescVersion::LATEST as _,
            ..Default::default()
        })
    }
}

#[derive(Error, Debug, Macro)]
#[error("{0}")]
struct ProtobufTypeError(#[message] String);

fn detect_loop_and_push(
    trace: &mut Vec<String>,
    fd: &FieldDescriptor,
) -> std::result::Result<(), ProtobufTypeError> {
    let identifier = format!("{}({})", fd.name(), fd.full_name());
    if trace.iter().any(|s| s == identifier.as_str()) {
        bail_protobuf_type_error!(
            "circular reference detected: {}, conflict with {}, kind {:?}. Adding {:?} to {:?} may help.",
            trace.iter().format("->"),
            identifier,
            fd.kind(),
            fd.kind(),
            PROTOBUF_MESSAGES_AS_JSONB,
        );
    }
    trace.push(identifier);
    Ok(())
}

pub fn from_protobuf_value<'a>(
    field_desc: &FieldDescriptor,
    value: &'a Value,
    type_expected: &DataType,
    messages_as_jsonb: &'a HashSet<String>,
) -> AccessResult<DatumCow<'a>> {
    let kind = field_desc.kind();

    macro_rules! borrowed {
        ($v:expr) => {
            return Ok(DatumCow::Borrowed(Some($v.into())))
        };
    }

    let v: ScalarImpl = match value {
        Value::Bool(v) => ScalarImpl::Bool(*v),
        Value::I32(i) => ScalarImpl::Int32(*i),
        Value::U32(i) => ScalarImpl::Int64(*i as i64),
        Value::I64(i) => ScalarImpl::Int64(*i),
        Value::U64(i) => ScalarImpl::Decimal(Decimal::from(*i)),
        Value::F32(f) => ScalarImpl::Float32(F32::from(*f)),
        Value::F64(f) => ScalarImpl::Float64(F64::from(*f)),
        Value::String(s) => borrowed!(s.as_str()),
        Value::EnumNumber(idx) => {
            let enum_desc = kind.as_enum().ok_or_else(|| AccessError::TypeError {
                expected: "enum".to_owned(),
                got: format!("{kind:?}"),
                value: value.to_string(),
            })?;
            let enum_symbol = enum_desc.get_value(*idx).ok_or_else(|| {
                uncategorized!("unknown enum index {} of enum {:?}", idx, enum_desc)
            })?;
            ScalarImpl::Utf8(enum_symbol.name().into())
        }
        Value::Message(dyn_msg) => {
            if messages_as_jsonb.contains(dyn_msg.descriptor().full_name()) {
                ScalarImpl::Jsonb(JsonbVal::from(
                    serde_json::to_value(dyn_msg).map_err(AccessError::ProtobufAnyToJson)?,
                ))
            } else {
                let desc = dyn_msg.descriptor();
                let DataType::Struct(st) = type_expected else {
                    return Err(AccessError::TypeError {
                        expected: type_expected.to_string(),
                        got: desc.full_name().to_owned(),
                        value: value.to_string(), // Protobuf TEXT
                    });
                };

                let mut rw_values = Vec::with_capacity(st.len());
                for (name, expected_field_type) in st.iter() {
                    let Some(field_desc) = desc.get_field_by_name(name) else {
                        // Field deleted in protobuf. Fallback to SQL NULL (of proper RW type).
                        rw_values.push(None);
                        continue;
                    };
                    let value = dyn_msg.get_field(&field_desc);
                    rw_values.push(
                        from_protobuf_value(
                            &field_desc,
                            &value,
                            expected_field_type,
                            messages_as_jsonb,
                        )?
                        .to_owned_datum(),
                    );
                }
                ScalarImpl::Struct(StructValue::new(rw_values))
            }
        }
        Value::List(values) => {
            let DataType::List(element_type) = type_expected else {
                return Err(AccessError::TypeError {
                    expected: type_expected.to_string(),
                    got: format!("repeated {:?}", kind),
                    value: value.to_string(), // Protobuf TEXT
                });
            };
            let mut builder = element_type.create_array_builder(values.len());
            for value in values {
                builder.append(from_protobuf_value(
                    field_desc,
                    value,
                    element_type,
                    messages_as_jsonb,
                )?);
            }
            ScalarImpl::List(ListValue::new(builder.finish()))
        }
        Value::Bytes(value) => borrowed!(&**value),
        Value::Map(map) => {
            let err = || {
                AccessError::TypeError {
                    expected: type_expected.to_string(),
                    got: format!("{:?}", kind),
                    value: value.to_string(), // Protobuf TEXT
                }
            };

            let DataType::Map(map_type) = type_expected else {
                return Err(err());
            };
            if !field_desc.is_map() {
                return Err(err());
            }
            let map_desc = kind.as_message().ok_or_else(err)?;

            let mut key_builder = map_type.key().create_array_builder(map.len());
            let mut value_builder = map_type.value().create_array_builder(map.len());
            // NOTE: HashMap's iter order is non-deterministic, but MapValue's
            // order matters. We sort by key here to have deterministic order
            // in tests. We might consider removing this, or make all MapValue sorted
            // in the future.
            for (key, value) in map.iter().sorted_by_key(|(k, _v)| *k) {
                key_builder.append(from_protobuf_value(
                    &map_desc.map_entry_key_field(),
                    &key.clone().into(),
                    map_type.key(),
                    messages_as_jsonb,
                )?);
                value_builder.append(from_protobuf_value(
                    &map_desc.map_entry_value_field(),
                    value,
                    map_type.value(),
                    messages_as_jsonb,
                )?);
            }
            let keys = key_builder.finish();
            let values = value_builder.finish();
            ScalarImpl::Map(
                MapValue::try_from_kv(ListValue::new(keys), ListValue::new(values))
                    .map_err(|e| uncategorized!("failed to convert protobuf map: {e}"))?,
            )
        }
    };
    Ok(Some(v).into())
}

/// Maps protobuf type to RW type.
fn protobuf_type_mapping(
    field_descriptor: &FieldDescriptor,
    parse_trace: &mut Vec<String>,
    messages_as_jsonb: &HashSet<String>,
) -> std::result::Result<DataType, ProtobufTypeError> {
    detect_loop_and_push(parse_trace, field_descriptor)?;
    let mut t = match field_descriptor.kind() {
        Kind::Bool => DataType::Boolean,
        Kind::Double => DataType::Float64,
        Kind::Float => DataType::Float32,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => DataType::Int32,
        // Fixed32 represents [0, 2^32 - 1]. It's equal to u32.
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 | Kind::Uint32 | Kind::Fixed32 => {
            DataType::Int64
        }
        Kind::Uint64 | Kind::Fixed64 => DataType::Decimal,
        Kind::String => DataType::Varchar,
        Kind::Message(m) => {
            if messages_as_jsonb.contains(m.full_name()) {
                // Well-Known Types are identified by their full name
                DataType::Jsonb
            } else if m.is_map_entry() {
                // Map is equivalent to `repeated MapFieldEntry map_field = N;`
                debug_assert!(field_descriptor.is_map());
                let key = protobuf_type_mapping(
                    &m.map_entry_key_field(),
                    parse_trace,
                    messages_as_jsonb,
                )?;
                let value = protobuf_type_mapping(
                    &m.map_entry_value_field(),
                    parse_trace,
                    messages_as_jsonb,
                )?;
                _ = parse_trace.pop();
                return Ok(DataType::Map(MapType::from_kv(key, value)));
            } else {
                let fields = m
                    .fields()
                    .map(|f| {
                        Ok((
                            f.name().to_owned(),
                            protobuf_type_mapping(&f, parse_trace, messages_as_jsonb)?,
                        ))
                    })
                    .try_collect::<_, Vec<_>, _>()?;
                StructType::new(fields).into()
            }
        }
        Kind::Enum(_) => DataType::Varchar,
        Kind::Bytes => DataType::Bytea,
    };
    if field_descriptor.cardinality() == Cardinality::Repeated {
        debug_assert!(!field_descriptor.is_map());
        t = DataType::List(Box::new(t))
    }
    _ = parse_trace.pop();
    Ok(t)
}
