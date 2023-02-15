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

use apache_avro::Schema;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::ColumnDesc;

pub(crate) fn avro_field_to_column_desc(
    name: &str,
    schema: &Schema,
    index: &mut i32,
) -> Result<ColumnDesc> {
    let data_type = avro_type_mapping(schema)?;
    match schema {
        Schema::Record {
            name: schema_name,
            fields,
            ..
        } => {
            let vec_column = fields
                .iter()
                .map(|f| avro_field_to_column_desc(&f.name, &f.schema, index))
                .collect::<Result<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                field_descs: vec_column,
                type_name: schema_name.to_string(),
            })
        }
        _ => {
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                ..Default::default()
            })
        }
    }
}

fn avro_type_mapping(schema: &Schema) -> Result<DataType> {
    let data_type = match schema {
        Schema::String => DataType::Varchar,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Boolean => DataType::Boolean,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Date => DataType::Date,
        Schema::TimestampMillis => DataType::Timestamp,
        Schema::TimestampMicros => DataType::Timestamp,
        Schema::Duration => DataType::Interval,
        Schema::Enum { .. } => DataType::Varchar,
        Schema::Decimal { .. } => DataType::Decimal,
        Schema::Record { fields, .. } => {
            let struct_fields = fields
                .iter()
                .map(|f| avro_type_mapping(&f.schema))
                .collect::<Result<Vec<_>>>()?;
            let struct_names = fields.iter().map(|f| f.name.clone()).collect_vec();
            DataType::new_struct(struct_fields, struct_names)
        }
        Schema::Array(item_schema) => {
            let item_type = avro_type_mapping(item_schema.as_ref())?;
            DataType::List {
                datatype: Box::new(item_type),
            }
        }
        Schema::Union(union_schema) => {
            let nested_schema = union_schema
                .variants()
                .iter()
                .find_or_first(|s| **s != Schema::Null)
                .ok_or_else(|| {
                    RwError::from(InternalError(format!(
                        "unsupported type in Avro: {:?}",
                        union_schema
                    )))
                })?;

            avro_type_mapping(nested_schema)?
        }
        _ => {
            return Err(RwError::from(InternalError(format!(
                "unsupported type in Avro: {:?}",
                schema
            ))));
        }
    };

    Ok(data_type)
}
