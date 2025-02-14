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

use itertools::Itertools;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::data::DataType;
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDesc, ColumnDescVersion};

pub trait ColumnDescTestExt {
    /// Create a [`ColumnDesc`] with the given name and type.
    ///
    /// **Note: Only used for tests.**
    fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self;

    /// Create a [`ColumnDesc`] with `Struct` type.
    ///
    /// **Note: Only used for tests.**
    fn new_struct(name: &str, column_id: i32, type_name: &str, fields: Vec<ColumnDesc>) -> Self;
}

impl ColumnDescTestExt for ColumnDesc {
    fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            column_type: Some(data_type),
            column_id,
            name: name.to_owned(),
            additional_column: Some(AdditionalColumn { column_type: None }),
            version: ColumnDescVersion::LATEST as _,
            ..Default::default()
        }
    }

    fn new_struct(name: &str, column_id: i32, type_name: &str, fields: Vec<ColumnDesc>) -> Self {
        let field_type = fields
            .iter()
            .map(|f| f.column_type.as_ref().unwrap().clone())
            .collect_vec();

        #[allow(deprecated)]
        Self {
            column_type: Some(DataType {
                type_name: TypeName::Struct as i32,
                is_nullable: true,
                field_type,
                field_names: fields.iter().map(|f| f.name.clone()).collect_vec(),
                ..Default::default()
            }),
            column_id,
            name: name.to_owned(),
            type_name: type_name.to_owned(), // deprecated
            field_descs: fields,             // deprecated
            generated_or_default_column: None,
            description: None,
            additional_column_type: 0, // deprecated
            additional_column: Some(AdditionalColumn { column_type: None }),
            version: ColumnDescVersion::LATEST as _,
        }
    }
}
