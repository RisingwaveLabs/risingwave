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

use std::borrow::Cow;

use itertools::Itertools;
use risingwave_common::types::Datum;
use risingwave_pb::expr::expr_node::{RexNode, Type as ExprType};
use risingwave_pb::expr::ExprNode;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{
    AdditionalColumn, ColumnDescVersion, DefaultColumnDesc, PbColumnCatalog, PbColumnDesc,
};

use super::schema::FieldLike;
use super::{
    iceberg_sequence_num_column_desc, row_id_column_desc, rw_timestamp_column_desc,
    USER_COLUMN_ID_OFFSET,
};
use crate::catalog::{cdc_table_name_column_desc, offset_column_desc, Field, ROW_ID_COLUMN_ID};
use crate::types::{DataType, StructType};
use crate::util::value_encoding::DatumToProtoExt;

/// Column ID is the unique identifier of a column in a table. Different from table ID, column ID is
/// not globally unique.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(i32);

impl std::fmt::Debug for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

impl ColumnId {
    pub const fn new(column_id: i32) -> Self {
        Self(column_id)
    }

    /// Sometimes the id field is filled later, we use this value for better debugging.
    pub const fn placeholder() -> Self {
        Self(i32::MAX - 1)
    }

    pub const fn first_user_column() -> Self {
        Self(USER_COLUMN_ID_OFFSET)
    }
}

impl ColumnId {
    pub const fn get_id(&self) -> i32 {
        self.0
    }

    /// Returns the subsequent column id.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn apply_delta_if_not_row_id(&mut self, delta: i32) {
        if self.0 != ROW_ID_COLUMN_ID.get_id() {
            self.0 += delta;
        }
    }
}

impl From<i32> for ColumnId {
    fn from(column_id: i32) -> Self {
        Self::new(column_id)
    }
}
impl From<&i32> for ColumnId {
    fn from(column_id: &i32) -> Self {
        Self::new(*column_id)
    }
}

impl From<ColumnId> for i32 {
    fn from(id: ColumnId) -> i32 {
        id.0
    }
}

impl From<&ColumnId> for i32 {
    fn from(id: &ColumnId) -> i32 {
        id.0
    }
}

impl std::fmt::Display for ColumnId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SystemColumn {
    RwTimestamp,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnDesc {
    pub data_type: DataType,
    pub column_id: ColumnId,
    pub name: String,
    pub field_descs: Vec<ColumnDesc>,
    pub type_name: String,
    pub generated_or_default_column: Option<GeneratedOrDefaultColumn>,
    pub description: Option<String>,
    pub additional_column: AdditionalColumn,
    pub version: ColumnDescVersion,
    /// Currently the system column is used for `_rw_timestamp` only and is generated at runtime,
    /// so this field is not persisted.
    pub system_column: Option<SystemColumn>,
}

impl ColumnDesc {
    pub fn unnamed(column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: String::new(),
            field_descs: vec![],
            type_name: String::new(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn named(name: impl Into<String>, column_id: ColumnId, data_type: DataType) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: name.into(),
            field_descs: vec![],
            type_name: String::new(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn named_with_default_value(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        snapshot_value: Datum,
    ) -> ColumnDesc {
        let default_col = DefaultColumnDesc {
            expr: Some(ExprNode {
                // equivalent to `Literal::to_expr_proto`
                function_type: ExprType::Unspecified as i32,
                return_type: Some(data_type.to_protobuf()),
                rex_node: Some(RexNode::Constant(snapshot_value.to_protobuf())),
            }),
            snapshot_value: Some(snapshot_value.to_protobuf()),
        };
        ColumnDesc {
            generated_or_default_column: Some(GeneratedOrDefaultColumn::DefaultColumn(default_col)),
            ..Self::named(name, column_id, data_type)
        }
    }

    pub fn named_with_additional_column(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        additional_column_type: AdditionalColumn,
    ) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: name.into(),
            field_descs: vec![],
            type_name: String::new(),
            generated_or_default_column: None,
            description: None,
            additional_column: additional_column_type,
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn named_with_system_column(
        name: impl Into<String>,
        column_id: ColumnId,
        data_type: DataType,
        system_column: SystemColumn,
    ) -> ColumnDesc {
        ColumnDesc {
            data_type,
            column_id,
            name: name.into(),
            field_descs: vec![],
            type_name: String::new(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: Some(system_column),
        }
    }

    /// Convert to proto
    pub fn to_protobuf(&self) -> PbColumnDesc {
        PbColumnDesc {
            column_type: Some(self.data_type.to_protobuf()),
            column_id: self.column_id.get_id(),
            name: self.name.clone(),
            field_descs: self
                .field_descs
                .clone()
                .into_iter()
                .map(|f| f.to_protobuf())
                .collect_vec(),
            type_name: self.type_name.clone(),
            generated_or_default_column: self.generated_or_default_column.clone(),
            description: self.description.clone(),
            additional_column_type: 0, // deprecated
            additional_column: Some(self.additional_column.clone()),
            version: self.version as i32,
        }
    }

    /// Flatten a nested column to a list of columns (including itself).
    /// If the type is atomic, it returns simply itself.
    /// If the type has multiple nesting levels, it traverses for the tree-like schema,
    /// and returns every tree node.
    pub fn flatten(&self) -> Vec<ColumnDesc> {
        let mut descs = vec![self.clone()];
        descs.extend(self.field_descs.iter().flat_map(|d| {
            let mut desc = d.clone();
            desc.name = self.name.clone() + "." + &desc.name;
            desc.flatten()
        }));
        descs
    }

    pub fn new_atomic(data_type: DataType, name: &str, column_id: i32) -> Self {
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_owned(),
            field_descs: vec![],
            type_name: "".to_owned(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn new_struct(
        name: &str,
        column_id: i32,
        type_name: &str,
        fields: Vec<ColumnDesc>,
    ) -> Self {
        let data_type =
            StructType::new(fields.iter().map(|f| (&f.name, f.data_type.clone()))).into();
        Self {
            data_type,
            column_id: ColumnId::new(column_id),
            name: name.to_owned(),
            field_descs: fields,
            type_name: type_name.to_owned(),
            generated_or_default_column: None,
            description: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn from_field_with_column_id(field: &Field, id: i32) -> Self {
        Self {
            data_type: field.data_type.clone(),
            column_id: ColumnId::new(id),
            name: field.name.clone(),
            field_descs: field
                .sub_fields
                .iter()
                .map(Self::from_field_without_column_id)
                .collect_vec(),
            type_name: field.type_name.clone(),
            description: None,
            generated_or_default_column: None,
            additional_column: AdditionalColumn { column_type: None },
            version: ColumnDescVersion::Pr13707,
            system_column: None,
        }
    }

    pub fn from_field_without_column_id(field: &Field) -> Self {
        Self::from_field_with_column_id(field, 0)
    }

    pub fn is_generated(&self) -> bool {
        matches!(
            self.generated_or_default_column,
            Some(GeneratedOrDefaultColumn::GeneratedColumn(_))
        )
    }

    pub fn is_default(&self) -> bool {
        matches!(
            self.generated_or_default_column,
            Some(GeneratedOrDefaultColumn::DefaultColumn(_))
        )
    }
}

impl From<PbColumnDesc> for ColumnDesc {
    fn from(prost: PbColumnDesc) -> Self {
        let additional_column = prost
            .get_additional_column()
            .unwrap_or(&AdditionalColumn { column_type: None })
            .clone();
        let version = prost.version();
        let field_descs: Vec<ColumnDesc> = prost
            .field_descs
            .into_iter()
            .map(ColumnDesc::from)
            .collect();
        Self {
            data_type: DataType::from(prost.column_type.as_ref().unwrap()),
            column_id: ColumnId::new(prost.column_id),
            name: prost.name,
            type_name: prost.type_name,
            field_descs,
            generated_or_default_column: prost.generated_or_default_column,
            description: prost.description.clone(),
            additional_column,
            version,
            system_column: None,
        }
    }
}

impl From<&PbColumnDesc> for ColumnDesc {
    fn from(prost: &PbColumnDesc) -> Self {
        prost.clone().into()
    }
}

impl From<&ColumnDesc> for PbColumnDesc {
    fn from(c: &ColumnDesc) -> Self {
        Self {
            column_type: c.data_type.to_protobuf().into(),
            column_id: c.column_id.into(),
            name: c.name.clone(),
            field_descs: c.field_descs.iter().map(ColumnDesc::to_protobuf).collect(),
            type_name: c.type_name.clone(),
            generated_or_default_column: c.generated_or_default_column.clone(),
            description: c.description.clone(),
            additional_column_type: 0, // deprecated
            additional_column: c.additional_column.clone().into(),
            version: c.version as i32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnCatalog {
    pub column_desc: ColumnDesc,
    pub is_hidden: bool,
}

impl ColumnCatalog {
    pub fn visible(column_desc: ColumnDesc) -> Self {
        Self {
            column_desc,
            is_hidden: false,
        }
    }

    pub fn hidden(column_desc: ColumnDesc) -> Self {
        Self {
            column_desc,
            is_hidden: true,
        }
    }

    /// Get the column catalog's is hidden.
    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    /// If the column is a generated column
    pub fn is_generated(&self) -> bool {
        self.column_desc.is_generated()
    }

    pub fn can_dml(&self) -> bool {
        !self.is_generated() && !self.is_rw_timestamp_column()
    }

    /// If the column is a generated column
    pub fn generated_expr(&self) -> Option<&ExprNode> {
        if let Some(GeneratedOrDefaultColumn::GeneratedColumn(desc)) =
            &self.column_desc.generated_or_default_column
        {
            Some(desc.expr.as_ref().unwrap())
        } else {
            None
        }
    }

    /// If the column is a column with default expr
    pub fn is_default(&self) -> bool {
        self.column_desc.is_default()
    }

    /// If the columns is an `INCLUDE ... AS ...` connector column.
    pub fn is_connector_additional_column(&self) -> bool {
        self.column_desc.additional_column.column_type.is_some()
    }

    /// Get a reference to the column desc's data type.
    pub fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    /// Get the column desc's column id.
    pub fn column_id(&self) -> ColumnId {
        self.column_desc.column_id
    }

    /// Get a reference to the column desc's name.
    pub fn name(&self) -> &str {
        self.column_desc.name.as_ref()
    }

    /// Convert column catalog to proto
    pub fn to_protobuf(&self) -> PbColumnCatalog {
        PbColumnCatalog {
            column_desc: Some(self.column_desc.to_protobuf()),
            is_hidden: self.is_hidden,
        }
    }

    /// Creates a row ID column (for implicit primary key).
    pub fn row_id_column() -> Self {
        Self {
            column_desc: row_id_column_desc(),
            is_hidden: true,
        }
    }

    pub fn is_rw_sys_column(&self) -> bool {
        self.column_desc.system_column.is_some()
    }

    pub fn rw_timestamp_column() -> Self {
        Self {
            column_desc: rw_timestamp_column_desc(),
            is_hidden: true,
        }
    }

    pub fn is_rw_timestamp_column(&self) -> bool {
        matches!(
            self.column_desc.system_column,
            Some(SystemColumn::RwTimestamp)
        )
    }

    pub fn offset_column() -> Self {
        Self {
            column_desc: offset_column_desc(),
            is_hidden: true,
        }
    }

    pub fn iceberg_sequence_num_column() -> Self {
        Self {
            column_desc: iceberg_sequence_num_column_desc(),
            is_hidden: true,
        }
    }

    pub fn cdc_table_name_column() -> Self {
        Self {
            column_desc: cdc_table_name_column_desc(),
            is_hidden: true,
        }
    }
}

impl From<PbColumnCatalog> for ColumnCatalog {
    fn from(prost: PbColumnCatalog) -> Self {
        Self {
            column_desc: prost.column_desc.unwrap().into(),
            is_hidden: prost.is_hidden,
        }
    }
}

impl ColumnCatalog {
    pub fn name_with_hidden(&self) -> Cow<'_, str> {
        if self.is_hidden {
            Cow::Owned(format!("{}(hidden)", self.column_desc.name))
        } else {
            Cow::Borrowed(&self.column_desc.name)
        }
    }
}

impl FieldLike for ColumnDesc {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl FieldLike for ColumnCatalog {
    fn data_type(&self) -> &DataType {
        &self.column_desc.data_type
    }

    fn name(&self) -> &str {
        &self.column_desc.name
    }
}

pub fn columns_extend(preserved_columns: &mut Vec<ColumnCatalog>, columns: Vec<ColumnCatalog>) {
    debug_assert_eq!(ROW_ID_COLUMN_ID.get_id(), 0);
    let mut max_incoming_column_id = ROW_ID_COLUMN_ID.get_id();
    columns.iter().for_each(|column| {
        let column_id = column.column_id().get_id();
        if column_id > max_incoming_column_id {
            max_incoming_column_id = column_id;
        }
    });
    preserved_columns.iter_mut().for_each(|column| {
        column
            .column_desc
            .column_id
            .apply_delta_if_not_row_id(max_incoming_column_id)
    });

    preserved_columns.extend(columns);
}

pub fn debug_assert_column_ids_distinct(columns: &[ColumnCatalog]) {
    debug_assert!(
        columns
            .iter()
            .map(|c| c.column_id())
            .duplicates()
            .next()
            .is_none(),
        "duplicate ColumnId found in source catalog. Columns: {columns:#?}"
    );
}

/// FIXME: Perhaps we should use sth like `ColumnIdGenerator::new_alter`,
/// However, the `SourceVersion` is problematic: It doesn't contain `next_col_id`.
/// (But for now this isn't a large problem, since drop column is not allowed for source yet..)
///
/// Besides, the logic of column id handling is a mess.
/// In some places, we use `ColumnId::placeholder()`, and use `col_id_gen` to fill it at the end;
/// In other places, we create column id ad-hoc.
pub fn max_column_id(columns: &[ColumnCatalog]) -> ColumnId {
    // XXX: should we check the column IDs of struct fields here?
    columns
        .iter()
        .fold(ColumnId::first_user_column(), |a, b| a.max(b.column_id()))
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::plan_common::PbColumnDesc;

    use crate::catalog::ColumnDesc;
    use crate::test_prelude::*;
    use crate::types::DataType;

    pub fn build_prost_desc() -> PbColumnDesc {
        let city = vec![
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.address", 2),
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.city.zipcode", 3),
        ];
        let country = vec![
            PbColumnDesc::new_atomic(DataType::Varchar.to_protobuf(), "country.address", 1),
            PbColumnDesc::new_struct("country.city", 4, ".test.City", city),
        ];
        PbColumnDesc::new_struct("country", 5, ".test.Country", country)
    }

    pub fn build_desc() -> ColumnDesc {
        let city = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.address", 2),
            ColumnDesc::new_atomic(DataType::Varchar, "country.city.zipcode", 3),
        ];
        let country = vec![
            ColumnDesc::new_atomic(DataType::Varchar, "country.address", 1),
            ColumnDesc::new_struct("country.city", 4, ".test.City", city),
        ];
        ColumnDesc::new_struct("country", 5, ".test.Country", country)
    }

    #[test]
    fn test_into_column_catalog() {
        let desc: ColumnDesc = build_prost_desc().into();
        assert_eq!(desc, build_desc());
    }
}
