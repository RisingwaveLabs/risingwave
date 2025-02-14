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

use risingwave_common::catalog::{
    ColumnDesc, ColumnId, KAFKA_TIMESTAMP_COLUMN_NAME, OFFSET_COLUMN_NAME, ROWID_PREFIX,
    TABLE_NAME_COLUMN_NAME,
};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDescVersion};

/// `SourceColumnDesc` is used to describe a column in the Source.
///
/// See the implementation of `From<&ColumnDesc>` for the difference between `SourceColumnDesc` and [`ColumnDesc`].
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataType,
    pub column_id: ColumnId,
    /// `additional_column` and `column_type` are orthogonal
    /// `additional_column` is used to indicate the column is from which part of the message
    /// `column_type` is used to indicate the type of the column, only used in cdc scenario
    pub additional_column: AdditionalColumn,
    // ------
    // Fields above are the same in `ColumnDesc`.
    // Fields below are specific to `SourceColumnDesc`.
    // ------
    pub column_type: SourceColumnType,
    /// `is_pk` is used to indicate whether the column is part of the primary key columns.
    pub is_pk: bool,
    /// `is_hidden_addition_col` is used to indicate whether the column is a hidden addition column.
    pub is_hidden_addition_col: bool,
}

/// `SourceColumnType` is used to indicate the type of a column emitted by the Source.
/// There are 4 types of columns:
/// - `Normal`: a visible column
/// - `RowId`: internal column to uniquely identify a row
/// - `Meta`: internal column to store source related metadata
/// - `Offset`: internal column to store upstream offset for a row, used in CDC source
#[derive(Clone, Debug, PartialEq)]
pub enum SourceColumnType {
    Normal,

    // internal columns
    RowId,
    Meta,
    Offset,
}

impl SourceColumnType {
    pub fn from_name(name: &str) -> Self {
        if name.starts_with(KAFKA_TIMESTAMP_COLUMN_NAME) || name.starts_with(TABLE_NAME_COLUMN_NAME)
        {
            Self::Meta
        } else if name == (ROWID_PREFIX) {
            Self::RowId
        } else if name == OFFSET_COLUMN_NAME {
            Self::Offset
        } else {
            Self::Normal
        }
    }
}

impl SourceColumnDesc {
    /// Create a [`SourceColumnDesc`].
    // TODO(struct): rename to `new`?
    pub fn simple(name: impl Into<String>, data_type: DataType, column_id: ColumnId) -> Self {
        let name = name.into();
        Self {
            name,
            data_type,
            column_id,
            column_type: SourceColumnType::Normal,
            is_pk: false,
            is_hidden_addition_col: false,
            additional_column: AdditionalColumn { column_type: None },
        }
    }

    pub fn hidden_addition_col_from_column_desc(c: &ColumnDesc) -> Self {
        Self {
            is_hidden_addition_col: true,
            ..c.into()
        }
    }

    pub fn is_row_id(&self) -> bool {
        self.column_type == SourceColumnType::RowId
    }

    pub fn is_meta(&self) -> bool {
        self.column_type == SourceColumnType::Meta
    }

    pub fn is_offset(&self) -> bool {
        self.column_type == SourceColumnType::Offset
    }

    #[inline]
    pub fn is_visible(&self) -> bool {
        !self.is_hidden_addition_col && self.column_type == SourceColumnType::Normal
    }
}

impl From<&ColumnDesc> for SourceColumnDesc {
    fn from(
        ColumnDesc {
            data_type,
            column_id,
            name,
            additional_column,
            // ignored fields below
            generated_or_default_column,
            description: _,
            version: _,
            system_column: _,
        }: &ColumnDesc,
    ) -> Self {
        if let Some(option) = generated_or_default_column {
            debug_assert!(
                matches!(option, GeneratedOrDefaultColumn::DefaultColumn(_)),
                "source column should not be generated: {:?}",
                generated_or_default_column.as_ref().unwrap()
            )
        }

        Self {
            name: name.clone(),
            data_type: data_type.clone(),
            column_id: *column_id,
            additional_column: additional_column.clone(),
            // additional fields below
            column_type: SourceColumnType::from_name(name),
            is_pk: false,
            is_hidden_addition_col: false,
        }
    }
}

impl From<&SourceColumnDesc> for ColumnDesc {
    fn from(
        SourceColumnDesc {
            name,
            data_type,
            column_id,
            additional_column,
            // ignored fields below
            column_type: _,
            is_pk: _,
            is_hidden_addition_col: _,
        }: &SourceColumnDesc,
    ) -> Self {
        ColumnDesc {
            data_type: data_type.clone(),
            column_id: *column_id,
            name: name.clone(),
            additional_column: additional_column.clone(),
            // additional fields below
            generated_or_default_column: None,
            description: None,
            version: ColumnDescVersion::LATEST,
            system_column: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_visible() {
        let mut c = SourceColumnDesc::simple("a", DataType::Int32, ColumnId::new(0));
        assert!(c.is_visible());
        c.column_type = SourceColumnType::RowId;
        assert!(!c.is_visible());
        c.column_type = SourceColumnType::Meta;
        assert!(!c.is_visible());
    }
}
