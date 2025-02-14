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

use std::ops::Index;

use risingwave_pb::plan_common::{PbColumnDesc, PbField};

use super::ColumnDesc;
use crate::array::ArrayBuilderImpl;
use crate::types::{DataType, StructType};
use crate::util::iter_util::ZipEqFast;

/// The field in the schema of the executor's return data
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            data_type,
            name: name.into(),
        }
    }
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.name, self.data_type)
    }
}

impl Field {
    pub fn to_prost(&self) -> PbField {
        PbField {
            data_type: Some(self.data_type.to_protobuf()),
            name: self.name.clone(),
        }
    }
}

impl From<&ColumnDesc> for Field {
    fn from(desc: &ColumnDesc) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: desc.name.clone(),
        }
    }
}

impl From<ColumnDesc> for Field {
    fn from(column_desc: ColumnDesc) -> Self {
        Self {
            data_type: column_desc.data_type,
            name: column_desc.name,
        }
    }
}

impl From<&PbColumnDesc> for Field {
    fn from(pb_column_desc: &PbColumnDesc) -> Self {
        Self {
            data_type: pb_column_desc.column_type.as_ref().unwrap().into(),
            name: pb_column_desc.name.clone(),
        }
    }
}

/// Something that has a data type and a name.
#[auto_impl::auto_impl(&)]
pub trait FieldLike {
    fn data_type(&self) -> &DataType;
    fn name(&self) -> &str;
}

impl FieldLike for Field {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct FieldDisplay<'a>(pub &'a Field);

impl std::fmt::Debug for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

impl std::fmt::Display for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

/// `schema_unnamed` builds a `Schema` with the given types, but without names.
#[macro_export]
macro_rules! schema_unnamed {
    ($($t:expr),*) => {{
        $crate::catalog::Schema {
            fields: vec![
                $( $crate::catalog::Field::unnamed($t) ),*
            ],
        }
    }};
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn empty() -> &'static Self {
        static EMPTY: Schema = Schema { fields: Vec::new() };
        &EMPTY
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    pub fn names(&self) -> Vec<String> {
        self.fields().iter().map(|f| f.name.clone()).collect()
    }

    pub fn names_str(&self) -> Vec<&str> {
        self.fields().iter().map(|f| f.name.as_str()).collect()
    }

    pub fn data_types(&self) -> Vec<DataType> {
        self.fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn into_fields(self) -> Vec<Field> {
        self.fields
    }

    /// Create array builders for all fields in this schema.
    pub fn create_array_builders(&self, capacity: usize) -> Vec<ArrayBuilderImpl> {
        self.fields
            .iter()
            .map(|field| field.data_type.create_array_builder(capacity))
            .collect()
    }

    pub fn to_prost(&self) -> Vec<PbField> {
        self.fields
            .clone()
            .into_iter()
            .map(|field| field.to_prost())
            .collect()
    }

    pub fn type_eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (a, b) in self.fields.iter().zip_eq_fast(other.fields.iter()) {
            if a.data_type != b.data_type {
                return false;
            }
        }

        true
    }

    pub fn all_type_eq<'a>(inputs: impl IntoIterator<Item = &'a Self>) -> bool {
        let mut iter = inputs.into_iter();
        if let Some(first) = iter.next() {
            iter.all(|x| x.type_eq(first))
        } else {
            true
        }
    }

    pub fn formatted_col_names(&self) -> String {
        self.fields
            .iter()
            .map(|f| format!("\"{}\"", &f.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Field {
    // TODO: rename to `new`
    pub fn with_name<S>(data_type: DataType, name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            data_type,
            name: name.into(),
        }
    }

    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            data_type,
            name: String::new(),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    pub fn from_with_table_name_prefix(desc: &ColumnDesc, table_name: &str) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: format!("{}.{}", table_name, desc.name),
        }
    }

    /// Get the sub fields if the data type is a struct, otherwise return an empty vector.
    pub fn sub_fields(&self) -> Vec<Field> {
        if let DataType::Struct(st) = &self.data_type {
            st.iter()
                .map(|(name, data_type)| Field::with_name(data_type.clone(), name))
                .collect()
        } else {
            Vec::new()
        }
    }
}

impl From<&PbField> for Field {
    fn from(prost_field: &PbField) -> Self {
        Self {
            data_type: DataType::from(prost_field.get_data_type().expect("data type not found")),
            name: prost_field.get_name().clone(),
        }
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

impl FromIterator<Field> for Schema {
    fn from_iter<I: IntoIterator<Item = Field>>(iter: I) -> Self {
        Schema {
            fields: iter.into_iter().collect::<Vec<_>>(),
        }
    }
}

impl From<&StructType> for Schema {
    fn from(t: &StructType) -> Self {
        Schema::new(
            t.iter()
                .map(|(s, d)| Field::with_name(d.clone(), s))
                .collect(),
        )
    }
}

pub mod test_utils {
    use super::*;

    pub fn field_n<const N: usize>(data_type: DataType) -> Schema {
        Schema::new(vec![Field::unnamed(data_type); N])
    }

    fn int32_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Int32)
    }

    /// Create a util schema **for test only** with two int32 fields.
    pub fn ii() -> Schema {
        int32_n::<2>()
    }

    /// Create a util schema **for test only** with three int32 fields.
    pub fn iii() -> Schema {
        int32_n::<3>()
    }

    fn varchar_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Varchar)
    }

    /// Create a util schema **for test only** with three varchar fields.
    pub fn sss() -> Schema {
        varchar_n::<3>()
    }

    fn decimal_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Decimal)
    }

    /// Create a util schema **for test only** with three decimal fields.
    pub fn ddd() -> Schema {
        decimal_n::<3>()
    }
}
