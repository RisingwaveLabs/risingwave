use std::ops::Index;

#[allow(unused_imports)]
use risingwave_pb::plan::{ColumnDesc, ExchangeInfo, Field as ProstField};

use crate::array::ArrayBuilderImpl;
use crate::error::Result;
use crate::types::DataType;

/// The field in the schema of the executor's return data
#[derive(Clone, PartialEq)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.name, self.data_type)
    }
}

impl Field {
    pub fn to_prost(&self) -> Result<ProstField> {
        Ok(ProstField {
            data_type: Some(self.data_type.to_protobuf()?),
            name: self.name.to_string(),
        })
    }
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Schema {
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
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

    /// Create array builders for all fields in this schema.
    pub fn create_array_builders(&self, capacity: usize) -> Result<Vec<ArrayBuilderImpl>> {
        self.fields
            .iter()
            .map(|field| field.data_type.create_array_builder(capacity))
            .collect()
    }
}

impl Field {
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

    pub fn from(prost_field: &ProstField) -> Self {
        Self {
            data_type: DataType::from(prost_field.get_data_type().expect("data type not found")),
            name: prost_field.get_name().clone(),
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

#[allow(unused)]
pub mod test_utils {
    use super::*;

    fn field_n<const N: usize>(data_type: DataType) -> Schema {
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
