// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow_array::ArrayRef;
use num_traits::abs;

pub use super::arrow_53::{
    arrow_array, arrow_buffer, arrow_cast, arrow_schema, FromArrow, ToArrow,
};
use crate::array::{Array, ArrayError, ArrayImpl, DataChunk, DataType, DecimalArray};
use crate::types::StructType;

pub struct IcebergArrowConvert;

impl IcebergArrowConvert {
    pub fn to_record_batch(
        &self,
        schema: arrow_schema::SchemaRef,
        chunk: &DataChunk,
    ) -> Result<arrow_array::RecordBatch, ArrayError> {
        ToArrow::to_record_batch(self, schema, chunk)
    }

    pub fn chunk_from_record_batch(
        &self,
        batch: &arrow_array::RecordBatch,
    ) -> Result<DataChunk, ArrayError> {
        FromArrow::from_record_batch(self, batch)
    }

    pub fn type_from_field(&self, field: &arrow_schema::Field) -> Result<DataType, ArrayError> {
        FromArrow::from_field(self, field)
    }

    pub fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        ToArrow::to_arrow_field(self, name, data_type)
    }

    pub fn struct_from_fields(
        &self,
        fields: &arrow_schema::Fields,
    ) -> Result<StructType, ArrayError> {
        FromArrow::from_fields(self, fields)
    }

    pub fn to_arrow_array(
        &self,
        data_type: &arrow_schema::DataType,
        array: &ArrayImpl,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        ToArrow::to_array(self, data_type, array)
    }

    pub fn array_from_arrow_array(
        &self,
        field: &arrow_schema::Field,
        array: &arrow_array::ArrayRef,
    ) -> Result<ArrayImpl, ArrayError> {
        FromArrow::from_array(self, field, array)
    }
}

impl ToArrow for IcebergArrowConvert {
    fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        let data_type = match data_type {
            DataType::Boolean => self.bool_type_to_arrow(),
            DataType::Int16 => self.int32_type_to_arrow(),
            DataType::Int32 => self.int32_type_to_arrow(),
            DataType::Int64 => self.int64_type_to_arrow(),
            DataType::Int256 => self.int256_type_to_arrow(),
            DataType::Float32 => self.float32_type_to_arrow(),
            DataType::Float64 => self.float64_type_to_arrow(),
            DataType::Date => self.date_type_to_arrow(),
            DataType::Time => self.time_type_to_arrow(),
            DataType::Timestamp => self.timestamp_type_to_arrow(),
            DataType::TimestampNanosecond => self.timestampns_type_to_arrow(),
            DataType::Timestamptz => self.timestamptz_type_to_arrow(),
            DataType::Interval => self.interval_type_to_arrow(),
            DataType::Varchar => self.varchar_type_to_arrow(),
            DataType::Bytea => self.bytea_type_to_arrow(),
            DataType::Serial => self.serial_type_to_arrow(),
            DataType::Decimal => return Ok(self.decimal_type_to_arrow(name)),
            DataType::Jsonb => self.varchar_type_to_arrow(),
            DataType::Struct(fields) => self.struct_type_to_arrow(fields)?,
            DataType::List(datatype) => self.list_type_to_arrow(datatype)?,
            DataType::Map(datatype) => self.map_type_to_arrow(datatype)?,
        };
        Ok(arrow_schema::Field::new(name, data_type, true))
    }

    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        // Fixed-point decimal; precision P, scale S Scale is fixed, precision must be less than 38.
        let data_type = arrow_schema::DataType::Decimal128(28, 10);
        arrow_schema::Field::new(name, data_type, true)
    }

    fn decimal_to_arrow(
        &self,
        data_type: &arrow_schema::DataType,
        array: &DecimalArray,
    ) -> Result<arrow_array::ArrayRef, ArrayError> {
        let (precision, max_scale) = match data_type {
            arrow_schema::DataType::Decimal128(precision, scale) => (*precision, *scale),
            _ => return Err(ArrayError::to_arrow("Invalid decimal type")),
        };

        // Convert Decimal to i128:
        let values: Vec<Option<i128>> = array
            .iter()
            .map(|e| {
                e.and_then(|e| match e {
                    crate::array::Decimal::Normalized(e) => {
                        let value = e.mantissa();
                        let scale = e.scale() as i8;
                        let diff_scale = abs(max_scale - scale);
                        let value = match scale {
                            _ if scale < max_scale => value.mul(10_i128.pow(diff_scale as u32)),
                            _ if scale > max_scale => value.div(10_i128.pow(diff_scale as u32)),
                            _ => value,
                        };
                        Some(value)
                    }
                    // For Inf, we replace them with the max/min value within the precision.
                    crate::array::Decimal::PositiveInf => {
                        let max_value = 10_i128.pow(precision as u32) - 1;
                        Some(max_value)
                    }
                    crate::array::Decimal::NegativeInf => {
                        let max_value = 10_i128.pow(precision as u32) - 1;
                        Some(-max_value)
                    }
                    crate::array::Decimal::NaN => None,
                })
            })
            .collect();

        let array = arrow_array::Decimal128Array::from(values)
            .with_precision_and_scale(precision, max_scale)
            .map_err(ArrayError::from_arrow)?;
        Ok(Arc::new(array) as ArrayRef)
    }
}

impl FromArrow for IcebergArrowConvert {}

/// Iceberg sink with `create_table_if_not_exists` option will use this struct to convert the
/// iceberg data type to arrow data type.
///
/// Specifically, it will add the field id to the arrow field metadata, because iceberg-rust and icelake need the field id to be set.
///
/// Note: this is different from [`IcebergArrowConvert`], which is used to read from/write to
/// an _existing_ iceberg table. In that case, we just need to make sure the data is compatible to the existing schema.
/// But to _create a new table_, we need to meet more requirements of iceberg.
#[derive(Default)]
pub struct IcebergCreateTableArrowConvert {
    next_field_id: RefCell<u32>,
}

impl IcebergCreateTableArrowConvert {
    pub fn to_arrow_field(
        &self,
        name: &str,
        data_type: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        ToArrow::to_arrow_field(self, name, data_type)
    }

    fn add_field_id(&self, arrow_field: &mut arrow_schema::Field) {
        *self.next_field_id.borrow_mut() += 1;
        let field_id = *self.next_field_id.borrow();

        let mut metadata = HashMap::new();
        // for iceberg-rust
        metadata.insert("PARQUET:field_id".to_owned(), field_id.to_string());
        // for icelake
        metadata.insert("column_id".to_owned(), field_id.to_string());
        arrow_field.set_metadata(metadata);
    }
}

impl ToArrow for IcebergCreateTableArrowConvert {
    #[inline]
    fn decimal_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        // To create a iceberg table, we need a decimal type with precision and scale to be set
        // We choose 28 here
        // The decimal type finally will be converted to an iceberg decimal type.
        // Iceberg decimal(P,S)
        // Fixed-point decimal; precision P, scale S Scale is fixed, precision must be less than 38.
        let data_type = arrow_schema::DataType::Decimal128(28, 10);

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        arrow_field
    }

    fn jsonb_type_to_arrow(&self, name: &str) -> arrow_schema::Field {
        let data_type = arrow_schema::DataType::Utf8;

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        arrow_field
    }

    /// Convert RisingWave data type to Arrow data type.
    ///
    /// This function returns a `Field` instead of `DataType` because some may be converted to
    /// extension types which require additional metadata in the field.
    fn to_arrow_field(
        &self,
        name: &str,
        value: &DataType,
    ) -> Result<arrow_schema::Field, ArrayError> {
        let data_type = match value {
            // using the inline function
            DataType::Boolean => self.bool_type_to_arrow(),
            DataType::Int16 => self.int32_type_to_arrow(),
            DataType::Int32 => self.int32_type_to_arrow(),
            DataType::Int64 => self.int64_type_to_arrow(),
            DataType::Int256 => self.varchar_type_to_arrow(),
            DataType::Float32 => self.float32_type_to_arrow(),
            DataType::Float64 => self.float64_type_to_arrow(),
            DataType::Date => self.date_type_to_arrow(),
            DataType::Time => self.time_type_to_arrow(),
            DataType::Timestamp => self.timestamp_type_to_arrow(),
            DataType::TimestampNanosecond => self.timestampns_type_to_arrow(),
            DataType::Timestamptz => self.timestamptz_type_to_arrow(),
            DataType::Interval => self.interval_type_to_arrow(),
            DataType::Varchar => self.varchar_type_to_arrow(),
            DataType::Bytea => self.bytea_type_to_arrow(),
            DataType::Serial => self.serial_type_to_arrow(),
            DataType::Decimal => return Ok(self.decimal_type_to_arrow(name)),
            DataType::Jsonb => self.varchar_type_to_arrow(),
            DataType::Struct(fields) => self.struct_type_to_arrow(fields)?,
            DataType::List(datatype) => self.list_type_to_arrow(datatype)?,
            DataType::Map(datatype) => self.map_type_to_arrow(datatype)?,
        };

        let mut arrow_field = arrow_schema::Field::new(name, data_type, true);
        self.add_field_id(&mut arrow_field);
        Ok(arrow_field)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::arrow_array::{ArrayRef, Decimal128Array};
    use super::arrow_schema::DataType;
    use super::*;
    use crate::array::{Decimal, DecimalArray};

    #[test]
    fn decimal() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let ty = DataType::Decimal128(6, 3);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            Decimal128Array::from(vec![
                None,
                None,
                Some(999999),
                Some(-999999),
                Some(123400),
                Some(123456),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }

    #[test]
    fn decimal_with_large_scale() {
        let array = DecimalArray::from_iter([
            None,
            Some(Decimal::NaN),
            Some(Decimal::PositiveInf),
            Some(Decimal::NegativeInf),
            Some(Decimal::Normalized("123.4".parse().unwrap())),
            Some(Decimal::Normalized("123.456".parse().unwrap())),
        ]);
        let ty = DataType::Decimal128(28, 10);
        let arrow_array = IcebergArrowConvert.decimal_to_arrow(&ty, &array).unwrap();
        let expect_array = Arc::new(
            Decimal128Array::from(vec![
                None,
                None,
                Some(9999999999999999999999999999),
                Some(-9999999999999999999999999999),
                Some(1234000000000),
                Some(1234560000000),
            ])
            .with_data_type(ty),
        ) as ArrayRef;
        assert_eq!(&arrow_array, &expect_array);
    }
}
