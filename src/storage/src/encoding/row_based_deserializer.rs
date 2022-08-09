// Copyright 2022 Singularity Data
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

use bytes::Buf;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_common::util::value_encoding::deserialize_datum;

#[derive(Clone)]
pub struct RowBasedDeserializer {
    data_types: Vec<DataType>,
}

impl RowBasedDeserializer {
    pub fn new(data_types: Vec<DataType>) -> Self {
        Self { data_types }
    }

    /// Deserialize bytes into a row.
    pub fn deserialize(&self, mut row: impl Buf) -> Result<Row> {
        // value encoding
        let mut values = Vec::with_capacity(self.data_types.len());
        for ty in &self.data_types {
            values.push(deserialize_datum(&mut row, ty)?);
        }
        Ok(Row(values))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Row;
    use risingwave_common::types::{DataType, IntervalUnit, ScalarImpl};

    use crate::encoding::row_based_deserializer::RowBasedDeserializer;
    use crate::encoding::row_based_serializer::RowBasedSerializer;

    #[test]
    fn test_row_based_serialize_and_deserialize_not_null() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);
        let mut se = RowBasedSerializer::new();
        let bytes = se.serialize(&row).unwrap();
        // each cell will add a null_tag (u8)
        assert_eq!(bytes.len(), 11 + 2 + 3 + 5 + 9 + 5 + 9 + 17 + 17);

        let de = RowBasedDeserializer::new(vec![
            DataType::Varchar,
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Interval,
        ]);
        let row1 = de.deserialize(&*bytes).unwrap();
        assert_eq!(row, row1);
    }

    #[test]
    fn test_row_based_serialize_and_deserialize_with_null() {
        let row = Row(vec![
            Some(ScalarImpl::Utf8("string".into())),
            Some(ScalarImpl::Bool(true)),
            Some(ScalarImpl::Int16(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int64(3)),
            Some(ScalarImpl::Float32(4.0.into())),
            Some(ScalarImpl::Float64(5.0.into())),
            Some(ScalarImpl::Decimal("-233.3".parse().unwrap())),
            Some(ScalarImpl::Interval(IntervalUnit::new(7, 8, 9))),
        ]);
        let mut se = RowBasedSerializer::new();
        let bytes = se.serialize(&row).unwrap();
        // each cell will add a is_none flag (u8)
        assert_eq!(bytes.len(), 11 + 2 + 3 + 5 + 9 + 5 + 9 + 17 + 17);

        let de = RowBasedDeserializer::new(vec![
            DataType::Varchar,
            DataType::Boolean,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal,
            DataType::Interval,
        ]);
        let row1 = de.deserialize(&*bytes).unwrap();
        assert_eq!(row, row1);
    }
}
