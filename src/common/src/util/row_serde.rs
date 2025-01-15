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

use std::borrow::Cow;

use bytes::BufMut;

use crate::row::{OwnedRow, Row};
use crate::types::{DataType, ToDatumRef};
use crate::util::iter_util::{ZipEqDebug, ZipEqFast};
use crate::util::memcmp_encoding;
use crate::util::sort_util::OrderType;

/// `OrderedRowSerde` is responsible for serializing and deserializing Ordered Row.
#[derive(Clone)]
pub struct OrderedRowSerde {
    schema: Vec<DataType>,
    order_types: Vec<OrderType>,
}

impl OrderedRowSerde {
    pub fn new(schema: Vec<DataType>, order_types: Vec<OrderType>) -> Self {
        assert_eq!(schema.len(), order_types.len());
        Self {
            schema,
            order_types,
        }
    }

    /// FIXME: This leads to allocation in most cases. Should allow using slices for the schema and
    /// order types.
    #[must_use]
    pub fn prefix(&self, len: usize) -> Cow<'_, Self> {
        if len == self.order_types.len() {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(Self {
                schema: self.schema[..len].to_vec(),
                order_types: self.order_types[..len].to_vec(),
            })
        }
    }

    #[must_use]
    pub fn index(&self, idx: usize) -> Cow<'_, Self> {
        if 1 == self.order_types.len() {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(Self {
                schema: vec![self.schema[idx].clone()],
                order_types: vec![self.order_types[idx]],
            })
        }
    }

    /// Note: prefer [`Row::memcmp_serialize`] if possible.
    pub fn serialize(&self, row: impl Row, append_to: impl BufMut) {
        self.serialize_datums(row.iter(), append_to)
    }

    /// Note: prefer [`Row::memcmp_serialize`] if possible.
    pub fn serialize_datums(
        &self,
        datum_refs: impl Iterator<Item = impl ToDatumRef>,
        mut append_to: impl BufMut,
    ) {
        let mut serializer = memcomparable::Serializer::new(&mut append_to);
        for (datum, order) in datum_refs.zip_eq_debug(self.order_types.iter().copied()) {
            memcmp_encoding::serialize_datum(datum, order, &mut serializer).unwrap();
        }
    }

    pub fn deserialize(&self, data: &[u8]) -> memcomparable::Result<OwnedRow> {
        let mut values = Vec::with_capacity(self.schema.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for (data_type, order) in self
            .schema
            .iter()
            .zip_eq_fast(self.order_types.iter().copied())
        {
            let datum = memcmp_encoding::deserialize_datum(data_type, order, &mut deserializer)?;
            values.push(datum);
        }
        Ok(OwnedRow::new(values))
    }

    pub fn get_order_types(&self) -> &[OrderType] {
        &self.order_types
    }

    pub fn get_data_types(&self) -> &[DataType] {
        &self.schema
    }

    pub fn deserialize_prefix_len(
        &self,
        key: &[u8],
        prefix_len: usize,
    ) -> memcomparable::Result<usize> {
        let mut len: usize = 0;
        for index in 0..prefix_len {
            let data_type = &self.schema[index];
            let data = &key[len..];
            len +=
                memcmp_encoding::calculate_encoded_size(data_type, self.order_types[index], data)?;
        }
        Ok(len)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use crate::types::{ScalarImpl as S, Timestamp};

    #[test]
    fn test_ordered_row_serializer() {
        let orders = vec![OrderType::descending(), OrderType::ascending()];
        let data_types = vec![DataType::Int16, DataType::Varchar];
        let serializer = OrderedRowSerde::new(data_types, orders);
        let row1 = OwnedRow::new(vec![Some(S::Int16(5)), Some(S::Utf8("abc".into()))]);
        let row2 = OwnedRow::new(vec![Some(S::Int16(5)), Some(S::Utf8("abd".into()))]);
        let row3 = OwnedRow::new(vec![Some(S::Int16(6)), Some(S::Utf8("abc".into()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        for row in &rows {
            let mut row_bytes = vec![];
            serializer.serialize(row, &mut row_bytes);
            array.push(row_bytes);
        }
        array.sort();
        // option 1 byte || number 2 bytes
        assert_eq!(array[0][2], !6i16.to_be_bytes()[1]);
        assert_eq!(&array[1][3..], [0, 1, b'a', b'b', b'c', 0, 0, 0, 0, 0, 3u8]);
        assert_eq!(&array[2][3..], [0, 1, b'a', b'b', b'd', 0, 0, 0, 0, 0, 3u8]);
    }

    #[test]
    fn test_ordered_row_deserializer() {
        use crate::types::*;
        {
            // basic
            let order_types = vec![OrderType::descending(), OrderType::ascending()];

            let schema = vec![DataType::Varchar, DataType::Int16];
            let serde = OrderedRowSerde::new(schema, order_types);
            let row1 = OwnedRow::new(vec![Some(S::Utf8("abc".into())), Some(S::Int16(5))]);
            let row2 = OwnedRow::new(vec![Some(S::Utf8("abd".into())), Some(S::Int16(5))]);
            let row3 = OwnedRow::new(vec![Some(S::Utf8("abc".into())), Some(S::Int16(6))]);
            let rows = vec![row1.clone(), row2.clone(), row3.clone()];
            let mut array = vec![];
            for row in &rows {
                let mut row_bytes = vec![];
                serde.serialize(row, &mut row_bytes);
                array.push(row_bytes);
            }
            assert_eq!(serde.deserialize(&array[0]).unwrap(), row1);
            assert_eq!(serde.deserialize(&array[1]).unwrap(), row2);
            assert_eq!(serde.deserialize(&array[2]).unwrap(), row3);
        }

        {
            // decimal

            let order_types = vec![OrderType::descending(), OrderType::ascending()];

            let schema = vec![DataType::Varchar, DataType::Decimal];
            let serde = OrderedRowSerde::new(schema, order_types);
            let row1 = OwnedRow::new(vec![
                Some(S::Utf8("abc".into())),
                Some(S::Decimal(Decimal::NaN)),
            ]);
            let row2 = OwnedRow::new(vec![
                Some(S::Utf8("abd".into())),
                Some(S::Decimal(Decimal::PositiveInf)),
            ]);
            let row3 = OwnedRow::new(vec![
                Some(S::Utf8("abc".into())),
                Some(S::Decimal(Decimal::NegativeInf)),
            ]);
            let rows = vec![row1.clone(), row2.clone(), row3.clone()];
            let mut array = vec![];
            for row in &rows {
                let mut row_bytes = vec![];
                serde.serialize(row, &mut row_bytes);
                array.push(row_bytes);
            }
            assert_eq!(serde.deserialize(&array[0]).unwrap(), row1);
            assert_eq!(serde.deserialize(&array[1]).unwrap(), row2);
            assert_eq!(serde.deserialize(&array[2]).unwrap(), row3);
        }
    }

    #[test]
    fn test_deserialize_with_column_indices() {
        let order_types = vec![OrderType::descending(), OrderType::ascending()];

        let schema = vec![DataType::Varchar, DataType::Int16];
        let serde = OrderedRowSerde::new(schema, order_types);
        let row1 = OwnedRow::new(vec![Some(S::Utf8("abc".into())), Some(S::Int16(5))]);
        let rows = vec![row1.clone()];
        let mut array = vec![];
        for row in &rows {
            let mut row_bytes = vec![];
            serde.serialize(row, &mut row_bytes);
            array.push(row_bytes);
        }

        {
            let row_0_idx_0_len = serde.deserialize_prefix_len(&array[0], 1).unwrap();

            let schema = vec![DataType::Varchar];
            let order_types = vec![OrderType::descending()];
            let deserde = OrderedRowSerde::new(schema, order_types);
            let prefix_slice = &array[0][0..row_0_idx_0_len];
            assert_eq!(
                deserde.deserialize(prefix_slice).unwrap(),
                OwnedRow::new(vec![Some(S::Utf8("abc".into()))])
            );
        }

        {
            let row_0_idx_1_len = serde.deserialize_prefix_len(&array[0], 2).unwrap();

            let order_types = vec![OrderType::descending(), OrderType::ascending()];
            let schema = vec![DataType::Varchar, DataType::Int16];
            let deserde = OrderedRowSerde::new(schema, order_types);
            let prefix_slice = &array[0][0..row_0_idx_1_len];
            assert_eq!(deserde.deserialize(prefix_slice).unwrap(), row1);
        }
    }

    #[test]
    fn test_encoding_data_size() {
        use std::mem::size_of;

        use crate::types::{Interval, F64};

        let order_types = vec![OrderType::ascending()];
        let schema = vec![DataType::Int16];
        let serde = OrderedRowSerde::new(schema, order_types.clone());

        // test fixed_size
        {
            {
                // test None
                let row = OwnedRow::new(vec![None]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Int16,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();
                assert_eq!(1, encoding_data_size);
            }

            {
                // float64
                let row = OwnedRow::new(vec![Some(S::Float64(6.4.into()))]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Float64,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();
                let data_size = size_of::<F64>();
                assert_eq!(8, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // bool
                let row = OwnedRow::new(vec![Some(S::Bool(false))]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Boolean,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();

                let data_size = size_of::<u8>();
                assert_eq!(1, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // ts
                let row = OwnedRow::new(vec![Some(S::Timestamp(Default::default()))]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Timestamp,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();
                let data_size = size_of::<Timestamp>();
                assert_eq!(12, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // tz
                let row = OwnedRow::new(vec![Some(S::Int64(1111111111))]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Timestamptz,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();
                let data_size = size_of::<i64>();
                assert_eq!(8, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }

            {
                // interval
                let row = OwnedRow::new(vec![Some(S::Interval(Interval::default()))]);
                let mut row_bytes = vec![];
                serde.serialize(&row, &mut row_bytes);
                let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                    &DataType::Interval,
                    order_types[0],
                    &row_bytes[..],
                )
                .unwrap();
                let data_size = size_of::<Interval>();
                assert_eq!(16, data_size);
                assert_eq!(1 + data_size, encoding_data_size);
            }
        }

        {
            // test dynamic_size
            {
                // test decimal
                pub use crate::types::Decimal;

                {
                    let d = Decimal::from_str("41721.900909090909090909090909").unwrap();
                    let row = OwnedRow::new(vec![Some(S::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serde.serialize(&row, &mut row_bytes);
                    let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                        &DataType::Decimal,
                        order_types[0],
                        &row_bytes[..],
                    )
                    .unwrap();
                    // [nulltag, flag, decimal_chunk]
                    assert_eq!(17, encoding_data_size);
                }

                {
                    let d = Decimal::from_str("1").unwrap();
                    let row = OwnedRow::new(vec![Some(S::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serde.serialize(&row, &mut row_bytes);
                    let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                        &DataType::Decimal,
                        order_types[0],
                        &row_bytes[..],
                    )
                    .unwrap();
                    // [nulltag, flag, decimal_chunk]
                    assert_eq!(3, encoding_data_size);
                }

                {
                    let d = Decimal::from_str("inf").unwrap();
                    let row = OwnedRow::new(vec![Some(S::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serde.serialize(&row, &mut row_bytes);
                    let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                        &DataType::Decimal,
                        order_types[0],
                        &row_bytes[..],
                    )
                    .unwrap();

                    assert_eq!(2, encoding_data_size); // [1, 35]
                }

                {
                    let d = Decimal::from_str("nan").unwrap();
                    let row = OwnedRow::new(vec![Some(S::Decimal(d))]);
                    let mut row_bytes = vec![];
                    serde.serialize(&row, &mut row_bytes);
                    let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                        &DataType::Decimal,
                        order_types[0],
                        &row_bytes[..],
                    )
                    .unwrap();
                    assert_eq!(2, encoding_data_size); // [1, 6]
                }

                {
                    // TODO(test list / struct)
                }

                {
                    // test varchar
                    let varchar = "abcdefghijklmn";
                    let row = OwnedRow::new(vec![Some(S::Utf8(varchar.into()))]);
                    let mut row_bytes = vec![];
                    serde.serialize(&row, &mut row_bytes);
                    let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                        &DataType::Varchar,
                        order_types[0],
                        &row_bytes[..],
                    )
                    .unwrap();
                    // [1, 1, 97, 98, 99, 100, 101, 102, 103, 104, 9, 105, 106, 107, 108, 109, 110,
                    // 0, 0, 6]
                    assert_eq!(6 + varchar.len(), encoding_data_size);
                }

                {
                    {
                        // test varchar Descending
                        let order_types = vec![OrderType::descending()];
                        let schema = vec![DataType::Varchar];
                        let serde = OrderedRowSerde::new(schema, order_types.clone());
                        let varchar = "abcdefghijklmnopq";
                        let row = OwnedRow::new(vec![Some(S::Utf8(varchar.into()))]);
                        let mut row_bytes = vec![];
                        serde.serialize(&row, &mut row_bytes);
                        let encoding_data_size = memcmp_encoding::calculate_encoded_size(
                            &DataType::Varchar,
                            order_types[0],
                            &row_bytes[..],
                        )
                        .unwrap();

                        // [254, 254, 158, 157, 156, 155, 154, 153, 152, 151, 246, 150, 149, 148,
                        // 147, 146, 145, 144, 143, 246, 142, 255, 255, 255, 255, 255, 255, 255,
                        // 254]
                        assert_eq!(12 + varchar.len(), encoding_data_size);
                    }
                }
            }
        }
    }
}
