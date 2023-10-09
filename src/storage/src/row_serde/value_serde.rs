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

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable).
use std::sync::Arc;

use either::for_both;
use futures::FutureExt;
use itertools::Itertools;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::row::{OwnedRow, RowDeserializer as BasicDeserializer};
use risingwave_common::types::*;
use risingwave_common::util::value_encoding::column_aware_row_encoding::{
    ColumnAwareSerde, Deserializer, Serializer,
};
use risingwave_common::util::value_encoding::error::ValueEncodingError;
use risingwave_common::util::value_encoding::{
    BasicSerde, BasicSerializer, EitherSerde, ValueRowDeserializer, ValueRowSerdeKind,
    ValueRowSerializer,
};
use risingwave_expr::expr::build_from_prost;
use risingwave_pb::plan_common::column_desc::GeneratedOrDefaultColumn;
use risingwave_pb::plan_common::DefaultColumnDesc;

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

/// Part of `ValueRowSerde` that implements `new` a serde given `column_ids` and `schema`
pub trait ValueRowSerdeNew: Clone {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> Self;
}

/// The compound trait used in `StateTableInner`, implemented by `BasicSerde` and `ColumnAwareSerde`
pub trait ValueRowSerde:
    ValueRowSerializer + ValueRowDeserializer + ValueRowSerdeNew + Sync + Send + 'static
{
    fn kind(&self) -> ValueRowSerdeKind;
}

impl ValueRowSerdeNew for EitherSerde {
    fn new(_value_indices: Arc<[usize]>, _table_columns: Arc<[ColumnDesc]>) -> EitherSerde {
        unreachable!("should construct manually")
    }
}

impl ValueRowSerdeNew for BasicSerde {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> BasicSerde {
        BasicSerde {
            serializer: BasicSerializer {},
            deserializer: BasicDeserializer::new(
                value_indices
                    .iter()
                    .map(|idx| table_columns[*idx].data_type.clone())
                    .collect_vec(),
            ),
        }
    }
}

impl ValueRowSerde for EitherSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        for_both!(&self.0, s => s.kind())
    }
}

impl ValueRowSerde for BasicSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::Basic
    }
}

impl ValueRowSerdeNew for ColumnAwareSerde {
    fn new(value_indices: Arc<[usize]>, table_columns: Arc<[ColumnDesc]>) -> ColumnAwareSerde {
        let column_ids = value_indices
            .iter()
            .map(|idx| table_columns[*idx].column_id)
            .collect_vec();
        let schema = value_indices
            .iter()
            .map(|idx| table_columns[*idx].data_type.clone())
            .collect_vec();
        if cfg!(debug_assertions) {
            let duplicates = column_ids.iter().duplicates().collect_vec();
            if !duplicates.is_empty() {
                panic!("duplicated column ids: {duplicates:?}");
            }
        }

        let column_with_default = table_columns.iter().enumerate().filter_map(|(i, c)| {
            if c.is_default() {
                if let GeneratedOrDefaultColumn::DefaultColumn(DefaultColumnDesc { expr }) =
                    c.generated_or_default_column.clone().unwrap()
                {
                    Some((
                        i,
                        build_from_prost(&expr.expect("expr should not be none"))
                            .expect("build_from_prost error")
                            .eval_row_infallible(&OwnedRow::empty())
                            .now_or_never()
                            .expect("constant expression should not be async"),
                    ))
                } else {
                    unreachable!()
                }
            } else {
                None
            }
        });

        let serializer = Serializer::new(&column_ids);
        let deserializer = Deserializer::new(&column_ids, schema.into(), column_with_default);
        ColumnAwareSerde {
            serializer,
            deserializer,
        }
    }
}

impl ValueRowSerde for ColumnAwareSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::ColumnAware
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::ColumnId;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::ScalarImpl::*;
    use risingwave_common::util::value_encoding::column_aware_row_encoding;

    use super::*;

    #[test]
    fn test_row_encoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let row2 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abd".into()))]);
        let row3 = OwnedRow::new(vec![Some(Int16(6)), Some(Utf8("abc".into()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        let serializer = column_aware_row_encoding::Serializer::new(&column_ids);
        for row in &rows {
            let row_bytes = serializer.serialize(row);
            array.push(row_bytes);
        }
        let zero_le_bytes = 0_i32.to_le_bytes();
        let one_le_bytes = 1_i32.to_le_bytes();

        assert_eq!(
            array[0],
            [
                0b10000001, // flag mid WW mid BB
                2,
                0,
                0,
                0,                // column nums
                zero_le_bytes[0], // start id 0
                zero_le_bytes[1],
                zero_le_bytes[2],
                zero_le_bytes[3],
                one_le_bytes[0], // start id 1
                one_le_bytes[1],
                one_le_bytes[2],
                one_le_bytes[3],
                0, // offset0: 0
                2, // offset1: 2
                5, // i16: 5
                0,
                3, // str: abc
                0,
                0,
                0,
                b'a',
                b'b',
                b'c'
            ]
        );
    }
    #[test]
    fn test_row_decoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let serializer = column_aware_row_encoding::Serializer::new(&column_ids);
        let row_bytes = serializer.serialize(row1);
        let data_types = vec![DataType::Int16, DataType::Varchar];
        let deserializer = column_aware_row_encoding::Deserializer::new(
            &column_ids[..],
            Arc::from(data_types.into_boxed_slice()),
            std::iter::empty(),
        );
        let decoded = deserializer.deserialize(&row_bytes[..]);
        assert_eq!(
            decoded.unwrap(),
            vec![Some(Int16(5)), Some(Utf8("abc".into()))]
        );
    }
    #[test]
    fn test_row_hard1() {
        let row = OwnedRow::new(vec![Some(Int16(233)); 20000]);
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..20000),
            Arc::from_iter(
                (0..20000).map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int16)),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), vec![Some(Int16(233)); 20000]);
    }
    #[test]
    fn test_row_hard2() {
        let mut data = vec![Some(Int16(233)); 5000];
        data.extend(vec![None; 5000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 5000]);
        data.extend(vec![None; 5000]);
        let row = OwnedRow::new(data.clone());
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..20000),
            Arc::from_iter(
                (0..10000)
                    .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int16))
                    .chain(
                        (10000..20000)
                            .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Varchar)),
                    ),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }
    #[test]
    fn test_row_hard3() {
        let mut data = vec![Some(Int64(233)); 500000];
        data.extend(vec![None; 250000]);
        data.extend(vec![Some(Utf8("risingwave risingwave".into())); 250000]);
        let row = OwnedRow::new(data.clone());
        let serde = ColumnAwareSerde::new(
            Arc::from_iter(0..1000000),
            Arc::from_iter(
                (0..500000)
                    .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Int64))
                    .chain(
                        (500000..1000000)
                            .map(|id| ColumnDesc::unnamed(ColumnId::new(id), DataType::Varchar)),
                    ),
            ),
        );
        let encoded_bytes = serde.serialize(row);
        let decoded_row = serde.deserialize(&encoded_bytes);
        assert_eq!(decoded_row.unwrap(), data);
    }
}
