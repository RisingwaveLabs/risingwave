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

//! For expression that only accept two nullable arguments as input.

use risingwave_common::array::*;
use risingwave_common::types::DataType;
use risingwave_pb::expr::expr_node::Type;

use super::BoxedExpression;
use crate::expr::template::BinaryNullableExpression;
use crate::for_all_cmp_variants;
use crate::vector_op::array_access::array_access;
use crate::vector_op::cmp::{general_is_distinct_from, str_is_distinct_from};
use crate::vector_op::conjunction::{and, or};

macro_rules! gen_nullable_cmp_impl {
    ([$l:expr, $r:expr, $ret:expr], $( { $i1:ident, $i2:ident, $cast:ident, $func:ident} ),*) => {
        match ($l.return_type(), $r.return_type()) {
            $(
                ($i1! { type_match_pattern }, $i2! { type_match_pattern }) => {
                    Box::new(
                        BinaryNullableExpression::<
                            $i1! { type_array },
                            $i2! { type_array },
                            BoolArray,
                            _
                        >::new(
                            $l,
                            $r,
                            $ret,
                            $func::<
                                <$i1! { type_array } as Array>::OwnedItem,
                                <$i2! { type_array } as Array>::OwnedItem,
                                <$cast! { type_array } as Array>::OwnedItem
                            >,
                        )
                    )
                }
            ),*
            _ => {
                unimplemented!("The expression ({:?}, {:?}) using vectorized expression framework is not supported yet!", $l.return_type(), $r.return_type())
            }
        }
    };
}

pub fn new_nullable_binary_expr(
    expr_type: Type,
    ret: DataType,
    l: BoxedExpression,
    r: BoxedExpression,
) -> BoxedExpression {
    match expr_type {
        Type::ArrayAccess => build_array_access_expr(ret, l, r),
        Type::And => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, and),
        ),
        Type::Or => Box::new(
            BinaryNullableExpression::<BoolArray, BoolArray, BoolArray, _>::new(l, r, ret, or),
        ),
        Type::IsDistinctFrom => new_distinct_from_expr(l, r, ret),
        tp => {
            unimplemented!(
                "The expression {:?} using vectorized expression framework is not supported yet!",
                tp
            )
        }
    }
}

fn build_array_access_expr(ret: DataType, l: BoxedExpression, r: BoxedExpression) -> BoxedExpression {

    macro_rules! array_access_expression {
        ($array:ty) => {
            Box::new(
                BinaryNullableExpression::<ListArray, I32Array, $array, _>::new(
                    l,
                    r,
                    ret,
                    array_access,
                ),
            )
        }
    }

    match ret {
        DataType::Boolean => array_access_expression!(BoolArray),
        DataType::Int16 => array_access_expression!(I16Array),
        DataType::Int32 => array_access_expression!(I32Array),
        DataType::Int64 => array_access_expression!(I64Array),
        DataType::Float32 => array_access_expression!(F32Array),
        DataType::Float64 => array_access_expression!(F64Array),
        DataType::Decimal => array_access_expression!(DecimalArray),
        DataType::Date => array_access_expression!(NaiveDateArray),
        DataType::Varchar => array_access_expression!(Utf8Array),
        DataType::Time => array_access_expression!(NaiveTimeArray),
        DataType::Timestamp => array_access_expression!(NaiveDateTimeArray),
        DataType::Timestampz => array_access_expression!(PrimitiveArray::<i64>),
        DataType::Interval => array_access_expression!(IntervalArray),
        DataType::Struct { .. } => array_access_expression!(StructArray),
        DataType::List { .. } => array_access_expression!(Utf8Array),
    }
}

pub fn new_distinct_from_expr(
    l: BoxedExpression,
    r: BoxedExpression,
    ret: DataType,
) -> BoxedExpression {
    use crate::expr::data_types::*;

    match (l.return_type(), r.return_type()) {
        (DataType::Varchar, DataType::Varchar) => Box::new(BinaryNullableExpression::<
            Utf8Array,
            Utf8Array,
            BoolArray,
            _,
        >::new(
            l, r, ret, str_is_distinct_from
        )),
        _ => {
            for_all_cmp_variants! {gen_nullable_cmp_impl, l, r, ret, general_is_distinct_from}
        }
    }
}
