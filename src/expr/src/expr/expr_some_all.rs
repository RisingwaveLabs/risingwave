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

use std::sync::Arc;

use itertools::{multizip, Itertools};
use risingwave_common::array::{Array, ArrayMeta, ArrayRef, BoolArray, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar, ScalarImpl, ScalarRefImpl};
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::Result;

#[derive(Debug)]
pub struct SomeAllExpression {
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    expr_type: Type,
    func: BoxedExpression,
}

impl SomeAllExpression {
    pub fn new(
        left_expr: BoxedExpression,
        right_expr: BoxedExpression,
        expr_type: Type,
        func: BoxedExpression,
    ) -> Self {
        SomeAllExpression {
            left_expr,
            right_expr,
            expr_type,
            func,
        }
    }

    fn resolve_boolean_vec(&self, boolean_vec: Vec<Option<bool>>) -> Option<bool> {
        if boolean_vec.is_empty() {
            return None;
        }

        match self.expr_type {
            Type::Some => {
                if boolean_vec.iter().any(|b| b.unwrap_or(false)) {
                    Some(true)
                } else if boolean_vec.iter().any(|b| b.is_none()) {
                    None
                } else {
                    Some(false)
                }
            }
            Type::All => {
                if boolean_vec.iter().all(|b| b.unwrap_or(false)) {
                    Some(true)
                } else if boolean_vec.iter().any(|b| !b.unwrap_or(true)) {
                    Some(false)
                } else {
                    None
                }
            }
            _ => unreachable!(),
        }
    }
}

impl Expression for SomeAllExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let arr_left = self.left_expr.eval_checked(data_chunk)?;
        let arr_right = self.right_expr.eval_checked(data_chunk)?;
        let bitmap = data_chunk.get_visibility_ref();
        let mut num_array = Vec::with_capacity(data_chunk.capacity());

        let arr_right_inner = arr_right.as_list();
        let ArrayMeta::List { datatype } = arr_right_inner.array_meta() else {
            unreachable!()
        };
        let capacity = arr_right_inner
            .iter()
            .flatten()
            .map(|list_ref| list_ref.flatten().len())
            .sum();

        let mut unfolded_arr_left_builder = arr_left.create_builder(capacity);
        let mut unfolded_arr_right_builder = datatype.create_array_builder(capacity);

        let mut unfolded_left_right =
            |left: Option<ScalarRefImpl<'_>>,
             right: Option<ScalarRefImpl<'_>>,
             num_array: &mut Vec<usize>| {
                let datum_left = left.map(|s| s.into_scalar_impl());
                let datum_right = right.map(|s| s.into_scalar_impl());
                if datum_right.is_none() {
                    num_array.push(0);
                    return;
                }

                let datum_right = datum_right.unwrap();
                match datum_right {
                    ScalarImpl::List(array) => {
                        let len = array.values().len();
                        num_array.push(len);
                        unfolded_arr_left_builder.append_datum_n(len, datum_left.as_ref());
                        for item in array.values() {
                            unfolded_arr_right_builder.append_datum(item.as_ref());
                        }
                    }
                    _ => unreachable!(),
                }
            };

        match bitmap {
            Some(bitmap) => {
                for ((left, right), visible) in
                    multizip((arr_left.iter(), arr_right.iter())).zip_eq(bitmap.iter())
                {
                    if !visible {
                        num_array.push(0);
                        continue;
                    }
                    unfolded_left_right(left, right, &mut num_array);
                }
            }
            None => {
                for (left, right) in multizip((arr_left.iter(), arr_right.iter())) {
                    unfolded_left_right(left, right, &mut num_array);
                }
            }
        }

        let data_chunk = DataChunk::new(
            vec![
                unfolded_arr_left_builder.finish().into(),
                unfolded_arr_right_builder.finish().into(),
            ],
            capacity,
        );

        let func_results = self.func.eval(&data_chunk)?;
        let mut func_results_iter = func_results.as_bool().iter();
        Ok(Arc::new(
            num_array
                .into_iter()
                .map(|num| {
                    self.resolve_boolean_vec(func_results_iter.by_ref().take(num).collect_vec())
                })
                .collect::<BoolArray>()
                .into(),
        ))
    }

    fn eval_row(&self, row: &OwnedRow) -> Result<Datum> {
        let datum_left = self.left_expr.eval_row(row)?;
        let datum_right = self.right_expr.eval_row(row)?;
        if let Some(array) = datum_right {
            match array {
                ScalarImpl::List(array) => {
                    let scalar_vec = array
                        .values()
                        .iter()
                        .map(|d| {
                            self.func
                                .eval_row(&OwnedRow::new(vec![datum_left.clone(), d.clone()]))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let boolean_vec = scalar_vec
                        .into_iter()
                        .map(|scalar_ref| scalar_ref.map(|s| s.into_bool()))
                        .collect_vec();
                    Ok(self
                        .resolve_boolean_vec(boolean_vec)
                        .map(|b| b.to_scalar_value()))
                }
                _ => unreachable!(),
            }
        } else {
            Ok(None)
        }
    }
}
