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

use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::expr::AggKind;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use crate::binder::bind_context::Clause;
use crate::binder::Binder;
use crate::expr::{AggCall, ExprImpl, ExprType, FunctionCall};

impl Binder {
    pub(super) fn bind_function(&mut self, f: Function) -> Result<ExprImpl> {
        let mut inputs = f
            .args
            .into_iter()
            .map(|arg| self.bind_function_arg(arg))
            .flatten_ok()
            .try_collect()?;

        if f.name.0.len() == 1 {
            let function_name = f.name.0.get(0).unwrap().value.as_str();
            let agg_kind = match function_name {
                "count" => Some(AggKind::Count),
                "sum" => Some(AggKind::Sum),
                "min" => Some(AggKind::Min),
                "max" => Some(AggKind::Max),
                "avg" => Some(AggKind::Avg),
                _ => None,
            };
            if let Some(kind) = agg_kind {
                self.ensure_aggregate_allowed()?;
                return Ok(ExprImpl::AggCall(Box::new(AggCall::new(kind, inputs)?)));
            }
            let function_type = match function_name {
                "substr" => ExprType::Substr,
                "length" => ExprType::Length,
                "like" => ExprType::Like,
                "upper" => ExprType::Upper,
                "lower" => ExprType::Lower,
                "trim" => ExprType::Trim,
                "replace" => ExprType::Replace,
                "position" => ExprType::Position,
                "ltrim" => ExprType::Ltrim,
                "rtrim" => ExprType::Rtrim,
                "case" => ExprType::Case,
                "is true" => ExprType::IsTrue,
                "is not true" => ExprType::IsNotTrue,
                "is false" => ExprType::IsFalse,
                "is not false" => ExprType::IsNotFalse,
                "is null" => ExprType::IsNull,
                "is not null" => ExprType::IsNotNull,
                "round" => {
                    inputs = Self::rewrite_round_args(inputs);
                    ExprType::RoundDigit
                }
                _ => {
                    return Err(ErrorCode::NotImplementedError(format!(
                        "unsupported function: {:?}",
                        function_name
                    ))
                    .into())
                }
            };
            Ok(FunctionCall::try_new(function_name, function_type, inputs)?.into())
        } else {
            Err(
                ErrorCode::NotImplementedError(format!("unsupported function: {:?}", f.name))
                    .into(),
            )
        }
    }

    /// Rewrite the arguments to be consistent with the `round` signature:
    /// - round(Decimal, Int32) -> Decimal
    fn rewrite_round_args(mut inputs: Vec<ExprImpl>) -> Vec<ExprImpl> {
        if inputs.len() == 2 {
            let digits = inputs.pop().unwrap();
            let input = inputs.pop().unwrap();
            vec![
                input.ensure_type(DataType::Decimal),
                digits.ensure_type(DataType::Int32),
            ]
        } else {
            inputs
        }
    }

    fn ensure_aggregate_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            if clause == Clause::Values || clause == Clause::Where {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "aggregate functions are not allowed in {}",
                    clause
                ))
                .into());
            }
        }
        Ok(())
    }

    fn bind_function_expr_arg(&mut self, arg_expr: FunctionArgExpr) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_) => todo!(),
            FunctionArgExpr::ExprQualifiedWildcard(_, _) => todo!(),
            FunctionArgExpr::Wildcard => Ok(vec![]),
        }
    }

    fn bind_function_arg(&mut self, arg: FunctionArg) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => todo!(),
        }
    }
}
