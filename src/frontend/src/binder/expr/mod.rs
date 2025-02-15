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

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, ColumnId, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::types::{DataType, MapType, StructType};
use risingwave_common::util::iter_util::zip_eq_fast;
use risingwave_common::{bail_no_function, bail_not_implemented, not_implemented};
use risingwave_pb::plan_common::{AdditionalColumn, ColumnDescVersion};
use risingwave_sqlparser::ast::{
    Array, BinaryOperator, DataType as AstDataType, EscapeChar, Expr, Function, JsonPredicateType,
    ObjectName, Query, StructField, TrimWhereField, UnaryOperator,
};

use crate::binder::expr::function::is_sys_function_without_args;
use crate::binder::Binder;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{Expr as _, ExprImpl, ExprType, FunctionCall, InputRef, Parameter, SubqueryKind};
use crate::handler::create_sql_function::SQL_UDF_PATTERN;

mod binary_op;
mod column;
mod function;
mod order_by;
mod subquery;
mod value;

/// The limit arms for case-when expression
/// When the number of condition arms exceed
/// this limit, we will try optimize the case-when
/// expression to `ConstantLookupExpression`
/// Check `case.rs` for details.
const CASE_WHEN_ARMS_OPTIMIZE_LIMIT: usize = 30;

impl Binder {
    /// Bind an expression with `bind_expr_inner`, attach the original expression
    /// to the error message.
    ///
    /// This may only be called at the root of the expression tree or when crossing
    /// the boundary of a subquery. Otherwise, the source chain might be too deep
    /// and confusing to the user.
    // TODO(error-handling): use a dedicated error type during binding to make it clear.
    pub fn bind_expr(&mut self, expr: Expr) -> Result<ExprImpl> {
        self.bind_expr_inner(expr.clone()).map_err(|e| {
            RwError::from(ErrorCode::BindErrorRoot {
                expr: expr.to_string(),
                error: Box::new(e),
            })
        })
    }

    fn bind_expr_inner(&mut self, expr: Expr) -> Result<ExprImpl> {
        match expr {
            // literal
            Expr::Value(v) => Ok(ExprImpl::Literal(Box::new(self.bind_value(v)?))),
            Expr::TypedString { data_type, value } => {
                let s: ExprImpl = self.bind_string(value)?.into();
                s.cast_explicit(bind_data_type(&data_type)?)
                    .map_err(Into::into)
            }
            Expr::Row(exprs) => self.bind_row(exprs),
            // input ref
            Expr::Identifier(ident) => {
                if is_sys_function_without_args(&ident) {
                    // Rewrite a system variable to a function call, e.g. `SELECT current_schema;`
                    // will be rewritten to `SELECT current_schema();`.
                    // NOTE: Here we don't 100% follow the behavior of Postgres, as it doesn't
                    // allow `session_user()` while we do.
                    self.bind_function(Function::no_arg(ObjectName(vec![ident])))
                } else if let Some(ref lambda_args) = self.context.lambda_args {
                    // We don't support capture, so if the expression is in the lambda context,
                    // we'll not bind it for table columns.
                    if let Some((arg_idx, arg_type)) = lambda_args.get(&ident.real_value()) {
                        Ok(InputRef::new(*arg_idx, arg_type.clone()).into())
                    } else {
                        Err(
                            ErrorCode::ItemNotFound(format!("Unknown arg: {}", ident.real_value()))
                                .into(),
                        )
                    }
                } else if let Some(ctx) = self.secure_compare_context.as_ref() {
                    // Currently, the generated columns are not supported yet. So the ident here should only be one of the following
                    // - `headers`
                    // - secret name
                    // - the name of the payload column
                    // TODO(Kexiang): Generated columns or INCLUDE clause should be supported.
                    if ident.real_value() == *"headers" {
                        Ok(InputRef::new(0, DataType::Jsonb).into())
                    } else if ctx.secret_name.is_some()
                        && ident.real_value() == *ctx.secret_name.as_ref().unwrap()
                    {
                        Ok(InputRef::new(1, DataType::Varchar).into())
                    } else if ident.real_value() == ctx.column_name {
                        Ok(InputRef::new(2, DataType::Bytea).into())
                    } else {
                        Err(
                            ErrorCode::ItemNotFound(format!("Unknown arg: {}", ident.real_value()))
                                .into(),
                        )
                    }
                } else {
                    self.bind_column(&[ident])
                }
            }
            Expr::CompoundIdentifier(idents) => self.bind_column(&idents),
            Expr::FieldIdentifier(field_expr, idents) => {
                self.bind_single_field_column(*field_expr, &idents)
            }
            // operators & functions
            Expr::UnaryOp { op, expr } => self.bind_unary_expr(op, *expr),
            Expr::BinaryOp { left, op, right } => self.bind_binary_op(*left, op, *right),
            Expr::Nested(expr) => self.bind_expr_inner(*expr),
            Expr::Array(Array { elem: exprs, .. }) => self.bind_array(exprs),
            Expr::Index { obj, index } => self.bind_index(*obj, *index),
            Expr::ArrayRangeIndex { obj, start, end } => {
                self.bind_array_range_index(*obj, start, end)
            }
            Expr::Function(f) => self.bind_function(f),
            Expr::Subquery(q) => self.bind_subquery_expr(*q, SubqueryKind::Scalar),
            Expr::Exists(q) => self.bind_subquery_expr(*q, SubqueryKind::Existential),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => self.bind_in_subquery(*expr, *subquery, negated),
            // special syntax (except date/time or string)
            Expr::Cast { expr, data_type } => self.bind_cast(*expr, data_type),
            Expr::IsNull(expr) => self.bind_is_operator(ExprType::IsNull, *expr),
            Expr::IsNotNull(expr) => self.bind_is_operator(ExprType::IsNotNull, *expr),
            Expr::IsTrue(expr) => self.bind_is_operator(ExprType::IsTrue, *expr),
            Expr::IsNotTrue(expr) => self.bind_is_operator(ExprType::IsNotTrue, *expr),
            Expr::IsFalse(expr) => self.bind_is_operator(ExprType::IsFalse, *expr),
            Expr::IsNotFalse(expr) => self.bind_is_operator(ExprType::IsNotFalse, *expr),
            Expr::IsUnknown(expr) => self.bind_is_unknown(ExprType::IsNull, *expr),
            Expr::IsNotUnknown(expr) => self.bind_is_unknown(ExprType::IsNotNull, *expr),
            Expr::IsDistinctFrom(left, right) => self.bind_distinct_from(*left, *right),
            Expr::IsNotDistinctFrom(left, right) => self.bind_not_distinct_from(*left, *right),
            Expr::IsJson {
                expr,
                negated,
                item_type,
                unique_keys: false,
            } => self.bind_is_json(*expr, negated, item_type),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.bind_case(operand, conditions, results, else_result),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => self.bind_between(*expr, negated, *low, *high),
            Expr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.bind_like(ExprType::Like, *expr, negated, *pattern, escape_char),
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => self.bind_like(ExprType::ILike, *expr, negated, *pattern, escape_char),
            Expr::SimilarTo {
                expr,
                negated,
                pattern,
                escape_char,
            } => self.bind_similar_to(*expr, negated, *pattern, escape_char),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.bind_in_list(*expr, list, negated),
            // special syntax for date/time
            Expr::Extract { field, expr } => self.bind_extract(field, *expr),
            Expr::AtTimeZone {
                timestamp,
                time_zone,
            } => self.bind_at_time_zone(*timestamp, *time_zone),
            // special syntax for string
            Expr::Trim {
                expr,
                trim_where,
                trim_what,
            } => self.bind_trim(*expr, trim_where, trim_what),
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
            } => self.bind_substring(*expr, substring_from, substring_for),
            Expr::Position { substring, string } => self.bind_position(*substring, *string),
            Expr::Overlay {
                expr,
                new_substring,
                start,
                count,
            } => self.bind_overlay(*expr, *new_substring, *start, count),
            Expr::Parameter { index } => self.bind_parameter(index),
            Expr::Collate { expr, collation } => self.bind_collate(*expr, collation),
            Expr::ArraySubquery(q) => self.bind_subquery_expr(*q, SubqueryKind::Array),
            Expr::Map { entries } => self.bind_map(entries),
            Expr::IsJson {
                unique_keys: true, ..
            }
            | Expr::SomeOp(_)
            | Expr::AllOp(_)
            | Expr::TryCast { .. }
            | Expr::GroupingSets(_)
            | Expr::Cube(_)
            | Expr::Rollup(_)
            | Expr::LambdaFunction { .. } => {
                bail_not_implemented!(issue = 112, "unsupported expression {:?}", expr)
            }
        }
    }

    pub(super) fn bind_extract(&mut self, field: String, expr: Expr) -> Result<ExprImpl> {
        let arg = self.bind_expr_inner(expr)?;
        let arg_type = arg.return_type();
        Ok(FunctionCall::new(
            ExprType::Extract,
            vec![self.bind_string(field.clone())?.into(), arg],
        )
        .map_err(|_| {
            not_implemented!(
                issue = 112,
                "function extract({} from {:?}) doesn't exist",
                field,
                arg_type
            )
        })?
        .into())
    }

    pub(super) fn bind_at_time_zone(&mut self, input: Expr, time_zone: Expr) -> Result<ExprImpl> {
        let input = self.bind_expr_inner(input)?;
        let time_zone = self.bind_expr_inner(time_zone)?;
        FunctionCall::new(ExprType::AtTimeZone, vec![input, time_zone]).map(Into::into)
    }

    pub(super) fn bind_in_list(
        &mut self,
        expr: Expr,
        list: Vec<Expr>,
        negated: bool,
    ) -> Result<ExprImpl> {
        let left = self.bind_expr_inner(expr)?;
        let mut bound_expr_list = vec![left.clone()];
        let mut non_const_exprs = vec![];
        for elem in list {
            let expr = self.bind_expr_inner(elem)?;
            match expr.is_const() {
                true => bound_expr_list.push(expr),
                false => non_const_exprs.push(expr),
            }
        }
        let mut ret = FunctionCall::new(ExprType::In, bound_expr_list)?.into();
        // Non-const exprs are not part of IN-expr in backend and rewritten into OR-Equal-exprs.
        for expr in non_const_exprs {
            ret = FunctionCall::new(
                ExprType::Or,
                vec![
                    ret,
                    FunctionCall::new(ExprType::Equal, vec![left.clone(), expr])?.into(),
                ],
            )?
            .into();
        }
        if negated {
            Ok(FunctionCall::new_unchecked(ExprType::Not, vec![ret], DataType::Boolean).into())
        } else {
            Ok(ret)
        }
    }

    pub(super) fn bind_in_subquery(
        &mut self,
        expr: Expr,
        subquery: Query,
        negated: bool,
    ) -> Result<ExprImpl> {
        let bound_expr = self.bind_expr_inner(expr)?;
        let bound_subquery = self.bind_subquery_expr(subquery, SubqueryKind::In(bound_expr))?;
        if negated {
            Ok(
                FunctionCall::new_unchecked(ExprType::Not, vec![bound_subquery], DataType::Boolean)
                    .into(),
            )
        } else {
            Ok(bound_subquery)
        }
    }

    pub(super) fn bind_is_json(
        &mut self,
        expr: Expr,
        negated: bool,
        item_type: JsonPredicateType,
    ) -> Result<ExprImpl> {
        let mut args = vec![self.bind_expr_inner(expr)?];
        // Avoid `JsonPredicateType::to_string` so that we decouple sqlparser from expr execution
        let type_symbol = match item_type {
            JsonPredicateType::Value => None,
            JsonPredicateType::Array => Some("ARRAY"),
            JsonPredicateType::Object => Some("OBJECT"),
            JsonPredicateType::Scalar => Some("SCALAR"),
        };
        if let Some(s) = type_symbol {
            args.push(ExprImpl::literal_varchar(s.into()));
        }

        let is_json = FunctionCall::new(ExprType::IsJson, args)?.into();
        if negated {
            Ok(FunctionCall::new(ExprType::Not, vec![is_json])?.into())
        } else {
            Ok(is_json)
        }
    }

    pub(super) fn bind_unary_expr(&mut self, op: UnaryOperator, expr: Expr) -> Result<ExprImpl> {
        let func_type = match op {
            UnaryOperator::Not => ExprType::Not,
            UnaryOperator::Minus => ExprType::Neg,
            UnaryOperator::PGAbs => ExprType::Abs,
            UnaryOperator::PGBitwiseNot => ExprType::BitwiseNot,
            UnaryOperator::Plus => {
                return self.rewrite_positive(expr);
            }
            UnaryOperator::PGSquareRoot => ExprType::Sqrt,
            UnaryOperator::PGCubeRoot => ExprType::Cbrt,
            _ => bail_not_implemented!(issue = 112, "unsupported unary expression: {:?}", op),
        };
        let expr = self.bind_expr_inner(expr)?;
        FunctionCall::new(func_type, vec![expr]).map(|f| f.into())
    }

    /// Directly returns the expression itself if it is a positive number.
    fn rewrite_positive(&mut self, expr: Expr) -> Result<ExprImpl> {
        let expr = self.bind_expr_inner(expr)?;
        let return_type = expr.return_type();
        if return_type.is_numeric() {
            return Ok(expr);
        }
        Err(ErrorCode::InvalidInputSyntax(format!("+ {:?}", return_type)).into())
    }

    pub(super) fn bind_trim(
        &mut self,
        expr: Expr,
        // BOTH | LEADING | TRAILING
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<Expr>>,
    ) -> Result<ExprImpl> {
        let mut inputs = vec![self.bind_expr_inner(expr)?];
        let func_type = match trim_where {
            Some(TrimWhereField::Both) => ExprType::Trim,
            Some(TrimWhereField::Leading) => ExprType::Ltrim,
            Some(TrimWhereField::Trailing) => ExprType::Rtrim,
            None => ExprType::Trim,
        };
        if let Some(t) = trim_what {
            inputs.push(self.bind_expr_inner(*t)?);
        }
        Ok(FunctionCall::new(func_type, inputs)?.into())
    }

    fn bind_substring(
        &mut self,
        expr: Expr,
        substring_from: Option<Box<Expr>>,
        substring_for: Option<Box<Expr>>,
    ) -> Result<ExprImpl> {
        let mut args = vec![
            self.bind_expr_inner(expr)?,
            match substring_from {
                Some(expr) => self.bind_expr_inner(*expr)?,
                None => ExprImpl::literal_int(1),
            },
        ];
        if let Some(expr) = substring_for {
            args.push(self.bind_expr_inner(*expr)?);
        }
        FunctionCall::new(ExprType::Substr, args).map(|f| f.into())
    }

    fn bind_position(&mut self, substring: Expr, string: Expr) -> Result<ExprImpl> {
        let args = vec![
            // Note that we reverse the order of arguments.
            self.bind_expr_inner(string)?,
            self.bind_expr_inner(substring)?,
        ];
        FunctionCall::new(ExprType::Position, args).map(Into::into)
    }

    fn bind_overlay(
        &mut self,
        expr: Expr,
        new_substring: Expr,
        start: Expr,
        count: Option<Box<Expr>>,
    ) -> Result<ExprImpl> {
        let mut args = vec![
            self.bind_expr_inner(expr)?,
            self.bind_expr_inner(new_substring)?,
            self.bind_expr_inner(start)?,
        ];
        if let Some(count) = count {
            args.push(self.bind_expr_inner(*count)?);
        }
        FunctionCall::new(ExprType::Overlay, args).map(|f| f.into())
    }

    fn bind_parameter(&mut self, index: u64) -> Result<ExprImpl> {
        // Special check for sql udf
        // Note: This is specific to sql udf with unnamed parameters, since the
        // parameters will be parsed and treated as `Parameter`.
        // For detailed explanation, consider checking `bind_column`.
        if self.udf_context.global_count() != 0 {
            if let Some(expr) = self.udf_context.get_expr(&format!("${index}")) {
                return Ok(expr.clone());
            }
            // Same as `bind_column`, the error message here
            // help with hint display when invalid definition occurs
            return Err(ErrorCode::BindError(format!(
                "{SQL_UDF_PATTERN} failed to find unnamed parameter ${index}"
            ))
            .into());
        }

        Ok(Parameter::new(index, self.param_types.clone()).into())
    }

    /// Bind `expr (not) between low and high`
    pub(super) fn bind_between(
        &mut self,
        expr: Expr,
        negated: bool,
        low: Expr,
        high: Expr,
    ) -> Result<ExprImpl> {
        let expr = self.bind_expr_inner(expr)?;
        let low = self.bind_expr_inner(low)?;
        let high = self.bind_expr_inner(high)?;

        let func_call = if negated {
            // negated = true: expr < low or expr > high
            FunctionCall::new_unchecked(
                ExprType::Or,
                vec![
                    FunctionCall::new(ExprType::LessThan, vec![expr.clone(), low])?.into(),
                    FunctionCall::new(ExprType::GreaterThan, vec![expr, high])?.into(),
                ],
                DataType::Boolean,
            )
        } else {
            // negated = false: expr >= low and expr <= high
            FunctionCall::new_unchecked(
                ExprType::And,
                vec![
                    FunctionCall::new(ExprType::GreaterThanOrEqual, vec![expr.clone(), low])?
                        .into(),
                    FunctionCall::new(ExprType::LessThanOrEqual, vec![expr, high])?.into(),
                ],
                DataType::Boolean,
            )
        };

        Ok(func_call.into())
    }

    fn bind_like(
        &mut self,
        expr_type: ExprType,
        expr: Expr,
        negated: bool,
        pattern: Expr,
        escape_char: Option<EscapeChar>,
    ) -> Result<ExprImpl> {
        if matches!(pattern, Expr::AllOp(_) | Expr::SomeOp(_)) {
            if escape_char.is_some() {
                // PostgreSQL also don't support the pattern due to the complexity of implementation.
                // The SQL will failed on PostgreSQL 16.1:
                // ```sql
                // select 'a' like any(array[null]) escape '';
                // ```
                bail_not_implemented!(
                    "LIKE with both ALL|ANY pattern and escape character is not supported"
                )
            }
            // Use the `bind_binary_op` path to handle the ALL|ANY pattern.
            let op = match (expr_type, negated) {
                (ExprType::Like, false) => BinaryOperator::PGLikeMatch,
                (ExprType::Like, true) => BinaryOperator::PGNotLikeMatch,
                (ExprType::ILike, false) => BinaryOperator::PGILikeMatch,
                (ExprType::ILike, true) => BinaryOperator::PGNotILikeMatch,
                _ => unreachable!(),
            };
            return self.bind_binary_op(expr, op, pattern);
        }
        let expr = self.bind_expr_inner(expr)?;
        let pattern = self.bind_expr_inner(pattern)?;
        match (expr.return_type(), pattern.return_type()) {
            (DataType::Varchar, DataType::Varchar) => {}
            (string_ty, pattern_ty) => match expr_type {
                ExprType::Like => bail_no_function!("like({}, {})", string_ty, pattern_ty),
                ExprType::ILike => bail_no_function!("ilike({}, {})", string_ty, pattern_ty),
                _ => unreachable!(),
            },
        }
        let args = match escape_char {
            Some(escape_char) => {
                let escape_char = ExprImpl::literal_varchar(escape_char.to_string());
                vec![expr, pattern, escape_char]
            }
            None => vec![expr, pattern],
        };
        let func_call = FunctionCall::new_unchecked(expr_type, args, DataType::Boolean);
        let func_call = if negated {
            FunctionCall::new_unchecked(ExprType::Not, vec![func_call.into()], DataType::Boolean)
        } else {
            func_call
        };
        Ok(func_call.into())
    }

    /// Bind `<expr> [ NOT ] SIMILAR TO <pat> ESCAPE <esc_text>`
    pub(super) fn bind_similar_to(
        &mut self,
        expr: Expr,
        negated: bool,
        pattern: Expr,
        escape_char: Option<EscapeChar>,
    ) -> Result<ExprImpl> {
        let expr = self.bind_expr_inner(expr)?;
        let pattern = self.bind_expr_inner(pattern)?;

        let esc_inputs = if let Some(escape_char) = escape_char {
            let escape_char = ExprImpl::literal_varchar(escape_char.to_string());
            vec![pattern, escape_char]
        } else {
            vec![pattern]
        };

        let esc_call =
            FunctionCall::new_unchecked(ExprType::SimilarToEscape, esc_inputs, DataType::Varchar);

        let regex_call = FunctionCall::new_unchecked(
            ExprType::RegexpEq,
            vec![expr, esc_call.into()],
            DataType::Boolean,
        );
        let func_call = if negated {
            FunctionCall::new_unchecked(ExprType::Not, vec![regex_call.into()], DataType::Boolean)
        } else {
            regex_call
        };

        Ok(func_call.into())
    }

    /// The optimization check for the following case-when expression pattern
    /// e.g., select case 1 when (...) then (...) else (...) end;
    fn check_constant_case_when_optimization(
        &mut self,
        conditions: Vec<Expr>,
        results_expr: Vec<ExprImpl>,
        operand: Option<Box<Expr>>,
        fallback: Option<ExprImpl>,
        constant_case_when_eval_inputs: &mut Vec<ExprImpl>,
    ) -> bool {
        // The operand value to be compared later
        let operand_value;

        if let Some(operand) = operand {
            let Ok(operand) = self.bind_expr_inner(*operand) else {
                return false;
            };
            if !operand.is_const() {
                return false;
            }
            operand_value = operand;
        } else {
            return false;
        }

        for (condition, result) in zip_eq_fast(conditions, results_expr) {
            if let Expr::Value(_) = condition.clone() {
                let Ok(res) = self.bind_expr_inner(condition.clone()) else {
                    return false;
                };
                // Found a match
                if res == operand_value {
                    constant_case_when_eval_inputs.push(result);
                    return true;
                }
            } else {
                return false;
            }
        }

        // Otherwise this will eventually go through fallback arm
        debug_assert!(
            constant_case_when_eval_inputs.is_empty(),
            "expect `inputs` to be empty"
        );

        let Some(fallback) = fallback else {
            return false;
        };

        constant_case_when_eval_inputs.push(fallback);
        true
    }

    /// Helper function to compare or set column identifier
    /// used in `check_convert_simple_form`
    fn compare_or_set(col_expr: &mut Option<Expr>, test_expr: Expr) -> bool {
        let Expr::Identifier(test_ident) = test_expr else {
            return false;
        };
        if let Some(expr) = col_expr {
            let Expr::Identifier(ident) = expr else {
                return false;
            };
            if ident.real_value() != test_ident.real_value() {
                return false;
            }
        } else {
            *col_expr = Some(Expr::Identifier(test_ident));
        }
        true
    }

    /// left expression and right expression must be either:
    /// `<constant> <Eq> <identifier>` or `<identifier> <Eq> <constant>`
    /// used in `check_convert_simple_form`
    fn check_invariant(left: Expr, op: BinaryOperator, right: Expr) -> bool {
        if op != BinaryOperator::Eq {
            return false;
        }
        if let Expr::Identifier(_) = left {
            // <identifier> <Eq> <constant>
            let Expr::Value(_) = right else {
                return false;
            };
        } else {
            // <constant> <Eq> <identifier>
            let Expr::Value(_) = left else {
                return false;
            };
            let Expr::Identifier(_) = right else {
                return false;
            };
        }
        true
    }

    /// Helper function to extract expression out and insert
    /// the corresponding bound version to `inputs`
    /// used in `check_convert_simple_form`
    /// Note: this function will be invoked per arm
    fn try_extract_simple_form(
        &mut self,
        ident_expr: Expr,
        constant_expr: Expr,
        column_expr: &mut Option<Expr>,
        inputs: &mut Vec<ExprImpl>,
    ) -> bool {
        if !Self::compare_or_set(column_expr, ident_expr) {
            return false;
        }
        let Ok(bound_expr) = self.bind_expr_inner(constant_expr) else {
            return false;
        };
        inputs.push(bound_expr);
        true
    }

    /// See if the case when expression in form
    /// `select case when <expr_1 = constant> (...with same pattern...) else <constant> end;`
    /// If so, this expression could also be converted to constant lookup
    fn check_convert_simple_form(
        &mut self,
        conditions: Vec<Expr>,
        results_expr: Vec<ExprImpl>,
        fallback: Option<ExprImpl>,
        constant_lookup_inputs: &mut Vec<ExprImpl>,
    ) -> bool {
        let mut column_expr = None;

        for (condition, result) in zip_eq_fast(conditions, results_expr) {
            if let Expr::BinaryOp { left, op, right } = condition {
                if !Self::check_invariant(*(left.clone()), op.clone(), *(right.clone())) {
                    return false;
                }
                if let Expr::Identifier(_) = *(left.clone()) {
                    if !self.try_extract_simple_form(
                        *left,
                        *right,
                        &mut column_expr,
                        constant_lookup_inputs,
                    ) {
                        return false;
                    }
                } else if !self.try_extract_simple_form(
                    *right,
                    *left,
                    &mut column_expr,
                    constant_lookup_inputs,
                ) {
                    return false;
                }
                constant_lookup_inputs.push(result);
            } else {
                return false;
            }
        }

        // Insert operand first
        let Some(operand) = column_expr else {
            return false;
        };
        let Ok(bound_operand) = self.bind_expr_inner(operand) else {
            return false;
        };
        constant_lookup_inputs.insert(0, bound_operand);

        // fallback insertion
        if let Some(expr) = fallback {
            constant_lookup_inputs.push(expr);
        }

        true
    }

    /// The helper function to check if the current case-when
    /// expression in `bind_case` could be optimized
    /// into `ConstantLookupExpression`
    fn check_bind_case_optimization(
        &mut self,
        conditions: Vec<Expr>,
        results_expr: Vec<ExprImpl>,
        operand: Option<Box<Expr>>,
        fallback: Option<ExprImpl>,
        constant_lookup_inputs: &mut Vec<ExprImpl>,
    ) -> bool {
        if conditions.len() < CASE_WHEN_ARMS_OPTIMIZE_LIMIT {
            return false;
        }

        if let Some(operand) = operand {
            let Ok(operand) = self.bind_expr_inner(*operand) else {
                return false;
            };
            // This optimization should be done in subsequent optimization phase
            // if the operand is const
            // e.g., select case 1 when 1 then 114514 else 1919810 end;
            if operand.is_const() {
                return false;
            }
            constant_lookup_inputs.push(operand);
        } else {
            // Try converting to simple form
            // see the example as illustrated in `check_convert_simple_form`
            return self.check_convert_simple_form(
                conditions,
                results_expr,
                fallback,
                constant_lookup_inputs,
            );
        }

        for (condition, result) in zip_eq_fast(conditions, results_expr) {
            if let Expr::Value(_) = condition.clone() {
                let Ok(input) = self.bind_expr_inner(condition.clone()) else {
                    return false;
                };
                constant_lookup_inputs.push(input);
            } else {
                // If at least one condition is not in the simple form / not constant,
                // we can NOT do the subsequent optimization pass
                return false;
            }

            constant_lookup_inputs.push(result);
        }

        // The fallback arm for case-when expression
        if let Some(expr) = fallback {
            constant_lookup_inputs.push(expr);
        }

        true
    }

    pub(super) fn bind_case(
        &mut self,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    ) -> Result<ExprImpl> {
        let mut inputs = Vec::new();
        let results_expr: Vec<ExprImpl> = results
            .into_iter()
            .map(|expr| self.bind_expr_inner(expr))
            .collect::<Result<_>>()?;
        let else_result_expr = else_result
            .map(|expr| self.bind_expr_inner(*expr))
            .transpose()?;

        let mut constant_lookup_inputs = Vec::new();
        let mut constant_case_when_eval_inputs = Vec::new();

        let constant_case_when_flag = self.check_constant_case_when_optimization(
            conditions.clone(),
            results_expr.clone(),
            operand.clone(),
            else_result_expr.clone(),
            &mut constant_case_when_eval_inputs,
        );

        if constant_case_when_flag {
            // Sanity check
            if constant_case_when_eval_inputs.len() != 1 {
                return Err(ErrorCode::BindError(
                    "expect `constant_case_when_eval_inputs` only contains a single bound expression".to_owned()
                )
                .into());
            }
            // Directly return the first element of the vector
            return Ok(constant_case_when_eval_inputs[0].take());
        }

        // See if the case-when expression can be optimized
        let optimize_flag = self.check_bind_case_optimization(
            conditions.clone(),
            results_expr.clone(),
            operand.clone(),
            else_result_expr.clone(),
            &mut constant_lookup_inputs,
        );

        if optimize_flag {
            return Ok(FunctionCall::new(ExprType::ConstantLookup, constant_lookup_inputs)?.into());
        }

        for (condition, result) in zip_eq_fast(conditions, results_expr) {
            let condition = match operand {
                Some(ref t) => Expr::BinaryOp {
                    left: t.clone(),
                    op: BinaryOperator::Eq,
                    right: Box::new(condition),
                },
                None => condition,
            };
            inputs.push(
                self.bind_expr_inner(condition)
                    .and_then(|expr| expr.enforce_bool_clause("CASE WHEN"))?,
            );
            inputs.push(result);
        }

        // The fallback arm for case-when expression
        if let Some(expr) = else_result_expr {
            inputs.push(expr);
        }

        if inputs.iter().any(ExprImpl::has_table_function) {
            return Err(
                ErrorCode::BindError("table functions are not allowed in CASE".into()).into(),
            );
        }

        Ok(FunctionCall::new(ExprType::Case, inputs)?.into())
    }

    pub(super) fn bind_is_operator(&mut self, func_type: ExprType, expr: Expr) -> Result<ExprImpl> {
        let expr = self.bind_expr_inner(expr)?;
        Ok(FunctionCall::new(func_type, vec![expr])?.into())
    }

    pub(super) fn bind_is_unknown(&mut self, func_type: ExprType, expr: Expr) -> Result<ExprImpl> {
        let expr = self
            .bind_expr_inner(expr)?
            .cast_implicit(DataType::Boolean)?;
        Ok(FunctionCall::new(func_type, vec![expr])?.into())
    }

    pub(super) fn bind_distinct_from(&mut self, left: Expr, right: Expr) -> Result<ExprImpl> {
        let left = self.bind_expr_inner(left)?;
        let right = self.bind_expr_inner(right)?;
        let func_call = FunctionCall::new(ExprType::IsDistinctFrom, vec![left, right]);
        Ok(func_call?.into())
    }

    pub(super) fn bind_not_distinct_from(&mut self, left: Expr, right: Expr) -> Result<ExprImpl> {
        let left = self.bind_expr_inner(left)?;
        let right = self.bind_expr_inner(right)?;
        let func_call = FunctionCall::new(ExprType::IsNotDistinctFrom, vec![left, right]);
        Ok(func_call?.into())
    }

    pub(super) fn bind_cast(&mut self, expr: Expr, data_type: AstDataType) -> Result<ExprImpl> {
        match &data_type {
            // Casting to Regclass type means getting the oid of expr.
            // See https://www.postgresql.org/docs/current/datatype-oid.html.
            AstDataType::Regclass => {
                let input = self.bind_expr_inner(expr)?;
                Ok(input.cast_to_regclass()?)
            }
            AstDataType::Regproc => {
                let lhs = self.bind_expr_inner(expr)?;
                let lhs_ty = lhs.return_type();
                if lhs_ty == DataType::Varchar {
                    // FIXME: Currently, we only allow VARCHAR to be casted to Regproc.
                    // FIXME: Check whether it's a valid proc
                    // FIXME: The return type should be casted to Regproc, but we don't have this type.
                    Ok(lhs)
                } else {
                    Err(ErrorCode::BindError(format!("Can't cast {} to regproc", lhs_ty)).into())
                }
            }
            _ => self.bind_cast_inner(expr, bind_data_type(&data_type)?),
        }
    }

    pub fn bind_cast_inner(&mut self, expr: Expr, data_type: DataType) -> Result<ExprImpl> {
        match (expr, data_type) {
            (Expr::Array(Array { elem: ref expr, .. }), DataType::List(element_type)) => {
                self.bind_array_cast(expr.clone(), element_type)
            }
            (Expr::Map { entries }, DataType::Map(m)) => self.bind_map_cast(entries, m),
            (expr, data_type) => {
                let lhs = self.bind_expr_inner(expr)?;
                lhs.cast_explicit(data_type).map_err(Into::into)
            }
        }
    }

    pub fn bind_collate(&mut self, expr: Expr, collation: ObjectName) -> Result<ExprImpl> {
        if !["C", "POSIX"].contains(&collation.real_value().as_str()) {
            bail_not_implemented!("Collate collation other than `C` or `POSIX` is not implemented");
        }

        let bound_inner = self.bind_expr_inner(expr)?;
        let ret_type = bound_inner.return_type();

        match ret_type {
            DataType::Varchar => {}
            _ => {
                return Err(ErrorCode::NotSupported(
                    format!("{} is not a collatable data type", ret_type),
                    "The only built-in collatable data types are `varchar`, please check your type"
                        .into(),
                )
                .into());
            }
        }

        Ok(bound_inner)
    }
}

/// Given a type `STRUCT<v1 int>`, this function binds the field `v1 int`.
pub fn bind_struct_field(column_def: &StructField) -> Result<ColumnDesc> {
    let field_descs = if let AstDataType::Struct(defs) = &column_def.data_type {
        defs.iter()
            .map(|f| {
                Ok(ColumnDesc::named(
                    f.name.real_value(),
                    ColumnId::new(0), // Literals don't have `column_id`.
                    bind_data_type(&f.data_type)?,
                ))
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![]
    };
    Ok(ColumnDesc {
        data_type: bind_data_type(&column_def.data_type)?,
        column_id: ColumnId::new(0),
        name: column_def.name.real_value(),
        field_descs,
        type_name: "".to_owned(),
        generated_or_default_column: None,
        description: None,
        additional_column: AdditionalColumn { column_type: None },
        version: ColumnDescVersion::LATEST,
        system_column: None,
    })
}

pub fn bind_data_type(data_type: &AstDataType) -> Result<DataType> {
    let new_err = || not_implemented!("unsupported data type: {:}", data_type);
    let data_type = match data_type {
        AstDataType::Boolean => DataType::Boolean,
        AstDataType::SmallInt => DataType::Int16,
        AstDataType::Int => DataType::Int32,
        AstDataType::BigInt => DataType::Int64,
        AstDataType::Real | AstDataType::Float(Some(1..=24)) => DataType::Float32,
        AstDataType::Double | AstDataType::Float(Some(25..=53) | None) => DataType::Float64,
        AstDataType::Float(Some(0 | 54..)) => unreachable!(),
        AstDataType::Decimal(None, None) => DataType::Decimal,
        AstDataType::Varchar | AstDataType::Text => DataType::Varchar,
        AstDataType::Date => DataType::Date,
        AstDataType::Time(false) => DataType::Time,
        AstDataType::Timestamp(false) => DataType::Timestamp,
        AstDataType::Timestamp(true) => DataType::Timestamptz,
        AstDataType::Interval => DataType::Interval,
        AstDataType::Array(datatype) => DataType::List(Box::new(bind_data_type(datatype)?)),
        AstDataType::Char(..) => {
            bail_not_implemented!("CHAR is not supported, please use VARCHAR instead")
        }
        AstDataType::Struct(types) => StructType::new(
            types
                .iter()
                .map(|f| Ok((f.name.real_value(), bind_data_type(&f.data_type)?)))
                .collect::<Result<Vec<_>>>()?,
        )
        .into(),
        AstDataType::Map(kv) => {
            let key = bind_data_type(&kv.0)?;
            let value = bind_data_type(&kv.1)?;
            DataType::Map(MapType::try_from_kv(key, value).map_err(ErrorCode::BindError)?)
        }
        AstDataType::Custom(qualified_type_name) => {
            let idents = qualified_type_name
                .0
                .iter()
                .map(|n| n.real_value())
                .collect_vec();
            let name = if idents.len() == 1 {
                idents[0].as_str() // `int2`
            } else if idents.len() == 2 && idents[0] == PG_CATALOG_SCHEMA_NAME {
                idents[1].as_str() // `pg_catalog.text`
            } else {
                return Err(new_err().into());
            };

            // In PostgreSQL, these are non-keywords or non-reserved keywords but pre-defined
            // names that could be extended by `CREATE TYPE`.
            match name {
                "int2" => DataType::Int16,
                "int4" => DataType::Int32,
                "int8" => DataType::Int64,
                "rw_int256" => DataType::Int256,
                "float4" => DataType::Float32,
                "float8" => DataType::Float64,
                "timestamptz" => DataType::Timestamptz,
                "timestamp_ns" => DataType::TimestampNanosecond,
                "text" => DataType::Varchar,
                "serial" => {
                    return Err(ErrorCode::NotSupported(
                        "Column type SERIAL is not supported".into(),
                        "Please remove the SERIAL column".into(),
                    )
                    .into())
                }
                _ => return Err(new_err().into()),
            }
        }
        AstDataType::Bytea => DataType::Bytea,
        AstDataType::Jsonb => DataType::Jsonb,
        AstDataType::Regclass
        | AstDataType::Regproc
        | AstDataType::Uuid
        | AstDataType::Decimal(_, _)
        | AstDataType::Time(true) => return Err(new_err().into()),
    };
    Ok(data_type)
}
