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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{INFORMATION_SCHEMA_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME};
use risingwave_common::types::DataType;
use risingwave_expr::aggregate::AggType;
use risingwave_expr::window_function::WindowFuncKind;
use risingwave_sqlparser::ast::{self, Function, FunctionArg, FunctionArgExpr, Ident};
use risingwave_sqlparser::parser::ParserError;

use crate::binder::bind_context::Clause;
use crate::binder::{Binder, UdfContext};
use crate::catalog::function_catalog::FunctionCatalog;
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{
    Expr, ExprImpl, ExprType, FunctionCallWithLambda, InputRef, TableFunction, TableFunctionType,
    UserDefinedFunction,
};

mod aggregate;
mod builtin_scalar;
mod window;

// Defines system functions that without args, ref: https://www.postgresql.org/docs/current/functions-info.html
const SYS_FUNCTION_WITHOUT_ARGS: &[&str] = &[
    "session_user",
    "user",
    "current_user",
    "current_role",
    "current_catalog",
    "current_schema",
    "current_timestamp",
];

pub(super) fn is_sys_function_without_args(ident: &Ident) -> bool {
    SYS_FUNCTION_WITHOUT_ARGS
        .iter()
        .any(|e| ident.real_value().as_str() == *e && ident.quote_style().is_none())
}

/// The global max calling depth for the global counter in `udf_context`
/// To reduce the chance that the current running rw thread
/// be killed by os, the current allowance depth of calling
/// stack is set to `16`.
const SQL_UDF_MAX_CALLING_DEPTH: u32 = 16;

macro_rules! reject_syntax {
    ($pred:expr, $msg:expr) => {
        if $pred {
            return Err(ErrorCode::InvalidInputSyntax($msg.to_string()).into());
        }
    };
}

impl Binder {
    pub(in crate::binder) fn bind_function(
        &mut self,
        Function {
            scalar_as_agg,
            name,
            arg_list,
            within_group,
            filter,
            over,
        }: Function,
    ) -> Result<ExprImpl> {
        let (schema_name, func_name) = match name.0.as_slice() {
            [name] => (None, name.real_value()),
            [schema, name] => {
                let schema_name = schema.real_value();
                let func_name = if schema_name == PG_CATALOG_SCHEMA_NAME {
                    // pg_catalog is always effectively part of the search path, so we can always bind the function.
                    // Ref: https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-CATALOG
                    name.real_value()
                } else if schema_name == INFORMATION_SCHEMA_SCHEMA_NAME {
                    // definition of information_schema: https://github.com/postgres/postgres/blob/e0b2eed047df9045664da6f724cb42c10f8b12f0/src/backend/catalog/information_schema.sql
                    //
                    // FIXME: handle schema correctly, so that the functions are hidden if the schema is not in the search path.
                    let function_name = name.real_value();
                    if function_name != "_pg_expandarray" {
                        bail_not_implemented!(
                            issue = 12422,
                            "Unsupported function name under schema: {}",
                            schema_name
                        );
                    }
                    function_name
                } else {
                    bail_not_implemented!(
                        issue = 12422,
                        "Unsupported function name under schema: {}",
                        schema_name
                    );
                };
                (Some(schema_name), func_name)
            }
            _ => bail_not_implemented!(issue = 112, "qualified function {}", name),
        };

        // FIXME: This is a hack to support [Bytebase queries](https://github.com/TennyZhuang/bytebase/blob/4a26f7c62b80e86e58ad2f77063138dc2f420623/backend/plugin/db/pg/sync.go#L549).
        // Bytebase widely used the pattern like `obj_description(format('%s.%s',
        // quote_ident(idx.schemaname), quote_ident(idx.indexname))::regclass) AS comment` to
        // retrieve object comment, however we don't support casting a non-literal expression to
        // regclass. We just hack the `obj_description` and `col_description` here, to disable it to
        // bind its arguments.
        if func_name == "obj_description" || func_name == "col_description" {
            return Ok(ExprImpl::literal_varchar("".to_owned()));
        }

        // special binding logic for `array_transform`
        if func_name == "array_transform" {
            // For type inference, we need to bind the array type first.
            reject_syntax!(
                scalar_as_agg,
                "`AGGREGATE:` prefix is not allowed for `array_transform`"
            );
            reject_syntax!(!arg_list.is_args_only(), "keywords like `DISTINCT`, `ORDER BY` are not allowed in `array_transform` argument list");
            reject_syntax!(
                within_group.is_some(),
                "`WITHIN GROUP` is not allowed in `array_transform` call"
            );
            reject_syntax!(
                filter.is_some(),
                "`FILTER` is not allowed in `array_transform` call"
            );
            reject_syntax!(
                over.is_some(),
                "`OVER` is not allowed in `array_transform` call"
            );
            return self.bind_array_transform(arg_list.args);
        }

        let mut args: Vec<_> = arg_list
            .args
            .iter()
            .map(|arg| self.bind_function_arg(arg.clone()))
            .flatten_ok()
            .try_collect()?;

        let mut referred_udfs = HashSet::new();

        let wrapped_agg_type = if scalar_as_agg {
            // Let's firstly try to apply the `AGGREGATE:` prefix.
            // We will reject functions that are not able to be wrapped as aggregate function.
            let mut array_args = args
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    InputRef::new(i, DataType::List(Box::new(expr.return_type()))).into()
                })
                .collect_vec();
            let schema_path = self.bind_schema_path(schema_name.as_deref());
            let scalar_func_expr = if let Ok((func, _)) = self.catalog.get_function_by_name_inputs(
                &self.db_name,
                schema_path,
                &func_name,
                &mut array_args,
            ) {
                // record the dependency upon the UDF
                referred_udfs.insert(func.id);

                if !func.kind.is_scalar() {
                    return Err(ErrorCode::InvalidInputSyntax(
                        "expect a scalar function after `AGGREGATE:`".to_owned(),
                    )
                    .into());
                }

                if func.language == "sql" {
                    self.bind_sql_udf(func.clone(), array_args)?
                } else {
                    UserDefinedFunction::new(func.clone(), array_args).into()
                }
            } else {
                self.bind_builtin_scalar_function(&func_name, array_args, arg_list.variadic)?
            };

            // now this is either an aggregate/window function call
            Some(AggType::WrapScalar(scalar_func_expr.to_expr_proto()))
        } else {
            None
        };

        let schema_path = self.bind_schema_path(schema_name.as_deref());
        let udf = if wrapped_agg_type.is_none()
            && let Ok((func, _)) = self.catalog.get_function_by_name_inputs(
                &self.db_name,
                schema_path,
                &func_name,
                &mut args,
            ) {
            // record the dependency upon the UDF
            referred_udfs.insert(func.id);
            Some(func.clone())
        } else {
            None
        };

        self.included_udfs.extend(referred_udfs);

        let agg_type = if wrapped_agg_type.is_some() {
            wrapped_agg_type
        } else if let Some(ref udf) = udf
            && udf.kind.is_aggregate()
        {
            assert_ne!(udf.language, "sql", "SQL UDAF is not supported yet");
            Some(AggType::UserDefined(udf.as_ref().into()))
        } else if let Ok(agg_type) = AggType::from_str(&func_name) {
            Some(agg_type)
        } else {
            None
        };

        // try to bind it as a window function call
        if let Some(over) = over {
            reject_syntax!(
                arg_list.distinct,
                "`DISTINCT` is not allowed in window function call"
            );
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in window function call"
            );
            reject_syntax!(
                !arg_list.order_by.is_empty(),
                "`ORDER BY` is not allowed in window function call argument list"
            );
            reject_syntax!(
                within_group.is_some(),
                "`WITHIN GROUP` is not allowed in window function call"
            );

            let kind = if let Some(agg_type) = agg_type {
                // aggregate as window function
                WindowFuncKind::Aggregate(agg_type)
            } else if let Ok(kind) = WindowFuncKind::from_str(&func_name) {
                kind
            } else {
                bail_not_implemented!(issue = 8961, "Unrecognized window function: {}", func_name);
            };
            return self.bind_window_function(kind, args, arg_list.ignore_nulls, filter, over);
        }

        // now it's an aggregate/scalar/table function call
        reject_syntax!(
            arg_list.ignore_nulls,
            "`IGNORE NULLS` is not allowed in aggregate/scalar/table function call"
        );

        // try to bind it as an aggregate function call
        if let Some(agg_type) = agg_type {
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in aggregate function call"
            );
            return self.bind_aggregate_function(
                agg_type,
                arg_list.distinct,
                args,
                arg_list.order_by,
                within_group,
                filter,
            );
        }

        // now it's a scalar/table function call
        reject_syntax!(
            arg_list.distinct,
            "`DISTINCT` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            !arg_list.order_by.is_empty(),
            "`ORDER BY` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            within_group.is_some(),
            "`WITHIN GROUP` is not allowed in scalar/table function call"
        );
        reject_syntax!(
            filter.is_some(),
            "`FILTER` is not allowed in scalar/table function call"
        );

        // try to bind it as a table function call
        {
            // `file_scan` table function
            if func_name.eq_ignore_ascii_case("file_scan") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_file_scan(args)?.into());
            }
            // `postgres_query` table function
            if func_name.eq("postgres_query") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_postgres_query(args)
                    .context("postgres_query error")?
                    .into());
            }
            // `mysql_query` table function
            if func_name.eq("mysql_query") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_mysql_query(args)
                    .context("mysql_query error")?
                    .into());
            }
            // `internal_backfill_progress` table function
            if func_name.eq("internal_backfill_progress") {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new_internal_backfill_progress().into());
            }
            // UDTF
            if let Some(ref udf) = udf
                && udf.kind.is_table()
            {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                if udf.language == "sql" {
                    return self.bind_sql_udf(udf.clone(), args);
                }
                return Ok(TableFunction::new_user_defined(udf.clone(), args).into());
            }
            // builtin table function
            if let Ok(function_type) = TableFunctionType::from_str(&func_name) {
                reject_syntax!(
                    arg_list.variadic,
                    "`VARIADIC` is not allowed in table function call"
                );
                self.ensure_table_function_allowed()?;
                return Ok(TableFunction::new(function_type, args)?.into());
            }
        }

        // try to bind it as a scalar function call
        if let Some(ref udf) = udf {
            assert!(udf.kind.is_scalar());
            reject_syntax!(
                arg_list.variadic,
                "`VARIADIC` is not allowed in user-defined function call"
            );
            if udf.language == "sql" {
                return self.bind_sql_udf(udf.clone(), args);
            }
            return Ok(UserDefinedFunction::new(udf.clone(), args).into());
        }

        self.bind_builtin_scalar_function(&func_name, args, arg_list.variadic)
    }

    fn bind_array_transform(&mut self, args: Vec<FunctionArg>) -> Result<ExprImpl> {
        let [array, lambda] = <[FunctionArg; 2]>::try_from(args).map_err(|args| -> RwError {
            ErrorCode::BindError(format!(
                "`array_transform` expect two inputs `array` and `lambda`, but {} were given",
                args.len()
            ))
            .into()
        })?;

        let bound_array = self.bind_function_arg(array)?;
        let [bound_array] = <[ExprImpl; 1]>::try_from(bound_array).map_err(|bound_array| -> RwError {
            ErrorCode::BindError(format!("The `array` argument for `array_transform` should be bound to one argument, but {} were got", bound_array.len()))
                .into()
        })?;

        let inner_ty = match bound_array.return_type() {
            DataType::List(ty) => *ty,
            real_type => {
                return Err(ErrorCode::BindError(format!(
                "The `array` argument for `array_transform` should be an array, but {} were got",
                real_type
            ))
                .into())
            }
        };

        let ast::FunctionArgExpr::Expr(ast::Expr::LambdaFunction {
            args: lambda_args,
            body: lambda_body,
        }) = lambda.get_expr()
        else {
            return Err(ErrorCode::BindError(
                "The `lambda` argument for `array_transform` should be a lambda function"
                    .to_owned(),
            )
            .into());
        };

        let [lambda_arg] = <[Ident; 1]>::try_from(lambda_args).map_err(|args| -> RwError {
            ErrorCode::BindError(format!(
                "The `lambda` argument for `array_transform` should be a lambda function with one argument, but {} were given",
                args.len()
            ))
            .into()
        })?;

        let bound_lambda = self.bind_unary_lambda_function(inner_ty, lambda_arg, *lambda_body)?;

        let lambda_ret_type = bound_lambda.return_type();
        let transform_ret_type = DataType::List(Box::new(lambda_ret_type));

        Ok(ExprImpl::FunctionCallWithLambda(Box::new(
            FunctionCallWithLambda::new_unchecked(
                ExprType::ArrayTransform,
                vec![bound_array],
                bound_lambda,
                transform_ret_type,
            ),
        )))
    }

    fn bind_unary_lambda_function(
        &mut self,
        input_ty: DataType,
        arg: Ident,
        body: ast::Expr,
    ) -> Result<ExprImpl> {
        let lambda_args = HashMap::from([(arg.real_value(), (0usize, input_ty))]);
        let orig_lambda_args = self.context.lambda_args.replace(lambda_args);
        let body = self.bind_expr_inner(body)?;
        self.context.lambda_args = orig_lambda_args;

        Ok(body)
    }

    fn ensure_table_function_allowed(&self) -> Result<()> {
        if let Some(clause) = self.context.clause {
            match clause {
                Clause::JoinOn
                | Clause::Where
                | Clause::Having
                | Clause::Filter
                | Clause::Values
                | Clause::Insert
                | Clause::GeneratedColumn => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "table functions are not allowed in {}",
                        clause
                    ))
                    .into());
                }
                Clause::GroupBy | Clause::From => {}
            }
        }
        Ok(())
    }

    fn bind_sql_udf(
        &mut self,
        func: Arc<FunctionCatalog>,
        args: Vec<ExprImpl>,
    ) -> Result<ExprImpl> {
        if func.body.is_none() {
            return Err(
                ErrorCode::InvalidInputSyntax("`body` must exist for sql udf".to_owned()).into(),
            );
        }

        // This represents the current user defined function is `language sql`
        let parse_result =
            risingwave_sqlparser::parser::Parser::parse_sql(func.body.as_ref().unwrap().as_str());
        if let Err(ParserError::ParserError(err)) | Err(ParserError::TokenizerError(err)) =
            parse_result
        {
            // Here we just return the original parse error message
            return Err(ErrorCode::InvalidInputSyntax(err).into());
        }

        debug_assert!(parse_result.is_ok());

        // We can safely unwrap here
        let ast = parse_result.unwrap();

        // Stash the current `udf_context`
        // Note that the `udf_context` may be empty,
        // if the current binding is the root (top-most) sql udf.
        // In this case the empty context will be stashed
        // and restored later, no need to maintain other flags.
        let stashed_udf_context = self.udf_context.get_context();

        // The actual inline logic for sql udf
        // Note that we will always create new udf context for each sql udf
        let mut udf_context = HashMap::new();
        for (i, arg) in args.into_iter().enumerate() {
            if func.arg_names[i].is_empty() {
                // unnamed argument, use `$1`, `$2` as the name
                udf_context.insert(format!("${}", i + 1), arg);
            } else {
                // named argument
                udf_context.insert(func.arg_names[i].clone(), arg);
            }
        }
        self.udf_context.update_context(udf_context);

        // Check for potential recursive calling
        if self.udf_context.global_count() >= SQL_UDF_MAX_CALLING_DEPTH {
            return Err(ErrorCode::BindError(format!(
                "function {} calling stack depth limit exceeded",
                func.name
            ))
            .into());
        } else {
            // Update the status for the global counter
            self.udf_context.incr_global_count();
        }

        if let Ok(expr) = UdfContext::extract_udf_expression(ast) {
            let bind_result = self.bind_expr(expr);

            // We should properly decrement global count after a successful binding
            // Since the subsequent probe operation in `bind_column` or
            // `bind_parameter` relies on global counting
            self.udf_context.decr_global_count();

            // Restore context information for subsequent binding
            self.udf_context.update_context(stashed_udf_context);

            return bind_result;
        }

        Err(ErrorCode::InvalidInputSyntax(
            "failed to parse the input query and extract the udf expression,
                please recheck the syntax"
                .to_owned(),
        )
        .into())
    }

    pub(in crate::binder) fn bind_function_expr_arg(
        &mut self,
        arg_expr: FunctionArgExpr,
    ) -> Result<Vec<ExprImpl>> {
        match arg_expr {
            FunctionArgExpr::Expr(expr) => Ok(vec![self.bind_expr_inner(expr)?]),
            FunctionArgExpr::QualifiedWildcard(_, _)
            | FunctionArgExpr::ExprQualifiedWildcard(_, _) => Err(ErrorCode::InvalidInputSyntax(
                format!("unexpected wildcard {}", arg_expr),
            )
            .into()),
            FunctionArgExpr::Wildcard(None) => Ok(vec![]),
            FunctionArgExpr::Wildcard(Some(_)) => unreachable!(),
        }
    }

    pub(in crate::binder) fn bind_function_arg(
        &mut self,
        arg: FunctionArg,
    ) -> Result<Vec<ExprImpl>> {
        match arg {
            FunctionArg::Unnamed(expr) => self.bind_function_expr_arg(expr),
            FunctionArg::Named { .. } => todo!(),
        }
    }
}
