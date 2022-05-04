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

use std::collections::HashMap;

use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{Expr, OrderByExpr, Query, Value};

use crate::binder::{Binder, BoundSetExpr};
use crate::expr::ExprImpl;
use crate::optimizer::property::{Direction, FieldOrder};

/// A validated sql query, including order and union.
/// An example of its relationship with `BoundSetExpr` and `BoundSelect` can be found here: <https://bit.ly/3GQwgPz>
#[derive(Debug)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
    pub order: Vec<FieldOrder>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub order_exprs: Vec<ExprImpl>,
}

impl BoundQuery {
    /// The schema returned by this [`BoundQuery`].
    pub fn schema(&self) -> &Schema {
        self.body.schema()
    }

    /// The types returned by this [`BoundQuery`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.schema().data_types()
    }

    pub fn is_correlated(&self) -> bool {
        self.body.is_correlated()
    }
}

impl Binder {
    /// Bind a [`Query`].
    ///
    /// Before binding the [`Query`], we push the current [`BindContext`](super::BindContext) to the
    /// stack and create a new context, because it may be a subquery.
    ///
    /// After finishing binding, we pop the previous context from the stack.
    pub fn bind_query(&mut self, query: Query) -> Result<BoundQuery> {
        self.push_context();
        let result = self.bind_query_inner(query);
        self.pop_context();
        result
    }

    /// Bind a [`Query`] using the current [`BindContext`](super::BindContext).
    pub(super) fn bind_query_inner(&mut self, query: Query) -> Result<BoundQuery> {
        let limit = query.get_limit_value();
        let offset = query.get_offset_value();
        let body = self.bind_set_expr(query.body)?;
        let mut name_to_index = HashMap::new();
        match &body {
            BoundSetExpr::Select(s) => s.aliases.iter().enumerate().for_each(|(index, alias)| {
                if let Some(name) = alias {
                    name_to_index.insert(name.clone(), index);
                }
            }),
            BoundSetExpr::Values(_) => {}
        };
        let mut order_exprs = vec![];
        let schema_len = body.schema().len();
        let order = query
            .order_by
            .into_iter()
            .map(|order_by_expr| {
                self.bind_order_by_expr(order_by_expr, &name_to_index, &mut order_exprs, schema_len)
            })
            .collect::<Result<_>>()?;
        Ok(BoundQuery {
            body,
            order,
            limit,
            offset,
            order_exprs,
        })
    }

    fn bind_order_by_expr(
        &mut self,
        order_by_expr: OrderByExpr,
        name_to_index: &HashMap<String, usize>,
        order_exprs: &mut Vec<ExprImpl>,
        schema_len: usize,
    ) -> Result<FieldOrder> {
        let direct = match order_by_expr.asc {
            None | Some(true) => Direction::Asc,
            Some(false) => Direction::Desc,
        };
        let index = match order_by_expr.expr {
            Expr::Identifier(name) => *name_to_index.get(&name.value).ok_or_else(|| {
                ErrorCode::ItemNotFound(format!("Output column \"{}\"", name.value))
            })?,
            Expr::Value(Value::Number(number, _)) => match number.parse::<usize>() {
                Ok(index) if index <= schema_len => index - 1,
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Invalid value in ORDER BY: {}",
                        number
                    ))
                    .into())
                }
            },
            expr => {
                order_exprs.push(self.bind_expr(expr)?);
                schema_len + order_exprs.len() - 1
            }
        };
        Ok(FieldOrder { index, direct })
    }
}
