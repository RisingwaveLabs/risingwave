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

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_pb::plan_common::JoinType;
use risingwave_sqlparser::ast::{
    BinaryOperator, Expr, Ident, JoinConstraint, JoinOperator, TableFactor, TableWithJoins, Value,
};

use crate::binder::{Binder, Relation};
use crate::expr::{Expr as _, ExprImpl};

#[derive(Debug, Clone)]
pub struct BoundJoin {
    pub join_type: JoinType,
    pub left: Relation,
    pub right: Relation,
    pub cond: ExprImpl,
}

impl Binder {
    pub(crate) fn bind_vec_table_with_joins(
        &mut self,
        from: Vec<TableWithJoins>,
    ) -> Result<Option<Relation>> {
        let mut from_iter = from.into_iter();
        let first = match from_iter.next() {
            Some(t) => t,
            None => return Ok(None),
        };
        self.push_lateral_context();
        let mut root = self.bind_table_with_joins(first)?;
        self.pop_and_merge_lateral_context()?;
        for t in from_iter {
            self.push_lateral_context();
            let right = self.bind_table_with_joins(t.clone())?;
            self.pop_and_merge_lateral_context()?;
            root = Relation::Join(Box::new(BoundJoin {
                join_type: JoinType::Inner,
                left: root,
                right,
                cond: ExprImpl::literal_bool(true),
            }));
        }
        Ok(Some(root))
    }

    fn bind_table_with_joins(&mut self, table: TableWithJoins) -> Result<Relation> {
        let mut root = self.bind_table_factor(table.relation)?;
        for join in table.joins {
            let (constraint, join_type) = match join.join_operator {
                JoinOperator::Inner(constraint) => (constraint, JoinType::Inner),
                JoinOperator::LeftOuter(constraint) => (constraint, JoinType::LeftOuter),
                JoinOperator::RightOuter(constraint) => (constraint, JoinType::RightOuter),
                JoinOperator::FullOuter(constraint) => (constraint, JoinType::FullOuter),
                // Cross join equals to inner join with with no constraint.
                JoinOperator::CrossJoin => (JoinConstraint::None, JoinType::Inner),
            };
            let right: Relation;
            let cond: ExprImpl;
            if let JoinConstraint::Using(_col) = constraint.clone() {
                let option_rel: Option<Relation>;
                (cond, option_rel) = self.bind_join_constraint(constraint, Some(join.relation))?;
                right = option_rel.unwrap();
            } else {
                right = self.bind_table_factor(join.relation.clone())?;
                (cond, _) = self.bind_join_constraint(constraint, None)?;
            }
            let join = BoundJoin {
                join_type,
                left: root,
                right,
                cond,
            };
            root = Relation::Join(Box::new(join));
        }

        Ok(root)
    }

    fn bind_join_constraint(
        &mut self,
        constraint: JoinConstraint,
        table_factor: Option<TableFactor>,
    ) -> Result<(ExprImpl, Option<Relation>)> {
        Ok(match constraint {
            JoinConstraint::None => (ExprImpl::literal_bool(true), None),
            JoinConstraint::Natural => {
                // First, we identify columns with the same name.
                let old_context = self.context.clone();
                let l_len = old_context.columns.len();
                // Bind this table factor to an empty context
                self.push_lateral_context();
                let table_factor = table_factor.unwrap();
                let right_table = get_table_name(&table_factor);
                let relation = self.bind_table_factor(table_factor)?;
                for (column, idxes) in &self.context.indexs_of {
                    if idxes.len() > 1 {
                        return Err(ErrorCode::InternalError(format!(
                            "Ambiguous column name: {}",
                            column
                        ))
                        .into());
                    }
                    if idxes.is_empty() {
                        return Err(ErrorCode::ItemNotFound(format!(
                            "Column {} does not have an associated index",
                            column
                        ))
                        .into());
                    }
                }
                let columns = self
                    .context
                    .indexs_of
                    .iter()
                    .filter(|(s, _)| *s != "_row_id") // filter out `_row_id`
                    .map(|(s, idxes)| (Ident::new(s.to_owned()), idxes))
                    .collect::<Vec<_>>();

                let mut col_indices = Vec::new();
                let mut binary_expr = Expr::Value(Value::Boolean(true));

                // Walk the LHS cols, checking to see if any share a name with the RHS cols
                for (column, idxes) in columns {
                    let indices = match old_context.get_unqualified_index(&column.value) {
                        Err(e) => {
                            if let ErrorCode::ItemNotFound(_) = e.inner() {
                                continue;
                            } else {
                                return Err(e);
                            }
                        }
                        Ok(idxs) => idxs,
                    };
                    // Select at most one column from each natural column group from left and right
                    col_indices.push((indices[0], idxes[0] + l_len));
                    binary_expr = Expr::BinaryOp {
                        left: Box::new(binary_expr),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column.clone())),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::Identifier(column.clone())),
                        }), 
                    }
                }
                self.pop_and_merge_lateral_context()?;
                for (l, r) in col_indices {
                    let non_nullable = match join_type {
                        JoinType::LefOuter | JoinType::Inner => Some(l),
                        JoinType::RightOuter => Some(r),
                        JoinType::FullOuter => None,
                        _ => unreachable!(),
                    };
                    self.context.add_natural_columns(l, r, non_nullable);
                }
            }
            JoinConstraint::On(expr) => {
                let bound_expr = self.bind_expr(expr)?;
                if bound_expr.return_type() != DataType::Boolean {
                    return Err(ErrorCode::InternalError(format!(
                        "argument of ON must be boolean, not type {:?}",
                        bound_expr.return_type()
                    ))
                    .into());
                }
                (bound_expr, None)
            }
            JoinConstraint::Using(columns) => {
                let table_factor = table_factor.unwrap();
                let right_table = get_table_name(&table_factor);
                let mut binary_expr = Expr::Value(Value::Boolean(true));
                for column in columns {
                    // TODO: we should be able to handle qualified column names as well
                    binary_expr = Expr::BinaryOp {
                        left: Box::new(binary_expr),
                        op: BinaryOperator::And,
                        right: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Identifier(column.clone())),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::CompoundIdentifier({
                                let mut right_table_clone = right_table.clone().unwrap();
                                right_table_clone.push(column.clone());
                                right_table_clone
                            })),
                        }),
                    }
                }
                // We cannot move this into ret expression since it should be done before bind_expr
                let relation = self.bind_table_factor(table_factor)?;
                (self.bind_expr(binary_expr)?, Some(relation))
            }
        })
    }
}

fn get_table_name(table_factor: &TableFactor) -> Option<Vec<Ident>> {
    match table_factor {
        TableFactor::Table { name, alias, .. } => {
            if let Some(table_alias) = alias {
                Some(vec![table_alias.name.clone()])
            } else {
                Some(name.0.clone())
            }
        }
        TableFactor::Derived { alias, .. } => alias
            .as_ref()
            .map(|table_alias| vec![table_alias.name.clone()]),
        TableFactor::TableFunction { expr: _, alias } => alias
            .as_ref()
            .map(|table_alias| vec![table_alias.name.clone()]),
        TableFactor::NestedJoin(table_with_joins) => get_table_name(&table_with_joins.relation),
    }
}
