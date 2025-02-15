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

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};
use risingwave_sqlparser::ast::{
    Cte, CteInner, Expr, Fetch, OrderByExpr, Query, SetExpr, SetOperator, Value, With,
};
use thiserror_ext::AsReport;

use super::bind_context::BindingCteState;
use super::statement::RewriteExprsRecursive;
use super::BoundValues;
use crate::binder::bind_context::{BindingCte, RecursiveUnion};
use crate::binder::{Binder, BoundSetExpr};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{CorrelatedId, Depth, ExprImpl, ExprRewriter};

/// A validated sql query, including order and union.
/// An example of its relationship with `BoundSetExpr` and `BoundSelect` can be found here: <https://bit.ly/3GQwgPz>
#[derive(Debug, Clone)]
pub struct BoundQuery {
    pub body: BoundSetExpr,
    pub order: Vec<ColumnOrder>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub with_ties: bool,
    pub extra_order_exprs: Vec<ExprImpl>,
}

impl BoundQuery {
    /// The schema returned by this [`BoundQuery`].
    pub fn schema(&self) -> std::borrow::Cow<'_, Schema> {
        self.body.schema()
    }

    /// The types returned by this [`BoundQuery`].
    pub fn data_types(&self) -> Vec<DataType> {
        self.schema().data_types()
    }

    /// Checks whether this query contains references to outer queries.
    ///
    /// Note there are 3 cases:
    /// ```sql
    /// select 1 from a having exists ( -- this is self
    ///   select 1 from b where exists (
    ///     select b1 from c
    ///   )
    /// );
    ///
    /// select 1 from a having exists ( -- this is self
    ///   select 1 from b where exists (
    ///     select a1 from c
    ///   )
    /// );
    ///
    /// select 1 from a where exists (
    ///   select 1 from b having exists ( -- this is self, not the one above
    ///     select a1 from c
    ///   )
    /// );
    /// ```
    /// We assume `self` is the subquery after `having`. In other words, the query with `from b` in
    /// first 2 examples and the query with `from c` in the last example.
    ///
    /// * The first example is uncorrelated, because it is self-contained and does not depend on
    ///   table `a`, although there is correlated input ref (`b1`) in it.
    /// * The second example is correlated, because it depend on a correlated input ref (`a1`) that
    ///   goes out.
    /// * The last example is also correlated. because it cannot be evaluated independently either.
    pub fn is_correlated(&self, depth: Depth) -> bool {
        self.body.is_correlated(depth + 1)
            || self
                .extra_order_exprs
                .iter()
                .any(|e| e.has_correlated_input_ref_by_depth(depth + 1))
    }

    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        let mut correlated_indices = vec![];

        correlated_indices.extend(
            self.body
                .collect_correlated_indices_by_depth_and_assign_id(depth + 1, correlated_id),
        );

        correlated_indices.extend(self.extra_order_exprs.iter_mut().flat_map(|expr| {
            expr.collect_correlated_indices_by_depth_and_assign_id(depth + 1, correlated_id)
        }));
        correlated_indices
    }

    /// Simple `VALUES` without other clauses.
    pub fn with_values(values: BoundValues) -> Self {
        BoundQuery {
            body: BoundSetExpr::Values(values.into()),
            order: vec![],
            limit: None,
            offset: None,
            with_ties: false,
            extra_order_exprs: vec![],
        }
    }
}

impl RewriteExprsRecursive for BoundQuery {
    fn rewrite_exprs_recursive(&mut self, rewriter: &mut impl ExprRewriter) {
        let new_extra_order_exprs = std::mem::take(&mut self.extra_order_exprs)
            .into_iter()
            .map(|expr| rewriter.rewrite_expr(expr))
            .collect::<Vec<_>>();
        self.extra_order_exprs = new_extra_order_exprs;

        self.body.rewrite_exprs_recursive(rewriter);
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
        self.pop_context()?;
        result
    }

    /// Bind a [`Query`] using the current [`BindContext`](super::BindContext).
    pub(super) fn bind_query_inner(
        &mut self,
        Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
        }: Query,
    ) -> Result<BoundQuery> {
        let mut with_ties = false;
        let limit = match (limit, fetch) {
            (None, None) => None,
            (
                None,
                Some(Fetch {
                    with_ties: fetch_with_ties,
                    quantity,
                }),
            ) => {
                with_ties = fetch_with_ties;
                match quantity {
                    Some(v) => Some(Expr::Value(Value::Number(v))),
                    None => Some(Expr::Value(Value::Number("1".to_owned()))),
                }
            }
            (Some(limit), None) => Some(limit),
            (Some(_), Some(_)) => unreachable!(), // parse error
        };
        let limit_expr = limit.map(|expr| self.bind_expr(expr)).transpose()?;
        let limit = if let Some(limit_expr) = limit_expr {
            // wrong type error is handled here
            let limit_cast_to_bigint = limit_expr.cast_assign(DataType::Int64).map_err(|_| {
                RwError::from(ErrorCode::ExprError(
                    "expects an integer or expression that can be evaluated to an integer after LIMIT"
                        .into(),
                ))
            })?;
            let limit = match limit_cast_to_bigint.try_fold_const() {
                Some(Ok(Some(datum))) => {
                    let value = datum.as_int64();
                    if *value < 0 {
                        return Err(ErrorCode::ExprError(
                            format!("LIMIT must not be negative, but found: {}", *value).into(),
                        )
                            .into());
                    }
                    *value as u64
                }
                // If evaluated to NULL, we follow PG to treat NULL as no limit
                Some(Ok(None)) => {
                    u64::MAX
                }
                // not const error
                None => return Err(ErrorCode::ExprError(
                    "expects an integer or expression that can be evaluated to an integer after LIMIT, but found non-const expression"
                        .into(),
                ).into()),
                // eval error
                Some(Err(e)) => {
                    return Err(ErrorCode::ExprError(
                        format!("expects an integer or expression that can be evaluated to an integer after LIMIT,\nbut the evaluation of the expression returns error:{}", e.as_report()
                        ).into(),
                    ).into())
                }
            };
            Some(limit)
        } else {
            None
        };

        let offset = offset
            .map(|s| parse_non_negative_i64("OFFSET", &s))
            .transpose()?
            .map(|v| v as u64);

        if let Some(with) = with {
            self.bind_with(with)?;
        }
        let body = self.bind_set_expr(body)?;
        let name_to_index =
            Self::build_name_to_index(body.schema().fields().iter().map(|f| f.name.clone()));
        let mut extra_order_exprs = vec![];
        let visible_output_num = body.schema().len();
        let order = order_by
            .into_iter()
            .map(|order_by_expr| {
                self.bind_order_by_expr_in_query(
                    order_by_expr,
                    &name_to_index,
                    &mut extra_order_exprs,
                    visible_output_num,
                )
            })
            .collect::<Result<_>>()?;
        Ok(BoundQuery {
            body,
            order,
            limit,
            offset,
            with_ties,
            extra_order_exprs,
        })
    }

    pub fn build_name_to_index(names: impl Iterator<Item = String>) -> HashMap<String, usize> {
        let mut m = HashMap::new();
        names.enumerate().for_each(|(index, name)| {
            m.entry(name)
                // Ambiguous (duplicate) output names are marked with usize::MAX.
                // This is not necessarily an error as long as not actually referenced.
                .and_modify(|v| *v = usize::MAX)
                .or_insert(index);
        });
        m
    }

    /// Bind an `ORDER BY` expression in a [`Query`], which can be either:
    /// * an output-column name
    /// * index of an output column
    /// * an arbitrary expression
    ///
    /// Refer to `bind_group_by_expr_in_select` to see their similarities and differences.
    ///
    /// # Arguments
    ///
    /// * `name_to_index` - visible output column name -> index. Ambiguous (duplicate) output names
    ///   are marked with `usize::MAX`.
    /// * `visible_output_num` - the number of all visible output columns, including duplicates.
    fn bind_order_by_expr_in_query(
        &mut self,
        OrderByExpr {
            expr,
            asc,
            nulls_first,
        }: OrderByExpr,
        name_to_index: &HashMap<String, usize>,
        extra_order_exprs: &mut Vec<ExprImpl>,
        visible_output_num: usize,
    ) -> Result<ColumnOrder> {
        let order_type = OrderType::from_bools(asc, nulls_first);
        let column_index = match expr {
            Expr::Identifier(name) if let Some(index) = name_to_index.get(&name.real_value()) => {
                match *index != usize::MAX {
                    true => *index,
                    false => {
                        return Err(ErrorCode::BindError(format!(
                            "ORDER BY \"{}\" is ambiguous",
                            name.real_value()
                        ))
                        .into())
                    }
                }
            }
            Expr::Value(Value::Number(number)) => match number.parse::<usize>() {
                Ok(index) if 1 <= index && index <= visible_output_num => index - 1,
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "Invalid ordinal number in ORDER BY: {}",
                        number
                    ))
                    .into())
                }
            },
            expr => {
                extra_order_exprs.push(self.bind_expr(expr)?);
                visible_output_num + extra_order_exprs.len() - 1
            }
        };
        Ok(ColumnOrder::new(column_index, order_type))
    }

    fn bind_with(&mut self, with: With) -> Result<()> {
        for cte_table in with.cte_tables {
            // note that the new `share_id` for the rcte is generated here
            let share_id = self.next_share_id();
            let Cte { alias, cte_inner } = cte_table;
            let table_name = alias.name.real_value();

            if with.recursive {
                if let CteInner::Query(query) = cte_inner {
                    let (
                        SetExpr::SetOperation {
                            op: SetOperator::Union,
                            all,
                            corresponding,
                            left,
                            right,
                        },
                        with,
                    ) = Self::validate_rcte(query)?
                    else {
                        return Err(ErrorCode::BindError(
                            "expect `SetOperation` as the return type of validation".into(),
                        )
                        .into());
                    };

                    // validated in `validate_rcte`
                    assert!(
                        !corresponding.is_corresponding(),
                        "`CORRESPONDING` is not supported in recursive CTE"
                    );

                    let entry = self
                        .context
                        .cte_to_relation
                        .entry(table_name)
                        .insert_entry(Rc::new(RefCell::new(BindingCte {
                            share_id,
                            state: BindingCteState::Init,
                            alias,
                        })))
                        .get()
                        .clone();

                    self.bind_rcte(with, entry, *left, *right, all)?;
                } else {
                    return Err(ErrorCode::BindError(
                        "RECURSIVE CTE only support query".to_owned(),
                    )
                    .into());
                }
            } else {
                match cte_inner {
                    CteInner::Query(query) => {
                        let bound_query = self.bind_query(query)?;
                        self.context.cte_to_relation.insert(
                            table_name,
                            Rc::new(RefCell::new(BindingCte {
                                share_id,
                                state: BindingCteState::Bound {
                                    query: either::Either::Left(bound_query),
                                },
                                alias,
                            })),
                        );
                    }
                    CteInner::ChangeLog(from_table_name) => {
                        self.push_context();
                        let from_table_relation =
                            self.bind_relation_by_name(from_table_name.clone(), None, None, true)?;
                        self.pop_context()?;
                        self.context.cte_to_relation.insert(
                            table_name,
                            Rc::new(RefCell::new(BindingCte {
                                share_id,
                                state: BindingCteState::ChangeLog {
                                    table: from_table_relation,
                                },
                                alias,
                            })),
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// syntactically validate the recursive cte ast with the current support features in rw.
    fn validate_rcte(query: Query) -> Result<(SetExpr, Option<With>)> {
        let Query {
            with,
            body,
            order_by,
            limit,
            offset,
            fetch,
        } = query;

        /// the input clause should not be supported.
        fn should_be_empty<T>(v: Option<T>, clause: &str) -> Result<()> {
            if v.is_some() {
                return Err(ErrorCode::BindError(format!(
                    "`{clause}` is not supported in recursive CTE"
                ))
                .into());
            }
            Ok(())
        }

        should_be_empty(order_by.first(), "ORDER BY")?;
        should_be_empty(limit, "LIMIT")?;
        should_be_empty(offset, "OFFSET")?;
        should_be_empty(fetch, "FETCH")?;

        let SetExpr::SetOperation {
            op: SetOperator::Union,
            all,
            corresponding,
            left,
            right,
        } = body
        else {
            return Err(
                ErrorCode::BindError("`UNION` is required in recursive CTE".to_owned()).into(),
            );
        };

        if !all {
            return Err(ErrorCode::BindError(
                "only `UNION ALL` is supported in recursive CTE now".to_owned(),
            )
            .into());
        }

        if corresponding.is_corresponding() {
            return Err(ErrorCode::BindError(
                "`CORRESPONDING` is not supported in recursive CTE".to_owned(),
            )
            .into());
        }

        Ok((
            SetExpr::SetOperation {
                op: SetOperator::Union,
                all,
                corresponding,
                left,
                right,
            },
            with,
        ))
    }

    fn bind_rcte(
        &mut self,
        with: Option<With>,
        entry: Rc<RefCell<BindingCte>>,
        left: SetExpr,
        right: SetExpr,
        all: bool,
    ) -> Result<()> {
        self.push_context();
        let result = self.bind_rcte_inner(with, entry, left, right, all);
        self.pop_context()?;
        result
    }

    fn bind_rcte_inner(
        &mut self,
        with: Option<With>,
        entry: Rc<RefCell<BindingCte>>,
        left: SetExpr,
        right: SetExpr,
        all: bool,
    ) -> Result<()> {
        if let Some(with) = with {
            self.bind_with(with)?;
        }

        // We assume `left` is the base term, otherwise the implementation may be very hard.
        // The behavior is the same as PostgreSQL's.
        // reference: <https://www.postgresql.org/docs/16/sql-select.html#:~:text=the%20recursive%20self%2Dreference%20must%20appear%20on%20the%20right%2Dhand%20side%20of%20the%20UNION>
        let mut base = self.bind_set_expr(left)?;

        entry.borrow_mut().state = BindingCteState::BaseResolved { base: base.clone() };

        // Reset context for right side, but keep `cte_to_relation`.
        let new_context = std::mem::take(&mut self.context);
        self.context
            .cte_to_relation
            .clone_from(&new_context.cte_to_relation);
        // bind the rest of the recursive cte
        let mut recursive = self.bind_set_expr(right)?;
        // Reset context for the set operation.
        self.context = Default::default();
        self.context.cte_to_relation = new_context.cte_to_relation;

        Self::align_schema(&mut base, &mut recursive, SetOperator::Union)?;
        let schema = base.schema().into_owned();

        let recursive_union = RecursiveUnion {
            all,
            base: Box::new(base),
            recursive: Box::new(recursive),
            schema,
        };

        entry.borrow_mut().state = BindingCteState::Bound {
            query: either::Either::Right(recursive_union),
        };

        Ok(())
    }
}

// TODO: Make clause a const generic param after <https://github.com/rust-lang/rust/issues/95174>.
fn parse_non_negative_i64(clause: &str, s: &str) -> Result<i64> {
    match s.parse::<i64>() {
        Ok(v) => {
            if v < 0 {
                Err(ErrorCode::InvalidInputSyntax(format!("{clause} must not be negative")).into())
            } else {
                Ok(v)
            }
        }
        Err(e) => Err(ErrorCode::InvalidInputSyntax(e.to_report_string()).into()),
    }
}
