// Copyright 2024 RisingWave Labs
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

use std::cmp::{max, min};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Schema, KAFKA_TIMESTAMP_COLUMN_NAME};
use risingwave_connector::source::DataType;

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchProject, ColPrunable, ExprRewritable, Logical, LogicalFilter, LogicalProject,
    PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchKafkaScan, ColumnPruningContext, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalKafkaScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a kafka property would be converted into a `LogicalKafkaScan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalKafkaScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    output_exprs: Option<Vec<ExprImpl>>,

    /// Kafka timestamp range.
    kafka_timestamp_range: (Bound<i64>, Bound<i64>),
}

impl LogicalKafkaScan {
    pub fn new(logical_source: &LogicalSource) -> Self {
        assert!(logical_source.core.is_kafka_connector());

        let base = logical_source.base.clone_with_new_plan_id();
        let core = logical_source.core.clone();
        let kafka_timestamp_range = (Bound::Unbounded, Bound::Unbounded);

        LogicalKafkaScan {
            base,
            core,
            output_exprs: logical_source.output_exprs.clone(),
            kafka_timestamp_range,
        }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    fn clone_with_kafka_timestamp_range(&self, range: (Bound<i64>, Bound<i64>)) -> Self {
        Self {
            base: self.base.clone(),
            core: self.core.clone(),
            output_exprs: self.output_exprs.clone(),
            kafka_timestamp_range: range,
        }
    }
}

impl_plan_tree_node_for_leaf! {LogicalKafkaScan}
impl Distill for LogicalKafkaScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            let time = Pretty::debug(&self.kafka_timestamp_range);
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                ("time_range", time),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalKafkaScan", fields)
    }
}

impl ColPrunable for LogicalKafkaScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalKafkaScan {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut output_exprs = self.output_exprs.clone();

        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            output_exprs,
            ..self.clone()
        }
        .into()
    }
}

impl ExprVisitable for LogicalKafkaScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

/// A util function to extract kafka offset timestamp range.
///
/// Currently we only support limiting kafka offset timestamp range using literals, e.g. we only
/// support expressions like `_rw_kafka_timestamp <= '2022-10-11 1:00:00+00:00'`.
///
/// # Parameters
///
/// * `expr`: Expression to be consumed.
/// * `range`: Original timestamp range, if `expr` can be recognized, we will update `range`.
/// * `schema`: Input schema.
///
/// # Return Value
///
/// If `expr` can be recognized and consumed by this function, then we return `None`.
/// Otherwise `expr` is returned.
fn expr_to_kafka_timestamp_range(
    expr: ExprImpl,
    range: &mut (Bound<i64>, Bound<i64>),
    schema: &Schema,
) -> Option<ExprImpl> {
    let merge_upper_bound = |first, second| -> Bound<i64> {
        match (first, second) {
            (first, Unbounded) => first,
            (Unbounded, second) => second,
            (Included(f1), Included(f2)) => Included(min(f1, f2)),
            (Included(f1), Excluded(f2)) => {
                if f1 < f2 {
                    Included(f1)
                } else {
                    Excluded(f2)
                }
            }
            (Excluded(f1), Included(f2)) => {
                if f2 < f1 {
                    Included(f2)
                } else {
                    Excluded(f1)
                }
            }
            (Excluded(f1), Excluded(f2)) => Excluded(min(f1, f2)),
        }
    };

    let merge_lower_bound = |first, second| -> Bound<i64> {
        match (first, second) {
            (first, Unbounded) => first,
            (Unbounded, second) => second,
            (Included(f1), Included(f2)) => Included(max(f1, f2)),
            (Included(f1), Excluded(f2)) => {
                if f1 > f2 {
                    Included(f1)
                } else {
                    Excluded(f2)
                }
            }
            (Excluded(f1), Included(f2)) => {
                if f2 > f1 {
                    Included(f2)
                } else {
                    Excluded(f1)
                }
            }
            (Excluded(f1), Excluded(f2)) => Excluded(max(f1, f2)),
        }
    };

    let extract_timestampz_literal = |expr: &ExprImpl| -> Result<Option<(i64, bool)>> {
        match expr {
            ExprImpl::FunctionCall(function_call) if function_call.inputs().len() == 2 => {
                match (&function_call.inputs()[0], &function_call.inputs()[1]) {
                    (ExprImpl::InputRef(input_ref), literal)
                        if let Some(datum) = literal.try_fold_const().transpose()?
                            && schema.fields[input_ref.index].name
                                == KAFKA_TIMESTAMP_COLUMN_NAME
                            && literal.return_type() == DataType::Timestamptz =>
                    {
                        Ok(Some((
                            datum.unwrap().into_timestamptz().timestamp_millis(),
                            false,
                        )))
                    }
                    (literal, ExprImpl::InputRef(input_ref))
                        if let Some(datum) = literal.try_fold_const().transpose()?
                            && schema.fields[input_ref.index].name
                                == KAFKA_TIMESTAMP_COLUMN_NAME
                            && literal.return_type() == DataType::Timestamptz =>
                    {
                        Ok(Some((
                            datum.unwrap().into_timestamptz().timestamp_millis(),
                            true,
                        )))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    };

    match &expr {
        ExprImpl::FunctionCall(function_call) => {
            if let Ok(Some((timestampz_literal, reverse))) = extract_timestampz_literal(&expr) {
                match function_call.func_type() {
                    ExprType::GreaterThan => {
                        if reverse {
                            range.1 = merge_upper_bound(range.1, Excluded(timestampz_literal));
                        } else {
                            range.0 = merge_lower_bound(range.0, Excluded(timestampz_literal));
                        }

                        None
                    }
                    ExprType::GreaterThanOrEqual => {
                        if reverse {
                            range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        } else {
                            range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        }
                        None
                    }
                    ExprType::Equal => {
                        range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        None
                    }
                    ExprType::LessThan => {
                        if reverse {
                            range.0 = merge_lower_bound(range.0, Excluded(timestampz_literal));
                        } else {
                            range.1 = merge_upper_bound(range.1, Excluded(timestampz_literal));
                        }
                        None
                    }
                    ExprType::LessThanOrEqual => {
                        if reverse {
                            range.0 = merge_lower_bound(range.0, Included(timestampz_literal));
                        } else {
                            range.1 = merge_upper_bound(range.1, Included(timestampz_literal));
                        }
                        None
                    }
                    _ => Some(expr),
                }
            } else {
                Some(expr)
            }
        }
        _ => Some(expr),
    }
}

impl PredicatePushdown for LogicalKafkaScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let mut range = self.kafka_timestamp_range;

        let mut new_conjunctions = Vec::with_capacity(predicate.conjunctions.len());
        for expr in predicate.conjunctions {
            if let Some(e) = expr_to_kafka_timestamp_range(expr, &mut range, self.base.schema()) {
                // Not recognized, so push back
                new_conjunctions.push(e);
            }
        }

        let new_source = self.clone_with_kafka_timestamp_range(range).into();

        if new_conjunctions.is_empty() {
            new_source
        } else {
            LogicalFilter::create(
                new_source,
                Condition {
                    conjunctions: new_conjunctions,
                },
            )
        }
    }
}

impl ToBatch for LogicalKafkaScan {
    fn to_batch(&self) -> Result<PlanRef> {
        let mut plan: PlanRef =
            BatchKafkaScan::new(self.core.clone(), self.kafka_timestamp_range).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.to_vec(), plan);
            plan = BatchProject::new(logical_project).into();
        }

        Ok(plan)
    }
}

impl ToStream for LogicalKafkaScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
