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

use std::collections::{BTreeMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnDesc, TableDesc};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::stream_plan::StreamScanType;
use risingwave_sqlparser::ast::AsOf;

use super::generic::{GenericPlanNode, GenericPlanRef};
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchFilter, BatchProject, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef,
    PredicatePushdown, StreamTableScan, ToBatch, ToStream,
};
use crate::catalog::{ColumnId, IndexCatalog};
use crate::error::Result;
use crate::expr::{CorrelatedInputRef, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    BatchSeqScan, ColumnPruningContext, LogicalFilter, LogicalProject, LogicalValues,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Cardinality, Order, WatermarkColumns};
use crate::optimizer::rule::IndexSelectionRule;
use crate::optimizer::ApplyResult;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};
use crate::TableCatalog;

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalScan {
    pub base: PlanBase<Logical>,
    core: generic::TableScan,
}

impl From<generic::TableScan> for LogicalScan {
    fn from(core: generic::TableScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::TableScan> for PlanRef {
    fn from(core: generic::TableScan) -> Self {
        LogicalScan::from(core).into()
    }
}

impl LogicalScan {
    /// Create a [`LogicalScan`] node. Used by planner.
    pub fn create(
        table_name: String, // explain-only
        table_catalog: Arc<TableCatalog>,
        indexes: Vec<Rc<IndexCatalog>>,
        ctx: OptimizerContextRef,
        as_of: Option<AsOf>,
        table_cardinality: Cardinality,
    ) -> Self {
        let output_col_idx: Vec<usize> = (0..table_catalog.columns().len()).collect();
        generic::TableScan::new(
            table_name,
            output_col_idx,
            table_catalog,
            indexes,
            ctx,
            Condition::true_cond(),
            as_of,
            table_cardinality,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }

    /// The cardinality of the table **without** applying the predicate.
    pub fn table_cardinality(&self) -> Cardinality {
        self.core.table_cardinality
    }

    // FIXME(kwannoel): Fetch from `table_catalog` + lazily instantiate?
    /// Get a reference to the logical scan's table desc.
    pub fn table_desc(&self) -> &TableDesc {
        self.core.table_desc.as_ref()
    }

    pub fn table_catalog(&self) -> Arc<TableCatalog> {
        self.core.table_catalog.clone()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.core.output_column_ids()
    }

    /// Get all indexes on this table
    pub fn indexes(&self) -> &[Rc<IndexCatalog>] {
        &self.core.indexes
    }

    /// Get the logical scan's filter predicate
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    /// Return indices of fields the output is ordered by and
    /// corresponding direction
    pub fn get_out_column_index_order(&self) -> Order {
        self.core.get_out_column_index_order()
    }

    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        self.core.distribution_key()
    }

    pub fn watermark_columns(&self) -> WatermarkColumns {
        self.core.watermark_columns()
    }

    /// Return indexes can satisfy the required order.
    pub fn indexes_satisfy_order(&self, required_order: &Order) -> Vec<&Rc<IndexCatalog>> {
        let output_col_map = self
            .output_col_idx()
            .iter()
            .cloned()
            .enumerate()
            .map(|(id, col)| (col, id))
            .collect::<BTreeMap<_, _>>();
        let unmatched_idx = output_col_map.len();
        self.indexes()
            .iter()
            .filter(|idx| {
                let s2p_mapping = idx.secondary_to_primary_mapping();
                Order {
                    column_orders: idx
                        .index_table
                        .pk()
                        .iter()
                        .map(|idx_item| {
                            let idx = match s2p_mapping.get(&idx_item.column_index) {
                                Some(col_idx) => {
                                    *output_col_map.get(col_idx).unwrap_or(&unmatched_idx)
                                }
                                // After we support index on expressions, we need to handle the case where the column is not in the `s2p_mapping`.
                                None => unmatched_idx,
                            };
                            ColumnOrder::new(idx, idx_item.order_type)
                        })
                        .collect(),
                }
                .satisfies(required_order)
            })
            .collect()
    }

    /// If the index can cover the scan, transform it to the index scan.
    pub fn to_index_scan_if_index_covered(&self, index: &Rc<IndexCatalog>) -> Option<LogicalScan> {
        let p2s_mapping = index.primary_to_secondary_mapping();
        if self
            .required_col_idx()
            .iter()
            .all(|x| p2s_mapping.contains_key(x))
        {
            let index_scan = self.core.to_index_scan(
                &index.name,
                index.index_table.clone(),
                p2s_mapping,
                index.function_mapping(),
            );
            Some(index_scan.into())
        } else {
            None
        }
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        self.core.primary_key()
    }

    /// a vec of `InputRef` corresponding to `output_col_idx`, which can represent a pulled project.
    fn output_idx_to_input_ref(&self) -> Vec<ExprImpl> {
        let output_idx = self
            .output_col_idx()
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                InputRef::new(i, self.table_desc().columns[col_idx].data_type.clone()).into()
            })
            .collect_vec();
        output_idx
    }

    /// Undo predicate push down when predicate in scan is not supported.
    pub fn predicate_pull_up(&self) -> (generic::TableScan, Condition, Option<Vec<ExprImpl>>) {
        let mut predicate = self.predicate().clone();
        if predicate.always_true() {
            return (self.core.clone(), Condition::true_cond(), None);
        }

        let mut inverse_mapping = {
            let mapping = ColIndexMapping::new(
                self.required_col_idx().iter().map(|i| Some(*i)).collect(),
                self.table_desc().columns.len(),
            );
            // Since `required_col_idx` mapping is not invertible, we need to inverse manually.
            let mut inverse_map = vec![None; mapping.target_size()];
            for (src, dst) in mapping.mapping_pairs() {
                inverse_map[dst] = Some(src);
            }
            ColIndexMapping::new(inverse_map, mapping.source_size())
        };

        predicate = predicate.rewrite_expr(&mut inverse_mapping);

        let scan_without_predicate = generic::TableScan::new(
            self.table_name().to_owned(),
            self.required_col_idx().to_vec(),
            self.core.table_catalog.clone(),
            self.indexes().to_vec(),
            self.ctx(),
            Condition::true_cond(),
            self.as_of(),
            self.table_cardinality(),
        );
        let project_expr = if self.required_col_idx() != self.output_col_idx() {
            Some(self.output_idx_to_input_ref())
        } else {
            None
        };
        (scan_without_predicate, predicate, project_expr)
    }

    fn clone_with_predicate(&self, predicate: Condition) -> Self {
        generic::TableScan::new_inner(
            self.table_name().to_owned(),
            self.output_col_idx().to_vec(),
            self.table_catalog(),
            self.indexes().to_vec(),
            self.base.ctx().clone(),
            predicate,
            self.as_of(),
            self.table_cardinality(),
        )
        .into()
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::TableScan::new_inner(
            self.table_name().to_owned(),
            output_col_idx,
            self.core.table_catalog.clone(),
            self.indexes().to_vec(),
            self.base.ctx().clone(),
            self.predicate().clone(),
            self.as_of(),
            self.table_cardinality(),
        )
        .into()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }

    pub fn required_col_idx(&self) -> &Vec<usize> {
        &self.core.required_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalScan}

impl Distill for LogicalScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns =
            self.predicate().always_true() || self.output_col_idx() == self.required_col_idx();
        let key = if key_is_columns {
            "columns"
        } else {
            "output_columns"
        };
        vec.push((key, self.core.columns_pretty(verbose)));
        if !key_is_columns {
            vec.push((
                "required_columns",
                Pretty::Array(
                    self.required_col_idx()
                        .iter()
                        .map(|i| {
                            let col_name = &self.table_desc().columns[*i].name;
                            Pretty::from(if verbose {
                                format!("{}.{}", self.table_name(), col_name)
                            } else {
                                col_name.to_string()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        if !self.predicate().always_true() {
            let input_schema = self.core.fields_pretty_schema();
            vec.push((
                "predicate",
                Pretty::display(&ConditionDisplay {
                    condition: self.predicate(),
                    input_schema: &input_schema,
                }),
            ))
        }

        if self.table_cardinality() != Cardinality::unknown() {
            vec.push(("cardinality", Pretty::display(&self.table_cardinality())));
        }

        childless_record("LogicalScan", vec)
    }
}

impl ColPrunable for LogicalScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.required_col_idx()[*i])
            .collect();
        assert!(output_col_idx
            .iter()
            .all(|i| self.output_col_idx().contains(i)));

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable for LogicalScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalScan {
    fn predicate_pushdown(
        &self,
        mut predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // If the predicate contains `CorrelatedInputRef` or `now()`. We don't push down.
        // This case could come from the predicate push down before the subquery unnesting.
        struct HasCorrelated {
            has: bool,
        }
        impl ExprVisitor for HasCorrelated {
            fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
                self.has = true;
            }
        }
        let non_pushable_predicate: Vec<_> = predicate
            .conjunctions
            .extract_if(|expr| {
                if expr.count_nows() > 0 {
                    true
                } else {
                    let mut visitor = HasCorrelated { has: false };
                    visitor.visit_expr(expr);
                    visitor.has
                }
            })
            .collect();
        let predicate = predicate.rewrite_expr(&mut ColIndexMapping::new(
            self.output_col_idx().iter().map(|i| Some(*i)).collect(),
            self.table_desc().columns.len(),
        ));
        if non_pushable_predicate.is_empty() {
            self.clone_with_predicate(predicate.and(self.predicate().clone()))
                .into()
        } else {
            LogicalFilter::create(
                self.clone_with_predicate(predicate.and(self.predicate().clone()))
                    .into(),
                Condition {
                    conjunctions: non_pushable_predicate,
                },
            )
        }
    }
}

impl LogicalScan {
    fn to_batch_inner_with_required(&self, required_order: &Order) -> Result<PlanRef> {
        if self.predicate().always_true() {
            required_order
                .enforce_if_not_satisfies(BatchSeqScan::new(self.core.clone(), vec![], None).into())
        } else {
            let (scan_ranges, predicate) = self.predicate().clone().split_to_scan_ranges(
                self.core.table_desc.clone(),
                self.base.ctx().session_ctx().config().max_split_range_gap() as u64,
            )?;
            let mut scan = self.clone();
            scan.core.predicate = predicate; // We want to keep `required_col_idx` unchanged, so do not call `clone_with_predicate`.

            let plan: PlanRef = if scan.core.predicate.always_false() {
                LogicalValues::create(vec![], scan.core.schema(), scan.core.ctx).to_batch()?
            } else {
                let (scan, predicate, project_expr) = scan.predicate_pull_up();

                let mut plan: PlanRef = BatchSeqScan::new(scan, scan_ranges, None).into();
                if !predicate.always_true() {
                    plan = BatchFilter::new(generic::Filter::new(predicate, plan)).into();
                }
                if let Some(exprs) = project_expr {
                    plan = BatchProject::new(generic::Project::new(exprs, plan)).into()
                }
                plan
            };

            assert_eq!(plan.schema(), self.schema());
            required_order.enforce_if_not_satisfies(plan)
        }
    }

    // For every index, check if the order of the index satisfies the required_order
    // If yes, use an index scan
    fn use_index_scan_if_order_is_satisfied(
        &self,
        required_order: &Order,
    ) -> Option<Result<PlanRef>> {
        if required_order.column_orders.is_empty() {
            return None;
        }

        let order_satisfied_index = self.indexes_satisfy_order(required_order);
        for index in order_satisfied_index {
            if let Some(index_scan) = self.to_index_scan_if_index_covered(index) {
                return Some(index_scan.to_batch());
            }
        }

        None
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let new = self.clone_with_predicate(self.predicate().clone());

        if !new.indexes().is_empty() {
            let index_selection_rule = IndexSelectionRule::create();
            if let ApplyResult::Ok(applied) = index_selection_rule.apply(new.clone().into()) {
                if let Some(scan) = applied.as_logical_scan() {
                    // covering index
                    return required_order.enforce_if_not_satisfies(scan.to_batch()?);
                } else if let Some(join) = applied.as_logical_join() {
                    // index lookup join
                    return required_order
                        .enforce_if_not_satisfies(join.index_lookup_join_to_batch_lookup_join()?);
                } else {
                    unreachable!();
                }
            } else {
                // Try to make use of index if it satisfies the required order
                if let Some(plan_ref) = new.use_index_scan_if_order_is_satisfied(required_order) {
                    return plan_ref;
                }
            }
        }
        new.to_batch_inner_with_required(required_order)
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        if self.predicate().always_true() {
            // Force rewrite scan type to cross-db scan
            if self.core.table_catalog.database_id != self.base.ctx().session_ctx().database_id() {
                Ok(StreamTableScan::new_with_stream_scan_type(
                    self.core.clone(),
                    StreamScanType::CrossDbSnapshotBackfill,
                )
                .into())
            } else {
                Ok(StreamTableScan::new_with_stream_scan_type(
                    self.core.clone(),
                    ctx.stream_scan_type(),
                )
                .into())
            }
        } else {
            let (scan, predicate, project_expr) = self.predicate_pull_up();
            let mut plan = LogicalFilter::create(scan.into(), predicate);
            if let Some(exprs) = project_expr {
                plan = LogicalProject::create(plan, exprs)
            }
            plan.to_stream(ctx)
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        match self.base.stream_key().is_none() {
            true => {
                let mut col_ids = HashSet::new();

                for &idx in self.output_col_idx() {
                    col_ids.insert(self.table_desc().columns[idx].column_id);
                }
                let col_need_to_add = self
                    .table_desc()
                    .pk
                    .iter()
                    .filter_map(|c| {
                        if !col_ids.contains(&self.table_desc().columns[c.column_index].column_id) {
                            Some(c.column_index)
                        } else {
                            None
                        }
                    })
                    .collect_vec();

                let mut output_col_idx = self.output_col_idx().clone();
                output_col_idx.extend(col_need_to_add);
                let new_len = output_col_idx.len();
                Ok((
                    self.clone_with_output_indices(output_col_idx).into(),
                    ColIndexMapping::identity_or_none(self.schema().len(), new_len),
                ))
            }
            false => Ok((
                self.clone().into(),
                ColIndexMapping::identity(self.schema().len()),
            )),
        }
    }
}
