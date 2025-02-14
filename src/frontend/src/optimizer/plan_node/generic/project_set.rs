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

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::{DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::expr::{Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::batch::BatchPlanRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::{FunctionalDependencySet, Order};
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt};

/// [`ProjectSet`] projects one row multiple times according to `select_list`.
///
/// Different from `Project`, it supports [`TableFunction`](crate::expr::TableFunction)s.
/// See also [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem) for examples.
///
/// To have a pk, it has a hidden column `projected_row_id` at the beginning. The implementation of
/// `LogicalProjectSet` is highly similar to [`super::super::LogicalProject`], except for the
/// additional hidden column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProjectSet<PlanRef> {
    pub select_list: Vec<ExprImpl>,
    pub input: PlanRef,
}

impl<PlanRef> ProjectSet<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.select_list = self
            .select_list
            .iter()
            .map(|e| r.rewrite_expr(e.clone()))
            .collect();
    }

    pub(crate) fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.select_list.iter().for_each(|e| v.visit_expr(e));
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.select_list, self.input)
    }
}

impl<PlanRef> DistillUnit for ProjectSet<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let fields = vec![("select_list", Pretty::debug(&self.select_list))];
        childless_record(name, fields)
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for ProjectSet<PlanRef> {
    fn schema(&self) -> Schema {
        let input_schema = self.input.schema();
        let o2i = self.o2i_col_mapping();
        let mut fields = vec![Field::with_name(DataType::Int64, "projected_row_id")];
        fields.extend(self.select_list.iter().enumerate().map(|(idx, expr)| {
            let idx = idx + 1;
            // Get field info from o2i.
            let name = match o2i.try_map(idx) {
                Some(input_idx) => input_schema.fields()[input_idx].name.clone(),
                None => format!("{:?}", ExprDisplay { expr, input_schema }),
            };
            Field::with_name(expr.return_type(), name)
        }));

        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        let i2o = self.i2o_col_mapping();
        let mut pk = self
            .input
            .stream_key()?
            .iter()
            .map(|pk_col| i2o.try_map(*pk_col))
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        // add `projected_row_id` to pk
        pk.push(0);
        Some(pk)
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let i2o = self.i2o_col_mapping();
        i2o.rewrite_functional_dependency_set(self.input.functional_dependency().clone())
    }
}

impl<PlanRef: GenericPlanRef> ProjectSet<PlanRef> {
    /// Gets the Mapping of columnIndex from output column index to input column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![None; 1 + self.select_list.len()];
        for (i, item) in self.select_list.iter().enumerate() {
            if let ExprImpl::InputRef(input) = item {
                map[1 + i] = Some(input.index())
            }
        }
        ColIndexMapping::new(map, input_len)
    }

    /// Gets the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![None; input_len];
        for (i, item) in self.select_list.iter().enumerate() {
            if let ExprImpl::InputRef(input) = item {
                map[input.index()] = Some(1 + i)
            }
        }
        ColIndexMapping::new(map, 1 + self.select_list.len())
    }
}

impl<PlanRef: BatchPlanRef> ProjectSet<PlanRef> {
    /// Map the order of the input to use the updated indices
    pub fn get_out_column_index_order(&self) -> Order {
        self.i2o_col_mapping()
            .rewrite_provided_order(self.input.order())
    }
}
