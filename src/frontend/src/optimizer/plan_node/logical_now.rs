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

use pretty_xmlish::XmlNode;
use risingwave_common::bail;

use super::generic::{self, GenericPlanRef};
use super::utils::{childless_record, Distill};
use super::{
    ColPrunable, ColumnPruningContext, ExprRewritable, Logical, LogicalFilter, PlanBase, PlanRef,
    PredicatePushdown, RewriteStreamContext, StreamNow, ToBatch, ToStream, ToStreamContext,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::utils::ColIndexMapping;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalNow {
    pub base: PlanBase<Logical>,
    core: generic::Now,
}

impl LogicalNow {
    pub fn new(core: generic::Now) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl Distill for LogicalNow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = if self.base.ctx().is_explain_verbose() {
            vec![("output", column_names_pretty(self.schema()))]
        } else {
            vec![]
        };

        childless_record("LogicalNow", vec)
    }
}

impl_plan_tree_node_for_leaf! { LogicalNow }

impl ExprRewritable for LogicalNow {}

impl ExprVisitable for LogicalNow {}

impl PredicatePushdown for LogicalNow {
    fn predicate_pushdown(
        &self,
        predicate: crate::utils::Condition,
        _ctx: &mut super::PredicatePushdownContext,
    ) -> crate::PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToStream for LogicalNow {
    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((self.clone().into(), ColIndexMapping::new(vec![Some(0)], 1)))
    }

    /// `to_stream` is equivalent to `to_stream_with_dist_required(RequiredDist::Any)`
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Ok(StreamNow::new(self.core.clone()).into())
    }
}

impl ToBatch for LogicalNow {
    fn to_batch(&self) -> Result<PlanRef> {
        bail!("`LogicalNow` can only be converted to stream")
    }
}

/// The trait for column pruning, only logical plan node will use it, though all plan node impl it.
impl ColPrunable for LogicalNow {
    fn prune_col(&self, _required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        self.clone().into()
    }
}
