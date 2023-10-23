// Copyright 2023 RisingWave Labs
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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::FilterNode;

use super::utils::impl_distill_by_unit;
use super::{generic, ExprRewritable, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::expr::{Expr, ExprImpl, ExprRewriter};
use crate::optimizer::plan_node::{PlanBase, ToLocalBatch};
use crate::utils::Condition;

/// `BatchFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchFilter {
    pub base: PlanBase,
    core: generic::Filter<PlanRef>,
}

impl BatchFilter {
    pub fn new(core: generic::Filter<PlanRef>) -> Self {
        // TODO: derive from input
        let base = PlanBase::new_batch_from_logical(
            &core,
            core.input.distribution().clone(),
            core.input.order().clone(),
        );
        BatchFilter { base, core }
    }

    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }
}
impl_distill_by_unit!(BatchFilter, core, "BatchFilter");

impl PlanTreeNodeUnary for BatchFilter {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchFilter }

impl ToDistributedBatch for BatchFilter {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchFilter {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Filter(FilterNode {
            search_condition: Some(ExprImpl::from(self.core.predicate.clone()).to_expr_proto()),
        })
    }
}

impl ToLocalBatch for BatchFilter {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchFilter {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}
