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

use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SortAggNode;

use super::batch::prelude::*;
use super::generic::{self, PlanAggCall};
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{BatchExchange, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order, RequiredDist};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSimpleAgg {
    pub base: PlanBase<Batch>,
    pub core: generic::Agg<PlanRef>,
}

impl BatchSimpleAgg {
    pub fn new(core: generic::Agg<PlanRef>) -> Self {
        let input_dist = core.input.distribution().clone();
        let base = PlanBase::new_batch_with_core(&core, input_dist, Order::any());
        BatchSimpleAgg { base, core }
    }

    pub fn agg_calls(&self) -> &[PlanAggCall] {
        &self.core.agg_calls
    }

    fn two_phase_agg_enabled(&self) -> bool {
        self.base
            .ctx()
            .session_ctx()
            .config()
            .enable_two_phase_agg()
    }

    pub(crate) fn can_two_phase_agg(&self) -> bool {
        self.core.can_two_phase_agg()
            && self
                .core
                // Ban two phase approx percentile.
                .agg_calls
                .iter()
                .map(|agg_call| &agg_call.agg_type)
                .all(|agg_type| !matches!(agg_type, AggType::Builtin(PbAggKind::ApproxPercentile)))
            && self.two_phase_agg_enabled()
    }
}

impl PlanTreeNodeUnary for BatchSimpleAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(generic::Agg {
            input,
            ..self.core.clone()
        })
    }
}
impl_plan_tree_node_for_unary! { BatchSimpleAgg }
impl_distill_by_unit!(BatchSimpleAgg, core, "BatchSimpleAgg");

impl ToDistributedBatch for BatchSimpleAgg {
    fn to_distributed(&self) -> Result<PlanRef> {
        // Ensure input is distributed, batch phase might not distribute it
        // (e.g. see distribution of BatchSeqScan::new vs BatchSeqScan::to_distributed)
        let dist_input = self.input().to_distributed()?;

        // TODO: distinct agg cannot use 2-phase agg yet.
        if dist_input.distribution().satisfies(&RequiredDist::AnyShard) && self.can_two_phase_agg()
        {
            // partial agg
            let partial_agg = self.clone_with_input(dist_input).into();

            // insert exchange
            let exchange =
                BatchExchange::new(partial_agg, Order::any(), Distribution::Single).into();

            // insert total agg
            let total_agg_types = self
                .core
                .agg_calls
                .iter()
                .enumerate()
                .map(|(partial_output_idx, agg_call)| {
                    agg_call.partial_to_total_agg_call(partial_output_idx)
                })
                .collect();
            let total_agg_logical =
                generic::Agg::new(total_agg_types, self.core.group_key.clone(), exchange);
            Ok(BatchSimpleAgg::new(total_agg_logical).into())
        } else {
            let new_input = self
                .input()
                .to_distributed_with_required(&Order::any(), &RequiredDist::single())?;
            Ok(self.clone_with_input(new_input).into())
        }
    }
}

impl ToBatchPb for BatchSimpleAgg {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::SortAgg(SortAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
            // We treat simple agg as a special sort agg without group key.
            group_key: vec![],
        })
    }
}

impl ToLocalBatch for BatchSimpleAgg {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;

        let new_input =
            RequiredDist::single().enforce_if_not_satisfies(new_input, &Order::any())?;

        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchSimpleAgg {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchSimpleAgg {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
