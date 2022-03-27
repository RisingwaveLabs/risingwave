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

use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::ProjectNode;

use super::{
    LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::expr::Expr;
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows
#[derive(Debug, Clone)]
pub struct BatchProject {
    pub base: PlanBase,
    logical: LogicalProject,
}

impl BatchProject {
    pub fn new(logical: LogicalProject) -> Self {
        let ctx = logical.base.ctx.clone();
        let i2o = LogicalProject::i2o_col_mapping(logical.input().schema().len(), logical.exprs());

        let i2o = LogicalProject::i2o_col_mapping(logical.input().schema().len(), logical.exprs());
        let distribution = i2o.rewrite_provided_distribution(logical.input().distribution());
        // TODO: Derive order from input
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            distribution,
            Order::any().clone(),
        );
        BatchProject { base, logical }
    }
}

impl fmt::Display for BatchProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchProject")
    }
}

impl PlanTreeNodeUnary for BatchProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchProject }

impl WithSchema for BatchProject {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchProject {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &Distribution,
    ) -> PlanRef {
        let o2i =
            LogicalProject::o2i_col_mapping(self.input().schema().len(), self.logical.exprs());
        let input_dist = match required_dist {
            Distribution::HashShard(dists) => {
                let input_dists = dists
                    .iter()
                    .map(|hash_col| o2i.try_map(*hash_col))
                    .collect::<Option<Vec<_>>>();
                match input_dists {
                    Some(input_dists) => Distribution::HashShard(input_dists),
                    None => Distribution::AnyShard,
                }
            }
            Distribution::AnyShard => Distribution::AnyShard,
            _ => Distribution::Any,
        };
        let new_input = self
            .input()
            .to_distributed_with_required(required_order, &input_dist);
        let new_logical = self.logical.clone_with_input(new_input);
        let batch_plan = BatchProject::new(new_logical);
        let batch_plan = required_order.enforce_if_not_satisfies(batch_plan.into());
        required_dist.enforce_if_not_satisfies(batch_plan, required_order)
    }
}

impl ToBatchProst for BatchProject {
    fn to_batch_prost_body(&self) -> NodeBody {
        let select_list = self
            .logical
            .exprs()
            .iter()
            .map(Expr::to_protobuf)
            .collect::<Vec<ExprNode>>();
        NodeBody::Project(ProjectNode { select_list })
    }
}
