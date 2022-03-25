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
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;

use super::logical_agg::PlanAggCall;
use super::{LogicalAgg, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamSimpleAgg {
    pub base: PlanBase,
    logical: LogicalAgg,
}

impl StreamSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.pk_indices.to_vec();
        // Simple agg executor might change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            Distribution::any().clone(),
            false,
        );
        StreamSimpleAgg { base, logical }
    }
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.logical.agg_calls()
    }
}

impl fmt::Display for StreamSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("StreamSimpleAgg")
            .field("aggs", &self.agg_calls())
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamSimpleAgg {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}
impl_plan_tree_node_for_unary! { StreamSimpleAgg }

impl WithSchema for StreamSimpleAgg {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamSimpleAgg {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        // TODO: local or global simple agg?
        ProstStreamNode::GlobalSimpleAggNode(SimpleAggNode {
            agg_calls: self
                .agg_calls()
                .iter()
                .map(PlanAggCall::to_protobuf)
                .collect(),
        })
    }
}
