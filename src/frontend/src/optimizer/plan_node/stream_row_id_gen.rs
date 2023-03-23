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

use std::fmt;

use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use crate::{optimizer::property::Distribution, stream_fragmenter::BuildFragmentGraphState};

use super::{ExprRewritable, LogicalRowIdGen, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};

/// `StreamRowIdGen` holds a stream `PlanBase` and a `LogicalRowIdGen`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamRowIdGen {
    pub base: PlanBase,
    logical: LogicalRowIdGen,
}

impl StreamRowIdGen {
    /// Create a `StreamRowIdGen` with intermediate `LogicalRowIdGen`
    pub fn new(logical: LogicalRowIdGen) -> Self {
        let distribution = if logical.input().append_only() {
            // remove exchange for append only source
            Distribution::HashShard(vec![logical.row_id_index()])
        } else {
            logical.input().distribution().clone()
        };
        let base = PlanBase::new_stream(
            logical.ctx(),
            logical.schema().clone(),
            logical.logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            distribution,
            logical.input().append_only(),
            logical.input().watermark_columns().clone(),
        );
        Self { base, logical }
    }
}

impl fmt::Display for StreamRowIdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamRowIdGen {{ row_id_index: {} }}",
            self.logical.row_id_index()
        )
    }
}

impl PlanTreeNodeUnary for StreamRowIdGen {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! {StreamRowIdGen}

impl StreamNode for StreamRowIdGen {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode{
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::RowIdGen(RowIdGenNode {
            row_id_index: self.logical.row_id_index() as _,
        })
    }
}

impl ExprRewritable for StreamRowIdGen {}
