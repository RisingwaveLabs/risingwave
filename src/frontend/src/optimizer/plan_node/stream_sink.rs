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

use risingwave_common::error::Result;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::{PlanBase, PlanRef, StreamNode};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone)]
pub struct StreamSink {
    pub base: PlanBase,
    input: PlanRef,
    table: TableCatalog,
}

impl StreamSink {
    fn derive_plan_base(input: &PlanRef) -> Result<PlanBase> {
        let ctx = input.ctx();

        let schema = input.schema().clone();
        let pk_indices = input.logical_pk();

        Ok(PlanBase::new_stream(
            ctx,
            schema,
            pk_indices.to_vec(),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
        ))
    }

    #[must_use]
    pub fn new(input: PlanRef, table: TableCatalog) -> Self {
        let base = Self::derive_plan_base(&input).unwrap();
        Self { base, input, table }
    }

    pub fn with_base(input: PlanRef, table: TableCatalog, base: PlanBase) -> Self {
        Self { base, input, table }
    }

    pub fn table(&self) -> &TableCatalog {
        &self.table
    }
}

impl PlanTreeNodeUnary for StreamSink {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.table.clone())
        // TODO(nanderstabel): Add assertions (assert_eq!)
    }
}

impl_plan_tree_node_for_unary! { StreamSink }

impl fmt::Display for StreamSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamSink");
        builder.finish()
    }
}

impl StreamNode for StreamSink {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;

        ProstStreamNode::Sink(SinkNode {
            table_id: self.table.id().into(),
            column_ids: vec![], // TODO(nanderstabel): fix empty Vector
            table: Some(self.table.to_internal_table_prost()),
        })
    }
}
