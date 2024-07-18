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

use fixedbitset::FixedBitSet;
use pretty_xmlish::{Pretty, XmlNode};
pub use risingwave_pb::expr::expr_node::Type as ExprType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::DynamicFilterNode;

use super::generic::DynamicFilter;
use super::stream::prelude::*;
use super::utils::{
    childless_record, column_names_pretty, plan_node_name, watermark_pretty, Distill,
};
use super::{generic, ExprRewritable};
use crate::expr::Expr;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNodeBinary, StreamNode};
use crate::optimizer::property::MonotonicityMap;
use crate::optimizer::PlanRef;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamDynamicFilter {
    pub base: PlanBase<Stream>,
    core: generic::DynamicFilter<PlanRef>,
    cleaned_by_watermark: bool,
    condition_always_relax: bool,
}

impl StreamDynamicFilter {
    pub fn new(core: DynamicFilter<PlanRef>) -> Self {
        let right_non_decreasing = core.right().columns_monotonicity()[0].is_non_decreasing();
        let condition_always_relax = right_non_decreasing
            && matches!(
                core.comparator(),
                ExprType::LessThan | ExprType::LessThanOrEqual
            );

        let out_append_only = if condition_always_relax {
            core.left().append_only()
        } else {
            false
        };

        let base = PlanBase::new_stream_with_core(
            &core,
            core.left().distribution().clone(),
            out_append_only,
            false, // TODO(rc): decide EOWC property
            Self::derive_watermark_columns(&core),
            MonotonicityMap::new(), // TODO: derive monotonicity
        );
        let cleaned_by_watermark = Self::cleaned_by_watermark(&core);
        Self {
            base,
            core,
            cleaned_by_watermark,
            condition_always_relax,
        }
    }

    fn derive_watermark_columns(core: &DynamicFilter<PlanRef>) -> FixedBitSet {
        let mut res = FixedBitSet::with_capacity(core.left().schema().len());
        let rhs_watermark_columns = core.right().watermark_columns();
        if rhs_watermark_columns.contains(0) {
            match core.comparator() {
                // We can derive output watermark only if the output is supposed to be always >= rhs.
                // While we have to keep in mind that, the propagation of watermark messages from
                // the right input must be delayed until `Update`/`Delete`s are sent to downstream,
                // otherwise, we will have watermark messages sent before the `Delete` of old rows.
                ExprType::GreaterThan | ExprType::GreaterThanOrEqual => {
                    res.set(core.left_index(), true)
                }
                _ => {}
            }
        }
        res
    }

    fn cleaned_by_watermark(core: &DynamicFilter<PlanRef>) -> bool {
        let rhs_watermark_columns = core.right().watermark_columns();
        if rhs_watermark_columns.contains(0) {
            match core.comparator() {
                ExprType::GreaterThan | ExprType::GreaterThanOrEqual => {
                    // For >= and >, watermark on rhs means there's no change that rows older than the watermark will
                    // ever be `Insert`ed again. So, we can clean up the state table. In this case, future lhs inputs
                    // that are less than the watermark can be safely ignored, and hence watermark can be propagated to
                    // downstream. See `derive_watermark_columns`.
                    true
                }
                _ => false,
            }
        } else {
            false
        }
    }
}

impl Distill for StreamDynamicFilter {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let pred = self.core.pretty_field();
        let mut vec = Vec::with_capacity(if verbose { 3 } else { 2 });
        vec.push(("predicate", pred));
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        vec.push(("output", column_names_pretty(self.schema())));
        if self.cleaned_by_watermark {
            vec.push((
                "cleaned_by_watermark",
                Pretty::display(&self.cleaned_by_watermark),
            ));
        }
        if self.condition_always_relax {
            vec.push((
                "condition_always_relax",
                Pretty::display(&self.condition_always_relax),
            ));
        }
        childless_record(
            plan_node_name!(
                "StreamDynamicFilter",
                { "append_only", self.append_only() },
            ),
            vec,
        )
    }
}

impl PlanTreeNodeBinary for StreamDynamicFilter {
    fn left(&self) -> PlanRef {
        self.core.left().clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right().clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.core.clone_with_left_right(left, right))
    }
}

impl_plan_tree_node_for_binary! { StreamDynamicFilter }

impl StreamNode for StreamDynamicFilter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        use generic::dynamic_filter::*;
        let cleaned_by_watermark = self.cleaned_by_watermark;
        let condition = self
            .core
            .predicate()
            .as_expr_unless_true()
            .map(|x| x.to_expr_proto());
        let left_index = self.core.left_index();
        let left_table = infer_left_internal_table_catalog(&self.base, left_index)
            .with_id(state.gen_table_id_wrapped())
            .with_cleaned_by_watermark(cleaned_by_watermark);
        let right = self.right();
        let right_table = infer_right_internal_table_catalog(right.plan_base())
            .with_id(state.gen_table_id_wrapped());
        NodeBody::DynamicFilter(DynamicFilterNode {
            left_key: left_index as u32,
            condition,
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
            condition_always_relax: self.condition_always_relax,
        })
    }
}

impl ExprRewritable for StreamDynamicFilter {}

impl ExprVisitable for StreamDynamicFilter {}
