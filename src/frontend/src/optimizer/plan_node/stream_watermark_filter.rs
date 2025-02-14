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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, FieldDisplay};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::catalog::WatermarkDesc;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{childless_record, watermark_pretty, Distill, TableCatalogBuilder};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{ExprDisplay, ExprImpl};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamWatermarkFilter {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    watermark_descs: Vec<WatermarkDesc>,
}

impl StreamWatermarkFilter {
    pub fn new(input: PlanRef, watermark_descs: Vec<WatermarkDesc>) -> Self {
        let ctx = input.ctx();
        let mut watermark_columns = input.watermark_columns().clone();
        for i in &watermark_descs {
            watermark_columns.insert(
                i.get_watermark_idx() as usize,
                ctx.next_watermark_group_id(), // each watermark descriptor creates a new watermark group
            );
        }
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.stream_key().map(|v| v.to_vec()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
            false, // TODO(rc): decide EOWC property
            watermark_columns,
            // watermark filter preserves input order and hence monotonicity
            input.columns_monotonicity().clone(),
        );
        Self::with_base(base, input, watermark_descs)
    }

    fn with_base(
        base: PlanBase<Stream>,
        input: PlanRef,
        watermark_descs: Vec<WatermarkDesc>,
    ) -> Self {
        Self {
            base,
            input,
            watermark_descs,
        }
    }
}

impl Distill for StreamWatermarkFilter {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let input_schema = self.input.schema();

        let display_watermark_descs = (self.watermark_descs.iter())
            .map(|desc| {
                let expr = ExprDisplay {
                    expr: &ExprImpl::from_expr_proto(desc.get_expr().unwrap()).unwrap(),
                    input_schema,
                };
                let fields = vec![
                    (
                        "column",
                        Pretty::display(&FieldDisplay(
                            &self.input.schema()[desc.watermark_idx as usize],
                        )),
                    ),
                    ("expr", Pretty::display(&expr)),
                ];
                Pretty::childless_record("Desc", fields)
            })
            .collect();
        let display_output_watermark_groups =
            watermark_pretty(self.base.watermark_columns(), input_schema).unwrap();
        let fields = vec![
            ("watermark_descs", Pretty::Array(display_watermark_descs)),
            ("output_watermarks", display_output_watermark_groups),
        ];
        childless_record("StreamWatermarkFilter", fields)
    }
}

impl PlanTreeNodeUnary for StreamWatermarkFilter {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.watermark_descs.clone())
    }
}

impl_plan_tree_node_for_unary! {StreamWatermarkFilter}

pub fn infer_internal_table_catalog(watermark_type: DataType) -> TableCatalog {
    let mut builder = TableCatalogBuilder::default();

    let key = Field {
        data_type: DataType::Int16,
        name: "vnode".to_owned(),
    };
    let value = Field {
        data_type: watermark_type,
        name: "offset".to_owned(),
    };

    let ordered_col_idx = builder.add_column(&key);
    builder.add_column(&value);
    builder.add_order_column(ordered_col_idx, OrderType::ascending());

    builder.set_vnode_col_idx(0);
    builder.set_value_indices(vec![1]);

    builder.build(vec![0], 1)
}

impl StreamNode for StreamWatermarkFilter {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        // TODO(yuhao): allow multiple watermark on source.
        let [watermark_desc]: [_; 1] = self.watermark_descs.clone().try_into().unwrap();
        let watermark_type = (&watermark_desc.expr.unwrap().return_type.unwrap()).into();

        let table = infer_internal_table_catalog(watermark_type);

        PbNodeBody::WatermarkFilter(Box::new(WatermarkFilterNode {
            watermark_descs: self.watermark_descs.clone(),
            tables: vec![table
                .with_id(state.gen_table_id_wrapped())
                .to_internal_table_prost()],
        }))
    }
}

// TODO(yuhao): may impl a `ExprRewritable` after store `ExplImpl` in catalog.
impl ExprRewritable for StreamWatermarkFilter {}

impl ExprVisitable for StreamWatermarkFilter {}
