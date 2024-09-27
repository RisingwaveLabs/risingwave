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
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::bail;
use risingwave_pb::expr::expr_node::PbType;
use risingwave_pb::plan_common::{AsOfJoinDesc, AsOfJoinType, JoinType, PbAsOfJoinInequalityType};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::AsOfJoinNode;

use super::generic::GenericPlanNode;
use super::stream::prelude::*;
use super::utils::{
    childless_record, plan_node_name, watermark_pretty, Distill, TableCatalogBuilder,
};
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeBinary, StreamNode};
use crate::error::{ErrorCode, Result};
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::plan_node::{EqJoinPredicate, EqJoinPredicateDisplay};
use crate::optimizer::property::{Distribution, MonotonicityMap};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;
use crate::TableCatalog;

/// [`StreamAsOfJoin`] implements [`super::LogicalJoin`] with hash tables.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamAsOfJoin {
    pub base: PlanBase<Stream>,
    core: generic::Join<PlanRef>,

    /// The join condition must be equivalent to `logical.on`, but separated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,

    /// Whether can optimize for append-only stream.
    /// It is true if input of both side is append-only
    is_append_only: bool,

    /// inequality description
    inequality_desc: AsOfJoinDesc,
}

impl StreamAsOfJoin {
    pub fn new(
        core: generic::Join<PlanRef>,
        eq_join_predicate: EqJoinPredicate,
        inequality_desc: AsOfJoinDesc,
    ) -> Self {
        // Inner join won't change the append-only behavior of the stream. The rest might.
        let append_only = match core.join_type {
            JoinType::Inner => core.left.append_only() && core.right.append_only(),
            _ => false,
        };

        let dist = Self::derive_dist(core.left.distribution(), core.right.distribution(), &core);

        // TODO: derive watermarks
        let watermark_columns = FixedBitSet::with_capacity(core.schema().len());

        // TODO: derive from input
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            append_only,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
            MonotonicityMap::new(), // TODO: derive monotonicity
        );

        Self {
            base,
            core,
            eq_join_predicate,
            is_append_only: append_only,
            inequality_desc,
        }
    }

    pub fn get_inequality_desc_from_predicate(
        predicate: EqJoinPredicate,
        left_input_len: usize,
    ) -> Result<AsOfJoinDesc> {
            let expr: ExprImpl = predicate.other_cond().clone().into();
            if let Some((left_input_ref, expr_type, right_input_ref)) = expr.as_comparison_cond() {
                if left_input_ref.index() < left_input_len
                    && right_input_ref.index() >= left_input_len
                {
                    Ok(AsOfJoinDesc {
                        left_idx: left_input_ref.index() as u32,
                        right_idx: (right_input_ref.index() - left_input_len) as u32,
                        inequality_type: Self::expr_type_to_comparison_type(expr_type)?.into(),
                    })
                } else {
                    bail!("inequal condition from the same side should be push down in optimizer");
                }
            } else {
                Err(ErrorCode::InvalidInputSyntax(
                    "AsOf join requires exactly 1 ineuquality condition".to_string(),
                )
                .into())
            }
    }

    fn expr_type_to_comparison_type(expr_type: PbType) -> Result<PbAsOfJoinInequalityType> {
        match expr_type {
            PbType::LessThan => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeLt),
            PbType::LessThanOrEqual => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeLe),
            PbType::GreaterThan => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeGt),
            PbType::GreaterThanOrEqual => Ok(PbAsOfJoinInequalityType::AsOfInequalityTypeGe),
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "Invalid comparison type: {}",
                expr_type.as_str_name()
            ))
            .into()),
        }
    }

    /// Get join type
    pub fn join_type(&self) -> JoinType {
        self.core.join_type
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }

    pub(super) fn derive_dist(
        left: &Distribution,
        right: &Distribution,
        logical: &generic::Join<PlanRef>,
    ) -> Distribution {
        match (left, right) {
            (Distribution::Single, Distribution::Single) => Distribution::Single,
            (Distribution::HashShard(_), Distribution::HashShard(_)) => match logical.join_type {
                JoinType::Unspecified
                | JoinType::FullOuter
                | JoinType::Inner
                | JoinType::LeftOuter
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightOuter => unreachable!(),
                JoinType::AsofInner | JoinType::AsofLeftOuter => {
                    let l2o = logical
                        .l2i_col_mapping()
                        .composite(&logical.i2o_col_mapping());
                    l2o.rewrite_provided_distribution(left)
                }
            },
            (_, _) => unreachable!(
                "suspicious distribution: left: {:?}, right: {:?}",
                left, right
            ),
        }
    }

    pub fn derive_dist_key_in_join_key(&self) -> Vec<usize> {
        let left_dk_indices = self.left().distribution().dist_column_indices().to_vec();
        let right_dk_indices = self.right().distribution().dist_column_indices().to_vec();
        let left_jk_indices = self.eq_join_predicate.left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate.right_eq_indexes();

        assert_eq!(left_jk_indices.len(), right_jk_indices.len());

        let mut dk_indices_in_jk = vec![];

        for (l_dk_idx, r_dk_idx) in left_dk_indices.iter().zip_eq_fast(right_dk_indices.iter()) {
            for dk_idx_in_jk in left_jk_indices.iter().positions(|idx| idx == l_dk_idx) {
                if right_jk_indices[dk_idx_in_jk] == *r_dk_idx {
                    dk_indices_in_jk.push(dk_idx_in_jk);
                    break;
                }
            }
        }

        assert_eq!(dk_indices_in_jk.len(), left_dk_indices.len());
        dk_indices_in_jk
    }

    /// Return stream hash join internal table catalog.
    pub fn infer_internal_table_catalog<I: StreamPlanRef>(
        input: I,
        join_key_indices: Vec<usize>,
        dk_indices_in_jk: Vec<usize>,
        inequality_key_idx: usize,
    ) -> (TableCatalog, Vec<usize>) {
        let schema = input.schema();

        let internal_table_dist_keys = dk_indices_in_jk
            .iter()
            .map(|idx| join_key_indices[*idx])
            .collect_vec();

        // The pk of AsOf join internal table should be join_key + inequality_key + input_pk.
        let join_key_len = join_key_indices.len();
        let mut pk_indices = join_key_indices;

        // dedup the pk in dist key..
        let mut deduped_input_pk_indices = vec![];
        for input_pk_idx in input.stream_key().unwrap() {
            if !pk_indices.contains(input_pk_idx)
                && !deduped_input_pk_indices.contains(input_pk_idx)
            {
                deduped_input_pk_indices.push(*input_pk_idx);
            }
        }

        pk_indices.push(inequality_key_idx);
        pk_indices.extend(deduped_input_pk_indices.clone());

        // Build internal table
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        internal_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk.clone());

        (
            internal_table_catalog_builder.build(internal_table_dist_keys, join_key_len),
            deduped_input_pk_indices,
        )
    }
}

impl Distill for StreamAsOfJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let (ljk, rjk) = self
            .eq_join_predicate
            .eq_indexes()
            .first()
            .cloned()
            .expect("first join key");

        let name = plan_node_name!("StreamAsOfJoin",
            { "window", self.left().watermark_columns().contains(ljk) && self.right().watermark_columns().contains(rjk) },
            { "append_only", self.is_append_only },
        );
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(6);
        vec.push(("type", Pretty::debug(&self.core.join_type)));

        let concat_schema = self.core.concat_schema();
        vec.push((
            "predicate",
            Pretty::debug(&EqJoinPredicateDisplay {
                eq_join_predicate: self.eq_join_predicate(),
                input_schema: &concat_schema,
            }),
        ));

        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }

        if verbose {
            let data = IndicesDisplay::from_join(&self.core, &concat_schema);
            vec.push(("output", data));
        }

        childless_record(name, vec)
    }
}

impl PlanTreeNodeBinary for StreamAsOfJoin {
    fn left(&self) -> PlanRef {
        self.core.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.core.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.left = left;
        core.right = right;
        Self::new(core, self.eq_join_predicate.clone(), self.inequality_desc)
    }
}

impl_plan_tree_node_for_binary! { StreamAsOfJoin }

impl StreamNode for StreamAsOfJoin {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let left_jk_indices = self.eq_join_predicate.left_eq_indexes();
        let right_jk_indices = self.eq_join_predicate.right_eq_indexes();
        let left_jk_indices_prost = left_jk_indices.iter().map(|idx| *idx as i32).collect_vec();
        let right_jk_indices_prost = right_jk_indices.iter().map(|idx| *idx as i32).collect_vec();

        let dk_indices_in_jk = self.derive_dist_key_in_join_key();

        let (left_table, left_deduped_input_pk_indices) = Self::infer_internal_table_catalog(
            self.left().plan_base(),
            left_jk_indices,
            dk_indices_in_jk.clone(),
            self.inequality_desc.left_idx as usize,
        );
        let (right_table, right_deduped_input_pk_indices) = Self::infer_internal_table_catalog(
            self.right().plan_base(),
            right_jk_indices,
            dk_indices_in_jk,
            self.inequality_desc.right_idx as usize,
        );

        let left_deduped_input_pk_indices = left_deduped_input_pk_indices
            .iter()
            .map(|idx| *idx as u32)
            .collect_vec();

        let right_deduped_input_pk_indices = right_deduped_input_pk_indices
            .iter()
            .map(|idx| *idx as u32)
            .collect_vec();

        let left_table = left_table.with_id(state.gen_table_id_wrapped());
        let right_table = right_table.with_id(state.gen_table_id_wrapped());

        let null_safe_prost = self.eq_join_predicate.null_safes().into_iter().collect();

        let asof_join_type = match self.core.join_type {
            JoinType::AsofInner => AsOfJoinType::Inner,
            JoinType::AsofLeftOuter => AsOfJoinType::LeftOuter,
            _ => unreachable!(),
        };

        NodeBody::AsOfJoin(AsOfJoinNode {
            join_type: asof_join_type.into(),
            left_key: left_jk_indices_prost,
            right_key: right_jk_indices_prost,
            null_safe: null_safe_prost,
            left_table: Some(left_table.to_internal_table_prost()),
            right_table: Some(right_table.to_internal_table_prost()),
            left_deduped_input_pk_indices,
            right_deduped_input_pk_indices,
            output_indices: self.core.output_indices.iter().map(|&x| x as u32).collect(),
            asof_desc: Some(self.inequality_desc),
        })
    }
}

impl ExprRewritable for StreamAsOfJoin {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        let eq_join_predicate = self.eq_join_predicate.rewrite_exprs(r);
        let desc = Self::get_inequality_desc_from_predicate(
            eq_join_predicate.clone(),
            core.left.schema().len(),
        )
        .unwrap();
        Self::new(core, eq_join_predicate, desc).into()
    }
}

impl ExprVisitable for StreamAsOfJoin {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
