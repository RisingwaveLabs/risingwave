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

use std::collections::HashSet;
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::{stream, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::{FunctionalDependencySet, Order, OrderDisplay};
use crate::TableCatalog;
/// `TopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopN<PlanRef> {
    pub input: PlanRef,
    pub limit: u64,
    pub offset: u64,
    pub with_ties: bool,
    pub order: Order,
    pub group_key: Vec<usize>,
}

impl<PlanRef: stream::StreamPlanRef> TopN<PlanRef> {
    /// Infers the state table catalog for [`StreamTopN`] and [`StreamGroupTopN`].
    pub fn infer_internal_table_catalog(
        &self,
        me: &impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        let schema = me.schema();
        let pk_indices = me.logical_pk();
        let columns_fields = schema.fields().to_vec();
        let column_orders = &self.order.column_orders;
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        let mut order_cols = HashSet::new();

        // Here we want the state table to store the states in the order we want, firstly in
        // ascending order by the columns specified by the group key, then by the columns
        // specified by `order`. If we do that, when the later group topN operator
        // does a prefix scanning with the group key, we can fetch the data in the
        // desired order.
        self.group_key.iter().for_each(|&idx| {
            internal_table_catalog_builder.add_order_column(idx, OrderType::ascending());
            order_cols.insert(idx);
        });

        let read_prefix_len_hint = internal_table_catalog_builder.get_current_pk_len();
        column_orders.iter().for_each(|order| {
            if !order_cols.contains(&order.column_index) {
                internal_table_catalog_builder
                    .add_order_column(order.column_index, order.order_type);
                order_cols.insert(order.column_index);
            }
        });

        pk_indices.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        });
        if let Some(vnode_col_idx) = vnode_col_idx {
            internal_table_catalog_builder.set_vnode_col_idx(vnode_col_idx);
        }

        internal_table_catalog_builder.build(
            self.input.distribution().dist_column_indices().to_vec(),
            read_prefix_len_hint,
        )
    }
}

impl<PlanRef: GenericPlanRef> TopN<PlanRef> {
    pub fn without_group(
        input: PlanRef,
        limit: u64,
        offset: u64,
        with_ties: bool,
        order: Order,
    ) -> Self {
        Self {
            input,
            limit,
            offset,
            with_ties,
            order,
            group_key: vec![],
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        let input_schema = self.input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: &self.order,
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &self.limit)
            .field("offset", &self.offset);
        if self.with_ties {
            builder.field("with_ties", &true);
        }
        if !self.group_key.is_empty() {
            builder.field("group_key", &self.group_key);
        }
        builder.finish()
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for TopN<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}
