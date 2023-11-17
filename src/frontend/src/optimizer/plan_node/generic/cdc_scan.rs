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

use std::rc::Rc;

use educe::Educe;
use fixedbitset::FixedBitSet;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, Field, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::ColumnOrder;

use super::GenericPlanNode;
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::FunctionalDependencySet;

/// [`CdcScan`] reads rows of a table from an external upstream database
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct CdcScan {
    pub table_name: String,
    /// Include `output_col_idx` and columns required in `predicate`
    pub output_col_idx: Vec<usize>,
    /// Descriptor of the external table for CDC
    pub cdc_table_desc: Rc<CdcTableDesc>,
    #[educe(PartialEq(ignore))]
    #[educe(Hash(ignore))]
    pub ctx: OptimizerContextRef,
}

impl CdcScan {
    pub fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) {}

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx
            .iter()
            .map(|i| self.get_table_columns()[*i].column_id)
            .collect()
    }

    pub fn primary_key(&self) -> &[ColumnOrder] {
        &self.cdc_table_desc.pk
    }

    pub fn watermark_columns(&self) -> FixedBitSet {
        FixedBitSet::with_capacity(self.get_table_columns().len())
    }

    pub(crate) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| format!("{}.{}", self.table_name, self.get_table_columns()[i].name))
            .collect()
    }

    pub(crate) fn column_names(&self) -> Vec<String> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].name.clone())
            .collect()
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            &self.output_col_idx,
            self.get_table_columns().len(),
        )
    }

    /// Get the ids of the output columns and primary key columns.
    pub fn output_and_pk_column_ids(&self) -> Vec<ColumnId> {
        let mut ids = self.output_column_ids();
        for column_order in self.primary_key() {
            let id = self.get_table_columns()[column_order.column_index].column_id;
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        ids
    }

    /// Create a logical scan node for CDC backfill
    pub(crate) fn new(
        table_name: String,
        output_col_idx: Vec<usize>, // the column index in the table
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
    ) -> Self {
        Self {
            table_name,
            output_col_idx,
            cdc_table_desc,
            ctx,
        }
    }

    pub(crate) fn columns_pretty<'a>(&self, verbose: bool) -> Pretty<'a> {
        Pretty::Array(
            match verbose {
                true => self.column_names_with_table_prefix(),
                false => self.column_names(),
            }
            .into_iter()
            .map(Pretty::from)
            .collect(),
        )
    }
}

// TODO: extend for cdc table
impl GenericPlanNode for CdcScan {
    fn schema(&self) -> Schema {
        let fields = self
            .output_col_idx
            .iter()
            .map(|tb_idx| {
                let col = &self.get_table_columns()[*tb_idx];
                Field::from_with_table_name_prefix(col, &self.table_name)
            })
            .collect();
        Schema { fields }
    }

    fn stream_key(&self) -> Option<Vec<usize>> {
        Some(self.cdc_table_desc.stream_key.clone())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.ctx.clone()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        let pk_indices = self.stream_key();
        let col_num = self.output_col_idx.len();
        match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(col_num, pk_indices),
            None => FunctionalDependencySet::new(col_num),
        }
    }
}

impl CdcScan {
    pub fn get_table_columns(&self) -> &[ColumnDesc] {
        &self.cdc_table_desc.columns
    }

    pub fn append_only(&self) -> bool {
        false
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.get_table_columns()[i].clone())
            .collect()
    }
}
