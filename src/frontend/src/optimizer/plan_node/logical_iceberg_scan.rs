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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    iceberg_scan_type: IcebergScanType,
}

impl LogicalIcebergScan {
    pub fn new(logical_source: &LogicalSource, iceberg_scan_type: IcebergScanType) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let core = logical_source.core.clone();
        let base = PlanBase::new_logical_with_core(&core);

        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergScan {
            base,
            core,
            iceberg_scan_type,
        }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());
        let mut core = self.core.clone();
        let mut has_row_id = false;
        core.column_catalog = required_cols
            .iter()
            .map(|idx| {
                if Some(*idx) == core.row_id_index {
                    has_row_id = true;
                }
                core.column_catalog[*idx].clone()
            })
            .collect();
        if !has_row_id {
            core.row_id_index = None;
        }
        let base = PlanBase::new_logical_with_core(&core);

        LogicalIcebergScan {
            base,
            core,
            iceberg_scan_type: self.iceberg_scan_type,
        }
    }
}

impl_plan_tree_node_for_leaf! {LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
                ("iceberg_scan_type", Pretty::debug(&self.iceberg_scan_type)),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalIcebergScan", fields)
    }
}

impl ColPrunable for LogicalIcebergScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        if required_cols.is_empty() {
            let mapping =
                ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
            // If reuqiured_cols is empty, we use the first column of iceberg to avoid the empty schema.
            LogicalProject::with_mapping(self.clone_with_required_cols(&[0]).into(), mapping).into()
        } else {
            self.clone_with_required_cols(required_cols).into()
        }
    }
}

impl ExprRewritable for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<PlanRef> {
        let plan: PlanRef = BatchIcebergScan::new(self.core.clone(), self.iceberg_scan_type).into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
