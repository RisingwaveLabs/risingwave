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

use std::hash::{Hash, Hasher};
use std::rc::Rc;

use iceberg::expr::Predicate as IcebergPredicate;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::DataType;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::IcebergScanNode;
use risingwave_sqlparser::ast::AsOf;

use super::batch::prelude::*;
use super::utils::{childless_record, column_names_pretty, Distill};
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, ToLocalBatch,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone)]
pub struct BatchIcebergScan {
    pub base: PlanBase<Batch>,
    pub core: generic::Source,
    iceberg_scan_type: IcebergScanType,
    pub predicate: IcebergPredicate,
}

impl PartialEq for BatchIcebergScan {
    fn eq(&self, other: &Self) -> bool {
        if self.predicate == IcebergPredicate::AlwaysTrue
            && other.predicate == IcebergPredicate::AlwaysTrue
        {
            self.base == other.base && self.core == other.core
        } else {
            panic!("BatchIcebergScan::eq: comparing non-AlwaysTrue predicates is not supported")
        }
    }
}

impl Eq for BatchIcebergScan {}

impl Hash for BatchIcebergScan {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.predicate != IcebergPredicate::AlwaysTrue {
            panic!("BatchIcebergScan::hash: hashing non-AlwaysTrue predicates is not supported")
        } else {
            self.base.hash(state);
            self.core.hash(state);
        }
    }
}

impl BatchIcebergScan {
    pub fn new(core: generic::Source, iceberg_scan_type: IcebergScanType) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            // Use `Single` by default, will be updated later with `clone_with_dist`.
            Distribution::Single,
            Order::any(),
        );

        Self {
            base,
            core,
            iceberg_scan_type,
            predicate: IcebergPredicate::AlwaysTrue,
        }
    }

    pub fn new_count_star_with_batch_iceberg_scan(batch_iceberg_scan: &BatchIcebergScan) -> Self {
        let mut core = batch_iceberg_scan.core.clone();
        core.column_catalog = vec![ColumnCatalog::visible(ColumnDesc::named(
            "count",
            ColumnId::first_user_column(),
            DataType::Int64,
        ))];
        let base = PlanBase::new_batch_with_core(
            &core,
            batch_iceberg_scan.base.distribution().clone(),
            batch_iceberg_scan.base.order().clone(),
        );
        Self {
            base,
            core,
            iceberg_scan_type: IcebergScanType::CountStar,
            predicate: IcebergPredicate::AlwaysTrue,
        }
    }

    pub fn iceberg_scan_type(&self) -> IcebergScanType {
        self.iceberg_scan_type
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema().names_str()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn clone_with_dist(&self) -> Self {
        let base = self
            .base
            .clone_with_new_distribution(Distribution::SomeShard);
        Self {
            base,
            core: self.core.clone(),
            iceberg_scan_type: self.iceberg_scan_type,
            predicate: self.predicate.clone(),
        }
    }

    pub fn clone_with_predicate(&self, predicate: IcebergPredicate) -> Self {
        Self {
            base: self.base.clone(),
            core: self.core.clone(),
            iceberg_scan_type: self.iceberg_scan_type,
            predicate,
        }
    }

    pub fn as_of(&self) -> Option<AsOf> {
        self.core.as_of.clone()
    }
}

impl_plan_tree_node_for_leaf! { BatchIcebergScan }

impl Distill for BatchIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().unwrap().name.clone());
        let fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
            ("iceberg_scan_type", Pretty::debug(&self.iceberg_scan_type)),
            ("predicate", Pretty::from(self.predicate.to_string())),
        ];
        childless_record("BatchIcebergScan", fields)
    }
}

impl ToLocalBatch for BatchIcebergScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToDistributedBatch for BatchIcebergScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchIcebergScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let source_catalog = self.source_catalog().unwrap();
        let (with_properties, secret_refs) = source_catalog.with_properties.clone().into_parts();
        NodeBody::IcebergScan(IcebergScanNode {
            columns: self
                .core
                .column_catalog
                .iter()
                .map(|c| c.to_protobuf())
                .collect(),
            with_properties,
            split: vec![],
            secret_refs,
            iceberg_scan_type: self.iceberg_scan_type as i32,
        })
    }
}

impl ExprRewritable for BatchIcebergScan {}

impl ExprVisitable for BatchIcebergScan {}
