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
use risingwave_common::util::scan_range::ScanRange;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::LogRowSeqScanNode;
use risingwave_pb::common::{BatchQueryCommittedEpoch, BatchQueryEpoch};

use super::batch::prelude::*;
use super::utils::{childless_record, scan_ranges_as_strs, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, ToDistributedBatch, TryToBatchPb};
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, DistributionDisplay, Order};
use crate::scheduler::SchedulerResult;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchLogSeqScan {
    pub base: PlanBase<Batch>,
    core: generic::LogScan,
    scan_ranges: Vec<ScanRange>,
}

impl BatchLogSeqScan {
    fn new_inner(core: generic::LogScan, dist: Distribution, scan_ranges: Vec<ScanRange>) -> Self {
        let order = if scan_ranges.len() > 1 {
            Order::any()
        } else {
            core.get_out_column_index_order()
        };
        let base = PlanBase::new_batch(core.ctx(), core.schema(), dist, order);

        Self {
            base,
            core,
            scan_ranges,
        }
    }

    pub fn new(core: generic::LogScan, scan_ranges: Vec<ScanRange>) -> Self {
        // Use `Single` by default, will be updated later with `clone_with_dist`.
        Self::new_inner(core, Distribution::Single, scan_ranges)
    }

    fn clone_with_dist(&self) -> Self {
        Self::new_inner(
            self.core.clone(),
            match self.core.distribution_key() {
                None => Distribution::SomeShard,
                Some(distribution_key) => {
                    if distribution_key.is_empty() {
                        Distribution::Single
                    } else {
                        Distribution::UpstreamHashShard(
                            distribution_key,
                            self.core.table_desc.table_id,
                        )
                    }
                }
            },
            self.scan_ranges.clone(),
        )
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn core(&self) -> &generic::LogScan {
        &self.core
    }
}

impl_plan_tree_node_for_leaf! { BatchLogSeqScan }

impl Distill for BatchLogSeqScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(3);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if verbose {
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("distribution", dist));
        }
        vec.push(("old_epoch", Pretty::from(self.core.old_epoch.to_string())));
        vec.push(("new_epoch", Pretty::from(self.core.new_epoch.to_string())));
        vec.push(("version_id", Pretty::from(self.core.version_id.to_string())));
        if !self.scan_ranges.is_empty() {
            let order_names = match verbose {
                true => self.core.order_names_with_table_prefix(),
                false => self.core.order_names(),
            };
            let range_strs = scan_ranges_as_strs(order_names, &self.scan_ranges);
            vec.push((
                "scan_ranges",
                Pretty::Array(range_strs.into_iter().map(Pretty::from).collect()),
            ));
        }

        childless_record("BatchLogSeqScan", vec)
    }
}

impl ToDistributedBatch for BatchLogSeqScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl TryToBatchPb for BatchLogSeqScan {
    fn try_to_batch_prost_body(&self) -> SchedulerResult<NodeBody> {
        Ok(NodeBody::LogRowSeqScan(LogRowSeqScanNode {
            table_desc: Some(self.core.table_desc.try_to_protobuf()?),
            column_ids: self
                .core
                .output_column_ids()
                .iter()
                .map(ColumnId::get_id)
                .collect(),
            vnode_bitmap: None,
            old_epoch: Some(BatchQueryEpoch {
                epoch: Some(risingwave_pb::common::batch_query_epoch::Epoch::Committed(
                    BatchQueryCommittedEpoch {
                        epoch: self.core.old_epoch,
                        hummock_version_id: 0,
                    },
                )),
            }),
            new_epoch: Some(BatchQueryEpoch {
                epoch: Some(risingwave_pb::common::batch_query_epoch::Epoch::Committed(
                    BatchQueryCommittedEpoch {
                        epoch: self.core.new_epoch,
                        hummock_version_id: 0,
                    },
                )),
            }),
            // It's currently true.
            ordered: !self.order().is_any(),
            scan_ranges: self.scan_ranges.iter().map(|r| r.to_protobuf()).collect(),
        }))
    }
}

impl ToLocalBatch for BatchLogSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        let dist = if let Some(distribution_key) = self.core.distribution_key()
            && !distribution_key.is_empty()
        {
            Distribution::UpstreamHashShard(distribution_key, self.core.table_desc.table_id)
        } else {
            Distribution::SomeShard
        };
        Ok(Self::new_inner(self.core.clone(), dist, self.scan_ranges.clone()).into())
    }
}

impl ExprRewritable for BatchLogSeqScan {}

impl ExprVisitable for BatchLogSeqScan {}
