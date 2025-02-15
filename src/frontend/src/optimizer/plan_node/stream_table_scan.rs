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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Field;
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{PbStreamNode, StreamScanType};

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::{ExprRewriter, ExprVisitor, FunctionCall};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::property::{Distribution, DistributionDisplay, MonotonicityMap};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to stream scan + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTableScan {
    pub base: PlanBase<Stream>,
    core: generic::TableScan,
    batch_plan_id: PlanNodeId,
    stream_scan_type: StreamScanType,
}

impl StreamTableScan {
    pub fn new_with_stream_scan_type(
        core: generic::TableScan,
        stream_scan_type: StreamScanType,
    ) -> Self {
        let batch_plan_id = core.ctx.next_plan_node_id();

        let distribution = {
            match core.distribution_key() {
                Some(distribution_key) => {
                    if distribution_key.is_empty() {
                        Distribution::Single
                    } else {
                        // See also `BatchSeqScan::clone_with_dist`.
                        Distribution::UpstreamHashShard(distribution_key, core.table_desc.table_id)
                    }
                }
                None => Distribution::SomeShard,
            }
        };

        let base = PlanBase::new_stream_with_core(
            &core,
            distribution,
            core.append_only(),
            false,
            core.watermark_columns(),
            MonotonicityMap::new(),
        );
        Self {
            base,
            core,
            batch_plan_id,
            stream_scan_type,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn core(&self) -> &generic::TableScan {
        &self.core
    }

    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_catalog: Arc<TableCatalog>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        function_mapping: &HashMap<FunctionCall, usize>,
        stream_scan_type: StreamScanType,
    ) -> StreamTableScan {
        let logical_index_scan = self.core.to_index_scan(
            index_name,
            index_table_catalog,
            primary_to_secondary_mapping,
            function_mapping,
        );
        logical_index_scan
            .distribution_key()
            .expect("distribution key of stream chain must exist in output columns");
        StreamTableScan::new_with_stream_scan_type(logical_index_scan, stream_scan_type)
    }

    pub fn stream_scan_type(&self) -> StreamScanType {
        self.stream_scan_type
    }

    // TODO: Add note to reviewer about safety, because of `generic::TableScan` limitation.
    fn get_upstream_state_table(&self) -> &TableCatalog {
        self.core.table_catalog.as_ref()
    }

    /// Build catalog for backfill state
    ///
    /// When `stream_scan_type` is not `StreamScanType::SnapshotBackfill`:
    ///
    /// Schema: | vnode | pk ... | `backfill_finished` | `row_count` |
    ///
    /// key:    | vnode |
    /// value:  | pk ... | `backfill_finished` | `row_count` |
    ///
    /// When we update the backfill progress,
    /// we update it for all vnodes.
    ///
    /// `pk` refers to the upstream pk which we use to track the backfill progress.
    ///
    /// `vnode` is the corresponding vnode of the upstream's distribution key.
    ///         It should also match the vnode of the backfill executor.
    ///
    /// `backfill_finished` is a boolean which just indicates if backfill is done.
    ///
    /// `row_count` is a count of rows which indicates the # of rows per executor.
    ///             We used to track this in memory.
    ///             But for backfill persistence we have to also persist it.
    ///
    /// FIXME(kwannoel):
    /// - Across all vnodes, the values are the same.
    /// - e.g. | vnode | pk ...  | `backfill_finished` | `row_count` |
    ///        | 1002 | Int64(1) | t                   | 10          |
    ///        | 1003 | Int64(1) | t                   | 10          |
    ///        | 1003 | Int64(1) | t                   | 10          |
    ///
    /// Eventually we should track progress per vnode, to support scaling with both mview and
    /// the corresponding `no_shuffle_backfill`.
    /// However this is not high priority, since we are working on supporting arrangement backfill,
    /// which already has this capability.
    ///
    ///
    /// When `stream_scan_type` is `StreamScanType::SnapshotBackfill`:
    ///
    /// Schema: | vnode | `epoch` | `row_count` | `is_epoch_finished` | pk ...
    ///
    /// key:    | vnode |
    /// value:  | `epoch` | `row_count` | `is_epoch_finished` | pk ...
    pub fn build_backfill_state_catalog(
        &self,
        state: &mut BuildFragmentGraphState,
        stream_scan_type: StreamScanType,
    ) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let upstream_schema = &self.core.get_table_columns();

        // We use vnode as primary key in state table.
        // If `Distribution::Single`, vnode will just be `VirtualNode::default()`.
        catalog_builder.add_column(&Field::with_name(VirtualNode::RW_TYPE, "vnode"));
        catalog_builder.add_order_column(0, OrderType::ascending());

        match stream_scan_type {
            StreamScanType::Chain
            | StreamScanType::Rearrange
            | StreamScanType::Backfill
            | StreamScanType::UpstreamOnly
            | StreamScanType::ArrangementBackfill => {
                // pk columns
                for col_order in self.core.primary_key() {
                    let col = &upstream_schema[col_order.column_index];
                    catalog_builder.add_column(&Field::from(col));
                }

                // `backfill_finished` column
                catalog_builder
                    .add_column(&Field::with_name(DataType::Boolean, "backfill_finished"));

                // `row_count` column
                catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));
            }
            StreamScanType::SnapshotBackfill | StreamScanType::CrossDbSnapshotBackfill => {
                // `epoch` column
                catalog_builder.add_column(&Field::with_name(DataType::Int64, "epoch"));

                // `row_count` column
                catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));

                // `is_finished` column
                catalog_builder
                    .add_column(&Field::with_name(DataType::Boolean, "is_epoch_finished"));

                // pk columns
                for col_order in self.core.primary_key() {
                    let col = &upstream_schema[col_order.column_index];
                    catalog_builder.add_column(&Field::from(col));
                }
            }
            StreamScanType::Unspecified => {
                unreachable!()
            }
        }

        // Reuse the state store pk (vnode) as the vnode as well.
        catalog_builder.set_vnode_col_idx(0);
        catalog_builder.set_dist_key_in_pk(vec![0]);

        let num_of_columns = catalog_builder.columns().len();
        catalog_builder.set_value_indices((1..num_of_columns).collect_vec());

        catalog_builder
            .build(vec![0], 1)
            .with_id(state.gen_table_id_wrapped())
    }
}

impl_plan_tree_node_for_leaf! { StreamTableScan }

impl Distill for StreamTableScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if verbose {
            vec.push(("stream_scan_type", Pretty::debug(&self.stream_scan_type)));
            let stream_key = IndicesDisplay {
                indices: self.stream_key().unwrap_or_default(),
                schema: self.base.schema(),
            };
            vec.push(("stream_key", stream_key.distill()));
            let pk = IndicesDisplay {
                indices: &self
                    .core
                    .primary_key()
                    .iter()
                    .map(|x| x.column_index)
                    .collect_vec(),
                schema: &self.core.table_catalog.column_schema(),
            };
            vec.push(("pk", pk.distill()));
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("dist", dist));
        }

        childless_record("StreamTableScan", vec)
    }
}

impl StreamNode for StreamTableScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamTableScan {
    pub fn adhoc_to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamNode> {
        use risingwave_pb::stream_plan::*;

        let stream_key = self
            .stream_key()
            .unwrap_or(&[])
            .iter()
            .map(|x| *x as u32)
            .collect_vec();

        // The required columns from the table (both scan and upstream).
        let upstream_column_ids = match self.stream_scan_type {
            // For backfill, we additionally need the primary key columns.
            StreamScanType::Backfill
            | StreamScanType::ArrangementBackfill
            | StreamScanType::SnapshotBackfill
            | StreamScanType::CrossDbSnapshotBackfill => self.core.output_and_pk_column_ids(),
            StreamScanType::Chain | StreamScanType::Rearrange | StreamScanType::UpstreamOnly => {
                self.core.output_column_ids()
            }
            StreamScanType::Unspecified => unreachable!(),
        }
        .iter()
        .map(ColumnId::get_id)
        .collect_vec();

        // The schema of the snapshot read stream
        let snapshot_schema = upstream_column_ids
            .iter()
            .map(|&id| {
                let col = self
                    .core
                    .get_table_columns()
                    .iter()
                    .find(|c| c.column_id.get_id() == id)
                    .unwrap();
                Field::from(col).to_prost()
            })
            .collect_vec();

        let upstream_schema = snapshot_schema.clone();

        // TODO: snapshot read of upstream mview
        let batch_plan_node = BatchPlanNode {
            table_desc: Some(self.core.table_desc.try_to_protobuf()?),
            column_ids: upstream_column_ids.clone(),
        };

        let catalog = self
            .build_backfill_state_catalog(state, self.stream_scan_type)
            .to_internal_table_prost();

        // For backfill, we first read pk + output_indices from upstream.
        // On this, we need to further project `output_indices` to the downstream.
        // This `output_indices` refers to that.
        let output_indices = self
            .core
            .output_column_ids()
            .iter()
            .map(|i| {
                upstream_column_ids
                    .iter()
                    .position(|&x| x == i.get_id())
                    .unwrap() as u32
            })
            .collect_vec();

        let arrangement_table = if self.stream_scan_type == StreamScanType::ArrangementBackfill {
            let upstream_table_catalog = self.get_upstream_state_table();
            Some(upstream_table_catalog.to_internal_table_prost())
        } else {
            None
        };

        let input = if self.stream_scan_type == StreamScanType::CrossDbSnapshotBackfill {
            vec![]
        } else {
            vec![
                // Upstream updates
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields: upstream_schema.clone(),
                    stream_key: vec![], // not used
                    ..Default::default()
                },
                // Snapshot read
                PbStreamNode {
                    node_body: Some(PbNodeBody::BatchPlan(Box::new(batch_plan_node))),
                    operator_id: self.batch_plan_id.0 as u64,
                    identity: "BatchPlanNode".into(),
                    fields: snapshot_schema,
                    stream_key: vec![], // not used
                    input: vec![],
                    append_only: true,
                },
            ]
        };

        let node_body = PbNodeBody::StreamScan(Box::new(StreamScanNode {
            table_id: self.core.table_desc.table_id.table_id,
            stream_scan_type: self.stream_scan_type as i32,
            // The column indices need to be forwarded to the downstream
            output_indices,
            upstream_column_ids,
            // The table desc used by backfill executor
            table_desc: Some(self.core.table_desc.try_to_protobuf()?),
            state_table: Some(catalog),
            arrangement_table,
            rate_limit: self.base.ctx().overwrite_options().backfill_rate_limit,
            ..Default::default()
        }));

        Ok(PbStreamNode {
            fields: self.schema().to_prost(),
            input,
            node_body: Some(node_body),
            stream_key,
            operator_id: self.base.id().0 as u64,
            identity: self.distill_to_string(),
            append_only: self.append_only(),
        })
    }
}

impl ExprRewritable for StreamTableScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new_with_stream_scan_type(core, self.stream_scan_type).into()
    }
}

impl ExprVisitable for StreamTableScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
