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

use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableDesc;
use risingwave_common::types::{ParallelUnitId, VnodeMapping};
use risingwave_common::util::scan_range::ScanRange;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{ExchangeInfo, ScanRange as ScanRangeProto};
use risingwave_pb::common::Buffer;
use risingwave_pb::plan_common::Field as FieldProst;
use serde::ser::SerializeStruct;
use serde::Serialize;
use uuid::Uuid;

use crate::catalog::catalog_service::CatalogReader;
use crate::optimizer::plan_node::{PlanNodeId, PlanNodeType};
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;
use crate::scheduler::worker_node_manager::WorkerNodeManagerRef;
use crate::scheduler::SchedulerResult;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct QueryId {
    pub id: String,
}

impl std::fmt::Display for QueryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryId:{}", self.id)
    }
}

pub type StageId = u32;

// Root stage always has only one task.
pub const ROOT_TASK_ID: u32 = 0;
// Root task has only one output.
pub const ROOT_TASK_OUTPUT_ID: u32 = 0;
pub type TaskId = u32;

/// Generated by [`BatchPlanFragmenter`] and used in query execution graph.
#[derive(Clone, Debug)]
pub struct ExecutionPlanNode {
    pub plan_node_id: PlanNodeId,
    pub plan_node_type: PlanNodeType,
    pub node: NodeBody,
    pub schema: Vec<FieldProst>,

    pub children: Vec<Arc<ExecutionPlanNode>>,

    /// The stage id of the source of `BatchExchange`.
    /// Used to find `ExchangeSource` from scheduler when creating `PlanNode`.
    ///
    /// `None` when this node is not `BatchExchange`.
    pub source_stage_id: Option<StageId>,
}

impl Serialize for ExecutionPlanNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("QueryStage", 5)?;
        state.serialize_field("plan_node_id", &self.plan_node_id)?;
        state.serialize_field("plan_node_type", &self.plan_node_type)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("children", &self.children)?;
        state.serialize_field("source_stage_id", &self.source_stage_id)?;
        state.end()
    }
}

impl From<PlanRef> for ExecutionPlanNode {
    fn from(plan_node: PlanRef) -> Self {
        Self {
            plan_node_id: plan_node.plan_base().id,
            plan_node_type: plan_node.node_type(),
            node: plan_node.to_batch_prost_body(),
            children: vec![],
            schema: plan_node.schema().to_prost(),
            source_stage_id: None,
        }
    }
}

impl ExecutionPlanNode {
    pub fn node_type(&self) -> PlanNodeType {
        self.plan_node_type
    }
}

/// `BatchPlanFragmenter` splits a query plan into fragments.
pub struct BatchPlanFragmenter {
    query_id: QueryId,
    stage_graph_builder: StageGraphBuilder,
    next_stage_id: u32,
    worker_node_manager: WorkerNodeManagerRef,
    catalog_reader: CatalogReader,
}

impl Default for QueryId {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
        }
    }
}

impl BatchPlanFragmenter {
    pub fn new(worker_node_manager: WorkerNodeManagerRef, catalog_reader: CatalogReader) -> Self {
        Self {
            query_id: Default::default(),
            stage_graph_builder: StageGraphBuilder::new(),
            next_stage_id: 0,
            worker_node_manager,
            catalog_reader,
        }
    }
}

/// The fragmented query generated by [`BatchPlanFragmenter`].
#[derive(Debug)]
pub struct Query {
    /// Query id should always be unique.
    pub query_id: QueryId,
    pub stage_graph: StageGraph,
}

impl Query {
    pub fn leaf_stages(&self) -> Vec<StageId> {
        let mut ret_leaf_stages = Vec::new();
        for stage_id in self.stage_graph.stages.keys() {
            if self
                .stage_graph
                .get_child_stages_unchecked(stage_id)
                .is_empty()
            {
                ret_leaf_stages.push(*stage_id);
            }
        }
        ret_leaf_stages
    }

    pub fn get_parents(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.stage_graph.parent_edges.get(stage_id).unwrap()
    }

    pub fn root_stage_id(&self) -> StageId {
        self.stage_graph.root_stage_id
    }

    pub fn query_id(&self) -> &QueryId {
        &self.query_id
    }

    pub fn stages_with_table_scan(&self) -> HashSet<StageId> {
        self.stage_graph
            .stages
            .iter()
            .filter_map(|(stage_id, stage_query)| {
                if stage_query.has_table_scan() {
                    Some(*stage_id)
                } else {
                    None
                }
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct TableScanInfo {
    /// Indicates the table partitions to be read by scan tasks. Unnecessary partitions are already
    /// pruned.
    ///
    /// `None` if the table is not partitioned (system table).
    pub partitions: Option<HashMap<ParallelUnitId, PartitionInfo>>,
}

#[derive(Clone, Debug)]
pub struct PartitionInfo {
    pub vnode_bitmap: Buffer,
    pub scan_ranges: Vec<ScanRangeProto>,
}

/// Fragment part of `Query`.
pub struct QueryStage {
    pub query_id: QueryId,
    pub id: StageId,
    pub root: Arc<ExecutionPlanNode>,
    pub exchange_info: ExchangeInfo,
    pub parallelism: u32,
    /// Indicates whether this stage contains a table scan node and the table's information if so.
    pub table_scan_info: Option<TableScanInfo>,
}

impl QueryStage {
    /// If true, this stage contains table scan executor that creates
    /// Hummock iterators to read data from table. The iterator is initialized during
    /// the executor building process on the batch execution engine.
    pub fn has_table_scan(&self) -> bool {
        self.table_scan_info.is_some()
    }
}

impl Debug for QueryStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryStage")
            .field("id", &self.id)
            .field("parallelism", &self.parallelism)
            .field("exchange_info", &self.exchange_info)
            .field("has_table_scan", &self.has_table_scan())
            .finish()
    }
}

impl Serialize for QueryStage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("QueryStage", 3)?;
        state.serialize_field("root", &self.root)?;
        state.serialize_field("parallelism", &self.parallelism)?;
        state.serialize_field("exchange_info", &self.exchange_info)?;
        state.end()
    }
}

pub type QueryStageRef = Arc<QueryStage>;

struct QueryStageBuilder {
    query_id: QueryId,
    id: StageId,
    root: Option<Arc<ExecutionPlanNode>>,
    parallelism: u32,
    exchange_info: ExchangeInfo,

    children_stages: Vec<QueryStageRef>,
    /// See also [`QueryStage::table_scan_info`].
    table_scan_info: Option<TableScanInfo>,
}

impl QueryStageBuilder {
    fn new(
        id: StageId,
        query_id: QueryId,
        parallelism: u32,
        exchange_info: ExchangeInfo,
        table_scan_info: Option<TableScanInfo>,
    ) -> Self {
        Self {
            query_id,
            id,
            root: None,
            parallelism,
            exchange_info,
            children_stages: vec![],
            table_scan_info,
        }
    }

    fn finish(self, stage_graph_builder: &mut StageGraphBuilder) -> QueryStageRef {
        let stage = Arc::new(QueryStage {
            query_id: self.query_id,
            id: self.id,
            root: self.root.unwrap(),
            exchange_info: self.exchange_info,
            parallelism: self.parallelism,
            table_scan_info: self.table_scan_info,
        });

        stage_graph_builder.add_node(stage.clone());
        for child_stage in self.children_stages {
            stage_graph_builder.link_to_child(self.id, child_stage.id);
        }
        stage
    }
}

/// Maintains how each stage are connected.
#[derive(Debug, Serialize)]
pub struct StageGraph {
    pub root_stage_id: StageId,
    pub stages: HashMap<StageId, QueryStageRef>,
    /// Traverse from top to down. Used in split plan into stages.
    child_edges: HashMap<StageId, HashSet<StageId>>,
    /// Traverse from down to top. Used in schedule each stage.
    parent_edges: HashMap<StageId, HashSet<StageId>>,
}

impl StageGraph {
    pub fn get_child_stages_unchecked(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.child_edges.get(stage_id).unwrap()
    }

    pub fn get_child_stages(&self, stage_id: &StageId) -> Option<&HashSet<StageId>> {
        self.child_edges.get(stage_id)
    }

    /// Returns stage ids in topology order, s.t. child stage always appears before its parent.
    pub fn stage_ids_by_topo_order(&self) -> impl Iterator<Item = StageId> {
        let mut stack = Vec::with_capacity(self.stages.len());
        stack.push(self.root_stage_id);
        let mut ret = Vec::with_capacity(self.stages.len());
        let mut existing = HashSet::with_capacity(self.stages.len());

        while let Some(s) = stack.pop() {
            if !existing.contains(&s) {
                ret.push(s);
                existing.insert(s);
                stack.extend(&self.child_edges[&s]);
            }
        }

        ret.into_iter().rev()
    }
}

struct StageGraphBuilder {
    stages: HashMap<StageId, QueryStageRef>,
    child_edges: HashMap<StageId, HashSet<StageId>>,
    parent_edges: HashMap<StageId, HashSet<StageId>>,
}

impl StageGraphBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            child_edges: HashMap::new(),
            parent_edges: HashMap::new(),
        }
    }

    pub fn build(self, root_stage_id: StageId) -> StageGraph {
        StageGraph {
            root_stage_id,
            stages: self.stages,
            child_edges: self.child_edges,
            parent_edges: self.parent_edges,
        }
    }

    /// Link parent stage and child stage. Maintain the mappings of parent -> child and child ->
    /// parent.
    pub fn link_to_child(&mut self, parent_id: StageId, child_id: StageId) {
        self.child_edges
            .get_mut(&parent_id)
            .unwrap()
            .insert(child_id);
        self.parent_edges
            .get_mut(&child_id)
            .unwrap()
            .insert(parent_id);
    }

    pub fn add_node(&mut self, stage: QueryStageRef) {
        // Insert here so that left/root stages also has linkage.
        self.child_edges.insert(stage.id, HashSet::new());
        self.parent_edges.insert(stage.id, HashSet::new());
        self.stages.insert(stage.id, stage);
    }
}

impl BatchPlanFragmenter {
    /// Split the plan node into each stages, based on exchange node.
    pub fn split(mut self, batch_node: PlanRef) -> SchedulerResult<Query> {
        let root_stage = self.new_stage(batch_node.clone(), Distribution::Single.to_prost(1));
        let stage_graph = self.stage_graph_builder.build(root_stage.id);
        Ok(Query {
            stage_graph,
            query_id: self.query_id,
        })
    }

    fn new_stage(&mut self, root: PlanRef, exchange_info: ExchangeInfo) -> QueryStageRef {
        let next_stage_id = self.next_stage_id;
        self.next_stage_id += 1;

        let table_scan_info = self.collect_stage_table_scan(root.clone());
        let parallelism = match root.distribution() {
            Distribution::Single => {
                assert!(
                    table_scan_info.is_none()
                        || table_scan_info.as_ref().unwrap().partitions.is_none(),
                    "The stage has single distribution, but contains a partitioned table scan
                node.\nplan: {:#?}",
                    root
                );
                1
            }
            _ => match &table_scan_info {
                None => self.worker_node_manager.worker_node_count(),
                Some(info) => info.partitions.as_ref().map(|m| m.len()).unwrap_or(1),
            },
        };

        let mut builder = QueryStageBuilder::new(
            next_stage_id,
            self.query_id.clone(),
            parallelism as u32,
            exchange_info,
            table_scan_info,
        );

        self.visit_node(root, &mut builder, None);

        builder.finish(&mut self.stage_graph_builder)
    }

    fn visit_node(
        &mut self,
        node: PlanRef,
        builder: &mut QueryStageBuilder,
        parent_exec_node: Option<&mut ExecutionPlanNode>,
    ) {
        match node.node_type() {
            PlanNodeType::BatchExchange => {
                self.visit_exchange(node.clone(), builder, parent_exec_node);
            }
            _ => {
                let mut execution_plan_node = ExecutionPlanNode::from(node.clone());

                for child in node.inputs() {
                    self.visit_node(child, builder, Some(&mut execution_plan_node));
                }

                if let Some(parent) = parent_exec_node {
                    parent.children.push(Arc::new(execution_plan_node));
                } else {
                    builder.root = Some(Arc::new(execution_plan_node));
                }
            }
        }
    }

    fn visit_exchange(
        &mut self,
        node: PlanRef,
        builder: &mut QueryStageBuilder,
        parent_exec_node: Option<&mut ExecutionPlanNode>,
    ) {
        let mut execution_plan_node = ExecutionPlanNode::from(node.clone());
        let child_exchange_info = node.distribution().to_prost(builder.parallelism);
        let child_stage = self.new_stage(node.inputs()[0].clone(), child_exchange_info);
        execution_plan_node.source_stage_id = Some(child_stage.id);

        if let Some(parent) = parent_exec_node {
            parent.children.push(Arc::new(execution_plan_node));
        } else {
            builder.root = Some(Arc::new(execution_plan_node));
        }

        builder.children_stages.push(child_stage);
    }

    /// Check whether this stage contains a table scan node and the table's information if so.
    ///
    /// If there are multiple scan nodes in this stage, they must have the same distribution, but
    /// maybe different vnodes partition. We just use the same partition for all
    /// the scan nodes.
    fn collect_stage_table_scan(&self, node: PlanRef) -> Option<TableScanInfo> {
        if node.node_type() == PlanNodeType::BatchExchange {
            // Do not visit next stage.
            return None;
        }

        if let Some(scan_node) = node.as_batch_seq_scan() {
            Some({
                let table_desc = scan_node.logical().table_desc();
                let partitions = self
                    .catalog_reader
                    .read_guard()
                    .get_table_by_id(&table_desc.table_id)
                    .map(|table| {
                        self.worker_node_manager
                            .get_fragment_mapping(&table.fragment_id)
                            .map(|vnode_mapping| {
                                derive_partitions(
                                    scan_node.scan_ranges(),
                                    table_desc,
                                    &vnode_mapping,
                                )
                            })
                    })
                    .ok()
                    .flatten();
                TableScanInfo { partitions }
            })
        } else {
            node.inputs()
                .into_iter()
                .map(|n| self.collect_stage_table_scan(n))
                .find_map(|o| o)
        }
    }
}

// TODO: let frontend store owner_mapping directly?
fn vnode_mapping_to_owner_mapping(vnode_mapping: VnodeMapping) -> HashMap<ParallelUnitId, Bitmap> {
    let mut m: HashMap<ParallelUnitId, BitmapBuilder> = HashMap::new();
    let num_vnodes = vnode_mapping.len();
    for (i, parallel_unit_id) in vnode_mapping.into_iter().enumerate() {
        let bitmap = m
            .entry(parallel_unit_id)
            .or_insert_with(|| BitmapBuilder::zeroed(num_vnodes));
        bitmap.set(i, true);
    }
    m.into_iter().map(|(k, v)| (k, v.finish())).collect()
}

fn bitmap_with_single_vnode(vnode: usize, num_vnodes: usize) -> Bitmap {
    let mut bitmap = BitmapBuilder::zeroed(num_vnodes);
    bitmap.set(vnode as usize, true);
    bitmap.finish()
}

/// Try to derive the partition to read from the scan range.
/// It can be derived if the value of the distribution key is already
/// known.
fn derive_partitions(
    scan_ranges: &[ScanRange],
    table_desc: &TableDesc,
    vnode_mapping: &Vec<u32>,
) -> HashMap<ParallelUnitId, PartitionInfo> {
    let all_partitions = || {
        vnode_mapping_to_owner_mapping(vnode_mapping.clone())
            .into_iter()
            .map(|(k, vnode_bitmap)| {
                (
                    k,
                    PartitionInfo {
                        vnode_bitmap: vnode_bitmap.to_protobuf(),
                        scan_ranges: scan_ranges.iter().map(|r| r.to_protobuf()).collect(),
                    },
                )
            })
            .collect()
    };

    let num_vnodes = vnode_mapping.len();
    let mut partitions: HashMap<u32, (BitmapBuilder, Vec<_>)> = HashMap::new();

    if scan_ranges.is_empty() {
        return all_partitions();
    }

    for scan_range in scan_ranges {
        let vnode = scan_range.try_compute_vnode(
            &table_desc.distribution_key,
            &table_desc.order_column_indices(),
        );
        match vnode {
            None => {
                // put this scan_range to all partitions
                vnode_mapping_to_owner_mapping(vnode_mapping.clone())
                    .into_iter()
                    .for_each(|(parallel_unit_id, vnode_bitmap)| {
                        let (bitmap, scan_ranges) = partitions
                            .entry(parallel_unit_id)
                            .or_insert_with(|| (BitmapBuilder::zeroed(num_vnodes), vec![]));
                        vnode_bitmap
                            .iter()
                            .enumerate()
                            .for_each(|(vnode, b)| bitmap.set(vnode, b));
                        scan_ranges.push(scan_range.to_protobuf());
                    });
            }
            // scan a single partition
            Some(vnode) => {
                let parallel_unit_id = vnode_mapping[vnode as usize];
                let (bitmap, scan_ranges) = partitions
                    .entry(parallel_unit_id)
                    .or_insert_with(|| (BitmapBuilder::zeroed(num_vnodes), vec![]));
                bitmap.set(vnode as usize, true);
                scan_ranges.push(scan_range.to_protobuf());
            }
        }
    }

    partitions
        .into_iter()
        .map(|(k, (bitmap, scan_ranges))| {
            (
                k,
                PartitionInfo {
                    vnode_bitmap: bitmap.finish().to_protobuf(),
                    scan_ranges,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::rc::Rc;
    use std::sync::Arc;

    use parking_lot::RwLock;
    use risingwave_common::catalog::{ColumnDesc, TableDesc};
    use risingwave_common::config::constant::hummock::TABLE_OPTION_DUMMY_RETENTION_SECOND;
    use risingwave_common::types::DataType;
    use risingwave_pb::batch_plan::plan_node::NodeBody;
    use risingwave_pb::common::{HostAddress, ParallelUnit, WorkerNode, WorkerType};
    use risingwave_pb::plan_common::JoinType;

    use crate::catalog::catalog_service::CatalogReader;
    use crate::catalog::root_catalog::Catalog;
    use crate::expr::InputRef;
    use crate::optimizer::plan_node::{
        BatchExchange, BatchFilter, BatchHashJoin, EqJoinPredicate, LogicalFilter, LogicalJoin,
        LogicalScan, PlanNodeType, ToBatch,
    };
    use crate::optimizer::property::{Distribution, Order};
    use crate::optimizer::PlanRef;
    use crate::scheduler::plan_fragmenter::{BatchPlanFragmenter, StageId};
    use crate::scheduler::worker_node_manager::WorkerNodeManager;
    use crate::session::OptimizerContext;
    use crate::utils::Condition;

    #[tokio::test]
    async fn test_fragmenter() {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Filter
        //          |
        //         Scan
        let ctx = OptimizerContext::mock().await;

        let batch_plan_node: PlanRef = LogicalScan::create(
            "".to_string(),
            false,
            Rc::new(TableDesc {
                table_id: 0.into(),
                stream_key: vec![],
                order_key: vec![],
                columns: vec![
                    ColumnDesc {
                        data_type: DataType::Int32,
                        column_id: 0.into(),
                        name: "a".to_string(),
                        type_name: String::new(),
                        field_descs: vec![],
                    },
                    ColumnDesc {
                        data_type: DataType::Float64,
                        column_id: 1.into(),
                        name: "b".to_string(),
                        type_name: String::new(),
                        field_descs: vec![],
                    },
                ],
                distribution_key: vec![],
                appendonly: false,
                retention_seconds: TABLE_OPTION_DUMMY_RETENTION_SECOND,
            }),
            vec![],
            ctx,
        )
        .to_batch()
        .unwrap()
        .to_distributed()
        .unwrap();
        let batch_filter = BatchFilter::new(LogicalFilter::new(
            batch_plan_node.clone(),
            Condition {
                conjunctions: vec![],
            },
        ))
        .into();
        let batch_exchange_node1: PlanRef = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let batch_exchange_node2: PlanRef = BatchExchange::new(
            batch_filter,
            Order::default(),
            Distribution::HashShard(vec![0, 1]),
        )
        .into();
        let hash_join_node: PlanRef = BatchHashJoin::new(
            LogicalJoin::new(
                batch_exchange_node1.clone(),
                batch_exchange_node2.clone(),
                JoinType::Inner,
                Condition::true_cond(),
            ),
            EqJoinPredicate::new(
                Condition::true_cond(),
                vec![
                    (
                        InputRef {
                            index: 0,
                            data_type: DataType::Int32,
                        },
                        InputRef {
                            index: 2,
                            data_type: DataType::Int32,
                        },
                        false,
                    ),
                    (
                        InputRef {
                            index: 1,
                            data_type: DataType::Float64,
                        },
                        InputRef {
                            index: 3,
                            data_type: DataType::Float64,
                        },
                        false,
                    ),
                ],
                2,
            ),
        )
        .into();
        let batch_exchange_node3: PlanRef = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into();

        let worker1 = WorkerNode {
            id: 0,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5687,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(0, 0),
        };
        let worker2 = WorkerNode {
            id: 1,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5688,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(8, 1),
        };
        let worker3 = WorkerNode {
            id: 2,
            r#type: WorkerType::ComputeNode as i32,
            host: Some(HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5689,
            }),
            state: risingwave_pb::common::worker_node::State::Running as i32,
            parallel_units: generate_parallel_units(16, 2),
        };
        let workers = vec![worker1, worker2, worker3];
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(workers));
        let catalog_reader = CatalogReader::new(Arc::new(RwLock::new(Catalog::default())));
        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new(worker_node_manager, catalog_reader);
        let query = fragmenter.split(batch_exchange_node3.clone()).unwrap();

        assert_eq!(query.stage_graph.root_stage_id, 0);
        assert_eq!(query.stage_graph.stages.len(), 4);

        // Check the mappings of child edges.
        assert_eq!(query.stage_graph.child_edges[&0], [1].into());
        assert_eq!(query.stage_graph.child_edges[&1], [2, 3].into());
        assert_eq!(query.stage_graph.child_edges[&2], HashSet::new());
        assert_eq!(query.stage_graph.child_edges[&3], HashSet::new());

        // Check the mappings of parent edges.
        assert_eq!(query.stage_graph.parent_edges[&0], HashSet::new());
        assert_eq!(query.stage_graph.parent_edges[&1], [0].into());
        assert_eq!(query.stage_graph.parent_edges[&2], [1].into());
        assert_eq!(query.stage_graph.parent_edges[&3], [1].into());

        // Verify topology order
        {
            let stage_id_to_pos: HashMap<StageId, usize> = query
                .stage_graph
                .stage_ids_by_topo_order()
                .enumerate()
                .map(|(pos, stage_id)| (stage_id, pos))
                .collect();

            for stage_id in query.stage_graph.stages.keys() {
                let stage_pos = stage_id_to_pos[stage_id];
                for child_stage_id in &query.stage_graph.child_edges[stage_id] {
                    let child_pos = stage_id_to_pos[child_stage_id];
                    assert!(stage_pos > child_pos);
                }
            }
        }

        // Check plan node in each stages.
        let root_exchange = query.stage_graph.stages.get(&0).unwrap();
        assert_eq!(root_exchange.root.node_type(), PlanNodeType::BatchExchange);
        assert_eq!(root_exchange.root.source_stage_id, Some(1));
        assert!(matches!(root_exchange.root.node, NodeBody::Exchange(_)));
        assert_eq!(root_exchange.parallelism, 1);
        assert!(!root_exchange.has_table_scan());

        let join_node = query.stage_graph.stages.get(&1).unwrap();
        assert_eq!(join_node.root.node_type(), PlanNodeType::BatchHashJoin);
        assert_eq!(join_node.parallelism, 3);

        assert!(matches!(join_node.root.node, NodeBody::HashJoin(_)));
        assert_eq!(join_node.root.source_stage_id, None);
        assert_eq!(2, join_node.root.children.len());

        assert!(matches!(
            join_node.root.children[0].node,
            NodeBody::Exchange(_)
        ));
        assert_eq!(join_node.root.children[0].source_stage_id, Some(2));
        assert_eq!(0, join_node.root.children[0].children.len());

        assert!(matches!(
            join_node.root.children[1].node,
            NodeBody::Exchange(_)
        ));
        assert_eq!(join_node.root.children[1].source_stage_id, Some(3));
        assert_eq!(0, join_node.root.children[1].children.len());
        assert!(!join_node.has_table_scan());

        let scan_node1 = query.stage_graph.stages.get(&2).unwrap();
        assert_eq!(scan_node1.root.node_type(), PlanNodeType::BatchSeqScan);
        assert_eq!(scan_node1.root.source_stage_id, None);
        assert_eq!(0, scan_node1.root.children.len());
        assert!(scan_node1.has_table_scan());

        let scan_node2 = query.stage_graph.stages.get(&3).unwrap();
        assert_eq!(scan_node2.root.node_type(), PlanNodeType::BatchFilter);
        assert_eq!(scan_node2.root.source_stage_id, None);
        assert_eq!(1, scan_node2.root.children.len());
        assert!(scan_node2.has_table_scan());
    }

    fn generate_parallel_units(start_id: u32, node_id: u32) -> Vec<ParallelUnit> {
        let parallel_degree = 8;
        (start_id..start_id + parallel_degree)
            .map(|id| ParallelUnit {
                id,
                worker_node_id: node_id,
            })
            .collect()
    }
}
