use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::error::Result;
use uuid::Uuid;

use crate::optimizer::plan_node::{BatchExchange, PlanNodeType, PlanTreeNode};
use crate::optimizer::property::Distribution;
use crate::optimizer::PlanRef;

pub(crate) type StageId = u64;
pub(crate) type TaskId = u64;

struct BatchPlanFragmenter {
    stage_graph_builder: StageGraphBuilder,
    next_stage_id: u64,
}

impl BatchPlanFragmenter {
    pub fn new() -> Self {
        Self {
            stage_graph_builder: StageGraphBuilder::new(),
            next_stage_id: 0,
        }
    }
}

pub(crate) struct Query {
    /// Query id should always be unique.
    query_id: Uuid,
    pub(crate) stage_graph: StageGraph,
}

impl Query {
    pub fn leaf_stages(&self) -> Vec<StageId> {
        let mut ret_leaf_stages = Vec::new();
        for stage_id in self.stage_graph.stages.keys() {
            if self.stage_graph.get_child_stages_unchecked(stage_id).len() == 0 {
                ret_leaf_stages.push(*stage_id);
            }
        }
        ret_leaf_stages
    }

    pub fn get_parents(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.stage_graph.parent_edges.get(stage_id).unwrap()
    }
}

pub(crate) struct QueryStage {
    pub id: StageId,
    pub root: PlanRef,
    pub distribution: Distribution,
}

pub(crate) type QueryStageRef = Arc<QueryStage>;

pub(crate) struct AugmentedStage {
    query_stage: QueryStageRef,
    /// Fill exchange source to augment phase.
    /// TODO(Bowen): Introduce augment phase to fill in exchange node info (#73).
    exchange_source: HashMap<StageId, ScheduledStageRef>,
    parallelism: u32,
}

impl AugmentedStage {
    pub fn new_with_query_stage(
        query_stage: QueryStageRef,
        exchange_source: &HashMap<StageId, ScheduledStageRef>,
        workers: &[WorkerNode],
        next_stage_parallelism: u32,
    ) -> Self {
        Self {
            query_stage: query_stage,
            exchange_source: exchange_source.clone(),
            parallelism: next_stage_parallelism,
        }
    }
}

type AugmentedQueryStageRef = Arc<QueryStage>;

pub(crate) struct ScheduledStage {
    pub id: StageId,
    pub assignments: HashMap<TaskId, WorkerNode>,
    pub query_stage: AugmentedQueryStageRef,
}

pub(crate) type ScheduledStageRef = Arc<ScheduledStage>;

#[derive(Clone)]
pub(crate) struct WorkerNode;

pub(crate) struct StageGraph {
    pub(crate) id: StageId,
    stages: HashMap<StageId, QueryStageRef>,
    /// Traverse from top to down. Used in split plan into stages.
    child_edges: HashMap<StageId, HashSet<StageId>>,
    /// Traverse from down to top. Used in schedule each stage.
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    /// Indicates which stage the exchange executor is running on.
    /// Look up child stage for exchange source so that parent stage knows where to pull data.
    exchange_id_to_stage: HashMap<u64, StageId>,
}

impl StageGraph {
    pub fn get_stage_unchecked(&self, stage_id: &StageId) -> QueryStageRef {
        self.stages.get(stage_id).unwrap().clone()
    }

    pub fn get_child_stages_unchecked(&self, stage_id: &StageId) -> &HashSet<StageId> {
        self.child_edges.get(stage_id).unwrap()
    }
}

struct StageGraphBuilder {
    stages: HashMap<StageId, QueryStageRef>,
    child_edges: HashMap<StageId, HashSet<StageId>>,
    parent_edges: HashMap<StageId, HashSet<StageId>>,
    exchange_id_to_stage: HashMap<u64, StageId>,
}

impl StageGraphBuilder {
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            child_edges: HashMap::new(),
            parent_edges: HashMap::new(),
            exchange_id_to_stage: HashMap::new(),
        }
    }

    pub fn build(mut self, stage_id: StageId) -> StageGraph {
        for stage_id in self.stages.keys() {
            if self.child_edges.get(stage_id).is_none() {
                self.child_edges.insert(*stage_id, HashSet::new());
            }

            if self.parent_edges.get(stage_id).is_none() {
                self.parent_edges.insert(*stage_id, HashSet::new());
            }
        }

        StageGraph {
            id: stage_id,
            stages: self.stages,
            child_edges: self.child_edges,
            parent_edges: self.parent_edges,
            exchange_id_to_stage: self.exchange_id_to_stage,
        }
    }

    /// Link parent stage and child stage. Maintain the mappings of parent -> child and child ->
    /// parent.
    ///
    /// # Arguments
    ///
    /// * `exchange_id` - The operator id of exchange executor.
    pub fn link_to_child(&mut self, parent_id: StageId, exchange_id: u64, child_id: StageId) {
        let child_ids = self.child_edges.get_mut(&parent_id);
        // If the parent id does not exist, create a new set containing the child ids. Otherwise
        // just insert.
        match child_ids {
            Some(childs) => {
                childs.insert(child_id);
            }

            None => {
                let mut childs = HashSet::new();
                childs.insert(child_id);
                self.child_edges.insert(parent_id, childs);
            }
        };

        let parent_ids = self.parent_edges.get_mut(&child_id);
        // If the child id does not exist, create a new set containing the parent ids. Otherwise
        // just insert.
        match parent_ids {
            Some(parent_ids) => {
                parent_ids.insert(parent_id);
            }

            None => {
                let mut parents = HashSet::new();
                parents.insert(child_id);
                self.parent_edges.insert(child_id, parents);
            }
        };
        self.exchange_id_to_stage.insert(exchange_id, child_id);
    }

    pub fn add_node(&mut self, stage: QueryStageRef) {
        self.stages.insert(stage.id, stage);
    }
}

impl BatchPlanFragmenter {
    /// Split the plan node into each stages, based on exchange node.
    pub fn split(mut self, batch_node: PlanRef) -> Result<Query> {
        let root_stage_graph = self.new_query_stage(batch_node.clone(), batch_node.distribution());
        self.build_stage(&root_stage_graph, batch_node.clone());
        let stage_graph = self.stage_graph_builder.build(root_stage_graph.id);
        Ok(Query {
            stage_graph,
            query_id: Uuid::new_v4(),
        })
    }

    fn new_query_stage(&mut self, node: PlanRef, distribution: Distribution) -> QueryStageRef {
        let next_stage_id = self.next_stage_id;
        self.next_stage_id += 1;
        let stage = Arc::new(QueryStage {
            id: next_stage_id,
            root: node.clone(),
            distribution,
        });
        self.stage_graph_builder.add_node(stage.clone());
        stage
    }

    /// Based on current stage, use stage graph builder to recursively build the DAG plan (splits
    /// the plan by exchange node.). Children under pipeline-breaker separately forms a stage
    /// (aka plan fragment).
    fn build_stage(&mut self, cur_stage: &QueryStage, node: PlanRef) {
        // NOTE: The breaker's children will not be logically removed after plan slicing,
        // but their serialized plan will ignore the children. Therefore, the compute-node
        // will eventually only receive the sliced part.
        if node.node_type() == PlanNodeType::BatchExchange {
            let exchange_node = node.downcast_ref::<BatchExchange>().unwrap();
            for child_node in exchange_node.inputs() {
                // If plan node is a exchange node, for each inputs (child), new a query stage and
                // link with current stage.
                let child_query_stage =
                    self.new_query_stage(child_node.clone(), child_node.distribution());
                // TODO(Bowen): replace mock exchange id 0 to real operator id (#67).
                self.stage_graph_builder
                    .link_to_child(cur_stage.id, 0, child_query_stage.id);
                self.build_stage(&child_query_stage, child_node);
            }
        } else {
            for child_node in node.inputs() {
                // All child nodes still belongs to current stage if no exchange.
                self.build_stage(cur_stage, child_node);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use risingwave_pb::plan::JoinType;

    use crate::optimizer::plan_node::{
        BatchExchange, BatchHashJoin, BatchSeqScan, IntoPlanRef, JoinPredicate, LogicalJoin,
        PlanNodeType,
    };
    use crate::optimizer::property::{Distribution, Order};
    use crate::scheduler::plan_fragmenter::BatchPlanFragmenter;

    #[test]
    fn test_fragmenter() {
        // Construct a Hash Join with Exchange node.
        // Logical plan:
        //
        //    HashJoin
        //     /    \
        //   Scan  Scan
        //
        let batch_plan_node = BatchSeqScan::default().into_plan_ref();
        let batch_exchange_node1 = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::Shard,
        )
        .into_plan_ref();
        let batch_exchange_node2 = BatchExchange::new(
            batch_plan_node.clone(),
            Order::default(),
            Distribution::Shard,
        )
        .into_plan_ref();
        let hash_join_node = BatchHashJoin::new(LogicalJoin::new(
            batch_exchange_node1.clone(),
            batch_exchange_node2.clone(),
            JoinType::Inner,
            JoinPredicate::new_empty(),
        ))
        .into_plan_ref();
        let batch_exchange_node3 = BatchExchange::new(
            hash_join_node.clone(),
            Order::default(),
            Distribution::Single,
        )
        .into_plan_ref();

        // Break the plan node into fragments.
        let fragmenter = BatchPlanFragmenter::new();
        let query = fragmenter.split(batch_exchange_node3).unwrap();

        assert_eq!(query.stage_graph.id, 0);
        assert_eq!(query.stage_graph.stages.len(), 4);

        // Check the mappings of child edges.
        assert_eq!(query.stage_graph.child_edges.get(&0).unwrap().len(), 1);
        assert_eq!(query.stage_graph.child_edges.get(&1).unwrap().len(), 2);
        assert_eq!(query.stage_graph.child_edges.get(&2).unwrap().len(), 0);
        assert_eq!(query.stage_graph.child_edges.get(&3).unwrap().len(), 0);

        // Check the mappings of parent edges.
        assert_eq!(query.stage_graph.parent_edges.get(&0).unwrap().len(), 0);
        assert_eq!(query.stage_graph.parent_edges.get(&1).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&2).unwrap().len(), 1);
        assert_eq!(query.stage_graph.parent_edges.get(&3).unwrap().len(), 1);

        // Check plan node in each stages.
        let root_exchange = query.stage_graph.stages.get(&0).unwrap();
        assert_eq!(root_exchange.root.node_type(), PlanNodeType::BatchExchange);
        let join_node = query.stage_graph.stages.get(&1).unwrap();
        assert_eq!(join_node.root.node_type(), PlanNodeType::BatchHashJoin);
        let scan_node1 = query.stage_graph.stages.get(&2).unwrap();
        assert_eq!(scan_node1.root.node_type(), PlanNodeType::BatchSeqScan);
        let scan_node2 = query.stage_graph.stages.get(&3).unwrap();
        assert_eq!(scan_node2.root.node_type(), PlanNodeType::BatchSeqScan);
    }
}
