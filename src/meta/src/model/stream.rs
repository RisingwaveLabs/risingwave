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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::AddAssign;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{VirtualNode, WorkerSlotId};
use risingwave_common::util::stream_graph_visitor::{self, visit_stream_node};
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::{SourceId, StreamingParallelism, WorkerId};
use risingwave_pb::catalog::Table;
use risingwave_pb::common::PbActorLocation;
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::{ActorStatus, Fragment, State};
use risingwave_pb::meta::table_parallelism::{
    FixedParallelism, Parallelism, PbAdaptiveParallelism, PbCustomParallelism, PbFixedParallelism,
    PbParallelism,
};
use risingwave_pb::meta::{PbTableFragments, PbTableParallelism};
use risingwave_pb::plan_common::PbExprContext;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{FragmentTypeFlag, PbStreamContext, StreamActor, StreamNode};

use super::{ActorId, FragmentId};
use crate::model::MetadataModelResult;
use crate::stream::{build_actor_connector_splits, SplitAssignment};

/// The parallelism for a `TableFragments`.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum TableParallelism {
    /// This is when the system decides the parallelism, based on the available worker parallelisms.
    Adaptive,
    /// We set this when the `TableFragments` parallelism is changed.
    /// All fragments which are part of the `TableFragment` will have the same parallelism as this.
    Fixed(usize),
    /// We set this when the individual parallelisms of the `Fragments`
    /// can differ within a `TableFragments`.
    /// This is set for `risectl`, since it has a low-level interface,
    /// scale individual `Fragments` within `TableFragments`.
    /// When that happens, the `TableFragments` no longer has a consistent
    /// parallelism, so we set this to indicate that.
    Custom,
}

impl From<PbTableParallelism> for TableParallelism {
    fn from(value: PbTableParallelism) -> Self {
        use Parallelism::*;
        match &value.parallelism {
            Some(Fixed(FixedParallelism { parallelism: n })) => Self::Fixed(*n as usize),
            Some(Adaptive(_)) | Some(Auto(_)) => Self::Adaptive,
            Some(Custom(_)) => Self::Custom,
            _ => unreachable!(),
        }
    }
}

impl From<TableParallelism> for PbTableParallelism {
    fn from(value: TableParallelism) -> Self {
        use TableParallelism::*;

        let parallelism = match value {
            Adaptive => PbParallelism::Adaptive(PbAdaptiveParallelism {}),
            Fixed(n) => PbParallelism::Fixed(PbFixedParallelism {
                parallelism: n as u32,
            }),
            Custom => PbParallelism::Custom(PbCustomParallelism {}),
        };

        Self {
            parallelism: Some(parallelism),
        }
    }
}

impl From<StreamingParallelism> for TableParallelism {
    fn from(value: StreamingParallelism) -> Self {
        match value {
            StreamingParallelism::Adaptive => TableParallelism::Adaptive,
            StreamingParallelism::Fixed(n) => TableParallelism::Fixed(n),
            StreamingParallelism::Custom => TableParallelism::Custom,
        }
    }
}

impl From<TableParallelism> for StreamingParallelism {
    fn from(value: TableParallelism) -> Self {
        match value {
            TableParallelism::Adaptive => StreamingParallelism::Adaptive,
            TableParallelism::Fixed(n) => StreamingParallelism::Fixed(n),
            TableParallelism::Custom => StreamingParallelism::Custom,
        }
    }
}

pub type ActorUpstreams = BTreeMap<FragmentId, HashSet<ActorId>>;
pub type FragmentActorUpstreams = HashMap<ActorId, ActorUpstreams>;
pub type StreamActorWithUpstreams = (StreamActor, ActorUpstreams);

/// Fragments of a streaming job. Corresponds to [`PbTableFragments`].
/// (It was previously called `TableFragments` due to historical reasons.)
///
/// We store whole fragments in a single column family as follow:
/// `stream_job_id` => `StreamJobFragments`.
#[derive(Debug, Clone)]
pub struct StreamJobFragments {
    /// The table id.
    pub stream_job_id: TableId,

    /// The state of the table fragments.
    pub state: State,

    /// The table fragments.
    pub fragments: BTreeMap<FragmentId, Fragment>,

    /// The status of actors
    pub actor_status: BTreeMap<ActorId, ActorStatus>,

    /// The splits of actors,
    /// incl. both `Source` and `SourceBackfill` actors.
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,

    /// The streaming context associated with this stream plan and its fragments
    pub ctx: StreamContext,

    /// The parallelism assigned to this table fragments
    pub assigned_parallelism: TableParallelism,

    /// The max parallelism specified when the streaming job was created, i.e., expected vnode count.
    ///
    /// The reason for persisting this value is mainly to check if a parallelism change (via `ALTER
    /// .. SET PARALLELISM`) is valid, so that the behavior can be consistent with the creation of
    /// the streaming job.
    ///
    /// Note that the actual vnode count, denoted by `vnode_count` in `fragments`, may be different
    /// from this value (see `StreamFragmentGraph.max_parallelism` for more details.). As a result,
    /// checking the parallelism change with this value can be inaccurate in some cases. However,
    /// when generating resizing plans, we still take the `vnode_count` of each fragment into account.
    pub max_parallelism: usize,
}

#[derive(Debug, Clone, Default)]
pub struct StreamContext {
    /// The timezone used to interpret timestamps and dates for conversion
    pub timezone: Option<String>,
}

impl StreamContext {
    pub fn to_protobuf(&self) -> PbStreamContext {
        PbStreamContext {
            timezone: self.timezone.clone().unwrap_or("".into()),
        }
    }

    pub fn to_expr_context(&self) -> PbExprContext {
        PbExprContext {
            // `self.timezone` must always be set; an invalid value is used here for debugging if it's not.
            time_zone: self.timezone.clone().unwrap_or("Empty Time Zone".into()),
            strict_mode: false,
        }
    }

    pub fn from_protobuf(prost: &PbStreamContext) -> Self {
        Self {
            timezone: if prost.get_timezone().is_empty() {
                None
            } else {
                Some(prost.get_timezone().clone())
            },
        }
    }
}

impl StreamJobFragments {
    pub fn to_protobuf(&self) -> PbTableFragments {
        PbTableFragments {
            table_id: self.stream_job_id.table_id(),
            state: self.state as _,
            fragments: self.fragments.clone().into_iter().collect(),
            actor_status: self.actor_status.clone().into_iter().collect(),
            actor_splits: build_actor_connector_splits(&self.actor_splits),
            ctx: Some(self.ctx.to_protobuf()),
            parallelism: Some(self.assigned_parallelism.into()),
            node_label: "".to_owned(),
            backfill_done: true,
            max_parallelism: Some(self.max_parallelism as _),
        }
    }
}

pub type StreamJobActorsToCreate =
    HashMap<WorkerId, HashMap<FragmentId, (StreamNode, Vec<StreamActorWithUpstreams>)>>;

impl StreamJobFragments {
    /// Create a new `TableFragments` with state of `Initial`, with other fields empty.
    pub fn for_test(table_id: TableId, fragments: BTreeMap<FragmentId, Fragment>) -> Self {
        Self::new(
            table_id,
            fragments,
            &BTreeMap::new(),
            StreamContext::default(),
            TableParallelism::Adaptive,
            VirtualNode::COUNT_FOR_TEST,
        )
    }

    /// Create a new `TableFragments` with state of `Initial`, with the status of actors set to
    /// `Inactive` on the given workers.
    pub fn new(
        stream_job_id: TableId,
        fragments: BTreeMap<FragmentId, Fragment>,
        actor_locations: &BTreeMap<ActorId, WorkerSlotId>,
        ctx: StreamContext,
        table_parallelism: TableParallelism,
        max_parallelism: usize,
    ) -> Self {
        let actor_status = actor_locations
            .iter()
            .map(|(&actor_id, worker_slot_id)| {
                (
                    actor_id,
                    ActorStatus {
                        location: PbActorLocation::from_worker(worker_slot_id.worker_id()),
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();

        Self {
            stream_job_id,
            state: State::Initial,
            fragments,
            actor_status,
            actor_splits: HashMap::default(),
            ctx,
            assigned_parallelism: table_parallelism,
            max_parallelism,
        }
    }

    pub fn fragment_ids(&self) -> impl Iterator<Item = FragmentId> + '_ {
        self.fragments.keys().cloned()
    }

    pub fn fragments(&self) -> impl Iterator<Item = &Fragment> {
        self.fragments.values()
    }

    /// Returns the table id.
    pub fn stream_job_id(&self) -> TableId {
        self.stream_job_id
    }

    /// Returns the state of the table fragments.
    pub fn state(&self) -> State {
        self.state
    }

    /// Returns the timezone of the table
    pub fn timezone(&self) -> Option<String> {
        self.ctx.timezone.clone()
    }

    /// Returns whether the table fragments is in `Created` state.
    pub fn is_created(&self) -> bool {
        self.state == State::Created
    }

    /// Returns whether the table fragments is in `Initial` state.
    pub fn is_initial(&self) -> bool {
        self.state == State::Initial
    }

    /// Set the state of the table fragments.
    pub fn set_state(&mut self, state: State) {
        self.state = state;
    }

    /// Update state of all actors
    pub fn update_actors_state(&mut self, state: ActorState) {
        for actor_status in self.actor_status.values_mut() {
            actor_status.set_state(state);
        }
    }

    pub fn set_actor_splits_by_split_assignment(&mut self, split_assignment: SplitAssignment) {
        self.actor_splits = split_assignment.into_values().flatten().collect();
    }

    /// Returns actor ids associated with this table.
    pub fn actor_ids(&self) -> Vec<ActorId> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
            .collect()
    }

    pub fn actor_fragment_mapping(&self) -> HashMap<ActorId, FragmentId> {
        self.fragments
            .values()
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, fragment.fragment_id))
            })
            .collect()
    }

    /// Returns actors associated with this table.
    #[cfg(test)]
    pub fn actors(&self) -> Vec<StreamActor> {
        self.fragments
            .values()
            .flat_map(|fragment| fragment.actors.clone())
            .collect()
    }

    /// Returns the actor ids with the given fragment type.
    pub fn filter_actor_ids(
        &self,
        check_type: impl Fn(u32) -> bool + 'static,
    ) -> impl Iterator<Item = ActorId> + '_ {
        self.fragments
            .values()
            .filter(move |fragment| check_type(fragment.get_fragment_type_mask()))
            .flat_map(|fragment| fragment.actors.iter().map(|actor| actor.actor_id))
    }

    /// Returns mview actor ids.
    pub fn mview_actor_ids(&self) -> Vec<ActorId> {
        Self::filter_actor_ids(self, |fragment_type_mask| {
            (fragment_type_mask & FragmentTypeFlag::Mview as u32) != 0
        })
        .collect()
    }

    /// Returns actor ids that need to be tracked when creating MV.
    pub fn tracking_progress_actor_ids(&self) -> Vec<(ActorId, BackfillUpstreamType)> {
        let mut actor_ids = vec![];
        for fragment in self.fragments.values() {
            if fragment.fragment_type_mask & FragmentTypeFlag::CdcFilter as u32 != 0 {
                // Note: CDC table job contains a StreamScan fragment (StreamCdcScan node) and a CdcFilter fragment.
                // We don't track any fragments' progress.
                return vec![];
            }
            if (fragment.fragment_type_mask
                & (FragmentTypeFlag::Values as u32
                    | FragmentTypeFlag::StreamScan as u32
                    | FragmentTypeFlag::SourceScan as u32))
                != 0
            {
                actor_ids.extend(fragment.actors.iter().map(|actor| {
                    (
                        actor.actor_id,
                        BackfillUpstreamType::from_fragment_type_mask(fragment.fragment_type_mask),
                    )
                }));
            }
        }
        actor_ids
    }

    pub fn root_fragment(&self) -> Option<Fragment> {
        self.mview_fragment()
            .or_else(|| self.sink_fragment())
            .or_else(|| self.source_fragment())
    }

    /// Returns the fragment with the `Mview` type flag.
    pub fn mview_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Mview as u32) != 0
            })
            .cloned()
    }

    pub fn source_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Source as u32) != 0
            })
            .cloned()
    }

    pub fn sink_fragment(&self) -> Option<Fragment> {
        self.fragments
            .values()
            .find(|fragment| {
                (fragment.get_fragment_type_mask() & FragmentTypeFlag::Sink as u32) != 0
            })
            .cloned()
    }

    pub fn snapshot_backfill_actor_ids(&self) -> HashSet<ActorId> {
        Self::filter_actor_ids(self, |mask| {
            (mask & FragmentTypeFlag::SnapshotBackfillStreamScan as u32) != 0
        })
        .collect()
    }

    /// Extract the fragments that include source executors that contains an external stream source,
    /// grouping by source id.
    pub fn stream_source_fragments(&self) -> HashMap<SourceId, BTreeSet<FragmentId>> {
        let mut source_fragments = HashMap::new();

        for fragment in self.fragments() {
            {
                if let Some(source_id) = fragment.nodes.as_ref().unwrap().find_stream_source() {
                    source_fragments
                        .entry(source_id as SourceId)
                        .or_insert(BTreeSet::new())
                        .insert(fragment.fragment_id as FragmentId);
                }
            }
        }
        source_fragments
    }

    /// Returns (`source_id`, -> (`source_backfill_fragment_id`, `upstream_source_fragment_id`)).
    ///
    /// Note: the fragment `source_backfill_fragment_id` may actually have multiple upstream fragments,
    /// but only one of them is the upstream source fragment, which is what we return.
    pub fn source_backfill_fragments(
        &self,
    ) -> MetadataModelResult<HashMap<SourceId, BTreeSet<(FragmentId, FragmentId)>>> {
        let mut source_backfill_fragments = HashMap::new();

        for fragment in self.fragments() {
            {
                if let Some((source_id, upstream_source_fragment_id)) =
                    fragment.nodes.as_ref().unwrap().find_source_backfill()
                {
                    source_backfill_fragments
                        .entry(source_id as SourceId)
                        .or_insert(BTreeSet::new())
                        .insert((fragment.fragment_id, upstream_source_fragment_id));
                }
            }
        }
        Ok(source_backfill_fragments)
    }

    /// Find the table job's `Union` fragment.
    /// Panics if not found.
    pub fn union_fragment_for_table(&mut self) -> &mut Fragment {
        let mut union_fragment_id = None;
        for (fragment_id, fragment) in &self.fragments {
            {
                if let Some(node) = &fragment.nodes {
                    visit_stream_node(node, |body| {
                        if let NodeBody::Union(_) = body {
                            if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                // The union fragment should be unique.
                                assert_eq!(*union_fragment_id, *fragment_id);
                            } else {
                                union_fragment_id = Some(*fragment_id);
                            }
                        }
                    })
                };
            }
        }

        let union_fragment_id =
            union_fragment_id.expect("fragment of placeholder merger not found");
        let union_fragment = self
            .fragments
            .get_mut(&union_fragment_id)
            .unwrap_or_else(|| panic!("fragment {} not found", union_fragment_id));

        union_fragment
    }

    /// Resolve dependent table
    fn resolve_dependent_table(stream_node: &StreamNode, table_ids: &mut HashMap<TableId, usize>) {
        let table_id = match stream_node.node_body.as_ref() {
            Some(NodeBody::StreamScan(stream_scan)) => Some(TableId::new(stream_scan.table_id)),
            Some(NodeBody::StreamCdcScan(stream_scan)) => Some(TableId::new(stream_scan.table_id)),
            _ => None,
        };
        if let Some(table_id) = table_id {
            table_ids.entry(table_id).or_default().add_assign(1);
        }

        for child in &stream_node.input {
            Self::resolve_dependent_table(child, table_ids);
        }
    }

    /// Returns a mapping of dependent table ids of the `TableFragments`
    /// to their corresponding count.
    pub fn dependent_table_ids(&self) -> HashMap<TableId, usize> {
        let mut table_ids = HashMap::new();
        self.fragments.values().for_each(|fragment| {
            Self::resolve_dependent_table(fragment.nodes.as_ref().unwrap(), &mut table_ids);
        });

        table_ids
    }

    /// Returns states of actors group by worker id.
    pub fn worker_actor_states(&self) -> BTreeMap<WorkerId, Vec<(ActorId, ActorState)>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.worker_id() as WorkerId;
            map.entry(node_id)
                .or_insert_with(Vec::new)
                .push((actor_id, actor_status.state()));
        }
        map
    }

    /// Returns actor locations group by worker id.
    pub fn worker_actor_ids(&self) -> BTreeMap<WorkerId, Vec<ActorId>> {
        let mut map = BTreeMap::default();
        for (&actor_id, actor_status) in &self.actor_status {
            let node_id = actor_status.worker_id() as WorkerId;
            map.entry(node_id).or_insert_with(Vec::new).push(actor_id);
        }
        map
    }

    /// Returns the status of actors group by worker id.
    pub fn active_actors(&self) -> Vec<StreamActor> {
        let mut actors = vec![];
        for fragment in self.fragments.values() {
            for actor in &fragment.actors {
                if self.actor_status[&actor.actor_id].state == ActorState::Inactive as i32 {
                    continue;
                }
                actors.push(actor.clone());
            }
        }
        actors
    }

    pub fn actors_to_create(
        &self,
    ) -> impl Iterator<
        Item = (
            FragmentId,
            &StreamNode,
            impl Iterator<Item = (&StreamActor, WorkerId)> + '_,
        ),
    > + '_ {
        self.fragments.values().map(move |fragment| {
            (
                fragment.fragment_id,
                fragment.nodes.as_ref().unwrap(),
                fragment.actors.iter().map(move |actor| {
                    let worker_id = self
                        .actor_status
                        .get(&actor.actor_id)
                        .expect("should exist")
                        .worker_id() as WorkerId;
                    (actor, worker_id)
                }),
            )
        })
    }

    pub fn mv_table_id(&self) -> Option<u32> {
        if self
            .fragments
            .values()
            .flat_map(|f| f.state_table_ids.iter())
            .any(|table_id| *table_id == self.stream_job_id.table_id)
        {
            Some(self.stream_job_id.table_id)
        } else {
            None
        }
    }

    /// Retrieve the **complete** internal tables map of the whole graph.
    ///
    /// Compared to [`crate::stream::StreamFragmentGraph::incomplete_internal_tables`],
    /// the table catalogs returned here are complete, with all fields filled.
    pub fn internal_tables(&self) -> BTreeMap<u32, Table> {
        self.collect_tables_inner(true)
    }

    /// `internal_tables()` with additional table in `Materialize` node.
    pub fn all_tables(&self) -> BTreeMap<u32, Table> {
        self.collect_tables_inner(false)
    }

    fn collect_tables_inner(&self, internal_tables_only: bool) -> BTreeMap<u32, Table> {
        let mut tables = BTreeMap::new();
        for fragment in self.fragments.values() {
            stream_graph_visitor::visit_stream_node_tables_inner(
                &mut fragment.nodes.clone().unwrap(),
                internal_tables_only,
                true,
                |table, _| {
                    let table_id = table.id;
                    tables
                        .try_insert(table_id, table.clone())
                        .unwrap_or_else(|_| panic!("duplicated table id `{}`", table_id));
                },
            );
        }
        tables
    }

    /// Returns the internal table ids without the mview table.
    pub fn internal_table_ids(&self) -> Vec<u32> {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
            .filter(|&t| t != self.stream_job_id.table_id)
            .collect_vec()
    }

    /// Returns all internal table ids including the mview table.
    pub fn all_table_ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.fragments
            .values()
            .flat_map(|f| f.state_table_ids.clone())
    }

    /// Fill the `expr_context` in `StreamActor`. Used for compatibility.
    pub fn fill_expr_context(mut self) -> Self {
        self.fragments.values_mut().for_each(|fragment| {
            fragment.actors.iter_mut().for_each(|actor| {
                if actor.expr_context.is_none() {
                    actor.expr_context = Some(self.ctx.to_expr_context());
                }
            });
        });
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillUpstreamType {
    MView,
    Values,
    Source,
}

impl BackfillUpstreamType {
    pub fn from_fragment_type_mask(mask: u32) -> Self {
        let is_mview = (mask & FragmentTypeFlag::StreamScan as u32) != 0;
        let is_values = (mask & FragmentTypeFlag::Values as u32) != 0;
        let is_source = (mask & FragmentTypeFlag::SourceScan as u32) != 0;

        // Note: in theory we can have multiple backfill executors in one fragment, but currently it's not possible.
        // See <https://github.com/risingwavelabs/risingwave/issues/6236>.
        debug_assert!(
            is_mview as u8 + is_values as u8 + is_source as u8 == 1,
            "a backfill fragment should either be mview, value or source, found {:?}",
            mask
        );

        if is_mview {
            BackfillUpstreamType::MView
        } else if is_values {
            BackfillUpstreamType::Values
        } else if is_source {
            BackfillUpstreamType::Source
        } else {
            unreachable!("invalid fragment type mask: {}", mask);
        }
    }
}
