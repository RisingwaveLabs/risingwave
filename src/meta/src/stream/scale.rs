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

use std::cmp::{min, Ordering};
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use futures::future::try_join_all;
use itertools::Itertools;
use num_integer::Integer;
use num_traits::abs;
use risingwave_common::bail;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::hash::ActorMapping;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_meta_model::{actor, fragment, ObjectId, WorkerId};
use risingwave_pb::common::{PbActorLocation, WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::meta::table_fragments::{self, ActorStatus, PbFragment, State};
use risingwave_pb::meta::FragmentWorkerSlotMappings;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    Dispatcher, DispatcherType, FragmentTypeFlag, PbDispatcher, PbStreamActor, StreamActor,
};
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Receiver;
use tokio::sync::{oneshot, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::barrier::{Command, Reschedule};
use crate::controller::scale::RescheduleWorkingSet;
use crate::manager::{LocalNotification, MetaSrvEnv, MetadataManager};
use crate::model::{ActorId, ActorUpstreams, DispatcherId, FragmentId, TableParallelism};
use crate::serving::{
    to_deleted_fragment_worker_slot_mapping, to_fragment_worker_slot_mapping, ServingVnodeMapping,
};
use crate::stream::{GlobalStreamManager, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorkerReschedule {
    pub worker_actor_diff: BTreeMap<WorkerId, isize>,
}

pub struct CustomFragmentInfo {
    pub fragment_id: u32,
    pub fragment_type_mask: u32,
    pub distribution_type: PbFragmentDistributionType,
    pub state_table_ids: Vec<u32>,
    pub upstream_fragment_ids: Vec<u32>,
    pub actor_template: PbStreamActor,
    pub actors: Vec<CustomActorInfo>,
}

#[derive(Default, Clone)]
pub struct CustomActorInfo {
    pub actor_id: u32,
    pub fragment_id: u32,
    pub dispatcher: Vec<Dispatcher>,
    /// `None` if singleton.
    pub vnode_bitmap: Option<Bitmap>,
}

impl From<&PbStreamActor> for CustomActorInfo {
    fn from(
        PbStreamActor {
            actor_id,
            fragment_id,
            dispatcher,
            vnode_bitmap,
            ..
        }: &PbStreamActor,
    ) -> Self {
        CustomActorInfo {
            actor_id: *actor_id,
            fragment_id: *fragment_id,
            dispatcher: dispatcher.clone(),
            vnode_bitmap: vnode_bitmap.as_ref().map(Bitmap::from),
        }
    }
}

impl From<&PbFragment> for CustomFragmentInfo {
    fn from(fragment: &PbFragment) -> Self {
        CustomFragmentInfo {
            fragment_id: fragment.fragment_id,
            fragment_type_mask: fragment.fragment_type_mask,
            distribution_type: fragment.distribution_type(),
            state_table_ids: fragment.state_table_ids.clone(),
            upstream_fragment_ids: fragment.upstream_fragment_ids.clone(),
            actor_template: fragment
                .actors
                .first()
                .cloned()
                .expect("no actor in fragment"),
            actors: fragment
                .actors
                .iter()
                .map(CustomActorInfo::from)
                .sorted_by(|actor_a, actor_b| actor_a.actor_id.cmp(&actor_b.actor_id))
                .collect(),
        }
    }
}

impl CustomFragmentInfo {
    pub fn get_fragment_type_mask(&self) -> u32 {
        self.fragment_type_mask
    }

    pub fn distribution_type(&self) -> FragmentDistributionType {
        self.distribution_type
    }
}

use educe::Educe;
use risingwave_common::util::stream_graph_visitor::{visit_stream_node, visit_stream_node_cont};

use super::SourceChange;
use crate::controller::id::IdCategory;
use crate::controller::utils::filter_workers_by_resource_group;

// The debug implementation is arbitrary. Just used in debug logs.
#[derive(Educe)]
#[educe(Debug)]
pub struct RescheduleContext {
    /// Meta information for all Actors
    #[educe(Debug(ignore))]
    actor_map: HashMap<ActorId, CustomActorInfo>,
    /// Status of all Actors, used to find the location of the `Actor`
    actor_status: BTreeMap<ActorId, WorkerId>,
    /// Meta information of all `Fragment`, used to find the `Fragment`'s `Actor`
    #[educe(Debug(ignore))]
    fragment_map: HashMap<FragmentId, CustomFragmentInfo>,
    /// Index of all `Actor` upstreams, specific to `Dispatcher`
    upstream_dispatchers: HashMap<ActorId, Vec<(FragmentId, DispatcherId, DispatcherType)>>,
    /// Fragments with `StreamSource`
    stream_source_fragment_ids: HashSet<FragmentId>,
    /// Fragments with `StreamSourceBackfill` and the corresponding upstream source fragment
    stream_source_backfill_fragment_ids: HashMap<FragmentId, FragmentId>,
    /// Target fragments in `NoShuffle` relation
    no_shuffle_target_fragment_ids: HashSet<FragmentId>,
    /// Source fragments in `NoShuffle` relation
    no_shuffle_source_fragment_ids: HashSet<FragmentId>,
    // index for dispatcher type from upstream fragment to downstream fragment
    fragment_dispatcher_map: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
}

impl RescheduleContext {
    fn actor_id_to_worker_id(&self, actor_id: &ActorId) -> MetaResult<WorkerId> {
        self.actor_status
            .get(actor_id)
            .cloned()
            .ok_or_else(|| anyhow!("could not find worker for actor {}", actor_id).into())
    }
}

/// This function provides an simple balancing method
/// The specific process is as follows
///
/// 1. Calculate the number of target actors, and calculate the average value and the remainder, and
///    use the average value as expected.
///
/// 2. Filter out the actor to be removed and the actor to be retained, and sort them from largest
///    to smallest (according to the number of virtual nodes held).
///
/// 3. Calculate their balance, 1) For the actors to be removed, the number of virtual nodes per
///    actor is the balance. 2) For retained actors, the number of virtual nodes - expected is the
///    balance. 3) For newly created actors, -expected is the balance (always negative).
///
/// 4. Allocate the remainder, high priority to newly created nodes.
///
/// 5. After that, merge removed, retained and created into a queue, with the head of the queue
///    being the source, and move the virtual nodes to the destination at the end of the queue.
///
/// This can handle scale in, scale out, migration, and simultaneous scaling with as much affinity
/// as possible.
///
/// Note that this function can only rebalance actors whose `vnode_bitmap` is not `None`, in other
/// words, for `Fragment` of `FragmentDistributionType::Single`, using this function will cause
/// assert to fail and should be skipped from the upper level.
///
/// The return value is the bitmap distribution after scaling, which covers all virtual node indexes
pub fn rebalance_actor_vnode(
    actors: &[CustomActorInfo],
    actors_to_remove: &BTreeSet<ActorId>,
    actors_to_create: &BTreeSet<ActorId>,
) -> HashMap<ActorId, Bitmap> {
    let actor_ids: BTreeSet<_> = actors.iter().map(|actor| actor.actor_id).collect();

    assert_eq!(actors_to_remove.difference(&actor_ids).count(), 0);
    assert_eq!(actors_to_create.intersection(&actor_ids).count(), 0);

    assert!(actors.len() >= actors_to_remove.len());

    let target_actor_count = actors.len() - actors_to_remove.len() + actors_to_create.len();
    assert!(target_actor_count > 0);

    // `vnode_bitmap` must be set on distributed fragments.
    let vnode_count = actors[0]
        .vnode_bitmap
        .as_ref()
        .expect("vnode bitmap unset")
        .len();

    // represents the balance of each actor, used to sort later
    #[derive(Debug)]
    struct Balance {
        actor_id: ActorId,
        balance: i32,
        builder: BitmapBuilder,
    }
    let (expected, mut remain) = vnode_count.div_rem(&target_actor_count);

    tracing::debug!(
        "expected {}, remain {}, prev actors {}, target actors {}",
        expected,
        remain,
        actors.len(),
        target_actor_count,
    );

    let (mut removed, mut rest): (Vec<_>, Vec<_>) = actors
        .iter()
        .map(|actor| {
            (
                actor.actor_id as ActorId,
                actor.vnode_bitmap.clone().expect("vnode bitmap unset"),
            )
        })
        .partition(|(actor_id, _)| actors_to_remove.contains(actor_id));

    let order_by_bitmap_desc =
        |(id_a, bitmap_a): &(ActorId, Bitmap), (id_b, bitmap_b): &(ActorId, Bitmap)| -> Ordering {
            bitmap_a
                .count_ones()
                .cmp(&bitmap_b.count_ones())
                .reverse()
                .then(id_a.cmp(id_b))
        };

    let builder_from_bitmap = |bitmap: &Bitmap| -> BitmapBuilder {
        let mut builder = BitmapBuilder::default();
        builder.append_bitmap(bitmap);
        builder
    };

    let (prev_expected, _) = vnode_count.div_rem(&actors.len());

    let prev_remain = removed
        .iter()
        .map(|(_, bitmap)| {
            assert!(bitmap.count_ones() >= prev_expected);
            bitmap.count_ones() - prev_expected
        })
        .sum::<usize>();

    removed.sort_by(order_by_bitmap_desc);
    rest.sort_by(order_by_bitmap_desc);

    let removed_balances = removed.into_iter().map(|(actor_id, bitmap)| Balance {
        actor_id,
        balance: bitmap.count_ones() as i32,
        builder: builder_from_bitmap(&bitmap),
    });

    let mut rest_balances = rest
        .into_iter()
        .map(|(actor_id, bitmap)| Balance {
            actor_id,
            balance: bitmap.count_ones() as i32 - expected as i32,
            builder: builder_from_bitmap(&bitmap),
        })
        .collect_vec();

    let mut created_balances = actors_to_create
        .iter()
        .map(|actor_id| Balance {
            actor_id: *actor_id,
            balance: -(expected as i32),
            builder: BitmapBuilder::zeroed(vnode_count),
        })
        .collect_vec();

    for balance in created_balances
        .iter_mut()
        .rev()
        .take(prev_remain)
        .chain(rest_balances.iter_mut())
    {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    // consume the rest `remain`
    for balance in &mut created_balances {
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
    }

    assert_eq!(remain, 0);

    let mut v: VecDeque<_> = removed_balances
        .chain(rest_balances)
        .chain(created_balances)
        .collect();

    // We will return the full bitmap here after rebalancing,
    // if we want to return only the changed actors, filter balance = 0 here
    let mut result = HashMap::with_capacity(target_actor_count);

    for balance in &v {
        tracing::debug!(
            "actor {:5}\tbalance {:5}\tR[{:5}]\tC[{:5}]",
            balance.actor_id,
            balance.balance,
            actors_to_remove.contains(&balance.actor_id),
            actors_to_create.contains(&balance.actor_id)
        );
    }

    while !v.is_empty() {
        if v.len() == 1 {
            let single = v.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !actors_to_remove.contains(&single.actor_id) {
                result.insert(single.actor_id, single.builder.finish());
            }

            continue;
        }

        let mut src = v.pop_front().unwrap();
        let mut dst = v.pop_back().unwrap();

        let n = min(abs(src.balance), abs(dst.balance));

        let mut moved = 0;
        for idx in (0..vnode_count).rev() {
            if moved >= n {
                break;
            }

            if src.builder.is_set(idx) {
                src.builder.set(idx, false);
                assert!(!dst.builder.is_set(idx));
                dst.builder.set(idx, true);
                moved += 1;
            }
        }

        src.balance -= n;
        dst.balance += n;

        if src.balance != 0 {
            v.push_front(src);
        } else if !actors_to_remove.contains(&src.actor_id) {
            result.insert(src.actor_id, src.builder.finish());
        }

        if dst.balance != 0 {
            v.push_back(dst);
        } else {
            result.insert(dst.actor_id, dst.builder.finish());
        }
    }

    result
}

#[derive(Debug, Clone, Copy)]
pub struct RescheduleOptions {
    /// Whether to resolve the upstream of `NoShuffle` when scaling. It will check whether all the reschedules in the no shuffle dependency tree are corresponding, and rewrite them to the root of the no shuffle dependency tree.
    pub resolve_no_shuffle_upstream: bool,

    /// Whether to skip creating new actors. If it is true, the scaling-out actors will not be created.
    pub skip_create_new_actors: bool,
}

pub type ScaleControllerRef = Arc<ScaleController>;

pub struct ScaleController {
    pub metadata_manager: MetadataManager,

    pub source_manager: SourceManagerRef,

    pub env: MetaSrvEnv,

    /// We will acquire lock during DDL to prevent scaling operations on jobs that are in the creating state.
    /// e.g., a MV cannot be rescheduled during foreground backfill.
    pub reschedule_lock: RwLock<()>,
}

impl ScaleController {
    pub fn new(
        metadata_manager: &MetadataManager,
        source_manager: SourceManagerRef,
        env: MetaSrvEnv,
    ) -> Self {
        Self {
            metadata_manager: metadata_manager.clone(),
            source_manager,
            env,
            reschedule_lock: RwLock::new(()),
        }
    }

    pub async fn integrity_check(&self) -> MetaResult<()> {
        self.metadata_manager
            .catalog_controller
            .integrity_check()
            .await
    }

    /// Build the context for rescheduling and do some validation for the request.
    async fn build_reschedule_context(
        &self,
        reschedule: &mut HashMap<FragmentId, WorkerReschedule>,
        options: RescheduleOptions,
        table_parallelisms: &mut HashMap<TableId, TableParallelism>,
    ) -> MetaResult<RescheduleContext> {
        let worker_nodes: HashMap<WorkerId, WorkerNode> = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?
            .into_iter()
            .map(|worker_node| (worker_node.id as _, worker_node))
            .collect();

        if worker_nodes.is_empty() {
            bail!("no available compute node in the cluster");
        }

        // Check if we are trying to move a fragment to a node marked as unschedulable
        let unschedulable_worker_ids: HashSet<_> = worker_nodes
            .values()
            .filter(|w| {
                w.property
                    .as_ref()
                    .map(|property| property.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id as WorkerId)
            .collect();

        for (fragment_id, reschedule) in &*reschedule {
            for (worker_id, change) in &reschedule.worker_actor_diff {
                if unschedulable_worker_ids.contains(worker_id) && change.is_positive() {
                    bail!(
                        "unable to move fragment {} to unschedulable worker {}",
                        fragment_id,
                        worker_id
                    );
                }
            }
        }

        // FIXME: the same as anther place calling `list_table_fragments` in scaling.
        // Index for StreamActor
        let mut actor_map = HashMap::new();
        // Index for Fragment
        let mut fragment_map = HashMap::new();
        // Index for actor status, including actor's worker id
        let mut actor_status = BTreeMap::new();
        let mut fragment_state = HashMap::new();
        let mut fragment_to_table = HashMap::new();

        async fn fulfill_index_by_fragment_ids(
            actor_map: &mut HashMap<u32, CustomActorInfo>,
            fragment_map: &mut HashMap<FragmentId, CustomFragmentInfo>,
            actor_status: &mut BTreeMap<ActorId, WorkerId>,
            fragment_state: &mut HashMap<FragmentId, State>,
            fragment_to_table: &mut HashMap<FragmentId, TableId>,
            mgr: &MetadataManager,
            fragment_ids: Vec<risingwave_meta_model::FragmentId>,
        ) -> Result<(), MetaError> {
            let RescheduleWorkingSet {
                fragments,
                actors,
                mut actor_dispatchers,
                fragment_downstreams: _,
                fragment_upstreams,
                related_jobs,
                job_resource_groups: _job_resource_groups,
            } = mgr
                .catalog_controller
                .resolve_working_set_for_reschedule_fragments(fragment_ids)
                .await?;

            let mut fragment_actors: HashMap<
                risingwave_meta_model::FragmentId,
                Vec<CustomActorInfo>,
            > = HashMap::new();

            let mut expr_contexts = HashMap::new();
            for (
                _,
                actor::Model {
                    actor_id,
                    fragment_id,
                    status: _,
                    splits: _,
                    worker_id,
                    vnode_bitmap,
                    expr_context,
                    ..
                },
            ) in actors
            {
                let dispatchers = actor_dispatchers
                    .remove(&actor_id)
                    .unwrap_or_default()
                    .into_iter()
                    .map(PbDispatcher::from)
                    .collect();

                let actor_info = CustomActorInfo {
                    actor_id: actor_id as _,
                    fragment_id: fragment_id as _,
                    dispatcher: dispatchers,
                    vnode_bitmap: vnode_bitmap.map(|b| Bitmap::from(&b.to_protobuf())),
                };

                actor_map.insert(actor_id as _, actor_info.clone());

                fragment_actors
                    .entry(fragment_id as _)
                    .or_default()
                    .push(actor_info);

                actor_status.insert(actor_id as _, worker_id as WorkerId);

                expr_contexts.insert(actor_id as u32, expr_context);
            }

            for (
                _,
                fragment::Model {
                    fragment_id,
                    job_id,
                    fragment_type_mask,
                    distribution_type,
                    stream_node,
                    state_table_ids,
                    vnode_count: _,
                },
            ) in fragments
            {
                let actors = fragment_actors
                    .remove(&(fragment_id as _))
                    .unwrap_or_default();

                let CustomActorInfo {
                    actor_id,
                    fragment_id,
                    dispatcher,
                    vnode_bitmap,
                } = actors.first().unwrap().clone();

                let (related_job, job_definition) =
                    related_jobs.get(&job_id).expect("job not found");

                let upstream_fragment_ids = fragment_upstreams
                    .get(&(fragment_id as _))
                    .map(|upstreams| upstreams.iter().map(|&(id, _)| id as u32).collect_vec())
                    .unwrap_or_default();

                #[expect(deprecated)]
                let fragment = CustomFragmentInfo {
                    fragment_id: fragment_id as _,
                    fragment_type_mask: fragment_type_mask as _,
                    distribution_type: distribution_type.into(),
                    state_table_ids: state_table_ids.into_u32_array(),
                    upstream_fragment_ids,
                    actor_template: PbStreamActor {
                        nodes: Some(stream_node.to_protobuf()),
                        actor_id,
                        fragment_id: fragment_id as _,
                        dispatcher,
                        upstream_actor_id: vec![],
                        vnode_bitmap: vnode_bitmap.map(|b| b.to_protobuf()),
                        mview_definition: job_definition.to_owned(),
                        expr_context: expr_contexts
                            .get(&actor_id)
                            .cloned()
                            .map(|expr_context| expr_context.to_protobuf()),
                    },
                    actors,
                };

                fragment_map.insert(fragment_id as _, fragment);

                fragment_to_table.insert(fragment_id as _, TableId::from(job_id as u32));

                fragment_state.insert(
                    fragment_id,
                    table_fragments::PbState::from(related_job.job_status),
                );
            }
            Ok(())
        }
        let fragment_ids = reschedule.keys().map(|id| *id as _).collect();

        fulfill_index_by_fragment_ids(
            &mut actor_map,
            &mut fragment_map,
            &mut actor_status,
            &mut fragment_state,
            &mut fragment_to_table,
            &self.metadata_manager,
            fragment_ids,
        )
        .await?;

        // NoShuffle relation index
        let mut no_shuffle_source_fragment_ids = HashSet::new();
        let mut no_shuffle_target_fragment_ids = HashSet::new();

        Self::build_no_shuffle_relation_index(
            &actor_map,
            &mut no_shuffle_source_fragment_ids,
            &mut no_shuffle_target_fragment_ids,
        );

        if options.resolve_no_shuffle_upstream {
            let original_reschedule_keys = reschedule.keys().cloned().collect();

            Self::resolve_no_shuffle_upstream_fragments(
                reschedule,
                &fragment_map,
                &no_shuffle_source_fragment_ids,
                &no_shuffle_target_fragment_ids,
            )?;

            if !table_parallelisms.is_empty() {
                // We need to reiterate through the NO_SHUFFLE dependencies in order to ascertain which downstream table the custom modifications of the table have been propagated from.
                Self::resolve_no_shuffle_upstream_tables(
                    original_reschedule_keys,
                    &fragment_map,
                    &no_shuffle_source_fragment_ids,
                    &no_shuffle_target_fragment_ids,
                    &fragment_to_table,
                    table_parallelisms,
                )?;
            }
        }

        let mut fragment_dispatcher_map = HashMap::new();
        Self::build_fragment_dispatcher_index(&actor_map, &mut fragment_dispatcher_map);

        // Then, we collect all available upstreams
        let mut upstream_dispatchers: HashMap<
            ActorId,
            Vec<(FragmentId, DispatcherId, DispatcherType)>,
        > = HashMap::new();
        for stream_actor in actor_map.values() {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    upstream_dispatchers
                        .entry(*downstream_actor_id as ActorId)
                        .or_default()
                        .push((
                            stream_actor.fragment_id as FragmentId,
                            dispatcher.dispatcher_id as DispatcherId,
                            dispatcher.r#type(),
                        ));
                }
            }
        }

        let mut stream_source_fragment_ids = HashSet::new();
        let mut stream_source_backfill_fragment_ids = HashMap::new();
        let mut no_shuffle_reschedule = HashMap::new();
        for (fragment_id, WorkerReschedule { worker_actor_diff }) in &*reschedule {
            let fragment = fragment_map
                .get(fragment_id)
                .ok_or_else(|| anyhow!("fragment {fragment_id} does not exist"))?;

            // Check if the rescheduling is supported.
            match fragment_state[fragment_id] {
                table_fragments::State::Unspecified => unreachable!(),
                state @ table_fragments::State::Initial => {
                    bail!(
                        "the materialized view of fragment {fragment_id} is in state {}",
                        state.as_str_name()
                    )
                }
                state @ table_fragments::State::Creating => {
                    let stream_node = fragment
                        .actor_template
                        .nodes
                        .as_ref()
                        .expect("empty nodes in fragment actor template");

                    let mut is_reschedulable = true;
                    visit_stream_node_cont(stream_node, |body| {
                        if let Some(NodeBody::StreamScan(node)) = &body.node_body {
                            if !node.stream_scan_type().is_reschedulable() {
                                is_reschedulable = false;

                                // fail fast
                                return false;
                            }

                            // continue visiting
                            return true;
                        }

                        // continue visiting
                        true
                    });

                    if !is_reschedulable {
                        bail!(
                            "the materialized view of fragment {fragment_id} is in state {}",
                            state.as_str_name()
                        )
                    }
                }
                table_fragments::State::Created => {}
            }

            if no_shuffle_target_fragment_ids.contains(fragment_id) {
                bail!("rescheduling NoShuffle downstream fragment (maybe Chain fragment) is forbidden, please use NoShuffle upstream fragment (like Materialized fragment) to scale");
            }

            // For the relation of NoShuffle (e.g. Materialize and Chain), we need a special
            // treatment because the upstream and downstream of NoShuffle are always 1-1
            // correspondence, so we need to clone the reschedule plan to the downstream of all
            // cascading relations.
            if no_shuffle_source_fragment_ids.contains(fragment_id) {
                // This fragment is a NoShuffle's upstream.
                let mut queue: VecDeque<_> = fragment_dispatcher_map
                    .get(fragment_id)
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect();

                while let Some(downstream_id) = queue.pop_front() {
                    if !no_shuffle_target_fragment_ids.contains(&downstream_id) {
                        continue;
                    }

                    if let Some(downstream_fragments) = fragment_dispatcher_map.get(&downstream_id)
                    {
                        let no_shuffle_downstreams = downstream_fragments
                            .iter()
                            .filter(|(_, ty)| **ty == DispatcherType::NoShuffle)
                            .map(|(fragment_id, _)| fragment_id);

                        queue.extend(no_shuffle_downstreams.copied());
                    }

                    no_shuffle_reschedule.insert(
                        downstream_id,
                        WorkerReschedule {
                            worker_actor_diff: worker_actor_diff.clone(),
                        },
                    );
                }
            }

            if (fragment.get_fragment_type_mask() & FragmentTypeFlag::Source as u32) != 0 {
                let stream_node = fragment.actor_template.nodes.as_ref().unwrap();
                if stream_node.find_stream_source().is_some() {
                    stream_source_fragment_ids.insert(*fragment_id);
                }
            }

            // Check if the reschedule plan is valid.
            let current_worker_ids = fragment
                .actors
                .iter()
                .map(|a| actor_status.get(&a.actor_id).cloned().unwrap())
                .collect::<HashSet<_>>();

            for (removed, change) in worker_actor_diff {
                if !current_worker_ids.contains(removed) && change.is_negative() {
                    bail!(
                        "no actor on the worker {} of fragment {}",
                        removed,
                        fragment_id
                    );
                }
            }

            let added_actor_count: usize = worker_actor_diff
                .values()
                .filter(|change| change.is_positive())
                .cloned()
                .map(|change| change as usize)
                .sum();

            let removed_actor_count: usize = worker_actor_diff
                .values()
                .filter(|change| change.is_positive())
                .cloned()
                .map(|v| v.unsigned_abs())
                .sum();

            match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    if fragment.actors.len() + added_actor_count <= removed_actor_count {
                        bail!("can't remove all actors from fragment {}", fragment_id);
                    }
                }
                FragmentDistributionType::Single => {
                    if added_actor_count != removed_actor_count {
                        bail!("single distribution fragment only support migration");
                    }
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            }
        }

        if !no_shuffle_reschedule.is_empty() {
            tracing::info!(
                "reschedule plan rewritten with NoShuffle reschedule {:?}",
                no_shuffle_reschedule
            );

            for noshuffle_downstream in no_shuffle_reschedule.keys() {
                let fragment = fragment_map.get(noshuffle_downstream).unwrap();
                // SourceScan is always a NoShuffle downstream, rescheduled together with the upstream Source.
                if (fragment.get_fragment_type_mask() & FragmentTypeFlag::SourceScan as u32) != 0 {
                    let stream_node = fragment.actor_template.nodes.as_ref().unwrap();
                    if let Some((_source_id, upstream_source_fragment_id)) =
                        stream_node.find_source_backfill()
                    {
                        stream_source_backfill_fragment_ids
                            .insert(fragment.fragment_id, upstream_source_fragment_id);
                    }
                }
            }
        }

        // Modifications for NoShuffle downstream.
        reschedule.extend(no_shuffle_reschedule.into_iter());

        Ok(RescheduleContext {
            actor_map,
            actor_status,
            fragment_map,
            upstream_dispatchers,
            stream_source_fragment_ids,
            stream_source_backfill_fragment_ids,
            no_shuffle_target_fragment_ids,
            no_shuffle_source_fragment_ids,
            fragment_dispatcher_map,
        })
    }

    /// From the high-level [`WorkerReschedule`] to the low-level reschedule plan [`Reschedule`].
    ///
    /// Returns `(reschedule_fragment, applied_reschedules)`
    /// - `reschedule_fragment`: the generated reschedule plan
    /// - `applied_reschedules`: the changes that need to be updated to the meta store (`pre_apply_reschedules`, only for V1).
    ///
    /// In [normal process of scaling](`GlobalStreamManager::reschedule_actors`), we use the returned values to
    /// build a [`Command::RescheduleFragment`], which will then flows through the barrier mechanism to perform scaling.
    /// Meta store is updated after the barrier is collected.
    ///
    /// During recovery, we don't need the barrier mechanism, and can directly use the returned values to update meta.
    pub(crate) async fn analyze_reschedule_plan(
        &self,
        mut reschedules: HashMap<FragmentId, WorkerReschedule>,
        options: RescheduleOptions,
        table_parallelisms: &mut HashMap<TableId, TableParallelism>,
    ) -> MetaResult<HashMap<FragmentId, Reschedule>> {
        tracing::debug!("build_reschedule_context, reschedules: {:#?}", reschedules);
        let ctx = self
            .build_reschedule_context(&mut reschedules, options, table_parallelisms)
            .await?;
        tracing::debug!("reschedule context: {:#?}", ctx);
        let reschedules = reschedules;

        // Here, the plan for both upstream and downstream of the NO_SHUFFLE Fragment should already have been populated.

        // Index of actors to create/remove
        // Fragment Id => ( Actor Id => Worker Id )
        let (fragment_actors_to_remove, fragment_actors_to_create) =
            self.arrange_reschedules(&reschedules, &ctx)?;

        let mut fragment_actor_bitmap = HashMap::new();
        for fragment_id in reschedules.keys() {
            if ctx.no_shuffle_target_fragment_ids.contains(fragment_id) {
                // skipping chain fragment, we need to clone the upstream materialize fragment's
                // mapping later
                continue;
            }

            let actors_to_create = fragment_actors_to_create
                .get(fragment_id)
                .map(|map| map.iter().map(|(actor_id, _)| *actor_id).collect())
                .unwrap_or_default();

            let actors_to_remove = fragment_actors_to_remove
                .get(fragment_id)
                .map(|map| map.iter().map(|(actor_id, _)| *actor_id).collect())
                .unwrap_or_default();

            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            match fragment.distribution_type() {
                FragmentDistributionType::Single => {
                    // Skip re-balancing action for single distribution (always None)
                    fragment_actor_bitmap
                        .insert(fragment.fragment_id as FragmentId, Default::default());
                }
                FragmentDistributionType::Hash => {
                    let actor_vnode = rebalance_actor_vnode(
                        &fragment.actors,
                        &actors_to_remove,
                        &actors_to_create,
                    );

                    fragment_actor_bitmap.insert(fragment.fragment_id as FragmentId, actor_vnode);
                }

                FragmentDistributionType::Unspecified => unreachable!(),
            }
        }

        // Index for fragment -> { actor -> worker_id } after reschedule.
        // Since we need to organize the upstream and downstream relationships of NoShuffle,
        // we need to organize the actor distribution after a scaling.
        let mut fragment_actors_after_reschedule = HashMap::with_capacity(reschedules.len());
        for fragment_id in reschedules.keys() {
            let fragment = ctx.fragment_map.get(fragment_id).unwrap();
            let mut new_actor_ids = BTreeMap::new();
            for actor in &fragment.actors {
                if let Some(actors_to_remove) = fragment_actors_to_remove.get(fragment_id) {
                    if actors_to_remove.contains_key(&actor.actor_id) {
                        continue;
                    }
                }
                let worker_id = ctx.actor_id_to_worker_id(&actor.actor_id)?;
                new_actor_ids.insert(actor.actor_id as ActorId, worker_id);
            }

            if let Some(actors_to_create) = fragment_actors_to_create.get(fragment_id) {
                for (actor_id, worker_id) in actors_to_create {
                    new_actor_ids.insert(*actor_id, *worker_id);
                }
            }

            assert!(
                !new_actor_ids.is_empty(),
                "should be at least one actor in fragment {} after rescheduling",
                fragment_id
            );

            fragment_actors_after_reschedule.insert(*fragment_id, new_actor_ids);
        }

        let fragment_actors_after_reschedule = fragment_actors_after_reschedule;

        // In order to maintain consistency with the original structure, the upstream and downstream
        // actors of NoShuffle need to be in the same worker slot and hold the same virtual nodes,
        // so for the actors after the upstream re-balancing, since we have sorted the actors of the same fragment by id on all workers,
        // we can identify the corresponding upstream actor with NO_SHUFFLE.
        // NOTE: There should be more asserts here to ensure correctness.
        fn arrange_no_shuffle_relation(
            ctx: &RescheduleContext,
            fragment_id: &FragmentId,
            upstream_fragment_id: &FragmentId,
            fragment_actors_after_reschedule: &HashMap<FragmentId, BTreeMap<ActorId, WorkerId>>,
            actor_group_map: &mut HashMap<ActorId, (FragmentId, ActorId)>,
            fragment_updated_bitmap: &mut HashMap<FragmentId, HashMap<ActorId, Bitmap>>,
            no_shuffle_upstream_actor_map: &mut HashMap<ActorId, HashMap<FragmentId, ActorId>>,
            no_shuffle_downstream_actors_map: &mut HashMap<ActorId, HashMap<FragmentId, ActorId>>,
        ) {
            if !ctx.no_shuffle_target_fragment_ids.contains(fragment_id) {
                return;
            }

            let fragment = &ctx.fragment_map[fragment_id];

            let upstream_fragment = &ctx.fragment_map[upstream_fragment_id];

            // build actor group map
            for upstream_actor in &upstream_fragment.actors {
                for dispatcher in &upstream_actor.dispatcher {
                    if let DispatcherType::NoShuffle = dispatcher.get_type().unwrap() {
                        let downstream_actor_id =
                            *dispatcher.downstream_actor_id.iter().exactly_one().unwrap();

                        // upstream is root
                        if !ctx
                            .no_shuffle_target_fragment_ids
                            .contains(upstream_fragment_id)
                        {
                            actor_group_map.insert(
                                upstream_actor.actor_id,
                                (upstream_fragment.fragment_id, upstream_actor.actor_id),
                            );
                            actor_group_map.insert(
                                downstream_actor_id,
                                (upstream_fragment.fragment_id, upstream_actor.actor_id),
                            );
                        } else {
                            let root_actor_id = actor_group_map[&upstream_actor.actor_id];

                            actor_group_map.insert(downstream_actor_id, root_actor_id);
                        }
                    }
                }
            }

            // If the upstream is a Singleton Fragment, there will be no Bitmap changes
            let upstream_fragment_bitmap = fragment_updated_bitmap
                .get(upstream_fragment_id)
                .cloned()
                .unwrap_or_default();

            // Question: Is it possible to have Hash Distribution Fragment but the Actor's bitmap remains unchanged?
            if upstream_fragment.distribution_type() == FragmentDistributionType::Single {
                assert!(
                    upstream_fragment_bitmap.is_empty(),
                    "single fragment should have no bitmap updates"
                );
            }

            let upstream_fragment_actor_map = fragment_actors_after_reschedule
                .get(upstream_fragment_id)
                .cloned()
                .unwrap();

            let fragment_actor_map = fragment_actors_after_reschedule
                .get(fragment_id)
                .cloned()
                .unwrap();

            let mut worker_reverse_index: HashMap<WorkerId, BTreeSet<_>> = HashMap::new();

            // first, find existing actor bitmap, copy them
            let mut fragment_bitmap = HashMap::new();

            for (actor_id, worker_id) in &fragment_actor_map {
                if let Some((root_fragment, root_actor_id)) = actor_group_map.get(actor_id) {
                    let root_bitmap = fragment_updated_bitmap
                        .get(root_fragment)
                        .expect("root fragment bitmap not found")
                        .get(root_actor_id)
                        .cloned()
                        .expect("root actor bitmap not found");

                    // Copy the bitmap
                    fragment_bitmap.insert(*actor_id, root_bitmap);

                    no_shuffle_upstream_actor_map
                        .entry(*actor_id as ActorId)
                        .or_default()
                        .insert(*upstream_fragment_id, *root_actor_id);
                    no_shuffle_downstream_actors_map
                        .entry(*root_actor_id)
                        .or_default()
                        .insert(*fragment_id, *actor_id);
                } else {
                    worker_reverse_index
                        .entry(*worker_id)
                        .or_default()
                        .insert(*actor_id);
                }
            }

            let mut upstream_worker_reverse_index: HashMap<WorkerId, BTreeSet<_>> = HashMap::new();

            for (actor_id, worker_id) in &upstream_fragment_actor_map {
                if !actor_group_map.contains_key(actor_id) {
                    upstream_worker_reverse_index
                        .entry(*worker_id)
                        .or_default()
                        .insert(*actor_id);
                }
            }

            // then, find the rest of the actors and copy the bitmap
            for (worker_id, actor_ids) in worker_reverse_index {
                let upstream_actor_ids = upstream_worker_reverse_index
                    .get(&worker_id)
                    .unwrap()
                    .clone();

                assert_eq!(actor_ids.len(), upstream_actor_ids.len());

                for (actor_id, upstream_actor_id) in actor_ids
                    .into_iter()
                    .zip_eq_debug(upstream_actor_ids.into_iter())
                {
                    match upstream_fragment_bitmap.get(&upstream_actor_id).cloned() {
                        None => {
                            // single fragment should have no bitmap updates (same as upstream)
                            assert_eq!(
                                upstream_fragment.distribution_type(),
                                FragmentDistributionType::Single
                            );
                        }
                        Some(bitmap) => {
                            // Copy the bitmap
                            fragment_bitmap.insert(actor_id, bitmap);
                        }
                    }

                    no_shuffle_upstream_actor_map
                        .entry(actor_id as ActorId)
                        .or_default()
                        .insert(*upstream_fragment_id, upstream_actor_id);
                    no_shuffle_downstream_actors_map
                        .entry(upstream_actor_id)
                        .or_default()
                        .insert(*fragment_id, actor_id);
                }
            }

            match fragment.distribution_type() {
                FragmentDistributionType::Hash => {}
                FragmentDistributionType::Single => {
                    // single distribution should update nothing
                    assert!(fragment_bitmap.is_empty());
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            }

            if let Err(e) = fragment_updated_bitmap.try_insert(*fragment_id, fragment_bitmap) {
                assert_eq!(
                    e.entry.get(),
                    &e.value,
                    "bitmaps derived from different no-shuffle upstreams mismatch"
                );
            }

            // Visit downstream fragments recursively.
            if let Some(downstream_fragments) = ctx.fragment_dispatcher_map.get(fragment_id) {
                let no_shuffle_downstreams = downstream_fragments
                    .iter()
                    .filter(|(_, ty)| **ty == DispatcherType::NoShuffle)
                    .map(|(fragment_id, _)| fragment_id);

                for downstream_fragment_id in no_shuffle_downstreams {
                    arrange_no_shuffle_relation(
                        ctx,
                        downstream_fragment_id,
                        fragment_id,
                        fragment_actors_after_reschedule,
                        actor_group_map,
                        fragment_updated_bitmap,
                        no_shuffle_upstream_actor_map,
                        no_shuffle_downstream_actors_map,
                    );
                }
            }
        }

        let mut no_shuffle_upstream_actor_map = HashMap::new();
        let mut no_shuffle_downstream_actors_map = HashMap::new();
        let mut actor_group_map = HashMap::new();
        // For all roots in the upstream and downstream dependency trees of NoShuffle, recursively
        // find all correspondences
        for fragment_id in reschedules.keys() {
            if ctx.no_shuffle_source_fragment_ids.contains(fragment_id)
                && !ctx.no_shuffle_target_fragment_ids.contains(fragment_id)
            {
                if let Some(downstream_fragments) = ctx.fragment_dispatcher_map.get(fragment_id) {
                    for downstream_fragment_id in downstream_fragments.keys() {
                        arrange_no_shuffle_relation(
                            &ctx,
                            downstream_fragment_id,
                            fragment_id,
                            &fragment_actors_after_reschedule,
                            &mut actor_group_map,
                            &mut fragment_actor_bitmap,
                            &mut no_shuffle_upstream_actor_map,
                            &mut no_shuffle_downstream_actors_map,
                        );
                    }
                }
            }
        }

        tracing::debug!("actor group map {:?}", actor_group_map);

        let mut new_created_actors = HashMap::new();
        for fragment_id in reschedules.keys() {
            let actors_to_create = fragment_actors_to_create
                .get(fragment_id)
                .cloned()
                .unwrap_or_default();

            let fragment = &ctx.fragment_map[fragment_id];

            assert!(!fragment.actors.is_empty());

            for (actor_to_create, sample_actor) in actors_to_create
                .iter()
                .zip_eq_debug(repeat(&fragment.actor_template).take(actors_to_create.len()))
            {
                let new_actor_id = actor_to_create.0;
                let mut new_actor = sample_actor.clone();
                let mut new_actor_upstream = ActorUpstreams::new();

                // This should be assigned before the `modify_actor_upstream_and_downstream` call,
                // because we need to use the new actor id to find the upstream and
                // downstream in the NoShuffle relationship
                new_actor.actor_id = *new_actor_id;

                Self::modify_actor_upstream_and_downstream(
                    &ctx,
                    &fragment_actors_to_remove,
                    &fragment_actors_to_create,
                    &fragment_actor_bitmap,
                    &no_shuffle_upstream_actor_map,
                    &no_shuffle_downstream_actors_map,
                    (&mut new_actor, &mut new_actor_upstream),
                )?;

                if let Some(bitmap) = fragment_actor_bitmap
                    .get(fragment_id)
                    .and_then(|actor_bitmaps| actor_bitmaps.get(new_actor_id))
                {
                    new_actor.vnode_bitmap = Some(bitmap.to_protobuf());
                }

                new_created_actors.insert(*new_actor_id, (new_actor, new_actor_upstream));
            }
        }

        // For stream source & source backfill fragments, we need to reallocate the splits.
        // Because we are in the Pause state, so it's no problem to reallocate
        let mut fragment_actor_splits = HashMap::new();
        for fragment_id in reschedules.keys() {
            let actors_after_reschedule = &fragment_actors_after_reschedule[fragment_id];

            if ctx.stream_source_fragment_ids.contains(fragment_id) {
                let fragment = &ctx.fragment_map[fragment_id];

                let prev_actor_ids = fragment
                    .actors
                    .iter()
                    .map(|actor| actor.actor_id)
                    .collect_vec();

                let curr_actor_ids = actors_after_reschedule.keys().cloned().collect_vec();

                let actor_splits = self
                    .source_manager
                    .migrate_splits_for_source_actors(
                        *fragment_id,
                        &prev_actor_ids,
                        &curr_actor_ids,
                    )
                    .await?;

                tracing::debug!(
                    "source actor splits: {:?}, fragment_id: {}",
                    actor_splits,
                    fragment_id
                );
                fragment_actor_splits.insert(*fragment_id, actor_splits);
            }
        }
        // We use 2 iterations to make sure source actors are migrated first, and then align backfill actors
        if !ctx.stream_source_backfill_fragment_ids.is_empty() {
            for fragment_id in reschedules.keys() {
                let actors_after_reschedule = &fragment_actors_after_reschedule[fragment_id];

                if let Some(upstream_source_fragment_id) =
                    ctx.stream_source_backfill_fragment_ids.get(fragment_id)
                {
                    let curr_actor_ids = actors_after_reschedule.keys().cloned().collect_vec();

                    let actor_splits = self.source_manager.migrate_splits_for_backfill_actors(
                        *fragment_id,
                        *upstream_source_fragment_id,
                        &curr_actor_ids,
                        &fragment_actor_splits,
                        &no_shuffle_upstream_actor_map,
                    )?;
                    tracing::debug!(
                        "source backfill actor splits: {:?}, fragment_id: {}",
                        actor_splits,
                        fragment_id
                    );
                    fragment_actor_splits.insert(*fragment_id, actor_splits);
                }
            }
        }

        // Generate fragment reschedule plan
        let mut reschedule_fragment: HashMap<FragmentId, Reschedule> =
            HashMap::with_capacity(reschedules.len());

        for (fragment_id, _) in reschedules {
            let mut actors_to_create: HashMap<_, Vec<_>> = HashMap::new();

            if let Some(actor_worker_maps) = fragment_actors_to_create.get(&fragment_id).cloned() {
                for (actor_id, worker_id) in actor_worker_maps {
                    actors_to_create
                        .entry(worker_id)
                        .or_default()
                        .push(actor_id);
                }
            }

            let actors_to_remove = fragment_actors_to_remove
                .get(&fragment_id)
                .cloned()
                .unwrap_or_default()
                .into_keys()
                .collect();

            let actors_after_reschedule = &fragment_actors_after_reschedule[&fragment_id];

            assert!(!actors_after_reschedule.is_empty());

            let fragment = &ctx.fragment_map[&fragment_id];

            let in_degree_types: HashSet<_> = fragment
                .upstream_fragment_ids
                .iter()
                .flat_map(|upstream_fragment_id| {
                    ctx.fragment_dispatcher_map
                        .get(upstream_fragment_id)
                        .and_then(|dispatcher_map| {
                            dispatcher_map.get(&fragment.fragment_id).cloned()
                        })
                })
                .collect();

            let upstream_dispatcher_mapping = match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    if !in_degree_types.contains(&DispatcherType::Hash) {
                        None
                    } else {
                        // Changes of the bitmap must occur in the case of HashDistribution
                        Some(ActorMapping::from_bitmaps(
                            &fragment_actor_bitmap[&fragment_id],
                        ))
                    }
                }

                FragmentDistributionType::Single => {
                    assert!(fragment_actor_bitmap.get(&fragment_id).unwrap().is_empty());
                    None
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            };

            let mut upstream_fragment_dispatcher_set = BTreeSet::new();

            for actor in &fragment.actors {
                if let Some(upstream_actor_tuples) = ctx.upstream_dispatchers.get(&actor.actor_id) {
                    for (upstream_fragment_id, upstream_dispatcher_id, upstream_dispatcher_type) in
                        upstream_actor_tuples
                    {
                        match upstream_dispatcher_type {
                            DispatcherType::Unspecified => unreachable!(),
                            DispatcherType::NoShuffle => {}
                            _ => {
                                upstream_fragment_dispatcher_set
                                    .insert((*upstream_fragment_id, *upstream_dispatcher_id));
                            }
                        }
                    }
                }
            }

            let downstream_fragment_ids = if let Some(downstream_fragments) =
                ctx.fragment_dispatcher_map.get(&fragment_id)
            {
                // Skip fragments' no-shuffle downstream, as there's no need to update the merger
                // (receiver) of a no-shuffle downstream
                downstream_fragments
                    .iter()
                    .filter(|(_, dispatcher_type)| *dispatcher_type != &DispatcherType::NoShuffle)
                    .map(|(fragment_id, _)| *fragment_id)
                    .collect_vec()
            } else {
                vec![]
            };

            let vnode_bitmap_updates = match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    let mut vnode_bitmap_updates =
                        fragment_actor_bitmap.remove(&fragment_id).unwrap();

                    // We need to keep the bitmaps from changed actors only,
                    // otherwise the barrier will become very large with many actors
                    for actor_id in actors_after_reschedule.keys() {
                        assert!(vnode_bitmap_updates.contains_key(actor_id));

                        // retain actor
                        if let Some(actor) = ctx.actor_map.get(actor_id) {
                            let bitmap = vnode_bitmap_updates.get(actor_id).unwrap();

                            if let Some(prev_bitmap) = actor.vnode_bitmap.as_ref() {
                                if prev_bitmap.eq(bitmap) {
                                    vnode_bitmap_updates.remove(actor_id);
                                }
                            }
                        }
                    }

                    vnode_bitmap_updates
                }
                FragmentDistributionType::Single => HashMap::new(),
                FragmentDistributionType::Unspecified => unreachable!(),
            };

            let upstream_fragment_dispatcher_ids =
                upstream_fragment_dispatcher_set.into_iter().collect_vec();

            let actor_splits = fragment_actor_splits
                .get(&fragment_id)
                .cloned()
                .unwrap_or_default();

            reschedule_fragment.insert(
                fragment_id,
                Reschedule {
                    added_actors: actors_to_create,
                    removed_actors: actors_to_remove,
                    vnode_bitmap_updates,
                    upstream_fragment_dispatcher_ids,
                    upstream_dispatcher_mapping,
                    downstream_fragment_ids,
                    actor_splits,
                    newly_created_actors: vec![],
                },
            );
        }

        let mut fragment_created_actors = HashMap::new();
        for (fragment_id, actors_to_create) in &fragment_actors_to_create {
            let mut created_actors = HashMap::new();
            for (actor_id, worker_id) in actors_to_create {
                let (actor, actor_upstreams) = new_created_actors.get(actor_id).cloned().unwrap();
                created_actors.insert(
                    *actor_id,
                    (
                        (actor, actor_upstreams),
                        ActorStatus {
                            location: PbActorLocation::from_worker(*worker_id as _),
                            state: ActorState::Inactive as i32,
                        },
                    ),
                );
            }

            fragment_created_actors.insert(*fragment_id, created_actors);
        }

        for (fragment_id, to_create) in &fragment_created_actors {
            let reschedule = reschedule_fragment.get_mut(fragment_id).unwrap();
            reschedule.newly_created_actors = to_create.values().cloned().collect();
        }
        tracing::debug!("analyze_reschedule_plan result: {:#?}", reschedule_fragment);

        Ok(reschedule_fragment)
    }

    #[expect(clippy::type_complexity)]
    fn arrange_reschedules(
        &self,
        reschedule: &HashMap<FragmentId, WorkerReschedule>,
        ctx: &RescheduleContext,
    ) -> MetaResult<(
        HashMap<FragmentId, BTreeMap<ActorId, WorkerId>>,
        HashMap<FragmentId, BTreeMap<ActorId, WorkerId>>,
    )> {
        let mut fragment_actors_to_remove = HashMap::with_capacity(reschedule.len());
        let mut fragment_actors_to_create = HashMap::with_capacity(reschedule.len());

        for (fragment_id, WorkerReschedule { worker_actor_diff }) in reschedule {
            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            // Actor Id => Worker Id
            let mut actors_to_remove = BTreeMap::new();
            let mut actors_to_create = BTreeMap::new();

            // NOTE(important): The value needs to be a BTreeSet to ensure that the actors on the worker are sorted in ascending order.
            let mut worker_to_actors = HashMap::new();

            for actor in &fragment.actors {
                let worker_id = ctx.actor_id_to_worker_id(&actor.actor_id).unwrap();
                worker_to_actors
                    .entry(worker_id)
                    .or_insert(BTreeSet::new())
                    .insert(actor.actor_id as ActorId);
            }

            let decreased_actor_count = worker_actor_diff
                .iter()
                .filter(|(_, change)| change.is_negative())
                .map(|(worker_id, change)| (worker_id, change.unsigned_abs()));

            for (worker_id, n) in decreased_actor_count {
                if let Some(actor_ids) = worker_to_actors.get(worker_id) {
                    if actor_ids.len() < n {
                        bail!("plan illegal, for fragment {}, worker {} only has {} actors, but needs to reduce {}",fragment_id, worker_id, actor_ids.len(), n);
                    }

                    let removed_actors: Vec<_> = actor_ids
                        .iter()
                        .skip(actor_ids.len().saturating_sub(n))
                        .cloned()
                        .collect();

                    for actor in removed_actors {
                        actors_to_remove.insert(actor, *worker_id);
                    }
                }
            }

            let increased_actor_count = worker_actor_diff
                .iter()
                .filter(|(_, change)| change.is_positive());

            for (worker, n) in increased_actor_count {
                for _ in 0..*n {
                    let id = self
                        .env
                        .id_gen_manager()
                        .generate_interval::<{ IdCategory::Actor }>(1)
                        as ActorId;
                    actors_to_create.insert(id, *worker);
                }
            }

            if !actors_to_remove.is_empty() {
                fragment_actors_to_remove.insert(*fragment_id as FragmentId, actors_to_remove);
            }

            if !actors_to_create.is_empty() {
                fragment_actors_to_create.insert(*fragment_id as FragmentId, actors_to_create);
            }
        }

        // sanity checking
        for actors_to_remove in fragment_actors_to_remove.values() {
            for actor_id in actors_to_remove.keys() {
                let actor = ctx.actor_map.get(actor_id).unwrap();
                for dispatcher in &actor.dispatcher {
                    if DispatcherType::NoShuffle == dispatcher.get_type().unwrap() {
                        let downstream_actor_id = dispatcher.downstream_actor_id.iter().exactly_one().expect("there should be only one downstream actor id in NO_SHUFFLE dispatcher");

                        let _should_exists = fragment_actors_to_remove
                            .get(&(dispatcher.dispatcher_id as FragmentId))
                            .expect("downstream fragment of NO_SHUFFLE relation should be in the removing map")
                            .get(downstream_actor_id)
                            .expect("downstream actor of NO_SHUFFLE relation should be in the removing map");
                    }
                }
            }
        }

        Ok((fragment_actors_to_remove, fragment_actors_to_create))
    }

    /// Modifies the upstream and downstream actors of the new created actor according to the
    /// overall changes, and is used to handle cascading updates
    fn modify_actor_upstream_and_downstream(
        ctx: &RescheduleContext,
        fragment_actors_to_remove: &HashMap<FragmentId, BTreeMap<ActorId, WorkerId>>,
        fragment_actors_to_create: &HashMap<FragmentId, BTreeMap<ActorId, WorkerId>>,
        fragment_actor_bitmap: &HashMap<FragmentId, HashMap<ActorId, Bitmap>>,
        no_shuffle_upstream_actor_map: &HashMap<ActorId, HashMap<FragmentId, ActorId>>,
        no_shuffle_downstream_actors_map: &HashMap<ActorId, HashMap<FragmentId, ActorId>>,
        (new_actor, actor_upstreams): (&mut StreamActor, &mut ActorUpstreams),
    ) -> MetaResult<()> {
        let fragment = &ctx.fragment_map[&new_actor.fragment_id];
        let mut applied_upstream_fragment_actor_ids = HashMap::new();

        for upstream_fragment_id in &fragment.upstream_fragment_ids {
            let upstream_dispatch_type = &ctx
                .fragment_dispatcher_map
                .get(upstream_fragment_id)
                .and_then(|map| map.get(&fragment.fragment_id))
                .unwrap();

            match upstream_dispatch_type {
                DispatcherType::Unspecified => unreachable!(),
                DispatcherType::Hash | DispatcherType::Broadcast | DispatcherType::Simple => {
                    let upstream_fragment = &ctx.fragment_map[upstream_fragment_id];
                    let mut upstream_actor_ids: HashSet<_> = upstream_fragment
                        .actors
                        .iter()
                        .map(|actor| actor.actor_id as ActorId)
                        .collect();

                    if let Some(upstream_actors_to_remove) =
                        fragment_actors_to_remove.get(upstream_fragment_id)
                    {
                        upstream_actor_ids
                            .retain(|actor_id| !upstream_actors_to_remove.contains_key(actor_id));
                    }

                    if let Some(upstream_actors_to_create) =
                        fragment_actors_to_create.get(upstream_fragment_id)
                    {
                        upstream_actor_ids.extend(upstream_actors_to_create.keys().cloned());
                    }

                    applied_upstream_fragment_actor_ids
                        .insert(*upstream_fragment_id as FragmentId, upstream_actor_ids);
                }
                DispatcherType::NoShuffle => {
                    let no_shuffle_upstream_actor_id = *no_shuffle_upstream_actor_map
                        .get(&new_actor.actor_id)
                        .and_then(|map| map.get(upstream_fragment_id))
                        .unwrap();

                    applied_upstream_fragment_actor_ids.insert(
                        *upstream_fragment_id as FragmentId,
                        HashSet::from_iter([no_shuffle_upstream_actor_id as ActorId]),
                    );
                }
            }
        }

        if let Some(node) = new_actor.nodes.as_mut() {
            visit_stream_node(node, |node_body| {
                if let NodeBody::Merge(s) = node_body {
                    let upstream_actor_ids = applied_upstream_fragment_actor_ids
                        .get(&s.upstream_fragment_id)
                        .cloned()
                        .unwrap();
                    actor_upstreams
                        .try_insert(s.upstream_fragment_id, upstream_actor_ids)
                        .expect("non-duplicate");
                }
            });
        }

        // Update downstream actor ids
        for dispatcher in &mut new_actor.dispatcher {
            let downstream_fragment_id = dispatcher
                .downstream_actor_id
                .iter()
                .filter_map(|actor_id| ctx.actor_map.get(actor_id).map(|actor| actor.fragment_id))
                .dedup()
                .exactly_one()
                .unwrap() as FragmentId;

            let downstream_fragment_actors_to_remove =
                fragment_actors_to_remove.get(&downstream_fragment_id);
            let downstream_fragment_actors_to_create =
                fragment_actors_to_create.get(&downstream_fragment_id);

            match dispatcher.r#type() {
                d @ (DispatcherType::Hash | DispatcherType::Simple | DispatcherType::Broadcast) => {
                    if let Some(downstream_actors_to_remove) = downstream_fragment_actors_to_remove
                    {
                        dispatcher
                            .downstream_actor_id
                            .retain(|id| !downstream_actors_to_remove.contains_key(id));
                    }

                    if let Some(downstream_actors_to_create) = downstream_fragment_actors_to_create
                    {
                        dispatcher
                            .downstream_actor_id
                            .extend(downstream_actors_to_create.keys().cloned())
                    }

                    // There should be still exactly one downstream actor
                    if d == DispatcherType::Simple {
                        assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                    }
                }
                DispatcherType::NoShuffle => {
                    assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                    let downstream_actor_id = no_shuffle_downstream_actors_map
                        .get(&new_actor.actor_id)
                        .and_then(|map| map.get(&downstream_fragment_id))
                        .unwrap();
                    dispatcher.downstream_actor_id = vec![*downstream_actor_id as ActorId];
                }
                DispatcherType::Unspecified => unreachable!(),
            }

            if let Some(mapping) = dispatcher.hash_mapping.as_mut() {
                if let Some(downstream_updated_bitmap) =
                    fragment_actor_bitmap.get(&downstream_fragment_id)
                {
                    // If downstream scale in/out
                    *mapping = ActorMapping::from_bitmaps(downstream_updated_bitmap).to_protobuf();
                }
            }
        }

        Ok(())
    }

    pub async fn post_apply_reschedule(
        &self,
        reschedules: &HashMap<FragmentId, Reschedule>,
        post_updates: &JobReschedulePostUpdates,
    ) -> MetaResult<()> {
        // Update fragment info after rescheduling in meta store.
        self.metadata_manager
            .post_apply_reschedules(reschedules.clone(), post_updates)
            .await?;

        // Update serving fragment info after rescheduling in meta store.
        if !reschedules.is_empty() {
            let workers = self
                .metadata_manager
                .list_active_serving_compute_nodes()
                .await?;
            let streaming_parallelisms = self
                .metadata_manager
                .running_fragment_parallelisms(Some(reschedules.keys().cloned().collect()))
                .await?;
            let serving_worker_slot_mapping = Arc::new(ServingVnodeMapping::default());
            let (upserted, failed) =
                serving_worker_slot_mapping.upsert(streaming_parallelisms, &workers);
            if !upserted.is_empty() {
                tracing::debug!(
                    "Update serving vnode mapping for fragments {:?}.",
                    upserted.keys()
                );
                self.env
                    .notification_manager()
                    .notify_frontend_without_version(
                        Operation::Update,
                        Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
                            mappings: to_fragment_worker_slot_mapping(&upserted),
                        }),
                    );
            }
            if !failed.is_empty() {
                tracing::debug!(
                    "Fail to update serving vnode mapping for fragments {:?}.",
                    failed
                );
                self.env
                    .notification_manager()
                    .notify_frontend_without_version(
                        Operation::Delete,
                        Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
                            mappings: to_deleted_fragment_worker_slot_mapping(&failed),
                        }),
                    );
            }
        }

        let mut stream_source_actor_splits = HashMap::new();
        let mut stream_source_dropped_actors = HashSet::new();

        // todo: handle adaptive splits
        for (fragment_id, reschedule) in reschedules {
            if !reschedule.actor_splits.is_empty() {
                stream_source_actor_splits
                    .insert(*fragment_id as FragmentId, reschedule.actor_splits.clone());
                stream_source_dropped_actors.extend(reschedule.removed_actors.clone());
            }
        }

        if !stream_source_actor_splits.is_empty() {
            self.source_manager
                .apply_source_change(SourceChange::Reschedule {
                    split_assignment: stream_source_actor_splits,
                    dropped_actors: stream_source_dropped_actors,
                })
                .await;
        }

        Ok(())
    }

    pub async fn generate_job_reschedule_plan(
        &self,
        policy: JobReschedulePolicy,
    ) -> MetaResult<JobReschedulePlan> {
        type VnodeCount = usize;

        let JobReschedulePolicy { targets } = policy;

        let workers = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?;

        // The `schedulable` field should eventually be replaced by resource groups like `unschedulable`
        let workers: HashMap<_, _> = workers
            .into_iter()
            .filter(|worker| worker.is_streaming_schedulable())
            .map(|worker| (worker.id, worker))
            .collect();

        #[derive(Debug)]
        struct JobUpdate {
            filtered_worker_ids: BTreeSet<WorkerId>,
            parallelism: TableParallelism,
        }

        let mut job_parallelism_updates = HashMap::new();

        let mut job_reschedule_post_updates = JobReschedulePostUpdates {
            parallelism_updates: Default::default(),
            resource_group_updates: Default::default(),
        };

        for (
            job_id,
            JobRescheduleTarget {
                parallelism: parallelism_update,
                resource_group: resource_group_update,
            },
        ) in &targets
        {
            let parallelism = match parallelism_update {
                JobParallelismTarget::Update(parallelism) => *parallelism,
                JobParallelismTarget::Refresh => {
                    let parallelism = self
                        .metadata_manager
                        .catalog_controller
                        .get_job_streaming_parallelisms(*job_id as _)
                        .await?;

                    parallelism.into()
                }
            };

            job_reschedule_post_updates
                .parallelism_updates
                .insert(TableId::from(*job_id), parallelism);

            let current_resource_group = match resource_group_update {
                JobResourceGroupTarget::Update(Some(specific_resource_group)) => {
                    job_reschedule_post_updates.resource_group_updates.insert(
                        *job_id as ObjectId,
                        Some(specific_resource_group.to_owned()),
                    );

                    specific_resource_group.to_owned()
                }
                JobResourceGroupTarget::Update(None) => {
                    let database_resource_group = self
                        .metadata_manager
                        .catalog_controller
                        .get_existing_job_database_resource_group(*job_id as _)
                        .await?;

                    job_reschedule_post_updates
                        .resource_group_updates
                        .insert(*job_id as ObjectId, None);
                    database_resource_group
                }
                JobResourceGroupTarget::Keep => {
                    self.metadata_manager
                        .catalog_controller
                        .get_existing_job_resource_group(*job_id as _)
                        .await?
                }
            };

            let filtered_worker_ids =
                filter_workers_by_resource_group(&workers, current_resource_group.as_str());

            if filtered_worker_ids.is_empty() {
                bail!("Cannot resize streaming_job {job_id} to empty worker set")
            }

            job_parallelism_updates.insert(
                *job_id,
                JobUpdate {
                    filtered_worker_ids,
                    parallelism,
                },
            );
        }

        // index for no shuffle relation
        let mut no_shuffle_source_fragment_ids = HashSet::new();
        let mut no_shuffle_target_fragment_ids = HashSet::new();

        // index for fragment_id -> (distribution_type, vnode_count)
        let mut fragment_distribution_map = HashMap::new();
        // index for actor -> worker id
        let mut actor_location = HashMap::new();
        // index for table_id -> [fragment_id]
        let mut table_fragment_id_map = HashMap::new();
        // index for fragment_id -> [actor_id]
        let mut fragment_actor_id_map = HashMap::new();

        async fn build_index(
            no_shuffle_source_fragment_ids: &mut HashSet<FragmentId>,
            no_shuffle_target_fragment_ids: &mut HashSet<FragmentId>,
            fragment_distribution_map: &mut HashMap<
                FragmentId,
                (FragmentDistributionType, VnodeCount),
            >,
            actor_location: &mut HashMap<ActorId, WorkerId>,
            table_fragment_id_map: &mut HashMap<u32, HashSet<FragmentId>>,
            fragment_actor_id_map: &mut HashMap<FragmentId, HashSet<u32>>,
            mgr: &MetadataManager,
            table_ids: Vec<ObjectId>,
        ) -> Result<(), MetaError> {
            let RescheduleWorkingSet {
                fragments,
                actors,
                actor_dispatchers: _actor_dispatchers,
                fragment_downstreams,
                fragment_upstreams: _fragment_upstreams,
                related_jobs: _related_jobs,
                job_resource_groups: _job_resource_groups,
            } = mgr
                .catalog_controller
                .resolve_working_set_for_reschedule_tables(table_ids)
                .await?;

            for (fragment_id, downstreams) in fragment_downstreams {
                for (downstream_fragment_id, dispatcher_type) in downstreams {
                    if let risingwave_meta_model::actor_dispatcher::DispatcherType::NoShuffle =
                        dispatcher_type
                    {
                        no_shuffle_source_fragment_ids.insert(fragment_id as FragmentId);
                        no_shuffle_target_fragment_ids.insert(downstream_fragment_id as FragmentId);
                    }
                }
            }

            for (fragment_id, fragment) in fragments {
                fragment_distribution_map.insert(
                    fragment_id as FragmentId,
                    (
                        FragmentDistributionType::from(fragment.distribution_type),
                        fragment.vnode_count as _,
                    ),
                );

                table_fragment_id_map
                    .entry(fragment.job_id as u32)
                    .or_default()
                    .insert(fragment_id as FragmentId);
            }

            for (actor_id, actor) in actors {
                actor_location.insert(actor_id as ActorId, actor.worker_id as WorkerId);
                fragment_actor_id_map
                    .entry(actor.fragment_id as FragmentId)
                    .or_default()
                    .insert(actor_id as ActorId);
            }

            Ok(())
        }

        let table_ids = targets.keys().map(|id| *id as ObjectId).collect();

        build_index(
            &mut no_shuffle_source_fragment_ids,
            &mut no_shuffle_target_fragment_ids,
            &mut fragment_distribution_map,
            &mut actor_location,
            &mut table_fragment_id_map,
            &mut fragment_actor_id_map,
            &self.metadata_manager,
            table_ids,
        )
        .await?;
        tracing::debug!(
            ?job_reschedule_post_updates,
            ?job_parallelism_updates,
            ?no_shuffle_source_fragment_ids,
            ?no_shuffle_target_fragment_ids,
            ?fragment_distribution_map,
            ?actor_location,
            ?table_fragment_id_map,
            ?fragment_actor_id_map,
            "generate_table_resize_plan, after build_index"
        );

        let mut target_plan = HashMap::new();

        for (
            table_id,
            JobUpdate {
                filtered_worker_ids,
                parallelism,
            },
        ) in job_parallelism_updates
        {
            let fragment_map = table_fragment_id_map.remove(&table_id).unwrap();

            let available_worker_slots = workers
                .iter()
                .filter(|(id, _)| filtered_worker_ids.contains(&(**id as WorkerId)))
                .map(|(_, worker)| (worker.id as WorkerId, worker.compute_node_parallelism()))
                .collect::<BTreeMap<_, _>>();

            for fragment_id in fragment_map {
                // Currently, all of our NO_SHUFFLE relation propagations are only transmitted from upstream to downstream.
                if no_shuffle_target_fragment_ids.contains(&fragment_id) {
                    continue;
                }

                let mut fragment_slots: BTreeMap<WorkerId, usize> = BTreeMap::new();

                for actor_id in &fragment_actor_id_map[&fragment_id] {
                    let worker_id = actor_location[actor_id];
                    *fragment_slots.entry(worker_id).or_default() += 1;
                }

                let available_slot_count: usize = available_worker_slots.values().cloned().sum();

                if available_slot_count == 0 {
                    bail!(
                        "No schedulable slots available for fragment {}",
                        fragment_id
                    );
                }

                let (dist, vnode_count) = fragment_distribution_map[&fragment_id];
                let max_parallelism = vnode_count;

                match dist {
                    FragmentDistributionType::Unspecified => unreachable!(),
                    FragmentDistributionType::Single => {
                        let (single_worker_id, should_be_one) = fragment_slots
                            .iter()
                            .exactly_one()
                            .expect("single fragment should have only one worker slot");

                        assert_eq!(*should_be_one, 1);

                        let units = schedule_units_for_slots(&available_worker_slots, 1, table_id)?;

                        let (chosen_target_worker_id, should_be_one) =
                            units.iter().exactly_one().ok().with_context(|| {
                                format!(
                                    "Cannot find a single target worker for fragment {fragment_id}"
                                )
                            })?;

                        assert_eq!(*should_be_one, 1);

                        if *chosen_target_worker_id == *single_worker_id {
                            tracing::debug!("single fragment {fragment_id} already on target worker {chosen_target_worker_id}");
                            continue;
                        }

                        target_plan.insert(
                            fragment_id,
                            WorkerReschedule {
                                worker_actor_diff: BTreeMap::from_iter(vec![
                                    (*chosen_target_worker_id, 1),
                                    (*single_worker_id, -1),
                                ]),
                            },
                        );
                    }
                    FragmentDistributionType::Hash => match parallelism {
                        TableParallelism::Adaptive => {
                            if available_slot_count > max_parallelism {
                                tracing::warn!("available parallelism for table {table_id} is larger than max parallelism, force limit to {max_parallelism}");
                                // force limit to `max_parallelism`
                                let target_worker_slots = schedule_units_for_slots(
                                    &available_worker_slots,
                                    max_parallelism,
                                    table_id,
                                )?;

                                target_plan.insert(
                                    fragment_id,
                                    Self::diff_worker_slot_changes(
                                        &fragment_slots,
                                        &target_worker_slots,
                                    ),
                                );
                            } else {
                                target_plan.insert(
                                    fragment_id,
                                    Self::diff_worker_slot_changes(
                                        &fragment_slots,
                                        &available_worker_slots,
                                    ),
                                );
                            }
                        }
                        TableParallelism::Fixed(mut n) => {
                            if n > max_parallelism {
                                tracing::warn!("specified parallelism {n} for table {table_id} is larger than max parallelism, force limit to {max_parallelism}");
                                n = max_parallelism
                            }

                            let target_worker_slots =
                                schedule_units_for_slots(&available_worker_slots, n, table_id)?;

                            target_plan.insert(
                                fragment_id,
                                Self::diff_worker_slot_changes(
                                    &fragment_slots,
                                    &target_worker_slots,
                                ),
                            );
                        }
                        TableParallelism::Custom => {
                            // skipping for custom
                        }
                    },
                }
            }
        }

        target_plan.retain(|_, plan| !plan.worker_actor_diff.is_empty());
        tracing::debug!(
            ?target_plan,
            "generate_table_resize_plan finished target_plan"
        );

        Ok(JobReschedulePlan {
            reschedules: target_plan,
            post_updates: job_reschedule_post_updates,
        })
    }

    fn diff_worker_slot_changes(
        fragment_worker_slots: &BTreeMap<WorkerId, usize>,
        target_worker_slots: &BTreeMap<WorkerId, usize>,
    ) -> WorkerReschedule {
        let mut increased_actor_count: BTreeMap<WorkerId, usize> = BTreeMap::new();
        let mut decreased_actor_count: BTreeMap<WorkerId, usize> = BTreeMap::new();

        for (&worker_id, &target_slots) in target_worker_slots {
            let &current_slots = fragment_worker_slots.get(&worker_id).unwrap_or(&0);

            if target_slots > current_slots {
                increased_actor_count.insert(worker_id, target_slots - current_slots);
            }
        }

        for (&worker_id, &current_slots) in fragment_worker_slots {
            let &target_slots = target_worker_slots.get(&worker_id).unwrap_or(&0);

            if current_slots > target_slots {
                decreased_actor_count.insert(worker_id, current_slots - target_slots);
            }
        }

        let worker_ids: HashSet<_> = increased_actor_count
            .keys()
            .chain(decreased_actor_count.keys())
            .cloned()
            .collect();

        let mut worker_actor_diff = BTreeMap::new();

        for worker_id in worker_ids {
            let increased = increased_actor_count.remove(&worker_id).unwrap_or(0) as isize;
            let decreased = decreased_actor_count.remove(&worker_id).unwrap_or(0) as isize;
            let change = increased - decreased;

            assert_ne!(change, 0);

            worker_actor_diff.insert(worker_id, change);
        }

        WorkerReschedule { worker_actor_diff }
    }

    fn build_no_shuffle_relation_index(
        actor_map: &HashMap<ActorId, CustomActorInfo>,
        no_shuffle_source_fragment_ids: &mut HashSet<FragmentId>,
        no_shuffle_target_fragment_ids: &mut HashSet<FragmentId>,
    ) {
        let mut fragment_cache = HashSet::new();
        for actor in actor_map.values() {
            if fragment_cache.contains(&actor.fragment_id) {
                continue;
            }

            for dispatcher in &actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    if let Some(downstream_actor) = actor_map.get(downstream_actor_id) {
                        // Checking for no shuffle dispatchers
                        if dispatcher.r#type() == DispatcherType::NoShuffle {
                            no_shuffle_source_fragment_ids.insert(actor.fragment_id as FragmentId);
                            no_shuffle_target_fragment_ids
                                .insert(downstream_actor.fragment_id as FragmentId);
                        }
                    }
                }
            }

            fragment_cache.insert(actor.fragment_id);
        }
    }

    fn build_fragment_dispatcher_index(
        actor_map: &HashMap<ActorId, CustomActorInfo>,
        fragment_dispatcher_map: &mut HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
    ) {
        for actor in actor_map.values() {
            for dispatcher in &actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    if let Some(downstream_actor) = actor_map.get(downstream_actor_id) {
                        fragment_dispatcher_map
                            .entry(actor.fragment_id as FragmentId)
                            .or_default()
                            .insert(
                                downstream_actor.fragment_id as FragmentId,
                                dispatcher.r#type(),
                            );
                    }
                }
            }
        }
    }

    pub fn resolve_no_shuffle_upstream_tables(
        fragment_ids: HashSet<FragmentId>,
        fragment_map: &HashMap<FragmentId, CustomFragmentInfo>,
        no_shuffle_source_fragment_ids: &HashSet<FragmentId>,
        no_shuffle_target_fragment_ids: &HashSet<FragmentId>,
        fragment_to_table: &HashMap<FragmentId, TableId>,
        table_parallelisms: &mut HashMap<TableId, TableParallelism>,
    ) -> MetaResult<()> {
        let mut queue: VecDeque<FragmentId> = fragment_ids.iter().cloned().collect();

        let mut fragment_ids = fragment_ids;

        // We trace the upstreams of each downstream under the hierarchy until we reach the top
        // for every no_shuffle relation.
        while let Some(fragment_id) = queue.pop_front() {
            if !no_shuffle_target_fragment_ids.contains(&fragment_id) {
                continue;
            }

            // for upstream
            for upstream_fragment_id in &fragment_map[&fragment_id].upstream_fragment_ids {
                if !no_shuffle_source_fragment_ids.contains(upstream_fragment_id) {
                    continue;
                }

                let table_id = &fragment_to_table[&fragment_id];
                let upstream_table_id = &fragment_to_table[upstream_fragment_id];

                // Only custom parallelism will be propagated to the no shuffle upstream.
                if let Some(TableParallelism::Custom) = table_parallelisms.get(table_id) {
                    if let Some(upstream_table_parallelism) =
                        table_parallelisms.get(upstream_table_id)
                    {
                        if upstream_table_parallelism != &TableParallelism::Custom {
                            bail!(
                                "Cannot change upstream table {} from {:?} to {:?}",
                                upstream_table_id,
                                upstream_table_parallelism,
                                TableParallelism::Custom
                            )
                        }
                    } else {
                        table_parallelisms.insert(*upstream_table_id, TableParallelism::Custom);
                    }
                }

                fragment_ids.insert(*upstream_fragment_id);
                queue.push_back(*upstream_fragment_id);
            }
        }

        let downstream_fragment_ids = fragment_ids
            .iter()
            .filter(|fragment_id| no_shuffle_target_fragment_ids.contains(fragment_id));

        let downstream_table_ids = downstream_fragment_ids
            .map(|fragment_id| fragment_to_table.get(fragment_id).unwrap())
            .collect::<HashSet<_>>();

        table_parallelisms.retain(|table_id, _| !downstream_table_ids.contains(table_id));

        Ok(())
    }

    pub fn resolve_no_shuffle_upstream_fragments<T>(
        reschedule: &mut HashMap<FragmentId, T>,
        fragment_map: &HashMap<FragmentId, CustomFragmentInfo>,
        no_shuffle_source_fragment_ids: &HashSet<FragmentId>,
        no_shuffle_target_fragment_ids: &HashSet<FragmentId>,
    ) -> MetaResult<()>
    where
        T: Clone + Eq,
    {
        let mut queue: VecDeque<FragmentId> = reschedule.keys().cloned().collect();

        // We trace the upstreams of each downstream under the hierarchy until we reach the top
        // for every no_shuffle relation.
        while let Some(fragment_id) = queue.pop_front() {
            if !no_shuffle_target_fragment_ids.contains(&fragment_id) {
                continue;
            }

            // for upstream
            for upstream_fragment_id in &fragment_map[&fragment_id].upstream_fragment_ids {
                if !no_shuffle_source_fragment_ids.contains(upstream_fragment_id) {
                    continue;
                }

                let reschedule_plan = &reschedule[&fragment_id];

                if let Some(upstream_reschedule_plan) = reschedule.get(upstream_fragment_id) {
                    if upstream_reschedule_plan != reschedule_plan {
                        bail!("Inconsistent NO_SHUFFLE plan, check target worker ids of fragment {} and {}", fragment_id, upstream_fragment_id);
                    }

                    continue;
                }

                reschedule.insert(*upstream_fragment_id, reschedule_plan.clone());

                queue.push_back(*upstream_fragment_id);
            }
        }

        reschedule.retain(|fragment_id, _| !no_shuffle_target_fragment_ids.contains(fragment_id));

        Ok(())
    }

    pub async fn resolve_related_no_shuffle_jobs(
        &self,
        jobs: &[TableId],
    ) -> MetaResult<HashSet<TableId>> {
        let RescheduleWorkingSet { related_jobs, .. } = self
            .metadata_manager
            .catalog_controller
            .resolve_working_set_for_reschedule_tables(
                jobs.iter().map(|id| id.table_id as _).collect(),
            )
            .await?;

        Ok(related_jobs
            .keys()
            .map(|id| TableId::new(*id as _))
            .collect())
    }
}

#[derive(Debug)]
pub enum JobParallelismTarget {
    Update(TableParallelism),
    Refresh,
}

#[derive(Debug)]
pub enum JobResourceGroupTarget {
    Update(Option<String>),
    Keep,
}

#[derive(Debug)]
pub struct JobRescheduleTarget {
    pub parallelism: JobParallelismTarget,
    pub resource_group: JobResourceGroupTarget,
}

#[derive(Debug)]
pub struct JobReschedulePolicy {
    pub(crate) targets: HashMap<u32, JobRescheduleTarget>,
}

// final updates for `post_collect`
#[derive(Debug, Clone)]
pub struct JobReschedulePostUpdates {
    pub parallelism_updates: HashMap<TableId, TableParallelism>,
    pub resource_group_updates: HashMap<ObjectId, Option<String>>,
}

#[derive(Debug)]
pub struct JobReschedulePlan {
    pub reschedules: HashMap<FragmentId, WorkerReschedule>,
    pub post_updates: JobReschedulePostUpdates,
}

impl GlobalStreamManager {
    pub async fn reschedule_lock_read_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.scale_controller.reschedule_lock.read().await
    }

    pub async fn reschedule_lock_write_guard(&self) -> RwLockWriteGuard<'_, ()> {
        self.scale_controller.reschedule_lock.write().await
    }

    /// The entrypoint of rescheduling actors.
    ///
    /// Used by:
    /// - The directly exposed low-level API `risingwave_meta_service::scale_service::ScaleService` (`risectl meta reschedule`)
    /// - High-level parallelism control API
    ///     * manual `ALTER [TABLE | INDEX | MATERIALIZED VIEW | SINK] SET PARALLELISM`
    ///     * automatic parallelism control for [`TableParallelism::Adaptive`] when worker nodes changed
    pub async fn reschedule_actors(
        &self,
        database_id: DatabaseId,
        plan: JobReschedulePlan,
        options: RescheduleOptions,
    ) -> MetaResult<()> {
        let JobReschedulePlan {
            reschedules,
            mut post_updates,
        } = plan;

        let reschedule_fragment = self
            .scale_controller
            .analyze_reschedule_plan(reschedules, options, &mut post_updates.parallelism_updates)
            .await?;

        tracing::debug!("reschedule plan: {:?}", reschedule_fragment);

        let up_down_stream_fragment: HashSet<_> = reschedule_fragment
            .iter()
            .flat_map(|(_, reschedule)| {
                reschedule
                    .upstream_fragment_dispatcher_ids
                    .iter()
                    .map(|(fragment_id, _)| *fragment_id)
                    .chain(reschedule.downstream_fragment_ids.iter().cloned())
            })
            .collect();

        let fragment_actors =
            try_join_all(up_down_stream_fragment.iter().map(|fragment_id| async {
                let actor_ids = self
                    .metadata_manager
                    .get_running_actors_of_fragment(*fragment_id)
                    .await?;
                Result::<_, MetaError>::Ok((*fragment_id, actor_ids))
            }))
            .await?
            .into_iter()
            .collect();

        let command = Command::RescheduleFragment {
            reschedules: reschedule_fragment,
            fragment_actors,
            post_updates,
        };

        let _guard = self.source_manager.pause_tick().await;

        self.barrier_scheduler
            .run_config_change_command_with_pause(database_id, command)
            .await?;

        tracing::info!("reschedule done");

        Ok(())
    }

    /// When new worker nodes joined, or the parallelism of existing worker nodes changed,
    /// examines if there are any jobs can be scaled, and scales them if found.
    ///
    /// This method will iterate over all `CREATED` jobs, and can be repeatedly called.
    ///
    /// Returns
    /// - `Ok(false)` if no jobs can be scaled;
    /// - `Ok(true)` if some jobs are scaled, and it is possible that there are more jobs can be scaled.
    async fn trigger_parallelism_control(&self) -> MetaResult<bool> {
        tracing::info!("trigger parallelism control");

        let _reschedule_job_lock = self.reschedule_lock_write_guard().await;

        let background_streaming_jobs = self
            .metadata_manager
            .list_background_creating_jobs()
            .await?;

        let skipped_jobs = if !background_streaming_jobs.is_empty() {
            let jobs = self
                .scale_controller
                .resolve_related_no_shuffle_jobs(&background_streaming_jobs)
                .await?;

            tracing::info!(
                "skipping parallelism control of background jobs {:?} and associated jobs {:?}",
                background_streaming_jobs,
                jobs
            );

            jobs
        } else {
            HashSet::new()
        };

        let job_ids: HashSet<_> = {
            let streaming_parallelisms = self
                .metadata_manager
                .catalog_controller
                .get_all_streaming_parallelisms()
                .await?;

            streaming_parallelisms
                .into_iter()
                .filter(|(table_id, _)| !skipped_jobs.contains(&TableId::new(*table_id as _)))
                .map(|(table_id, _)| table_id)
                .collect()
        };

        let workers = self
            .metadata_manager
            .cluster_controller
            .list_active_streaming_workers()
            .await?;

        let schedulable_worker_ids: BTreeSet<_> = workers
            .iter()
            .filter(|worker| {
                !worker
                    .property
                    .as_ref()
                    .map(|p| p.is_unschedulable)
                    .unwrap_or(false)
            })
            .map(|worker| worker.id as WorkerId)
            .collect();

        if job_ids.is_empty() {
            tracing::info!("no streaming jobs for scaling, maybe an empty cluster");
            return Ok(false);
        }

        let batch_size = match self.env.opts.parallelism_control_batch_size {
            0 => job_ids.len(),
            n => n,
        };

        tracing::info!(
            "total {} streaming jobs, batch size {}, schedulable worker ids: {:?}",
            job_ids.len(),
            batch_size,
            schedulable_worker_ids
        );

        let batches: Vec<_> = job_ids
            .into_iter()
            .chunks(batch_size)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect();

        let mut reschedules = None;

        for batch in batches {
            let targets: HashMap<_, _> = batch
                .into_iter()
                .map(|job_id| {
                    (
                        job_id as u32,
                        JobRescheduleTarget {
                            parallelism: JobParallelismTarget::Refresh,
                            resource_group: JobResourceGroupTarget::Keep,
                        },
                    )
                })
                .collect();

            let plan = self
                .scale_controller
                .generate_job_reschedule_plan(JobReschedulePolicy { targets })
                .await?;

            if !plan.reschedules.is_empty() {
                tracing::info!("reschedule plan generated for streaming jobs {:?}", plan);
                reschedules = Some(plan);
                break;
            }
        }

        let Some(plan) = reschedules else {
            tracing::info!("no reschedule plan generated");
            return Ok(false);
        };

        // todo
        for (database_id, reschedules) in self
            .metadata_manager
            .split_fragment_map_by_database(plan.reschedules)
            .await?
        {
            self.reschedule_actors(
                database_id,
                JobReschedulePlan {
                    reschedules,
                    post_updates: plan.post_updates.clone(),
                },
                RescheduleOptions {
                    resolve_no_shuffle_upstream: false,
                    skip_create_new_actors: false,
                },
            )
            .await?;
        }

        Ok(true)
    }

    /// Handles notification of worker node activation and deletion, and triggers parallelism control.
    async fn run(&self, mut shutdown_rx: Receiver<()>) {
        tracing::info!("starting automatic parallelism control monitor");

        let check_period =
            Duration::from_secs(self.env.opts.parallelism_control_trigger_period_sec);

        let mut ticker = tokio::time::interval_at(
            Instant::now()
                + Duration::from_secs(self.env.opts.parallelism_control_trigger_first_delay_sec),
            check_period,
        );
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // waiting for the first tick
        ticker.tick().await;

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();

        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;

        let worker_nodes = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .expect("list active streaming compute nodes");

        let mut worker_cache: BTreeMap<_, _> = worker_nodes
            .into_iter()
            .map(|worker| (worker.id, worker))
            .collect();

        let mut should_trigger = false;

        loop {
            tokio::select! {
                biased;

                _ = &mut shutdown_rx => {
                    tracing::info!("Stream manager is stopped");
                    break;
                }

                _ = ticker.tick(), if should_trigger => {
                    let include_workers = worker_cache.keys().copied().collect_vec();

                    if include_workers.is_empty() {
                        tracing::debug!("no available worker nodes");
                        should_trigger = false;
                        continue;
                    }

                    match self.trigger_parallelism_control().await {
                        Ok(cont) => {
                            should_trigger = cont;
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "Failed to trigger scale out, waiting for next tick to retry after {}s", ticker.period().as_secs());
                            ticker.reset();
                        }
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.expect("local notification channel closed in loop of stream manager");

                    // Only maintain the cache for streaming compute nodes.
                    let worker_is_streaming_compute = |worker: &WorkerNode| {
                        worker.get_type() == Ok(WorkerType::ComputeNode)
                            && worker.property.as_ref().unwrap().is_streaming
                    };

                    match notification {
                        LocalNotification::WorkerNodeActivated(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            tracing::info!(worker = worker.id, "worker activated notification received");

                            let prev_worker = worker_cache.insert(worker.id, worker.clone());

                            match prev_worker {
                                Some(prev_worker) if prev_worker.compute_node_parallelism() != worker.compute_node_parallelism()  => {
                                    tracing::info!(worker = worker.id, "worker parallelism changed");
                                    should_trigger = true;
                                }
                                Some(prev_worker) if  prev_worker.resource_group() != worker.resource_group()  => {
                                    tracing::info!(worker = worker.id, "worker label changed");
                                    should_trigger = true;
                                }
                                None => {
                                    tracing::info!(worker = worker.id, "new worker joined");
                                    should_trigger = true;
                                }
                                _ => {}
                            }
                        }

                        // Since our logic for handling passive scale-in is within the barrier manager,
                        // there’s not much we can do here. All we can do is proactively remove the entries from our cache.
                        LocalNotification::WorkerNodeDeleted(worker) => {
                            if !worker_is_streaming_compute(&worker) {
                                continue;
                            }

                            match worker_cache.remove(&worker.id) {
                                Some(prev_worker) => {
                                    tracing::info!(worker = prev_worker.id, "worker removed from stream manager cache");
                                }
                                None => {
                                    tracing::warn!(worker = worker.id, "worker not found in stream manager cache, but it was removed");
                                }
                            }
                        }

                        _ => {}
                    }
                }
            }
        }
    }

    pub fn start_auto_parallelism_monitor(
        self: Arc<Self>,
    ) -> (JoinHandle<()>, oneshot::Sender<()>) {
        tracing::info!("Automatic parallelism scale-out is enabled for streaming jobs");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }
}

pub fn schedule_units_for_slots(
    slots: &BTreeMap<WorkerId, usize>,
    total_unit_size: usize,
    salt: u32,
) -> MetaResult<BTreeMap<WorkerId, usize>> {
    let mut ch = ConsistentHashRing::new(salt);

    for (worker_id, parallelism) in slots {
        ch.add_worker(*worker_id as _, *parallelism as u32);
    }

    let target_distribution = ch.distribute_tasks(total_unit_size as u32)?;

    Ok(target_distribution
        .into_iter()
        .map(|(worker_id, task_count)| (worker_id as WorkerId, task_count as usize))
        .collect())
}

pub struct ConsistentHashRing {
    ring: BTreeMap<u64, u32>,
    weights: BTreeMap<u32, u32>,
    virtual_nodes: u32,
    salt: u32,
}

impl ConsistentHashRing {
    fn new(salt: u32) -> Self {
        ConsistentHashRing {
            ring: BTreeMap::new(),
            weights: BTreeMap::new(),
            virtual_nodes: 1024,
            salt,
        }
    }

    fn hash<T: Hash, S: Hash>(key: T, salt: S) -> u64 {
        let mut hasher = DefaultHasher::new();
        salt.hash(&mut hasher);
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn add_worker(&mut self, id: u32, weight: u32) {
        let virtual_nodes_count = self.virtual_nodes;

        for i in 0..virtual_nodes_count {
            let virtual_node_key = (id, i);
            let hash = Self::hash(virtual_node_key, self.salt);
            self.ring.insert(hash, id);
        }

        self.weights.insert(id, weight);
    }

    fn distribute_tasks(&self, total_tasks: u32) -> MetaResult<BTreeMap<u32, u32>> {
        let total_weight = self.weights.values().sum::<u32>();

        let mut soft_limits = HashMap::new();
        for (worker_id, worker_capacity) in &self.weights {
            soft_limits.insert(
                *worker_id,
                (total_tasks as f64 * (*worker_capacity as f64 / total_weight as f64)).ceil()
                    as u32,
            );
        }

        let mut task_distribution: BTreeMap<u32, u32> = BTreeMap::new();
        let mut task_hashes = (0..total_tasks)
            .map(|task_idx| Self::hash(task_idx, self.salt))
            .collect_vec();

        // Sort task hashes to disperse them around the hash ring
        task_hashes.sort();

        for task_hash in task_hashes {
            let mut assigned = false;

            // Iterator that starts from the current task_hash or the next node in the ring
            let ring_range = self.ring.range(task_hash..).chain(self.ring.iter());

            for (_, &worker_id) in ring_range {
                let task_limit = soft_limits[&worker_id];

                let worker_task_count = task_distribution.entry(worker_id).or_insert(0);

                if *worker_task_count < task_limit {
                    *worker_task_count += 1;
                    assigned = true;
                    break;
                }
            }

            if !assigned {
                bail!("Could not distribute tasks due to capacity constraints.");
            }
        }

        Ok(task_distribution)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_SALT: u32 = 42;

    #[test]
    fn test_single_worker_capacity() {
        let mut ch = ConsistentHashRing::new(DEFAULT_SALT);
        ch.add_worker(1, 10);

        let total_tasks = 5;
        let task_distribution = ch.distribute_tasks(total_tasks).unwrap();

        assert_eq!(task_distribution.get(&1).cloned().unwrap_or(0), 5);
    }

    #[test]
    fn test_multiple_workers_even_distribution() {
        let mut ch = ConsistentHashRing::new(DEFAULT_SALT);

        ch.add_worker(1, 1);
        ch.add_worker(2, 1);
        ch.add_worker(3, 1);

        let total_tasks = 3;
        let task_distribution = ch.distribute_tasks(total_tasks).unwrap();

        for id in 1..=3 {
            assert_eq!(task_distribution.get(&id).cloned().unwrap_or(0), 1);
        }
    }

    #[test]
    fn test_weighted_distribution() {
        let mut ch = ConsistentHashRing::new(DEFAULT_SALT);

        ch.add_worker(1, 2);
        ch.add_worker(2, 3);
        ch.add_worker(3, 5);

        let total_tasks = 10;
        let task_distribution = ch.distribute_tasks(total_tasks).unwrap();

        assert_eq!(task_distribution.get(&1).cloned().unwrap_or(0), 2);
        assert_eq!(task_distribution.get(&2).cloned().unwrap_or(0), 3);
        assert_eq!(task_distribution.get(&3).cloned().unwrap_or(0), 5);
    }

    #[test]
    fn test_over_capacity() {
        let mut ch = ConsistentHashRing::new(DEFAULT_SALT);

        ch.add_worker(1, 1);
        ch.add_worker(2, 2);
        ch.add_worker(3, 3);

        let total_tasks = 10; // More tasks than the total weight
        let task_distribution = ch.distribute_tasks(total_tasks);

        assert!(task_distribution.is_ok());
    }

    #[test]
    fn test_balance_distribution() {
        for mut worker_capacity in 1..10 {
            for workers in 3..10 {
                let mut ring = ConsistentHashRing::new(DEFAULT_SALT);

                for worker_id in 0..workers {
                    ring.add_worker(worker_id, worker_capacity);
                }

                // Here we simulate a real situation where the actual parallelism cannot fill all the capacity.
                // This is to ensure an average distribution, for example, when three workers with 6 parallelism are assigned 9 tasks,
                // they should ideally get an exact distribution of 3, 3, 3 respectively.
                if worker_capacity % 2 == 0 {
                    worker_capacity /= 2;
                }

                let total_tasks = worker_capacity * workers;

                let task_distribution = ring.distribute_tasks(total_tasks).unwrap();

                for (_, v) in task_distribution {
                    assert_eq!(v, worker_capacity);
                }
            }
        }
    }
}
