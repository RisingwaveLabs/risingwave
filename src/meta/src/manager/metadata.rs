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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::pin::pin;
use std::time::Duration;

use anyhow::anyhow;
use futures::future::{select, Either};
use risingwave_common::catalog::{DatabaseId, TableId, TableOption};
use risingwave_meta_model::{ObjectId, SinkId, SourceId, WorkerId};
use risingwave_pb::catalog::{PbSink, PbSource, PbTable};
use risingwave_pb::common::worker_node::{PbResource, Property as AddNodeProperty, State};
use risingwave_pb::common::{HostAddress, PbWorkerNode, PbWorkerType, WorkerNode, WorkerType};
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::table_fragments::{Fragment, PbFragment};
use risingwave_pb::stream_plan::{PbDispatchStrategy, PbStreamScanType};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant};
use tracing::warn;

use crate::barrier::Reschedule;
use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::{ClusterControllerRef, StreamingClusterInfo, WorkerExtraInfo};
use crate::controller::fragment::FragmentParallelismInfo;
use crate::manager::{LocalNotification, NotificationVersion};
use crate::model::{
    ActorId, ClusterId, FragmentId, StreamActorWithUpstreams, StreamJobFragments, SubscriptionId,
};
use crate::stream::{JobReschedulePostUpdates, SplitAssignment};
use crate::telemetry::MetaTelemetryJobDesc;
use crate::{MetaError, MetaResult};

#[derive(Clone)]
pub struct MetadataManager {
    pub cluster_controller: ClusterControllerRef,
    pub catalog_controller: CatalogControllerRef,
}

#[derive(Debug)]
pub(crate) enum ActiveStreamingWorkerChange {
    Add(WorkerNode),
    #[expect(dead_code)]
    Remove(WorkerNode),
    Update(WorkerNode),
}

pub struct ActiveStreamingWorkerNodes {
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    rx: UnboundedReceiver<LocalNotification>,
    #[cfg_attr(not(debug_assertions), expect(dead_code))]
    meta_manager: MetadataManager,
}

impl Debug for ActiveStreamingWorkerNodes {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveStreamingWorkerNodes")
            .field("worker_nodes", &self.worker_nodes)
            .finish()
    }
}

impl ActiveStreamingWorkerNodes {
    pub(crate) fn uninitialized(meta_manager: MetadataManager) -> Self {
        Self {
            worker_nodes: Default::default(),
            rx: unbounded_channel().1,
            meta_manager,
        }
    }

    /// Return an uninitialized one as a placeholder for future initialized
    pub(crate) async fn new_snapshot(meta_manager: MetadataManager) -> MetaResult<Self> {
        let (nodes, rx) = meta_manager
            .subscribe_active_streaming_compute_nodes()
            .await?;
        Ok(Self {
            worker_nodes: nodes.into_iter().map(|node| (node.id as _, node)).collect(),
            rx,
            meta_manager,
        })
    }

    pub(crate) fn current(&self) -> &HashMap<WorkerId, WorkerNode> {
        &self.worker_nodes
    }

    pub(crate) async fn wait_changed(
        &mut self,
        verbose_internal: Duration,
        verbose_timeout: Duration,
        verbose_fn: impl Fn(&Self),
    ) -> Option<ActiveStreamingWorkerChange> {
        let start = Instant::now();
        loop {
            if let Either::Left((change, _)) =
                select(pin!(self.changed()), pin!(sleep(verbose_internal))).await
            {
                break Some(change);
            }

            if start.elapsed() > verbose_timeout {
                break None;
            }

            verbose_fn(self)
        }
    }

    pub(crate) async fn changed(&mut self) -> ActiveStreamingWorkerChange {
        let ret = loop {
            let notification = self
                .rx
                .recv()
                .await
                .expect("notification stopped or uninitialized");
            match notification {
                LocalNotification::WorkerNodeDeleted(worker) => {
                    let is_streaming_compute_node = worker.r#type == WorkerType::ComputeNode as i32
                        && worker.property.as_ref().unwrap().is_streaming;
                    let Some(prev_worker) = self.worker_nodes.remove(&(worker.id as _)) else {
                        if is_streaming_compute_node {
                            warn!(
                                ?worker,
                                "notify to delete an non-existing streaming compute worker"
                            );
                        }
                        continue;
                    };
                    if !is_streaming_compute_node {
                        warn!(
                            ?worker,
                            ?prev_worker,
                            "deleted worker has a different recent type"
                        );
                    }
                    if worker.state == State::Starting as i32 {
                        warn!(
                            id = worker.id,
                            host = ?worker.host,
                            state = worker.state,
                            "a starting streaming worker is deleted"
                        );
                    }
                    break ActiveStreamingWorkerChange::Remove(prev_worker);
                }
                LocalNotification::WorkerNodeActivated(worker) => {
                    if worker.r#type != WorkerType::ComputeNode as i32
                        || !worker.property.as_ref().unwrap().is_streaming
                    {
                        if let Some(prev_worker) = self.worker_nodes.remove(&(worker.id as _)) {
                            warn!(
                                ?worker,
                                ?prev_worker,
                                "the type of a streaming worker is changed"
                            );
                            break ActiveStreamingWorkerChange::Remove(prev_worker);
                        } else {
                            continue;
                        }
                    }
                    assert_eq!(
                        worker.state,
                        State::Running as i32,
                        "not started worker added: {:?}",
                        worker
                    );
                    if let Some(prev_worker) =
                        self.worker_nodes.insert(worker.id as _, worker.clone())
                    {
                        assert_eq!(prev_worker.host, worker.host);
                        assert_eq!(prev_worker.r#type, worker.r#type);
                        warn!(
                            ?prev_worker,
                            ?worker,
                            eq = prev_worker == worker,
                            "notify to update an existing active worker"
                        );
                        if prev_worker == worker {
                            continue;
                        } else {
                            break ActiveStreamingWorkerChange::Update(worker);
                        }
                    } else {
                        break ActiveStreamingWorkerChange::Add(worker);
                    }
                }
                _ => {
                    continue;
                }
            }
        };

        ret
    }

    #[cfg(debug_assertions)]
    pub(crate) async fn validate_change(&self) {
        use risingwave_pb::common::WorkerNode;
        use thiserror_ext::AsReport;
        match self
            .meta_manager
            .list_active_streaming_compute_nodes()
            .await
        {
            Ok(worker_nodes) => {
                let ignore_irrelevant_info = |node: &WorkerNode| {
                    (
                        node.id,
                        WorkerNode {
                            id: node.id,
                            r#type: node.r#type,
                            host: node.host.clone(),
                            property: node.property.clone(),
                            resource: node.resource.clone(),
                            ..Default::default()
                        },
                    )
                };
                let worker_nodes: HashMap<_, _> =
                    worker_nodes.iter().map(ignore_irrelevant_info).collect();
                let curr_worker_nodes: HashMap<_, _> = self
                    .current()
                    .values()
                    .map(ignore_irrelevant_info)
                    .collect();
                if worker_nodes != curr_worker_nodes {
                    warn!(
                        ?worker_nodes,
                        ?curr_worker_nodes,
                        "different to global snapshot"
                    );
                }
            }
            Err(e) => {
                warn!(e = ?e.as_report(), "fail to list_active_streaming_compute_nodes to compare with local snapshot");
            }
        }
    }
}

impl MetadataManager {
    pub fn new(
        cluster_controller: ClusterControllerRef,
        catalog_controller: CatalogControllerRef,
    ) -> Self {
        Self {
            cluster_controller,
            catalog_controller,
        }
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        self.cluster_controller.get_worker_by_id(worker_id).await
    }

    pub async fn count_worker_node(&self) -> MetaResult<HashMap<WorkerType, u64>> {
        let node_map = self.cluster_controller.count_worker_by_type().await?;
        Ok(node_map
            .into_iter()
            .map(|(ty, cnt)| (ty.into(), cnt as u64))
            .collect())
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.cluster_controller
            .get_worker_info_by_id(worker_id as _)
            .await
    }

    pub async fn add_worker_node(
        &self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        property: AddNodeProperty,
        resource: PbResource,
    ) -> MetaResult<WorkerId> {
        self.cluster_controller
            .add_worker(r#type, host_address, property, resource)
            .await
            .map(|id| id as WorkerId)
    }

    pub async fn list_worker_node(
        &self,
        worker_type: Option<WorkerType>,
        worker_state: Option<State>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller
            .list_workers(worker_type.map(Into::into), worker_state.map(Into::into))
            .await
    }

    pub async fn subscribe_active_streaming_compute_nodes(
        &self,
    ) -> MetaResult<(Vec<WorkerNode>, UnboundedReceiver<LocalNotification>)> {
        self.cluster_controller
            .subscribe_active_streaming_compute_nodes()
            .await
    }

    pub async fn list_active_streaming_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller
            .list_active_streaming_workers()
            .await
    }

    pub async fn list_active_serving_compute_nodes(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.cluster_controller.list_active_serving_workers().await
    }

    pub async fn list_active_database_ids(&self) -> MetaResult<HashSet<DatabaseId>> {
        Ok(self
            .catalog_controller
            .list_fragment_database_ids(None)
            .await?
            .into_iter()
            .map(|(_, database_id)| DatabaseId::new(database_id as _))
            .collect())
    }

    pub async fn split_fragment_map_by_database<T: Debug>(
        &self,
        fragment_map: HashMap<FragmentId, T>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<FragmentId, T>>> {
        let fragment_to_database_map: HashMap<_, _> = self
            .catalog_controller
            .list_fragment_database_ids(Some(
                fragment_map
                    .keys()
                    .map(|fragment_id| *fragment_id as _)
                    .collect(),
            ))
            .await?
            .into_iter()
            .map(|(fragment_id, database_id)| {
                (fragment_id as FragmentId, DatabaseId::new(database_id as _))
            })
            .collect();
        let mut ret: HashMap<_, HashMap<_, _>> = HashMap::new();
        for (fragment_id, value) in fragment_map {
            let database_id = *fragment_to_database_map
                .get(&fragment_id)
                .ok_or_else(|| anyhow!("cannot get database_id of fragment {fragment_id}"))?;
            ret.entry(database_id)
                .or_default()
                .try_insert(fragment_id, value)
                .expect("non duplicate");
        }
        Ok(ret)
    }

    pub async fn list_background_creating_jobs(&self) -> MetaResult<Vec<TableId>> {
        let tables = self
            .catalog_controller
            .list_background_creating_mviews(false)
            .await?;

        Ok(tables
            .into_iter()
            .map(|table| TableId::from(table.table_id as u32))
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        self.catalog_controller.list_sources().await
    }

    pub async fn post_apply_reschedules(
        &self,
        reschedules: HashMap<FragmentId, Reschedule>,
        post_updates: &JobReschedulePostUpdates,
    ) -> MetaResult<()> {
        // temp convert u32 to i32
        let reschedules = reschedules.into_iter().map(|(k, v)| (k as _, v)).collect();

        self.catalog_controller
            .post_apply_reschedules(reschedules, post_updates)
            .await
    }

    pub async fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> MetaResult<HashMap<FragmentId, FragmentParallelismInfo>> {
        let id_filter = id_filter.map(|ids| ids.into_iter().map(|id| id as _).collect());
        Ok(self
            .catalog_controller
            .running_fragment_parallelisms(id_filter)
            .await?
            .into_iter()
            .map(|(k, v)| (k as FragmentId, v))
            .collect())
    }

    /// Get and filter the "**root**" fragments of the specified relations.
    /// The root fragment is the bottom-most fragment of its fragment graph, and can be a `MView` or a `Source`.
    ///
    /// See also [`crate::controller::catalog::CatalogController::get_root_fragments`].
    pub async fn get_upstream_root_fragments(
        &self,
        upstream_table_ids: &HashSet<TableId>,
    ) -> MetaResult<(HashMap<TableId, Fragment>, HashMap<ActorId, WorkerId>)> {
        let (upstream_root_fragments, actors) = self
            .catalog_controller
            .get_root_fragments(
                upstream_table_ids
                    .iter()
                    .map(|id| id.table_id as _)
                    .collect(),
            )
            .await?;

        let actors = actors
            .into_iter()
            .map(|(actor, worker)| (actor as u32, worker))
            .collect();

        Ok((
            upstream_root_fragments
                .into_iter()
                .map(|(id, fragment)| ((id as u32).into(), fragment))
                .collect(),
            actors,
        ))
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        self.cluster_controller.get_streaming_cluster_info().await
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<u32, TableOption>> {
        self.catalog_controller
            .get_all_table_options()
            .await
            .map(|tops| tops.into_iter().map(|(id, opt)| (id as u32, opt)).collect())
    }

    pub async fn get_table_name_type_mapping(&self) -> MetaResult<HashMap<u32, (String, String)>> {
        let mappings = self
            .catalog_controller
            .get_table_name_type_mapping()
            .await?;
        Ok(mappings
            .into_iter()
            .map(|(id, value)| (id as u32, value))
            .collect())
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<u32>> {
        let table_ids = self.catalog_controller.get_created_table_ids().await?;
        Ok(table_ids.into_iter().map(|id| id as u32).collect())
    }

    pub async fn get_table_associated_source_id(
        &self,
        table_id: u32,
    ) -> MetaResult<Option<SourceId>> {
        self.catalog_controller
            .get_table_associated_source_id(table_id as _)
            .await
    }

    pub async fn get_table_catalog_by_ids(&self, ids: Vec<u32>) -> MetaResult<Vec<PbTable>> {
        self.catalog_controller
            .get_table_by_ids(ids.into_iter().map(|id| id as _).collect())
            .await
    }

    pub async fn get_sink_catalog_by_ids(&self, ids: &[u32]) -> MetaResult<Vec<PbSink>> {
        self.catalog_controller
            .get_sink_by_ids(ids.iter().map(|id| *id as _).collect())
            .await
    }

    pub async fn get_table_catalog_by_cdc_table_id(
        &self,
        cdc_table_id: &String,
    ) -> MetaResult<Vec<PbTable>> {
        self.catalog_controller
            .get_table_by_cdc_table_id(cdc_table_id)
            .await
    }

    pub async fn get_downstream_fragments(
        &self,
        job_id: u32,
    ) -> MetaResult<(
        Vec<(PbDispatchStrategy, PbFragment)>,
        HashMap<ActorId, WorkerId>,
    )> {
        let (fragments, actors) = self
            .catalog_controller
            .get_downstream_fragments(job_id as _)
            .await?;

        let actors = actors
            .into_iter()
            .map(|(actor, worker)| (actor as u32, worker))
            .collect();

        Ok((fragments, actors))
    }

    pub async fn get_worker_actor_ids(
        &self,
        job_ids: HashSet<TableId>,
    ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
        let worker_actors = self
            .catalog_controller
            .get_worker_actor_ids(job_ids.into_iter().map(|id| id.table_id as _).collect())
            .await?;
        Ok(worker_actors
            .into_iter()
            .map(|(id, actors)| {
                (
                    id as WorkerId,
                    actors.into_iter().map(|id| id as ActorId).collect(),
                )
            })
            .collect())
    }

    pub async fn get_job_id_to_internal_table_ids_mapping(&self) -> Option<Vec<(u32, Vec<u32>)>> {
        let job_internal_table_ids = self.catalog_controller.get_job_internal_table_ids().await;
        job_internal_table_ids.map(|ids| {
            ids.into_iter()
                .map(|(id, internal_ids)| {
                    (
                        id as u32,
                        internal_ids.into_iter().map(|id| id as u32).collect(),
                    )
                })
                .collect()
        })
    }

    pub async fn get_job_fragments_by_id(
        &self,
        job_id: &TableId,
    ) -> MetaResult<StreamJobFragments> {
        self.catalog_controller
            .get_job_fragments_by_id(job_id.table_id as _)
            .await
    }

    pub async fn get_running_actors_of_fragment(
        &self,
        id: FragmentId,
    ) -> MetaResult<HashSet<ActorId>> {
        let actor_ids = self
            .catalog_controller
            .get_running_actors_of_fragment(id as _)
            .await?;
        Ok(actor_ids.into_iter().map(|id| id as ActorId).collect())
    }

    pub async fn get_running_actors_for_source_backfill(
        &self,
        id: FragmentId,
    ) -> MetaResult<HashSet<(ActorId, ActorId)>> {
        let actor_ids = self
            .catalog_controller
            .get_running_actors_for_source_backfill(id as _)
            .await?;
        Ok(actor_ids
            .into_iter()
            .map(|(id, upstream)| (id as ActorId, upstream as ActorId))
            .collect())
    }

    pub async fn get_job_fragments_by_ids(
        &self,
        ids: &[TableId],
    ) -> MetaResult<Vec<StreamJobFragments>> {
        let mut table_fragments = vec![];
        for id in ids {
            table_fragments.push(
                self.catalog_controller
                    .get_job_fragments_by_id(id.table_id as _)
                    .await?,
            );
        }
        Ok(table_fragments)
    }

    pub async fn all_active_actors(
        &self,
    ) -> MetaResult<HashMap<ActorId, StreamActorWithUpstreams>> {
        let table_fragments = self.catalog_controller.table_fragments().await?;
        let mut actor_maps = HashMap::new();
        for (_, tf) in table_fragments {
            for actor in tf.active_actors() {
                actor_maps
                    .try_insert(actor.0.actor_id, actor)
                    .expect("non duplicate");
            }
        }
        Ok(actor_maps)
    }

    pub async fn worker_actor_count(&self) -> MetaResult<HashMap<WorkerId, usize>> {
        let actor_cnt = self.catalog_controller.worker_actor_count().await?;
        Ok(actor_cnt
            .into_iter()
            .map(|(id, cnt)| (id as WorkerId, cnt))
            .collect())
    }

    pub async fn count_streaming_job(&self) -> MetaResult<usize> {
        self.catalog_controller
            .list_streaming_job_infos()
            .await
            .map(|x| x.len())
    }

    pub async fn list_stream_job_desc(&self) -> MetaResult<Vec<MetaTelemetryJobDesc>> {
        self.catalog_controller
            .list_stream_job_desc_for_telemetry()
            .await
    }

    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let fragment_actors = self
            .catalog_controller
            .update_source_rate_limit_by_source_id(source_id as _, rate_limit)
            .await?;
        Ok(fragment_actors
            .into_iter()
            .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
            .collect())
    }

    pub async fn update_backfill_rate_limit_by_table_id(
        &self,
        table_id: TableId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let fragment_actors = self
            .catalog_controller
            .update_backfill_rate_limit_by_job_id(table_id.table_id as _, rate_limit)
            .await?;
        Ok(fragment_actors
            .into_iter()
            .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
            .collect())
    }

    pub async fn update_sink_rate_limit_by_sink_id(
        &self,
        sink_id: SinkId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let fragment_actors = self
            .catalog_controller
            .update_sink_rate_limit_by_job_id(sink_id as _, rate_limit)
            .await?;
        Ok(fragment_actors
            .into_iter()
            .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
            .collect())
    }

    pub async fn update_dml_rate_limit_by_table_id(
        &self,
        table_id: TableId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let fragment_actors = self
            .catalog_controller
            .update_dml_rate_limit_by_job_id(table_id.table_id as _, rate_limit)
            .await?;
        Ok(fragment_actors
            .into_iter()
            .map(|(id, actors)| (id as _, actors.into_iter().map(|id| id as _).collect()))
            .collect())
    }

    pub async fn update_actor_splits_by_split_assignment(
        &self,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        self.catalog_controller
            .update_actor_splits(split_assignment)
            .await
    }

    pub async fn get_mv_depended_subscriptions(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<SubscriptionId, u64>>>> {
        let database_id = database_id.map(|database_id| database_id.database_id as _);
        Ok(self
            .catalog_controller
            .get_mv_depended_subscriptions(database_id)
            .await?
            .into_iter()
            .map(|(loaded_database_id, mv_depended_subscriptions)| {
                if let Some(database_id) = database_id {
                    assert_eq!(loaded_database_id, database_id);
                }
                (
                    DatabaseId::new(loaded_database_id as _),
                    mv_depended_subscriptions
                        .into_iter()
                        .map(|(table_id, subscriptions)| {
                            (
                                TableId::new(table_id as _),
                                subscriptions
                                    .into_iter()
                                    .map(|(subscription_id, retention_time)| {
                                        (subscription_id as SubscriptionId, retention_time)
                                    })
                                    .collect(),
                            )
                        })
                        .collect(),
                )
            })
            .collect())
    }

    pub async fn get_job_max_parallelism(&self, table_id: TableId) -> MetaResult<usize> {
        self.catalog_controller
            .get_max_parallelism_by_id(table_id.table_id as _)
            .await
    }

    pub async fn get_existing_job_resource_group(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<String> {
        self.catalog_controller
            .get_existing_job_resource_group(streaming_job_id)
            .await
    }

    pub async fn get_database_resource_group(&self, database_id: ObjectId) -> MetaResult<String> {
        self.catalog_controller
            .get_database_resource_group(database_id)
            .await
    }

    pub fn cluster_id(&self) -> &ClusterId {
        self.cluster_controller.cluster_id()
    }

    pub async fn list_rate_limits(&self) -> MetaResult<Vec<RateLimitInfo>> {
        let rate_limits = self.catalog_controller.list_rate_limits().await?;
        Ok(rate_limits)
    }

    pub async fn get_job_backfill_scan_types(
        &self,
        job_id: &TableId,
    ) -> MetaResult<HashMap<FragmentId, PbStreamScanType>> {
        let backfill_types = self
            .catalog_controller
            .get_job_fragment_backfill_scan_type(job_id.table_id as _)
            .await?;
        Ok(backfill_types)
    }
}

impl MetadataManager {
    /// Wait for job finishing notification in `TrackingJob::finish`.
    /// The progress is updated per barrier.
    pub(crate) async fn wait_streaming_job_finished(
        &self,
        id: ObjectId,
    ) -> MetaResult<NotificationVersion> {
        tracing::debug!("wait_streaming_job_finished: {id:?}");
        let mut mgr = self.catalog_controller.get_inner_write_guard().await;
        if mgr.streaming_job_is_finished(id).await? {
            return Ok(self.catalog_controller.current_notification_version().await);
        }
        let (tx, rx) = oneshot::channel();

        mgr.register_finish_notifier(id, tx);
        drop(mgr);
        rx.await.map_err(|e| anyhow!(e))?
    }

    pub(crate) async fn notify_finish_failed(&self, err: &MetaError) {
        let mut mgr = self.catalog_controller.get_inner_write_guard().await;
        mgr.notify_finish_failed(err);
    }
}
