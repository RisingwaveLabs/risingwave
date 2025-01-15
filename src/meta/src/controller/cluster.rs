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

use std::cmp;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use risingwave_common::hash::WorkerSlotId;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::resource_util::cpu::total_cpu_available;
use risingwave_common::util::resource_util::memory::system_memory_available_bytes;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_common::RW_VERSION;
use risingwave_license::LicenseManager;
use risingwave_meta_model::prelude::{Worker, WorkerProperty};
use risingwave_meta_model::worker::{WorkerStatus, WorkerType};
use risingwave_meta_model::{worker, worker_property, TransactionId, WorkerId};
use risingwave_pb::common::worker_node::{
    PbProperty, PbProperty as AddNodeProperty, PbResource, PbState,
};
use risingwave_pb::common::{HostAddress, PbHostAddress, PbWorkerNode, PbWorkerType, WorkerNode};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
use sea_orm::prelude::Expr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    QueryFilter, QuerySelect, TransactionTrait,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;

use crate::controller::utils::filter_workers_by_resource_group;
use crate::manager::{LocalNotification, MetaSrvEnv, WorkerKey, META_NODE_ID};
use crate::model::ClusterId;
use crate::{MetaError, MetaResult};

pub type ClusterControllerRef = Arc<ClusterController>;

pub struct ClusterController {
    env: MetaSrvEnv,
    max_heartbeat_interval: Duration,
    inner: RwLock<ClusterControllerInner>,
    /// Used as timestamp when meta node starts in sec.
    started_at: u64,
}

struct WorkerInfo(
    worker::Model,
    Option<worker_property::Model>,
    WorkerExtraInfo,
);

impl From<WorkerInfo> for PbWorkerNode {
    fn from(info: WorkerInfo) -> Self {
        Self {
            id: info.0.worker_id as _,
            r#type: PbWorkerType::from(info.0.worker_type) as _,
            host: Some(PbHostAddress {
                host: info.0.host,
                port: info.0.port,
            }),
            state: PbState::from(info.0.status) as _,
            property: info.1.as_ref().map(|p| PbProperty {
                is_streaming: p.is_streaming,
                is_serving: p.is_serving,
                is_unschedulable: p.is_unschedulable,
                internal_rpc_host_addr: p.internal_rpc_host_addr.clone().unwrap_or_default(),
                resource_group: p.resource_group.clone(),
                parallelism: info.1.as_ref().map(|p| p.parallelism).unwrap_or_default() as u32,
            }),
            transactional_id: info.0.transaction_id.map(|id| id as _),
            resource: info.2.resource,
            started_at: info.2.started_at,
        }
    }
}

impl ClusterController {
    pub async fn new(env: MetaSrvEnv, max_heartbeat_interval: Duration) -> MetaResult<Self> {
        let inner = ClusterControllerInner::new(
            env.meta_store_ref().conn.clone(),
            env.opts.disable_automatic_parallelism_control,
        )
        .await?;
        Ok(Self {
            env,
            max_heartbeat_interval,
            inner: RwLock::new(inner),
            started_at: timestamp_now_sec(),
        })
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_read_guard(&self) -> RwLockReadGuard<'_, ClusterControllerInner> {
        self.inner.read().await
    }

    pub async fn count_worker_by_type(&self) -> MetaResult<HashMap<WorkerType, i64>> {
        self.inner.read().await.count_worker_by_type().await
    }

    /// A worker node will immediately register itself to meta when it bootstraps.
    /// The meta will assign it with a unique ID and set its state as `Starting`.
    /// When the worker node is fully ready to serve, it will request meta again
    /// (via `activate_worker_node`) to set its state to `Running`.
    pub async fn add_worker(
        &self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        property: AddNodeProperty,
        resource: PbResource,
    ) -> MetaResult<WorkerId> {
        self.inner
            .write()
            .await
            .add_worker(
                r#type,
                host_address,
                property,
                resource,
                self.max_heartbeat_interval,
            )
            .await
    }

    pub async fn activate_worker(&self, worker_id: WorkerId) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let worker = inner.activate_worker(worker_id).await?;

        // Notify frontends of new compute node.
        // Always notify because a running worker's property may have been changed.
        if worker.r#type() == PbWorkerType::ComputeNode {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Add, Info::Node(worker.clone()))
                .await;
        }
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::WorkerNodeActivated(worker))
            .await;

        Ok(())
    }

    pub async fn delete_worker(&self, host_address: HostAddress) -> MetaResult<WorkerNode> {
        let mut inner = self.inner.write().await;
        let worker = inner.delete_worker(host_address).await?;
        if worker.r#type() == PbWorkerType::ComputeNode {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Delete, Info::Node(worker.clone()))
                .await;
        }

        // Notify local subscribers.
        // Note: Any type of workers may pin some hummock resource. So `HummockManager` expect this
        // local notification.
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::WorkerNodeDeleted(worker.clone()))
            .await;

        Ok(worker)
    }

    pub async fn update_schedulability(
        &self,
        worker_ids: Vec<WorkerId>,
        schedulability: Schedulability,
    ) -> MetaResult<()> {
        self.inner
            .write()
            .await
            .update_schedulability(worker_ids, schedulability)
            .await
    }

    /// Invoked when it receives a heartbeat from a worker node.
    pub async fn heartbeat(&self, worker_id: WorkerId) -> MetaResult<()> {
        tracing::trace!(target: "events::meta::server_heartbeat", worker_id = worker_id, "receive heartbeat");
        self.inner
            .write()
            .await
            .heartbeat(worker_id, self.max_heartbeat_interval)
    }

    pub fn start_heartbeat_checker(
        cluster_controller: ClusterControllerRef,
        check_interval: Duration,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Heartbeat checker is stopped");
                        return;
                    }
                }

                let mut inner = cluster_controller.inner.write().await;
                // 1. Initialize new workers' TTL.
                for worker in inner
                    .worker_extra_info
                    .values_mut()
                    .filter(|worker| worker.expire_at.is_none())
                {
                    worker.update_ttl(cluster_controller.max_heartbeat_interval);
                }

                // 2. Collect expired workers.
                let now = timestamp_now_sec();
                let worker_to_delete = inner
                    .worker_extra_info
                    .iter()
                    .filter(|(_, info)| info.expire_at.unwrap() < now)
                    .map(|(id, _)| *id)
                    .collect_vec();

                // 3. Delete expired workers.
                let worker_infos = match Worker::find()
                    .select_only()
                    .column(worker::Column::WorkerId)
                    .column(worker::Column::WorkerType)
                    .column(worker::Column::Host)
                    .column(worker::Column::Port)
                    .filter(worker::Column::WorkerId.is_in(worker_to_delete.clone()))
                    .into_tuple::<(WorkerId, WorkerType, String, i32)>()
                    .all(&inner.db)
                    .await
                {
                    Ok(keys) => keys,
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "Failed to load expire worker info from db");
                        continue;
                    }
                };
                drop(inner);

                for (worker_id, worker_type, host, port) in worker_infos {
                    let host_addr = PbHostAddress { host, port };
                    match cluster_controller.delete_worker(host_addr.clone()).await {
                        Ok(_) => {
                            tracing::warn!(
                                worker_id,
                                ?host_addr,
                                %now,
                                "Deleted expired worker"
                            );
                            match worker_type {
                                WorkerType::Frontend
                                | WorkerType::ComputeNode
                                | WorkerType::Compactor
                                | WorkerType::RiseCtl => {
                                    cluster_controller
                                        .env
                                        .notification_manager()
                                        .delete_sender(worker_type.into(), WorkerKey(host_addr))
                                        .await
                                }
                                _ => {}
                            };
                        }
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "Failed to delete expire worker from db");
                        }
                    }
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    /// Get live nodes with the specified type and state.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `worker_state` Filter by this state if it is not None.
    pub async fn list_workers(
        &self,
        worker_type: Option<WorkerType>,
        worker_status: Option<WorkerStatus>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        let mut workers = vec![];
        // fill meta info.
        if worker_type.is_none() {
            workers.push(meta_node_info(
                &self.env.opts.advertise_addr,
                Some(self.started_at),
            ));
        }
        workers.extend(
            self.inner
                .read()
                .await
                .list_workers(worker_type, worker_status)
                .await?,
        );
        Ok(workers)
    }

    pub(crate) async fn subscribe_active_streaming_compute_nodes(
        &self,
    ) -> MetaResult<(Vec<WorkerNode>, UnboundedReceiver<LocalNotification>)> {
        let inner = self.inner.read().await;
        let worker_nodes = inner.list_active_streaming_workers().await?;
        let (tx, rx) = unbounded_channel();

        // insert before release the read lock to ensure that we don't lose any update in between
        self.env
            .notification_manager()
            .insert_local_sender(tx)
            .await;
        drop(inner);
        Ok((worker_nodes, rx))
    }

    /// A convenient method to get all running compute nodes that may have running actors on them
    /// i.e. CNs which are running
    pub async fn list_active_streaming_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.inner
            .read()
            .await
            .list_active_streaming_workers()
            .await
    }

    pub async fn list_active_worker_slots(&self) -> MetaResult<Vec<WorkerSlotId>> {
        self.inner.read().await.list_active_worker_slots().await
    }

    /// Get the cluster info used for scheduling a streaming job, containing all nodes that are
    /// running and schedulable
    pub async fn list_active_serving_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.inner.read().await.list_active_serving_workers().await
    }

    /// Get the cluster info used for scheduling a streaming job.
    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        self.inner.read().await.get_streaming_cluster_info().await
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        self.inner.read().await.get_worker_by_id(worker_id).await
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.inner
            .read()
            .await
            .get_worker_extra_info_by_id(worker_id)
    }

    pub fn cluster_id(&self) -> &ClusterId {
        self.env.cluster_id()
    }

    pub fn meta_store_endpoint(&self) -> String {
        self.env.meta_store_ref().endpoint.clone()
    }
}

/// The cluster info used for scheduling a streaming job.
#[derive(Debug, Clone)]
pub struct StreamingClusterInfo {
    /// All **active** compute nodes in the cluster.
    pub worker_nodes: HashMap<u32, WorkerNode>,

    /// All schedulable compute nodes in the cluster. Normally for resource group based scheduling.
    pub schedulable_workers: HashSet<u32>,

    /// All unschedulable compute nodes in the cluster.
    pub unschedulable_workers: HashSet<u32>,
}

// Encapsulating the use of parallelism
impl StreamingClusterInfo {
    pub fn parallelism(&self, resource_group: String) -> usize {
        let available_worker_ids =
            filter_workers_by_resource_group(&self.worker_nodes, resource_group.as_str());

        self.worker_nodes
            .values()
            .filter(|worker| available_worker_ids.contains(&(worker.id as WorkerId)))
            .map(|worker| worker.compute_node_parallelism())
            .sum()
    }

    pub fn filter_schedulable_workers_by_resource_group(
        &self,
        resource_group: &str,
    ) -> HashMap<u32, WorkerNode> {
        let worker_ids = filter_workers_by_resource_group(&self.worker_nodes, resource_group);
        self.worker_nodes
            .iter()
            .filter(|(id, _)| worker_ids.contains(&(**id as WorkerId)))
            .map(|(id, worker)| (*id, worker.clone()))
            .collect()
    }
}

#[derive(Default, Clone)]
pub struct WorkerExtraInfo {
    // Volatile values updated by meta node as follows.
    //
    // Unix timestamp that the worker will expire at.
    expire_at: Option<u64>,
    started_at: Option<u64>,
    resource: Option<PbResource>,
}

impl WorkerExtraInfo {
    fn update_ttl(&mut self, ttl: Duration) {
        let expire = cmp::max(
            self.expire_at.unwrap_or_default(),
            SystemTime::now()
                .add(ttl)
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Clock may have gone backwards")
                .as_secs(),
        );
        self.expire_at = Some(expire);
    }

    fn update_started_at(&mut self) {
        self.started_at = Some(timestamp_now_sec());
    }
}

fn timestamp_now_sec() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("Clock may have gone backwards")
        .as_secs()
}

fn meta_node_info(host: &str, started_at: Option<u64>) -> PbWorkerNode {
    PbWorkerNode {
        id: META_NODE_ID,
        r#type: WorkerType::Meta as _,
        host: HostAddr::try_from(host)
            .as_ref()
            .map(HostAddr::to_protobuf)
            .ok(),
        state: PbState::Running as _,
        property: None,
        transactional_id: None,
        resource: Some(risingwave_pb::common::worker_node::Resource {
            rw_version: RW_VERSION.to_owned(),
            total_memory_bytes: system_memory_available_bytes() as _,
            total_cpu_cores: total_cpu_available() as _,
        }),
        started_at,
    }
}

pub struct ClusterControllerInner {
    db: DatabaseConnection,
    /// Record for tracking available machine ids, one is available.
    available_transactional_ids: VecDeque<TransactionId>,
    worker_extra_info: HashMap<WorkerId, WorkerExtraInfo>,
    disable_automatic_parallelism_control: bool,
}

impl ClusterControllerInner {
    pub const MAX_WORKER_REUSABLE_ID_BITS: usize = 10;
    pub const MAX_WORKER_REUSABLE_ID_COUNT: usize = 1 << Self::MAX_WORKER_REUSABLE_ID_BITS;

    pub async fn new(
        db: DatabaseConnection,
        disable_automatic_parallelism_control: bool,
    ) -> MetaResult<Self> {
        let workers: Vec<(WorkerId, Option<TransactionId>)> = Worker::find()
            .select_only()
            .column(worker::Column::WorkerId)
            .column(worker::Column::TransactionId)
            .into_tuple()
            .all(&db)
            .await?;
        let inuse_txn_ids: HashSet<_> = workers
            .iter()
            .cloned()
            .filter_map(|(_, txn_id)| txn_id)
            .collect();
        let available_transactional_ids = (0..Self::MAX_WORKER_REUSABLE_ID_COUNT as TransactionId)
            .filter(|id| !inuse_txn_ids.contains(id))
            .collect();

        let worker_extra_info = workers
            .into_iter()
            .map(|(w, _)| (w, WorkerExtraInfo::default()))
            .collect();

        Ok(Self {
            db,
            available_transactional_ids,
            worker_extra_info,
            disable_automatic_parallelism_control,
        })
    }

    pub async fn count_worker_by_type(&self) -> MetaResult<HashMap<WorkerType, i64>> {
        let workers: Vec<(WorkerType, i64)> = Worker::find()
            .select_only()
            .column(worker::Column::WorkerType)
            .column_as(worker::Column::WorkerId.count(), "count")
            .group_by(worker::Column::WorkerType)
            .into_tuple()
            .all(&self.db)
            .await?;

        Ok(workers.into_iter().collect())
    }

    pub fn update_worker_ttl(&mut self, worker_id: WorkerId, ttl: Duration) -> MetaResult<()> {
        if let Some(info) = self.worker_extra_info.get_mut(&worker_id) {
            let expire = cmp::max(
                info.expire_at.unwrap_or_default(),
                SystemTime::now()
                    .add(ttl)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs(),
            );
            info.expire_at = Some(expire);
            Ok(())
        } else {
            Err(MetaError::invalid_worker(worker_id, "worker not found"))
        }
    }

    fn update_resource_and_started_at(
        &mut self,
        worker_id: WorkerId,
        resource: PbResource,
    ) -> MetaResult<()> {
        if let Some(info) = self.worker_extra_info.get_mut(&worker_id) {
            info.resource = Some(resource);
            info.update_started_at();
            Ok(())
        } else {
            Err(MetaError::invalid_worker(worker_id, "worker not found"))
        }
    }

    fn get_extra_info_checked(&self, worker_id: WorkerId) -> MetaResult<WorkerExtraInfo> {
        self.worker_extra_info
            .get(&worker_id)
            .cloned()
            .ok_or_else(|| MetaError::invalid_worker(worker_id, "worker not found"))
    }

    fn apply_transaction_id(&self, r#type: PbWorkerType) -> MetaResult<Option<TransactionId>> {
        match (self.available_transactional_ids.front(), r#type) {
            (None, _) => Err(MetaError::unavailable("no available reusable machine id")),
            // We only assign transactional id to compute node and frontend.
            (Some(id), PbWorkerType::ComputeNode | PbWorkerType::Frontend) => Ok(Some(*id)),
            _ => Ok(None),
        }
    }

    /// Check if the total CPU cores in the cluster exceed the license limit, after counting the
    /// newly joined compute node.
    pub async fn check_cpu_core_limit_on_newly_joined_compute_node(
        &self,
        txn: &DatabaseTransaction,
        host_address: &HostAddress,
        resource: &PbResource,
    ) -> MetaResult<()> {
        let this = resource.total_cpu_cores;

        let other_worker_ids: Vec<WorkerId> = Worker::find()
            .filter(
                (worker::Column::Host
                    .eq(host_address.host.clone())
                    .and(worker::Column::Port.eq(host_address.port)))
                .not()
                .and(worker::Column::WorkerType.eq(WorkerType::ComputeNode)),
            )
            .select_only()
            .column(worker::Column::WorkerId)
            .into_tuple()
            .all(txn)
            .await?;

        let others = other_worker_ids
            .into_iter()
            .flat_map(|id| self.worker_extra_info.get(&id))
            .flat_map(|info| info.resource.as_ref().map(|r| r.total_cpu_cores))
            .sum::<u64>();

        LicenseManager::get()
            .check_cpu_core_limit(this + others)
            .map_err(anyhow::Error::from)?;

        Ok(())
    }

    pub async fn add_worker(
        &mut self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        add_property: AddNodeProperty,
        resource: PbResource,
        ttl: Duration,
    ) -> MetaResult<WorkerId> {
        let txn = self.db.begin().await?;

        if let PbWorkerType::ComputeNode = r#type {
            self.check_cpu_core_limit_on_newly_joined_compute_node(&txn, &host_address, &resource)
                .await?;
        }

        let worker = Worker::find()
            .filter(
                worker::Column::Host
                    .eq(host_address.host.clone())
                    .and(worker::Column::Port.eq(host_address.port)),
            )
            .find_also_related(WorkerProperty)
            .one(&txn)
            .await?;
        // Worker already exist.
        if let Some((worker, property)) = worker {
            assert_eq!(worker.worker_type, r#type.into());
            return if worker.worker_type == WorkerType::ComputeNode {
                let property = property.unwrap();
                let mut current_parallelism = property.parallelism as usize;
                let new_parallelism = add_property.parallelism as usize;
                match new_parallelism.cmp(&current_parallelism) {
                    Ordering::Less => {
                        if !self.disable_automatic_parallelism_control {
                            // Handing over to the subsequent recovery loop for a forced reschedule.
                            tracing::info!(
                                "worker {} parallelism reduced from {} to {}",
                                worker.worker_id,
                                current_parallelism,
                                new_parallelism
                            );
                            current_parallelism = new_parallelism;
                        } else {
                            // Warn and keep the original parallelism if the worker registered with a
                            // smaller parallelism.
                            tracing::warn!(
                                "worker {} parallelism is less than current, current is {}, but received {}",
                                worker.worker_id,
                                current_parallelism,
                                new_parallelism
                            );
                        }
                    }
                    Ordering::Greater => {
                        tracing::info!(
                            "worker {} parallelism updated from {} to {}",
                            worker.worker_id,
                            current_parallelism,
                            new_parallelism
                        );
                        current_parallelism = new_parallelism;
                    }
                    Ordering::Equal => {}
                }
                let mut property: worker_property::ActiveModel = property.into();

                // keep `is_unschedulable` unchanged.
                property.is_streaming = Set(add_property.is_streaming);
                property.is_serving = Set(add_property.is_serving);
                property.parallelism = Set(current_parallelism as _);
                property.resource_group =
                    Set(Some(add_property.resource_group.unwrap_or_else(|| {
                        tracing::warn!(
                            "resource_group is not set for worker {}, fallback to `default`",
                            worker.worker_id
                        );
                        DEFAULT_RESOURCE_GROUP.to_owned()
                    })));

                WorkerProperty::update(property).exec(&txn).await?;
                txn.commit().await?;
                self.update_worker_ttl(worker.worker_id, ttl)?;
                self.update_resource_and_started_at(worker.worker_id, resource)?;
                Ok(worker.worker_id)
            } else if worker.worker_type == WorkerType::Frontend && property.is_none() {
                let worker_property = worker_property::ActiveModel {
                    worker_id: Set(worker.worker_id),
                    parallelism: Set(add_property
                        .parallelism
                        .try_into()
                        .expect("invalid parallelism")),
                    is_streaming: Set(add_property.is_streaming),
                    is_serving: Set(add_property.is_serving),
                    is_unschedulable: Set(add_property.is_unschedulable),
                    internal_rpc_host_addr: Set(Some(add_property.internal_rpc_host_addr)),
                    resource_group: Set(None),
                };
                WorkerProperty::insert(worker_property).exec(&txn).await?;
                txn.commit().await?;
                self.update_worker_ttl(worker.worker_id, ttl)?;
                self.update_resource_and_started_at(worker.worker_id, resource)?;
                Ok(worker.worker_id)
            } else {
                self.update_worker_ttl(worker.worker_id, ttl)?;
                self.update_resource_and_started_at(worker.worker_id, resource)?;
                Ok(worker.worker_id)
            };
        }
        let txn_id = self.apply_transaction_id(r#type)?;

        let worker = worker::ActiveModel {
            worker_id: Default::default(),
            worker_type: Set(r#type.into()),
            host: Set(host_address.host),
            port: Set(host_address.port),
            status: Set(WorkerStatus::Starting),
            transaction_id: Set(txn_id),
        };
        let insert_res = Worker::insert(worker).exec(&txn).await?;
        let worker_id = insert_res.last_insert_id as WorkerId;
        if r#type == PbWorkerType::ComputeNode || r#type == PbWorkerType::Frontend {
            let property = worker_property::ActiveModel {
                worker_id: Set(worker_id),
                parallelism: Set(add_property
                    .parallelism
                    .try_into()
                    .expect("invalid parallelism")),
                is_streaming: Set(add_property.is_streaming),
                is_serving: Set(add_property.is_serving),
                is_unschedulable: Set(add_property.is_unschedulable),
                internal_rpc_host_addr: Set(Some(add_property.internal_rpc_host_addr)),
                resource_group: if r#type == PbWorkerType::ComputeNode {
                    Set(add_property.resource_group.clone())
                } else {
                    Set(None)
                },
            };
            WorkerProperty::insert(property).exec(&txn).await?;
        }

        txn.commit().await?;
        if let Some(txn_id) = txn_id {
            self.available_transactional_ids.retain(|id| *id != txn_id);
        }
        let extra_info = WorkerExtraInfo {
            started_at: Some(timestamp_now_sec()),
            resource: Some(resource),
            ..Default::default()
        };
        self.worker_extra_info.insert(worker_id, extra_info);

        Ok(worker_id)
    }

    pub async fn activate_worker(&self, worker_id: WorkerId) -> MetaResult<PbWorkerNode> {
        let worker = worker::ActiveModel {
            worker_id: Set(worker_id),
            status: Set(WorkerStatus::Running),
            ..Default::default()
        };

        let worker = worker.update(&self.db).await?;
        let worker_property = WorkerProperty::find_by_id(worker.worker_id)
            .one(&self.db)
            .await?;
        let extra_info = self.get_extra_info_checked(worker_id)?;
        Ok(WorkerInfo(worker, worker_property, extra_info).into())
    }

    pub async fn update_schedulability(
        &self,
        worker_ids: Vec<WorkerId>,
        schedulability: Schedulability,
    ) -> MetaResult<()> {
        let is_unschedulable = schedulability == Schedulability::Unschedulable;
        WorkerProperty::update_many()
            .col_expr(
                worker_property::Column::IsUnschedulable,
                Expr::value(is_unschedulable),
            )
            .filter(worker_property::Column::WorkerId.is_in(worker_ids))
            .exec(&self.db)
            .await?;

        Ok(())
    }

    pub async fn delete_worker(&mut self, host_addr: HostAddress) -> MetaResult<PbWorkerNode> {
        let worker = Worker::find()
            .filter(
                worker::Column::Host
                    .eq(host_addr.host)
                    .and(worker::Column::Port.eq(host_addr.port)),
            )
            .find_also_related(WorkerProperty)
            .one(&self.db)
            .await?;
        let Some((worker, property)) = worker else {
            return Err(MetaError::invalid_parameter("worker not found!"));
        };

        let res = Worker::delete_by_id(worker.worker_id)
            .exec(&self.db)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::invalid_parameter("worker not found!"));
        }

        let extra_info = self.worker_extra_info.remove(&worker.worker_id).unwrap();
        if let Some(txn_id) = &worker.transaction_id {
            self.available_transactional_ids.push_back(*txn_id);
        }
        Ok(WorkerInfo(worker, property, extra_info).into())
    }

    pub fn heartbeat(&mut self, worker_id: WorkerId, ttl: Duration) -> MetaResult<()> {
        if let Some(worker_info) = self.worker_extra_info.get_mut(&worker_id) {
            worker_info.update_ttl(ttl);
            Ok(())
        } else {
            Err(MetaError::invalid_worker(worker_id, "worker not found"))
        }
    }

    pub async fn list_workers(
        &self,
        worker_type: Option<WorkerType>,
        worker_status: Option<WorkerStatus>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        let mut find = Worker::find();
        if let Some(worker_type) = worker_type {
            find = find.filter(worker::Column::WorkerType.eq(worker_type));
        }
        if let Some(worker_status) = worker_status {
            find = find.filter(worker::Column::Status.eq(worker_status));
        }
        let workers = find.find_also_related(WorkerProperty).all(&self.db).await?;
        Ok(workers
            .into_iter()
            .map(|(worker, property)| {
                let extra_info = self.get_extra_info_checked(worker.worker_id).unwrap();
                WorkerInfo(worker, property, extra_info).into()
            })
            .collect_vec())
    }

    pub async fn list_active_streaming_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        let workers = Worker::find()
            .filter(
                worker::Column::WorkerType
                    .eq(WorkerType::ComputeNode)
                    .and(worker::Column::Status.eq(WorkerStatus::Running)),
            )
            .inner_join(WorkerProperty)
            .select_also(WorkerProperty)
            .filter(worker_property::Column::IsStreaming.eq(true))
            .all(&self.db)
            .await?;

        Ok(workers
            .into_iter()
            .map(|(worker, property)| {
                let extra_info = self.get_extra_info_checked(worker.worker_id).unwrap();
                WorkerInfo(worker, property, extra_info).into()
            })
            .collect_vec())
    }

    pub async fn list_active_worker_slots(&self) -> MetaResult<Vec<WorkerSlotId>> {
        let worker_parallelisms: Vec<(WorkerId, i32)> = WorkerProperty::find()
            .select_only()
            .column(worker_property::Column::WorkerId)
            .column(worker_property::Column::Parallelism)
            .inner_join(Worker)
            .filter(worker::Column::Status.eq(WorkerStatus::Running))
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(worker_parallelisms
            .into_iter()
            .flat_map(|(worker_id, parallelism)| {
                (0..parallelism).map(move |idx| WorkerSlotId::new(worker_id as u32, idx as usize))
            })
            .collect_vec())
    }

    pub async fn list_active_serving_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        let workers = Worker::find()
            .filter(
                worker::Column::WorkerType
                    .eq(WorkerType::ComputeNode)
                    .and(worker::Column::Status.eq(WorkerStatus::Running)),
            )
            .inner_join(WorkerProperty)
            .select_also(WorkerProperty)
            .filter(worker_property::Column::IsServing.eq(true))
            .all(&self.db)
            .await?;

        Ok(workers
            .into_iter()
            .map(|(worker, property)| {
                let extra_info = self.get_extra_info_checked(worker.worker_id).unwrap();
                WorkerInfo(worker, property, extra_info).into()
            })
            .collect_vec())
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        let mut streaming_workers = self.list_active_streaming_workers().await?;

        let unschedulable_workers: HashSet<_> = streaming_workers
            .extract_if(|worker| {
                worker
                    .property
                    .as_ref()
                    .map_or(false, |p| p.is_unschedulable)
            })
            .map(|w| w.id)
            .collect();

        let schedulable_workers = streaming_workers
            .iter()
            .map(|worker| worker.id)
            .filter(|id| !unschedulable_workers.contains(id))
            .collect();

        let active_workers: HashMap<_, _> =
            streaming_workers.into_iter().map(|w| (w.id, w)).collect();

        Ok(StreamingClusterInfo {
            worker_nodes: active_workers,
            schedulable_workers,
            unschedulable_workers,
        })
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        let worker = Worker::find_by_id(worker_id)
            .find_also_related(WorkerProperty)
            .one(&self.db)
            .await?;
        if worker.is_none() {
            return Ok(None);
        }
        let extra_info = self.get_extra_info_checked(worker_id)?;
        Ok(worker.map(|(w, p)| WorkerInfo(w, p, extra_info).into()))
    }

    pub fn get_worker_extra_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.worker_extra_info.get(&worker_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_worker_hosts_for_test(count: usize) -> Vec<HostAddress> {
        (0..count)
            .map(|i| HostAddress {
                host: "localhost".to_owned(),
                port: 5000 + i as i32,
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_cluster_controller() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_ctl = ClusterController::new(env, Duration::from_secs(1)).await?;

        let parallelism_num = 4_usize;
        let worker_count = 5_usize;
        let property = AddNodeProperty {
            parallelism: parallelism_num as _,
            is_streaming: true,
            is_serving: true,
            is_unschedulable: false,
            ..Default::default()
        };
        let hosts = mock_worker_hosts_for_test(worker_count);
        let mut worker_ids = vec![];
        for host in &hosts {
            worker_ids.push(
                cluster_ctl
                    .add_worker(
                        PbWorkerType::ComputeNode,
                        host.clone(),
                        property.clone(),
                        PbResource::default(),
                    )
                    .await?,
            );
        }

        // Since no worker is active, the parallelism should be 0.
        assert_eq!(cluster_ctl.list_active_worker_slots().await?.len(), 0);

        for id in &worker_ids {
            cluster_ctl.activate_worker(*id).await?;
        }
        let worker_cnt_map = cluster_ctl.count_worker_by_type().await?;
        assert_eq!(
            *worker_cnt_map.get(&WorkerType::ComputeNode).unwrap() as usize,
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_streaming_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_serving_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_worker_slots().await?.len(),
            parallelism_num * worker_count
        );

        // re-register existing worker node with larger parallelism and change its serving mode.
        let mut new_property = property.clone();
        new_property.parallelism = (parallelism_num * 2) as _;
        new_property.is_serving = false;
        cluster_ctl
            .add_worker(
                PbWorkerType::ComputeNode,
                hosts[0].clone(),
                new_property,
                PbResource::default(),
            )
            .await?;

        assert_eq!(
            cluster_ctl.list_active_streaming_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_serving_workers().await?.len(),
            worker_count - 1
        );
        let worker_slots = cluster_ctl.list_active_worker_slots().await?;
        assert!(worker_slots.iter().all_unique());
        assert_eq!(worker_slots.len(), parallelism_num * (worker_count + 1));

        // delete workers.
        for host in hosts {
            cluster_ctl.delete_worker(host).await?;
        }
        assert_eq!(cluster_ctl.list_active_streaming_workers().await?.len(), 0);
        assert_eq!(cluster_ctl.list_active_serving_workers().await?.len(), 0);
        assert_eq!(cluster_ctl.list_active_worker_slots().await?.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_schedulability() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_ctl = ClusterController::new(env, Duration::from_secs(1)).await?;

        let host = HostAddress {
            host: "localhost".to_owned(),
            port: 5001,
        };
        let mut property = AddNodeProperty {
            is_streaming: true,
            is_serving: true,
            is_unschedulable: false,
            parallelism: 4,
            ..Default::default()
        };
        let worker_id = cluster_ctl
            .add_worker(
                PbWorkerType::ComputeNode,
                host.clone(),
                property.clone(),
                PbResource::default(),
            )
            .await?;

        cluster_ctl.activate_worker(worker_id).await?;
        cluster_ctl
            .update_schedulability(vec![worker_id], Schedulability::Unschedulable)
            .await?;

        let workers = cluster_ctl.list_active_streaming_workers().await?;
        assert_eq!(workers.len(), 1);
        assert!(workers[0].property.as_ref().unwrap().is_unschedulable);

        // re-register existing worker node and change its serving mode, the schedulable state should not be changed.
        property.is_unschedulable = false;
        property.is_serving = false;
        let new_worker_id = cluster_ctl
            .add_worker(
                PbWorkerType::ComputeNode,
                host.clone(),
                property,
                PbResource::default(),
            )
            .await?;
        assert_eq!(worker_id, new_worker_id);

        let workers = cluster_ctl.list_active_streaming_workers().await?;
        assert_eq!(workers.len(), 1);
        assert!(workers[0].property.as_ref().unwrap().is_unschedulable);

        cluster_ctl.delete_worker(host).await?;

        Ok(())
    }
}
