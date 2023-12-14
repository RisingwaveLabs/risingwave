// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta::MetaResult;
use risingwave_pb::backup_service::MetaBackupManifestId;
use risingwave_pb::catalog::Table;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::hummock::WriteLimits;
use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    FragmentParallelUnitMapping, MetaSnapshot, SubscribeRequest, SubscribeType,
};
use risingwave_pb::user::UserInfo;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status};

use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::{Catalog, MetaSrvEnv, Notification, NotificationVersion, WorkerKey};
use crate::serving::ServingVnodeMappingRef;

pub struct NotificationServiceImpl {
    env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    serving_vnode_mapping: ServingVnodeMappingRef,
}

impl NotificationServiceImpl {
    pub fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        backup_manager: BackupManagerRef,
        serving_vnode_mapping: ServingVnodeMappingRef,
    ) -> Self {
        Self {
            env,
            metadata_manager,
            hummock_manager,
            backup_manager,
            serving_vnode_mapping,
        }
    }

    async fn get_catalog_snapshot(
        &self,
    ) -> MetaResult<(Catalog, Vec<UserInfo>, NotificationVersion)> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let catalog_guard = mgr.catalog_manager.get_catalog_core_guard().await;
                let (
                    databases,
                    schemas,
                    tables,
                    sources,
                    sinks,
                    indexes,
                    views,
                    functions,
                    connections,
                ) = catalog_guard.database.get_catalog();
                let users = catalog_guard.user.list_users();
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((
                    (
                        databases,
                        schemas,
                        tables,
                        sources,
                        sinks,
                        indexes,
                        views,
                        functions,
                        connections,
                    ),
                    users,
                    notification_version,
                ))
            }
            MetadataManager::V2(mgr) => {
                let catalog_guard = mgr.catalog_controller.get_inner_read_guard().await;
                let (
                    (
                        databases,
                        schemas,
                        tables,
                        sources,
                        sinks,
                        indexes,
                        views,
                        functions,
                        connections,
                    ),
                    users,
                ) = catalog_guard.snapshot().await?;
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((
                    (
                        databases,
                        schemas,
                        tables,
                        sources,
                        sinks,
                        indexes,
                        views,
                        functions,
                        connections,
                    ),
                    users,
                    notification_version,
                ))
            }
        }
    }

    async fn get_parallel_unit_mapping_snapshot(
        &self,
    ) -> MetaResult<(Vec<FragmentParallelUnitMapping>, NotificationVersion)> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let fragment_guard = mgr.fragment_manager.get_fragment_read_guard().await;
                let parallel_unit_mappings =
                    fragment_guard.all_running_fragment_mappings().collect_vec();
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((parallel_unit_mappings, notification_version))
            }
            MetadataManager::V2(mgr) => {
                let fragment_guard = mgr.catalog_controller.get_inner_read_guard().await;
                let parallel_unit_mappings = fragment_guard
                    .all_running_fragment_mappings()
                    .await?
                    .collect_vec();
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((parallel_unit_mappings, notification_version))
            }
        }
    }

    fn get_serving_vnode_mappings(&self) -> Vec<FragmentParallelUnitMapping> {
        self.serving_vnode_mapping
            .all()
            .iter()
            .map(|(fragment_id, mapping)| FragmentParallelUnitMapping {
                fragment_id: *fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect()
    }

    async fn get_worker_node_snapshot(&self) -> MetaResult<(Vec<WorkerNode>, NotificationVersion)> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let cluster_guard = mgr.cluster_manager.get_cluster_core_guard().await;
                let nodes =
                    cluster_guard.list_worker_node(Some(WorkerType::ComputeNode), Some(Running));
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((nodes, notification_version))
            }
            MetadataManager::V2(mgr) => {
                let cluster_guard = mgr.cluster_controller.get_inner_read_guard().await;
                let nodes = cluster_guard
                    .list_workers(Some(WorkerType::ComputeNode.into()), Some(Running.into()))
                    .await?;
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((nodes, notification_version))
            }
        }
    }

    async fn get_tables_and_creating_tables_snapshot(
        &self,
    ) -> MetaResult<(Vec<Table>, NotificationVersion)> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let catalog_guard = mgr.catalog_manager.get_catalog_core_guard().await;
                let tables = catalog_guard.database.list_tables();
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((tables, notification_version))
            }
            MetadataManager::V2(mgr) => {
                let catalog_guard = mgr.catalog_controller.get_inner_read_guard().await;
                let tables = catalog_guard.list_all_state_tables().await?;
                let notification_version = self.env.notification_manager().current_version().await;
                Ok((tables, notification_version))
            }
        }
    }

    async fn compactor_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (tables, catalog_version) = self.get_tables_and_creating_tables_snapshot().await?;

        Ok(MetaSnapshot {
            tables,
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    async fn frontend_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (
            (databases, schemas, tables, sources, sinks, indexes, views, functions, connections),
            users,
            catalog_version,
        ) = self.get_catalog_snapshot().await?;
        let (parallel_unit_mappings, parallel_unit_mapping_version) =
            self.get_parallel_unit_mapping_snapshot().await?;
        let serving_parallel_unit_mappings = self.get_serving_vnode_mappings();
        let (nodes, worker_node_version) = self.get_worker_node_snapshot().await?;

        let hummock_snapshot = Some(self.hummock_manager.latest_snapshot());

        Ok(MetaSnapshot {
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            views,
            functions,
            connections,
            users,
            parallel_unit_mappings,
            nodes,
            hummock_snapshot,
            serving_parallel_unit_mappings,
            version: Some(SnapshotVersion {
                catalog_version,
                parallel_unit_mapping_version,
                worker_node_version,
            }),
            ..Default::default()
        })
    }

    async fn hummock_subscribe(&self) -> MetaResult<MetaSnapshot> {
        let (tables, catalog_version) = self.get_tables_and_creating_tables_snapshot().await?;
        let hummock_version = self.hummock_manager.get_current_version().await;
        let hummock_write_limits = self.hummock_manager.write_limits().await;
        let meta_backup_manifest_id = self.backup_manager.manifest().manifest_id;

        Ok(MetaSnapshot {
            tables,
            hummock_version: Some(hummock_version),
            version: Some(SnapshotVersion {
                catalog_version,
                ..Default::default()
            }),
            meta_backup_manifest_id: Some(MetaBackupManifestId {
                id: meta_backup_manifest_id,
            }),
            hummock_write_limits: Some(WriteLimits {
                write_limits: hummock_write_limits,
            }),
            ..Default::default()
        })
    }

    fn compute_subscribe(&self) -> MetaSnapshot {
        MetaSnapshot::default()
    }
}

#[async_trait::async_trait]
impl NotificationService for NotificationServiceImpl {
    type SubscribeStream = UnboundedReceiverStream<Notification>;

    #[cfg_attr(coverage, coverage(off))]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        let host_address = req.get_host()?.clone();
        let subscribe_type = req.get_subscribe_type()?;

        let worker_key = WorkerKey(host_address);

        let (tx, rx) = mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_sender(subscribe_type, worker_key.clone(), tx)
            .await;

        let meta_snapshot = match subscribe_type {
            SubscribeType::Compactor => self.compactor_subscribe().await?,
            SubscribeType::Frontend => {
                self.hummock_manager
                    .pin_snapshot(req.get_worker_id())
                    .await?;
                self.frontend_subscribe().await?
            }
            SubscribeType::Hummock => {
                self.hummock_manager
                    .pin_version(req.get_worker_id())
                    .await?;
                self.hummock_subscribe().await?
            }
            SubscribeType::Compute => self.compute_subscribe(),
            SubscribeType::Unspecified => unreachable!(),
        };

        self.env
            .notification_manager()
            .notify_snapshot(worker_key, subscribe_type, meta_snapshot);

        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }
}
