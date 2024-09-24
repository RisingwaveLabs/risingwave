// Copyright 2024 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_batch::worker_manager::worker_node_manager::WorkerNodeManagerRef;
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common_service::ObserverState;
use risingwave_hummock_sdk::FrontendHummockVersion;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::{HummockVersionDeltas, HummockVersionStats};
use risingwave_pb::meta::relation::RelationInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{FragmentWorkerSlotMapping, MetaSnapshot, SubscribeResponse};
use risingwave_rpc_client::ComputeClientPoolRef;
use tokio::sync::watch::Sender;

use crate::catalog::root_catalog::Catalog;
use crate::catalog::{FragmentId, SecretId};
use crate::scheduler::HummockSnapshotManagerRef;
use crate::session::SessionMapRef;
use crate::user::user_manager::UserInfoManager;
use crate::user::UserInfoVersion;

pub struct FrontendObserverNode {
    worker_node_manager: WorkerNodeManagerRef,
    catalog: Arc<RwLock<Catalog>>,
    catalog_updated_tx: Sender<CatalogVersion>,
    user_info_manager: Arc<RwLock<UserInfoManager>>,
    user_info_updated_tx: Sender<UserInfoVersion>,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    system_params_manager: LocalSystemParamsManagerRef,
    session_params: Arc<RwLock<SessionConfig>>,
    compute_client_pool: ComputeClientPoolRef,
    sessions_map: SessionMapRef,
}

impl ObserverState for FrontendObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Frontend
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        // TODO: this clone can be avoided
        match info.to_owned() {
            Info::Database(_)
            | Info::Schema(_)
            | Info::RelationGroup(_)
            | Info::Function(_)
            | Info::Connection(_) => {
                self.handle_catalog_notification(resp);
            }
            Info::Secret(_) => {
                self.handle_catalog_notification(resp.clone());
                self.handle_secret_notification(resp);
            }
            Info::Node(node) => {
                self.update_worker_node_manager(resp.operation(), node);
            }
            Info::User(_) => {
                self.handle_user_notification(resp);
            }
            Info::Snapshot(_) => {
                panic!(
                    "receiving a snapshot in the middle is unsupported now {:?}",
                    resp
                )
            }
            Info::HummockVersionDeltas(deltas) => {
                let table_ids = deltas
                    .version_deltas
                    .iter()
                    .flat_map(|version_deltas| version_deltas.change_log_delta.keys())
                    .map(|table_id| TableId::new(*table_id))
                    .collect_vec();
                self.handle_hummock_snapshot_notification(deltas);
                self.handle_cursor_notify(table_ids);
            }
            Info::MetaBackupManifestId(_) => {
                panic!("frontend node should not receive MetaBackupManifestId");
            }
            Info::HummockWriteLimits(_) => {
                panic!("frontend node should not receive HummockWriteLimits");
            }
            Info::SystemParams(p) => {
                self.system_params_manager.try_set_params(p);
            }
            Info::SessionParam(p) => {
                self.session_params
                    .write()
                    .set(&p.param, p.value().to_string(), &mut ())
                    .unwrap();
            }
            Info::HummockStats(stats) => {
                self.handle_table_stats_notification(stats);
            }
            Info::StreamingWorkerSlotMapping(_) => self.handle_fragment_mapping_notification(resp),
            Info::ServingWorkerSlotMappings(m) => {
                self.handle_fragment_serving_mapping_notification(m.mappings, resp.operation())
            }
            Info::Recovery(_) => {
                self.compute_client_pool.invalidate_all();
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        let mut catalog_guard = self.catalog.write();
        let mut user_guard = self.user_info_manager.write();
        catalog_guard.clear();
        user_guard.clear();

        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };
        let MetaSnapshot {
            databases,
            schemas,
            sources,
            sinks,
            tables,
            indexes,
            views,
            subscriptions,
            functions,
            connections,
            users,
            nodes,
            hummock_version,
            meta_backup_manifest_id: _,
            hummock_write_limits: _,
            streaming_worker_slot_mappings,
            serving_worker_slot_mappings,
            session_params,
            version,
            secrets,
        } = snapshot;

        for db in databases {
            catalog_guard.create_database(&db)
        }
        for schema in schemas {
            catalog_guard.create_schema(&schema)
        }
        for source in sources {
            catalog_guard.create_source(&source)
        }
        for sink in sinks {
            catalog_guard.create_sink(&sink)
        }
        for subscription in subscriptions {
            catalog_guard.create_subscription(&subscription)
        }
        for table in tables {
            catalog_guard.create_table(&table)
        }
        for index in indexes {
            catalog_guard.create_index(&index)
        }
        for view in views {
            catalog_guard.create_view(&view)
        }
        for function in functions {
            catalog_guard.create_function(&function)
        }
        for connection in connections {
            catalog_guard.create_connection(&connection)
        }
        for secret in &secrets {
            catalog_guard.create_secret(secret)
        }
        for user in users {
            user_guard.create_user(user)
        }

        self.worker_node_manager.refresh(
            nodes,
            convert_worker_slot_mapping(&streaming_worker_slot_mappings),
            convert_worker_slot_mapping(&serving_worker_slot_mappings),
        );
        let hummock_version = FrontendHummockVersion::from_protobuf(hummock_version.unwrap());
        let table_ids = hummock_version
            .table_change_log
            .keys()
            .cloned()
            .collect_vec();
        self.hummock_snapshot_manager.init(hummock_version);
        self.handle_cursor_notify(table_ids);

        let snapshot_version = version.unwrap();
        catalog_guard.set_version(snapshot_version.catalog_version);
        self.catalog_updated_tx
            .send(snapshot_version.catalog_version)
            .unwrap();
        user_guard.set_version(snapshot_version.catalog_version);
        self.user_info_updated_tx
            .send(snapshot_version.catalog_version)
            .unwrap();
        *self.session_params.write() =
            serde_json::from_str(&session_params.unwrap().params).unwrap();
        LocalSecretManager::global().init_secrets(secrets);
    }
}

impl FrontendObserverNode {
    pub fn new(
        worker_node_manager: WorkerNodeManagerRef,
        catalog: Arc<RwLock<Catalog>>,
        catalog_updated_tx: Sender<CatalogVersion>,
        user_info_manager: Arc<RwLock<UserInfoManager>>,
        user_info_updated_tx: Sender<UserInfoVersion>,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
        system_params_manager: LocalSystemParamsManagerRef,
        session_params: Arc<RwLock<SessionConfig>>,
        compute_client_pool: ComputeClientPoolRef,
        sessions_map: SessionMapRef,
    ) -> Self {
        Self {
            worker_node_manager,
            catalog,
            catalog_updated_tx,
            user_info_manager,
            user_info_updated_tx,
            hummock_snapshot_manager,
            system_params_manager,
            session_params,
            compute_client_pool,
            sessions_map,
        }
    }

    fn handle_table_stats_notification(&mut self, table_stats: HummockVersionStats) {
        let mut catalog_guard = self.catalog.write();
        catalog_guard.set_table_stats(table_stats);
    }

    fn handle_catalog_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let mut catalog_guard = self.catalog.write();
        match info {
            Info::Database(database) => match resp.operation() {
                Operation::Add => catalog_guard.create_database(database),
                Operation::Delete => {
                    let table_ids = catalog_guard.get_all_tables_id_in_database(database.id);
                    catalog_guard.drop_database(database.id);
                    self.handle_cursor_remove_table_ids(table_ids);
                }
                Operation::Update => catalog_guard.update_database(database),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Schema(schema) => match resp.operation() {
                Operation::Add => catalog_guard.create_schema(schema),
                Operation::Delete => {
                    let table_ids =
                        catalog_guard.get_all_tables_id_in_schema(schema.database_id, schema.id);
                    catalog_guard.drop_schema(schema.database_id, schema.id);
                    self.handle_cursor_remove_table_ids(table_ids);
                }
                Operation::Update => catalog_guard.update_schema(schema),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::RelationGroup(relation_group) => {
                for relation in &relation_group.relations {
                    let Some(relation) = relation.relation_info.as_ref() else {
                        continue;
                    };
                    match relation {
                        RelationInfo::Table(table) => match resp.operation() {
                            Operation::Add => catalog_guard.create_table(table),
                            Operation::Delete => {
                                catalog_guard.drop_table(
                                    table.database_id,
                                    table.schema_id,
                                    table.id.into(),
                                );
                                self.handle_cursor_remove_table_ids(vec![table.id.into()]);
                            }
                            Operation::Update => {
                                let old_fragment_id = catalog_guard
                                    .get_any_table_by_id(&table.id.into())
                                    .unwrap()
                                    .fragment_id;
                                catalog_guard.update_table(table);
                                if old_fragment_id != table.fragment_id {
                                    // FIXME: the frontend node delete its fragment for the update
                                    // operation by itself.
                                    self.worker_node_manager
                                        .remove_streaming_fragment_mapping(&old_fragment_id);
                                }
                            }
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        RelationInfo::Source(source) => match resp.operation() {
                            Operation::Add => catalog_guard.create_source(source),
                            Operation::Delete => catalog_guard.drop_source(
                                source.database_id,
                                source.schema_id,
                                source.id,
                            ),
                            Operation::Update => catalog_guard.update_source(source),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        RelationInfo::Sink(sink) => match resp.operation() {
                            Operation::Add => catalog_guard.create_sink(sink),
                            Operation::Delete => {
                                catalog_guard.drop_sink(sink.database_id, sink.schema_id, sink.id)
                            }
                            Operation::Update => catalog_guard.update_sink(sink),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        RelationInfo::Subscription(subscription) => match resp.operation() {
                            Operation::Add => catalog_guard.create_subscription(subscription),
                            Operation::Delete => catalog_guard.drop_subscription(
                                subscription.database_id,
                                subscription.schema_id,
                                subscription.id,
                            ),
                            Operation::Update => catalog_guard.update_subscription(subscription),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        RelationInfo::Index(index) => match resp.operation() {
                            Operation::Add => catalog_guard.create_index(index),
                            Operation::Delete => catalog_guard.drop_index(
                                index.database_id,
                                index.schema_id,
                                index.id.into(),
                            ),
                            Operation::Update => catalog_guard.update_index(index),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                        RelationInfo::View(view) => match resp.operation() {
                            Operation::Add => catalog_guard.create_view(view),
                            Operation::Delete => {
                                catalog_guard.drop_view(view.database_id, view.schema_id, view.id)
                            }
                            Operation::Update => catalog_guard.update_view(view),
                            _ => panic!("receive an unsupported notify {:?}", resp),
                        },
                    }
                }
            }
            Info::Function(function) => match resp.operation() {
                Operation::Add => catalog_guard.create_function(function),
                Operation::Delete => catalog_guard.drop_function(
                    function.database_id,
                    function.schema_id,
                    function.id.into(),
                ),
                Operation::Update => catalog_guard.update_function(function),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Connection(connection) => match resp.operation() {
                Operation::Add => catalog_guard.create_connection(connection),
                Operation::Delete => catalog_guard.drop_connection(
                    connection.database_id,
                    connection.schema_id,
                    connection.id,
                ),
                Operation::Update => catalog_guard.update_connection(connection),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            Info::Secret(secret) => {
                let mut secret = secret.clone();
                // The secret value should not be revealed to users. So mask it in the frontend catalog.
                secret.value = "SECRET VALUE SHOULD NOT BE REVEALED".as_bytes().to_vec();
                match resp.operation() {
                    Operation::Add => catalog_guard.create_secret(&secret),
                    Operation::Delete => catalog_guard.drop_secret(
                        secret.database_id,
                        secret.schema_id,
                        SecretId::new(secret.id),
                    ),
                    Operation::Update => catalog_guard.update_secret(&secret),
                    _ => panic!("receive an unsupported notify {:?}", resp),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            resp.version > catalog_guard.version(),
            "resp version={:?}, current version={:?}",
            resp.version,
            catalog_guard.version()
        );
        catalog_guard.set_version(resp.version);
        self.catalog_updated_tx.send(resp.version).unwrap();
    }

    fn handle_user_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let mut user_guard = self.user_info_manager.write();
        match info {
            Info::User(user) => match resp.operation() {
                Operation::Add => user_guard.create_user(user.clone()),
                Operation::Delete => user_guard.drop_user(user.id),
                Operation::Update => user_guard.update_user(user.clone()),
                _ => panic!("receive an unsupported notify {:?}", resp),
            },
            _ => unreachable!(),
        }
        assert!(
            resp.version > user_guard.version(),
            "resp version={:?}, current version={:?}",
            resp.version,
            user_guard.version()
        );
        user_guard.set_version(resp.version);
        self.user_info_updated_tx.send(resp.version).unwrap();
    }

    fn handle_fragment_mapping_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };
        match info {
            Info::StreamingWorkerSlotMapping(streaming_worker_slot_mapping) => {
                let fragment_id = streaming_worker_slot_mapping.fragment_id;
                let mapping = || {
                    WorkerSlotMapping::from_protobuf(
                        streaming_worker_slot_mapping.mapping.as_ref().unwrap(),
                    )
                };

                match resp.operation() {
                    Operation::Add => {
                        self.worker_node_manager
                            .insert_streaming_fragment_mapping(fragment_id, mapping());
                    }
                    Operation::Delete => {
                        self.worker_node_manager
                            .remove_streaming_fragment_mapping(&fragment_id);
                    }
                    Operation::Update => {
                        self.worker_node_manager
                            .update_streaming_fragment_mapping(fragment_id, mapping());
                    }
                    _ => panic!("receive an unsupported notify {:?}", resp),
                }
            }
            _ => unreachable!(),
        }
    }

    fn handle_fragment_serving_mapping_notification(
        &mut self,
        mappings: Vec<FragmentWorkerSlotMapping>,
        op: Operation,
    ) {
        match op {
            Operation::Add | Operation::Update => {
                self.worker_node_manager
                    .upsert_serving_fragment_mapping(convert_worker_slot_mapping(&mappings));
            }
            Operation::Delete => self.worker_node_manager.remove_serving_fragment_mapping(
                &mappings.into_iter().map(|m| m.fragment_id).collect_vec(),
            ),
            Operation::Snapshot => {
                self.worker_node_manager
                    .set_serving_fragment_mapping(convert_worker_slot_mapping(&mappings));
            }
            _ => panic!("receive an unsupported notify {:?}", op),
        }
    }

    /// Update max committed epoch in `HummockSnapshotManager`.
    fn handle_hummock_snapshot_notification(&self, deltas: HummockVersionDeltas) {
        self.hummock_snapshot_manager.update(deltas);
    }

    fn handle_cursor_notify(&self, table_ids: Vec<TableId>) {
        for session in self.sessions_map.read().values() {
            session
                .get_cursor_manager()
                .get_cursor_notifies()
                .notify_cursors(&table_ids);
        }
    }

    fn handle_cursor_remove_table_ids(&self, table_ids: Vec<TableId>) {
        for session in self.sessions_map.read().values() {
            session
                .get_cursor_manager()
                .get_cursor_notifies()
                .remove_tables_ids(&table_ids);
        }
    }

    fn handle_secret_notification(&mut self, resp: SubscribeResponse) {
        let resp_op = resp.operation();
        let Some(Info::Secret(secret)) = resp.info else {
            unreachable!();
        };
        match resp_op {
            Operation::Add => {
                LocalSecretManager::global().add_secret(secret.id, secret.value);
            }
            Operation::Delete => {
                LocalSecretManager::global().remove_secret(secret.id);
            }
            _ => {
                panic!("error type notification");
            }
        }
    }

    /// `update_worker_node_manager` is called in `start` method.
    /// It calls `add_worker_node` and `remove_worker_node` of `WorkerNodeManager`.
    fn update_worker_node_manager(&self, operation: Operation, node: WorkerNode) {
        tracing::debug!(
            "Update worker nodes, operation: {:?}, node: {:?}",
            operation,
            node
        );

        match operation {
            Operation::Add => self.worker_node_manager.add_worker_node(node),
            Operation::Delete => self.worker_node_manager.remove_worker_node(node),
            _ => (),
        }
    }
}

fn convert_worker_slot_mapping(
    worker_slot_mappings: &[FragmentWorkerSlotMapping],
) -> HashMap<FragmentId, WorkerSlotMapping> {
    worker_slot_mappings
        .iter()
        .map(
            |FragmentWorkerSlotMapping {
                 fragment_id,
                 mapping,
             }| {
                let mapping = WorkerSlotMapping::from_protobuf(mapping.as_ref().unwrap());
                (*fragment_id, mapping)
            },
        )
        .collect()
}
