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

use risingwave_common::license::LicenseManager;
use risingwave_common_service::ObserverState;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_trace::TraceSpan;
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::UnboundedSender;

use crate::compaction_catalog_manager::CompactionCatalogManagerRef;
use crate::hummock::backup_reader::BackupReaderRef;
use crate::hummock::event_handler::HummockVersionUpdate;
use crate::hummock::write_limiter::WriteLimiterRef;

pub struct HummockObserverNode {
    compaction_catalog_manager: CompactionCatalogManagerRef,
    backup_reader: BackupReaderRef,
    write_limiter: WriteLimiterRef,
    version_update_sender: UnboundedSender<HummockVersionUpdate>,
    version: u64,
}

impl ObserverState for HummockObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Hummock
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        match info.to_owned() {
            Info::ObjectGroup(object_group) => {
                for object in object_group.objects {
                    match object.object_info.unwrap() {
                        PbObjectInfo::Table(table_catalog) => {
                            self.handle_catalog_notification(resp.operation(), table_catalog);
                        }
                        _ => panic!("error type notification"),
                    };
                }
                assert!(
                    resp.version > self.version,
                    "resp version={:?}, current version={:?}",
                    resp.version,
                    self.version
                );
                self.version = resp.version;
            }
            Info::HummockVersionDeltas(hummock_version_deltas) => {
                let _ = self
                    .version_update_sender
                    .send(HummockVersionUpdate::VersionDeltas(
                        hummock_version_deltas
                            .version_deltas
                            .iter()
                            .map(HummockVersionDelta::from_rpc_protobuf)
                            .collect(),
                    ))
                    .inspect_err(|e| {
                        tracing::error!(event = ?e.0, "unable to send version delta");
                    });
            }

            Info::MetaBackupManifestId(id) => {
                self.backup_reader.try_refresh_manifest(id.id);
            }

            Info::HummockWriteLimits(write_limits) => {
                self.write_limiter
                    .update_write_limits(write_limits.write_limits);
            }

            Info::ComputeNodeTotalCpuCount(count) => {
                LicenseManager::get().update_cpu_core_count(count as _);
            }

            _ => {
                panic!("error type notification");
            }
        }
    }

    fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
        let _span: risingwave_hummock_trace::MayTraceSpan =
            TraceSpan::new_meta_message_span(resp.clone());

        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };

        self.handle_catalog_snapshot(snapshot.tables);
        self.backup_reader.try_refresh_manifest(
            snapshot
                .meta_backup_manifest_id
                .expect("should get meta backup manifest id")
                .id,
        );
        self.write_limiter.update_write_limits(
            snapshot
                .hummock_write_limits
                .expect("should get hummock_write_limits")
                .write_limits,
        );
        let _ = self
            .version_update_sender
            .send(HummockVersionUpdate::PinnedVersion(Box::new(
                HummockVersion::from_rpc_protobuf(
                    &snapshot
                        .hummock_version
                        .expect("should get hummock version"),
                ),
            )))
            .inspect_err(|e| {
                tracing::error!(event = ?e.0, "unable to send full version");
            });
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
        LicenseManager::get().update_cpu_core_count(snapshot.compute_node_total_cpu_count as _);
    }
}

impl HummockObserverNode {
    pub fn new(
        compaction_catalog_manager: CompactionCatalogManagerRef,
        backup_reader: BackupReaderRef,
        version_update_sender: UnboundedSender<HummockVersionUpdate>,
        write_limiter: WriteLimiterRef,
    ) -> Self {
        Self {
            compaction_catalog_manager,
            backup_reader,
            version_update_sender,
            version: 0,
            write_limiter,
        }
    }

    fn handle_catalog_snapshot(&mut self, tables: Vec<Table>) {
        self.compaction_catalog_manager
            .sync(tables.into_iter().map(|t| (t.id, t)).collect());
    }

    fn handle_catalog_notification(&mut self, operation: Operation, table_catalog: Table) {
        match operation {
            Operation::Add | Operation::Update => {
                self.compaction_catalog_manager
                    .update(table_catalog.id, table_catalog);
            }

            Operation::Delete => {
                self.compaction_catalog_manager.remove(table_catalog.id);
            }

            _ => panic!("receive an unsupported notify {:?}", operation),
        }
    }
}
