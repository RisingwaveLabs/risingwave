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
use risingwave_common::system_param::local_manager::LocalSystemParamsManagerRef;
use risingwave_common_service::ObserverState;
use risingwave_pb::catalog::Table;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use risingwave_storage::compaction_catalog_manager::CompactionCatalogManagerRef;

pub struct CompactorObserverNode {
    compaction_catalog_manager: CompactionCatalogManagerRef,
    system_params_manager: LocalSystemParamsManagerRef,
    version: u64,
}

impl ObserverState for CompactorObserverNode {
    fn subscribe_type() -> risingwave_pb::meta::SubscribeType {
        risingwave_pb::meta::SubscribeType::Compactor
    }

    fn handle_notification(&mut self, resp: SubscribeResponse) {
        let Some(info) = resp.info.as_ref() else {
            return;
        };

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
            Info::HummockVersionDeltas(_) => {}
            Info::SystemParams(p) => {
                self.system_params_manager.try_set_params(p);
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
        let Some(Info::Snapshot(snapshot)) = resp.info else {
            unreachable!();
        };
        self.handle_catalog_snapshot(snapshot.tables);
        let snapshot_version = snapshot.version.unwrap();
        self.version = snapshot_version.catalog_version;
        LicenseManager::get().update_cpu_core_count(snapshot.compute_node_total_cpu_count as _);
    }
}

impl CompactorObserverNode {
    pub fn new(
        compaction_catalog_manager: CompactionCatalogManagerRef,
        system_params_manager: LocalSystemParamsManagerRef,
    ) -> Self {
        Self {
            compaction_catalog_manager,
            system_params_manager,
            version: 0,
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
