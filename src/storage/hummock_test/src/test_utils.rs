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

use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common_service::ObserverManager;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::TableKey;
pub use risingwave_hummock_sdk::key::{gen_key_from_bytes, gen_key_from_str};
use risingwave_meta::controller::cluster::ClusterControllerRef;
use risingwave_meta::hummock::test_utils::{
    register_table_ids_to_compaction_group, setup_compute_env,
};
use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
use risingwave_meta::manager::MetaSrvEnv;
use risingwave_pb::catalog::{PbTable, Table};
use risingwave_rpc_client::HummockMetaClient;
use risingwave_storage::compaction_catalog_manager::{
    CompactionCatalogManager, CompactionCatalogManagerRef,
};
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::backup_reader::BackupReader;
use risingwave_storage::hummock::event_handler::HummockVersionUpdate;
use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::observer_manager::HummockObserverNode;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::write_limiter::WriteLimiter;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::mock_notification_client::get_notification_client_for_test;

pub async fn prepare_first_valid_version(
    env: MetaSrvEnv,
    hummock_manager_ref: HummockManagerRef,
    cluster_controller_ref: ClusterControllerRef,
    worker_id: i32,
) -> (
    PinnedVersion,
    UnboundedSender<HummockVersionUpdate>,
    UnboundedReceiver<HummockVersionUpdate>,
) {
    let (tx, mut rx) = unbounded_channel();
    let notification_client = get_notification_client_for_test(
        env,
        hummock_manager_ref.clone(),
        cluster_controller_ref,
        worker_id,
    )
    .await;
    let backup_manager = BackupReader::unused().await;
    let write_limiter = WriteLimiter::unused();
    let observer_manager = ObserverManager::new(
        notification_client,
        HummockObserverNode::new(
            Arc::new(CompactionCatalogManager::default()),
            backup_manager,
            tx.clone(),
            write_limiter,
        ),
    )
    .await;
    observer_manager.start().await;
    let hummock_version = match rx.recv().await {
        Some(HummockVersionUpdate::PinnedVersion(version)) => version,
        _ => unreachable!("should be full version"),
    };

    (
        PinnedVersion::new(*hummock_version, unbounded_channel().0),
        tx,
        rx,
    )
}

#[async_trait::async_trait]
pub trait TestIngestBatch: LocalStateStore {
    async fn ingest_batch(
        &mut self,
        kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize>;
}

#[async_trait::async_trait]
impl<S: LocalStateStore> TestIngestBatch for S {
    async fn ingest_batch(
        &mut self,
        kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
        write_options: WriteOptions,
    ) -> StorageResult<usize> {
        assert_eq!(self.epoch(), write_options.epoch);
        for (key, value) in kv_pairs {
            match value.user_value {
                None => self.delete(key, Bytes::new())?,
                Some(value) => self.insert(key, value, None)?,
            }
        }
        self.flush().await
    }
}

pub async fn with_hummock_storage(
    table_id: TableId,
) -> (HummockStorage, Arc<MockHummockMetaClient>) {
    let sstable_store = mock_sstable_store().await;
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
    let meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));

    let hummock_storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        meta_client.clone(),
        get_notification_client_for_test(
            env,
            hummock_manager_ref.clone(),
            cluster_ctl_ref,
            worker_id as _,
        )
        .await,
    )
    .await
    .unwrap();

    register_tables_with_id_for_test(
        hummock_storage.compaction_catalog_manager_ref(),
        &hummock_manager_ref,
        &[table_id.table_id()],
    )
    .await;

    (hummock_storage, meta_client)
}

pub fn update_filter_key_extractor_for_table_ids(
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    table_ids: &[u32],
) {
    for table_id in table_ids {
        let mock_table = PbTable {
            id: *table_id,
            read_prefix_len_hint: 0,
            maybe_vnode_count: Some(VirtualNode::COUNT_FOR_TEST as u32),
            ..Default::default()
        };
        compaction_catalog_manager_ref.update(*table_id, mock_table);
    }
}

pub async fn register_tables_with_id_for_test(
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    hummock_manager_ref: &HummockManagerRef,
    table_ids: &[u32],
) {
    update_filter_key_extractor_for_table_ids(compaction_catalog_manager_ref, table_ids);
    register_table_ids_to_compaction_group(
        hummock_manager_ref,
        table_ids,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
}

pub fn update_filter_key_extractor_for_tables(
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    tables: &[PbTable],
) {
    for table in tables {
        compaction_catalog_manager_ref.update(table.id, table.clone())
    }
}
pub async fn register_tables_with_catalog_for_test(
    compaction_catalog_manager_ref: CompactionCatalogManagerRef,
    hummock_manager_ref: &HummockManagerRef,
    tables: &[Table],
) {
    update_filter_key_extractor_for_tables(compaction_catalog_manager_ref, tables);
    let table_ids = tables.iter().map(|t| t.id).collect_vec();
    register_table_ids_to_compaction_group(
        hummock_manager_ref,
        &table_ids,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
}

pub struct HummockTestEnv {
    pub storage: HummockStorage,
    pub manager: HummockManagerRef,
    pub meta_client: Arc<MockHummockMetaClient>,
}

impl HummockTestEnv {
    async fn wait_version_sync(&self) {
        self.storage
            .wait_version(self.manager.get_current_version().await)
            .await
    }

    pub async fn register_table_id(&self, table_id: TableId) {
        register_tables_with_id_for_test(
            self.storage.compaction_catalog_manager_ref(),
            &self.manager,
            &[table_id.table_id()],
        )
        .await;
        self.wait_version_sync().await;
    }

    pub async fn register_table(&self, table: PbTable) {
        register_tables_with_catalog_for_test(
            self.storage.compaction_catalog_manager_ref(),
            &self.manager,
            &[table],
        )
        .await;
        self.wait_version_sync().await;
    }

    // Seal, sync and commit a epoch.
    // On completion of this function call, the provided epoch should be committed and visible.
    pub async fn commit_epoch(&self, epoch: u64) {
        let table_ids = self
            .manager
            .get_current_version()
            .await
            .state_table_info
            .info()
            .keys()
            .cloned()
            .collect();
        let res = self
            .storage
            .seal_and_sync_epoch(epoch, table_ids)
            .await
            .unwrap();
        self.meta_client.commit_epoch(epoch, res).await.unwrap();

        self.wait_sync_committed_version().await;
    }

    pub async fn wait_sync_committed_version(&self) {
        let version = self.manager.get_current_version().await;
        self.storage.wait_version(version).await;
    }
}

pub async fn prepare_hummock_test_env() -> HummockTestEnv {
    let sstable_store = mock_sstable_store().await;
    let hummock_options = Arc::new(default_opts_for_test());
    let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;

    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager_ref.clone(),
        worker_id as _,
    ));

    let notification_client = get_notification_client_for_test(
        env,
        hummock_manager_ref.clone(),
        cluster_ctl_ref,
        worker_id as _,
    )
    .await;

    let storage = HummockStorage::for_test(
        hummock_options,
        sstable_store,
        hummock_meta_client.clone(),
        notification_client,
    )
    .await
    .unwrap();

    HummockTestEnv {
        storage,
        manager: hummock_manager_ref,
        meta_client: hummock_meta_client,
    }
}
