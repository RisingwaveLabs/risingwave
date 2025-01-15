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

#![cfg(test)]

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use prometheus::Registry;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::epoch::{test_epoch, EpochExt};
use risingwave_hummock_sdk::compact::compact_task_to_string;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::get_compaction_group_ssts;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::{gen_key_from_str, FullKey};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
use risingwave_hummock_sdk::table_stats::{to_prost_table_stats_map, TableStats, TableStatsMap};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockEpoch, HummockSstableObjectId, HummockVersionId,
    LocalSstableInfo, SyncResult, FIRST_VERSION_ID,
};
use risingwave_pb::common::worker_node::Property;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::hummock::compact_task::TaskStatus;
use risingwave_pb::hummock::HummockPinnedVersion;
use risingwave_rpc_client::HummockMetaClient;
use thiserror_ext::AsReport;

use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction::selector::{default_compaction_selector, ManualCompactionOption};
use crate::hummock::error::Error;
use crate::hummock::test_utils::*;
use crate::hummock::{HummockManagerRef, MockHummockMetaClient};
use crate::manager::MetaSrvEnv;
use crate::rpc::metrics::MetaMetrics;

pub fn version_max_committed_epoch(version: &HummockVersion) -> u64 {
    let committed_epoch = version
        .state_table_info
        .info()
        .values()
        .next()
        .unwrap()
        .committed_epoch;
    assert!(
        version
            .state_table_info
            .info()
            .values()
            .all(|info| info.committed_epoch == committed_epoch),
        "info: {:?}",
        version.state_table_info.info()
    );
    committed_epoch
}

fn pin_versions_sum(pin_versions: &[HummockPinnedVersion]) -> usize {
    pin_versions.iter().len()
}

fn gen_sstable_info(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> SstableInfo {
    gen_sstable_info_impl(sst_id, table_ids, epoch).into()
}

fn gen_sstable_info_impl(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> SstableInfoInner {
    let table_key_l = gen_key_from_str(VirtualNode::ZERO, "1");
    let table_key_r = gen_key_from_str(VirtualNode::MAX_FOR_TEST, "1");
    let full_key_l = FullKey::for_test(
        TableId::new(*table_ids.first().unwrap()),
        table_key_l,
        epoch,
    )
    .encode();
    let full_key_r =
        FullKey::for_test(TableId::new(*table_ids.last().unwrap()), table_key_r, epoch).encode();

    SstableInfoInner {
        sst_id,
        key_range: KeyRange {
            left: full_key_l.into(),
            right: full_key_r.into(),
            right_exclusive: false,
        },
        table_ids,
        object_id: sst_id,
        min_epoch: 20,
        max_epoch: 20,
        file_size: 100,
        sst_size: 100,
        ..Default::default()
    }
}

fn gen_local_sstable_info(sst_id: u64, table_ids: Vec<u32>, epoch: u64) -> LocalSstableInfo {
    LocalSstableInfo {
        sst_info: gen_sstable_info(sst_id, table_ids, epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    }
}
fn get_compaction_group_object_ids(
    version: &HummockVersion,
    group_id: CompactionGroupId,
) -> Vec<HummockSstableObjectId> {
    get_compaction_group_ssts(version, group_id)
        .map(|(object_id, _)| object_id)
        .collect_vec()
}

async fn list_pinned_version_from_meta_store(env: &MetaSrvEnv) -> Vec<HummockPinnedVersion> {
    use risingwave_meta_model::hummock_pinned_version;
    use sea_orm::EntityTrait;
    hummock_pinned_version::Entity::find()
        .all(&env.meta_store_ref().conn)
        .await
        .unwrap()
        .into_iter()
        .map(Into::into)
        .collect()
}

#[tokio::test]
async fn test_hummock_compaction_task() {
    let (_, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let sst_num = 2;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let table_id = 1;
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        table_id,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), table_id).await;
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // Cancel the task and succeed.
    assert!(hummock_manager
        .cancel_compact_task(compact_task.task_id, TaskStatus::ManualCanceled)
        .await
        .unwrap());

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.task_id, 3);
    // Finish the task and succeed.

    assert!(hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default()
        )
        .await
        .unwrap());
}

#[tokio::test]
async fn test_hummock_table() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let epoch = test_epoch(1);
    let original_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let pinned_version = hummock_manager.get_current_version().await;
    let levels = pinned_version.get_compaction_group_levels(compaction_group_id);
    assert_eq!(
        Ordering::Equal,
        levels
            .l0
            .sub_levels
            .iter()
            .chain(levels.levels.iter())
            .flat_map(|level| level.table_infos.iter())
            .map(|info| info.object_id)
            .sorted()
            .cmp(original_tables.iter().map(|ot| ot.object_id).sorted())
    );

    // Confirm tables got are equal to original tables
    assert_eq!(
        get_sorted_object_ids(&original_tables),
        get_sorted_committed_object_ids(&pinned_version, compaction_group_id)
    );
}

#[tokio::test]
async fn test_hummock_transaction() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let mut committed_tables = vec![];
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    // Add and commit tables in epoch1.
    // BEFORE:  committed_epochs = []
    // AFTER:   committed_epochs = [epoch1]
    let epoch1 = test_epoch(1);
    {
        // Add tables in epoch1
        let tables_in_epoch1 = generate_test_tables(epoch1, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch1,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch1. No tables should be returned.
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        assert!(get_sorted_committed_object_ids(&current_version, compaction_group_id).is_empty());

        // Commit epoch1
        hummock_meta_client
            .commit_epoch(
                epoch1,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&tables_in_epoch1),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch1.clone());

        // Get tables after committing epoch1. All tables committed in epoch1 should be returned
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        assert_eq!(version_max_committed_epoch(&current_version), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );
    }

    // Add and commit tables in epoch2.
    // BEFORE:  committed_epochs = [epoch1]
    // AFTER:   committed_epochs = [epoch1, epoch2]
    let epoch2 = epoch1.next_epoch();
    {
        // Add tables in epoch2
        let tables_in_epoch2 = generate_test_tables(epoch2, get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &tables_in_epoch2,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        // Get tables before committing epoch2. tables_in_epoch1 should be returned and
        // tables_in_epoch2 should be invisible.
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        assert_eq!(version_max_committed_epoch(&current_version), epoch1);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );

        // Commit epoch2
        hummock_meta_client
            .commit_epoch(
                epoch2,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&tables_in_epoch2),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        committed_tables.extend(tables_in_epoch2);

        // Get tables after committing epoch2. tables_in_epoch1 and tables_in_epoch2 should be
        // returned
        let current_version = hummock_manager.get_current_version().await;
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        assert_eq!(version_max_committed_epoch(&current_version), epoch2);
        assert_eq!(
            get_sorted_object_ids(&committed_tables),
            get_sorted_committed_object_ids(&current_version, compaction_group_id)
        );
    }
}

#[tokio::test]
async fn test_release_context_resource() {
    let (env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(1).await;
    let context_id_1 = worker_id as _;

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_owned(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                is_unschedulable: false,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2 as _;

    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
    hummock_manager.pin_version(context_id_1).await.unwrap();
    hummock_manager.pin_version(context_id_2).await.unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        2
    );
    assert_eq!(list_pinned_version_from_meta_store(&env).await.len(), 2);
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    let pinned_versions = list_pinned_version_from_meta_store(&env).await;
    assert_eq!(pin_versions_sum(&pinned_versions), 1);
    assert_eq!(pinned_versions[0].context_id, context_id_2);
    // it's OK to call again
    hummock_manager
        .release_contexts(&vec![context_id_1])
        .await
        .unwrap();
    hummock_manager
        .release_contexts(&vec![context_id_2])
        .await
        .unwrap();
    assert_eq!(
        pin_versions_sum(&list_pinned_version_from_meta_store(&env).await),
        0
    );
}

#[tokio::test]
async fn test_context_id_validation() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let invalid_context_id = HummockContextId::MAX;
    let context_id = worker_id as _;

    // Invalid context id is rejected.
    let error = hummock_manager
        .pin_version(invalid_context_id)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidContext(_)));

    // Valid context id is accepted.
    hummock_manager.pin_version(context_id).await.unwrap();
    // Pin multiple times is OK.
    hummock_manager.pin_version(context_id).await.unwrap();
}

#[tokio::test]
async fn test_hummock_manager_basic() {
    let (_env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(1).await;
    let context_id_1 = worker_id as _;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id_1,
    ));

    let fake_host_address_2 = HostAddress {
        host: "127.0.0.1".to_owned(),
        port: 2,
    };
    let fake_parallelism = 4;
    let worker_node_2 = cluster_ctl
        .add_worker(
            WorkerType::ComputeNode,
            fake_host_address_2,
            Property {
                parallelism: fake_parallelism,
                is_streaming: true,
                is_serving: true,
                is_unschedulable: false,
                ..Default::default()
            },
            Default::default(),
        )
        .await
        .unwrap();
    let context_id_2 = worker_node_2 as _;

    // initial version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        FIRST_VERSION_ID
    );

    let mut epoch = test_epoch(1);
    let mut register_log_count = 0;
    let mut commit_log_count = 0;
    let commit_one = |epoch: HummockEpoch,
                      hummock_manager: HummockManagerRef,
                      hummock_meta_client: Arc<dyn HummockMetaClient>| async move {
        let original_tables =
            generate_test_tables(test_epoch(epoch), get_sst_ids(&hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            &hummock_manager,
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;

        hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: to_local_sstable_info(&original_tables),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    };

    commit_one(epoch, hummock_manager.clone(), hummock_meta_client.clone()).await;
    register_log_count += 1;
    commit_log_count += 1;
    epoch.inc_epoch();

    let init_version_id = FIRST_VERSION_ID;

    // increased version id
    assert_eq!(
        hummock_manager.get_current_version().await.id,
        init_version_id + commit_log_count + register_log_count
    );

    // min pinned version id if no clients
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
    for _ in 0..2 {
        hummock_manager
            .unpin_version_before(context_id_1, HummockVersionId::MAX)
            .await
            .unwrap();
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            HummockVersionId::MAX
        );

        // should pin latest because u64::MAX
        let version = hummock_manager.pin_version(context_id_1).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await,
            init_version_id + commit_log_count + register_log_count
        );
    }

    commit_one(epoch, hummock_manager.clone(), hummock_meta_client.clone()).await;
    commit_log_count += 1;
    register_log_count += 1;

    for _ in 0..2 {
        // should pin latest because deltas cannot contain INVALID_EPOCH
        let version = hummock_manager.pin_version(context_id_2).await.unwrap();
        assert_eq!(
            version.id,
            init_version_id + commit_log_count + register_log_count
        );
        // pinned by context_id_1
        assert_eq!(
            hummock_manager.get_min_pinned_version_id().await + 2,
            init_version_id + commit_log_count + register_log_count,
        );
    }
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        commit_log_count + register_log_count
    );
    assert_eq!(
        hummock_manager.delete_version_deltas().await.unwrap(),
        (commit_log_count + register_log_count) as usize
    );
    hummock_manager
        .unpin_version_before(context_id_1, HummockVersionId::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        init_version_id + commit_log_count + register_log_count
    );
    hummock_manager
        .unpin_version_before(context_id_2, HummockVersionId::MAX)
        .await
        .unwrap();
    assert_eq!(
        hummock_manager.get_min_pinned_version_id().await,
        HummockVersionId::MAX
    );
}

#[tokio::test]
async fn test_pin_snapshot_response_lost() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let mut epoch = test_epoch(1);
    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ ] -> [ e0 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Pin a snapshot with smallest last_pin
    // [ e0 ] -> [ e0:pinned ]
    let mut epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend, prev_epoch);

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0:pinned ] -> [ e0:pinned, e1 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Assume the response of the previous rpc is lost.
    // [ e0:pinned, e1 ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    let prev_epoch = epoch.prev_epoch();
    assert_eq!(epoch_recorded_in_frontend, prev_epoch);

    // Assume the response of the previous rpc is lost.
    // [ e0, e1:pinned ] -> [ e0, e1:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned ] -> [ e0, e1:pinned, e2 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Use correct snapshot id.
    // [ e0, e1:pinned, e2 ] -> [ e0, e1:pinned, e2:pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());

    let test_tables = generate_test_tables(epoch, get_sst_ids(&hummock_manager, 2).await);
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &test_tables,
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    // [ e0, e1:pinned, e2:pinned ] -> [ e0, e1:pinned, e2:pinned, e3 ]
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&test_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    epoch.inc_epoch();

    // Use u64::MAX as epoch to pin greatest snapshot
    // [ e0, e1:pinned, e2:pinned, e3 ] -> [ e0, e1:pinned, e2:pinned, e3::pinned ]
    epoch_recorded_in_frontend = hummock_manager
        .on_current_version(version_max_committed_epoch)
        .await;
    assert_eq!(epoch_recorded_in_frontend, epoch.prev_epoch());
}

#[tokio::test]
async fn test_print_compact_task() {
    let (_, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables =
        generate_test_sstables_with_table_id(epoch, 1, get_sst_ids(&hummock_manager, 2).await);
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    register_sstable_infos_to_compaction_group(
        &hummock_manager,
        &original_tables,
        compaction_group_id,
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);

    let s = compact_task_to_string(&compact_task);
    assert!(s.contains("Compaction task id: 1, group-id: 2, type: Dynamic, target level: 0"));
}

#[tokio::test]
async fn test_invalid_sst_id() {
    let (_, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let epoch = test_epoch(1);
    let ssts = generate_test_tables(epoch, vec![1]);
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    register_sstable_infos_to_compaction_group(&hummock_manager, &ssts, compaction_group_id).await;
    let ssts = to_local_sstable_info(&ssts);
    // reject due to invalid context id
    {
        let hummock_meta_client: Arc<dyn HummockMetaClient> =
            Arc::new(MockHummockMetaClient::new(hummock_manager.clone(), 23333));
        let error = hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: ssts.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.as_report().to_string(),
            "mock error: SST 1 is invalid"
        );
    }

    // reject due to SST's timestamp is below watermark
    let ssts_below_watermerk = ssts
        .iter()
        .map(|s| LocalSstableInfo {
            sst_info: s.sst_info.clone(),
            table_stats: s.table_stats.clone(),
            created_at: 0,
        })
        .collect();
    let error = hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts_below_watermerk,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert!(error
        .as_report()
        .to_string()
        .contains("is rejected from being committed since it's below watermark"));

    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts.clone(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn test_trigger_manual_compaction() {
    let (_, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;

    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compactor is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // No compaction task available.
    let compactor_manager_ref = hummock_manager.compactor_manager_ref_for_test();
    let receiver = compactor_manager_ref.add_compactor(context_id);
    {
        let option = ManualCompactionOption::default();
        // to check no compaction task
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;

        assert_eq!(
            "trigger_manual_compaction No compaction_task is available. compaction_group 2",
            result.err().unwrap().to_string()
        );
    }

    // Generate data for compaction task
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let _ = add_test_tables(
        &hummock_manager,
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    {
        // to check compactor send task fail
        drop(receiver);
        {
            let option = ManualCompactionOption::default();
            let result = hummock_manager
                .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
                .await;
            assert!(result.is_err());
        }
    }

    compactor_manager_ref.remove_compactor(context_id);
    let _receiver = compactor_manager_ref.add_compactor(context_id);

    {
        let option = ManualCompactionOption {
            level: 6,
            key_range: KeyRange::default(),
            ..Default::default()
        };

        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_ok());
    }

    {
        let option = ManualCompactionOption::default();
        // all sst pending , test no compaction avail
        let result = hummock_manager
            .trigger_manual_compaction(StaticCompactionGroupId::StateDefault.into(), option)
            .await;
        assert!(result.is_err());
    }
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;

    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let sst_num = 2;

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) =
        HummockManager::hummock_timer_task(hummock_manager.clone(), None);

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    for i in 0..10 {
        // send heartbeats to the task over 2.5 seconds
        let req = CompactTaskProgress {
            task_id: compact_task.task_id,
            num_ssts_sealed: i + 1,
            ..Default::default()
        };
        compactor_manager.update_task_heartbeats(&vec![req]);
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    // Cancel the task immediately and succeed.
    assert!(hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::ExecuteFailed,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap());

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.task_id, 3);

    // Cancel the task after heartbeat has triggered and fail.

    // do not send heartbeats to the task for 30s seconds (ttl = 1s, heartbeat check freq. = 1s)
    // default_interval = 30s
    tokio::time::sleep(std::time::Duration::from_secs(32)).await;

    assert!(!hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::ExecuteFailed,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap());
    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

// This is a non-deterministic test
#[cfg(madsim)]
#[tokio::test]
async fn test_hummock_compaction_task_heartbeat_removal_on_node_removal() {
    use risingwave_pb::hummock::CompactTaskProgress;

    use crate::hummock::HummockManager;
    let (_env, hummock_manager, cluster_ctl, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let sst_num = 2;

    let worker_node = cluster_ctl
        .get_worker_by_id(worker_id)
        .await
        .unwrap()
        .unwrap();

    let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
    let _tx = compactor_manager.add_compactor(context_id);

    let (join_handle, shutdown_tx) =
        HummockManager::hummock_timer_task(hummock_manager.clone(), None);

    // No compaction task available.
    assert!(hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .is_none());

    // Add some sstables and commit.
    let epoch = test_epoch(1);
    let original_tables = generate_test_sstables_with_table_id(
        epoch,
        1,
        get_sst_ids(&hummock_manager, sst_num).await,
    );
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1],
        StaticCompactionGroupId::StateDefault.into(),
    )
    .await;
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: to_local_sstable_info(&original_tables),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Get a compaction task.
    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(compact_task.input_ssts.first().unwrap().level_idx, 0);
    assert_eq!(compact_task.task_id, 2);

    // send heartbeats to the task immediately
    let req = CompactTaskProgress {
        task_id: compact_task.task_id,
        num_ssts_sealed: 1,
        num_ssts_uploaded: 1,
        ..Default::default()
    };
    compactor_manager.update_task_heartbeats(&vec![req.clone()]);

    // Removing the node from cluster will invalidate context id.
    cluster_ctl
        .delete_worker(worker_node.host.unwrap())
        .await
        .unwrap();
    hummock_manager
        .release_contexts([context_id])
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    join_handle.await.unwrap();
}

#[tokio::test]
async fn test_extend_objects_to_delete() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let context_id = worker_id as _;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id,
    ));
    let _pinned_version1 = hummock_manager.pin_version(context_id).await.unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let sst_infos = add_test_tables(
        hummock_manager.as_ref(),
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    let max_committed_object_id = sst_infos
        .iter()
        .map(|ssts| {
            ssts.iter()
                .max_by_key(|s| s.object_id)
                .map(|s| s.object_id)
                .unwrap()
        })
        .max()
        .unwrap();
    let orphan_sst_num: usize = 10;
    let all_object_ids = sst_infos
        .iter()
        .flatten()
        .map(|s| s.object_id)
        .chain(max_committed_object_id + 1..=max_committed_object_id + orphan_sst_num as u64)
        .collect_vec();
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );

    // Checkpoint
    assert_eq!(
        hummock_manager.create_version_checkpoint(1).await.unwrap(),
        6
    );
    // since version1 is still pinned, the sst removed in compaction can not be reclaimed.
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );
    let pinned_version2: HummockVersion = hummock_manager.pin_version(context_id).await.unwrap();
    hummock_manager
        .unpin_version_before(context_id, pinned_version2.id)
        .await
        .unwrap();
    // version1 is unpin, but version2 is pinned, and version2 is the checkpoint version.
    // stale objects are combined in the checkpoint of version2, so no sst to reclaim
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num
    );
    let new_epoch = version_max_committed_epoch(&pinned_version2).next_epoch();
    hummock_meta_client
        .commit_epoch(
            new_epoch,
            SyncResult {
                uncommitted_ssts: vec![],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let pinned_version3: HummockVersion = hummock_manager.pin_version(context_id).await.unwrap();
    assert_eq!(new_epoch, version_max_committed_epoch(&pinned_version3));
    hummock_manager
        .unpin_version_before(context_id, pinned_version3.id)
        .await
        .unwrap();
    // version3 is the min pinned, and sst removed in compaction can be reclaimed, because they were tracked
    // in the stale objects of version2 checkpoint
    assert_eq!(
        hummock_manager
            .finalize_objects_to_delete(all_object_ids.clone().into_iter())
            .await
            .unwrap()
            .len(),
        orphan_sst_num + 3
    );
}

#[tokio::test]
async fn test_version_stats() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let init_stats = hummock_manager.get_version_stats().await;
    assert!(init_stats.table_stats.is_empty());

    // Commit epoch
    let epoch = test_epoch(1);
    register_table_ids_to_compaction_group(
        &hummock_manager,
        &[1, 2, 3],
        StaticCompactionGroupId::StateDefault as _,
    )
    .await;
    let table_stats_change = TableStats {
        total_key_size: 1000,
        total_value_size: 100,
        total_key_count: 10,

        total_compressed_size: 1024 * 1024,
    };
    let ssts_with_table_ids = vec![vec![1, 2], vec![2, 3]];
    let sst_ids = get_sst_ids(&hummock_manager, ssts_with_table_ids.len() as _).await;
    let ssts = ssts_with_table_ids
        .into_iter()
        .enumerate()
        .map(|(idx, table_ids)| LocalSstableInfo {
            sst_info: SstableInfoInner {
                object_id: sst_ids[idx],
                sst_id: sst_ids[idx],
                key_range: KeyRange {
                    left: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right: iterator_test_key_of_epoch(1, 1, 1).into(),
                    right_exclusive: false,
                },
                file_size: 1024 * 1024 * 1024,
                table_ids: table_ids.clone(),
                sst_size: 1024 * 1024 * 1024,
                ..Default::default()
            }
            .into(),
            table_stats: table_ids
                .iter()
                .map(|table_id| (*table_id, table_stats_change.clone()))
                .collect(),
            created_at: u64::MAX,
        })
        .collect_vec();
    hummock_meta_client
        .commit_epoch(
            epoch,
            SyncResult {
                uncommitted_ssts: ssts,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let stats_after_commit = hummock_manager.get_version_stats().await;
    assert_eq!(stats_after_commit.table_stats.len(), 3);
    let table1_stats = stats_after_commit.table_stats.get(&1).unwrap();
    let table2_stats = stats_after_commit.table_stats.get(&2).unwrap();
    let table3_stats = stats_after_commit.table_stats.get(&3).unwrap();
    assert_eq!(table1_stats.total_key_count, 10);
    assert_eq!(table1_stats.total_value_size, 100);
    assert_eq!(table1_stats.total_key_size, 1000);
    assert_eq!(table2_stats.total_key_count, 20);
    assert_eq!(table2_stats.total_value_size, 200);
    assert_eq!(table2_stats.total_key_size, 2000);
    assert_eq!(table3_stats.total_key_count, 10);
    assert_eq!(table3_stats.total_value_size, 100);
    assert_eq!(table3_stats.total_key_size, 1000);

    let compact_task = hummock_manager
        .get_compact_task(
            StaticCompactionGroupId::StateDefault.into(),
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();
    // compact_task.task_status = TaskStatus::Success as _;
    let compact_table_stats_change = TableStatsMap::from([
        (
            2,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
        (
            3,
            TableStats {
                total_key_size: -1000,
                total_value_size: -100,
                total_key_count: -10,
                total_compressed_size: 0, // unused
            },
        ),
    ]);
    hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            vec![],
            Some(to_prost_table_stats_map(compact_table_stats_change)),
            HashMap::default(),
        )
        .await
        .unwrap();
    let stats_after_compact = hummock_manager.get_version_stats().await;
    let compact_table1_stats = stats_after_compact.table_stats.get(&1).unwrap();
    let compact_table2_stats = stats_after_compact.table_stats.get(&2).unwrap();
    let compact_table3_stats = stats_after_compact.table_stats.get(&3).unwrap();
    assert_eq!(compact_table1_stats, table1_stats);
    assert_eq!(compact_table2_stats.total_key_count, 10);
    assert_eq!(compact_table2_stats.total_value_size, 100);
    assert_eq!(compact_table2_stats.total_key_size, 1000);
    assert_eq!(compact_table3_stats.total_key_count, 0);
    assert_eq!(compact_table3_stats.total_value_size, 0);
    assert_eq!(compact_table3_stats.total_key_size, 0);
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_commit() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 3)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: HashMap::from([
            (
                100,
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
            (
                101,
                TableStats {
                    total_compressed_size: 50,
                    ..Default::default()
                },
            ),
        ]),
        created_at: u64::MAX,
    };
    hummock_meta_client
        .commit_epoch(
            test_epoch(30),
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 2),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, 3),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(2)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(3)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_basic() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(100, &[0], None)
        .await
        .unwrap_err();
    assert_eq!("compaction group error: invalid group 100", err.to_string());

    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2, &[100], None)
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: table 100 doesn't in group 2",
        err.to_string()
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    hummock_meta_client
        .commit_epoch(
            test_epoch(30),
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let err = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100, 101], None)
        .await
        .unwrap_err();
    assert_eq!(
        "compaction group error: invalid split attempt for group 2: all member tables are moved",
        err.to_string()
    );

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100, 101], None)
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;

    let old_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 102).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, old_compaction_group_id),
        Vec::<u64>::new()
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(old_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .sorted()
            .collect_vec(),
        vec![100, 101]
    );
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_non_trivial() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 3);
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let old_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, old_compaction_group_id),
        vec![10]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, new_compaction_group_id),
        vec![10]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(old_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(new_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_trivial_expired() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_3 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 8,
            object_id: 8,
            ..sst_2.sst_info.get_inner()
        }
        .into(),
        ..sst_2.clone()
    };
    let sst_4 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 9,
            object_id: 9,
            ..sst_1.sst_info.get_inner()
        }
        .into(),
        ..sst_1.clone()
    };
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3, sst_4],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(current_version.levels.len(), 3);
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .sorted()
            .collect_vec(),
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );

    let task2 = hummock_manager
        .get_compact_task(left_compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    let ret = hummock_manager
        .report_compact_task(
            task2.task_id,
            TaskStatus::Success,
            vec![SstableInfoInner {
                object_id: 12,
                sst_id: 12,
                key_range: KeyRange::default(),
                table_ids: vec![100],
                min_epoch: 20,
                max_epoch: 20,
                file_size: 100,
                sst_size: 100,
                ..Default::default()
            }
            .into()],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    assert!(ret);
    let ret = hummock_manager
        .report_compact_task(
            task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    // the task has been canceled
    assert!(!ret);
}

async fn get_manual_compact_task(
    hummock_manager_ref: HummockManagerRef,
    compaction_group_id: u64,
    level: usize,
) -> CompactTask {
    let manual_compcation_option = ManualCompactionOption {
        level,
        ..Default::default()
    };

    hummock_manager_ref
        .manual_get_compact_task(compaction_group_id, manual_compcation_option)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn test_move_state_tables_to_dedicated_compaction_group_on_demand_bottom_levels() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2), (102, 2), (103, 2)])
        .await
        .unwrap();

    let epoch = test_epoch(1);
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![101, 102], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_3 = LocalSstableInfo {
        sst_info: gen_sstable_info(12, vec![103], epoch),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let manual_compcation_option = ManualCompactionOption {
        level: 0,
        ..Default::default()
    };
    // 2. get compact task
    let compact_task = hummock_manager
        .manual_get_compact_task(compaction_group_id, manual_compcation_option)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        compact_task
            .input_ssts
            .iter()
            .map(|level| level.table_infos.len())
            .sum::<usize>(),
        3,
    );

    hummock_manager
        .report_compact_task(
            compact_task.task_id,
            TaskStatus::Success,
            compact_task
                .input_ssts
                .iter()
                .flat_map(|level| level.table_infos.clone())
                .collect(),
            None,
            HashMap::default(),
        )
        .await
        .unwrap();

    let base_level: usize = 6;

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2, &[100], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        1
    );

    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[0]
            .table_ids,
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        1
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos
            .len(),
        2
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(left_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[0]
            .table_ids,
        vec![100]
    );
    assert_eq!(
        current_version
            .get_compaction_group_levels(right_compaction_group_id)
            .levels[base_level - 1]
            .table_infos[1]
            .table_ids,
        vec![103]
    );
}

#[tokio::test]
async fn test_compaction_task_expiration_due_to_split_group() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    hummock_manager
        .register_table_ids_for_test(&[(100, compaction_group_id), (101, compaction_group_id)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let compaction_task =
        get_manual_compact_task(hummock_manager.clone(), compaction_group_id, 0).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100], None)
        .await
        .unwrap();

    let version_1 = hummock_manager.get_current_version().await;
    assert!(!hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default()
        )
        .await
        .unwrap());
    let version_2 = hummock_manager.get_current_version().await;
    assert_eq!(
        version_1, version_2,
        "version should not change because compaction task has been cancelled"
    );

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    let compaction_task =
        get_manual_compact_task(hummock_manager.clone(), compaction_group_id, 0).await;
    assert_eq!(compaction_task.input_ssts[0].table_infos.len(), 2);
    hummock_manager
        .report_compact_task(
            compaction_task.task_id,
            TaskStatus::Success,
            vec![],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();

    let version_3 = hummock_manager.get_current_version().await;
    assert_ne!(
        version_2, version_3,
        "version should change because compaction task has succeeded"
    );
}

#[tokio::test]
async fn test_move_tables_between_compaction_group() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2), (102, 2)])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, vec![100, 101, 102], test_epoch(1));

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let sst_2 = gen_local_sstable_info(14, vec![101, 102], test_epoch(2));

    hummock_meta_client
        .commit_epoch(
            31,
            SyncResult {
                uncommitted_ssts: vec![sst_2.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(2, sst_ids.len());
    assert!(sst_ids.contains(&10));
    assert!(sst_ids.contains(&14));

    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(2, &[100], None)
        .await
        .unwrap();
    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    let current_version = hummock_manager.get_current_version().await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(2, sst_ids.len());
    assert!(!sst_ids.contains(&10));

    let compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let sst_ids = current_version
        .get_sst_ids_by_group_id(compaction_group_id)
        .collect_vec();
    assert_eq!(1, sst_ids.len());
    assert!(!sst_ids.contains(&10));
}

#[tokio::test]
async fn test_gc_stats() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(1)
        .level0_max_compact_file_number(130)
        .level0_sub_level_compact_level_count(1)
        .level0_overlapping_sub_level_compact_level_count(1)
        .build();
    let registry = Registry::new();
    let (_env, hummock_manager, _, worker_id) =
        setup_compute_env_with_metric(80, config, Some(MetaMetrics::for_test(&registry))).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let context_id = worker_id as _;
    let assert_eq_gc_stats = |stale_object_size,
                              stale_object_count,
                              old_version_object_size,
                              old_version_object_count,
                              current_version_object_count,
                              current_version_object_size| {
        assert_eq!(
            hummock_manager.metrics.stale_object_size.get(),
            stale_object_size
        );
        assert_eq!(
            hummock_manager.metrics.stale_object_count.get(),
            stale_object_count
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_size.get(),
            old_version_object_size
        );
        assert_eq!(
            hummock_manager.metrics.old_version_object_count.get(),
            old_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_count.get(),
            current_version_object_count
        );
        assert_eq!(
            hummock_manager.metrics.current_version_object_size.get(),
            current_version_object_size
        );
    };
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    hummock_manager.pin_version(context_id).await.unwrap();
    let _ = add_test_tables(
        &hummock_manager,
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    assert_eq_gc_stats(0, 0, 0, 0, 0, 0);
    assert_ne!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    hummock_manager
        .unpin_version_before(context_id, HummockVersionId::MAX)
        .await
        .unwrap();

    assert_eq_gc_stats(0, 0, 6, 3, 2, 4);
    assert_eq!(
        hummock_manager.create_version_checkpoint(0).await.unwrap(),
        0
    );
}

#[tokio::test]
async fn test_partition_level() {
    let config = CompactionConfigBuilder::new()
        .level0_tier_compact_file_number(3)
        .level0_sub_level_compact_level_count(3)
        .level0_overlapping_sub_level_compact_level_count(3)
        .build();
    let registry = Registry::new();
    let (env, hummock_manager, _, worker_id) =
        setup_compute_env_with_metric(80, config.clone(), Some(MetaMetrics::for_test(&registry)))
            .await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    let sst_1 = gen_local_sstable_info(10, vec![100, 101], test_epoch(1));

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(
            compaction_group_id,
            &[100],
            Some(env.opts.partition_vnode_count),
        )
        .await
        .unwrap();
    let new_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let mut global_sst_id = 13;
    const MB: u64 = 1024 * 1024;
    let mut selector = default_compaction_selector();
    for epoch in 31..100 {
        let mut sst = gen_sstable_info_impl(global_sst_id, vec![100], test_epoch(epoch));
        sst.file_size = 10 * MB;
        sst.sst_size = 10 * MB;
        sst.uncompressed_file_size = 10 * MB;
        let sst = LocalSstableInfo {
            sst_info: sst.into(),
            table_stats: Default::default(),
            created_at: u64::MAX,
        };

        hummock_meta_client
            .commit_epoch(
                epoch,
                SyncResult {
                    uncommitted_ssts: vec![sst],
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        global_sst_id += 1;
        if let Some(task) = hummock_manager
            .get_compact_task(new_compaction_group_id, &mut selector)
            .await
            .unwrap()
        {
            let mut sst = gen_sstable_info_impl(global_sst_id, vec![100], test_epoch(epoch));
            sst.file_size = task
                .input_ssts
                .iter()
                .map(|level| {
                    level
                        .table_infos
                        .iter()
                        .map(|sst| sst.sst_size)
                        .sum::<u64>()
                })
                .sum::<u64>();
            sst.sst_size = sst.file_size;
            global_sst_id += 1;
            let ret = hummock_manager
                .report_compact_task(
                    task.task_id,
                    TaskStatus::Success,
                    vec![sst.into()],
                    None,
                    HashMap::default(),
                )
                .await
                .unwrap();
            assert!(ret);
        }
    }
    let current_version = hummock_manager.get_current_version().await;
    let group = current_version.get_compaction_group_levels(new_compaction_group_id);
    for sub_level in &group.l0.sub_levels {
        if sub_level.total_file_size > config.sub_level_max_compaction_bytes {
            assert!(sub_level.vnode_partition_count > 0);
        }
    }
}

#[tokio::test]
async fn test_unregister_moved_table() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(
        original_groups,
        vec![
            StaticCompactionGroupId::StateDefault as u64,
            StaticCompactionGroupId::MaterializedView as u64
        ]
    );

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(10, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(11, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let new_compaction_group_id = hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100], None)
        .await
        .unwrap()
        .0;

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    assert_eq!(new_compaction_group_id, left_compaction_group_id);
    assert_eq!(current_version.levels.len(), 3);

    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(
        get_compaction_group_object_ids(&current_version, right_compaction_group_id),
        vec![11]
    );
    assert_eq!(
        get_compaction_group_object_ids(&current_version, left_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![101]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );

    hummock_manager
        .unregister_table_ids([TableId::new(101)])
        .await
        .unwrap();
    let current_version = hummock_manager.get_current_version().await;
    assert_eq!(current_version.levels.len(), 2);
    assert_eq!(
        get_compaction_group_object_ids(&current_version, left_compaction_group_id),
        vec![10, 11]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );
}

#[tokio::test]
async fn test_merge_compaction_group_task_expired() {
    let (_env, hummock_manager, _, worker_id) = setup_compute_env(80).await;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        worker_id as _,
    ));
    let original_groups = hummock_manager
        .get_current_version()
        .await
        .levels
        .keys()
        .cloned()
        .sorted()
        .collect_vec();
    assert_eq!(original_groups, vec![2, 3]);

    hummock_manager
        .register_table_ids_for_test(&[(100, 2), (101, 2)])
        .await
        .unwrap();
    let sst_1 = LocalSstableInfo {
        sst_info: gen_sstable_info(1, vec![100], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };

    let sst_2 = LocalSstableInfo {
        sst_info: gen_sstable_info(2, vec![100, 101], test_epoch(20)),
        table_stats: Default::default(),
        created_at: u64::MAX,
    };
    let sst_3 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 3,
            object_id: 3,
            ..sst_2.sst_info.get_inner()
        }
        .into(),
        ..sst_2.clone()
    };
    let sst_4 = LocalSstableInfo {
        sst_info: SstableInfoInner {
            sst_id: 4,
            object_id: 4,
            ..sst_1.sst_info.get_inner()
        }
        .into(),
        ..sst_1.clone()
    };
    hummock_meta_client
        .commit_epoch(
            30,
            SyncResult {
                uncommitted_ssts: vec![sst_1, sst_2, sst_3, sst_4],
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Now group 2 has member tables [100,101,102], so split [100, 101] can succeed even though
    // there is no data of 102.
    hummock_manager
        .register_table_ids_for_test(&[(102, 2)])
        .await
        .unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();

    // for task pending
    let _task = hummock_manager
        .get_compact_task(compaction_group_id, &mut default_compaction_selector())
        .await
        .unwrap()
        .unwrap();

    // will cancel the task
    hummock_manager
        .move_state_tables_to_dedicated_compaction_group(compaction_group_id, &[100], None)
        .await
        .unwrap();

    let current_version = hummock_manager.get_current_version().await;
    let left_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 100).await;
    let right_compaction_group_id =
        get_compaction_group_id_by_table_id(hummock_manager.clone(), 101).await;
    assert_eq!(current_version.levels.len(), 3);
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(right_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .sorted()
            .collect_vec(),
        vec![101, 102]
    );
    assert_eq!(
        current_version
            .state_table_info
            .compaction_group_member_table_ids(left_compaction_group_id)
            .iter()
            .map(|table_id| table_id.table_id)
            .collect_vec(),
        vec![100]
    );

    let current_version_2 = hummock_manager.get_current_version().await;
    // The compaction task is cancelled or failed.
    assert_eq!(current_version_2, current_version);

    let task2 = hummock_manager
        .get_compact_task(
            right_compaction_group_id,
            &mut default_compaction_selector(),
        )
        .await
        .unwrap()
        .unwrap();

    let created_tables = HashSet::from_iter(vec![100, 101, 102]);

    // will cancel the task2
    hummock_manager
        .merge_compaction_group_for_test(
            left_compaction_group_id,
            right_compaction_group_id,
            created_tables,
        )
        .await
        .unwrap();

    let report_sst_id = 100;
    let ret = hummock_manager
        .report_compact_task(
            task2.task_id,
            TaskStatus::Success,
            vec![SstableInfoInner {
                object_id: report_sst_id,
                sst_id: report_sst_id,
                key_range: KeyRange::default(),
                table_ids: vec![100],
                min_epoch: 20,
                max_epoch: 20,
                file_size: 100,
                sst_size: 100,
                ..Default::default()
            }
            .into()],
            None,
            HashMap::default(),
        )
        .await
        .unwrap();
    // has been cancelled by merge
    assert!(!ret);

    let current_version_3 = hummock_manager.get_current_version().await;
    let sst_ids = current_version_3
        .get_sst_ids_by_group_id(left_compaction_group_id)
        .collect::<HashSet<_>>();
    // task 2 report failed due to the compaction group is merged
    assert!(!sst_ids.contains(&report_sst_id));
}

#[tokio::test]
async fn test_vacuum() {
    let (_env, hummock_manager, _cluster_manager, worker_id) = setup_compute_env(80).await;
    let context_id = worker_id as _;
    let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
        hummock_manager.clone(),
        context_id,
    ));
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    hummock_manager.pin_version(context_id).await.unwrap();
    let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
    let sst_infos = add_test_tables(
        hummock_manager.as_ref(),
        hummock_meta_client.clone(),
        compaction_group_id,
    )
    .await;
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);
    hummock_manager.create_version_checkpoint(1).await.unwrap();
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 6);
    assert_eq!(hummock_manager.delete_version_deltas().await.unwrap(), 0);

    hummock_manager
        .unpin_version_before(context_id, HummockVersionId::MAX)
        .await
        .unwrap();
    hummock_manager.create_version_checkpoint(0).await.unwrap();
    let deleted = hummock_manager
        .complete_gc_batch(
            sst_infos
                .iter()
                .flat_map(|ssts| ssts.iter().map(|s| s.object_id))
                .collect(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(deleted, 3);
}
