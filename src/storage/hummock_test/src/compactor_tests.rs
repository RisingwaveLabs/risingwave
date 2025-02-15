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

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
    use std::ops::Bound;
    use std::sync::Arc;

    use bytes::{BufMut, Bytes, BytesMut};
    use foyer::CacheHint;
    use itertools::Itertools;
    use rand::{Rng, RngCore, SeedableRng};
    use risingwave_common::bitmap::BitmapBuilder;
    use risingwave_common::catalog::TableId;
    use risingwave_common::constants::hummock::CompactionFilterFlag;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{test_epoch, Epoch, EpochExt};
    use risingwave_common_service::NotificationClient;
    use risingwave_hummock_sdk::compact_task::CompactTask;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{
        next_key, prefix_slice_with_vnode, prefixed_range_with_vnode, FullKey, TableKey,
        TABLE_PREFIX_LEN,
    };
    use risingwave_hummock_sdk::key_range::KeyRange;
    use risingwave_hummock_sdk::level::InputLevel;
    use risingwave_hummock_sdk::sstable_info::SstableInfo;
    use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
    use risingwave_hummock_sdk::table_watermark::{
        ReadTableWatermark, TableWatermarks, VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
    };
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{can_concat, CompactionGroupId};
    use risingwave_meta::hummock::compaction::selector::{
        default_compaction_selector, ManualCompactionOption,
    };
    use risingwave_meta::hummock::test_utils::{
        get_compaction_group_id_by_table_id, register_table_ids_to_compaction_group,
        setup_compute_env, unregister_table_ids_from_compaction_group,
    };
    use risingwave_meta::hummock::{HummockManagerRef, MockHummockMetaClient};
    use risingwave_pb::hummock::TableOption;
    use risingwave_rpc_client::HummockMetaClient;
    use risingwave_storage::compaction_catalog_manager::{
        CompactionCatalogAgent, CompactionCatalogAgentRef, FilterKeyExtractorImpl,
        FixedLengthFilterKeyExtractor, MultiFilterKeyExtractor,
    };
    use risingwave_storage::hummock::compactor::compactor_runner::{
        compact_with_agent, CompactorRunner,
    };
    use risingwave_storage::hummock::compactor::fast_compactor_runner::CompactorRunner as FastCompactorRunner;
    use risingwave_storage::hummock::compactor::{
        CompactionExecutor, CompactorContext, DummyCompactionFilter, TaskProgress,
    };
    use risingwave_storage::hummock::iterator::test_utils::mock_sstable_store;
    use risingwave_storage::hummock::iterator::{
        ConcatIterator, NonPkPrefixSkipWatermarkIterator, NonPkPrefixSkipWatermarkState,
        PkPrefixSkipWatermarkIterator, PkPrefixSkipWatermarkState, UserIterator,
    };
    use risingwave_storage::hummock::sstable_store::SstableStoreRef;
    use risingwave_storage::hummock::test_utils::*;
    use risingwave_storage::hummock::value::HummockValue;
    use risingwave_storage::hummock::{
        BlockedXor16FilterBuilder, CachePolicy, CompressionAlgorithm, FilterBuilder,
        HummockStorage as GlobalHummockStorage, HummockStorage, LocalHummockStorage, MemoryLimiter,
        SharedComapctorObjectIdManager, Sstable, SstableBuilder, SstableBuilderOptions,
        SstableIteratorReadOptions, SstableObjectIdManager, SstableWriterOptions,
    };
    use risingwave_storage::monitor::{CompactorMetrics, StoreLocalStatistic};
    use risingwave_storage::opts::StorageOpts;
    use risingwave_storage::storage_value::StorageValue;
    use risingwave_storage::store::*;

    use crate::get_notification_client_for_test;
    use crate::local_state_store_test_utils::LocalStateStoreTestExt;
    use crate::test_utils::{
        register_tables_with_id_for_test, update_filter_key_extractor_for_table_ids,
        TestIngestBatch,
    };

    pub(crate) async fn get_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
        hummock_manager_ref: &HummockManagerRef,
        table_ids: &[u32],
    ) -> HummockStorage {
        let remote_dir = "hummock_001_test".to_owned();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store().await;

        let hummock = GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap();

        register_tables_with_id_for_test(
            hummock.compaction_catalog_manager_ref(),
            hummock_manager_ref,
            table_ids,
        )
        .await;

        hummock
    }

    async fn get_global_hummock_storage(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        notification_client: impl NotificationClient,
    ) -> GlobalHummockStorage {
        let remote_dir = "hummock_001_test".to_owned();
        let options = Arc::new(StorageOpts {
            sstable_size_mb: 1,
            block_size_kb: 1,
            bloom_false_positive: 0.1,
            data_directory: remote_dir.clone(),
            write_conflict_detection_enabled: true,
            ..Default::default()
        });
        let sstable_store = mock_sstable_store().await;

        GlobalHummockStorage::for_test(
            options,
            sstable_store,
            hummock_meta_client.clone(),
            notification_client,
        )
        .await
        .unwrap()
    }

    async fn prepare_test_put_data(
        storage: &HummockStorage,
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        key: &Bytes,
        value_size: usize,
        epochs: Vec<u64>,
    ) {
        for epoch in &epochs {
            storage.start_epoch(*epoch, HashSet::from_iter([Default::default()]));
        }
        let mut local = storage
            .new_local(NewLocalOptions::for_test(TableId::default()))
            .await;
        let table_id = local.table_id();
        let table_id_set = HashSet::from_iter([table_id]);
        // 1. add sstables
        let val = b"0"[..].repeat(value_size);
        local.init_for_test(epochs[0]).await.unwrap();
        for (i, &e) in epochs.iter().enumerate() {
            let epoch = e;
            let val_str = e / test_epoch(1);
            let mut new_val = val.clone();
            new_val.extend_from_slice(&val_str.to_be_bytes());
            local
                .ingest_batch(
                    vec![(
                        TableKey(key.clone()),
                        StorageValue::new_put(Bytes::from(new_val)),
                    )],
                    WriteOptions {
                        epoch,
                        table_id: Default::default(),
                    },
                )
                .await
                .unwrap();
            if i + 1 < epochs.len() {
                local.seal_current_epoch(epochs[i + 1], SealCurrentEpochOptions::for_test());
            } else {
                local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());
            }
            let res = storage
                .seal_and_sync_epoch(epoch, table_id_set.clone())
                .await
                .unwrap();
            hummock_meta_client.commit_epoch(epoch, res).await.unwrap();
        }
    }

    pub fn get_compactor_context(storage: &HummockStorage) -> CompactorContext {
        get_compactor_context_impl(storage.storage_opts().clone(), storage.sstable_store())
    }

    fn get_compactor_context_impl(
        storage_opts: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
    ) -> CompactorContext {
        CompactorContext {
            storage_opts,
            sstable_store,
            compactor_metrics: Arc::new(CompactorMetrics::unused()),
            is_share_buffer_compact: false,
            compaction_executor: Arc::new(CompactionExecutor::new(Some(1))),
            memory_limiter: MemoryLimiter::unlimit(),
            task_progress_manager: Default::default(),
            await_tree_reg: None,
        }
    }

    #[tokio::test]
    async fn test_compaction_same_key_not_split() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let table_id = 0;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[table_id],
        )
        .await;

        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables with 1MB value
        let mut key = BytesMut::default();
        key.put_u16(0);
        key.put_slice(b"same_key");
        let key = key.freeze();
        const SST_COUNT: u64 = 16;

        let mut val = b"0"[..].repeat(1 << 20);
        val.extend_from_slice(&SST_COUNT.to_be_bytes());
        prepare_test_put_data(
            &storage,
            &hummock_meta_client,
            &key,
            1 << 20,
            (1..SST_COUNT + 1).map(test_epoch).collect_vec(),
        )
        .await;

        let compaction_catalog_agent_ref = storage
            .compaction_catalog_manager_ref()
            .acquire(vec![table_id])
            .await
            .unwrap();

        // 2. get compact task
        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), table_id).await;
        while let Some(compact_task) = hummock_manager_ref
            .get_compact_task(compaction_group_id, &mut default_compaction_selector())
            .await
            .unwrap()
        {
            // 3. compact
            let (_tx, rx) = tokio::sync::oneshot::channel();
            let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
                compact_ctx.clone(),
                compact_task.clone(),
                rx,
                Box::new(sstable_object_id_manager.clone()),
                compaction_catalog_agent_ref.clone(),
            )
            .await;

            hummock_manager_ref
                .report_compact_task(
                    result_task.task_id,
                    result_task.task_status,
                    result_task.sorted_output_ssts,
                    Some(to_prost_table_stats_map(task_stats)),
                    object_timestamps,
                )
                .await
                .unwrap();
        }

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_tables = version
            .get_compaction_group_levels(compaction_group_id)
            .levels
            .iter()
            .flat_map(|level| level.table_infos.clone())
            .collect_vec();
        for output_table in &output_tables {
            let table = storage
                .sstable_store()
                .sstable(output_table, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            let target_table_size = storage.storage_opts().sstable_size_mb * (1 << 20);
            assert!(
                table.meta.estimated_size > target_table_size,
                "table.meta.estimated_size {} <= target_table_size {}",
                table.meta.estimated_size,
                target_table_size
            );
        }
        // 5. storage get back the correct kv after compaction
        storage.wait_version(version).await;
        let get_epoch = test_epoch(SST_COUNT + 1);
        let get_val = storage
            .get(
                TableKey(key.clone()),
                get_epoch,
                ReadOptions {
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .unwrap()
            .to_vec();
        assert_eq!(get_val, val);
    }

    pub(crate) async fn flush_and_commit(
        hummock_meta_client: &Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        epoch: u64,
        table_id: TableId,
    ) {
        let res = storage
            .seal_and_sync_epoch(epoch, HashSet::from([table_id]))
            .await
            .unwrap();
        hummock_meta_client.commit_epoch(epoch, res).await.unwrap();
    }

    async fn prepare_data(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        storage: &HummockStorage,
        existing_table_id: u32,
        keys_per_epoch: usize,
    ) {
        let table_id = existing_table_id.into();
        let kv_count: u16 = 128;
        let mut epoch = test_epoch(1);
        let mut local = storage.new_local(NewLocalOptions::for_test(table_id)).await;
        let table_id_set = HashSet::from_iter([table_id]);

        storage.start_epoch(epoch, table_id_set);

        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value
        for idx in 0..kv_count {
            if idx == 0 {
                local.init_for_test(epoch).await.unwrap();
            }

            for _ in 0..keys_per_epoch {
                let mut key = idx.to_be_bytes().to_vec();
                let ramdom_key = rand::thread_rng().gen::<[u8; 32]>();
                key.extend_from_slice(&ramdom_key);
                local
                    .insert(TableKey(Bytes::from(key)), val.clone(), None)
                    .unwrap();
            }
            local.flush().await.unwrap();
            let next_epoch = epoch.next_epoch();
            storage.start_epoch(next_epoch, HashSet::from_iter([table_id]));
            local.seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());

            flush_and_commit(&hummock_meta_client, storage, epoch, table_id).await;
            epoch.inc_epoch();
        }
    }

    #[tokio::test]
    async fn test_compaction_drop_all_key() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let notification_client = get_notification_client_for_test(
            env,
            hummock_manager_ref.clone(),
            cluster_ctl_ref,
            worker_id,
        )
        .await;

        let existing_table_id: u32 = 1;
        let storage_existing_table_id = get_hummock_storage(
            hummock_meta_client.clone(),
            notification_client,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;

        prepare_data(
            hummock_meta_client.clone(),
            &storage_existing_table_id,
            existing_table_id,
            1,
        )
        .await;

        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), existing_table_id)
                .await;

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[existing_table_id])
            .await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .manual_get_compact_task(compaction_group_id, manual_compcation_option)
            .await
            .unwrap();

        assert!(compact_task.is_none());

        let current_version = hummock_manager_ref.get_current_version().await;
        assert!(current_version
            .get_sst_ids_by_group_id(compaction_group_id)
            .collect_vec()
            .is_empty());
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_existing_table_id() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let global_storage = get_global_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
        )
        .await;

        // register the local_storage to global_storage
        let mut storage_1 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(1)))
            .await;
        let mut storage_2 = global_storage
            .new_local(NewLocalOptions::for_test(TableId::from(2)))
            .await;

        let table_id_1 = storage_1.table_id();
        let table_id_2 = storage_2.table_id();
        let table_id_set = HashSet::from_iter([table_id_1, table_id_2]);

        update_filter_key_extractor_for_table_ids(
            global_storage.compaction_catalog_manager_ref(),
            &[table_id_1.table_id(), table_id_2.table_id()],
        );

        let compaction_catalog_agent_ref = global_storage
            .compaction_catalog_manager_ref()
            .acquire(vec![table_id_1.table_id(), table_id_2.table_id()])
            .await
            .unwrap();

        let compact_ctx = get_compactor_context_impl(
            global_storage.storage_opts().clone(),
            global_storage.sstable_store(),
        );
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            global_storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables
        let val = Bytes::from(b"0"[..].repeat(1 << 10)); // 1024 Byte value

        let drop_table_id = table_id_1.table_id();
        let existing_table_id = table_id_2.table_id();
        let kv_count: usize = 128;
        let mut epoch = test_epoch(1);
        register_table_ids_to_compaction_group(
            &hummock_manager_ref,
            &[drop_table_id, existing_table_id],
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;

        global_storage
            .wait_version(hummock_manager_ref.get_current_version().await)
            .await;

        let vnode = VirtualNode::from_index(1);
        global_storage.start_epoch(epoch, table_id_set.clone());
        for index in 0..kv_count {
            let next_epoch = epoch.next_epoch();
            global_storage.start_epoch(next_epoch, table_id_set.clone());
            if index == 0 {
                storage_1.init_for_test(epoch).await.unwrap();
                storage_2.init_for_test(epoch).await.unwrap();
            }

            let (storage, other) = if index % 2 == 0 {
                (&mut storage_1, &mut storage_2)
            } else {
                (&mut storage_2, &mut storage_1)
            };

            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.extend_from_slice(&vnode.to_be_bytes());
            prefix.put_slice(random_key.as_slice());

            storage
                .insert(TableKey(prefix.freeze()), val.clone(), None)
                .unwrap();
            storage.flush().await.unwrap();
            storage.seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());
            other.seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());

            let res = global_storage
                .seal_and_sync_epoch(epoch, table_id_set.clone())
                .await
                .unwrap();
            hummock_meta_client.commit_epoch(epoch, res).await.unwrap();
            epoch.inc_epoch();
        }

        // Mimic dropping table
        unregister_table_ids_from_compaction_group(&hummock_manager_ref, &[drop_table_id]).await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), existing_table_id)
                .await;
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(compaction_group_id, manual_compcation_option)
            .await
            .unwrap()
            .unwrap();
        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .filter(|level| level.level_idx != compact_task.target_level)
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count
        );

        // 4. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            compaction_catalog_agent_ref.clone(),
        )
        .await;
        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status,
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
                object_timestamps,
            )
            .await
            .unwrap();

        // 5. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(compaction_group_id, |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += global_storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .meta
                .key_count;
        }
        assert_eq!((kv_count / 2) as u32, key_count);

        // 6. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(compaction_group_id, &mut default_compaction_selector())
            .await
            .unwrap();
        assert!(compact_task.is_none());

        // to update version for hummock_storage
        global_storage.wait_version(version).await;

        // 7. scan kv to check key table_id
        let scan_result = global_storage
            .scan(
                prefixed_range_with_vnode(
                    (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                    vnode,
                ),
                epoch,
                None,
                ReadOptions {
                    table_id: TableId::from(existing_table_id),
                    prefetch_options: PrefetchOptions::default(),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_drop_key_by_retention_seconds() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let existing_table_id = 2;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;

        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));

        let compaction_catalog_agent_ref = storage
            .compaction_catalog_manager_ref()
            .acquire(vec![existing_table_id])
            .await
            .unwrap();
        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value

        let kv_count = 11;
        let prev_epoch: u64 = hummock_manager_ref
            .get_current_version()
            .await
            .table_committed_epoch(existing_table_id.into())
            .unwrap();
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let vnode = VirtualNode::from_index(1);
        let mut epoch_set = BTreeSet::new();

        let table_id_set = HashSet::from_iter([existing_table_id.into()]);
        storage.start_epoch(epoch, table_id_set.clone());

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        for i in 0..kv_count {
            let next_epoch = epoch + millisec_interval_epoch;
            storage.start_epoch(next_epoch, table_id_set.clone());
            if i == 0 {
                local
                    .init_for_test_with_prev_epoch(epoch, prev_epoch)
                    .await
                    .unwrap();
            }
            epoch_set.insert(epoch);
            let mut prefix = BytesMut::default();
            let random_key = rand::thread_rng().gen::<[u8; 32]>();
            prefix.extend_from_slice(&vnode.to_be_bytes());
            prefix.put_slice(random_key.as_slice());

            local
                .insert(TableKey(prefix.freeze()), val.clone(), None)
                .unwrap();
            local.flush().await.unwrap();
            local.seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());

            let res = storage
                .seal_and_sync_epoch(epoch, table_id_set.clone())
                .await
                .unwrap();
            hummock_meta_client.commit_epoch(epoch, res).await.unwrap();
            epoch += millisec_interval_epoch;
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), existing_table_id)
                .await;
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(compaction_group_id, manual_compcation_option)
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        let retention_seconds_expire_second = 1;
        compact_task.table_options = BTreeMap::from_iter([(
            existing_table_id,
            TableOption {
                retention_seconds: Some(retention_seconds_expire_second),
            },
        )]);
        compact_task.current_epoch_time = Epoch::now().0;

        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            kv_count,
        );

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            compaction_catalog_agent_ref.clone(),
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status,
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
                object_timestamps,
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let mut tables_from_version = vec![];
        version.level_iter(compaction_group_id, |level| {
            tables_from_version.extend(level.table_infos.iter().cloned());
            true
        });

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(&table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32 - retention_seconds_expire_second + 1;
        assert_eq!(expect_count, key_count); // retention_seconds will clean the key (which epoch < epoch - retention_seconds)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(compaction_group_id, &mut default_compaction_selector())
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch.inc_epoch();
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let scan_result = storage
            .scan(
                prefixed_range_with_vnode(
                    (Bound::<Bytes>::Unbounded, Bound::<Bytes>::Unbounded),
                    vnode,
                ),
                epoch,
                None,
                ReadOptions {
                    table_id: TableId::from(existing_table_id),
                    prefetch_options: PrefetchOptions::default(),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[tokio::test]
    async fn test_compaction_with_filter_key_extractor() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let existing_table_id = 2;
        let mut key = BytesMut::default();
        key.put_u16(1);
        key.put_slice(b"key_prefix");
        let key_prefix = key.freeze();
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;

        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        multi_filter_key_extractor.register(
            existing_table_id,
            FilterKeyExtractorImpl::FixedLength(FixedLengthFilterKeyExtractor::new(
                TABLE_PREFIX_LEN + key_prefix.len(),
            )),
        );

        let table_id_to_vnode =
            HashMap::from_iter([(existing_table_id, VirtualNode::COUNT_FOR_TEST)]);
        let table_id_to_watermark_serde = HashMap::from_iter(vec![(0, None)]);
        let compaction_catalog_agent_ref = Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(multi_filter_key_extractor),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        ));

        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        // 1. add sstables
        let val = Bytes::from(b"0"[..].to_vec()); // 1 Byte value
        let kv_count = 11;
        let prev_epoch: u64 = hummock_manager_ref
            .get_current_version()
            .await
            .table_committed_epoch(existing_table_id.into())
            .unwrap();
        // let base_epoch = Epoch(0);
        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;
        let mut epoch_set = BTreeSet::new();

        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        let table_id_set = HashSet::from_iter([existing_table_id.into()]);
        storage.start_epoch(epoch, table_id_set.clone());
        for i in 0..kv_count {
            if i == 0 {
                local
                    .init_for_test_with_prev_epoch(epoch, prev_epoch)
                    .await
                    .unwrap();
            }
            let next_epoch = epoch + millisec_interval_epoch;
            storage.start_epoch(next_epoch, table_id_set.clone());
            epoch_set.insert(epoch);

            let ramdom_key = [key_prefix.as_ref(), &rand::thread_rng().gen::<[u8; 32]>()].concat();
            local
                .insert(TableKey(Bytes::from(ramdom_key)), val.clone(), None)
                .unwrap();
            local.flush().await.unwrap();
            local.seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());
            let res = storage
                .seal_and_sync_epoch(epoch, table_id_set.clone())
                .await
                .unwrap();
            hummock_meta_client.commit_epoch(epoch, res).await.unwrap();
            epoch += millisec_interval_epoch;
        }

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), existing_table_id)
                .await;
        // 2. get compact task
        let mut compact_task = hummock_manager_ref
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
            kv_count,
        );

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        compact_task.current_epoch_time = Epoch::now().0;

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            compaction_catalog_agent_ref.clone(),
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status,
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
                object_timestamps,
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version: HummockVersion = hummock_manager_ref.get_current_version().await;
        let tables_from_version: Vec<_> = version
            .get_compaction_group_levels(compaction_group_id)
            .levels
            .iter()
            .flat_map(|level| level.table_infos.iter())
            .collect::<Vec<_>>();

        let mut key_count = 0;
        for table in tables_from_version {
            key_count += storage
                .sstable_store()
                .sstable(table, &mut StoreLocalStatistic::default())
                .await
                .unwrap()
                .meta
                .key_count;
        }
        let expect_count = kv_count as u32;
        assert_eq!(expect_count, key_count); // ttl will clean the key (which epoch < epoch - ttl)

        // 5. get compact task and there should be none
        let compact_task = hummock_manager_ref
            .get_compact_task(compaction_group_id, &mut default_compaction_selector())
            .await
            .unwrap();
        assert!(compact_task.is_none());

        epoch.inc_epoch();
        // to update version for hummock_storage
        storage.wait_version(version).await;

        // 6. scan kv to check key table_id
        let bloom_filter_key = [
            existing_table_id.to_be_bytes().to_vec(),
            key_prefix.to_vec(),
        ]
        .concat();
        let start_bound_key = TableKey(key_prefix);
        let end_bound_key = TableKey(Bytes::from(next_key(start_bound_key.as_ref())));
        let scan_result = storage
            .scan(
                (
                    Bound::Included(start_bound_key),
                    Bound::Excluded(end_bound_key),
                ),
                epoch,
                None,
                ReadOptions {
                    prefix_hint: Some(Bytes::from(bloom_filter_key)),
                    table_id: TableId::from(existing_table_id),
                    prefetch_options: PrefetchOptions::default(),
                    cache_policy: CachePolicy::Fill(CacheHint::Normal),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let mut scan_count = 0;
        for (k, _) in scan_result {
            let table_id = k.user_key.table_id.table_id();
            assert_eq!(table_id, existing_table_id);
            scan_count += 1;
        }
        assert_eq!(key_count, scan_count);
    }

    #[ignore]
    // may update the test after https://github.com/risingwavelabs/risingwave/pull/14320 is merged.
    // This PR will support
    #[tokio::test]
    async fn test_compaction_delete_range() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;
        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));
        prepare_data(hummock_meta_client.clone(), &storage, existing_table_id, 2).await;
        let mut local = storage
            .new_local(NewLocalOptions::for_test(existing_table_id.into()))
            .await;
        let epoch = test_epoch(130);
        local.init_for_test(epoch).await.unwrap();

        local.flush().await.unwrap();
        // TODO: seal with table watermark like the following code
        // let prefix_key_range = |k: u16| {
        //     let key = k.to_be_bytes();
        //     (
        //         Bound::Included(Bytes::copy_from_slice(key.as_slice())),
        //         Bound::Excluded(Bytes::copy_from_slice(next_key(key.as_slice()).as_slice())),
        //     )
        // };
        // local
        //     .flush(vec![prefix_key_range(1u16), prefix_key_range(2u16)])
        //     .await
        //     .unwrap();
        local.seal_current_epoch(u64::MAX, SealCurrentEpochOptions::for_test());

        flush_and_commit(
            &hummock_meta_client,
            &storage,
            epoch,
            existing_table_id.into(),
        )
        .await;

        let manual_compcation_option = ManualCompactionOption {
            level: 0,
            ..Default::default()
        };
        // 2. get compact task
        let compaction_group_id =
            get_compaction_group_id_by_table_id(hummock_manager_ref.clone(), existing_table_id)
                .await;
        let mut compact_task = hummock_manager_ref
            .manual_get_compact_task(compaction_group_id, manual_compcation_option)
            .await
            .unwrap()
            .unwrap();

        let compaction_filter_flag = CompactionFilterFlag::STATE_CLEAN;
        compact_task.compaction_filter_mask = compaction_filter_flag.bits();
        // assert compact_task
        assert_eq!(
            compact_task
                .input_ssts
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>(),
            129
        );

        let compaction_catalog_agent_ref = storage
            .compaction_catalog_manager_ref()
            .acquire(vec![existing_table_id])
            .await
            .unwrap();

        // 3. compact
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
            compact_ctx,
            compact_task.clone(),
            rx,
            Box::new(sstable_object_id_manager.clone()),
            compaction_catalog_agent_ref.clone(),
        )
        .await;

        hummock_manager_ref
            .report_compact_task(
                result_task.task_id,
                result_task.task_status,
                result_task.sorted_output_ssts,
                Some(to_prost_table_stats_map(task_stats)),
                object_timestamps,
            )
            .await
            .unwrap();

        // 4. get the latest version and check
        let version = hummock_manager_ref.get_current_version().await;
        let output_level_info = version
            .get_compaction_group_levels(compaction_group_id)
            .levels
            .last()
            .unwrap();
        assert_eq!(1, output_level_info.table_infos.len());
        assert_eq!(252, output_level_info.table_infos[0].total_key_count);
    }

    type KeyValue = (FullKey<Vec<u8>>, HummockValue<Vec<u8>>);
    async fn check_compaction_result(
        sstable_store: SstableStoreRef,
        ret: Vec<SstableInfo>,
        fast_ret: Vec<SstableInfo>,
        capacity: u64,
    ) {
        let mut fast_tables = Vec::with_capacity(fast_ret.len());
        let mut normal_tables = Vec::with_capacity(ret.len());
        let mut stats = StoreLocalStatistic::default();
        for sst_info in &fast_ret {
            fast_tables.push(sstable_store.sstable(sst_info, &mut stats).await.unwrap());
        }

        for sst_info in &ret {
            normal_tables.push(sstable_store.sstable(sst_info, &mut stats).await.unwrap());
        }
        assert!(fast_ret.iter().all(|f| f.file_size < capacity * 6 / 5));
        assert!(can_concat(&ret));
        assert!(can_concat(&fast_ret));
        let read_options = Arc::new(SstableIteratorReadOptions::default());

        let mut normal_iter = UserIterator::for_test(
            ConcatIterator::new(ret, sstable_store.clone(), read_options.clone()),
            (Bound::Unbounded, Bound::Unbounded),
        );
        let mut fast_iter = UserIterator::for_test(
            ConcatIterator::new(fast_ret, sstable_store.clone(), read_options.clone()),
            (Bound::Unbounded, Bound::Unbounded),
        );

        normal_iter.rewind().await.unwrap();
        fast_iter.rewind().await.unwrap();
        let mut count = 0;
        while normal_iter.is_valid() {
            assert_eq!(
                normal_iter.key(),
                fast_iter.key(),
                "not equal in {}, len: {} {} vs {}",
                count,
                normal_iter.key().user_key.table_key.as_ref().len(),
                u64::from_be_bytes(
                    normal_iter.key().user_key.table_key.as_ref()[0..8]
                        .try_into()
                        .unwrap()
                ),
                u64::from_be_bytes(
                    fast_iter.key().user_key.table_key.as_ref()[0..8]
                        .try_into()
                        .unwrap()
                ),
            );
            let hash = Sstable::hash_for_bloom_filter(
                fast_iter.key().user_key.encode().as_slice(),
                fast_iter.key().user_key.table_id.table_id,
            );
            assert_eq!(normal_iter.value(), fast_iter.value());
            let key = fast_iter.key();
            let key_ref = key.user_key.as_ref();
            assert!(normal_tables.iter().any(|table| {
                table.may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            }));
            assert!(fast_tables.iter().any(|table| {
                table.may_match_hash(&(Bound::Included(key_ref), Bound::Included(key_ref)), hash)
            }));
            normal_iter.next().await.unwrap();
            fast_iter.next().await.unwrap();
            count += 1;
        }
    }

    async fn run_fast_and_normal_runner(
        compact_ctx: CompactorContext,
        task: CompactTask,
        compaction_catalog_agent_ref: CompactionCatalogAgentRef,
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        let compaction_filter = DummyCompactionFilter {};
        let slow_compact_runner = CompactorRunner::new(
            0,
            compact_ctx.clone(),
            task.clone(),
            Box::new(SharedComapctorObjectIdManager::for_test(
                VecDeque::from_iter([5, 6, 7, 8, 9, 10, 11, 12, 13]),
            )),
        );

        let fast_compact_runner = FastCompactorRunner::new(
            compact_ctx.clone(),
            task.clone(),
            compaction_catalog_agent_ref.clone(),
            Box::new(SharedComapctorObjectIdManager::for_test(
                VecDeque::from_iter([22, 23, 24, 25, 26, 27, 28, 29]),
            )),
            Arc::new(TaskProgress::default()),
        );
        let (_, ret1, _) = slow_compact_runner
            .run(
                compaction_filter,
                compaction_catalog_agent_ref,
                Arc::new(TaskProgress::default()),
            )
            .await
            .unwrap();
        let ret = ret1.into_iter().map(|sst| sst.sst_info).collect_vec();
        let (ssts, _) = fast_compact_runner.run().await.unwrap();
        let fast_ret = ssts.into_iter().map(|sst| sst.sst_info).collect_vec();
        (ret, fast_ret)
    }

    async fn test_fast_compact_impl(data: Vec<Vec<KeyValue>>) {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;
        hummock_manager_ref.get_new_sst_ids(10).await.unwrap();
        let compact_ctx = get_compactor_context(&storage);
        let compaction_catalog_agent_ref =
            CompactionCatalogAgent::for_test(vec![existing_table_id]);

        let sstable_store = compact_ctx.sstable_store.clone();
        let capacity = 256 * 1024;
        let options = SstableBuilderOptions {
            capacity,
            block_capacity: 2048,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };
        let capacity = options.capacity as u64;
        let mut ssts = vec![];
        for (idx, sst_input) in data.into_iter().enumerate() {
            let sst = gen_test_sstable_info(
                options.clone(),
                (idx + 1) as u64,
                sst_input,
                sstable_store.clone(),
            )
            .await;
            ssts.push(sst);
        }
        let select_file_count = ssts.len() / 2;

        let task = CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 5,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: ssts.drain(..select_file_count).collect_vec(),
                },
                InputLevel {
                    level_idx: 6,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: ssts,
                },
            ],
            existing_table_ids: vec![1],
            task_id: 1,
            splits: vec![KeyRange::inf()],
            target_level: 6,
            base_level: 4,
            target_file_size: capacity,
            compression_algorithm: 1,
            gc_delete_keys: true,
            ..Default::default()
        };
        let (ret, fast_ret) =
            run_fast_and_normal_runner(compact_ctx.clone(), task, compaction_catalog_agent_ref)
                .await;
        check_compaction_result(compact_ctx.sstable_store, ret, fast_ret, capacity).await;
    }

    #[tokio::test]
    async fn test_fast_compact() {
        const KEY_COUNT: usize = 20000;
        let mut last_k: u64 = 0;
        let mut rng = rand::rngs::StdRng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let mut data1 = Vec::with_capacity(KEY_COUNT / 2);
        let mut data = Vec::with_capacity(KEY_COUNT);
        let mut last_epoch = test_epoch(400);
        for _ in 0..KEY_COUNT {
            let rand_v = rng.next_u32() % 100;
            let (k, epoch) = if rand_v == 0 {
                (last_k + 2000, test_epoch(400))
            } else if rand_v < 5 {
                (last_k, last_epoch - test_epoch(1))
            } else {
                (last_k + 1, test_epoch(400))
            };
            let key = k.to_be_bytes().to_vec();
            let key = FullKey::new(TableId::new(1), TableKey(key), epoch);
            let rand_v = rng.next_u32() % 10;
            let v = if rand_v == 1 {
                HummockValue::delete()
            } else {
                HummockValue::put(format!("sst1-{}", epoch).into_bytes())
            };
            if last_k != k && data1.is_empty() && data.len() >= KEY_COUNT / 2 {
                std::mem::swap(&mut data, &mut data1);
            }
            data.push((key, v));
            last_k = k;
            last_epoch = epoch;
        }
        let data2 = data;
        let mut data3 = Vec::with_capacity(KEY_COUNT);
        let mut data = Vec::with_capacity(KEY_COUNT);
        let mut last_k: u64 = 0;
        let max_epoch = std::cmp::min(test_epoch(300), last_epoch - test_epoch(1));
        last_epoch = max_epoch;

        for _ in 0..KEY_COUNT * 4 {
            let rand_v = rng.next_u32() % 100;
            let (k, epoch) = if rand_v == 0 {
                (last_k + 1000, max_epoch)
            } else if rand_v < 5 {
                (last_k, last_epoch - test_epoch(1))
            } else {
                (last_k + 1, max_epoch)
            };
            let key = k.to_be_bytes().to_vec();
            let key = FullKey::new(TableId::new(1), TableKey(key), epoch);
            let v = HummockValue::put(format!("sst2-{}", epoch).into_bytes());
            if last_k != k && data3.is_empty() && data.len() >= KEY_COUNT {
                std::mem::swap(&mut data, &mut data3);
            }
            data.push((key, v));
            last_k = k;
            last_epoch = epoch;
        }
        let data4 = data;
        test_fast_compact_impl(vec![data1, data2, data3, data4]).await;
    }

    #[tokio::test]
    async fn test_fast_compact_cut_file() {
        const KEY_COUNT: usize = 20000;
        let mut rng = rand::rngs::StdRng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let mut data1 = Vec::with_capacity(KEY_COUNT / 2);
        let epoch1 = test_epoch(400);
        for start_idx in 0..3 {
            let base = start_idx * KEY_COUNT;
            for k in 0..KEY_COUNT / 3 {
                let key = (k + base).to_be_bytes().to_vec();
                let key = FullKey::new(TableId::new(1), TableKey(key), epoch1);
                let rand_v = rng.next_u32() % 10;
                let v = if rand_v == 1 {
                    HummockValue::delete()
                } else {
                    HummockValue::put(format!("sst1-{}", 400).into_bytes())
                };
                data1.push((key, v));
            }
        }

        let mut data2 = Vec::with_capacity(KEY_COUNT);
        let epoch2 = test_epoch(300);
        for k in 0..KEY_COUNT * 4 {
            let key = k.to_be_bytes().to_vec();
            let key = FullKey::new(TableId::new(1), TableKey(key), epoch2);
            let v = HummockValue::put(format!("sst2-{}", 300).into_bytes());
            data2.push((key, v));
        }
        test_fast_compact_impl(vec![data1, data2]).await;
    }

    #[tokio::test]
    async fn test_tombstone_recycle() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;
        hummock_manager_ref.get_new_sst_ids(10).await.unwrap();
        let compact_ctx = get_compactor_context(&storage);
        let compaction_catalog_agent_ref =
            CompactionCatalogAgent::for_test(vec![existing_table_id]);

        let sstable_store = compact_ctx.sstable_store.clone();
        let capacity = 256 * 1024;
        let opts = SstableBuilderOptions {
            capacity,
            block_capacity: 2048,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        const KEY_COUNT: usize = 20000;
        let mut rng = rand::rngs::StdRng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let mut sst_infos = vec![];
        let mut max_sst_file_size = 0;
        for object_id in 1..3 {
            let mut builder = SstableBuilder::<_, BlockedXor16FilterBuilder>::new(
                object_id,
                sstable_store
                    .clone()
                    .create_sst_writer(object_id, SstableWriterOptions::default()),
                BlockedXor16FilterBuilder::create(opts.bloom_false_positive, opts.capacity / 16),
                opts.clone(),
                compaction_catalog_agent_ref.clone(),
                None,
            );
            let mut last_k: u64 = 1;
            let init_epoch = test_epoch(100 * object_id);
            let mut last_epoch = init_epoch;

            for idx in 0..KEY_COUNT {
                let rand_v = rng.next_u32() % 10;
                let (k, epoch) = if rand_v == 0 {
                    (last_k + 1000 * object_id, init_epoch)
                } else if rand_v < 5 {
                    (last_k, last_epoch.prev_epoch())
                } else {
                    (last_k + 1, init_epoch)
                };
                let key = k.to_be_bytes().to_vec();
                let key = FullKey::new(TableId::new(1), TableKey(key.as_slice()), epoch);
                let rand_v = rng.next_u32() % 10;
                let v = if (5..7).contains(&rand_v) {
                    HummockValue::delete()
                } else {
                    HummockValue::put(format!("{}-{}", idx, epoch).into_bytes())
                };
                if rand_v < 5 && builder.current_block_size() > opts.block_capacity / 2 {
                    // cut block when the key is same with the last key.
                    builder.build_block().await.unwrap();
                }
                builder.add(key, v.as_slice()).await.unwrap();
                last_k = k;
                last_epoch = epoch;
            }

            let output = builder.finish().await.unwrap();
            output.writer_output.await.unwrap().unwrap();
            let sst_info = output.sst_info.sst_info;
            max_sst_file_size = std::cmp::max(max_sst_file_size, sst_info.file_size);
            sst_infos.push(sst_info);
        }

        let target_file_size = max_sst_file_size / 4;

        let task = CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 5,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: sst_infos.drain(..1).collect_vec(),
                },
                InputLevel {
                    level_idx: 6,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: sst_infos,
                },
            ],
            existing_table_ids: vec![1],
            task_id: 1,
            splits: vec![KeyRange::inf()],
            target_level: 6,
            base_level: 4,
            target_file_size,
            compression_algorithm: 1,
            gc_delete_keys: true,
            ..Default::default()
        };
        let (ret, fast_ret) = run_fast_and_normal_runner(
            compact_ctx.clone(),
            task,
            compaction_catalog_agent_ref.clone(),
        )
        .await;
        check_compaction_result(compact_ctx.sstable_store, ret, fast_ret, target_file_size).await;
    }

    #[tokio::test]
    async fn test_skip_watermark() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));
        let existing_table_id: u32 = 1;
        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[existing_table_id],
        )
        .await;
        hummock_manager_ref.get_new_sst_ids(10).await.unwrap();
        let compact_ctx = get_compactor_context(&storage);
        let compaction_catalog_agent_ref =
            CompactionCatalogAgent::for_test(vec![existing_table_id]);

        let sstable_store = compact_ctx.sstable_store.clone();
        let capacity = 256 * 1024;
        let opts = SstableBuilderOptions {
            capacity,
            block_capacity: 2048,
            restart_interval: 16,
            bloom_false_positive: 0.1,
            compression_algorithm: CompressionAlgorithm::Lz4,
            ..Default::default()
        };

        const KEY_COUNT: usize = 20000;
        let mut rng = rand::rngs::StdRng::seed_from_u64(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        let mut sst_infos = vec![];
        let mut max_sst_file_size = 0;
        for object_id in 1..3 {
            let mut builder = SstableBuilder::<_, BlockedXor16FilterBuilder>::new(
                object_id,
                sstable_store
                    .clone()
                    .create_sst_writer(object_id, SstableWriterOptions::default()),
                BlockedXor16FilterBuilder::create(opts.bloom_false_positive, opts.capacity / 16),
                opts.clone(),
                compaction_catalog_agent_ref.clone(),
                None,
            );
            let key_count = KEY_COUNT / VirtualNode::COUNT_FOR_TEST * 2;
            for vnode_id in 0..VirtualNode::COUNT_FOR_TEST / 2 {
                let mut last_k: u64 = 1;
                let init_epoch = test_epoch(100 * object_id);
                let mut last_epoch = init_epoch;
                for idx in 0..key_count {
                    let rand_v = rng.next_u32() % 10;
                    let (k, epoch) = if rand_v == 0 {
                        (last_k + 1000 * object_id, init_epoch)
                    } else if rand_v < 5 {
                        (last_k, last_epoch.prev_epoch())
                    } else {
                        (last_k + 1, init_epoch)
                    };
                    let key = prefix_slice_with_vnode(
                        VirtualNode::from_index(vnode_id),
                        k.to_be_bytes().as_slice(),
                    );
                    let key = FullKey::new(TableId::new(1), TableKey(key), epoch);
                    let rand_v = rng.next_u32() % 10;
                    let v = if (5..7).contains(&rand_v) {
                        HummockValue::delete()
                    } else {
                        HummockValue::put(format!("{}-{}", idx, epoch).into_bytes())
                    };
                    if rand_v < 5 && builder.current_block_size() > opts.block_capacity / 2 {
                        // cut block when the key is same with the last key.
                        builder.build_block().await.unwrap();
                    }
                    builder.add(key.to_ref(), v.as_slice()).await.unwrap();
                    last_k = k;
                    last_epoch = epoch;
                }
            }

            let output = builder.finish().await.unwrap();
            output.writer_output.await.unwrap().unwrap();
            let sst_info = output.sst_info.sst_info;
            max_sst_file_size = std::cmp::max(max_sst_file_size, sst_info.file_size);
            sst_infos.push(sst_info);
        }

        let target_file_size = max_sst_file_size / 4;
        let mut table_watermarks = BTreeMap::default();
        let key_count = KEY_COUNT / VirtualNode::COUNT_FOR_TEST * 2;
        let mut vnode_builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        for i in 0..VirtualNode::COUNT_FOR_TEST / 2 {
            if i % 2 == 0 {
                vnode_builder.set(i, true);
            } else {
                vnode_builder.set(i, false);
            }
        }
        let bitmap = Arc::new(vnode_builder.finish());
        let watermark_idx = key_count * 20;
        let watermark_key = Bytes::from(watermark_idx.to_be_bytes().to_vec());
        table_watermarks.insert(
            1,
            TableWatermarks {
                watermarks: vec![(
                    test_epoch(500),
                    vec![VnodeWatermark::new(bitmap.clone(), watermark_key.clone())].into(),
                )],
                direction: WatermarkDirection::Ascending,
                watermark_type: WatermarkSerdeType::PkPrefix,
            },
        );

        let task = CompactTask {
            input_ssts: vec![
                InputLevel {
                    level_idx: 5,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: sst_infos.drain(..1).collect_vec(),
                },
                InputLevel {
                    level_idx: 6,
                    level_type: risingwave_pb::hummock::LevelType::Nonoverlapping,
                    table_infos: sst_infos,
                },
            ],
            existing_table_ids: vec![existing_table_id],
            task_id: 1,
            splits: vec![KeyRange::inf()],
            target_level: 6,
            base_level: 4,
            target_file_size,
            compression_algorithm: 1,
            gc_delete_keys: true,
            pk_prefix_table_watermarks: table_watermarks,
            ..Default::default()
        };
        let (ret, fast_ret) = run_fast_and_normal_runner(
            compact_ctx.clone(),
            task,
            compaction_catalog_agent_ref.clone(),
        )
        .await;
        let mut fast_tables = Vec::with_capacity(fast_ret.len());
        let mut normal_tables = Vec::with_capacity(ret.len());
        let mut stats = StoreLocalStatistic::default();
        for sst_info in &fast_ret {
            fast_tables.push(sstable_store.sstable(sst_info, &mut stats).await.unwrap());
        }

        for sst_info in &ret {
            normal_tables.push(sstable_store.sstable(sst_info, &mut stats).await.unwrap());
        }
        assert!(can_concat(&ret));
        assert!(can_concat(&fast_ret));
        let read_options = Arc::new(SstableIteratorReadOptions::default());

        let mut watermark = ReadTableWatermark {
            direction: WatermarkDirection::Ascending,
            vnode_watermarks: BTreeMap::default(),
        };
        for i in 0..VirtualNode::COUNT_FOR_TEST {
            if i % 2 == 0 {
                watermark
                    .vnode_watermarks
                    .insert(VirtualNode::from_index(i), watermark_key.clone());
            }
        }
        let watermark = BTreeMap::from_iter([(TableId::new(1), watermark)]);
        let compaction_catalog_agent = CompactionCatalogAgent::for_test(vec![1]);

        let combine_iter = {
            let iter = PkPrefixSkipWatermarkIterator::new(
                ConcatIterator::new(ret, sstable_store.clone(), read_options.clone()),
                PkPrefixSkipWatermarkState::new(watermark.clone()),
            );

            NonPkPrefixSkipWatermarkIterator::new(
                iter,
                NonPkPrefixSkipWatermarkState::new(
                    BTreeMap::default(),
                    compaction_catalog_agent.clone(),
                ),
            )
        };

        let mut normal_iter =
            UserIterator::for_test(combine_iter, (Bound::Unbounded, Bound::Unbounded));

        let combine_iter = {
            let iter = PkPrefixSkipWatermarkIterator::new(
                ConcatIterator::new(fast_ret, sstable_store.clone(), read_options.clone()),
                PkPrefixSkipWatermarkState::new(watermark.clone()),
            );

            NonPkPrefixSkipWatermarkIterator::new(
                iter,
                NonPkPrefixSkipWatermarkState::new(
                    BTreeMap::default(),
                    compaction_catalog_agent.clone(),
                ),
            )
        };
        let mut fast_iter =
            UserIterator::for_test(combine_iter, (Bound::Unbounded, Bound::Unbounded));
        normal_iter.rewind().await.unwrap();
        fast_iter.rewind().await.unwrap();
        let mut count = 0;
        while normal_iter.is_valid() {
            assert_eq!(normal_iter.key(), fast_iter.key(), "not equal in {}", count,);
            normal_iter.next().await.unwrap();
            fast_iter.next().await.unwrap();
            count += 1;
        }
    }

    #[tokio::test]
    async fn test_split_and_merge() {
        let (env, hummock_manager_ref, cluster_ctl_ref, worker_id) = setup_compute_env(8080).await;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref.clone(),
            worker_id as _,
        ));

        let table_id_1 = TableId::from(1);
        let table_id_2 = TableId::from(2);

        let storage = get_hummock_storage(
            hummock_meta_client.clone(),
            get_notification_client_for_test(
                env,
                hummock_manager_ref.clone(),
                cluster_ctl_ref,
                worker_id,
            )
            .await,
            &hummock_manager_ref,
            &[table_id_1.table_id(), table_id_2.table_id()],
        )
        .await;

        // basic cg2 -> [1, 2]
        let mut key = BytesMut::default();
        key.put_u16(1);
        key.put_slice(b"key_prefix");
        let key_prefix = key.freeze();

        let compaction_catalog_agent_ref =
            CompactionCatalogAgent::for_test(vec![table_id_1.table_id(), table_id_2.table_id()]);

        let compact_ctx = get_compactor_context(&storage);
        let sstable_object_id_manager = Arc::new(SstableObjectIdManager::new(
            hummock_meta_client.clone(),
            storage
                .storage_opts()
                .clone()
                .sstable_id_remote_fetch_number,
        ));

        let base_epoch = Epoch::now();
        let mut epoch: u64 = base_epoch.0;
        let millisec_interval_epoch: u64 = (1 << 16) * 100;

        let mut local_1 = storage
            .new_local(NewLocalOptions::for_test(table_id_1))
            .await;
        let mut local_2 = storage
            .new_local(NewLocalOptions::for_test(table_id_2))
            .await;

        let val = Bytes::from(b"0"[..].to_vec());

        async fn write_data(
            storage: &HummockStorage,
            local_1: (&mut LocalHummockStorage, bool),
            local_2: (&mut LocalHummockStorage, bool),
            epoch: &mut u64,
            val: Bytes,
            kv_count: u64,
            millisec_interval_epoch: u64,
            key_prefix: Bytes,
            hummock_meta_client: Arc<dyn HummockMetaClient>,
            is_init: &mut bool,
        ) {
            let table_id_set =
                HashSet::from_iter(vec![local_1.0.table_id(), local_2.0.table_id()].into_iter());

            let version = hummock_meta_client.get_current_version().await.unwrap();

            storage.start_epoch(*epoch, table_id_set.clone());
            for i in 0..kv_count {
                if i == 0 && *is_init {
                    let prev_epoch_1 = version.table_committed_epoch(local_1.0.table_id()).unwrap();
                    local_1
                        .0
                        .init_for_test_with_prev_epoch(*epoch, prev_epoch_1)
                        .await
                        .unwrap();
                    let prev_epoch_2 = version.table_committed_epoch(local_2.0.table_id()).unwrap();
                    local_2
                        .0
                        .init_for_test_with_prev_epoch(*epoch, prev_epoch_2)
                        .await
                        .unwrap();

                    *is_init = false;
                }
                let next_epoch = *epoch + millisec_interval_epoch;
                storage.start_epoch(next_epoch, table_id_set.clone());

                let ramdom_key =
                    [key_prefix.as_ref(), &rand::thread_rng().gen::<[u8; 32]>()].concat();

                if local_1.1 {
                    local_1
                        .0
                        .insert(TableKey(Bytes::from(ramdom_key.clone())), val.clone(), None)
                        .unwrap();
                }
                local_1.0.flush().await.unwrap();
                local_1
                    .0
                    .seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());

                if local_2.1 {
                    local_2
                        .0
                        .insert(TableKey(Bytes::from(ramdom_key.clone())), val.clone(), None)
                        .unwrap();
                }
                local_2.0.flush().await.unwrap();
                local_2
                    .0
                    .seal_current_epoch(next_epoch, SealCurrentEpochOptions::for_test());

                let res = storage
                    .seal_and_sync_epoch(*epoch, table_id_set.clone())
                    .await
                    .unwrap();
                hummock_meta_client.commit_epoch(*epoch, res).await.unwrap();
                *epoch += millisec_interval_epoch;
            }
        }

        let mut is_init = true;
        write_data(
            &storage,
            (&mut local_1, true),
            (&mut local_2, true),
            &mut epoch,
            val.clone(),
            1,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;
        epoch += millisec_interval_epoch;

        let parent_group_id = 2;
        let split_table_ids = vec![table_id_2.table_id()];

        async fn compact_once(
            group_id: CompactionGroupId,
            level: usize,
            hummock_manager_ref: HummockManagerRef,
            compact_ctx: CompactorContext,
            compaction_catalog_agent_ref: CompactionCatalogAgentRef,
            sstable_object_id_manager: Arc<SstableObjectIdManager>,
        ) {
            // compact left group
            let manual_compcation_option = ManualCompactionOption {
                level,
                ..Default::default()
            };
            // 2. get compact task
            let compact_task = hummock_manager_ref
                .manual_get_compact_task(group_id, manual_compcation_option)
                .await
                .unwrap();

            if compact_task.is_none() {
                return;
            }

            let mut compact_task = compact_task.unwrap();

            let compaction_filter_flag =
                CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
            compact_task.compaction_filter_mask = compaction_filter_flag.bits();
            compact_task.current_epoch_time = Epoch::now().0;

            // 3. compact
            let (_tx, rx) = tokio::sync::oneshot::channel();
            let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
                compact_ctx,
                compact_task.clone(),
                rx,
                Box::new(sstable_object_id_manager.clone()),
                compaction_catalog_agent_ref.clone(),
            )
            .await;

            hummock_manager_ref
                .report_compact_task(
                    result_task.task_id,
                    result_task.task_status,
                    result_task.sorted_output_ssts,
                    Some(to_prost_table_stats_map(task_stats)),
                    object_timestamps,
                )
                .await
                .unwrap();
        }

        let new_cg_id = hummock_manager_ref
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &split_table_ids,
                None,
            )
            .await
            .unwrap()
            .0;

        write_data(
            &storage,
            (&mut local_1, true),
            (&mut local_2, true),
            &mut epoch,
            val.clone(),
            100,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;
        epoch += millisec_interval_epoch;

        compact_once(
            parent_group_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        compact_once(
            new_cg_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        let created_tables = HashSet::from_iter(vec![table_id_1.table_id(), table_id_2.table_id()]);

        // try merge
        hummock_manager_ref
            .merge_compaction_group_for_test(parent_group_id, new_cg_id, created_tables.clone())
            .await
            .unwrap();

        let new_cg_id = hummock_manager_ref
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &split_table_ids,
                None,
            )
            .await
            .unwrap()
            .0;

        compact_once(
            parent_group_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        compact_once(
            new_cg_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        // write left
        write_data(
            &storage,
            (&mut local_1, true),
            (&mut local_2, false),
            &mut epoch,
            val.clone(),
            16,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;

        epoch += millisec_interval_epoch;

        // try merge
        hummock_manager_ref
            .merge_compaction_group_for_test(parent_group_id, new_cg_id, created_tables.clone())
            .await
            .unwrap();

        // compact
        compact_once(
            parent_group_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        // try split
        let new_cg_id = hummock_manager_ref
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &split_table_ids,
                None,
            )
            .await
            .unwrap()
            .0;

        // write right
        write_data(
            &storage,
            (&mut local_1, false),
            (&mut local_2, true),
            &mut epoch,
            val.clone(),
            16,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;

        epoch += millisec_interval_epoch;

        hummock_manager_ref
            .merge_compaction_group_for_test(parent_group_id, new_cg_id, created_tables.clone())
            .await
            .unwrap();

        // write left and right

        write_data(
            &storage,
            (&mut local_1, true),
            (&mut local_2, true),
            &mut epoch,
            val.clone(),
            1,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;

        epoch += millisec_interval_epoch;

        compact_once(
            parent_group_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        compact_once(
            new_cg_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        async fn compact_all(
            group_id: CompactionGroupId,
            level: usize,
            hummock_manager_ref: HummockManagerRef,
            compact_ctx: CompactorContext,
            compaction_catalog_agent_ref: CompactionCatalogAgentRef,
            sstable_object_id_manager: Arc<SstableObjectIdManager>,
        ) {
            loop {
                let manual_compcation_option = ManualCompactionOption {
                    level,
                    ..Default::default()
                };
                let compact_task = hummock_manager_ref
                    .manual_get_compact_task(group_id, manual_compcation_option)
                    .await
                    .unwrap();

                if compact_task.is_none() {
                    break;
                }

                let mut compact_task = compact_task.unwrap();
                let compaction_filter_flag =
                    CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL;
                compact_task.compaction_filter_mask = compaction_filter_flag.bits();
                compact_task.current_epoch_time = Epoch::now().0;

                // 3. compact
                let (_tx, rx) = tokio::sync::oneshot::channel();
                let ((result_task, task_stats, object_timestamps), _) = compact_with_agent(
                    compact_ctx.clone(),
                    compact_task.clone(),
                    rx,
                    Box::new(sstable_object_id_manager.clone()),
                    compaction_catalog_agent_ref.clone(),
                )
                .await;

                hummock_manager_ref
                    .report_compact_task(
                        result_task.task_id,
                        result_task.task_status,
                        result_task.sorted_output_ssts,
                        Some(to_prost_table_stats_map(task_stats)),
                        object_timestamps,
                    )
                    .await
                    .unwrap();
            }
        }

        // try split
        let new_cg_id = hummock_manager_ref
            .move_state_tables_to_dedicated_compaction_group(
                parent_group_id,
                &split_table_ids,
                None,
            )
            .await
            .unwrap()
            .0;

        // write left and write
        write_data(
            &storage,
            (&mut local_1, true),
            (&mut local_2, true),
            &mut epoch,
            val.clone(),
            200,
            millisec_interval_epoch,
            key_prefix.clone(),
            hummock_meta_client.clone(),
            &mut is_init,
        )
        .await;

        compact_all(
            parent_group_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        compact_all(
            new_cg_id,
            0,
            hummock_manager_ref.clone(),
            compact_ctx.clone(),
            compaction_catalog_agent_ref.clone(),
            sstable_object_id_manager.clone(),
        )
        .await;

        // try merge
        hummock_manager_ref
            .merge_compaction_group_for_test(parent_group_id, new_cg_id, created_tables.clone())
            .await
            .unwrap();
    }
}
