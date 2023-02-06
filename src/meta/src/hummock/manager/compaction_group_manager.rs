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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::DerefMut;

use function_name::named;
use itertools::Itertools;
use risingwave_common::catalog::TableOption;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::{
    get_compaction_group_ids, get_member_table_ids, try_get_compaction_group_id_by_table_id,
    HummockVersionExt, HummockVersionUpdateExt,
};
use risingwave_hummock_sdk::compaction_group::{StateTableId, StaticCompactionGroupId};
use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::{
    CompactionConfig, GroupConstruct, GroupDelta, GroupMetaChange, HummockVersionDelta,
    TableOption as ProstTableOption,
};
use tokio::sync::{OnceCell, RwLock};

use super::versioning::Versioning;
use super::write_lock;
use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
use crate::hummock::compaction_group::CompactionGroup;
use crate::hummock::error::{Error, Result};
use crate::hummock::manager::{read_lock, HummockManager};
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{
    BTreeMapEntryTransaction, BTreeMapTransaction, MetadataModel, TableFragments, ValTransaction,
};
use crate::storage::{MetaStore, Transaction};

impl<S: MetaStore> HummockManager<S> {
    pub(super) async fn build_compaction_group_manager(
        env: &MetaSrvEnv<S>,
    ) -> Result<RwLock<CompactionGroupManager>> {
        let config = CompactionConfigBuilder::new().build();
        Self::build_compaction_group_manager_with_config(env, config).await
    }

    pub(super) async fn build_compaction_group_manager_with_config(
        env: &MetaSrvEnv<S>,
        config: CompactionConfig,
    ) -> Result<RwLock<CompactionGroupManager>> {
        let compaction_group_manager = RwLock::new(CompactionGroupManager {
            compaction_groups: BTreeMap::new(),
            default_config: config,
        });
        compaction_group_manager
            .write()
            .await
            .init(env.meta_store())
            .await?;
        Ok(compaction_group_manager)
    }

    /// Gets compaction config for `compaction_group_id` if exists, or returns default.
    #[named]
    pub async fn get_compaction_config(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> CompactionConfig {
        self.compaction_group_manager
            .read()
            .await
            .get_compaction_config(compaction_group_id)
    }

    #[named]
    pub async fn compaction_group_ids(&self) -> Vec<CompactionGroupId> {
        get_compaction_group_ids(&read_lock!(self, versioning).await.current_version)
    }

    /// Registers `table_fragments` to compaction groups.
    pub async fn register_table_fragments(
        &self,
        table_fragments: &TableFragments,
        table_properties: &HashMap<String, String>,
    ) -> Result<Vec<StateTableId>> {
        let is_independent_compaction_group = table_properties
            .get("independent_compaction_group")
            .map(|s| s == "1")
            == Some(true);
        let table_option = TableOption::build_table_option(table_properties);
        let mut pairs = vec![];
        // materialized_view
        pairs.push((
            table_fragments.table_id().table_id,
            if is_independent_compaction_group {
                CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
            } else {
                CompactionGroupId::from(StaticCompactionGroupId::MaterializedView)
            },
            table_option,
        ));
        // internal states
        for table_id in table_fragments.internal_table_ids() {
            assert_ne!(table_id, table_fragments.table_id().table_id);
            pairs.push((
                table_id,
                if is_independent_compaction_group {
                    CompactionGroupId::from(StaticCompactionGroupId::NewCompactionGroup)
                } else {
                    CompactionGroupId::from(StaticCompactionGroupId::StateDefault)
                },
                table_option,
            ));
        }
        self.register_table_ids(&mut pairs).await?;
        Ok(pairs.iter().map(|(table_id, ..)| *table_id).collect_vec())
    }

    /// Unregisters `table_fragments` from compaction groups
    pub async fn unregister_table_fragments_vec(
        &self,
        table_fragments: &[TableFragments],
    ) -> Result<()> {
        self.unregister_table_ids(
            &table_fragments
                .iter()
                .flat_map(|t| t.all_table_ids())
                .collect_vec(),
        )
        .await
    }

    /// Unregisters stale members and groups
    #[named]
    pub async fn purge(&self, table_fragments_list: &[TableFragments]) -> Result<()> {
        let valid_ids = table_fragments_list
            .iter()
            .flat_map(|table_fragments| table_fragments.all_table_ids())
            .collect_vec();
        let registered_members =
            get_member_table_ids(&read_lock!(self, versioning).await.current_version);
        let to_unregister = registered_members
            .into_iter()
            .filter(|table_id| !valid_ids.contains(table_id))
            .collect_vec();
        // As we have released versioning lock, the version that `to_unregister` is calculated from
        // may not be the same as the one used in unregister_table_ids. It is OK.
        self.unregister_table_ids(&to_unregister).await?;
        Ok(())
    }

    /// Prefer using [`register_table_fragments`].
    /// Use [`register_table_ids`] only when [`TableFragments`] is unavailable.
    #[named]
    pub async fn register_table_ids(
        &self,
        pairs: &[(StateTableId, CompactionGroupId, TableOption)],
    ) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;

        for (table_id, new_compaction_group_id, _) in pairs.iter() {
            if let Some(old_compaction_group_id) =
                try_get_compaction_group_id_by_table_id(&current_version, *table_id)
            {
                if old_compaction_group_id != *new_compaction_group_id {
                    return Err(Error::InvalidCompactionGroupMember(*table_id));
                }
            }
        }
        // All NewCompactionGroup pairs are mapped to one new compaction group.
        let new_compaction_group_id: OnceCell<CompactionGroupId> = OnceCell::new();
        let mut new_version_delta = BTreeMapEntryTransaction::new_insert(
            &mut versioning.hummock_version_deltas,
            current_version.id + 1,
            HummockVersionDelta {
                prev_id: current_version.id,
                safe_epoch: current_version.safe_epoch,
                trivial_move: false,
                ..Default::default()
            },
        );

        for (table_id, raw_group_id, table_option) in pairs.iter() {
            let mut group_id = *raw_group_id;
            if group_id == StaticCompactionGroupId::NewCompactionGroup as u64 {
                group_id = *new_compaction_group_id
                    .get_or_try_init(|| async {
                        self.env
                            .id_gen_manager()
                            .generate::<{ IdCategory::CompactionGroup }>()
                            .await
                            .map(|new_group_id| {
                                let group_deltas = &mut new_version_delta
                                    .group_deltas
                                    .entry(new_group_id)
                                    .or_default()
                                    .group_deltas;
                                group_deltas.push(GroupDelta {
                                    delta_type: Some(DeltaType::GroupConstruct(GroupConstruct {
                                        group_config: Some(CompactionConfigBuilder::new().build()),
                                        group_id: new_group_id,
                                        ..Default::default()
                                    })),
                                });
                                new_group_id
                            })
                    })
                    .await?;
            }
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(group_id)
                .or_default()
                .group_deltas;
            group_deltas.push(GroupDelta {
                delta_type: Some(DeltaType::GroupMetaChange(GroupMetaChange {
                    table_ids_add: vec![*table_id],
                    table_id_to_options_add: HashMap::from([(
                        *table_id,
                        ProstTableOption::from(table_option),
                    )]),
                    ..Default::default()
                })),
            });
        }

        let mut trx = Transaction::default();
        new_version_delta.apply_to_txn(&mut trx)?;
        self.env.meta_store().txn(trx).await?;
        let sst_split_info = versioning
            .current_version
            .apply_version_delta(&new_version_delta);
        assert!(sst_split_info.is_empty());
        new_version_delta.commit();

        self.notify_last_version_delta(versioning);

        Ok(())
    }

    /// Prefer using [`unregister_table_fragments_vec`].
    /// Only Use [`unregister_table_ids`] only when [`TableFragments`] is unavailable.
    #[named]
    pub async fn unregister_table_ids(&self, table_ids: &[StateTableId]) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let current_version = &versioning.current_version;

        let mut new_version_delta = BTreeMapEntryTransaction::new_insert(
            &mut versioning.hummock_version_deltas,
            current_version.id + 1,
            HummockVersionDelta {
                prev_id: current_version.id,
                safe_epoch: current_version.safe_epoch,
                trivial_move: false,
                ..Default::default()
            },
        );

        let mut modified_groups: HashMap<CompactionGroupId, /* #member table */ u64> =
            HashMap::new();
        for table_id in table_ids.iter().unique() {
            let group_id =
                match try_get_compaction_group_id_by_table_id(&current_version, *table_id) {
                    Some(group_id) => group_id,
                    None => continue,
                };
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(group_id)
                .or_default()
                .group_deltas;
            group_deltas.push(GroupDelta {
                delta_type: Some(DeltaType::GroupMetaChange(GroupMetaChange {
                    table_ids_remove: vec![*table_id],
                    ..Default::default()
                })),
            });
            modified_groups
                .entry(group_id)
                .and_modify(|count| *count -= 1)
                .or_insert(
                    current_version
                        .get_compaction_group_levels(group_id)
                        .member_table_ids
                        .len() as u64
                        - 1,
                );
        }

        // TODO remove empty group and GC SSTs

        Ok(())
    }

    #[named]
    pub async fn all_table_ids(&self) -> HashSet<StateTableId> {
        get_member_table_ids(&read_lock!(self, versioning).await.current_version)
            .into_iter()
            .collect()
    }

    pub async fn update_compaction_config(
        &self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
    ) -> Result<()> {
        self.compaction_group_manager
            .write()
            .await
            .update_compaction_config(
                compaction_group_ids,
                config_to_update,
                self.env.meta_store(),
            )
            .await
    }
}

#[derive(Default)]
pub(super) struct CompactionGroupManager {
    compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup>,
    default_config: CompactionConfig,
}

impl CompactionGroupManager {
    async fn init<S: MetaStore>(&mut self, meta_store: &S) -> Result<()> {
        let loaded_compaction_groups: BTreeMap<CompactionGroupId, CompactionGroup> =
            CompactionGroup::list(meta_store)
                .await?
                .into_iter()
                .map(|cg| (cg.group_id(), cg))
                .collect();
        if !loaded_compaction_groups.is_empty() {
            self.compaction_groups = loaded_compaction_groups;
        }
        Ok(())
    }

    fn get_compaction_config(&self, compaction_group_id: CompactionGroupId) -> CompactionConfig {
        self.compaction_groups
            .get(&compaction_group_id)
            .map(|group| group.compaction_config.clone())
            .unwrap_or_else(|| self.default_config.clone())
    }

    async fn update_compaction_config<S: MetaStore>(
        &mut self,
        compaction_group_ids: &[CompactionGroupId],
        config_to_update: &[MutableConfig],
        meta_store: &S,
    ) -> Result<()> {
        let mut compaction_groups = BTreeMapTransaction::new(&mut self.compaction_groups);
        for compaction_group_id in compaction_group_ids {
            if !compaction_groups.contains_key(compaction_group_id) {
                compaction_groups.insert(
                    *compaction_group_id,
                    CompactionGroup::new(*compaction_group_id, self.default_config.clone()),
                );
            }
            let mut group = compaction_groups.get_mut(*compaction_group_id).unwrap();
            let config = &mut group.compaction_config;
            update_compaction_config(config, config_to_update);
        }
        let mut trx = Transaction::default();
        compaction_groups.apply_to_txn(&mut trx)?;
        meta_store.txn(trx).await?;
        compaction_groups.commit();
        Ok(())
    }
}

fn update_compaction_config(target: &mut CompactionConfig, items: &[MutableConfig]) {
    for item in items {
        match item {
            MutableConfig::MaxBytesForLevelBase(c) => {
                target.max_bytes_for_level_base = *c;
            }
            MutableConfig::MaxBytesForLevelMultiplier(c) => {
                target.max_bytes_for_level_multiplier = *c;
            }
            MutableConfig::MaxCompactionBytes(c) => {
                target.max_compaction_bytes = *c;
            }
            MutableConfig::SubLevelMaxCompactionBytes(c) => {
                target.sub_level_max_compaction_bytes = *c;
            }
            MutableConfig::Level0TierCompactFileNumber(c) => {
                target.level0_tier_compact_file_number = *c;
            }
            MutableConfig::TargetFileSizeBase(c) => {
                target.target_file_size_base = *c;
            }
            MutableConfig::CompactionFilterMask(c) => {
                target.compaction_filter_mask = *c;
            }
            MutableConfig::MaxSubCompaction(c) => {
                target.max_sub_compaction = *c;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::ops::Deref;

    use risingwave_common::catalog::{TableId, TableOption};
    use risingwave_common::constants::hummock::PROPERTIES_RETENTION_SECOND_KEY;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::meta::table_fragments::Fragment;

    use crate::hummock::manager::compaction_group_manager::CompactionGroupManager;
    use crate::hummock::manager::versioning::Versioning;
    use crate::hummock::test_utils::setup_compute_env;
    use crate::hummock::HummockManager;
    use crate::model::TableFragments;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_inner() {
        let (env, hummock_manager_ref, ..) = setup_compute_env(8080).await;
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();

        let registered_number = |inner: &CompactionGroupManager<MemStore>| {
            inner
                .compaction_groups
                .values()
                .map(|cg| cg.member_table_ids.len())
                .sum::<usize>()
        };

        let table_option_number = |inner: &CompactionGroupManager<MemStore>| {
            inner
                .compaction_groups
                .values()
                .map(|cg| cg.table_id_to_options().len())
                .sum::<usize>()
        };

        assert!(inner.read().await.index.is_empty());
        assert_eq!(registered_number(inner.read().await.deref()), 0);

        let table_properties = HashMap::from([(
            String::from(PROPERTIES_RETENTION_SECOND_KEY),
            String::from("300"),
        )]);
        let table_option = TableOption::build_table_option(&table_properties);

        // Test register
        inner
            .write()
            .await
            .register(
                &hummock_manager_ref,
                &mut Versioning::default(),
                &mut [(
                    1u32,
                    StaticCompactionGroupId::StateDefault.into(),
                    table_option,
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        inner
            .write()
            .await
            .register(
                &hummock_manager_ref,
                &mut Versioning::default(),
                &mut [(
                    2u32,
                    StaticCompactionGroupId::MaterializedView.into(),
                    table_option,
                )],
                env.meta_store(),
            )
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);

        // Test init
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 2);
        assert_eq!(registered_number(inner.read().await.deref()), 2);
        assert_eq!(table_option_number(inner.read().await.deref()), 2);

        // Test unregister
        inner
            .write()
            .await
            .unregister(None, &[2u32], env.meta_store())
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);
        assert_eq!(table_option_number(inner.read().await.deref()), 1);

        // Test init
        let inner = HummockManager::build_compaction_group_manager(&env)
            .await
            .unwrap();
        assert_eq!(inner.read().await.index.len(), 1);
        assert_eq!(registered_number(inner.read().await.deref()), 1);
        assert_eq!(table_option_number(inner.read().await.deref()), 1);

        // Test table_option_by_table_id
        {
            let table_option = inner
                .read()
                .await
                .table_option_by_table_id(StaticCompactionGroupId::StateDefault.into(), 1u32)
                .unwrap();
            assert_eq!(300, table_option.retention_seconds.unwrap());
        }

        {
            // unregistered table_id
            let table_option_default = inner
                .read()
                .await
                .table_option_by_table_id(StaticCompactionGroupId::StateDefault.into(), 2u32);
            assert!(table_option_default.is_ok());
            assert_eq!(None, table_option_default.unwrap().retention_seconds);
        }
    }

    #[tokio::test]
    async fn test_manager() {
        let (_, compaction_group_manager, ..) = setup_compute_env(8080).await;
        let table_fragment_1 = TableFragments::for_test(
            TableId::new(10),
            BTreeMap::from([(
                1,
                Fragment {
                    fragment_id: 1,
                    state_table_ids: vec![10, 11, 12, 13],
                    ..Default::default()
                },
            )]),
        );
        let table_fragment_2 = TableFragments::for_test(
            TableId::new(20),
            BTreeMap::from([(
                2,
                Fragment {
                    fragment_id: 2,
                    state_table_ids: vec![20, 21, 22, 23],
                    ..Default::default()
                },
            )]),
        );

        // Test register_table_fragments
        let registered_number = || async {
            compaction_group_manager
                .compaction_groups()
                .await
                .iter()
                .map(|cg| cg.member_table_ids.len())
                .sum::<usize>()
        };
        let group_number = || async { compaction_group_manager.compaction_groups().await.len() };
        assert_eq!(registered_number().await, 0);
        let mut table_properties = HashMap::from([(
            String::from(PROPERTIES_RETENTION_SECOND_KEY),
            String::from("300"),
        )]);

        compaction_group_manager
            .register_table_fragments(&table_fragment_1, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager
            .register_table_fragments(&table_fragment_2, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 8);

        // Test unregister_table_fragments
        compaction_group_manager
            .unregister_table_fragments_vec(&[table_fragment_1.clone()])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);

        // Test purge_stale_members: table fragments
        compaction_group_manager
            .purge(&[table_fragment_2])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        compaction_group_manager.purge(&[]).await.unwrap();
        assert_eq!(registered_number().await, 0);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `register_table_fragments`
        assert_eq!(group_number().await, 2);
        table_properties.insert(
            String::from("independent_compaction_group"),
            String::from("1"),
        );
        compaction_group_manager
            .register_table_fragments(&table_fragment_1, &table_properties)
            .await
            .unwrap();
        assert_eq!(registered_number().await, 4);
        assert_eq!(group_number().await, 3);

        // Test `StaticCompactionGroupId::NewCompactionGroup` in `unregister_table_fragments`
        compaction_group_manager
            .unregister_table_fragments_vec(&[table_fragment_1])
            .await
            .unwrap();
        assert_eq!(registered_number().await, 0);
        assert_eq!(group_number().await, 2);
    }
}
