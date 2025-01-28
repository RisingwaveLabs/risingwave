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

use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::change_log::ChangeLogDelta;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_watermark::TableWatermarks;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{CompactionGroupId, FrontendHummockVersionDelta, HummockVersionId};
use risingwave_pb::hummock::{
    CompatibilityVersion, GroupConstruct, HummockVersionDeltas, HummockVersionStats,
    StateTableInfoDelta,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};

use crate::hummock::model::CompactionGroup;
use crate::manager::NotificationManager;
use crate::model::{
    InMemValTransaction, MetadataModelResult, Transactional, ValTransaction, VarTransaction,
};
use crate::rpc::metrics::MetaMetrics;

fn trigger_delta_log_stats(metrics: &MetaMetrics, total_number: usize) {
    metrics.delta_log_count.set(total_number as _);
}

fn trigger_version_stat(metrics: &MetaMetrics, current_version: &HummockVersion) {
    metrics
        .version_size
        .set(current_version.estimated_encode_len() as i64);
    metrics
        .current_version_id
        .set(current_version.id.to_u64() as i64);
}

pub(super) struct HummockVersionTransaction<'a> {
    orig_version: &'a mut HummockVersion,
    orig_deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
    notification_manager: &'a NotificationManager,
    meta_metrics: &'a MetaMetrics,

    pre_applied_version: Option<(HummockVersion, Vec<HummockVersionDelta>)>,
    disable_apply_to_txn: bool,
}

impl<'a> HummockVersionTransaction<'a> {
    pub(super) fn new(
        version: &'a mut HummockVersion,
        deltas: &'a mut BTreeMap<HummockVersionId, HummockVersionDelta>,
        notification_manager: &'a NotificationManager,
        meta_metrics: &'a MetaMetrics,
    ) -> Self {
        Self {
            orig_version: version,
            orig_deltas: deltas,
            pre_applied_version: None,
            disable_apply_to_txn: false,
            notification_manager,
            meta_metrics,
        }
    }

    pub(super) fn disable_apply_to_txn(&mut self) {
        assert!(
            self.pre_applied_version.is_none(),
            "should only call disable at the beginning of txn"
        );
        self.disable_apply_to_txn = true;
    }

    pub(super) fn latest_version(&self) -> &HummockVersion {
        if let Some((version, _)) = &self.pre_applied_version {
            version
        } else {
            self.orig_version
        }
    }

    pub(super) fn new_delta<'b>(&'b mut self) -> SingleDeltaTransaction<'a, 'b> {
        let delta = self.latest_version().version_delta_after();
        SingleDeltaTransaction {
            version_txn: self,
            delta: Some(delta),
        }
    }

    fn pre_apply(&mut self, delta: HummockVersionDelta) {
        let (version, deltas) = self
            .pre_applied_version
            .get_or_insert_with(|| (self.orig_version.clone(), Vec::with_capacity(1)));
        version.apply_version_delta(&delta);
        deltas.push(delta);
    }

    /// Returns a duplicate delta, used by time travel.
    pub(super) fn pre_commit_epoch(
        &mut self,
        tables_to_commit: &HashMap<TableId, u64>,
        new_compaction_groups: Vec<CompactionGroup>,
        group_id_to_sub_levels: BTreeMap<CompactionGroupId, Vec<Vec<SstableInfo>>>,
        new_table_ids: &HashMap<TableId, CompactionGroupId>,
        new_table_watermarks: HashMap<TableId, TableWatermarks>,
        change_log_delta: HashMap<TableId, ChangeLogDelta>,
    ) -> HummockVersionDelta {
        let mut new_version_delta = self.new_delta();
        new_version_delta.new_table_watermarks = new_table_watermarks;
        new_version_delta.change_log_delta = change_log_delta;

        for compaction_group in &new_compaction_groups {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group.group_id())
                .or_default()
                .group_deltas;

            #[expect(deprecated)]
            group_deltas.push(GroupDelta::GroupConstruct(Box::new(GroupConstruct {
                group_config: Some(compaction_group.compaction_config().as_ref().clone()),
                group_id: compaction_group.group_id(),
                parent_group_id: StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId,
                new_sst_start_id: 0, // No need to set it when `NewCompactionGroup`
                table_ids: vec![],
                version: CompatibilityVersion::SplitGroupByTableId as i32,
                split_key: None,
            })));
        }

        // Append SSTs to a new version.
        for (compaction_group_id, sub_levels) in group_id_to_sub_levels {
            let group_deltas = &mut new_version_delta
                .group_deltas
                .entry(compaction_group_id)
                .or_default()
                .group_deltas;

            for sub_level in sub_levels {
                group_deltas.push(GroupDelta::NewL0SubLevel(sub_level));
            }
        }

        // update state table info
        new_version_delta.with_latest_version(|version, delta| {
            for (table_id, cg_id) in new_table_ids {
                assert!(
                    !version.state_table_info.info().contains_key(table_id),
                    "newly added table exists previously: {:?}",
                    table_id
                );
                let committed_epoch = *tables_to_commit.get(table_id).expect("newly added table must exist in tables_to_commit");
                delta.state_table_info_delta.insert(
                    *table_id,
                    StateTableInfoDelta {
                        committed_epoch,
                        compaction_group_id: *cg_id,
                    },
                );
            }

            for (table_id, committed_epoch) in tables_to_commit {
                if new_table_ids.contains_key(table_id) {
                    continue;
                }
                let info = version.state_table_info.info().get(table_id).unwrap_or_else(|| {
                    panic!("tables_to_commit {:?} contains table_id {} that is not newly added but not exists previously", tables_to_commit, table_id);
                });
                assert!(delta
                    .state_table_info_delta
                    .insert(
                        *table_id,
                        StateTableInfoDelta {
                            committed_epoch: *committed_epoch,
                            compaction_group_id: info.compaction_group_id,
                        }
                    )
                    .is_none());
            }
        });

        let time_travel_delta = (*new_version_delta).clone();
        new_version_delta.pre_apply();
        time_travel_delta
    }
}

impl InMemValTransaction for HummockVersionTransaction<'_> {
    fn commit(self) {
        if let Some((version, deltas)) = self.pre_applied_version {
            *self.orig_version = version;
            if !self.disable_apply_to_txn {
                let pb_deltas = deltas.iter().map(|delta| delta.to_protobuf()).collect();
                self.notification_manager.notify_hummock_without_version(
                    Operation::Add,
                    Info::HummockVersionDeltas(risingwave_pb::hummock::HummockVersionDeltas {
                        version_deltas: pb_deltas,
                    }),
                );
                self.notification_manager.notify_frontend_without_version(
                    Operation::Update,
                    Info::HummockVersionDeltas(HummockVersionDeltas {
                        version_deltas: deltas
                            .iter()
                            .map(|delta| {
                                FrontendHummockVersionDelta::from_delta(delta).to_protobuf()
                            })
                            .collect(),
                    }),
                );
            }
            for delta in deltas {
                assert!(self.orig_deltas.insert(delta.id, delta.clone()).is_none());
            }

            trigger_delta_log_stats(self.meta_metrics, self.orig_deltas.len());
            trigger_version_stat(self.meta_metrics, self.orig_version);
        }
    }
}

impl<'a, TXN> ValTransaction<TXN> for HummockVersionTransaction<'a>
where
    HummockVersionDelta: Transactional<TXN>,
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        if self.disable_apply_to_txn {
            return Ok(());
        }
        for delta in self
            .pre_applied_version
            .iter()
            .flat_map(|(_, deltas)| deltas.iter())
        {
            delta.upsert_in_transaction(txn).await?;
        }
        Ok(())
    }
}

pub(super) struct SingleDeltaTransaction<'a, 'b> {
    version_txn: &'b mut HummockVersionTransaction<'a>,
    delta: Option<HummockVersionDelta>,
}

impl SingleDeltaTransaction<'_, '_> {
    pub(super) fn latest_version(&self) -> &HummockVersion {
        self.version_txn.latest_version()
    }

    pub(super) fn pre_apply(mut self) {
        self.version_txn.pre_apply(self.delta.take().unwrap());
    }

    pub(super) fn with_latest_version(
        &mut self,
        f: impl FnOnce(&HummockVersion, &mut HummockVersionDelta),
    ) {
        f(
            self.version_txn.latest_version(),
            self.delta.as_mut().expect("should exist"),
        )
    }
}

impl Deref for SingleDeltaTransaction<'_, '_> {
    type Target = HummockVersionDelta;

    fn deref(&self) -> &Self::Target {
        self.delta.as_ref().expect("should exist")
    }
}

impl DerefMut for SingleDeltaTransaction<'_, '_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.delta.as_mut().expect("should exist")
    }
}

impl Drop for SingleDeltaTransaction<'_, '_> {
    fn drop(&mut self) {
        if let Some(delta) = self.delta.take() {
            self.version_txn.pre_apply(delta);
        }
    }
}

pub(super) struct HummockVersionStatsTransaction<'a> {
    stats: VarTransaction<'a, HummockVersionStats>,
    notification_manager: &'a NotificationManager,
}

impl<'a> HummockVersionStatsTransaction<'a> {
    pub(super) fn new(
        stats: &'a mut HummockVersionStats,
        notification_manager: &'a NotificationManager,
    ) -> Self {
        Self {
            stats: VarTransaction::new(stats),
            notification_manager,
        }
    }
}

impl InMemValTransaction for HummockVersionStatsTransaction<'_> {
    fn commit(self) {
        if self.stats.has_new_value() {
            let stats = self.stats.clone();
            self.stats.commit();
            self.notification_manager
                .notify_frontend_without_version(Operation::Update, Info::HummockStats(stats));
        }
    }
}

impl<'a, TXN> ValTransaction<TXN> for HummockVersionStatsTransaction<'a>
where
    HummockVersionStats: Transactional<TXN>,
{
    async fn apply_to_txn(&self, txn: &mut TXN) -> MetadataModelResult<()> {
        self.stats.apply_to_txn(txn).await
    }
}

impl Deref for HummockVersionStatsTransaction<'_> {
    type Target = HummockVersionStats;

    fn deref(&self) -> &Self::Target {
        self.stats.deref()
    }
}

impl DerefMut for HummockVersionStatsTransaction<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.stats.deref_mut()
    }
}
