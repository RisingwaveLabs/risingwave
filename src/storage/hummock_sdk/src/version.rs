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

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::mem::{replace, size_of};
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_pb::hummock::group_delta::{DeltaType, PbDeltaType};
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use risingwave_pb::hummock::{
    CompactionConfig, PbGroupConstruct, PbGroupDelta, PbGroupDestroy, PbGroupMerge,
    PbHummockVersion, PbHummockVersionDelta, PbIntraLevelDelta, PbNewL0SubLevel, PbSstableInfo,
    PbStateTableInfo, StateTableInfo, StateTableInfoDelta,
};
use tracing::warn;

use crate::change_log::{ChangeLogDeltaCommon, TableChangeLogCommon};
use crate::compaction_group::hummock_version_ext::build_initial_compaction_group_levels;
use crate::compaction_group::StaticCompactionGroupId;
use crate::level::LevelsCommon;
use crate::sstable_info::SstableInfo;
use crate::table_watermark::TableWatermarks;
use crate::{
    CompactionGroupId, HummockEpoch, HummockSstableId, HummockSstableObjectId, HummockVersionId,
    FIRST_VERSION_ID,
};

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersionStateTableInfo {
    state_table_info: HashMap<TableId, PbStateTableInfo>,

    // in memory index
    compaction_group_member_tables: HashMap<CompactionGroupId, BTreeSet<TableId>>,
}

impl HummockVersionStateTableInfo {
    pub fn empty() -> Self {
        Self {
            state_table_info: HashMap::new(),
            compaction_group_member_tables: HashMap::new(),
        }
    }

    fn build_compaction_group_member_tables(
        state_table_info: &HashMap<TableId, PbStateTableInfo>,
    ) -> HashMap<CompactionGroupId, BTreeSet<TableId>> {
        let mut ret: HashMap<_, BTreeSet<_>> = HashMap::new();
        for (table_id, info) in state_table_info {
            assert!(ret
                .entry(info.compaction_group_id)
                .or_default()
                .insert(*table_id));
        }
        ret
    }

    pub fn build_table_compaction_group_id(&self) -> HashMap<TableId, CompactionGroupId> {
        self.state_table_info
            .iter()
            .map(|(table_id, info)| (*table_id, info.compaction_group_id))
            .collect()
    }

    pub fn from_protobuf(state_table_info: &HashMap<u32, PbStateTableInfo>) -> Self {
        let state_table_info = state_table_info
            .iter()
            .map(|(table_id, info)| (TableId::new(*table_id), *info))
            .collect();
        let compaction_group_member_tables =
            Self::build_compaction_group_member_tables(&state_table_info);
        Self {
            state_table_info,
            compaction_group_member_tables,
        }
    }

    pub fn to_protobuf(&self) -> HashMap<u32, PbStateTableInfo> {
        self.state_table_info
            .iter()
            .map(|(table_id, info)| (table_id.table_id, *info))
            .collect()
    }

    pub fn apply_delta(
        &mut self,
        delta: &HashMap<TableId, StateTableInfoDelta>,
        removed_table_id: &HashSet<TableId>,
    ) -> (HashMap<TableId, Option<StateTableInfo>>, bool) {
        let mut changed_table = HashMap::new();
        let mut has_bumped_committed_epoch = false;
        fn remove_table_from_compaction_group(
            compaction_group_member_tables: &mut HashMap<CompactionGroupId, BTreeSet<TableId>>,
            compaction_group_id: CompactionGroupId,
            table_id: TableId,
        ) {
            let member_tables = compaction_group_member_tables
                .get_mut(&compaction_group_id)
                .expect("should exist");
            assert!(member_tables.remove(&table_id));
            if member_tables.is_empty() {
                assert!(compaction_group_member_tables
                    .remove(&compaction_group_id)
                    .is_some());
            }
        }
        for table_id in removed_table_id {
            if let Some(prev_info) = self.state_table_info.remove(table_id) {
                remove_table_from_compaction_group(
                    &mut self.compaction_group_member_tables,
                    prev_info.compaction_group_id,
                    *table_id,
                );
                assert!(changed_table.insert(*table_id, Some(prev_info)).is_none());
            } else {
                warn!(
                    table_id = table_id.table_id,
                    "table to remove does not exist"
                );
            }
        }
        for (table_id, delta) in delta {
            if removed_table_id.contains(table_id) {
                continue;
            }
            let new_info = StateTableInfo {
                committed_epoch: delta.committed_epoch,
                compaction_group_id: delta.compaction_group_id,
            };
            match self.state_table_info.entry(*table_id) {
                Entry::Occupied(mut entry) => {
                    let prev_info = entry.get_mut();
                    assert!(
                        new_info.committed_epoch >= prev_info.committed_epoch,
                        "state table info regress. table id: {}, prev_info: {:?}, new_info: {:?}",
                        table_id.table_id,
                        prev_info,
                        new_info
                    );
                    if new_info.committed_epoch > prev_info.committed_epoch {
                        has_bumped_committed_epoch = true;
                    }
                    if prev_info.compaction_group_id != new_info.compaction_group_id {
                        // table moved to another compaction group
                        remove_table_from_compaction_group(
                            &mut self.compaction_group_member_tables,
                            prev_info.compaction_group_id,
                            *table_id,
                        );
                        assert!(self
                            .compaction_group_member_tables
                            .entry(new_info.compaction_group_id)
                            .or_default()
                            .insert(*table_id));
                    }
                    let prev_info = replace(prev_info, new_info);
                    changed_table.insert(*table_id, Some(prev_info));
                }
                Entry::Vacant(entry) => {
                    assert!(self
                        .compaction_group_member_tables
                        .entry(new_info.compaction_group_id)
                        .or_default()
                        .insert(*table_id));
                    has_bumped_committed_epoch = true;
                    entry.insert(new_info);
                    changed_table.insert(*table_id, None);
                }
            }
        }
        debug_assert_eq!(
            self.compaction_group_member_tables,
            Self::build_compaction_group_member_tables(&self.state_table_info)
        );
        (changed_table, has_bumped_committed_epoch)
    }

    pub fn info(&self) -> &HashMap<TableId, StateTableInfo> {
        &self.state_table_info
    }

    pub fn compaction_group_member_table_ids(
        &self,
        compaction_group_id: CompactionGroupId,
    ) -> &BTreeSet<TableId> {
        static EMPTY_SET: LazyLock<BTreeSet<TableId>> = LazyLock::new(BTreeSet::new);
        self.compaction_group_member_tables
            .get(&compaction_group_id)
            .unwrap_or_else(|| EMPTY_SET.deref())
    }

    pub fn compaction_group_member_tables(&self) -> &HashMap<CompactionGroupId, BTreeSet<TableId>> {
        &self.compaction_group_member_tables
    }

    pub fn max_table_committed_epoch(&self) -> Option<HummockEpoch> {
        self.state_table_info
            .values()
            .map(|info| info.committed_epoch)
            .max()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersionCommon<T> {
    pub id: HummockVersionId,
    pub levels: HashMap<CompactionGroupId, LevelsCommon<T>>,
    #[deprecated]
    pub(crate) max_committed_epoch: u64,
    pub table_watermarks: HashMap<TableId, Arc<TableWatermarks>>,
    pub table_change_log: HashMap<TableId, TableChangeLogCommon<T>>,
    pub state_table_info: HummockVersionStateTableInfo,
}

pub type HummockVersion = HummockVersionCommon<SstableInfo>;

impl Default for HummockVersion {
    fn default() -> Self {
        HummockVersion::from(&PbHummockVersion::default())
    }
}

impl<T> HummockVersionCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert the `PbHummockVersion` received from rpc to `HummockVersion`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(pb_version: &PbHummockVersion) -> Self {
        pb_version.into()
    }

    /// Convert the `PbHummockVersion` deserialized from persisted state to `HummockVersion`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(pb_version: &PbHummockVersion) -> Self {
        pb_version.into()
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        self.into()
    }
}

impl HummockVersion {
    pub fn estimated_encode_len(&self) -> usize {
        self.levels.len() * size_of::<CompactionGroupId>()
            + self
                .levels
                .values()
                .map(|level| level.estimated_encode_len())
                .sum::<usize>()
            + self.table_watermarks.len() * size_of::<u32>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| table_watermark.estimated_encode_len())
                .sum::<usize>()
            + self
                .table_change_log
                .values()
                .map(|c| {
                    c.iter()
                        .map(|l| {
                            l.old_value
                                .iter()
                                .chain(l.new_value.iter())
                                .map(|s| s.estimated_encode_len())
                                .sum::<usize>()
                        })
                        .sum::<usize>()
                })
                .sum::<usize>()
    }
}

impl<T> From<&PbHummockVersion> for HummockVersionCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_version: &PbHummockVersion) -> Self {
        #[expect(deprecated)]
        Self {
            id: HummockVersionId(pb_version.id),
            levels: pb_version
                .levels
                .iter()
                .map(|(group_id, levels)| {
                    (*group_id as CompactionGroupId, LevelsCommon::from(levels))
                })
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| {
                    (
                        TableId::new(*table_id),
                        Arc::new(TableWatermarks::from(table_watermark)),
                    )
                })
                .collect(),
            table_change_log: pb_version
                .table_change_logs
                .iter()
                .map(|(table_id, change_log)| {
                    (
                        TableId::new(*table_id),
                        TableChangeLogCommon::from_protobuf(change_log),
                    )
                })
                .collect(),
            state_table_info: HummockVersionStateTableInfo::from_protobuf(
                &pb_version.state_table_info,
            ),
        }
    }
}

impl<T> From<&HummockVersionCommon<T>> for PbHummockVersion
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version: &HummockVersionCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version.id.0,
            levels: version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.as_ref().into()))
                .collect(),
            table_change_logs: version
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| (table_id.table_id, change_log.to_protobuf()))
                .collect(),
            state_table_info: version.state_table_info.to_protobuf(),
        }
    }
}

impl<T> From<HummockVersionCommon<T>> for PbHummockVersion
where
    PbSstableInfo: From<T>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version: HummockVersionCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version.id.0,
            levels: version
                .levels
                .into_iter()
                .map(|(group_id, levels)| (group_id as _, levels.into()))
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version
                .table_watermarks
                .into_iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.as_ref().into()))
                .collect(),
            table_change_logs: version
                .table_change_log
                .into_iter()
                .map(|(table_id, change_log)| (table_id.table_id, change_log.to_protobuf()))
                .collect(),
            state_table_info: version.state_table_info.to_protobuf(),
        }
    }
}

impl HummockVersion {
    pub fn next_version_id(&self) -> HummockVersionId {
        self.id.next()
    }

    pub fn need_fill_backward_compatible_state_table_info_delta(&self) -> bool {
        // for backward-compatibility of previous hummock version delta
        self.state_table_info.state_table_info.is_empty()
            && self.levels.values().any(|group| {
                // state_table_info is not previously filled, but there previously exists some tables
                #[expect(deprecated)]
                !group.member_table_ids.is_empty()
            })
    }

    pub fn may_fill_backward_compatible_state_table_info_delta(
        &self,
        delta: &mut HummockVersionDelta,
    ) {
        #[expect(deprecated)]
        // for backward-compatibility of previous hummock version delta
        for (cg_id, group) in &self.levels {
            for table_id in &group.member_table_ids {
                assert!(
                    delta
                        .state_table_info_delta
                        .insert(
                            TableId::new(*table_id),
                            StateTableInfoDelta {
                                committed_epoch: self.max_committed_epoch,
                                compaction_group_id: *cg_id,
                            }
                        )
                        .is_none(),
                    "duplicate table id {} in cg {}",
                    table_id,
                    cg_id
                );
            }
        }
    }

    pub fn table_committed_epoch(&self, table_id: TableId) -> Option<u64> {
        self.state_table_info
            .info()
            .get(&table_id)
            .map(|info| info.committed_epoch)
    }

    pub fn create_init_version(default_compaction_config: Arc<CompactionConfig>) -> HummockVersion {
        #[expect(deprecated)]
        let mut init_version = HummockVersion {
            id: FIRST_VERSION_ID,
            levels: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            table_watermarks: HashMap::new(),
            table_change_log: HashMap::new(),
            state_table_info: HummockVersionStateTableInfo::empty(),
        };
        for group_id in [
            StaticCompactionGroupId::StateDefault as CompactionGroupId,
            StaticCompactionGroupId::MaterializedView as CompactionGroupId,
        ] {
            init_version.levels.insert(
                group_id,
                build_initial_compaction_group_levels(group_id, default_compaction_config.as_ref()),
            );
        }
        init_version
    }

    pub fn version_delta_after(&self) -> HummockVersionDelta {
        #[expect(deprecated)]
        HummockVersionDelta {
            id: self.next_version_id(),
            prev_id: self.id,
            trivial_move: false,
            max_committed_epoch: self.max_committed_epoch,
            group_deltas: Default::default(),
            new_table_watermarks: HashMap::new(),
            removed_table_ids: HashSet::new(),
            change_log_delta: HashMap::new(),
            state_table_info_delta: Default::default(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HummockVersionDeltaCommon<T> {
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub group_deltas: HashMap<CompactionGroupId, GroupDeltasCommon<T>>,
    #[deprecated]
    pub(crate) max_committed_epoch: u64,
    pub trivial_move: bool,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: HashSet<TableId>,
    pub change_log_delta: HashMap<TableId, ChangeLogDeltaCommon<T>>,
    pub state_table_info_delta: HashMap<TableId, StateTableInfoDelta>,
}

pub type HummockVersionDelta = HummockVersionDeltaCommon<SstableInfo>;

impl Default for HummockVersionDelta {
    fn default() -> Self {
        HummockVersionDelta::from(&PbHummockVersionDelta::default())
    }
}

impl<T> HummockVersionDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
    PbSstableInfo: for<'a> From<&'a T>,
{
    /// Convert the `PbHummockVersionDelta` deserialized from persisted state to `HummockVersionDelta`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(delta: &PbHummockVersionDelta) -> Self {
        delta.into()
    }

    /// Convert the `PbHummockVersionDelta` received from rpc to `HummockVersionDelta`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(delta: &PbHummockVersionDelta) -> Self {
        delta.into()
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        self.into()
    }
}

pub trait SstableIdReader {
    fn sst_id(&self) -> HummockSstableId;
}

pub trait ObjectIdReader {
    fn object_id(&self) -> HummockSstableObjectId;
}

impl<T> HummockVersionDeltaCommon<T>
where
    T: SstableIdReader + ObjectIdReader,
{
    /// Get the newly added object ids from the version delta.
    ///
    /// Note: the result can be false positive because we only collect the set of sst object ids in the `inserted_table_infos`,
    /// but it is possible that the object is moved or split from other compaction groups or levels.
    pub fn newly_added_object_ids(&self) -> HashSet<HummockSstableObjectId> {
        self.newly_added_sst_infos()
            .map(|sst| sst.object_id())
            .collect()
    }

    pub fn newly_added_sst_ids(&self) -> HashSet<HummockSstableObjectId> {
        self.newly_added_sst_infos()
            .map(|sst| sst.sst_id())
            .collect()
    }

    pub fn newly_added_sst_infos(&self) -> impl Iterator<Item = &'_ T> {
        self.group_deltas
            .values()
            .flat_map(|group_deltas| {
                group_deltas.group_deltas.iter().flat_map(|group_delta| {
                    let sst_slice = match &group_delta {
                        GroupDeltaCommon::NewL0SubLevel(inserted_table_infos)
                        | GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon {
                            inserted_table_infos,
                            ..
                        }) => Some(inserted_table_infos.iter()),
                        GroupDeltaCommon::GroupConstruct(_)
                        | GroupDeltaCommon::GroupDestroy(_)
                        | GroupDeltaCommon::GroupMerge(_) => None,
                    };
                    sst_slice.into_iter().flatten()
                })
            })
            .chain(self.change_log_delta.values().flat_map(|delta| {
                // TODO: optimization: strip table change log
                let new_log = delta.new_log.as_ref().unwrap();
                new_log.new_value.iter().chain(new_log.old_value.iter())
            }))
    }
}

impl HummockVersionDelta {
    #[expect(deprecated)]
    pub fn max_committed_epoch_for_migration(&self) -> HummockEpoch {
        self.max_committed_epoch
    }
}

impl<T> From<&PbHummockVersionDelta> for HummockVersionDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_version_delta: &PbHummockVersionDelta) -> Self {
        #[expect(deprecated)]
        Self {
            id: HummockVersionId(pb_version_delta.id),
            prev_id: HummockVersionId(pb_version_delta.prev_id),
            group_deltas: pb_version_delta
                .group_deltas
                .iter()
                .map(|(group_id, deltas)| {
                    (
                        *group_id as CompactionGroupId,
                        GroupDeltasCommon::from(deltas),
                    )
                })
                .collect(),
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            trivial_move: pb_version_delta.trivial_move,
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| {
                    (TableId::new(*table_id), TableWatermarks::from(watermarks))
                })
                .collect(),
            removed_table_ids: pb_version_delta
                .removed_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
            change_log_delta: pb_version_delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| {
                    (
                        TableId::new(*table_id),
                        ChangeLogDeltaCommon {
                            new_log: log_delta.new_log.as_ref().map(Into::into),
                            truncate_epoch: log_delta.truncate_epoch,
                        },
                    )
                })
                .collect(),

            state_table_info_delta: pb_version_delta
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (TableId::new(*table_id), *delta))
                .collect(),
        }
    }
}

impl<T> From<&HummockVersionDeltaCommon<T>> for PbHummockVersionDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(version_delta: &HummockVersionDeltaCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version_delta.id.0,
            prev_id: version_delta.prev_id.0,
            group_deltas: version_delta
                .group_deltas
                .iter()
                .map(|(group_id, deltas)| (*group_id as _, deltas.into()))
                .collect(),
            max_committed_epoch: version_delta.max_committed_epoch,
            trivial_move: version_delta.trivial_move,
            new_table_watermarks: version_delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta
                .removed_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
            change_log_delta: version_delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (table_id.table_id, log_delta.into()))
                .collect(),
            state_table_info_delta: version_delta
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (table_id.table_id, *delta))
                .collect(),
        }
    }
}

impl<T> From<HummockVersionDeltaCommon<T>> for PbHummockVersionDelta
where
    PbSstableInfo: From<T>,
{
    fn from(version_delta: HummockVersionDeltaCommon<T>) -> Self {
        #[expect(deprecated)]
        Self {
            id: version_delta.id.0,
            prev_id: version_delta.prev_id.0,
            group_deltas: version_delta
                .group_deltas
                .into_iter()
                .map(|(group_id, deltas)| (group_id as _, deltas.into()))
                .collect(),
            max_committed_epoch: version_delta.max_committed_epoch,
            trivial_move: version_delta.trivial_move,
            new_table_watermarks: version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.into()))
                .collect(),
            removed_table_ids: version_delta
                .removed_table_ids
                .into_iter()
                .map(|table_id| table_id.table_id)
                .collect(),
            change_log_delta: version_delta
                .change_log_delta
                .into_iter()
                .map(|(table_id, log_delta)| (table_id.table_id, log_delta.into()))
                .collect(),
            state_table_info_delta: version_delta
                .state_table_info_delta
                .into_iter()
                .map(|(table_id, delta)| (table_id.table_id, delta))
                .collect(),
        }
    }
}

impl<T> From<PbHummockVersionDelta> for HummockVersionDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_version_delta: PbHummockVersionDelta) -> Self {
        #[expect(deprecated)]
        Self {
            id: HummockVersionId(pb_version_delta.id),
            prev_id: HummockVersionId(pb_version_delta.prev_id),
            group_deltas: pb_version_delta
                .group_deltas
                .into_iter()
                .map(|(group_id, deltas)| (group_id as CompactionGroupId, deltas.into()))
                .collect(),
            max_committed_epoch: pb_version_delta.max_committed_epoch,
            trivial_move: pb_version_delta.trivial_move,
            new_table_watermarks: pb_version_delta
                .new_table_watermarks
                .into_iter()
                .map(|(table_id, watermarks)| (TableId::new(table_id), watermarks.into()))
                .collect(),
            removed_table_ids: pb_version_delta
                .removed_table_ids
                .into_iter()
                .map(TableId::new)
                .collect(),
            change_log_delta: pb_version_delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| {
                    (
                        TableId::new(*table_id),
                        ChangeLogDeltaCommon {
                            new_log: log_delta.new_log.clone().map(Into::into),
                            truncate_epoch: log_delta.truncate_epoch,
                        },
                    )
                })
                .collect(),
            state_table_info_delta: pb_version_delta
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (TableId::new(*table_id), *delta))
                .collect(),
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct IntraLevelDeltaCommon<T> {
    pub level_idx: u32,
    pub l0_sub_level_id: u64,
    pub removed_table_ids: HashSet<u64>,
    pub inserted_table_infos: Vec<T>,
    pub vnode_partition_count: u32,
}

pub type IntraLevelDelta = IntraLevelDeltaCommon<SstableInfo>;

impl IntraLevelDelta {
    pub fn estimated_encode_len(&self) -> usize {
        size_of::<u32>()
            + size_of::<u64>()
            + self.removed_table_ids.len() * size_of::<u32>()
            + self
                .inserted_table_infos
                .iter()
                .map(|sst| sst.estimated_encode_len())
                .sum::<usize>()
            + size_of::<u32>()
    }
}

impl<T> From<PbIntraLevelDelta> for IntraLevelDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_intra_level_delta: PbIntraLevelDelta) -> Self {
        Self {
            level_idx: pb_intra_level_delta.level_idx,
            l0_sub_level_id: pb_intra_level_delta.l0_sub_level_id,
            removed_table_ids: HashSet::from_iter(pb_intra_level_delta.removed_table_ids),
            inserted_table_infos: pb_intra_level_delta
                .inserted_table_infos
                .into_iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: pb_intra_level_delta.vnode_partition_count,
        }
    }
}

impl<T> From<IntraLevelDeltaCommon<T>> for PbIntraLevelDelta
where
    PbSstableInfo: From<T>,
{
    fn from(intra_level_delta: IntraLevelDeltaCommon<T>) -> Self {
        Self {
            level_idx: intra_level_delta.level_idx,
            l0_sub_level_id: intra_level_delta.l0_sub_level_id,
            removed_table_ids: intra_level_delta.removed_table_ids.into_iter().collect(),
            inserted_table_infos: intra_level_delta
                .inserted_table_infos
                .into_iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: intra_level_delta.vnode_partition_count,
        }
    }
}

impl<T> From<&IntraLevelDeltaCommon<T>> for PbIntraLevelDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(intra_level_delta: &IntraLevelDeltaCommon<T>) -> Self {
        Self {
            level_idx: intra_level_delta.level_idx,
            l0_sub_level_id: intra_level_delta.l0_sub_level_id,
            removed_table_ids: intra_level_delta
                .removed_table_ids
                .iter()
                .cloned()
                .collect(),
            inserted_table_infos: intra_level_delta
                .inserted_table_infos
                .iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: intra_level_delta.vnode_partition_count,
        }
    }
}

impl<T> From<&PbIntraLevelDelta> for IntraLevelDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_intra_level_delta: &PbIntraLevelDelta) -> Self {
        Self {
            level_idx: pb_intra_level_delta.level_idx,
            l0_sub_level_id: pb_intra_level_delta.l0_sub_level_id,
            removed_table_ids: HashSet::from_iter(
                pb_intra_level_delta.removed_table_ids.iter().cloned(),
            ),
            inserted_table_infos: pb_intra_level_delta
                .inserted_table_infos
                .iter()
                .map(Into::into)
                .collect_vec(),
            vnode_partition_count: pb_intra_level_delta.vnode_partition_count,
        }
    }
}

impl IntraLevelDelta {
    pub fn new(
        level_idx: u32,
        l0_sub_level_id: u64,
        removed_table_ids: HashSet<u64>,
        inserted_table_infos: Vec<SstableInfo>,
        vnode_partition_count: u32,
    ) -> Self {
        Self {
            level_idx,
            l0_sub_level_id,
            removed_table_ids,
            inserted_table_infos,
            vnode_partition_count,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum GroupDeltaCommon<T> {
    NewL0SubLevel(Vec<T>),
    IntraLevel(IntraLevelDeltaCommon<T>),
    GroupConstruct(PbGroupConstruct),
    GroupDestroy(PbGroupDestroy),
    GroupMerge(PbGroupMerge),
}

pub type GroupDelta = GroupDeltaCommon<SstableInfo>;

impl<T> From<PbGroupDelta> for GroupDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_group_delta: PbGroupDelta) -> Self {
        match pb_group_delta.delta_type {
            Some(PbDeltaType::IntraLevel(pb_intra_level_delta)) => {
                GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon::from(pb_intra_level_delta))
            }
            Some(PbDeltaType::GroupConstruct(pb_group_construct)) => {
                GroupDeltaCommon::GroupConstruct(pb_group_construct)
            }
            Some(PbDeltaType::GroupDestroy(pb_group_destroy)) => {
                GroupDeltaCommon::GroupDestroy(pb_group_destroy)
            }
            Some(PbDeltaType::GroupMerge(pb_group_merge)) => {
                GroupDeltaCommon::GroupMerge(pb_group_merge)
            }
            Some(DeltaType::NewL0SubLevel(pb_new_sub_level)) => GroupDeltaCommon::NewL0SubLevel(
                pb_new_sub_level
                    .inserted_table_infos
                    .into_iter()
                    .map(T::from)
                    .collect(),
            ),
            None => panic!("delta_type is not set"),
        }
    }
}

impl<T> From<GroupDeltaCommon<T>> for PbGroupDelta
where
    PbSstableInfo: From<T>,
{
    fn from(group_delta: GroupDeltaCommon<T>) -> Self {
        match group_delta {
            GroupDeltaCommon::IntraLevel(intra_level_delta) => PbGroupDelta {
                delta_type: Some(PbDeltaType::IntraLevel(intra_level_delta.into())),
            },
            GroupDeltaCommon::GroupConstruct(pb_group_construct) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupConstruct(pb_group_construct)),
            },
            GroupDeltaCommon::GroupDestroy(pb_group_destroy) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupDestroy(pb_group_destroy)),
            },
            GroupDeltaCommon::GroupMerge(pb_group_merge) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupMerge(pb_group_merge)),
            },
            GroupDeltaCommon::NewL0SubLevel(new_sub_level) => PbGroupDelta {
                delta_type: Some(PbDeltaType::NewL0SubLevel(PbNewL0SubLevel {
                    inserted_table_infos: new_sub_level
                        .into_iter()
                        .map(PbSstableInfo::from)
                        .collect(),
                })),
            },
        }
    }
}

impl<T> From<&GroupDeltaCommon<T>> for PbGroupDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(group_delta: &GroupDeltaCommon<T>) -> Self {
        match group_delta {
            GroupDeltaCommon::IntraLevel(intra_level_delta) => PbGroupDelta {
                delta_type: Some(PbDeltaType::IntraLevel(intra_level_delta.into())),
            },
            GroupDeltaCommon::GroupConstruct(pb_group_construct) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupConstruct(pb_group_construct.clone())),
            },
            GroupDeltaCommon::GroupDestroy(pb_group_destroy) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupDestroy(*pb_group_destroy)),
            },
            GroupDeltaCommon::GroupMerge(pb_group_merge) => PbGroupDelta {
                delta_type: Some(PbDeltaType::GroupMerge(*pb_group_merge)),
            },
            GroupDeltaCommon::NewL0SubLevel(new_sub_level) => PbGroupDelta {
                delta_type: Some(PbDeltaType::NewL0SubLevel(PbNewL0SubLevel {
                    inserted_table_infos: new_sub_level.iter().map(PbSstableInfo::from).collect(),
                })),
            },
        }
    }
}

impl<T> From<&PbGroupDelta> for GroupDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_group_delta: &PbGroupDelta) -> Self {
        match &pb_group_delta.delta_type {
            Some(PbDeltaType::IntraLevel(pb_intra_level_delta)) => {
                GroupDeltaCommon::IntraLevel(IntraLevelDeltaCommon::from(pb_intra_level_delta))
            }
            Some(PbDeltaType::GroupConstruct(pb_group_construct)) => {
                GroupDeltaCommon::GroupConstruct(pb_group_construct.clone())
            }
            Some(PbDeltaType::GroupDestroy(pb_group_destroy)) => {
                GroupDeltaCommon::GroupDestroy(*pb_group_destroy)
            }
            Some(PbDeltaType::GroupMerge(pb_group_merge)) => {
                GroupDeltaCommon::GroupMerge(*pb_group_merge)
            }
            Some(DeltaType::NewL0SubLevel(pb_new_sub_level)) => GroupDeltaCommon::NewL0SubLevel(
                pb_new_sub_level
                    .inserted_table_infos
                    .iter()
                    .map(T::from)
                    .collect(),
            ),
            None => panic!("delta_type is not set"),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Default)]
pub struct GroupDeltasCommon<T> {
    pub group_deltas: Vec<GroupDeltaCommon<T>>,
}

pub type GroupDeltas = GroupDeltasCommon<SstableInfo>;

impl<T> From<PbGroupDeltas> for GroupDeltasCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(pb_group_deltas: PbGroupDeltas) -> Self {
        Self {
            group_deltas: pb_group_deltas
                .group_deltas
                .into_iter()
                .map(GroupDeltaCommon::from)
                .collect_vec(),
        }
    }
}

impl<T> From<GroupDeltasCommon<T>> for PbGroupDeltas
where
    PbSstableInfo: From<T>,
{
    fn from(group_deltas: GroupDeltasCommon<T>) -> Self {
        Self {
            group_deltas: group_deltas
                .group_deltas
                .into_iter()
                .map(|group_delta| group_delta.into())
                .collect_vec(),
        }
    }
}

impl<T> From<&GroupDeltasCommon<T>> for PbGroupDeltas
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(group_deltas: &GroupDeltasCommon<T>) -> Self {
        Self {
            group_deltas: group_deltas
                .group_deltas
                .iter()
                .map(|group_delta| group_delta.into())
                .collect_vec(),
        }
    }
}

impl<T> From<&PbGroupDeltas> for GroupDeltasCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(pb_group_deltas: &PbGroupDeltas) -> Self {
        Self {
            group_deltas: pb_group_deltas
                .group_deltas
                .iter()
                .map(GroupDeltaCommon::from)
                .collect_vec(),
        }
    }
}

impl<T> GroupDeltasCommon<T>
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    pub fn to_protobuf(&self) -> PbGroupDeltas {
        self.into()
    }
}
