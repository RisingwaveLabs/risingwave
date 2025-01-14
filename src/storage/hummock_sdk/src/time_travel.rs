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

use std::collections::{HashMap, HashSet};

use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::hummock_version_delta::{PbChangeLogDelta, PbGroupDeltas};
use risingwave_pb::hummock::{group_delta, PbEpochNewChangeLog, PbLevel, PbSstableInfo};

use crate::change_log::{TableChangeLog, TableChangeLogCommon};
use crate::compaction_group::StateTableId;
use crate::level::Level;
use crate::sstable_info::SstableInfo;
use crate::version::{
    HummockVersion, HummockVersionCommon, HummockVersionDelta, HummockVersionDeltaCommon,
    ObjectIdReader, SstableIdReader,
};
use crate::{CompactionGroupId, HummockSstableId, HummockSstableObjectId};

pub type IncompleteHummockVersion = HummockVersionCommon<SstableIdInVersion>;

/// Populates `SstableInfo` for `table_id`.
/// `SstableInfo` not associated with `table_id` is removed.
pub fn refill_version(
    version: &mut HummockVersion,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
    table_id: u32,
) {
    for level in version.levels.values_mut().flat_map(|level| {
        level
            .l0
            .sub_levels
            .iter_mut()
            .rev()
            .chain(level.levels.iter_mut())
    }) {
        refill_level(level, sst_id_to_info);
        level
            .table_infos
            .retain(|t| t.table_ids.contains(&table_id));
    }
    version
        .table_change_log
        .retain(|t, _| t.table_id == table_id);
    for t in version.table_change_log.values_mut() {
        refill_table_change_log(t, sst_id_to_info);
    }
}

fn refill_level(level: &mut Level, sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>) {
    for s in &mut level.table_infos {
        refill_sstable_info(s, sst_id_to_info);
    }
}

fn refill_table_change_log(
    table_change_log: &mut TableChangeLog,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
) {
    for c in table_change_log.iter_mut() {
        for s in &mut c.old_value {
            refill_sstable_info(s, sst_id_to_info);
        }
        for s in &mut c.new_value {
            refill_sstable_info(s, sst_id_to_info);
        }
    }
}

/// Caller should ensure `sst_id_to_info` includes an entry corresponding to `sstable_info`.
fn refill_sstable_info(
    sstable_info: &mut SstableInfo,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
) {
    *sstable_info = sst_id_to_info
        .get(&sstable_info.sst_id)
        .unwrap_or_else(|| panic!("SstableInfo should exist"))
        .clone();
}

/// `SStableInfo` will be stripped.
impl From<(&HummockVersion, &HashSet<StateTableId>)> for IncompleteHummockVersion {
    fn from(p: (&HummockVersion, &HashSet<StateTableId>)) -> Self {
        let (version, time_travel_table_ids) = p;
        #[expect(deprecated)]
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .map(|(group_id, levels)| {
                    let pblevels = rewrite_levels(PbLevels::from(levels), time_travel_table_ids);
                    (*group_id as CompactionGroupId, pblevels.into())
                })
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version.table_watermarks.clone(),
            table_change_log: version
                .table_change_log
                .iter()
                .filter_map(|(table_id, change_log)| {
                    if !time_travel_table_ids.contains(&table_id.table_id()) {
                        return None;
                    }
                    debug_assert!(change_log.iter().all(|d| {
                        d.new_value.iter().chain(d.old_value.iter()).all(|s| {
                            s.table_ids
                                .iter()
                                .any(|tid| time_travel_table_ids.contains(tid))
                        })
                    }));
                    let incomplete_table_change_log = change_log
                        .iter()
                        .map(|e| PbEpochNewChangeLog::from(e).into());
                    Some((
                        *table_id,
                        TableChangeLogCommon::new(incomplete_table_change_log),
                    ))
                })
                .collect(),
            state_table_info: version.state_table_info.clone(),
        }
    }
}

/// Removes SST refs that don't contain any of `time_travel_table_ids`.
fn rewrite_levels(mut levels: PbLevels, time_travel_table_ids: &HashSet<StateTableId>) -> PbLevels {
    fn rewrite_level(level: &mut PbLevel, time_travel_table_ids: &HashSet<StateTableId>) {
        // The stats like `total_file_size` are not updated accordingly since they won't be used in time travel query.
        level.table_infos.retain(|sst| {
            sst.table_ids
                .iter()
                .any(|tid| time_travel_table_ids.contains(tid))
        });
    }
    for level in &mut levels.levels {
        rewrite_level(level, time_travel_table_ids);
    }
    if let Some(l0) = levels.l0.as_mut() {
        for sub_level in &mut l0.sub_levels {
            rewrite_level(sub_level, time_travel_table_ids);
        }
        l0.sub_levels.retain(|s| !s.table_infos.is_empty());
    }
    levels
}

/// [`IncompleteHummockVersionDelta`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbGroupDeltas`
/// - `ChangeLogDelta`
pub type IncompleteHummockVersionDelta = HummockVersionDeltaCommon<SstableIdInVersion>;

/// `SStableInfo` will be stripped.
impl From<(&HummockVersionDelta, &HashSet<StateTableId>)> for IncompleteHummockVersionDelta {
    fn from(p: (&HummockVersionDelta, &HashSet<StateTableId>)) -> Self {
        let (delta, time_travel_table_ids) = p;
        #[expect(deprecated)]
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta
                .group_deltas
                .iter()
                .map(|(cg_id, deltas)| {
                    let pb_group_deltas =
                        rewrite_group_deltas(PbGroupDeltas::from(deltas), time_travel_table_ids);
                    (*cg_id, pb_group_deltas.into())
                })
                .collect(),
            max_committed_epoch: delta.max_committed_epoch,
            trivial_move: delta.trivial_move,
            new_table_watermarks: delta.new_table_watermarks.clone(),
            removed_table_ids: delta.removed_table_ids.clone(),
            change_log_delta: delta
                .change_log_delta
                .iter()
                .filter_map(|(table_id, log_delta)| {
                    if !time_travel_table_ids.contains(&table_id.table_id()) {
                        return None;
                    }
                    debug_assert!(log_delta
                        .new_log
                        .as_ref()
                        .map(|d| {
                            d.new_value.iter().chain(d.old_value.iter()).all(|s| {
                                s.table_ids
                                    .iter()
                                    .any(|tid| time_travel_table_ids.contains(tid))
                            })
                        })
                        .unwrap_or(true));
                    Some((*table_id, PbChangeLogDelta::from(log_delta).into()))
                })
                .collect(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
        }
    }
}

/// Removes SST refs that don't contain any of `time_travel_table_ids`.
fn rewrite_group_deltas(
    mut group_deltas: PbGroupDeltas,
    time_travel_table_ids: &HashSet<StateTableId>,
) -> PbGroupDeltas {
    for group_delta in &mut group_deltas.group_deltas {
        let Some(group_delta::DeltaType::NewL0SubLevel(new_sub_level)) =
            &mut group_delta.delta_type
        else {
            tracing::error!(?group_delta, "unexpected delta type");
            continue;
        };
        new_sub_level.inserted_table_infos.retain(|sst| {
            sst.table_ids
                .iter()
                .any(|tid| time_travel_table_ids.contains(tid))
        });
    }
    group_deltas
}

pub struct SstableIdInVersion {
    sst_id: HummockSstableId,
    object_id: HummockSstableObjectId,
}

impl SstableIdReader for SstableIdInVersion {
    fn sst_id(&self) -> HummockSstableId {
        self.sst_id
    }
}

impl ObjectIdReader for SstableIdInVersion {
    fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
}

impl From<&SstableIdInVersion> for PbSstableInfo {
    fn from(sst_id: &SstableIdInVersion) -> Self {
        Self {
            sst_id: sst_id.sst_id,
            object_id: sst_id.object_id,
            ..Default::default()
        }
    }
}

impl From<SstableIdInVersion> for PbSstableInfo {
    fn from(sst_id: SstableIdInVersion) -> Self {
        (&sst_id).into()
    }
}

impl From<&PbSstableInfo> for SstableIdInVersion {
    fn from(s: &PbSstableInfo) -> Self {
        SstableIdInVersion {
            sst_id: s.sst_id,
            object_id: s.object_id,
        }
    }
}

impl From<PbSstableInfo> for SstableIdInVersion {
    fn from(value: PbSstableInfo) -> Self {
        (&value).into()
    }
}
