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

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_pb::hummock::hummock_version_delta::PbChangeLogDelta;
use risingwave_pb::hummock::{
    PbEpochNewChangeLog, PbHummockVersion, PbHummockVersionDelta, PbSstableInfo, PbTableChangeLog,
    StateTableInfoDelta,
};

use crate::change_log::{ChangeLogDeltaCommon, EpochNewChangeLogCommon, TableChangeLogCommon};
use crate::version::{HummockVersion, HummockVersionDelta, HummockVersionStateTableInfo};
use crate::{HummockVersionId, INVALID_VERSION_ID};

#[derive(Clone, Debug)]
pub struct FrontendHummockVersion {
    pub id: HummockVersionId,
    pub state_table_info: HummockVersionStateTableInfo,
    pub table_change_log: HashMap<TableId, TableChangeLogCommon<()>>,
}

impl FrontendHummockVersion {
    pub fn from_version(version: &HummockVersion) -> Self {
        Self {
            id: version.id,
            state_table_info: version.state_table_info.clone(),
            table_change_log: version
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| {
                    (
                        *table_id,
                        TableChangeLogCommon::new(change_log.iter().map(|change_log| {
                            EpochNewChangeLogCommon {
                                new_value: vec![],
                                old_value: vec![],
                                epochs: change_log.epochs.clone(),
                            }
                        })),
                    )
                })
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        #[expect(deprecated)]
        PbHummockVersion {
            id: self.id.0,
            levels: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            table_watermarks: Default::default(),
            table_change_logs: self
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| {
                    (
                        table_id.table_id,
                        PbTableChangeLog {
                            change_logs: change_log
                                .iter()
                                .map(|change_log| PbEpochNewChangeLog {
                                    old_value: vec![],
                                    new_value: vec![],
                                    epochs: change_log.epochs.clone(),
                                })
                                .collect(),
                        },
                    )
                })
                .collect(),
            state_table_info: self.state_table_info.to_protobuf(),
        }
    }

    pub fn from_protobuf(value: PbHummockVersion) -> Self {
        Self {
            id: HummockVersionId(value.id),
            state_table_info: HummockVersionStateTableInfo::from_protobuf(&value.state_table_info),
            table_change_log: value
                .table_change_logs
                .into_iter()
                .map(|(table_id, change_log)| {
                    (
                        TableId::new(table_id),
                        TableChangeLogCommon::new(change_log.change_logs.into_iter().map(
                            |change_log| EpochNewChangeLogCommon {
                                new_value: vec![],
                                old_value: vec![],
                                epochs: change_log.epochs,
                            },
                        )),
                    )
                })
                .collect(),
        }
    }

    pub fn apply_delta(&mut self, delta: FrontendHummockVersionDelta) {
        if self.id != INVALID_VERSION_ID {
            assert_eq!(self.id, delta.prev_id);
        }
        self.id = delta.id;
        let (changed_table_info, _) = self
            .state_table_info
            .apply_delta(&delta.state_table_info_delta, &delta.removed_table_id);
        HummockVersion::apply_change_log_delta(
            &mut self.table_change_log,
            &delta.change_log_delta,
            &delta.removed_table_id,
            &delta.state_table_info_delta,
            &changed_table_info,
        );
    }
}

pub struct FrontendHummockVersionDelta {
    pub prev_id: HummockVersionId,
    pub id: HummockVersionId,
    pub removed_table_id: HashSet<TableId>,
    pub state_table_info_delta: HashMap<TableId, StateTableInfoDelta>,
    pub change_log_delta: HashMap<TableId, ChangeLogDeltaCommon<()>>,
}

impl FrontendHummockVersionDelta {
    pub fn from_delta(delta: &HummockVersionDelta) -> Self {
        Self {
            prev_id: delta.prev_id,
            id: delta.id,
            removed_table_id: delta.removed_table_ids.clone(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, change_log_delta)| {
                    (
                        *table_id,
                        ChangeLogDeltaCommon {
                            truncate_epoch: change_log_delta.truncate_epoch,
                            new_log: change_log_delta.new_log.as_ref().map(|new_log| {
                                EpochNewChangeLogCommon {
                                    // Here we need to determine if value is null but don't care what the value is, so we fill him in using `()`
                                    new_value: vec![(); new_log.new_value.len()],
                                    old_value: vec![(); new_log.old_value.len()],
                                    epochs: new_log.epochs.clone(),
                                }
                            }),
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        #[expect(deprecated)]
        PbHummockVersionDelta {
            id: self.id.to_u64(),
            prev_id: self.prev_id.to_u64(),
            group_deltas: Default::default(),
            max_committed_epoch: INVALID_EPOCH,
            trivial_move: false,
            new_table_watermarks: Default::default(),
            removed_table_ids: self
                .removed_table_id
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
            change_log_delta: self
                .change_log_delta
                .iter()
                .map(|(table_id, delta)| {
                    (
                        table_id.table_id,
                        PbChangeLogDelta {
                            new_log: delta.new_log.as_ref().map(|new_log| PbEpochNewChangeLog {
                                // Here we need to determine if value is null but don't care what the value is, so we fill him in using `PbSstableInfo::default()`
                                old_value: vec![PbSstableInfo::default(); new_log.old_value.len()],
                                new_value: vec![PbSstableInfo::default(); new_log.new_value.len()],
                                epochs: new_log.epochs.clone(),
                            }),
                            truncate_epoch: delta.truncate_epoch,
                        },
                    )
                })
                .collect(),
            state_table_info_delta: self
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (table_id.table_id, *delta))
                .collect(),
        }
    }

    pub fn from_protobuf(delta: PbHummockVersionDelta) -> Self {
        Self {
            prev_id: HummockVersionId::new(delta.prev_id),
            id: HummockVersionId::new(delta.id),
            removed_table_id: delta
                .removed_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
            state_table_info_delta: delta
                .state_table_info_delta
                .into_iter()
                .map(|(table_id, delta)| (TableId::new(table_id), delta))
                .collect(),
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, change_log_delta)| {
                    (
                        TableId::new(*table_id),
                        ChangeLogDeltaCommon {
                            truncate_epoch: change_log_delta.truncate_epoch,
                            new_log: change_log_delta.new_log.as_ref().map(|new_log| {
                                EpochNewChangeLogCommon {
                                    // Here we need to determine if value is null but don't care what the value is, so we fill him in using `()`
                                    new_value: vec![(); new_log.new_value.len()],
                                    old_value: vec![(); new_log.old_value.len()],
                                    epochs: new_log.epochs.clone(),
                                }
                            }),
                        },
                    )
                })
                .collect(),
        }
    }
}
