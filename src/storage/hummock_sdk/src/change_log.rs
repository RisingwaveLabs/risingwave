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

use std::collections::{HashMap, VecDeque};

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::hummock_version_delta::PbChangeLogDelta;
use risingwave_pb::hummock::{PbEpochNewChangeLog, PbSstableInfo, PbTableChangeLog};
use tracing::warn;

use crate::sstable_info::SstableInfo;

#[derive(Debug, Clone, PartialEq)]
pub struct TableChangeLogCommon<T>(
    // older log at the front
    VecDeque<EpochNewChangeLogCommon<T>>,
);

impl<T> TableChangeLogCommon<T> {
    pub fn new(logs: impl IntoIterator<Item = EpochNewChangeLogCommon<T>>) -> Self {
        let logs = logs.into_iter().collect::<VecDeque<_>>();
        debug_assert!(logs.iter().flat_map(|log| log.epochs.iter()).is_sorted());
        Self(logs)
    }

    pub fn iter(&self) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> {
        self.0.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut EpochNewChangeLogCommon<T>> {
        self.0.iter_mut()
    }

    pub fn add_change_log(&mut self, new_change_log: EpochNewChangeLogCommon<T>) {
        if let Some(prev_log) = self.0.back() {
            assert!(
                prev_log.epochs.last().expect("non-empty")
                    < new_change_log.epochs.first().expect("non-empty")
            );
        }
        self.0.push_back(new_change_log);
    }
}

pub type TableChangeLog = TableChangeLogCommon<SstableInfo>;

#[derive(Debug, Clone, PartialEq)]
pub struct EpochNewChangeLogCommon<T> {
    pub new_value: Vec<T>,
    pub old_value: Vec<T>,
    // epochs are sorted in ascending order
    pub epochs: Vec<u64>,
}

pub type EpochNewChangeLog = EpochNewChangeLogCommon<SstableInfo>;

impl<T> From<&EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(val: &EpochNewChangeLogCommon<T>) -> Self {
        Self {
            new_value: val.new_value.iter().map(|a| a.into()).collect(),
            old_value: val.old_value.iter().map(|a| a.into()).collect(),
            epochs: val.epochs.clone(),
        }
    }
}

impl<T> From<&PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(value: &PbEpochNewChangeLog) -> Self {
        Self {
            new_value: value.new_value.iter().map(|a| a.into()).collect(),
            old_value: value.old_value.iter().map(|a| a.into()).collect(),
            epochs: value.epochs.clone(),
        }
    }
}

impl<T> From<EpochNewChangeLogCommon<T>> for PbEpochNewChangeLog
where
    PbSstableInfo: From<T>,
{
    fn from(val: EpochNewChangeLogCommon<T>) -> Self {
        Self {
            new_value: val.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: val.old_value.into_iter().map(|a| a.into()).collect(),
            epochs: val.epochs,
        }
    }
}

impl<T> From<PbEpochNewChangeLog> for EpochNewChangeLogCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(value: PbEpochNewChangeLog) -> Self {
        Self {
            new_value: value.new_value.into_iter().map(|a| a.into()).collect(),
            old_value: value.old_value.into_iter().map(|a| a.into()).collect(),
            epochs: value.epochs,
        }
    }
}

impl<T> TableChangeLogCommon<T> {
    pub fn filter_epoch(
        &self,
        (min_epoch, max_epoch): (u64, u64),
    ) -> impl Iterator<Item = &EpochNewChangeLogCommon<T>> + '_ {
        let start = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.last().expect("non-empty") < &min_epoch
        });
        let end = self.0.partition_point(|epoch_change_log| {
            epoch_change_log.epochs.first().expect("non-empty") <= &max_epoch
        });
        self.0.range(start..end)
    }

    /// Returns epochs where value is non-null and >= `min_epoch`.
    pub fn get_non_empty_epochs(&self, min_epoch: u64, max_count: usize) -> Vec<u64> {
        self.filter_epoch((min_epoch, u64::MAX))
            .filter(|epoch_change_log| {
                // Filter out empty change logs
                let new_value_empty = epoch_change_log.new_value.is_empty();
                let old_value_empty = epoch_change_log.old_value.is_empty();
                !new_value_empty || !old_value_empty
            })
            .flat_map(|epoch_change_log| epoch_change_log.epochs.iter().cloned())
            .filter(|a| a >= &min_epoch)
            .take(max_count)
            .collect()
    }

    pub fn truncate(&mut self, truncate_epoch: u64) {
        while let Some(change_log) = self.0.front()
            && *change_log.epochs.last().expect("non-empty") <= truncate_epoch
        {
            let _change_log = self.0.pop_front().expect("non-empty");
        }
        if let Some(first_log) = self.0.front_mut() {
            first_log.epochs.retain(|epoch| *epoch > truncate_epoch);
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    pub fn to_protobuf(&self) -> PbTableChangeLog {
        PbTableChangeLog {
            change_logs: self.0.iter().map(|a| a.into()).collect(),
        }
    }
}

impl<T> TableChangeLogCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    pub fn from_protobuf(val: &PbTableChangeLog) -> Self {
        Self(val.change_logs.iter().map(|a| a.into()).collect())
    }
}

pub fn build_table_change_log_delta<'a>(
    old_value_ssts: impl Iterator<Item = SstableInfo>,
    new_value_ssts: impl Iterator<Item = &'a SstableInfo>,
    epochs: &Vec<u64>,
    log_store_table_ids: impl Iterator<Item = (u32, u64)>,
) -> HashMap<TableId, ChangeLogDelta> {
    let mut table_change_log: HashMap<_, _> = log_store_table_ids
        .map(|(table_id, truncate_epoch)| {
            (
                TableId::new(table_id),
                ChangeLogDelta {
                    truncate_epoch,
                    new_log: Some(EpochNewChangeLog {
                        new_value: vec![],
                        old_value: vec![],
                        epochs: epochs.clone(),
                    }),
                },
            )
        })
        .collect();
    for sst in old_value_ssts {
        for table_id in &sst.table_ids {
            match table_change_log.get_mut(&TableId::new(*table_id)) {
                Some(log) => {
                    log.new_log.as_mut().unwrap().old_value.push(sst.clone());
                }
                None => {
                    warn!(table_id, ?sst, "old value sst contains non-log-store table");
                }
            }
        }
    }
    for sst in new_value_ssts {
        for table_id in &sst.table_ids {
            if let Some(log) = table_change_log.get_mut(&TableId::new(*table_id)) {
                log.new_log.as_mut().unwrap().new_value.push(sst.clone());
            }
        }
    }
    table_change_log
}

#[derive(Debug, PartialEq, Clone)]
pub struct ChangeLogDeltaCommon<T> {
    pub truncate_epoch: u64,
    pub new_log: Option<EpochNewChangeLogCommon<T>>,
}

pub type ChangeLogDelta = ChangeLogDeltaCommon<SstableInfo>;

impl<T> From<&ChangeLogDeltaCommon<T>> for PbChangeLogDelta
where
    PbSstableInfo: for<'a> From<&'a T>,
{
    fn from(val: &ChangeLogDeltaCommon<T>) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.as_ref().map(|a| a.into()),
        }
    }
}

impl<T> From<&PbChangeLogDelta> for ChangeLogDeltaCommon<T>
where
    T: for<'a> From<&'a PbSstableInfo>,
{
    fn from(val: &PbChangeLogDelta) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.as_ref().map(|a| a.into()),
        }
    }
}

impl<T> From<ChangeLogDeltaCommon<T>> for PbChangeLogDelta
where
    PbSstableInfo: From<T>,
{
    fn from(val: ChangeLogDeltaCommon<T>) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.map(|a| a.into()),
        }
    }
}

impl<T> From<PbChangeLogDelta> for ChangeLogDeltaCommon<T>
where
    T: From<PbSstableInfo>,
{
    fn from(val: PbChangeLogDelta) -> Self {
        Self {
            truncate_epoch: val.truncate_epoch,
            new_log: val.new_log.map(|a| a.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::change_log::{EpochNewChangeLog, TableChangeLogCommon};
    use crate::sstable_info::SstableInfo;

    #[test]
    fn test_filter_epoch() {
        let table_change_log = TableChangeLogCommon::<SstableInfo>::new([
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![2],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![3, 4],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![5],
            },
        ]);

        let epochs = [1, 2, 3, 4, 5, 6];
        for i in 0..epochs.len() {
            for j in i..epochs.len() {
                let min_epoch = epochs[i];
                let max_epoch = epochs[j];
                let expected = table_change_log
                    .0
                    .iter()
                    .filter(|log| {
                        &min_epoch <= log.epochs.last().unwrap()
                            && log.epochs.first().unwrap() <= &max_epoch
                    })
                    .cloned()
                    .collect_vec();
                let actual = table_change_log
                    .filter_epoch((min_epoch, max_epoch))
                    .cloned()
                    .collect_vec();
                assert_eq!(expected, actual, "{:?}", (min_epoch, max_epoch));
            }
        }
    }

    #[test]
    fn test_truncate() {
        let mut table_change_log = TableChangeLogCommon::<SstableInfo>::new([
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![1],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![2],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![3, 4],
            },
            EpochNewChangeLog {
                new_value: vec![],
                old_value: vec![],
                epochs: vec![5],
            },
        ]);

        table_change_log.truncate(1);
        assert_eq!(
            table_change_log,
            TableChangeLogCommon::<SstableInfo>::new([
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![2],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![3, 4],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![5],
                },
            ])
        );

        table_change_log.truncate(3);
        assert_eq!(
            table_change_log,
            TableChangeLogCommon::<SstableInfo>::new([
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![4],
                },
                EpochNewChangeLog {
                    new_value: vec![],
                    old_value: vec![],
                    epochs: vec![5],
                },
            ])
        )
    }
}
