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

use std::collections::{HashMap, HashSet, VecDeque};

use anyhow::anyhow;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::time_travel::{
    refill_version, IncompleteHummockVersion, IncompleteHummockVersionDelta,
};
use risingwave_hummock_sdk::version::{GroupDeltaCommon, HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockEpoch, HummockSstableId, HummockSstableObjectId,
};
use risingwave_meta_model::hummock_sstable_info::SstableInfoV2Backend;
use risingwave_meta_model::{
    hummock_epoch_to_version, hummock_sstable_info, hummock_time_travel_delta,
    hummock_time_travel_version,
};
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, Condition, DatabaseTransaction, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect, TransactionTrait,
};

use crate::hummock::error::{Error, Result};
use crate::hummock::HummockManager;

/// Time travel.
impl HummockManager {
    pub(crate) async fn init_time_travel_state(&self) -> Result<()> {
        let sql_store = self.env.meta_store_ref();
        let mut guard = self.versioning.write().await;
        guard.mark_next_time_travel_version_snapshot();

        guard.last_time_travel_snapshot_sst_ids = HashSet::new();
        let Some(version) = hummock_time_travel_version::Entity::find()
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .map(|v| IncompleteHummockVersion::from_persisted_protobuf(&v.version.to_protobuf()))
        else {
            return Ok(());
        };
        guard.last_time_travel_snapshot_sst_ids = version.get_sst_ids();
        Ok(())
    }

    pub(crate) async fn truncate_time_travel_metadata(
        &self,
        epoch_watermark: HummockEpoch,
    ) -> Result<()> {
        let min_pinned_version_id = self.context_info.read().await.min_pinned_version_id();
        let sql_store = self.env.meta_store_ref();
        let txn = sql_store.conn.begin().await?;
        let version_watermark = hummock_epoch_to_version::Entity::find()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .order_by_asc(hummock_epoch_to_version::Column::VersionId)
            .one(&txn)
            .await?;
        let Some(version_watermark) = version_watermark else {
            txn.commit().await?;
            return Ok(());
        };
        let watermark_version_id = std::cmp::min(
            version_watermark.version_id,
            min_pinned_version_id.to_u64().try_into().unwrap(),
        );
        let res = hummock_epoch_to_version::Entity::delete_many()
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lt(risingwave_meta_model::Epoch::try_from(epoch_watermark).unwrap()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark,
            "delete {} rows from hummock_epoch_to_version",
            res.rows_affected
        );
        let latest_valid_version = hummock_time_travel_version::Entity::find()
            .filter(hummock_time_travel_version::Column::VersionId.lte(watermark_version_id))
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&txn)
            .await?
            .map(|m| IncompleteHummockVersion::from_persisted_protobuf(&m.version.to_protobuf()));
        let Some(latest_valid_version) = latest_valid_version else {
            txn.commit().await?;
            return Ok(());
        };
        let (
            latest_valid_version_id,
            latest_valid_version_sst_ids,
            latest_valid_version_object_ids,
        ) = {
            (
                latest_valid_version.id,
                latest_valid_version.get_sst_ids(),
                latest_valid_version.get_object_ids(),
            )
        };
        let mut object_ids_to_delete: HashSet<_> = HashSet::default();
        let version_ids_to_delete: Vec<risingwave_meta_model::HummockVersionId> =
            hummock_time_travel_version::Entity::find()
                .select_only()
                .column(hummock_time_travel_version::Column::VersionId)
                .filter(
                    hummock_time_travel_version::Column::VersionId
                        .lt(latest_valid_version_id.to_u64()),
                )
                .order_by_desc(hummock_time_travel_version::Column::VersionId)
                .into_tuple()
                .all(&txn)
                .await?;
        let delta_ids_to_delete: Vec<risingwave_meta_model::HummockVersionId> =
            hummock_time_travel_delta::Entity::find()
                .select_only()
                .column(hummock_time_travel_delta::Column::VersionId)
                .filter(
                    hummock_time_travel_delta::Column::VersionId
                        .lt(latest_valid_version_id.to_u64()),
                )
                .into_tuple()
                .all(&txn)
                .await?;
        for delta_id_to_delete in delta_ids_to_delete {
            let delta_to_delete = hummock_time_travel_delta::Entity::find_by_id(delta_id_to_delete)
                .one(&txn)
                .await?
                .ok_or_else(|| {
                    Error::TimeTravel(anyhow!(format!(
                        "version delta {} not found",
                        delta_id_to_delete
                    )))
                })?;
            let delta_to_delete = IncompleteHummockVersionDelta::from_persisted_protobuf(
                &delta_to_delete.version_delta.to_protobuf(),
            );
            let new_sst_ids = delta_to_delete.newly_added_sst_ids();
            // The SST ids added and then deleted by compaction between the 2 versions.
            let sst_ids_to_delete = &new_sst_ids - &latest_valid_version_sst_ids;
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            let new_object_ids = delta_to_delete.newly_added_object_ids();
            object_ids_to_delete.extend(&new_object_ids - &latest_valid_version_object_ids);
            tracing::debug!(
                delta_id = delta_to_delete.id.to_u64(),
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
        }
        let mut next_version_sst_ids = latest_valid_version_sst_ids;
        let mut sst_ids_to_delete: HashSet<_> = HashSet::default();
        for prev_version_id in version_ids_to_delete {
            let prev_version = {
                let prev_version = hummock_time_travel_version::Entity::find_by_id(prev_version_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| {
                        Error::TimeTravel(anyhow!(format!(
                            "prev_version {} not found",
                            prev_version_id
                        )))
                    })?;
                IncompleteHummockVersion::from_persisted_protobuf(
                    &prev_version.version.to_protobuf(),
                )
            };
            let sst_ids = prev_version.get_sst_ids();
            // The SST ids deleted by compaction between the 2 versions.
            sst_ids_to_delete.extend(&sst_ids - &next_version_sst_ids);
            // Reuse hummock_time_travel_epoch_version_insert_batch_size as threshold.
            if sst_ids_to_delete.len()
                >= self
                    .env
                    .opts
                    .hummock_time_travel_epoch_version_insert_batch_size
            {
                let res = hummock_sstable_info::Entity::delete_many()
                    .filter(
                        hummock_sstable_info::Column::SstId
                            .is_in(std::mem::take(&mut sst_ids_to_delete)),
                    )
                    .exec(&txn)
                    .await?;
                tracing::debug!(
                    "delete {} rows from hummock_sstable_info",
                    res.rows_affected
                );
            }
            let new_object_ids = prev_version.get_object_ids();
            object_ids_to_delete.extend(&new_object_ids - &latest_valid_version_object_ids);
            next_version_sst_ids = sst_ids;
        }
        if !sst_ids_to_delete.is_empty() {
            let res = hummock_sstable_info::Entity::delete_many()
                .filter(hummock_sstable_info::Column::SstId.is_in(sst_ids_to_delete))
                .exec(&txn)
                .await?;
            tracing::debug!(
                "delete {} rows from hummock_sstable_info",
                res.rows_affected
            );
        }

        if !object_ids_to_delete.is_empty() {
            // IMPORTANT: object_ids_to_delete may include objects that are still being used by SSTs not included in time travel metadata.
            // So it's crucial to filter out those objects before actually deleting them, i.e. when using `try_take_may_delete_object_ids`.
            self.gc_manager
                .add_may_delete_object_ids(object_ids_to_delete.into_iter());
        }

        let res = hummock_time_travel_version::Entity::delete_many()
            .filter(
                hummock_time_travel_version::Column::VersionId.lt(latest_valid_version_id.to_u64()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = ?watermark_version_id,
            ?latest_valid_version_id,
            "delete {} rows from hummock_time_travel_version",
            res.rows_affected
        );

        let res = hummock_time_travel_delta::Entity::delete_many()
            .filter(
                hummock_time_travel_delta::Column::VersionId.lt(latest_valid_version_id.to_u64()),
            )
            .exec(&txn)
            .await?;
        tracing::debug!(
            epoch_watermark_version_id = ?watermark_version_id,
            ?latest_valid_version_id,
            "delete {} rows from hummock_time_travel_delta",
            res.rows_affected
        );

        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn filter_out_objects_by_time_travel(
        &self,
        objects: impl Iterator<Item = HummockSstableObjectId>,
        batch_size: usize,
    ) -> Result<HashSet<HummockSstableObjectId>> {
        // The input object count is much smaller than time travel pinned object count in meta store.
        // So search input object in meta store.
        let mut result: HashSet<_> = objects.collect();
        let mut remain: VecDeque<_> = result.iter().copied().collect();
        while !remain.is_empty() {
            let batch = remain.drain(..std::cmp::min(remain.len(), batch_size));
            let reject_object_ids: Vec<risingwave_meta_model::HummockSstableObjectId> =
                hummock_sstable_info::Entity::find()
                    .filter(hummock_sstable_info::Column::ObjectId.is_in(batch))
                    .select_only()
                    .column(hummock_sstable_info::Column::ObjectId)
                    .into_tuple()
                    .all(&self.env.meta_store_ref().conn)
                    .await?;
            for reject in reject_object_ids {
                let object_id = HummockSstableObjectId::try_from(reject).unwrap();
                result.remove(&object_id);
            }
        }
        Ok(result)
    }

    pub(crate) async fn time_travel_pinned_object_count(&self) -> Result<u64> {
        let count = hummock_sstable_info::Entity::find()
            .count(&self.env.meta_store_ref().conn)
            .await?;
        Ok(count)
    }

    /// Attempt to locate the version corresponding to `query_epoch`.
    ///
    /// The version is retrieved from `hummock_epoch_to_version`, selecting the entry with the largest epoch that's lte `query_epoch`.
    ///
    /// The resulted version is complete, i.e. with correct `SstableInfo`.
    pub async fn epoch_to_version(
        &self,
        query_epoch: HummockEpoch,
        table_id: u32,
    ) -> Result<HummockVersion> {
        let sql_store = self.env.meta_store_ref();
        let _permit = self.inflight_time_travel_query.try_acquire().map_err(|_| {
            anyhow!(format!(
                "too many inflight time travel queries, max_inflight_time_travel_query={}",
                self.env.opts.max_inflight_time_travel_query
            ))
        })?;
        let epoch_to_version = hummock_epoch_to_version::Entity::find()
            .filter(
                Condition::any()
                    .add(hummock_epoch_to_version::Column::TableId.eq(i64::from(table_id)))
                    // for backward compatibility
                    .add(hummock_epoch_to_version::Column::TableId.eq(0)),
            )
            .filter(
                hummock_epoch_to_version::Column::Epoch
                    .lte(risingwave_meta_model::Epoch::try_from(query_epoch).unwrap()),
            )
            .order_by_desc(hummock_epoch_to_version::Column::Epoch)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "version not found for epoch {}",
                    query_epoch
                )))
            })?;
        let timer = self
            .metrics
            .time_travel_version_replay_latency
            .start_timer();
        let actual_version_id = epoch_to_version.version_id;
        tracing::debug!(
            query_epoch,
            query_tz = ?(Epoch(query_epoch).as_timestamptz()),
            actual_epoch = epoch_to_version.epoch,
            actual_tz = ?(Epoch(u64::try_from(epoch_to_version.epoch).unwrap()).as_timestamptz()),
            actual_version_id,
            "convert query epoch"
        );

        let replay_version = hummock_time_travel_version::Entity::find()
            .filter(hummock_time_travel_version::Column::VersionId.lte(actual_version_id))
            .order_by_desc(hummock_time_travel_version::Column::VersionId)
            .one(&sql_store.conn)
            .await?
            .ok_or_else(|| {
                Error::TimeTravel(anyhow!(format!(
                    "no replay version found for epoch {}, version {}",
                    query_epoch, actual_version_id,
                )))
            })?;
        let deltas = hummock_time_travel_delta::Entity::find()
            .filter(hummock_time_travel_delta::Column::VersionId.gt(replay_version.version_id))
            .filter(hummock_time_travel_delta::Column::VersionId.lte(actual_version_id))
            .order_by_asc(hummock_time_travel_delta::Column::VersionId)
            .all(&sql_store.conn)
            .await?;
        // SstableInfo in actual_version is incomplete before refill_version.
        let mut actual_version = replay_archive(
            replay_version.version.to_protobuf(),
            deltas.into_iter().map(|d| d.version_delta.to_protobuf()),
        );

        let mut sst_ids = actual_version
            .get_sst_ids()
            .into_iter()
            .collect::<VecDeque<_>>();
        let sst_count = sst_ids.len();
        let mut sst_id_to_info = HashMap::with_capacity(sst_count);
        let sst_info_fetch_batch_size = self.env.opts.hummock_time_travel_sst_info_fetch_batch_size;
        while !sst_ids.is_empty() {
            let sst_infos = hummock_sstable_info::Entity::find()
                .filter(hummock_sstable_info::Column::SstId.is_in(
                    sst_ids.drain(..std::cmp::min(sst_info_fetch_batch_size, sst_ids.len())),
                ))
                .all(&sql_store.conn)
                .await?;
            for sst_info in sst_infos {
                let sst_info: SstableInfo = sst_info.sstable_info.to_protobuf().into();
                sst_id_to_info.insert(sst_info.sst_id, sst_info);
            }
        }
        if sst_count != sst_id_to_info.len() {
            return Err(Error::TimeTravel(anyhow!(format!(
                "some SstableInfos not found for epoch {}, version {}",
                query_epoch, actual_version_id,
            ))));
        }
        refill_version(&mut actual_version, &sst_id_to_info, table_id);
        timer.observe_duration();
        Ok(actual_version)
    }

    pub(crate) async fn write_time_travel_metadata(
        &self,
        txn: &DatabaseTransaction,
        version: Option<&HummockVersion>,
        delta: HummockVersionDelta,
        time_travel_table_ids: HashSet<StateTableId>,
        skip_sst_ids: &HashSet<HummockSstableId>,
        tables_to_commit: impl Iterator<Item = (&TableId, &CompactionGroupId, u64)>,
    ) -> Result<Option<HashSet<HummockSstableId>>> {
        if self
            .env
            .system_params_reader()
            .await
            .time_travel_retention_ms()
            == 0
        {
            return Ok(None);
        }
        async fn write_sstable_infos(
            mut sst_infos: impl Iterator<Item = &SstableInfo>,
            txn: &DatabaseTransaction,
            batch_size: usize,
        ) -> Result<usize> {
            let mut count = 0;
            let mut is_finished = false;
            while !is_finished {
                let mut remain = batch_size;
                let mut batch = vec![];
                while remain > 0 {
                    let Some(sst_info) = sst_infos.next() else {
                        is_finished = true;
                        break;
                    };
                    batch.push(hummock_sstable_info::ActiveModel {
                        sst_id: Set(sst_info.sst_id.try_into().unwrap()),
                        object_id: Set(sst_info.object_id.try_into().unwrap()),
                        sstable_info: Set(SstableInfoV2Backend::from(&sst_info.to_protobuf())),
                    });
                    remain -= 1;
                    count += 1;
                }
                if batch.is_empty() {
                    break;
                }
                hummock_sstable_info::Entity::insert_many(batch)
                    .on_conflict_do_nothing()
                    .exec(txn)
                    .await?;
            }
            Ok(count)
        }

        let mut batch = vec![];
        for (table_id, _cg_id, committed_epoch) in tables_to_commit {
            let version_id: u64 = delta.id.to_u64();
            let m = hummock_epoch_to_version::ActiveModel {
                epoch: Set(committed_epoch.try_into().unwrap()),
                table_id: Set(table_id.table_id.into()),
                version_id: Set(version_id.try_into().unwrap()),
            };
            batch.push(m);
            if batch.len()
                >= self
                    .env
                    .opts
                    .hummock_time_travel_epoch_version_insert_batch_size
            {
                // There should be no conflict rows.
                hummock_epoch_to_version::Entity::insert_many(std::mem::take(&mut batch))
                    .do_nothing()
                    .exec(txn)
                    .await?;
            }
        }
        if !batch.is_empty() {
            // There should be no conflict rows.
            hummock_epoch_to_version::Entity::insert_many(batch)
                .do_nothing()
                .exec(txn)
                .await?;
        }

        let mut version_sst_ids = None;
        if let Some(version) = version {
            // `version_sst_ids` is used to update `last_time_travel_snapshot_sst_ids`.
            version_sst_ids = Some(
                version
                    .get_sst_infos()
                    .filter_map(|s| {
                        if s.table_ids
                            .iter()
                            .any(|tid| time_travel_table_ids.contains(tid))
                        {
                            return Some(s.sst_id);
                        }
                        None
                    })
                    .collect(),
            );
            write_sstable_infos(
                version.get_sst_infos().filter(|s| {
                    !skip_sst_ids.contains(&s.sst_id)
                        && s.table_ids
                            .iter()
                            .any(|tid| time_travel_table_ids.contains(tid))
                }),
                txn,
                self.env.opts.hummock_time_travel_sst_info_insert_batch_size,
            )
            .await?;
            let m = hummock_time_travel_version::ActiveModel {
                version_id: Set(risingwave_meta_model::HummockVersionId::try_from(
                    version.id.to_u64(),
                )
                .unwrap()),
                version: Set(
                    (&IncompleteHummockVersion::from((version, &time_travel_table_ids))
                        .to_protobuf())
                        .into(),
                ),
            };
            hummock_time_travel_version::Entity::insert(m)
                .on_conflict_do_nothing()
                .exec(txn)
                .await?;
            // Return early to skip persisting delta.
            return Ok(version_sst_ids);
        }
        let written = write_sstable_infos(
            delta.newly_added_sst_infos().filter(|s| {
                !skip_sst_ids.contains(&s.sst_id)
                    && s.table_ids
                        .iter()
                        .any(|tid| time_travel_table_ids.contains(tid))
            }),
            txn,
            self.env.opts.hummock_time_travel_sst_info_insert_batch_size,
        )
        .await?;
        // Ignore delta which adds no data.
        if written > 0 {
            let m = hummock_time_travel_delta::ActiveModel {
                version_id: Set(risingwave_meta_model::HummockVersionId::try_from(
                    delta.id.to_u64(),
                )
                .unwrap()),
                version_delta: Set((&IncompleteHummockVersionDelta::from((
                    &delta,
                    &time_travel_table_ids,
                ))
                .to_protobuf())
                    .into()),
            };
            hummock_time_travel_delta::Entity::insert(m)
                .on_conflict_do_nothing()
                .exec(txn)
                .await?;
        }

        Ok(version_sst_ids)
    }
}

/// The `HummockVersion` is actually `InHummockVersion`. It requires `refill_version`.
fn replay_archive(
    version: PbHummockVersion,
    deltas: impl Iterator<Item = PbHummockVersionDelta>,
) -> HummockVersion {
    // The pb version ann pb version delta are actually written by InHummockVersion and InHummockVersionDelta, respectively.
    // Using HummockVersion make it easier for `refill_version` later.
    let mut last_version = HummockVersion::from_persisted_protobuf(&version);
    for d in deltas {
        let d = HummockVersionDelta::from_persisted_protobuf(&d);
        debug_assert!(
            !should_mark_next_time_travel_version_snapshot(&d),
            "unexpected time travel delta {:?}",
            d
        );
        // Need to work around the assertion in `apply_version_delta`.
        // Because compaction deltas are not included in time travel archive.
        while last_version.id < d.prev_id {
            last_version.id = last_version.id + 1;
        }
        last_version.apply_version_delta(&d);
    }
    last_version
}

pub fn require_sql_meta_store_err() -> Error {
    Error::TimeTravel(anyhow!("require SQL meta store"))
}

/// Time travel delta replay only expect `NewL0SubLevel`. In all other cases, a new version snapshot should be created.
pub fn should_mark_next_time_travel_version_snapshot(delta: &HummockVersionDelta) -> bool {
    delta.group_deltas.iter().any(|(_, deltas)| {
        deltas
            .group_deltas
            .iter()
            .any(|d| !matches!(d, GroupDeltaCommon::NewL0SubLevel(_)))
    })
}
