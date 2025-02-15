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

use std::collections::HashMap;
use std::ops::Bound;
use std::ops::Bound::*;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use either::Either;
use foyer::CacheHint;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use futures_async_stream::for_await;
use itertools::{izip, Itertools};
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{ArrayImplBuilder, ArrayRef, DataChunk, Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{
    get_dist_key_in_pk_indices, ColumnDesc, ColumnId, TableId, TableOption,
};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt, VnodeCountCompat};
use risingwave_common::row::{self, once, Once, OwnedRow, Row, RowExt};
use risingwave_common::types::{DataType, Datum, DefaultOrd, DefaultOrdered, ScalarImpl};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_hummock_sdk::key::{
    end_bound_of_prefix, prefixed_range_with_vnode, start_bound_of_excluded_prefix, CopyFromSlice,
    TableKey, TableKeyRange,
};
use risingwave_hummock_sdk::table_watermark::{
    VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::catalog::Table;
use risingwave_storage::error::{ErrorKind, StorageError, StorageResult};
use risingwave_storage::hummock::CachePolicy;
use risingwave_storage::mem_table::MemTableError;
use risingwave_storage::row_serde::find_columns_by_ids;
use risingwave_storage::row_serde::row_serde_util::{
    deserialize_pk_with_vnode, serialize_pk, serialize_pk_with_vnode,
};
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::{
    InitOptions, LocalStateStore, NewLocalOptions, OpConsistencyLevel, PrefetchOptions,
    ReadLogOptions, ReadOptions, SealCurrentEpochOptions, StateStoreIter, StateStoreIterExt,
    TryWaitEpochOptions,
};
use risingwave_storage::table::merge_sort::merge_sort;
use risingwave_storage::table::{
    deserialize_log_stream, ChangeLogRow, KeyedRow, TableDistribution,
};
use risingwave_storage::StateStore;
use thiserror_ext::AsReport;
use tracing::{trace, Instrument};

use crate::cache::cache_may_stale;
use crate::common::state_cache::{StateCache, StateCacheFiller};
use crate::common::table::state_table_cache::StateTableWatermarkCache;
use crate::executor::{StreamExecutorError, StreamExecutorResult};

/// Mostly watermark operators will have inserts (append-only).
/// So this number should not need to be very large.
/// But we may want to improve this choice in the future.
const WATERMARK_CACHE_ENTRIES: usize = 16;

/// This macro is used to mark a point where we want to randomly discard the operation and early
/// return, only in insane mode.
macro_rules! insane_mode_discard_point {
    () => {{
        use rand::Rng;
        if crate::consistency::insane() && rand::thread_rng().gen_bool(0.3) {
            return;
        }
    }};
}

/// `StateTableInner` is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding.
#[derive(Clone)]
pub struct StateTableInner<
    S,
    SD = BasicSerde,
    const IS_REPLICATED: bool = false,
    const USE_WATERMARK_CACHE: bool = false,
> where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Id for this table.
    table_id: TableId,

    /// State store backend.
    local_store: S::Local,

    /// State store for accessing snapshot data
    store: S,

    /// Used for serializing and deserializing the primary key.
    pk_serde: OrderedRowSerde,

    /// Row deserializer with value encoding
    row_serde: SD,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Distribution of the state table.
    ///
    /// It holds vnode bitmap. Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. The table will also check whether the written rows
    /// conform to this partition.
    distribution: TableDistribution,

    prefix_hint_len: usize,

    /// Used for catalog `table_properties`
    table_option: TableOption,

    value_indices: Option<Vec<usize>>,

    /// Pending watermark for state cleaning. Old states below this watermark will be cleaned when committing.
    pending_watermark: Option<ScalarImpl>,
    /// Last committed watermark for state cleaning. Will be restored on state table recovery.
    committed_watermark: Option<ScalarImpl>,
    /// Cache for the top-N primary keys for reducing unnecessary range deletion.
    watermark_cache: StateTableWatermarkCache,

    /// Data Types
    /// We will need to use to build data chunks from state table rows.
    data_types: Vec<DataType>,

    /// "i" here refers to the base `state_table`'s actual schema.
    /// "o" here refers to the replicated state table's output schema.
    /// This mapping is used to reconstruct a row being written from replicated state table.
    /// Such that the schema of this row will match the full schema of the base state table.
    /// It is only applicable for replication.
    i2o_mapping: ColIndexMapping,

    /// Output indices
    /// Used for:
    /// 1. Computing `output_value_indices` to ser/de replicated rows.
    /// 2. Computing output pk indices to used them for backfill state.
    output_indices: Vec<usize>,

    op_consistency_level: StateTableOpConsistencyLevel,

    clean_watermark_index_in_pk: Option<i32>,

    /// Flag to indicate whether the state table has called `commit`, but has not called
    /// `post_yield_barrier` on the `StateTablePostCommit` callback yet.
    on_post_commit: bool,
}

/// `StateTable` will use `BasicSerde` as default
pub type StateTable<S> = StateTableInner<S, BasicSerde>;
/// `ReplicatedStateTable` is meant to replicate upstream shared buffer.
/// Used for `ArrangementBackfill` executor.
pub type ReplicatedStateTable<S, SD> = StateTableInner<S, SD, true>;
/// `WatermarkCacheStateTable` caches the watermark column.
/// It will reduce state cleaning overhead.
pub type WatermarkCacheStateTable<S> = StateTableInner<S, BasicSerde, false, true>;
pub type WatermarkCacheParameterizedStateTable<S, const USE_WATERMARK_CACHE: bool> =
    StateTableInner<S, BasicSerde, false, USE_WATERMARK_CACHE>;

// initialize
impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// In streaming executors, this methods must be called **after** receiving and yielding the first barrier,
    /// and otherwise, deadlock can be likely to happen.
    pub async fn init_epoch(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.local_store.init(InitOptions::new(epoch)).await?;
        Ok(())
    }

    pub async fn try_wait_committed_epoch(&self, prev_epoch: u64) -> StorageResult<()> {
        self.store
            .try_wait_epoch(
                HummockReadEpoch::Committed(prev_epoch),
                TryWaitEpochOptions {
                    table_id: self.table_id,
                },
            )
            .await
    }

    pub fn state_store(&self) -> &S {
        &self.store
    }
}

fn consistent_old_value_op(
    row_serde: impl ValueRowSerde,
    is_log_store: bool,
) -> OpConsistencyLevel {
    OpConsistencyLevel::ConsistentOldValue {
        check_old_value: Arc::new(move |first: &Bytes, second: &Bytes| {
            if first == second {
                return true;
            }
            let first = match row_serde.deserialize(first) {
                Ok(rows) => rows,
                Err(e) => {
                    error!(error = %e.as_report(), value = ?first, "fail to deserialize serialized value");
                    return false;
                }
            };
            let second = match row_serde.deserialize(second) {
                Ok(rows) => rows,
                Err(e) => {
                    error!(error = %e.as_report(), value = ?second, "fail to deserialize serialized value");
                    return false;
                }
            };
            if first != second {
                error!(first = ?first, second = ?second, "sanity check fail");
                false
            } else {
                true
            }
        }),
        is_log_store,
    }
}

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum StateTableOpConsistencyLevel {
    /// Op is inconsistent
    Inconsistent,
    /// Op is consistent.
    /// - Insert op should ensure that the key does not exist previously
    /// - Delete and Update op should ensure that the key exists and the previous value matches the passed old value
    ConsistentOldValue,
    /// The requirement on operation consistency is the same as `ConsistentOldValue`.
    /// The difference is that in the `LogStoreEnabled`, the state table should also flush and store and old value.
    LogStoreEnabled,
}

// initialize
// FIXME(kwannoel): Enforce that none of the constructors here
// should be used by replicated state table.
// Apart from from_table_catalog_inner.
impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Create state table from table catalog and store.
    ///
    /// If `vnodes` is `None`, [`TableDistribution::singleton()`] will be used.
    pub async fn from_table_catalog(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        Self::from_table_catalog_with_consistency_level(
            table_catalog,
            store,
            vnodes,
            StateTableOpConsistencyLevel::ConsistentOldValue,
        )
        .await
    }

    /// Create state table from table catalog and store with sanity check disabled.
    pub async fn from_table_catalog_inconsistent_op(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
    ) -> Self {
        Self::from_table_catalog_with_consistency_level(
            table_catalog,
            store,
            vnodes,
            StateTableOpConsistencyLevel::Inconsistent,
        )
        .await
    }

    pub async fn from_table_catalog_with_consistency_level(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
        consistency_level: StateTableOpConsistencyLevel,
    ) -> Self {
        Self::from_table_catalog_inner(table_catalog, store, vnodes, consistency_level, vec![])
            .await
    }

    /// Create state table from table catalog and store.
    async fn from_table_catalog_inner(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
        op_consistency_level: StateTableOpConsistencyLevel,
        output_column_ids: Vec<ColumnId>,
    ) -> Self {
        let table_id = TableId::new(table_catalog.id);
        let table_columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|col| col.column_desc.as_ref().unwrap().into())
            .collect();
        let data_types: Vec<DataType> = table_catalog
            .columns
            .iter()
            .map(|col| {
                col.get_column_desc()
                    .unwrap()
                    .get_column_type()
                    .unwrap()
                    .into()
            })
            .collect();
        let order_types: Vec<OrderType> = table_catalog
            .pk
            .iter()
            .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
            .collect();
        let dist_key_indices: Vec<usize> = table_catalog
            .distribution_key
            .iter()
            .map(|dist_index| *dist_index as usize)
            .collect();

        let pk_indices = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index as usize)
            .collect_vec();

        // FIXME(yuhao): only use `dist_key_in_pk` in the proto
        let dist_key_in_pk_indices = if table_catalog.get_dist_key_in_pk().is_empty() {
            get_dist_key_in_pk_indices(&dist_key_indices, &pk_indices).unwrap()
        } else {
            table_catalog
                .get_dist_key_in_pk()
                .iter()
                .map(|idx| *idx as usize)
                .collect()
        };

        let vnode_col_idx_in_pk = table_catalog.vnode_col_index.as_ref().and_then(|idx| {
            let vnode_col_idx = *idx as usize;
            pk_indices.iter().position(|&i| vnode_col_idx == i)
        });

        let distribution =
            TableDistribution::new(vnodes, dist_key_in_pk_indices, vnode_col_idx_in_pk);
        assert_eq!(
            distribution.vnode_count(),
            table_catalog.vnode_count(),
            "vnode count mismatch, scanning table {} under wrong distribution?",
            table_catalog.name,
        );

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serde = OrderedRowSerde::new(pk_data_types, order_types);

        let input_value_indices = table_catalog
            .value_indices
            .iter()
            .map(|val| *val as usize)
            .collect_vec();

        let no_shuffle_value_indices = (0..table_columns.len()).collect_vec();

        // if value_indices is the no shuffle full columns.
        let value_indices = match input_value_indices.len() == table_columns.len()
            && input_value_indices == no_shuffle_value_indices
        {
            true => None,
            false => Some(input_value_indices),
        };
        let prefix_hint_len = table_catalog.read_prefix_len_hint as usize;

        let make_row_serde = || {
            SD::new(
                Arc::from_iter(table_catalog.value_indices.iter().map(|val| *val as usize)),
                Arc::from(table_columns.clone().into_boxed_slice()),
            )
        };

        let state_table_op_consistency_level = op_consistency_level;
        let op_consistency_level = match op_consistency_level {
            StateTableOpConsistencyLevel::Inconsistent => OpConsistencyLevel::Inconsistent,
            StateTableOpConsistencyLevel::ConsistentOldValue => {
                let row_serde = make_row_serde();
                consistent_old_value_op(row_serde, false)
            }
            StateTableOpConsistencyLevel::LogStoreEnabled => {
                let row_serde = make_row_serde();
                consistent_old_value_op(row_serde, true)
            }
        };

        let table_option = TableOption::new(table_catalog.retention_seconds);
        let new_local_options = if IS_REPLICATED {
            NewLocalOptions::new_replicated(
                table_id,
                op_consistency_level,
                table_option,
                distribution.vnodes().clone(),
            )
        } else {
            NewLocalOptions::new(
                table_id,
                op_consistency_level,
                table_option,
                distribution.vnodes().clone(),
            )
        };
        let local_state_store = store.new_local(new_local_options).await;

        let row_serde = make_row_serde();

        // If state table has versioning, that means it supports
        // Schema change. In that case, the row encoding should be column aware as well.
        // Otherwise both will be false.
        // NOTE(kwannoel): Replicated table will follow upstream table's versioning. I'm not sure
        // If ALTER TABLE will propagate to this replicated table as well. Ideally it won't
        assert_eq!(
            table_catalog.version.is_some(),
            row_serde.kind().is_column_aware()
        );

        // Restore persisted table watermark.
        let watermark_serde = if pk_indices.is_empty() {
            None
        } else {
            match table_catalog.clean_watermark_index_in_pk {
                None => Some(pk_serde.index(0)),
                Some(clean_watermark_index_in_pk) => {
                    Some(pk_serde.index(clean_watermark_index_in_pk as usize))
                }
            }
        };
        let max_watermark_of_vnodes = distribution
            .vnodes()
            .iter_vnodes()
            .filter_map(|vnode| local_state_store.get_table_watermark(vnode))
            .max();
        let committed_watermark = if let Some(deser) = watermark_serde
            && let Some(max_watermark) = max_watermark_of_vnodes
        {
            let deserialized = deser.deserialize(&max_watermark).ok().and_then(|row| {
                assert!(row.len() == 1);
                row[0].clone()
            });
            if deserialized.is_none() {
                tracing::error!(
                    vnodes = ?distribution.vnodes(),
                    watermark = ?max_watermark,
                    "Failed to deserialize persisted watermark from state store.",
                );
            }
            deserialized
        } else {
            None
        };

        let watermark_cache = if USE_WATERMARK_CACHE {
            StateTableWatermarkCache::new(WATERMARK_CACHE_ENTRIES)
        } else {
            StateTableWatermarkCache::new(0)
        };

        // Get info for replicated state table.
        let output_column_ids_to_input_idx = output_column_ids
            .iter()
            .enumerate()
            .map(|(pos, id)| (*id, pos))
            .collect::<HashMap<_, _>>();

        // Compute column descriptions
        let columns: Vec<ColumnDesc> = table_catalog
            .columns
            .iter()
            .map(|c| c.column_desc.as_ref().unwrap().into())
            .collect_vec();

        // Compute i2o mapping
        // Note that this can be a partial mapping, since we use the i2o mapping to get
        // any 1 of the output columns, and use that to fill the input column.
        let mut i2o_mapping = vec![None; columns.len()];
        for (i, column) in columns.iter().enumerate() {
            if let Some(pos) = output_column_ids_to_input_idx.get(&column.column_id) {
                i2o_mapping[i] = Some(*pos);
            }
        }
        // We can prune any duplicate column indices
        let i2o_mapping = ColIndexMapping::new(i2o_mapping, output_column_ids.len());

        // Compute output indices
        let (_, output_indices) = find_columns_by_ids(&columns[..], &output_column_ids);

        Self {
            table_id,
            local_store: local_state_store,
            store,
            pk_serde,
            row_serde,
            pk_indices,
            distribution,
            prefix_hint_len,
            table_option,
            value_indices,
            pending_watermark: None,
            committed_watermark,
            watermark_cache,
            data_types,
            output_indices,
            i2o_mapping,
            op_consistency_level: state_table_op_consistency_level,
            clean_watermark_index_in_pk: table_catalog.clean_watermark_index_in_pk,
            on_post_commit: false,
        }
    }

    pub fn get_data_types(&self) -> &[DataType] {
        &self.data_types
    }

    pub fn table_id(&self) -> u32 {
        self.table_id.table_id
    }

    /// get the newest epoch of the state store and panic if the `init_epoch()` has never be called
    pub fn epoch(&self) -> u64 {
        self.local_store.epoch()
    }

    /// Get the vnode value with given (prefix of) primary key
    fn compute_prefix_vnode(&self, pk_prefix: impl Row) -> VirtualNode {
        self.distribution
            .try_compute_vnode_by_pk_prefix(pk_prefix)
            .expect("For streaming, the given prefix must be enough to calculate the vnode")
    }

    /// Get the vnode value of the given primary key
    pub fn compute_vnode_by_pk(&self, pk: impl Row) -> VirtualNode {
        self.distribution.compute_vnode_by_pk(pk)
    }

    /// NOTE(kwannoel): This is used by backfill.
    /// We want to check pk indices of upstream table.
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    /// Get the indices of the primary key columns in the output columns.
    ///
    /// Returns `None` if any of the primary key columns is not in the output columns.
    pub fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        assert!(IS_REPLICATED);
        self.pk_indices
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect()
    }

    pub fn pk_serde(&self) -> &OrderedRowSerde {
        &self.pk_serde
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        self.distribution.vnodes()
    }

    pub fn value_indices(&self) -> &Option<Vec<usize>> {
        &self.value_indices
    }

    fn is_dirty(&self) -> bool {
        self.local_store.is_dirty() || self.pending_watermark.is_some()
    }

    pub fn is_consistent_op(&self) -> bool {
        matches!(
            self.op_consistency_level,
            StateTableOpConsistencyLevel::ConsistentOldValue
                | StateTableOpConsistencyLevel::LogStoreEnabled
        )
    }
}

impl<S, SD, const USE_WATERMARK_CACHE: bool> StateTableInner<S, SD, true, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Create replicated state table from table catalog with output indices
    pub async fn from_table_catalog_with_output_column_ids(
        table_catalog: &Table,
        store: S,
        vnodes: Option<Arc<Bitmap>>,
        output_column_ids: Vec<ColumnId>,
    ) -> Self {
        Self::from_table_catalog_inner(
            table_catalog,
            store,
            vnodes,
            StateTableOpConsistencyLevel::Inconsistent,
            output_column_ids,
        )
        .await
    }
}

// point get
impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// Get a single row from state table.
    pub async fn get_row(&self, pk: impl Row) -> StreamExecutorResult<Option<OwnedRow>> {
        let encoded_row: Option<Bytes> = self.get_encoded_row(pk).await?;
        match encoded_row {
            Some(encoded_row) => {
                let row = self.row_serde.deserialize(&encoded_row)?;
                if IS_REPLICATED {
                    // If the table is replicated, we need to deserialize the row with the output
                    // indices.
                    let row = row.project(&self.output_indices);
                    Ok(Some(row.into_owned_row()))
                } else {
                    Ok(Some(OwnedRow::new(row)))
                }
            }
            None => Ok(None),
        }
    }

    /// Get a raw encoded row from state table.
    pub async fn get_encoded_row(&self, pk: impl Row) -> StreamExecutorResult<Option<Bytes>> {
        assert!(pk.len() <= self.pk_indices.len());

        let serialized_pk =
            serialize_pk_with_vnode(&pk, &self.pk_serde, self.compute_vnode_by_pk(&pk));

        let prefix_hint = if self.prefix_hint_len != 0 && self.prefix_hint_len == pk.len() {
            Some(serialized_pk.slice(VirtualNode::SIZE..))
        } else {
            #[cfg(debug_assertions)]
            if self.prefix_hint_len != 0 {
                warn!("prefix_hint_len is not equal to pk.len(), may not be able to utilize bloom filter");
            }
            None
        };

        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            cache_policy: CachePolicy::Fill(CacheHint::Normal),
            ..Default::default()
        };

        self.local_store
            .get(serialized_pk, read_options)
            .await
            .map_err(Into::into)
    }
}

/// A callback struct returned from [`StateTableInner::commit`].
///
/// Introduced to support single barrier configuration change proposed in <https://github.com/risingwavelabs/risingwave/issues/18312>.
/// In brief, to correctly handle the configuration change, when each stateful executor receives an upstream barrier, it should handle
/// the barrier in the order of `state_table.commit()` -> `yield barrier` -> `update_vnode_bitmap`.
///
/// The `StateTablePostCommit` captures the mutable reference of `state_table` when calling `state_table.commit()`, and after the executor
/// runs `yield barrier`, it should call `StateTablePostCommit::post_yield_barrier` to apply the vnode bitmap update if there is any.
/// The `StateTablePostCommit` is marked with `must_use`. The method name `post_yield_barrier` indicates that it should be called after
/// we have yielded the barrier. In `StateTable`, we add a flag `on_post_commit`, to indicate that whether the `StateTablePostCommit` is handled
/// properly. On `state_table.commit()`, we will mark the `on_post_commit` as true, and in `StateTablePostCommit::post_yield_barrier`, we will
/// remark the flag as false, and on `state_table.commit()`, we will assert that the `on_post_commit` must be false. Note that, the `post_yield_barrier`
/// should be called for all barriers rather than only for the barrier with update vnode bitmap. In this way, though we don't have scale test for all
/// streaming executor, we can ensure that all executor covered by normal e2e test have properly handled the `StateTablePostCommit`.
#[must_use]
pub struct StateTablePostCommit<
    'a,
    S,
    SD = BasicSerde,
    const IS_REPLICATED: bool = false,
    const USE_WATERMARK_CACHE: bool = false,
> where
    S: StateStore,
    SD: ValueRowSerde,
{
    inner: &'a mut StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>,
    barrier_epoch: EpochPair,
}

impl<'a, S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTablePostCommit<'a, S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    pub async fn post_yield_barrier(
        mut self,
        new_vnodes: Option<Arc<Bitmap>>,
    ) -> StreamExecutorResult<
        Option<(
            (
                Arc<Bitmap>,
                Arc<Bitmap>,
                &'a mut StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>,
            ),
            bool,
        )>,
    > {
        self.inner.on_post_commit = false;
        Ok(if let Some(new_vnodes) = new_vnodes {
            self.inner
                .try_wait_committed_epoch(self.barrier_epoch.prev)
                .await?;
            let (old_vnodes, cache_may_stale) = self.update_vnode_bitmap(new_vnodes.clone());
            Some(((new_vnodes, old_vnodes, self.inner), cache_may_stale))
        } else {
            None
        })
    }

    pub fn inner(&self) -> &StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE> {
        &*self.inner
    }

    /// Update the vnode bitmap of the state table, returns the previous vnode bitmap.
    fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> (Arc<Bitmap>, bool) {
        assert!(
            !self.inner.is_dirty(),
            "vnode bitmap should only be updated when state table is clean"
        );
        let prev_vnodes = self
            .inner
            .local_store
            .update_vnode_bitmap(new_vnodes.clone());
        assert_eq!(
            &prev_vnodes,
            self.inner.vnodes(),
            "state table and state store vnode bitmap mismatches"
        );

        if self.inner.distribution.is_singleton() {
            assert_eq!(
                &new_vnodes,
                self.inner.vnodes(),
                "should not update vnode bitmap for singleton table"
            );
        }
        assert_eq!(self.inner.vnodes().len(), new_vnodes.len());

        let cache_may_stale = cache_may_stale(self.inner.vnodes(), &new_vnodes);

        if cache_may_stale {
            self.inner.pending_watermark = None;
            if USE_WATERMARK_CACHE {
                self.inner.watermark_cache.clear();
            }
        }

        (
            self.inner.distribution.update_vnode_bitmap(new_vnodes),
            cache_may_stale,
        )
    }
}

// write
impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    fn handle_mem_table_error(&self, e: StorageError) {
        let e = match e.into_inner() {
            ErrorKind::MemTable(e) => e,
            _ => unreachable!("should only get memtable error"),
        };
        match *e {
            MemTableError::InconsistentOperation { key, prev, new } => {
                let (vnode, key) = deserialize_pk_with_vnode(&key, &self.pk_serde).unwrap();
                panic!(
                    "mem-table operation inconsistent! table_id: {}, vnode: {}, key: {:?}, prev: {}, new: {}",
                    self.table_id(),
                    vnode,
                    &key,
                    prev.debug_fmt(&self.row_serde),
                    new.debug_fmt(&self.row_serde),
                )
            }
        }
    }

    fn serialize_value(&self, value: impl Row) -> Bytes {
        if let Some(value_indices) = self.value_indices.as_ref() {
            self.row_serde
                .serialize(value.project(value_indices))
                .into()
        } else {
            self.row_serde.serialize(value).into()
        }
    }

    fn insert_inner(&mut self, key: TableKey<Bytes>, value_bytes: Bytes) {
        insane_mode_discard_point!();
        self.local_store
            .insert(key, value_bytes, None)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    fn delete_inner(&mut self, key: TableKey<Bytes>, value_bytes: Bytes) {
        insane_mode_discard_point!();
        self.local_store
            .delete(key, value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    fn update_inner(
        &mut self,
        key_bytes: TableKey<Bytes>,
        old_value_bytes: Option<Bytes>,
        new_value_bytes: Bytes,
    ) {
        insane_mode_discard_point!();
        self.local_store
            .insert(key_bytes, new_value_bytes, old_value_bytes)
            .unwrap_or_else(|e| self.handle_mem_table_error(e));
    }

    /// Insert a row into state table. Must provide a full row corresponding to the column desc of
    /// the table.
    pub fn insert(&mut self, value: impl Row) {
        let pk_indices = &self.pk_indices;
        let pk = (&value).project(pk_indices);
        if USE_WATERMARK_CACHE {
            self.watermark_cache.insert(&pk);
        }

        let key_bytes = serialize_pk_with_vnode(pk, &self.pk_serde, self.compute_vnode_by_pk(pk));
        let value_bytes = self.serialize_value(value);
        self.insert_inner(key_bytes, value_bytes);
    }

    /// Delete a row from state table. Must provide a full row of old value corresponding to the
    /// column desc of the table.
    pub fn delete(&mut self, old_value: impl Row) {
        let pk_indices = &self.pk_indices;
        let pk = (&old_value).project(pk_indices);
        if USE_WATERMARK_CACHE {
            self.watermark_cache.delete(&pk);
        }

        let key_bytes = serialize_pk_with_vnode(pk, &self.pk_serde, self.compute_vnode_by_pk(pk));
        let value_bytes = self.serialize_value(old_value);
        self.delete_inner(key_bytes, value_bytes);
    }

    /// Update a row. The old and new value should have the same pk.
    pub fn update(&mut self, old_value: impl Row, new_value: impl Row) {
        let old_pk = (&old_value).project(self.pk_indices());
        let new_pk = (&new_value).project(self.pk_indices());
        debug_assert!(
            Row::eq(&old_pk, new_pk),
            "pk should not change: {old_pk:?} vs {new_pk:?}",
        );

        let new_key_bytes =
            serialize_pk_with_vnode(new_pk, &self.pk_serde, self.compute_vnode_by_pk(new_pk));
        let old_value_bytes = self.serialize_value(old_value);
        let new_value_bytes = self.serialize_value(new_value);

        self.update_inner(new_key_bytes, Some(old_value_bytes), new_value_bytes);
    }

    /// Write a record into state table. Must have the same schema with the table.
    pub fn write_record(&mut self, record: Record<impl Row>) {
        match record {
            Record::Insert { new_row } => self.insert(new_row),
            Record::Delete { old_row } => self.delete(old_row),
            Record::Update { old_row, new_row } => self.update(old_row, new_row),
        }
    }

    fn fill_non_output_indices(&self, chunk: StreamChunk) -> StreamChunk {
        fill_non_output_indices(&self.i2o_mapping, &self.data_types, chunk)
    }

    /// Write batch with a `StreamChunk` which should have the same schema with the table.
    // allow(izip, which use zip instead of zip_eq)
    #[allow(clippy::disallowed_methods)]
    pub fn write_chunk(&mut self, chunk: StreamChunk) {
        let chunk = if IS_REPLICATED {
            self.fill_non_output_indices(chunk)
        } else {
            chunk
        };
        let (chunk, op) = chunk.into_parts();

        let vnodes = self
            .distribution
            .compute_chunk_vnode(&chunk, &self.pk_indices);

        let values = if let Some(ref value_indices) = self.value_indices {
            chunk.project(value_indices).serialize_with(&self.row_serde)
        } else {
            chunk.serialize_with(&self.row_serde)
        };

        // TODO(kwannoel): Seems like we are doing vis check twice here.
        // Once below, when using vis, and once here,
        // when using vis to set rows empty or not.
        // If we are to use the vis optimization, we should skip this.
        let key_chunk = chunk.project(self.pk_indices());
        let vnode_and_pks = key_chunk
            .rows_with_holes()
            .zip_eq_fast(vnodes.iter())
            .map(|(r, vnode)| {
                let mut buffer = BytesMut::new();
                buffer.put_slice(&vnode.to_be_bytes()[..]);
                if let Some(r) = r {
                    self.pk_serde.serialize(r, &mut buffer);
                }
                (r, buffer.freeze())
            })
            .collect_vec();

        if !key_chunk.is_compacted() {
            for ((op, (key, key_bytes), value), vis) in
                izip!(op.iter(), vnode_and_pks, values).zip_eq_debug(key_chunk.visibility().iter())
            {
                if vis {
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            if USE_WATERMARK_CACHE && let Some(ref pk) = key {
                                self.watermark_cache.insert(pk);
                            }
                            self.insert_inner(TableKey(key_bytes), value);
                        }
                        Op::Delete | Op::UpdateDelete => {
                            if USE_WATERMARK_CACHE && let Some(ref pk) = key {
                                self.watermark_cache.delete(pk);
                            }
                            self.delete_inner(TableKey(key_bytes), value);
                        }
                    }
                }
            }
        } else {
            for (op, (key, key_bytes), value) in izip!(op.iter(), vnode_and_pks, values) {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if USE_WATERMARK_CACHE && let Some(ref pk) = key {
                            self.watermark_cache.insert(pk);
                        }
                        self.insert_inner(TableKey(key_bytes), value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        if USE_WATERMARK_CACHE && let Some(ref pk) = key {
                            self.watermark_cache.delete(pk);
                        }
                        self.delete_inner(TableKey(key_bytes), value);
                    }
                }
            }
        }
    }

    /// Update watermark for state cleaning.
    ///
    /// # Arguments
    ///
    /// * `watermark` - Latest watermark received.
    pub fn update_watermark(&mut self, watermark: ScalarImpl) {
        trace!(table_id = %self.table_id, watermark = ?watermark, "update watermark");
        self.pending_watermark = Some(watermark);
    }

    /// Get the committed watermark of the state table. Watermarks should be fed into the state
    /// table through `update_watermark` method.
    pub fn get_committed_watermark(&self) -> Option<&ScalarImpl> {
        self.committed_watermark.as_ref()
    }

    pub async fn commit(
        &mut self,
        new_epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>>
    {
        self.commit_inner(new_epoch, None).await
    }

    #[cfg(test)]
    pub async fn commit_for_test(&mut self, new_epoch: EpochPair) -> StreamExecutorResult<()> {
        self.commit_assert_no_update_vnode_bitmap(new_epoch).await
    }

    pub async fn commit_assert_no_update_vnode_bitmap(
        &mut self,
        new_epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        let post_commit = self.commit_inner(new_epoch, None).await?;
        post_commit.post_yield_barrier(None).await?;
        Ok(())
    }

    pub async fn commit_may_switch_consistent_op(
        &mut self,
        new_epoch: EpochPair,
        op_consistency_level: StateTableOpConsistencyLevel,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>>
    {
        if self.op_consistency_level != op_consistency_level {
            info!(
                ?new_epoch,
                prev_op_consistency_level = ?self.op_consistency_level,
                ?op_consistency_level,
                table_id = self.table_id.table_id,
                "switch to new op consistency level"
            );
            self.commit_inner(new_epoch, Some(op_consistency_level))
                .await
        } else {
            self.commit_inner(new_epoch, None).await
        }
    }

    async fn commit_inner(
        &mut self,
        new_epoch: EpochPair,
        switch_consistent_op: Option<StateTableOpConsistencyLevel>,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>>
    {
        assert!(!self.on_post_commit);
        assert_eq!(self.epoch(), new_epoch.prev);
        let switch_op_consistency_level = switch_consistent_op.map(|new_consistency_level| {
            assert_ne!(self.op_consistency_level, new_consistency_level);
            self.op_consistency_level = new_consistency_level;
            match new_consistency_level {
                StateTableOpConsistencyLevel::Inconsistent => OpConsistencyLevel::Inconsistent,
                StateTableOpConsistencyLevel::ConsistentOldValue => {
                    consistent_old_value_op(self.row_serde.clone(), false)
                }
                StateTableOpConsistencyLevel::LogStoreEnabled => {
                    consistent_old_value_op(self.row_serde.clone(), true)
                }
            }
        });
        trace!(
            table_id = %self.table_id,
            epoch = ?self.epoch(),
            "commit state table"
        );

        let mut table_watermarks = None;
        if self.is_dirty() {
            self.local_store
                .flush()
                .instrument(tracing::info_span!("state_table_flush"))
                .await?;
            table_watermarks = self.commit_pending_watermark();
        }
        self.local_store.seal_current_epoch(
            new_epoch.curr,
            SealCurrentEpochOptions {
                table_watermarks,
                switch_op_consistency_level,
            },
        );

        // Refresh watermark cache if it is out of sync.
        if USE_WATERMARK_CACHE && !self.watermark_cache.is_synced() {
            if let Some(ref watermark) = self.committed_watermark {
                let range: (Bound<Once<Datum>>, Bound<Once<Datum>>) =
                    (Included(once(Some(watermark.clone()))), Unbounded);
                // NOTE(kwannoel): We buffer `pks` before inserting into watermark cache
                // because we can't hold an immutable ref (via `iter_key_and_val_with_pk_range`)
                // and a mutable ref (via `self.watermark_cache.insert`) at the same time.
                // TODO(kwannoel): We can optimize it with:
                // 1. Either use `RefCell`.
                // 2. Or pass in a direct reference to LocalStateStore,
                //    instead of referencing it indirectly from `self`.
                //    Similar to how we do for pk_indices.
                let mut pks = Vec::with_capacity(self.watermark_cache.capacity());
                {
                    let mut streams = vec![];
                    for vnode in self.vnodes().iter_vnodes() {
                        let stream = self
                            .iter_keyed_row_with_vnode(vnode, &range, PrefetchOptions::default())
                            .await?;
                        streams.push(Box::pin(stream));
                    }
                    let merged_stream = merge_sort(streams);
                    pin_mut!(merged_stream);

                    #[for_await]
                    for entry in merged_stream.take(self.watermark_cache.capacity()) {
                        let keyed_row = entry?;
                        let pk = self.pk_serde.deserialize(keyed_row.key())?;
                        // watermark column should be part of the pk
                        if !pk.is_null_at(self.clean_watermark_index_in_pk.unwrap_or(0) as usize) {
                            pks.push(pk);
                        }
                    }
                }

                let mut filler = self.watermark_cache.begin_syncing();
                for pk in pks {
                    filler.insert_unchecked(DefaultOrdered(pk), ());
                }
                filler.finish();

                let n_cache_entries = self.watermark_cache.len();
                if n_cache_entries < self.watermark_cache.capacity() {
                    self.watermark_cache.set_table_row_count(n_cache_entries);
                }
            }
        }

        self.on_post_commit = true;
        Ok(StateTablePostCommit {
            inner: self,
            barrier_epoch: new_epoch,
        })
    }

    /// Commit pending watermark and return vnode bitmap-watermark pairs to seal.
    fn commit_pending_watermark(
        &mut self,
    ) -> Option<(WatermarkDirection, Vec<VnodeWatermark>, WatermarkSerdeType)> {
        let watermark = self.pending_watermark.take();
        watermark.as_ref().inspect(|watermark| {
            trace!(table_id = %self.table_id, watermark = ?watermark, "state cleaning");
        });

        let watermark_serializer = if self.pk_indices().is_empty() {
            None
        } else {
            match self.clean_watermark_index_in_pk {
                None => Some(self.pk_serde.index(0)),
                Some(clean_watermark_index_in_pk) => {
                    Some(self.pk_serde.index(clean_watermark_index_in_pk as usize))
                }
            }
        };

        let watermark_type = match self.clean_watermark_index_in_pk {
            None => WatermarkSerdeType::PkPrefix,
            Some(clean_watermark_index_in_pk) => match clean_watermark_index_in_pk {
                0 => WatermarkSerdeType::PkPrefix,
                _ => WatermarkSerdeType::NonPkPrefix,
            },
        };

        let should_clean_watermark = match watermark {
            Some(ref watermark) => {
                if USE_WATERMARK_CACHE && self.watermark_cache.is_synced() {
                    if let Some(key) = self.watermark_cache.lowest_key() {
                        watermark.as_scalar_ref_impl().default_cmp(&key).is_ge()
                    } else {
                        // Watermark cache is synced,
                        // And there's no key in watermark cache.
                        // That implies table is empty.
                        // We should not clean watermark.
                        false
                    }
                } else {
                    // Either we are not using watermark cache,
                    // Or watermark_cache is not synced.
                    // In either case we should clean watermark.
                    true
                }
            }
            None => false,
        };

        let watermark_suffix = watermark.as_ref().map(|watermark| {
            serialize_pk(
                row::once(Some(watermark.clone())),
                watermark_serializer.as_ref().unwrap(),
            )
        });

        let mut seal_watermark: Option<(WatermarkDirection, VnodeWatermark, WatermarkSerdeType)> =
            None;

        // Compute Delete Ranges
        if should_clean_watermark && let Some(watermark_suffix) = watermark_suffix {
            trace!(table_id = %self.table_id, watermark = ?watermark_suffix, vnodes = ?{
                self.vnodes().iter_vnodes().collect_vec()
            }, "delete range");

            let order_type = watermark_serializer
                .as_ref()
                .unwrap()
                .get_order_types()
                .get(0)
                .unwrap();

            if order_type.is_ascending() {
                seal_watermark = Some((
                    WatermarkDirection::Ascending,
                    VnodeWatermark::new(
                        self.vnodes().clone(),
                        Bytes::copy_from_slice(watermark_suffix.as_ref()),
                    ),
                    watermark_type,
                ));
            } else {
                seal_watermark = Some((
                    WatermarkDirection::Descending,
                    VnodeWatermark::new(
                        self.vnodes().clone(),
                        Bytes::copy_from_slice(watermark_suffix.as_ref()),
                    ),
                    watermark_type,
                ));
            }
        }
        self.committed_watermark = watermark;

        // Clear the watermark cache and force a resync.
        // TODO(kwannoel): This can be further optimized:
        // 1. Add a `cache.drain_until` interface, so we only clear the watermark cache
        //    up to the largest end of delete ranges.
        // 2. Mark the cache as not_synced, so we can still refill it later.
        // 3. When refilling the cache,
        //    we just refill from the largest value of the cache, as the lower bound.
        if USE_WATERMARK_CACHE && seal_watermark.is_some() {
            self.watermark_cache.clear();
        }

        seal_watermark.map(|(direction, watermark, is_non_pk_prefix)| {
            (direction, vec![watermark], is_non_pk_prefix)
        })
    }

    pub async fn try_flush(&mut self) -> StreamExecutorResult<()> {
        self.local_store.try_flush().await?;
        Ok(())
    }
}

pub trait RowStream<'a> = Stream<Item = StreamExecutorResult<OwnedRow>> + 'a;
pub trait KeyedRowStream<'a> = Stream<Item = StreamExecutorResult<KeyedRow<Bytes>>> + 'a;
pub trait PkRowStream<'a, K> = Stream<Item = StreamExecutorResult<(K, OwnedRow)>> + 'a;

// Iterator functions
impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    /// This function scans rows from the relational table with specific `pk_range` under the same
    /// `vnode`.
    pub async fn iter_with_vnode(
        &self,

        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTableInner` owns.
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        Ok(deserialize_keyed_row_stream::<'_, ()>(
            self.iter_kv_with_pk_range(pk_range, vnode, prefetch_options)
                .await?,
            &self.row_serde,
        )
        .map_ok(|(_, row)| row))
    }

    pub async fn iter_keyed_row_with_vnode(
        &self,
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl KeyedRowStream<'_>> {
        Ok(deserialize_keyed_row_stream(
            self.iter_kv_with_pk_range(pk_range, vnode, prefetch_options)
                .await?,
            &self.row_serde,
        )
        .map_ok(|(key, row)| KeyedRow::new(TableKey(key), row)))
    }

    pub async fn iter_with_vnode_and_output_indices(
        &self,
        vnode: VirtualNode,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        assert!(IS_REPLICATED);
        let stream = self
            .iter_with_vnode(vnode, pk_range, prefetch_options)
            .await?;
        Ok(stream.map(|row| row.map(|row| row.project(&self.output_indices).into_owned_row())))
    }

    async fn iter_kv(
        &self,
        table_key_range: TableKeyRange,
        prefix_hint: Option<Bytes>,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<<S::Local as LocalStateStore>::Iter<'_>> {
        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            prefetch_options,
            cache_policy: CachePolicy::Fill(CacheHint::Normal),
            ..Default::default()
        };

        Ok(self.local_store.iter(table_key_range, read_options).await?)
    }

    async fn rev_iter_kv(
        &self,
        table_key_range: TableKeyRange,
        prefix_hint: Option<Bytes>,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<<S::Local as LocalStateStore>::RevIter<'_>> {
        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            prefetch_options,
            cache_policy: CachePolicy::Fill(CacheHint::Normal),
            ..Default::default()
        };

        Ok(self
            .local_store
            .rev_iter(table_key_range, read_options)
            .await?)
    }

    /// This function scans rows from the relational table with specific `prefix` and `sub_range` under the same
    /// `vnode`. If `sub_range` is (Unbounded, Unbounded), it scans rows from the relational table with specific `pk_prefix`.
    /// `pk_prefix` is used to identify the exact vnode the scan should perform on.
    pub async fn iter_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        let stream = self.iter_with_prefix_inner::</* REVERSE */ false, ()>(pk_prefix, sub_range, prefetch_options)
            .await?;
        Ok(stream.map_ok(|(_, row)| row))
    }

    pub async fn iter_keyed_row_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl KeyedRowStream<'_>> {
        Ok(
            self.iter_with_prefix_inner::</* REVERSE */ false, Bytes>(pk_prefix, sub_range, prefetch_options)
            .await?.map_ok(|(key, row)| KeyedRow::new(TableKey(key), row)),
        )
    }

    /// This function scans the table just like `iter_with_prefix`, but in reverse order.
    pub async fn rev_iter_with_prefix(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl RowStream<'_>> {
        Ok(
            self.iter_with_prefix_inner::</* REVERSE */ true, ()>(pk_prefix, sub_range, prefetch_options)
            .await?.map_ok(|(_, row)| row),
        )
    }

    async fn iter_with_prefix_inner<const REVERSE: bool, K: CopyFromSlice>(
        &self,
        pk_prefix: impl Row,
        sub_range: &(Bound<impl Row>, Bound<impl Row>),
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<impl PkRowStream<'_, K>> {
        let prefix_serializer = self.pk_serde.prefix(pk_prefix.len());
        let encoded_prefix = serialize_pk(&pk_prefix, &prefix_serializer);

        // We assume that all usages of iterating the state table only access a single vnode.
        // If this assertion fails, then something must be wrong with the operator implementation or
        // the distribution derivation from the optimizer.
        let vnode = self.compute_prefix_vnode(&pk_prefix);

        // Construct prefix hint for prefix bloom filter.
        let pk_prefix_indices = &self.pk_indices[..pk_prefix.len()];
        if self.prefix_hint_len != 0 {
            debug_assert_eq!(self.prefix_hint_len, pk_prefix.len());
        }
        let prefix_hint = {
            if self.prefix_hint_len == 0 || self.prefix_hint_len > pk_prefix.len() {
                None
            } else {
                let encoded_prefix_len = self
                    .pk_serde
                    .deserialize_prefix_len(&encoded_prefix, self.prefix_hint_len)?;

                Some(Bytes::copy_from_slice(
                    &encoded_prefix[..encoded_prefix_len],
                ))
            }
        };

        trace!(
            table_id = %self.table_id(),
            ?prefix_hint, ?pk_prefix,
            ?pk_prefix_indices,
            iter_direction = if REVERSE { "reverse" } else { "forward" },
            "storage_iter_with_prefix"
        );

        let memcomparable_range =
            prefix_and_sub_range_to_memcomparable(&self.pk_serde, sub_range, pk_prefix);

        let memcomparable_range_with_vnode = prefixed_range_with_vnode(memcomparable_range, vnode);

        Ok(if REVERSE {
            futures::future::Either::Left(deserialize_keyed_row_stream(
                self.rev_iter_kv(
                    memcomparable_range_with_vnode,
                    prefix_hint,
                    prefetch_options,
                )
                .await?,
                &self.row_serde,
            ))
        } else {
            futures::future::Either::Right(deserialize_keyed_row_stream(
                self.iter_kv(
                    memcomparable_range_with_vnode,
                    prefix_hint,
                    prefetch_options,
                )
                .await?,
                &self.row_serde,
            ))
        })
    }

    /// This function scans raw key-values from the relational table with specific `pk_range` under
    /// the same `vnode`.
    async fn iter_kv_with_pk_range(
        &self,
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
        // Optional vnode that returns an iterator only over the given range under that vnode.
        // For now, we require this parameter, and will panic. In the future, when `None`, we can
        // iterate over each vnode that the `StateTableInner` owns.
        vnode: VirtualNode,
        prefetch_options: PrefetchOptions,
    ) -> StreamExecutorResult<<S::Local as LocalStateStore>::Iter<'_>> {
        let memcomparable_range = prefix_range_to_memcomparable(&self.pk_serde, pk_range);
        let memcomparable_range_with_vnode = prefixed_range_with_vnode(memcomparable_range, vnode);

        // TODO: provide a trace of useful params.
        self.iter_kv(memcomparable_range_with_vnode, None, prefetch_options)
            .await
            .map_err(StreamExecutorError::from)
    }

    #[cfg(test)]
    pub fn get_watermark_cache(&self) -> &StateTableWatermarkCache {
        &self.watermark_cache
    }
}

impl<S, SD, const IS_REPLICATED: bool, const USE_WATERMARK_CACHE: bool>
    StateTableInner<S, SD, IS_REPLICATED, USE_WATERMARK_CACHE>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    pub async fn iter_log_with_vnode(
        &self,
        vnode: VirtualNode,
        epoch_range: (u64, u64),
        pk_range: &(Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<impl Stream<Item = StreamExecutorResult<ChangeLogRow>> + '_> {
        let memcomparable_range = prefix_range_to_memcomparable(&self.pk_serde, pk_range);
        let memcomparable_range_with_vnode = prefixed_range_with_vnode(memcomparable_range, vnode);
        Ok(deserialize_log_stream(
            self.store
                .iter_log(
                    epoch_range,
                    memcomparable_range_with_vnode,
                    ReadLogOptions {
                        table_id: self.table_id,
                    },
                )
                .await?,
            &self.row_serde,
        )
        .map_err(Into::into))
    }
}

fn deserialize_keyed_row_stream<'a, K: CopyFromSlice>(
    iter: impl StateStoreIter + 'a,
    deserializer: &'a impl ValueRowSerde,
) -> impl PkRowStream<'a, K> {
    iter.into_stream(move |(key, value)| {
        Ok((
            K::copy_from_slice(key.user_key.table_key.as_ref()),
            deserializer.deserialize(value).map(OwnedRow::new)?,
        ))
    })
    .map_err(Into::into)
}

pub fn prefix_range_to_memcomparable(
    pk_serde: &OrderedRowSerde,
    range: &(Bound<impl Row>, Bound<impl Row>),
) -> (Bound<Bytes>, Bound<Bytes>) {
    (
        start_range_to_memcomparable(pk_serde, &range.0),
        end_range_to_memcomparable(pk_serde, &range.1, None),
    )
}

fn prefix_and_sub_range_to_memcomparable(
    pk_serde: &OrderedRowSerde,
    sub_range: &(Bound<impl Row>, Bound<impl Row>),
    pk_prefix: impl Row,
) -> (Bound<Bytes>, Bound<Bytes>) {
    let (range_start, range_end) = sub_range;
    let prefix_serializer = pk_serde.prefix(pk_prefix.len());
    let serialized_pk_prefix = serialize_pk(&pk_prefix, &prefix_serializer);
    let start_range = match range_start {
        Included(start_range) => Bound::Included(Either::Left((&pk_prefix).chain(start_range))),
        Excluded(start_range) => Bound::Excluded(Either::Left((&pk_prefix).chain(start_range))),
        Unbounded => Bound::Included(Either::Right(&pk_prefix)),
    };
    let end_range = match range_end {
        Included(end_range) => Bound::Included((&pk_prefix).chain(end_range)),
        Excluded(end_range) => Bound::Excluded((&pk_prefix).chain(end_range)),
        Unbounded => Unbounded,
    };
    (
        start_range_to_memcomparable(pk_serde, &start_range),
        end_range_to_memcomparable(pk_serde, &end_range, Some(serialized_pk_prefix)),
    )
}

fn start_range_to_memcomparable<R: Row>(
    pk_serde: &OrderedRowSerde,
    bound: &Bound<R>,
) -> Bound<Bytes> {
    let serialize_pk_prefix = |pk_prefix: &R| {
        let prefix_serializer = pk_serde.prefix(pk_prefix.len());
        serialize_pk(pk_prefix, &prefix_serializer)
    };
    match bound {
        Unbounded => Unbounded,
        Included(r) => {
            let serialized = serialize_pk_prefix(r);

            Included(serialized)
        }
        Excluded(r) => {
            let serialized = serialize_pk_prefix(r);

            start_bound_of_excluded_prefix(&serialized)
        }
    }
}

fn end_range_to_memcomparable<R: Row>(
    pk_serde: &OrderedRowSerde,
    bound: &Bound<R>,
    serialized_pk_prefix: Option<Bytes>,
) -> Bound<Bytes> {
    let serialize_pk_prefix = |pk_prefix: &R| {
        let prefix_serializer = pk_serde.prefix(pk_prefix.len());
        serialize_pk(pk_prefix, &prefix_serializer)
    };
    match bound {
        Unbounded => match serialized_pk_prefix {
            Some(serialized_pk_prefix) => end_bound_of_prefix(&serialized_pk_prefix),
            None => Unbounded,
        },
        Included(r) => {
            let serialized = serialize_pk_prefix(r);

            end_bound_of_prefix(&serialized)
        }
        Excluded(r) => {
            let serialized = serialize_pk_prefix(r);
            Excluded(serialized)
        }
    }
}

fn fill_non_output_indices(
    i2o_mapping: &ColIndexMapping,
    data_types: &[DataType],
    chunk: StreamChunk,
) -> StreamChunk {
    let cardinality = chunk.cardinality();
    let (ops, columns, vis) = chunk.into_inner();
    let mut full_columns = Vec::with_capacity(data_types.len());
    for (i, data_type) in data_types.iter().enumerate() {
        if let Some(j) = i2o_mapping.try_map(i) {
            full_columns.push(columns[j].clone());
        } else {
            let mut column_builder = ArrayImplBuilder::with_type(cardinality, data_type.clone());
            column_builder.append_n_null(cardinality);
            let column: ArrayRef = column_builder.finish().into();
            full_columns.push(column)
        }
    }
    let data_chunk = DataChunk::new(full_columns, vis);
    StreamChunk::from_parts(ops, data_chunk)
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_fill_non_output_indices() {
        let data_types = vec![DataType::Int32, DataType::Int32, DataType::Int32];
        let replicated_chunk = [OwnedRow::new(vec![
            Some(222_i32.into()),
            Some(2_i32.into()),
        ])];
        let replicated_chunk = StreamChunk::from_parts(
            vec![Op::Insert],
            DataChunk::from_rows(&replicated_chunk, &[DataType::Int32, DataType::Int32]),
        );
        let i2o_mapping = ColIndexMapping::new(vec![Some(1), None, Some(0)], 2);
        let filled_chunk = fill_non_output_indices(&i2o_mapping, &data_types, replicated_chunk);
        check(
            filled_chunk,
            expect![[r#"
            StreamChunk { cardinality: 1, capacity: 1, data:
            +---+---+---+-----+
            | + | 2 |   | 222 |
            +---+---+---+-----+
             }"#]],
        );
    }
}
