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

use std::future::Future;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::{Bytes, BytesMut};
use foyer::CacheHint;
use futures::future::try_join_all;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use more_asserts::assert_gt;
use risingwave_common::array::{ArrayBuilderImpl, ArrayRef, DataChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::ToOwnedDatum;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::row_serde::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{BasicSerde, EitherSerde};
use risingwave_hummock_sdk::key::{
    end_bound_of_prefix, next_key, prefixed_range_with_vnode, CopyFromSlice, TableKeyRange,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use tracing::trace;

use crate::error::{StorageError, StorageResult};
use crate::hummock::CachePolicy;
use crate::row_serde::row_serde_util::{serialize_pk, serialize_pk_with_vnode};
use crate::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use crate::row_serde::{find_columns_by_ids, ColumnMapping};
use crate::store::{
    NextEpochOptions, PrefetchOptions, ReadLogOptions, ReadOptions, StateStoreIter,
    StateStoreIterExt, TryWaitEpochOptions,
};
use crate::table::merge_sort::NodePeek;
use crate::table::{ChangeLogRow, KeyedRow, TableDistribution, TableIter};
use crate::StateStore;

/// [`StorageTableInner`] is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding format, and is used in batch mode.
#[derive(Clone)]
pub struct StorageTableInner<S: StateStore, SD: ValueRowSerde> {
    /// Id for this table.
    table_id: TableId,

    /// State store backend.
    store: S,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// `RowSeqScanExecutor`.
    schema: Schema,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: OrderedRowSerde,

    output_indices: Vec<usize>,

    /// the key part of `output_indices`.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of `output_indices`.
    value_output_indices: Vec<usize>,

    /// used for deserializing key part of output row from pk.
    output_row_in_key_indices: Vec<usize>,

    /// Mapping from column id to column index for deserializing the row.
    mapping: Arc<ColumnMapping>,

    /// The index of system column `_rw_timestamp` in the output columns.
    epoch_idx: Option<usize>,

    /// Row deserializer to deserialize the value in storage to a row.
    /// The row can be either complete or partial, depending on whether the row encoding is versioned.
    row_serde: Arc<SD>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    distribution: TableDistribution,

    /// Used for catalog `table_properties`
    table_option: TableOption,

    read_prefix_len_hint: usize,
}

/// `StorageTable` will use [`EitherSerde`] as default so that we can support both versioned and
/// non-versioned tables with the same type.
pub type StorageTable<S> = StorageTableInner<S, EitherSerde>;

impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for StorageTableInner<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTableInner").finish_non_exhaustive()
    }
}

// init
impl<S: StateStore> StorageTableInner<S, EitherSerde> {
    /// Create a  [`StorageTableInner`] given a complete set of `columns` and a partial
    /// set of `output_column_ids`.
    /// When reading from the storage table,
    /// the chunks or rows will only contain columns with the given ids (`output_column_ids`).
    /// They will in the same order as the given `output_column_ids`.
    ///
    /// NOTE(kwannoel): The `output_column_ids` here may be slightly different
    /// from those supplied to associated executors.
    /// These `output_column_ids` may have `pk` appended, since they will be needed to scan from
    /// storage. The associated executors may not have these `pk` fields.
    pub fn new_partial(
        store: S,
        output_column_ids: Vec<ColumnId>,
        vnodes: Option<Arc<Bitmap>>,
        table_desc: &StorageTableDesc,
    ) -> Self {
        let table_id = TableId {
            table_id: table_desc.table_id,
        };
        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let order_types: Vec<OrderType> = table_desc
            .pk
            .iter()
            .map(|order| OrderType::from_protobuf(order.get_order_type().unwrap()))
            .collect();

        let pk_indices = table_desc
            .pk
            .iter()
            .map(|k| k.column_index as usize)
            .collect_vec();

        let table_option = TableOption {
            retention_seconds: table_desc.retention_seconds,
        };
        let value_indices = table_desc
            .get_value_indices()
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let prefix_hint_len = table_desc.get_read_prefix_len_hint() as usize;
        let versioned = table_desc.versioned;
        let distribution = TableDistribution::new_from_storage_table_desc(vnodes, table_desc);

        Self::new_inner(
            store,
            table_id,
            column_descs,
            output_column_ids,
            order_types,
            pk_indices,
            distribution,
            table_option,
            value_indices,
            prefix_hint_len,
            versioned,
        )
    }

    pub fn for_test_with_partial_columns(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        output_column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        Self::new_inner(
            store,
            table_id,
            columns,
            output_column_ids,
            order_types,
            pk_indices,
            TableDistribution::singleton(),
            Default::default(),
            value_indices,
            0,
            false,
        )
    }

    pub fn for_test(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        let output_column_ids = columns.iter().map(|c| c.column_id).collect();
        Self::for_test_with_partial_columns(
            store,
            table_id,
            columns,
            output_column_ids,
            order_types,
            pk_indices,
            value_indices,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        output_column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: TableDistribution,
        table_option: TableOption,
        value_indices: Vec<usize>,
        read_prefix_len_hint: usize,
        versioned: bool,
    ) -> Self {
        assert_eq!(order_types.len(), pk_indices.len());

        let (output_columns, output_indices) =
            find_columns_by_ids(&table_columns, &output_column_ids);

        let mut value_output_indices = vec![];
        let mut key_output_indices = vec![];
        // system column currently only contains `_rw_timestamp`
        let mut epoch_idx = None;

        for idx in &output_indices {
            if value_indices.contains(idx) {
                value_output_indices.push(*idx);
            } else if pk_indices.contains(idx) {
                key_output_indices.push(*idx);
            } else {
                assert!(epoch_idx.is_none());
                epoch_idx = Some(*idx);
            }
        }

        let output_row_in_key_indices = key_output_indices
            .iter()
            .map(|&di| pk_indices.iter().position(|&pi| di == pi).unwrap())
            .collect_vec();
        let schema = Schema::new(output_columns.iter().map(Into::into).collect());

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serializer = OrderedRowSerde::new(pk_data_types, order_types);
        let (row_serde, mapping) = {
            if versioned {
                let value_output_indices_dedup = value_output_indices
                    .iter()
                    .unique()
                    .copied()
                    .collect::<Vec<_>>();
                let output_row_in_value_output_indices_dedup = value_output_indices
                    .iter()
                    .map(|&di| {
                        value_output_indices_dedup
                            .iter()
                            .position(|&pi| di == pi)
                            .unwrap()
                    })
                    .collect_vec();
                let mapping = ColumnMapping::new(output_row_in_value_output_indices_dedup);
                let serde =
                    ColumnAwareSerde::new(value_output_indices_dedup.into(), table_columns.into());
                (serde.into(), mapping)
            } else {
                let output_row_in_value_indices = value_output_indices
                    .iter()
                    .map(|&di| value_indices.iter().position(|&pi| di == pi).unwrap())
                    .collect_vec();
                let mapping = ColumnMapping::new(output_row_in_value_indices);
                let serde = BasicSerde::new(value_indices.into(), table_columns.into());
                (serde.into(), mapping)
            }
        };

        let key_output_indices = match key_output_indices.is_empty() {
            true => None,
            false => Some(key_output_indices),
        };
        Self {
            table_id,
            store,
            schema,
            pk_serializer,
            output_indices,
            key_output_indices,
            value_output_indices,
            output_row_in_key_indices,
            mapping: Arc::new(mapping),
            epoch_idx,
            row_serde: Arc::new(row_serde),
            pk_indices,
            distribution,
            table_option,
            read_prefix_len_hint,
        }
    }
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    pub fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn output_indices(&self) -> &[usize] {
        &self.output_indices
    }

    /// Get the indices of the primary key columns in the output columns.
    ///
    /// Returns `None` if any of the primary key columns is not in the output columns.
    pub fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        self.pk_indices
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect()
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        self.distribution.vnodes()
    }
}
/// Point get
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Get a single row by point get
    pub async fn get_row(
        &self,
        pk: impl Row,
        wait_epoch: HummockReadEpoch,
    ) -> StorageResult<Option<OwnedRow>> {
        let epoch = wait_epoch.get_epoch();
        let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
        let read_committed = wait_epoch.is_read_committed();
        self.store
            .try_wait_epoch(
                wait_epoch,
                TryWaitEpochOptions {
                    table_id: self.table_id,
                },
            )
            .await?;
        let serialized_pk = serialize_pk_with_vnode(
            &pk,
            &self.pk_serializer,
            self.distribution.compute_vnode_by_pk(&pk),
        );
        assert!(pk.len() <= self.pk_indices.len());

        let prefix_hint = if self.read_prefix_len_hint != 0 && self.read_prefix_len_hint == pk.len()
        {
            Some(serialized_pk.slice(VirtualNode::SIZE..))
        } else {
            None
        };

        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            read_version_from_backup: read_backup,
            read_committed,
            cache_policy: CachePolicy::Fill(CacheHint::Normal),
            ..Default::default()
        };
        if let Some((full_key, value)) = self
            .store
            .get_keyed_row(serialized_pk, epoch, read_options)
            .await?
        {
            let row = self.row_serde.deserialize(&value)?;
            let result_row_in_value = self.mapping.project(OwnedRow::new(row));

            match &self.key_output_indices {
                Some(key_output_indices) => {
                    let result_row_in_key =
                        pk.project(&self.output_row_in_key_indices).into_owned_row();
                    let mut result_row_vec = vec![];
                    for idx in &self.output_indices {
                        if let Some(epoch_idx) = self.epoch_idx
                            && *idx == epoch_idx
                        {
                            let epoch = Epoch::from(full_key.epoch_with_gap.pure_epoch());
                            result_row_vec
                                .push(risingwave_common::types::Datum::from(epoch.as_scalar()));
                        } else if self.value_output_indices.contains(idx) {
                            let item_position_in_value_indices = &self
                                .value_output_indices
                                .iter()
                                .position(|p| idx == p)
                                .unwrap();
                            result_row_vec.push(
                                result_row_in_value
                                    .datum_at(*item_position_in_value_indices)
                                    .to_owned_datum(),
                            );
                        } else {
                            let item_position_in_pk_indices =
                                key_output_indices.iter().position(|p| idx == p).unwrap();
                            result_row_vec.push(
                                result_row_in_key
                                    .datum_at(item_position_in_pk_indices)
                                    .to_owned_datum(),
                            );
                        }
                    }
                    let result_row = OwnedRow::new(result_row_vec);
                    Ok(Some(result_row))
                }
                None => match &self.epoch_idx {
                    Some(epoch_idx) => {
                        let mut result_row_vec = vec![];
                        for idx in &self.output_indices {
                            if idx == epoch_idx {
                                let epoch = Epoch::from(full_key.epoch_with_gap.pure_epoch());
                                result_row_vec
                                    .push(risingwave_common::types::Datum::from(epoch.as_scalar()));
                            } else {
                                let item_position_in_value_indices = &self
                                    .value_output_indices
                                    .iter()
                                    .position(|p| idx == p)
                                    .unwrap();
                                result_row_vec.push(
                                    result_row_in_value
                                        .datum_at(*item_position_in_value_indices)
                                        .to_owned_datum(),
                                );
                            }
                        }
                        let result_row = OwnedRow::new(result_row_vec);
                        Ok(Some(result_row))
                    }
                    None => Ok(Some(result_row_in_value.into_owned_row())),
                },
            }
        } else {
            Ok(None)
        }
    }

    /// Update the vnode bitmap of the storage table, returns the previous vnode bitmap.
    #[must_use = "the executor should decide whether to manipulate the cache based on the previous vnode bitmap"]
    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        self.distribution.update_vnode_bitmap(new_vnodes)
    }
}

/// The row iterator of the storage table.
/// The wrapper of stream item `StorageResult<OwnedRow>` if pk is not persisted.
impl<S: Stream<Item = StorageResult<OwnedRow>> + Send + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>> {
        self.next().await.transpose()
    }
}

mod merge_vnode_stream {

    use bytes::Bytes;
    use futures::{Stream, StreamExt, TryStreamExt};
    use risingwave_hummock_sdk::key::TableKey;

    use crate::error::StorageResult;
    use crate::table::merge_sort::{merge_sort, NodePeek};
    use crate::table::KeyedRow;

    pub(super) enum VnodeStreamType<RowSt, KeyedRowSt> {
        Single(RowSt),
        Unordered(Vec<RowSt>),
        Ordered(Vec<KeyedRowSt>),
    }

    pub(super) type MergedVnodeStream<
        R: Send,
        RowSt: Stream<Item = StorageResult<((), R)>> + Send,
        KeyedRowSt: Stream<Item = StorageResult<(SortKeyType, R)>> + Send,
    >
    where
        KeyedRow<SortKeyType, R>: NodePeek + Send + Sync,
    = impl Stream<Item = StorageResult<R>> + Send;

    pub(super) type SortKeyType = Bytes; // TODO: may use Vec

    pub(super) fn merge_stream<
        R: Send,
        RowSt: Stream<Item = StorageResult<((), R)>> + Send,
        KeyedRowSt: Stream<Item = StorageResult<(SortKeyType, R)>> + Send,
    >(
        stream: VnodeStreamType<RowSt, KeyedRowSt>,
    ) -> MergedVnodeStream<R, RowSt, KeyedRowSt>
    where
        KeyedRow<SortKeyType, R>: NodePeek + Send + Sync,
    {
        #[auto_enums::auto_enum(futures03::Stream)]
        match stream {
            VnodeStreamType::Single(stream) => stream.map_ok(|(_, row)| row),
            VnodeStreamType::Unordered(streams) => futures::stream::iter(
                streams
                    .into_iter()
                    .map(|stream| Box::pin(stream.map_ok(|(_, row)| row))),
            )
            .flatten_unordered(1024),
            VnodeStreamType::Ordered(streams) => merge_sort(streams.into_iter().map(|stream| {
                Box::pin(stream.map_ok(|(key, row)| KeyedRow {
                    vnode_prefixed_key: TableKey(key),
                    row,
                }))
            }))
            .map_ok(|keyed_row| keyed_row.row),
        }
    }
}

use merge_vnode_stream::*;

async fn build_vnode_stream<
    R: Send,
    RowSt: Stream<Item = StorageResult<((), R)>> + Send,
    KeyedRowSt: Stream<Item = StorageResult<(SortKeyType, R)>> + Send,
    RowStFut: Future<Output = StorageResult<RowSt>>,
    KeyedRowStFut: Future<Output = StorageResult<KeyedRowSt>>,
>(
    row_stream_fn: impl Fn(VirtualNode) -> RowStFut,
    keyed_row_stream_fn: impl Fn(VirtualNode) -> KeyedRowStFut,
    vnodes: &[VirtualNode],
    ordered: bool,
) -> StorageResult<MergedVnodeStream<R, RowSt, KeyedRowSt>>
where
    KeyedRow<SortKeyType, R>: NodePeek + Send + Sync,
{
    let stream = match vnodes {
        [] => unreachable!(),
        [vnode] => VnodeStreamType::Single(row_stream_fn(*vnode).await?),
        // Concat all iterators if not to preserve order.
        vnodes if !ordered => VnodeStreamType::Unordered(
            try_join_all(vnodes.iter().map(|vnode| row_stream_fn(*vnode))).await?,
        ),
        // Merge all iterators if to preserve order.
        vnodes => VnodeStreamType::Ordered(
            try_join_all(vnodes.iter().map(|vnode| keyed_row_stream_fn(*vnode))).await?,
        ),
    };
    Ok(merge_stream(stream))
}

/// Iterators
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Get multiple stream item `StorageResult<OwnedRow>` based on the specified vnodes of this table with
    /// `vnode_hint`, and merge or concat them by given `ordered`.
    async fn iter_with_encoded_key_range(
        &self,
        prefix_hint: Option<Bytes>,
        (start_bound, end_bound): (Bound<Bytes>, Bound<Bytes>),
        wait_epoch: HummockReadEpoch,
        vnode_hint: Option<VirtualNode>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<OwnedRow>> + Send + 'static> {
        let vnodes = match vnode_hint {
            // If `vnode_hint` is set, we can only access this single vnode.
            Some(vnode) => {
                assert!(
                    self.distribution.vnodes().is_set(vnode.to_index()),
                    "vnode unset: {:?}, distribution: {:?}",
                    vnode,
                    self.distribution
                );
                vec![vnode]
            }
            // Otherwise, we need to access all vnodes of this table.
            None => self.distribution.vnodes().iter_vnodes().collect_vec(),
        };

        build_vnode_stream(
            |vnode| {
                self.iter_vnode_with_encoded_key_range(
                    prefix_hint.clone(),
                    (start_bound.as_ref(), end_bound.as_ref()),
                    wait_epoch,
                    vnode,
                    prefetch_options,
                )
            },
            |vnode| {
                self.iter_vnode_with_encoded_key_range(
                    prefix_hint.clone(),
                    (start_bound.as_ref(), end_bound.as_ref()),
                    wait_epoch,
                    vnode,
                    prefetch_options,
                )
            },
            &vnodes,
            ordered,
        )
        .await
    }

    async fn iter_vnode_with_encoded_key_range<K: CopyFromSlice>(
        &self,
        prefix_hint: Option<Bytes>,
        encoded_key_range: (Bound<&Bytes>, Bound<&Bytes>),
        wait_epoch: HummockReadEpoch,
        vnode: VirtualNode,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<(K, OwnedRow)>> + Send> {
        let cache_policy = match &encoded_key_range {
            // To prevent unbounded range scan queries from polluting the block cache, use the
            // low priority fill policy.
            (Unbounded, _) | (_, Unbounded) => CachePolicy::Fill(CacheHint::Low),
            _ => CachePolicy::Fill(CacheHint::Normal),
        };

        let table_key_range = prefixed_range_with_vnode::<&Bytes>(encoded_key_range, vnode);

        {
            let prefix_hint = prefix_hint.clone();
            let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
            let read_committed = wait_epoch.is_read_committed();
            {
                let read_options = ReadOptions {
                    prefix_hint,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.table_id,
                    read_version_from_backup: read_backup,
                    read_committed,
                    prefetch_options,
                    cache_policy,
                };
                let pk_serializer = match self.output_row_in_key_indices.is_empty() {
                    true => None,
                    false => Some(Arc::new(self.pk_serializer.clone())),
                };
                let iter = StorageTableInnerIterInner::<S, SD>::new(
                    &self.store,
                    self.mapping.clone(),
                    self.epoch_idx,
                    pk_serializer,
                    self.output_indices.clone(),
                    self.key_output_indices.clone(),
                    self.value_output_indices.clone(),
                    self.output_row_in_key_indices.clone(),
                    self.row_serde.clone(),
                    table_key_range,
                    read_options,
                    wait_epoch,
                )
                .await?
                .into_stream::<K>();
                Ok(iter)
            }
        }
    }

    // TODO: directly use `prefixed_range`.
    fn serialize_pk_bound(
        &self,
        pk_prefix: impl Row,
        range_bound: Bound<&OwnedRow>,
        is_start_bound: bool,
    ) -> Bound<Bytes> {
        match range_bound {
            Included(k) => {
                let pk_prefix_serializer = self.pk_serializer.prefix(pk_prefix.len() + k.len());
                let key = pk_prefix.chain(k);
                let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                if is_start_bound {
                    Included(serialized_key)
                } else {
                    // Should use excluded next key for end bound.
                    // Otherwise keys starting with the bound is not included.
                    end_bound_of_prefix(&serialized_key)
                }
            }
            Excluded(k) => {
                let pk_prefix_serializer = self.pk_serializer.prefix(pk_prefix.len() + k.len());
                let key = pk_prefix.chain(k);
                let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                if is_start_bound {
                    // Storage doesn't support excluded begin key yet, so transform it to
                    // included.
                    // We always serialize a u8 for null of datum which is not equal to '\xff',
                    // so we can assert that the next_key would never be empty.
                    let next_serialized_key = next_key(&serialized_key);
                    assert!(!next_serialized_key.is_empty());
                    Included(Bytes::from(next_serialized_key))
                } else {
                    Excluded(serialized_key)
                }
            }
            Unbounded => {
                let pk_prefix_serializer = self.pk_serializer.prefix(pk_prefix.len());
                let serialized_pk_prefix = serialize_pk(&pk_prefix, &pk_prefix_serializer);
                if pk_prefix.is_empty() {
                    Unbounded
                } else if is_start_bound {
                    Included(serialized_pk_prefix)
                } else {
                    end_bound_of_prefix(&serialized_pk_prefix)
                }
            }
        }
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    async fn iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<OwnedRow>> + Send> {
        let start_key = self.serialize_pk_bound(&pk_prefix, range_bounds.start_bound(), true);
        let end_key = self.serialize_pk_bound(&pk_prefix, range_bounds.end_bound(), false);
        assert!(pk_prefix.len() <= self.pk_indices.len());
        let pk_prefix_indices = (0..pk_prefix.len())
            .map(|index| self.pk_indices[index])
            .collect_vec();

        let prefix_hint = if self.read_prefix_len_hint != 0
            && self.read_prefix_len_hint <= pk_prefix.len()
        {
            let encoded_prefix = if let Bound::Included(start_key) = start_key.as_ref() {
                start_key
            } else {
                unreachable!()
            };
            let prefix_len = self
                .pk_serializer
                .deserialize_prefix_len(encoded_prefix, self.read_prefix_len_hint)?;
            Some(Bytes::from(encoded_prefix[..prefix_len].to_vec()))
        } else {
            trace!(
                    "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?}  pk_prefix_indices {:?}",
                    self.table_id,
                    pk_prefix,
                    pk_prefix_indices
                );
            None
        };

        trace!(
            "iter_with_pk_bounds table_id {} prefix_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?}  pk_prefix_indices {:?}" ,
            self.table_id,
            prefix_hint,
            start_key,
            end_key,
            pk_prefix,
            pk_prefix_indices
        );

        self.iter_with_encoded_key_range(
            prefix_hint,
            (start_key, end_key),
            epoch,
            self.distribution.try_compute_vnode_by_pk_prefix(pk_prefix),
            ordered,
            prefetch_options,
        )
        .await
    }

    // Construct a stream of (columns, row_count) from a row stream
    #[try_stream(ok = (Vec<ArrayRef>, usize), error = StorageError)]
    async fn convert_row_stream_to_array_vec_stream(
        iter: impl Stream<Item = StorageResult<OwnedRow>>,
        schema: Schema,
        chunk_size: usize,
    ) {
        use futures::{pin_mut, TryStreamExt};
        use risingwave_common::util::iter_util::ZipEqFast;

        pin_mut!(iter);

        let mut builders: Option<Vec<ArrayBuilderImpl>> = None;
        let mut row_count = 0;

        while let Some(row) = iter.try_next().await? {
            row_count += 1;
            // Uses ArrayBuilderImpl instead of DataChunkBuilder here to demonstrate how to build chunk in a columnar manner
            let builders_ref =
                builders.get_or_insert_with(|| schema.create_array_builders(chunk_size));
            for (datum, builder) in row.iter().zip_eq_fast(builders_ref.iter_mut()) {
                builder.append(datum);
            }
            if row_count == chunk_size {
                let columns: Vec<_> = builders
                    .take()
                    .unwrap()
                    .into_iter()
                    .map(|builder| builder.finish().into())
                    .collect();
                yield (columns, row_count);
                assert!(builders.is_none());
                row_count = 0;
            }
        }

        if let Some(builders) = builders {
            assert_gt!(row_count, 0);
            // yield the last chunk if any
            let columns: Vec<_> = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();
            yield (columns, row_count);
        }
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    /// Returns a stream of chunks of columns with the provided `chunk_size`
    async fn chunk_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        chunk_size: usize,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<(Vec<ArrayRef>, usize)>> + Send> {
        let iter = self
            .iter_with_pk_bounds(epoch, pk_prefix, range_bounds, ordered, prefetch_options)
            .await?;

        Ok(Self::convert_row_stream_to_array_vec_stream(
            iter,
            self.schema.clone(),
            chunk_size,
        ))
    }

    /// Construct a stream item `StorageResult<OwnedRow>` for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<OwnedRow>> + Send> {
        self.iter_with_pk_bounds(epoch, pk_prefix, range_bounds, ordered, prefetch_options)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(
        &self,
        epoch: HummockReadEpoch,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<OwnedRow>> + Send> {
        self.batch_iter_with_pk_bounds(epoch, row::empty(), .., ordered, prefetch_options)
            .await
    }

    pub async fn batch_iter_vnode(
        &self,
        epoch: HummockReadEpoch,
        start_pk: Option<&OwnedRow>,
        vnode: VirtualNode,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<OwnedRow>> + Send + 'static> {
        let start_bound = if let Some(start_pk) = start_pk {
            let mut bytes = BytesMut::new();
            self.pk_serializer.serialize(start_pk, &mut bytes);
            let bytes = bytes.freeze();
            Included(bytes)
        } else {
            Unbounded
        };
        Ok(self
            .iter_vnode_with_encoded_key_range::<()>(
                None,
                (start_bound.as_ref(), Unbounded),
                epoch,
                vnode,
                prefetch_options,
            )
            .await?
            .map_ok(|(_, row)| row))
    }

    pub async fn next_epoch(&self, epoch: u64) -> StorageResult<u64> {
        self.store
            .next_epoch(
                epoch,
                NextEpochOptions {
                    table_id: self.table_id,
                },
            )
            .await
    }

    async fn batch_iter_log_inner<K: CopyFromSlice>(
        &self,
        start_epoch: u64,
        end_epoch: HummockReadEpoch,
        encoded_key_range: (Bound<&Bytes>, Bound<&Bytes>),
        vnode: VirtualNode,
    ) -> StorageResult<impl Stream<Item = StorageResult<(K, ChangeLogRow)>>> {
        let table_key_range = prefixed_range_with_vnode::<&Bytes>(encoded_key_range, vnode);
        let read_options = ReadLogOptions {
            table_id: self.table_id,
        };
        let iter = StorageTableInnerIterLogInner::<S, SD>::new(
            &self.store,
            self.mapping.clone(),
            self.row_serde.clone(),
            table_key_range,
            read_options,
            start_epoch,
            end_epoch,
        )
        .await?
        .into_stream::<K>();

        Ok(iter)
    }

    pub async fn batch_iter_vnode_log(
        &self,
        start_epoch: u64,
        end_epoch: HummockReadEpoch,
        start_pk: Option<&OwnedRow>,
        vnode: VirtualNode,
    ) -> StorageResult<impl Stream<Item = StorageResult<ChangeLogRow>>> {
        let start_bound = if let Some(start_pk) = start_pk {
            let mut bytes = BytesMut::new();
            self.pk_serializer.serialize(start_pk, &mut bytes);
            let bytes = bytes.freeze();
            Included(bytes)
        } else {
            Unbounded
        };
        let stream = self
            .batch_iter_log_inner::<()>(
                start_epoch,
                end_epoch,
                (start_bound.as_ref(), Unbounded),
                vnode,
            )
            .await?;
        Ok(stream.map_ok(|(_, row)| row))
    }

    pub async fn batch_iter_log_with_pk_bounds(
        &self,
        start_epoch: u64,
        end_epoch: HummockReadEpoch,
        ordered: bool,
        range_bounds: impl RangeBounds<OwnedRow>,
        pk_prefix: impl Row,
    ) -> StorageResult<impl Stream<Item = StorageResult<ChangeLogRow>> + Send + 'static> {
        let start_key = self.serialize_pk_bound(&pk_prefix, range_bounds.start_bound(), true);
        let end_key = self.serialize_pk_bound(&pk_prefix, range_bounds.end_bound(), false);
        let vnodes = self.distribution.vnodes().iter_vnodes().collect_vec();
        build_vnode_stream(
            |vnode| {
                self.batch_iter_log_inner(
                    start_epoch,
                    end_epoch,
                    (start_key.as_ref(), end_key.as_ref()),
                    vnode,
                )
            },
            |vnode| {
                self.batch_iter_log_inner(
                    start_epoch,
                    end_epoch,
                    (start_key.as_ref(), end_key.as_ref()),
                    vnode,
                )
            },
            &vnodes,
            ordered,
        )
        .await
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    /// Returns a stream of `DataChunk` with the provided `chunk_size`
    pub async fn batch_chunk_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        chunk_size: usize,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<impl Stream<Item = StorageResult<DataChunk>> + Send> {
        let iter = self
            .chunk_iter_with_pk_bounds(
                epoch,
                pk_prefix,
                range_bounds,
                ordered,
                chunk_size,
                prefetch_options,
            )
            .await?;

        Ok(iter.map(|item| {
            let (columns, row_count) = item?;
            Ok(DataChunk::new(columns, row_count))
        }))
    }
}

/// [`StorageTableInnerIterInner`] iterates on the storage table.
struct StorageTableInnerIterInner<S: StateStore, SD: ValueRowSerde> {
    /// An iterator that returns raw bytes from storage.
    iter: S::Iter,

    mapping: Arc<ColumnMapping>,

    /// The index of system column `_rw_timestamp` in the output columns.
    epoch_idx: Option<usize>,

    row_deserializer: Arc<SD>,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: Option<Arc<OrderedRowSerde>>,

    output_indices: Vec<usize>,

    /// the key part of `output_indices`.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of `output_indices`.
    value_output_indices: Vec<usize>,

    /// used for deserializing key part of output row from pk.
    output_row_in_key_indices: Vec<usize>,
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInnerIterInner<S, SD> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    #[allow(clippy::too_many_arguments)]
    async fn new(
        store: &S,
        mapping: Arc<ColumnMapping>,
        epoch_idx: Option<usize>,
        pk_serializer: Option<Arc<OrderedRowSerde>>,
        output_indices: Vec<usize>,
        key_output_indices: Option<Vec<usize>>,
        value_output_indices: Vec<usize>,
        output_row_in_key_indices: Vec<usize>,
        row_deserializer: Arc<SD>,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self> {
        let raw_epoch = epoch.get_epoch();
        store
            .try_wait_epoch(
                epoch,
                TryWaitEpochOptions {
                    table_id: read_options.table_id,
                },
            )
            .await?;
        let iter = store.iter(table_key_range, raw_epoch, read_options).await?;
        let iter = Self {
            iter,
            mapping,
            epoch_idx,
            row_deserializer,
            pk_serializer,
            output_indices,
            key_output_indices,
            value_output_indices,
            output_row_in_key_indices,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (K, OwnedRow), error = StorageError)]
    async fn into_stream<K: CopyFromSlice>(mut self) {
        while let Some((k, v)) = self
            .iter
            .try_next()
            .verbose_instrument_await("storage_table_iter_next")
            .await?
        {
            let (table_key, value, epoch_with_gap) = (k.user_key.table_key, v, k.epoch_with_gap);
            let row = self.row_deserializer.deserialize(value)?;
            let result_row_in_value = self.mapping.project(OwnedRow::new(row));
            let row = match &self.key_output_indices {
                Some(key_output_indices) => {
                    let result_row_in_key = match self.pk_serializer.clone() {
                        Some(pk_serializer) => {
                            let pk = pk_serializer.deserialize(table_key.key_part().as_ref())?;

                            pk.project(&self.output_row_in_key_indices).into_owned_row()
                        }
                        None => OwnedRow::empty(),
                    };

                    let mut result_row_vec = vec![];
                    for idx in &self.output_indices {
                        if let Some(epoch_idx) = self.epoch_idx
                            && *idx == epoch_idx
                        {
                            let epoch = Epoch::from(epoch_with_gap.pure_epoch());
                            result_row_vec
                                .push(risingwave_common::types::Datum::from(epoch.as_scalar()));
                        } else if self.value_output_indices.contains(idx) {
                            let item_position_in_value_indices = &self
                                .value_output_indices
                                .iter()
                                .position(|p| idx == p)
                                .unwrap();
                            result_row_vec.push(
                                result_row_in_value
                                    .datum_at(*item_position_in_value_indices)
                                    .to_owned_datum(),
                            );
                        } else {
                            let item_position_in_pk_indices =
                                key_output_indices.iter().position(|p| idx == p).unwrap();
                            result_row_vec.push(
                                result_row_in_key
                                    .datum_at(item_position_in_pk_indices)
                                    .to_owned_datum(),
                            );
                        }
                    }
                    OwnedRow::new(result_row_vec)
                }
                None => match &self.epoch_idx {
                    Some(epoch_idx) => {
                        let mut result_row_vec = vec![];
                        for idx in &self.output_indices {
                            if idx == epoch_idx {
                                let epoch = Epoch::from(epoch_with_gap.pure_epoch());
                                result_row_vec
                                    .push(risingwave_common::types::Datum::from(epoch.as_scalar()));
                            } else {
                                let item_position_in_value_indices = &self
                                    .value_output_indices
                                    .iter()
                                    .position(|p| idx == p)
                                    .unwrap();
                                result_row_vec.push(
                                    result_row_in_value
                                        .datum_at(*item_position_in_value_indices)
                                        .to_owned_datum(),
                                );
                            }
                        }
                        OwnedRow::new(result_row_vec)
                    }
                    None => result_row_in_value.into_owned_row(),
                },
            };
            yield (K::copy_from_slice(table_key.as_ref()), row);
        }
    }
}

/// [`StorageTableInnerIterLogInner`] iterates on the storage table.
struct StorageTableInnerIterLogInner<S: StateStore, SD: ValueRowSerde> {
    /// An iterator that returns raw bytes from storage.
    iter: S::ChangeLogIter,

    mapping: Arc<ColumnMapping>,

    row_deserializer: Arc<SD>,
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInnerIterLogInner<S, SD> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    #[allow(clippy::too_many_arguments)]
    async fn new(
        store: &S,
        mapping: Arc<ColumnMapping>,
        row_deserializer: Arc<SD>,
        table_key_range: TableKeyRange,
        read_options: ReadLogOptions,
        start_epoch: u64,
        end_epoch: HummockReadEpoch,
    ) -> StorageResult<Self> {
        store
            .try_wait_epoch(
                end_epoch,
                TryWaitEpochOptions {
                    table_id: read_options.table_id,
                },
            )
            .await?;
        let iter = store
            .iter_log(
                (start_epoch, end_epoch.get_epoch()),
                table_key_range,
                read_options,
            )
            .await?;
        let iter = Self {
            iter,
            mapping,
            row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    fn into_stream<K: CopyFromSlice>(self) -> impl Stream<Item = StorageResult<(K, ChangeLogRow)>> {
        self.iter.into_stream(move |(table_key, value)| {
            value
                .try_map(|value| {
                    let full_row = self.row_deserializer.deserialize(value)?;
                    let row = self
                        .mapping
                        .project(OwnedRow::new(full_row))
                        .into_owned_row();
                    Ok(row)
                })
                .map(|row| (K::copy_from_slice(table_key.as_ref()), row))
        })
    }
}
