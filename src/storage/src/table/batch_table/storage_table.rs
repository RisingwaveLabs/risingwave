// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use auto_enums::auto_enum;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{self, Row, Row2, RowDeserializer, RowExt};
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{end_bound_of_prefix, next_key, prefixed_range};
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::trace;

use super::iter_utils;
use crate::error::{StorageError, StorageResult};
use crate::row_serde::row_serde_util::{
    parse_raw_key_to_vnode_and_key, serialize_pk, serialize_pk_with_vnode,
};
use crate::row_serde::{find_columns_by_ids, ColumnMapping};
use crate::store::ReadOptions;
use crate::table::{compute_vnode, Distribution, TableIter};
use crate::{StateStore, StateStoreIter};

/// [`StorageTable`] is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding format, and is used in batch mode.
#[derive(Clone)]
pub struct StorageTable<S: StateStore> {
    /// Id for this table.
    table_id: TableId,

    /// State store backend.
    store: S,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    schema: Schema,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerde,

    /// Mapping from column id to column index for deserializing the row.
    mapping: Arc<ColumnMapping>,

    /// Row deserializer to deserialize the whole value in storage to a row.
    row_deserializer: Arc<RowDeserializer>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,
    distribution_key_start_index_in_pk: Option<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. For READ_WRITE instances, the table will also check whether the written rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,

    /// Used for catalog table_properties
    table_option: TableOption,
}

impl<S: StateStore> std::fmt::Debug for StorageTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTable").finish_non_exhaustive()
    }
}

// init
impl<S: StateStore> StorageTable<S> {
    /// Create a  [`StorageTable`] given a complete set of `columns` and a partial
    /// set of `column_ids`. The output will only contains columns with the given ids in the same
    /// order.
    #[allow(clippy::too_many_arguments)]
    pub fn new_partial(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: Distribution,
        table_options: TableOption,
        value_indices: Vec<usize>,
    ) -> Self {
        Self::new_inner(
            store,
            table_id,
            table_columns,
            column_ids,
            order_types,
            pk_indices,
            distribution,
            table_options,
            value_indices,
        )
    }

    pub fn for_test(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        let column_ids = columns.iter().map(|c| c.column_id).collect();
        let value_indices = (0..columns.len()).collect_vec();
        Self::new_inner(
            store,
            table_id,
            columns,
            column_ids,
            order_types,
            pk_indices,
            Distribution::fallback(),
            Default::default(),
            value_indices,
        )
    }
}

impl<S: StateStore> StorageTable<S> {
    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        Distribution {
            dist_key_indices,
            vnodes,
        }: Distribution,
        table_option: TableOption,
        value_indices: Vec<usize>,
    ) -> Self {
        assert_eq!(order_types.len(), pk_indices.len());

        let (output_columns, output_indices) = find_columns_by_ids(&table_columns, &column_ids);
        assert!(
            output_indices.iter().all(|i| value_indices.contains(i)),
            "output_indices must be a subset of value_indices"
        );
        let schema = Schema::new(output_columns.iter().map(Into::into).collect());
        let mapping = ColumnMapping::new(output_indices);

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let all_data_types = table_columns.iter().map(|d| d.data_type.clone()).collect();
        let pk_serializer = OrderedRowSerde::new(pk_data_types, order_types);
        let row_deserializer = RowDeserializer::new(all_data_types);

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        let distribution_key_start_index_in_pk = match !dist_key_in_pk_indices.is_empty()
            && *dist_key_in_pk_indices.iter().min().unwrap() + dist_key_in_pk_indices.len() - 1
                == *dist_key_in_pk_indices.iter().max().unwrap()
        {
            false => None,
            true => Some(*dist_key_in_pk_indices.iter().min().unwrap()),
        };
        Self {
            table_id,
            store,
            schema,
            pk_serializer,
            mapping: Arc::new(mapping),
            row_deserializer: Arc::new(row_deserializer),
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            distribution_key_start_index_in_pk,
            vnodes,
            table_option,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }
}

/// Point get
impl<S: StateStore> StorageTable<S> {
    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: impl Row2) -> VirtualNode {
        compute_vnode(pk, &self.dist_key_in_pk_indices, &self.vnodes)
    }

    /// Try getting vnode value with given primary key prefix, used for `vnode_hint` in iterators.
    /// Return `None` if the provided columns are not enough.
    fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: impl Row2) -> Option<VirtualNode> {
        self.dist_key_in_pk_indices
            .iter()
            .all(|&d| d < pk_prefix.len())
            .then(|| compute_vnode(pk_prefix, &self.dist_key_in_pk_indices, &self.vnodes))
    }

    /// Get a single row by point get
    pub async fn get_row(
        &self,
        pk: impl Row2,
        wait_epoch: HummockReadEpoch,
    ) -> StorageResult<Option<Row>> {
        let epoch = wait_epoch.get_epoch();
        self.store.try_wait_epoch(wait_epoch).await?;
        let serialized_pk =
            serialize_pk_with_vnode(&pk, &self.pk_serializer, self.compute_vnode_by_pk(&pk));
        assert!(pk.len() <= self.pk_indices.len());
        let key_indices = (0..pk.len())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();

        let read_options = ReadOptions {
            dist_key_hint: None,
            check_bloom_filter: self.dist_key_indices == key_indices,
            retention_seconds: self.table_option.retention_seconds,
            ignore_range_tombstone: false,
            table_id: self.table_id,
        };
        if let Some(value) = self.store.get(&serialized_pk, epoch, read_options).await? {
            let full_row = self.row_deserializer.deserialize(value)?;
            let result_row = self.mapping.project(full_row);
            Ok(Some(result_row))
        } else {
            Ok(None)
        }
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The row iterator of the storage table.
/// The wrapper of [`StorageTableIter`] if pk is not persisted.
pub type StorageTableIter<S: StateStore> = impl PkAndRowStream;

#[async_trait::async_trait]
impl<S: PkAndRowStream + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<Row>> {
        self.next()
            .await
            .transpose()
            .map(|r| r.map(|(_pk, row)| row))
    }
}

/// Iterators
impl<S: StateStore> StorageTable<S> {
    /// Get multiple [`StorageTableIter`] based on the specified vnodes of this table with
    /// `vnode_hint`, and merge or concat them by given `ordered`.
    async fn iter_with_encoded_key_range<R, B>(
        &self,
        dist_key_hint: Option<Vec<u8>>,
        encoded_key_range: R,
        wait_epoch: HummockReadEpoch,
        vnode_hint: Option<VirtualNode>,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S>>
    where
        R: RangeBounds<B> + Send + Clone,
        B: AsRef<[u8]> + Send,
    {
        // Vnodes that are set and should be accessed.
        #[auto_enum(Iterator)]
        let vnodes = match vnode_hint {
            // If `vnode_hint` is set, we can only access this single vnode.
            Some(vnode) => std::iter::once(vnode),
            // Otherwise, we need to access all vnodes of this table.
            None => self
                .vnodes
                .iter()
                .enumerate()
                .filter(|&(_, set)| set)
                .map(|(i, _)| VirtualNode::from_index(i)),
        };

        // For each vnode, construct an iterator.
        // TODO: if there're some vnodes continuously in the range and we don't care about order, we
        // can use a single iterator.
        let iterators: Vec<_> = try_join_all(vnodes.map(|vnode| {
            let raw_key_range = prefixed_range(encoded_key_range.clone(), &vnode.to_be_bytes());

            let dist_key_hint = dist_key_hint.clone();
            let wait_epoch = wait_epoch.clone();
            async move {
                let check_bloom_filter = dist_key_hint.is_some();
                let read_options = ReadOptions {
                    dist_key_hint,
                    check_bloom_filter,
                    ignore_range_tombstone: false,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.table_id,
                };
                let iter = StorageTableIterInner::<S>::new(
                    &self.store,
                    self.mapping.clone(),
                    self.row_deserializer.clone(),
                    raw_key_range,
                    read_options,
                    wait_epoch,
                )
                .await?
                .into_stream();

                Ok::<_, StorageError>(iter)
            }
        }))
        .await?;

        #[auto_enum(futures::Stream)]
        let iter = match iterators.len() {
            0 => unreachable!(),
            1 => iterators.into_iter().next().unwrap(),
            // Concat all iterators if not to preserve order.
            _ if !ordered => futures::stream::iter(iterators).flatten(),
            // Merge all iterators if to preserve order.
            _ => iter_utils::merge_sort(iterators.into_iter().map(Box::pin).collect()),
        };

        Ok(iter)
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    async fn iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row2,
        range_bounds: impl RangeBounds<Row>,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S>> {
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerde,
            pk_prefix: impl Row2,
            range_bound: Bound<&Row>,
            is_start_bound: bool,
        ) -> Bound<Vec<u8>> {
            match range_bound {
                Included(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len() + k.len());
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
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len() + k.len());
                    let key = pk_prefix.chain(k);
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        // Storage doesn't support excluded begin key yet, so transform it to
                        // included.
                        // We always serialize a u8 for null of datum which is not equal to '\xff',
                        // so we can assert that the next_key would never be empty.
                        let next_serialized_key = next_key(&serialized_key);
                        assert!(!next_serialized_key.is_empty());
                        Included(next_serialized_key)
                    } else {
                        Excluded(serialized_key)
                    }
                }
                Unbounded => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len());
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

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            range_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            range_bounds.end_bound(),
            false,
        );

        assert!(pk_prefix.len() <= self.pk_indices.len());
        let pk_prefix_indices = (0..pk_prefix.len())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();
        let dist_key_hint = if self.dist_key_indices.is_empty()
            || !is_subset(self.dist_key_indices.clone(), pk_prefix_indices.clone())
            || self.dist_key_indices.len() + self.distribution_key_start_index_in_pk.unwrap()
                > pk_prefix.len()
        {
            trace!(
                "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}",
                self.table_id,
                pk_prefix,
                self.dist_key_indices,
                pk_prefix_indices
            );
            None
        } else {
            let distribution_key_end_index_in_pk =
                self.distribution_key_start_index_in_pk.unwrap() + self.dist_key_indices.len();
            let dist_key_serializer = self.pk_serializer.dist_key_serde(
                self.distribution_key_start_index_in_pk.unwrap(),
                distribution_key_end_index_in_pk,
            );
            let dist_key = (&pk_prefix).project(&self.dist_key_in_pk_indices);
            let serialized_dist_key = serialize_pk(&dist_key, &dist_key_serializer);
            Some(serialized_dist_key)
        };

        trace!(
            "iter_with_pk_bounds table_id {} dist_key_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}" ,
            self.table_id,
            dist_key_hint,
            start_key,
            end_key,
            pk_prefix,
            self.dist_key_indices,
            pk_prefix_indices
        );

        self.iter_with_encoded_key_range(
            dist_key_hint,
            (start_key, end_key),
            epoch,
            self.try_compute_vnode_by_pk_prefix(pk_prefix),
            ordered,
        )
        .await
    }

    /// Construct a [`StorageTableIter`] for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row2,
        range_bounds: impl RangeBounds<Row>,
    ) -> StorageResult<StorageTableIter<S>> {
        self.iter_with_pk_bounds(epoch, pk_prefix, range_bounds, true)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(&self, epoch: HummockReadEpoch) -> StorageResult<StorageTableIter<S>> {
        self.batch_iter_with_pk_bounds(epoch, row::empty(), ..)
            .await
    }
}

fn is_subset(vec1: Vec<usize>, vec2: Vec<usize>) -> bool {
    HashSet::<usize>::from_iter(vec1).is_subset(&vec2.into_iter().collect())
}

/// [`StorageTableIterInner`] iterates on the storage table.
struct StorageTableIterInner<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: S::Iter,

    mapping: Arc<ColumnMapping>,

    row_deserializer: Arc<RowDeserializer>,
}

impl<S: StateStore> StorageTableIterInner<S> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    async fn new<R, B>(
        store: &S,
        mapping: Arc<ColumnMapping>,
        row_deserializer: Arc<RowDeserializer>,
        raw_key_range: R,
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let raw_epoch = epoch.get_epoch();
        let range = (
            raw_key_range.start_bound().map(|b| b.as_ref().to_vec()),
            raw_key_range.end_bound().map(|b| b.as_ref().to_vec()),
        );
        store.try_wait_epoch(epoch).await?;
        let iter = store.iter(range, raw_epoch, read_options).await?;
        let iter = Self {
            iter,
            mapping,
            row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(self) {
        use crate::store::StateStoreIterExt;

        // No need for table id and epoch.
        let mut iter = self.iter.map(|(k, v)| (k.user_key.table_key.0, v));
        while let Some((raw_key, value)) = iter
            .next()
            .verbose_stack_trace("storage_table_iter_next")
            .await?
        {
            let (_, key) = parse_raw_key_to_vnode_and_key(&raw_key);
            let full_row = self.row_deserializer.deserialize(value)?;
            let row = self.mapping.project(full_row);
            yield (key.to_vec(), row)
        }
    }
}
