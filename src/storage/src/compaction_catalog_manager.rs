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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::hash::{VirtualNode, VnodeCountCompat};
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{get_table_id, TABLE_PREFIX_LEN};
use risingwave_pb::catalog::Table;
use risingwave_rpc_client::error::{Result as RpcResult, RpcError};
use risingwave_rpc_client::MetaClient;
use thiserror_ext::AsReport;

use crate::hummock::{HummockError, HummockResult};

/// `FilterKeyExtractor` generally used to extract key which will store in BloomFilter
pub trait FilterKeyExtractor: Send + Sync {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8];
}

pub enum FilterKeyExtractorImpl {
    Schema(SchemaFilterKeyExtractor),
    FullKey(FullKeyFilterKeyExtractor),
    Dummy(DummyFilterKeyExtractor),
    Multi(MultiFilterKeyExtractor),
    FixedLength(FixedLengthFilterKeyExtractor),
}

impl FilterKeyExtractorImpl {
    pub fn from_table(table_catalog: &Table) -> Self {
        let read_prefix_len = table_catalog.get_read_prefix_len_hint() as usize;

        if read_prefix_len == 0 || read_prefix_len > table_catalog.get_pk().len() {
            // for now frontend had not infer the table_id_to_filter_key_extractor, so we
            // use FullKeyFilterKeyExtractor
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor)
        } else {
            FilterKeyExtractorImpl::Schema(SchemaFilterKeyExtractor::new(table_catalog))
        }
    }
}

macro_rules! impl_filter_key_extractor {
    ($( { $variant_name:ident } ),*) => {
        impl FilterKeyExtractorImpl {
            pub fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8]{
                match self {
                    $( Self::$variant_name(inner) => inner.extract(full_key), )*
                }
            }
        }
    }

}

macro_rules! for_all_filter_key_extractor_variants {
    ($macro:ident) => {
        $macro! {
            { Schema },
            { FullKey },
            { Dummy },
            { Multi },
            { FixedLength }
        }
    };
}

for_all_filter_key_extractor_variants! { impl_filter_key_extractor }

#[derive(Default)]
pub struct FullKeyFilterKeyExtractor;

impl FilterKeyExtractor for FullKeyFilterKeyExtractor {
    fn extract<'a>(&self, user_key: &'a [u8]) -> &'a [u8] {
        user_key
    }
}

#[derive(Default)]
pub struct DummyFilterKeyExtractor;
impl FilterKeyExtractor for DummyFilterKeyExtractor {
    fn extract<'a>(&self, _full_key: &'a [u8]) -> &'a [u8] {
        &[]
    }
}

/// [`SchemaFilterKeyExtractor`] build from `table_catalog` and extract a `full_key` to prefix for
#[derive(Default)]
pub struct FixedLengthFilterKeyExtractor {
    fixed_length: usize,
}

impl FilterKeyExtractor for FixedLengthFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        &full_key[0..self.fixed_length]
    }
}

impl FixedLengthFilterKeyExtractor {
    pub fn new(fixed_length: usize) -> Self {
        Self { fixed_length }
    }
}

/// [`SchemaFilterKeyExtractor`] build from `table_catalog` and transform a `full_key` to prefix for
/// `prefix_bloom_filter`
pub struct SchemaFilterKeyExtractor {
    /// Each stateful operator has its own read pattern, partly using prefix scan.
    /// Prefix key length can be decoded through its `DataType` and `OrderType` which obtained from
    /// `TableCatalog`. `read_pattern_prefix_column` means the count of column to decode prefix
    /// from storage key.
    read_prefix_len: usize,
    deserializer: OrderedRowSerde,
    // TODO:need some bench test for same prefix case like join (if we need a prefix_cache for same
    // prefix_key)
}

impl FilterKeyExtractor for SchemaFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        if full_key.len() < TABLE_PREFIX_LEN + VirtualNode::SIZE {
            return &[];
        }

        let (_table_prefix, key) = full_key.split_at(TABLE_PREFIX_LEN);
        let (_vnode_prefix, pk) = key.split_at(VirtualNode::SIZE);

        // if the key with table_id deserializer fail from schema, that should panic here for early
        // detection.

        let bloom_filter_key_len = self
            .deserializer
            .deserialize_prefix_len(pk, self.read_prefix_len)
            .unwrap();

        let end_position = TABLE_PREFIX_LEN + VirtualNode::SIZE + bloom_filter_key_len;
        &full_key[TABLE_PREFIX_LEN + VirtualNode::SIZE..end_position]
    }
}

impl SchemaFilterKeyExtractor {
    pub fn new(table_catalog: &Table) -> Self {
        let pk_indices: Vec<usize> = table_catalog
            .pk
            .iter()
            .map(|col_order| col_order.column_index as usize)
            .collect();

        let read_prefix_len = table_catalog.get_read_prefix_len_hint() as usize;

        let data_types = pk_indices
            .iter()
            .map(|column_idx| &table_catalog.columns[*column_idx])
            .map(|col| ColumnDesc::from(col.column_desc.as_ref().unwrap()).data_type)
            .collect();

        let order_types: Vec<OrderType> = table_catalog
            .pk
            .iter()
            .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
            .collect();

        Self {
            read_prefix_len,
            deserializer: OrderedRowSerde::new(data_types, order_types),
        }
    }
}

#[derive(Default)]
pub struct MultiFilterKeyExtractor {
    id_to_filter_key_extractor: HashMap<u32, FilterKeyExtractorImpl>,
}

impl MultiFilterKeyExtractor {
    pub fn register(&mut self, table_id: u32, filter_key_extractor: FilterKeyExtractorImpl) {
        self.id_to_filter_key_extractor
            .insert(table_id, filter_key_extractor);
    }

    pub fn size(&self) -> usize {
        self.id_to_filter_key_extractor.len()
    }

    pub fn get_existing_table_ids(&self) -> HashSet<u32> {
        self.id_to_filter_key_extractor.keys().cloned().collect()
    }
}

impl Debug for MultiFilterKeyExtractor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MultiFilterKeyExtractor size {} ", self.size())
    }
}

impl FilterKeyExtractor for MultiFilterKeyExtractor {
    fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        if full_key.len() < TABLE_PREFIX_LEN + VirtualNode::SIZE {
            return full_key;
        }

        let table_id = get_table_id(full_key);
        self.id_to_filter_key_extractor
            .get(&table_id)
            .unwrap()
            .extract(full_key)
    }
}

#[async_trait::async_trait]
pub trait StateTableAccessor: Send + Sync {
    async fn get_tables(&self, table_ids: &[u32]) -> RpcResult<HashMap<u32, Table>>;
}

#[derive(Default)]
pub struct FakeRemoteTableAccessor {}

pub struct RemoteTableAccessor {
    meta_client: MetaClient,
}

impl RemoteTableAccessor {
    pub fn new(meta_client: MetaClient) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl StateTableAccessor for RemoteTableAccessor {
    async fn get_tables(&self, table_ids: &[u32]) -> RpcResult<HashMap<u32, Table>> {
        self.meta_client.get_tables(table_ids).await
    }
}

#[async_trait::async_trait]
impl StateTableAccessor for FakeRemoteTableAccessor {
    async fn get_tables(&self, _table_ids: &[u32]) -> RpcResult<HashMap<u32, Table>> {
        Err(RpcError::Internal(anyhow::anyhow!(
            "fake accessor does not support fetch remote table"
        )))
    }
}

/// `CompactionCatalogManager` is a manager to manage all `Table` which used in compaction
pub struct CompactionCatalogManager {
    // `table_id_to_catalog` is a map to store all `Table` which used in compaction
    table_id_to_catalog: RwLock<HashMap<StateTableId, Table>>,
    // `table_accessor` is a accessor to fetch `Table` from meta when the table not found
    table_accessor: Box<dyn StateTableAccessor>,
}

impl Default for CompactionCatalogManager {
    fn default() -> Self {
        Self::new(Box::<FakeRemoteTableAccessor>::default())
    }
}

impl CompactionCatalogManager {
    pub fn new(table_accessor: Box<dyn StateTableAccessor>) -> Self {
        Self {
            table_id_to_catalog: Default::default(),
            table_accessor,
        }
    }
}

impl CompactionCatalogManager {
    /// `update` is used to update `Table` in `table_id_to_catalog` from notification
    pub fn update(&self, table_id: u32, catalog: Table) {
        self.table_id_to_catalog.write().insert(table_id, catalog);
    }

    /// `sync` is used to sync all `Table` in `table_id_to_catalog` from notification whole snapshot
    pub fn sync(&self, catalog_map: HashMap<u32, Table>) {
        let mut guard = self.table_id_to_catalog.write();
        guard.clear();
        guard.extend(catalog_map);
    }

    /// `remove` is used to remove `Table` in `table_id_to_catalog` by `table_id`
    pub fn remove(&self, table_id: u32) {
        self.table_id_to_catalog.write().remove(&table_id);
    }

    /// `acquire` is used to acquire `CompactionCatalogAgent` by `table_ids`
    /// if the table not found in `table_id_to_catalog`, it will fetch from meta
    pub async fn acquire(
        &self,
        mut table_ids: Vec<StateTableId>,
    ) -> HummockResult<CompactionCatalogAgentRef> {
        if table_ids.is_empty() {
            // table_id_set is empty
            // the table in sst has been deleted

            // use full key as default
            return Err(HummockError::other("table_id_set is empty"));
        }

        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        let mut table_id_to_vnode = HashMap::new();
        let mut table_id_to_watermark_serde = HashMap::new();

        {
            let guard = self.table_id_to_catalog.read();
            table_ids.retain(|table_id| match guard.get(table_id) {
                Some(table_catalog) => {
                    // filter-key-extractor
                    multi_filter_key_extractor
                        .register(*table_id, FilterKeyExtractorImpl::from_table(table_catalog));

                    // vnode
                    table_id_to_vnode.insert(*table_id, table_catalog.vnode_count());

                    // watermark
                    table_id_to_watermark_serde
                        .insert(*table_id, build_watermark_col_serde(table_catalog));

                    false
                }

                None => true,
            });
        }

        if !table_ids.is_empty() {
            let mut state_tables =
                self.table_accessor
                    .get_tables(&table_ids)
                    .await
                    .map_err(|e| {
                        HummockError::other(format!(
                            "request rpc list_tables for meta failed: {}",
                            e.as_report()
                        ))
                    })?;

            let mut guard = self.table_id_to_catalog.write();
            for table_id in table_ids {
                if let Some(table) = state_tables.remove(&table_id) {
                    let table_id = table.id;
                    let key_extractor = FilterKeyExtractorImpl::from_table(&table);
                    let vnode = table.vnode_count();
                    let watermark_serde = build_watermark_col_serde(&table);
                    guard.insert(table_id, table);
                    // filter-key-extractor
                    multi_filter_key_extractor.register(table_id, key_extractor);

                    // vnode
                    table_id_to_vnode.insert(table_id, vnode);

                    // watermark
                    table_id_to_watermark_serde.insert(table_id, watermark_serde);
                }
            }
        }

        Ok(Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(multi_filter_key_extractor),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        )))
    }

    /// `build_compaction_catalog_agent` is used to build `CompactionCatalogAgent` by `table_catalogs`
    pub fn build_compaction_catalog_agent(
        table_catalogs: HashMap<StateTableId, Table>,
    ) -> CompactionCatalogAgentRef {
        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        let mut table_id_to_vnode = HashMap::new();
        let mut table_id_to_watermark_serde = HashMap::new();
        for (table_id, table_catalog) in table_catalogs {
            // filter-key-extractor
            multi_filter_key_extractor
                .register(table_id, FilterKeyExtractorImpl::from_table(&table_catalog));

            // vnode
            table_id_to_vnode.insert(table_id, table_catalog.vnode_count());

            // watermark
            table_id_to_watermark_serde.insert(table_id, build_watermark_col_serde(&table_catalog));
        }

        Arc::new(CompactionCatalogAgent::new(
            FilterKeyExtractorImpl::Multi(multi_filter_key_extractor),
            table_id_to_vnode,
            table_id_to_watermark_serde,
        ))
    }
}

/// `CompactionCatalogAgent` is a wrapper of `filter_key_extractor_manager` and `table_id_to_vnode`
/// The `CompactionCatalogAgent` belongs to a compaction task call, which we will build from the `table_ids` contained in a compact task and use it during the compaction.
/// The `CompactionCatalogAgent` can act as a agent for the `CompactionCatalogManager`, providing `extract` and `vnode_count` capabilities.
pub struct CompactionCatalogAgent {
    filter_key_extractor_manager: FilterKeyExtractorImpl,
    table_id_to_vnode: HashMap<StateTableId, usize>,
    // table_id ->(pk_prefix_serde, clean_watermark_col_serde, watermark_col_idx)
    // cache for reduce serde build
    table_id_to_watermark_serde:
        HashMap<StateTableId, Option<(OrderedRowSerde, OrderedRowSerde, usize)>>,
}

impl CompactionCatalogAgent {
    pub fn new(
        filter_key_extractor_manager: FilterKeyExtractorImpl,
        table_id_to_vnode: HashMap<StateTableId, usize>,
        table_id_to_watermark_serde: HashMap<
            StateTableId,
            Option<(OrderedRowSerde, OrderedRowSerde, usize)>,
        >,
    ) -> Self {
        Self {
            filter_key_extractor_manager,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        }
    }

    pub fn dummy() -> Self {
        Self {
            filter_key_extractor_manager: FilterKeyExtractorImpl::Dummy(DummyFilterKeyExtractor),
            table_id_to_vnode: Default::default(),
            table_id_to_watermark_serde: Default::default(),
        }
    }

    pub fn for_test(table_ids: Vec<StateTableId>) -> Arc<Self> {
        let full_key_filter_key_extractor =
            FilterKeyExtractorImpl::FullKey(FullKeyFilterKeyExtractor);

        let table_id_to_vnode: HashMap<u32, usize> = table_ids
            .into_iter()
            .map(|table_id| (table_id, VirtualNode::COUNT_FOR_TEST))
            .collect();

        let table_id_to_watermark_serde = table_id_to_vnode
            .keys()
            .map(|table_id| (*table_id, None))
            .collect();

        Arc::new(CompactionCatalogAgent::new(
            full_key_filter_key_extractor,
            table_id_to_vnode,
            table_id_to_watermark_serde,
        ))
    }
}

impl CompactionCatalogAgent {
    pub fn extract<'a>(&self, full_key: &'a [u8]) -> &'a [u8] {
        self.filter_key_extractor_manager.extract(full_key)
    }

    pub fn vnode_count(&self, table_id: StateTableId) -> usize {
        *self.table_id_to_vnode.get(&table_id).unwrap_or_else(|| {
            panic!(
                "table_id not found {} all_table_ids {:?}",
                table_id,
                self.table_id_to_vnode.keys()
            )
        })
    }

    pub fn watermark_serde(
        &self,
        table_id: StateTableId,
    ) -> Option<(OrderedRowSerde, OrderedRowSerde, usize)> {
        self.table_id_to_watermark_serde
            .get(&table_id)
            .unwrap_or_else(|| {
                panic!(
                    "table_id not found {} all_table_ids {:?}",
                    table_id,
                    self.table_id_to_watermark_serde.keys()
                )
            })
            .clone()
    }

    pub fn table_id_to_vnode_ref(&self) -> &HashMap<StateTableId, usize> {
        &self.table_id_to_vnode
    }

    pub fn table_ids(&self) -> impl Iterator<Item = StateTableId> + '_ {
        self.table_id_to_vnode.keys().cloned()
    }
}

pub type CompactionCatalogManagerRef = Arc<CompactionCatalogManager>;
pub type CompactionCatalogAgentRef = Arc<CompactionCatalogAgent>;

fn build_watermark_col_serde(
    table_catalog: &Table,
) -> Option<(OrderedRowSerde, OrderedRowSerde, usize)> {
    match table_catalog.clean_watermark_index_in_pk {
        None => {
            // non watermark table or watermark column is the first column (pk_prefix_watermark)
            None
        }

        Some(clean_watermark_index_in_pk) => {
            use risingwave_common::types::DataType;
            let table_columns: Vec<ColumnDesc> = table_catalog
                .columns
                .iter()
                .map(|col| col.column_desc.as_ref().unwrap().into())
                .collect();

            let pk_data_types: Vec<DataType> = table_catalog
                .pk
                .iter()
                .map(|col_order| {
                    table_columns[col_order.column_index as usize]
                        .data_type
                        .clone()
                })
                .collect();

            let pk_order_types = table_catalog
                .pk
                .iter()
                .map(|col_order| OrderType::from_protobuf(col_order.get_order_type().unwrap()))
                .collect_vec();

            assert_eq!(pk_data_types.len(), pk_order_types.len());
            let pk_serde = OrderedRowSerde::new(pk_data_types, pk_order_types);
            let watermark_col_serde = pk_serde
                .index(clean_watermark_index_in_pk as usize)
                .into_owned();
            Some((
                pk_serde,
                watermark_col_serde,
                clean_watermark_index_in_pk as usize,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use bytes::{BufMut, BytesMut};
    use itertools::Itertools;
    use risingwave_common::catalog::ColumnDesc;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::DataType;
    use risingwave_common::types::ScalarImpl::{self};
    use risingwave_common::util::row_serde::OrderedRowSerde;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_hummock_sdk::key::TABLE_PREFIX_LEN;
    use risingwave_pb::catalog::table::{PbEngine, TableType};
    use risingwave_pb::catalog::{PbCreateType, PbStreamJobStatus, PbTable};
    use risingwave_pb::common::{PbColumnOrder, PbDirection, PbNullsAre, PbOrderType};
    use risingwave_pb::plan_common::PbColumnCatalog;

    use super::{DummyFilterKeyExtractor, FilterKeyExtractor, SchemaFilterKeyExtractor};
    use crate::compaction_catalog_manager::{
        FilterKeyExtractorImpl, FullKeyFilterKeyExtractor, MultiFilterKeyExtractor,
    };
    const fn dummy_vnode() -> [u8; VirtualNode::SIZE] {
        VirtualNode::from_index(233).to_be_bytes()
    }

    #[test]
    fn test_default_filter_key_extractor() {
        let dummy_filter_key_extractor = DummyFilterKeyExtractor;
        let full_key = "full_key".as_bytes();
        let output_key = dummy_filter_key_extractor.extract(full_key);

        assert_eq!("".as_bytes(), output_key);

        let full_key_filter_key_extractor = FullKeyFilterKeyExtractor;
        let output_key = full_key_filter_key_extractor.extract(full_key);

        assert_eq!(full_key, output_key);
    }

    fn build_table_with_prefix_column_num(column_count: u32) -> PbTable {
        PbTable {
            id: 0,
            schema_id: 0,
            database_id: 0,
            name: "test".to_owned(),
            table_type: TableType::Table as i32,
            columns: vec![
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::new_atomic(DataType::Int64, "_row_id", 0)).into(),
                    ),
                    is_hidden: true,
                },
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::new_atomic(DataType::Int64, "col_1", 0)).into(),
                    ),
                    is_hidden: false,
                },
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::new_atomic(DataType::Float64, "col_2", 0)).into(),
                    ),
                    is_hidden: false,
                },
                PbColumnCatalog {
                    column_desc: Some(
                        (&ColumnDesc::new_atomic(DataType::Varchar, "col_3", 0)).into(),
                    ),
                    is_hidden: false,
                },
            ],
            pk: vec![
                PbColumnOrder {
                    column_index: 1,
                    order_type: Some(PbOrderType {
                        direction: PbDirection::Ascending as _,
                        nulls_are: PbNullsAre::Largest as _,
                    }),
                },
                PbColumnOrder {
                    column_index: 3,
                    order_type: Some(PbOrderType {
                        direction: PbDirection::Ascending as _,
                        nulls_are: PbNullsAre::Largest as _,
                    }),
                },
            ],
            stream_key: vec![0],
            dependent_relations: vec![],
            distribution_key: (0..column_count as i32).collect_vec(),
            optional_associated_source_id: None,
            append_only: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            retention_seconds: Some(300),
            fragment_id: 0,
            dml_fragment_id: None,
            initialized_at_epoch: None,
            vnode_col_index: None,
            row_id_index: Some(0),
            value_indices: vec![0],
            definition: "".into(),
            handle_pk_conflict_behavior: 0,
            version_column_index: None,
            read_prefix_len_hint: 1,
            version: None,
            watermark_indices: vec![],
            dist_key_in_pk: vec![],
            cardinality: None,
            created_at_epoch: None,
            cleaned_by_watermark: false,
            stream_job_status: PbStreamJobStatus::Created.into(),
            create_type: PbCreateType::Foreground.into(),
            description: None,
            incoming_sinks: vec![],
            initialized_at_cluster_version: None,
            created_at_cluster_version: None,
            cdc_table_id: None,
            maybe_vnode_count: None,
            webhook_info: None,
            job_id: None,
            engine: Some(PbEngine::Hummock as i32),
            clean_watermark_index_in_pk: None,
        }
    }

    #[test]
    fn test_schema_filter_key_extractor() {
        let prost_table = build_table_with_prefix_column_num(1);
        let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);

        let order_types: Vec<OrderType> = vec![OrderType::ascending(), OrderType::ascending()];
        let schema = vec![DataType::Int64, DataType::Varchar];
        let serializer = OrderedRowSerde::new(schema, order_types);
        let row = OwnedRow::new(vec![
            Some(ScalarImpl::Int64(100)),
            Some(ScalarImpl::Utf8("abc".into())),
        ]);
        let mut row_bytes = vec![];
        serializer.serialize(&row, &mut row_bytes);

        let table_prefix = {
            let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
            buf.put_u32(1);
            buf.to_vec()
        };

        let vnode_prefix = &dummy_vnode()[..];

        let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
        let output_key = schema_filter_key_extractor.extract(&full_key);
        assert_eq!(1 + mem::size_of::<i64>(), output_key.len());
    }

    #[test]
    fn test_multi_filter_key_extractor() {
        let mut multi_filter_key_extractor = MultiFilterKeyExtractor::default();
        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(1);
            let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);
            multi_filter_key_extractor.register(
                1,
                FilterKeyExtractorImpl::Schema(schema_filter_key_extractor),
            );
            let order_types: Vec<OrderType> = vec![OrderType::ascending(), OrderType::ascending()];
            let schema = vec![DataType::Int64, DataType::Varchar];
            let serializer = OrderedRowSerde::new(schema, order_types);
            let row = OwnedRow::new(vec![
                Some(ScalarImpl::Int64(100)),
                Some(ScalarImpl::Utf8("abc".into())),
            ]);
            let mut row_bytes = vec![];
            serializer.serialize(&row, &mut row_bytes);

            let table_prefix = {
                let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
                buf.put_u32(1);
                buf.to_vec()
            };

            let vnode_prefix = &dummy_vnode()[..];

            let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
            let output_key = multi_filter_key_extractor.extract(&full_key);

            let data_types = vec![DataType::Int64];
            let order_types = vec![OrderType::ascending()];
            let deserializer = OrderedRowSerde::new(data_types, order_types);

            let pk_prefix_len = deserializer.deserialize_prefix_len(&row_bytes, 1).unwrap();
            assert_eq!(pk_prefix_len, output_key.len());
        }

        {
            // test table_id 1
            let prost_table = build_table_with_prefix_column_num(2);
            let schema_filter_key_extractor = SchemaFilterKeyExtractor::new(&prost_table);
            multi_filter_key_extractor.register(
                2,
                FilterKeyExtractorImpl::Schema(schema_filter_key_extractor),
            );
            let order_types: Vec<OrderType> = vec![OrderType::ascending(), OrderType::ascending()];
            let schema = vec![DataType::Int64, DataType::Varchar];
            let serializer = OrderedRowSerde::new(schema, order_types);
            let row = OwnedRow::new(vec![
                Some(ScalarImpl::Int64(100)),
                Some(ScalarImpl::Utf8("abc".into())),
            ]);
            let mut row_bytes = vec![];
            serializer.serialize(&row, &mut row_bytes);

            let table_prefix = {
                let mut buf = BytesMut::with_capacity(TABLE_PREFIX_LEN);
                buf.put_u32(2);
                buf.to_vec()
            };

            let vnode_prefix = &dummy_vnode()[..];

            let full_key = [&table_prefix, vnode_prefix, &row_bytes].concat();
            let output_key = multi_filter_key_extractor.extract(&full_key);

            let data_types = vec![DataType::Int64, DataType::Varchar];
            let order_types = vec![OrderType::ascending(), OrderType::ascending()];
            let deserializer = OrderedRowSerde::new(data_types, order_types);

            let pk_prefix_len = deserializer.deserialize_prefix_len(&row_bytes, 1).unwrap();

            assert_eq!(pk_prefix_len, output_key.len());
        }
    }

    #[tokio::test]
    async fn test_compaction_catalog_manager_exception() {
        let compaction_catalog_manager = super::CompactionCatalogManager::default();

        {
            let ret = compaction_catalog_manager.acquire(vec![]).await;
            assert!(ret.is_err());
            if let Err(e) = ret {
                assert_eq!(e.to_string(), "Other error: table_id_set is empty");
            }
        }

        {
            // network error with FakeRemoteTableAccessor
            let ret = compaction_catalog_manager.acquire(vec![1]).await;
            assert!(ret.is_err());
            if let Err(e) = ret {
                assert_eq!(
                    e.to_string(),
                    "Other error: request rpc list_tables for meta failed: fake accessor does not support fetch remote table"
                );
            }
        }
    }
}
