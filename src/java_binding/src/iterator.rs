// Copyright 2023 RisingWave Labs
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

use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::{OwnedRow, RowDeserializer};
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::select_all;
use risingwave_hummock_sdk::key::{map_table_key_range, prefixed_range, TableKeyRange};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::java_binding::key_range::Bound;
use risingwave_pb::java_binding::{KeyRange, ReadPlan};
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::store::state_store::HummockStorageIterator;
use risingwave_storage::hummock::store::version::HummockVersionReader;
use risingwave_storage::hummock::{SstableStore, TieredCache};
use risingwave_storage::monitor::HummockStateStoreMetrics;
use risingwave_storage::store::{ReadOptions, StateStoreReadIterStream, StreamTypeOfIter};
use tokio::sync::mpsc::unbounded_channel;

type SelectAllIterStream = impl StateStoreReadIterStream + Unpin;

fn select_all_vnode_stream(
    streams: Vec<StreamTypeOfIter<HummockStorageIterator>>,
) -> SelectAllIterStream {
    select_all(streams.into_iter().map(Box::pin))
}

pub struct Iterator {
    row_serializer: RowDeserializer,
    stream: SelectAllIterStream,
}

pub struct KeyedRow {
    key: Bytes,
    row: OwnedRow,
}

impl KeyedRow {
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.row[idx].is_none()
    }

    pub fn get_int16(&self, idx: usize) -> i16 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Int16(num) => *num,
            _ => unreachable!("type is not int16 at index: {}", idx),
        }
    }

    pub fn get_int32(&self, idx: usize) -> i32 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Int32(num) => *num,
            _ => unreachable!("type is not int32 at index: {}", idx),
        }
    }

    pub fn get_int64(&self, idx: usize) -> i64 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Int64(num) => *num,
            _ => unreachable!("type is not int64 at index: {}", idx),
        }
    }

    pub fn get_f32(&self, idx: usize) -> f32 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Float32(num) => num.into_inner(),
            _ => unreachable!("type is not float32 at index: {}", idx),
        }
    }

    pub fn get_f64(&self, idx: usize) -> f64 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Float64(num) => num.into_inner(),
            _ => unreachable!("type is not float64 at index: {}", idx),
        }
    }

    pub fn get_bool(&self, idx: usize) -> bool {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Bool(num) => *num,
            _ => unreachable!("type is not boolean at index: {}", idx),
        }
    }

    pub fn get_utf8(&self, idx: usize) -> &str {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Utf8(s) => s.as_ref(),
            _ => unreachable!("type is not utf8 at index: {}", idx),
        }
    }
}

impl Iterator {
    pub async fn new(read_plan: ReadPlan) -> StorageResult<Self> {
        let object_store = Arc::new(
            parse_remote_object_store(
                &read_plan.object_store_url,
                Arc::new(ObjectStoreMetrics::unused()),
                "Hummock",
            )
            .await,
        );
        let sstable_store = Arc::new(SstableStore::new(
            object_store,
            read_plan.data_dir,
            1 << 10,
            1 << 10,
            TieredCache::none(),
        ));
        let reader =
            HummockVersionReader::new(sstable_store, Arc::new(HummockStateStoreMetrics::unused()));

        let mut streams = Vec::with_capacity(read_plan.vnode_ids.len());
        let key_range = read_plan.key_range.unwrap();
        let pin_version = PinnedVersion::new(read_plan.version.unwrap(), unbounded_channel().0);

        for vnode in read_plan.vnode_ids {
            let stream = reader
                .iter(
                    table_key_range_from_prost(
                        VirtualNode::from_index(vnode as usize),
                        key_range.clone(),
                    ),
                    read_plan.epoch,
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone: false,
                        retention_seconds: None,
                        table_id: read_plan.table_id.into(),
                        read_version_from_backup: false,
                    },
                    (vec![], vec![], pin_version.clone()),
                )
                .await?;
            streams.push(stream);
        }

        let stream = select_all_vnode_stream(streams);

        Ok(Self {
            row_serializer: RowDeserializer::new(
                read_plan
                    .table_catalog
                    .unwrap()
                    .columns
                    .into_iter()
                    .map(|c| (&c.column_desc.unwrap().column_type.unwrap()).into())
                    .collect(),
            ),
            stream,
        })
    }

    pub async fn next(&mut self) -> StorageResult<Option<KeyedRow>> {
        let item = self.stream.try_next().await?;
        Ok(match item {
            Some((key, value)) => Some(KeyedRow {
                key: key.user_key.table_key.0,
                row: self.row_serializer.deserialize(value)?,
            }),
            None => None,
        })
    }
}

fn table_key_range_from_prost(vnode: VirtualNode, r: KeyRange) -> TableKeyRange {
    let map_bound = |b, v| match b {
        Bound::Unbounded => std::ops::Bound::Unbounded,
        Bound::Included => std::ops::Bound::Included(v),
        Bound::Excluded => std::ops::Bound::Excluded(v),
        _ => unreachable!(),
    };
    let left_bound = r.left_bound();
    let right_bound = r.right_bound();
    let left = map_bound(left_bound, r.left);
    let right = map_bound(right_bound, r.right);

    let vnode_slice = vnode.to_be_bytes();

    map_table_key_range(prefixed_range((left, right), &vnode_slice[..]))
}
