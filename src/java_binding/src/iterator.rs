// Copyright 2023 Singularity Data
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

use std::ops::Bound::Unbounded;
use std::pin::Pin;
use std::sync::Arc;

use futures::TryStreamExt;
use risingwave_common::row::{OwnedRow, RowDeserializer};
use risingwave_common::types::ScalarImpl;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::parse_remote_object_store;
use risingwave_pb::java_binding::ReadPlan;
use risingwave_storage::error::{StorageError, StorageResult};
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::store::state_store::HummockStorageIterator;
use risingwave_storage::hummock::store::version::HummockVersionReader;
use risingwave_storage::hummock::{SstableStore, TieredCache};
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::store::{ReadOptions, StreamTypeOfIter};
use tokio::sync::mpsc::unbounded_channel;

pub struct Iterator {
    row_serializer: RowDeserializer,
    stream: Pin<Box<StreamTypeOfIter<HummockStorageIterator>>>,
}

pub struct Record {
    key: Vec<u8>,
    row: OwnedRow,
}

impl Record {
    pub fn key(&self) -> &[u8] {
        self.key.as_slice()
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.row[idx].is_some()
    }

    pub fn get_int64(&self, idx: usize) -> i64 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Int64(num) => *num,
            _ => unreachable!("type is not int64 at index: {}", idx),
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
    pub async fn new(state_store: &str, read_plan: ReadPlan) -> StorageResult<Self> {
        let object_store = Arc::new(
            parse_remote_object_store(
                state_store
                    .strip_prefix("hummock+")
                    .expect("object store must be hummock for Java Iterator"),
                Arc::new(ObjectStoreMetrics::unused()),
                true,
                "Hummock",
            )
            .await,
        );
        let sstable_store = Arc::new(SstableStore::new(
            object_store,
            "hummock_001".to_string(),
            1 << 10,
            1 << 10,
            TieredCache::none(),
        ));
        let reader =
            HummockVersionReader::new(sstable_store, Arc::new(StateStoreMetrics::unused()));

        let stream = {
            let stream = reader
                .iter(
                    (Unbounded, Unbounded),
                    HummockEpoch::MAX,
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone: false,
                        check_bloom_filter: false,
                        retention_seconds: None,
                        table_id: read_plan.table_id.into(),
                        read_version_from_backup: false,
                    },
                    (
                        vec![],
                        vec![],
                        PinnedVersion::new(read_plan.version.unwrap(), unbounded_channel().0),
                    ),
                )
                .await?;
            Ok::<std::pin::Pin<Box<StreamTypeOfIter<HummockStorageIterator>>>, StorageError>(
                Box::pin(stream),
            )
        }?;

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

    pub async fn next(&mut self) -> StorageResult<Option<Record>> {
        let item = self.stream.try_next().await?;
        Ok(match item {
            Some((key, value)) => Some(Record {
                key: key.user_key.table_key.0,
                row: self.row_serializer.deserialize(value)?,
            }),
            None => None,
        })
    }
}
