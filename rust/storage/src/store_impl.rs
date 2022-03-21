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
//
use std::fmt::Debug;
use std::sync::Arc;

use risingwave_common::config::StorageConfig;
use risingwave_common::error::{Result, RwError};
use risingwave_rpc_client::MetaClient;

use crate::hummock::hummock_meta_client::RpcHummockMetaClient;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::{HummockStateStore, SstableStore};
use crate::memory::MemoryStateStore;
use crate::monitor::{HummockMetrics, MonitoredStateStore as Monitored, StateStoreMetrics};
use crate::object::{InMemObjectStore, ObjectStore, S3ObjectStore};
use crate::rocksdb_local::RocksDBStateStore;
use crate::tikv::TikvStateStore;
use crate::StateStore;

/// The type erased [`StateStore`].
#[derive(Clone)]
pub enum StateStoreImpl {
    /// The Hummock state store, which operates on an S3-like service. URLs beginning with
    /// `hummock` will be automatically recognized as Hummock state store.
    ///
    /// Example URLs:
    ///
    /// * `hummock+s3://bucket`
    /// * `hummock+minio://KEY:SECRET@minio-ip:port`
    /// * `hummock+memory` (should only be used in 1 compute node mode)
    HummockStateStore(Monitored<HummockStateStore>),
    /// In-memory B-Tree state store. Should only be used in unit and integration tests. If you
    /// want speed up e2e test, you should use Hummock in-memory mode instead. Also, this state
    /// store misses some critical implementation to ensure the correctness of persisting streaming
    /// state. (e.g., no read_epoch support, no async checkpoint)
    MemoryStateStore(Monitored<MemoryStateStore>),
    /// Should enable `rocksdb-local` feature to use this state store. Not feature-complete, and
    /// should never be used in tests and production.
    RocksDBStateStore(Monitored<RocksDBStateStore>),
    /// Should enable `tikv` feature to use this state store. Not feature-complete, and
    /// should never be used in tests and production.
    TikvStateStore(Monitored<TikvStateStore>),
}

impl StateStoreImpl {
    pub fn shared_in_memory_store(state_store_metrics: Arc<StateStoreMetrics>) -> Self {
        Self::MemoryStateStore(MemoryStateStore::shared().monitored(state_store_metrics))
    }
}

impl Debug for StateStoreImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StateStoreImpl::HummockStateStore(_) => write!(f, "HummockStateStore"),
            StateStoreImpl::MemoryStateStore(_) => write!(f, "MemoryStateStore"),
            StateStoreImpl::RocksDBStateStore(_) => write!(f, "RocksDBStateStore"),
            StateStoreImpl::TikvStateStore(_) => write!(f, "TikvStateStore"),
        }
    }
}

#[macro_export]
macro_rules! dispatch_state_store {
    ($impl:expr, $store:ident, $body:tt) => {
        match $impl {
            StateStoreImpl::MemoryStateStore($store) => $body,
            StateStoreImpl::HummockStateStore($store) => $body,
            StateStoreImpl::TikvStateStore($store) => $body,
            StateStoreImpl::RocksDBStateStore($store) => $body,
        }
    };
}

impl StateStoreImpl {
    pub async fn new(
        s: &str,
        config: Arc<StorageConfig>,
        meta_client: MetaClient,
        state_store_stats: Arc<StateStoreMetrics>,
        hummock_stats: Arc<HummockMetrics>,
    ) -> Result<Self> {
        let store = match s {
            hummock if hummock.starts_with("hummock") => {
                use crate::hummock::HummockStorage;

                let object_store = match hummock {
                    s3 if s3.starts_with("hummock+s3://") => Arc::new(
                        S3ObjectStore::new(s3.strip_prefix("hummock+s3://").unwrap().to_string())
                            .await,
                    )
                        as Arc<dyn ObjectStore>,
                    minio if minio.starts_with("hummock+minio://") => Arc::new(
                        S3ObjectStore::new_with_minio(minio.strip_prefix("hummock+").unwrap())
                            .await,
                    )
                        as Arc<dyn ObjectStore>,
                    memory if memory.starts_with("hummock+memory") => {
                        tracing::warn!("You're using Hummock in-memory object store. This should never be used in benchmarks and production environment.");
                        Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>
                    }
                    other => {
                        unimplemented!(
                            "{} Hummock only supports s3, minio and memory for now.",
                            other
                        )
                    }
                };

                let sstable_store = Arc::new(SstableStore::new(
                    object_store,
                    config.data_directory.to_string(),
                    Some(state_store_stats.clone()),
                ));
                let inner = HummockStateStore::new(
                    HummockStorage::new(
                        config.clone(),
                        sstable_store.clone(),
                        Arc::new(LocalVersionManager::new(sstable_store)),
                        Arc::new(RpcHummockMetaClient::new(
                            meta_client,
                            hummock_stats.clone(),
                        )),
                        state_store_stats.clone(),
                    )
                    .await
                    .map_err(RwError::from)?,
                );
                StateStoreImpl::HummockStateStore(inner.monitored(state_store_stats))
            }

            "in_memory" | "in-memory" => {
                tracing::warn!("in-memory state backend should never be used in benchmarks and production environment.");
                StateStoreImpl::shared_in_memory_store(state_store_stats.clone())
            }

            tikv if tikv.starts_with("tikv") => {
                let inner =
                    TikvStateStore::new(vec![tikv.strip_prefix("tikv://").unwrap().to_string()]);
                StateStoreImpl::TikvStateStore(inner.monitored(state_store_stats))
            }

            rocksdb if rocksdb.starts_with("rocksdb_local://") => {
                let inner =
                    RocksDBStateStore::new(rocksdb.strip_prefix("rocksdb_local://").unwrap());
                StateStoreImpl::RocksDBStateStore(inner.monitored(state_store_stats))
            }

            other => unimplemented!("{} state store is not supported", other),
        };

        Ok(store)
    }
}
