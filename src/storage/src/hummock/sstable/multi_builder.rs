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

use std::sync::Arc;

use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;
use zstd::zstd_safe::WriteBuf;

use crate::hummock::compactor::TaskProgressTracker;
use crate::hummock::utils::MemoryTracker;
use crate::hummock::value::HummockValue;
use crate::hummock::{CachePolicy, HummockResult, SstableBuilder, SstableStoreWrite};
use crate::monitor::StateStoreMetrics;

#[async_trait::async_trait]
pub trait TableBuilderFactory {
    async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder)>;
}

pub struct SealedSstableBuilder {
    pub sst_info: SstableInfo,
    pub upload_join_handle: JoinHandle<HummockResult<()>>,
    pub bloom_filter_size: usize,
}

/// A wrapper for [`SstableBuilder`] which automatically split key-value pairs into multiple tables,
/// based on their target capacity set in options.
///
/// When building is finished, one may call `finish` to get the results of zero, one or more tables.
pub struct CapacitySplitTableBuilder<F: TableBuilderFactory> {
    /// When creating a new [`SstableBuilder`], caller use this closure to specify the id and
    /// options.
    builder_factory: F,

    sealed_builders: Vec<SealedSstableBuilder>,

    current_builder: Option<SstableBuilder>,

    policy: CachePolicy,

    sstable_store: Arc<dyn SstableStoreWrite>,

    tracker: Option<MemoryTracker>,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    task_progress: Option<TaskProgressTracker>,
}

impl<F: TableBuilderFactory> CapacitySplitTableBuilder<F> {
    /// Creates a new [`CapacitySplitTableBuilder`] using given configuration generator.
    pub fn new(
        builder_factory: F,
        policy: CachePolicy,
        sstable_store: Arc<dyn SstableStoreWrite>,
        stats: Arc<StateStoreMetrics>,
        task_progress: Option<TaskProgressTracker>,
    ) -> Self {
        Self {
            builder_factory,
            sealed_builders: Vec::new(),
            current_builder: None,
            policy,
            sstable_store,
            tracker: None,
            stats,
            task_progress,
        }
    }

    pub fn new_for_test(
        builder_factory: F,
        policy: CachePolicy,
        sstable_store: Arc<dyn SstableStoreWrite>,
    ) -> Self {
        Self {
            builder_factory,
            sealed_builders: Vec::new(),
            current_builder: None,
            policy,
            sstable_store,
            tracker: None,
            task_progress: None,
            stats: Arc::new(StateStoreMetrics::unused()),
        }
    }

    /// Returns the number of [`SstableBuilder`]s.
    pub fn len(&self) -> usize {
        self.sealed_builders.len() + if self.current_builder.is_some() { 1 } else { 0 }
    }

    /// Returns true if no builder is created.
    pub fn is_empty(&self) -> bool {
        self.sealed_builders.is_empty() && self.current_builder.is_none()
    }

    /// Adds a user key-value pair to the underlying builders, with given `epoch`.
    ///
    /// If the current builder reaches its capacity, this function will create a new one with the
    /// configuration generated by the closure provided earlier.
    pub async fn add_user_key(
        &mut self,
        user_key: Vec<u8>,
        value: HummockValue<&[u8]>,
        epoch: HummockEpoch,
    ) -> HummockResult<()> {
        assert!(!user_key.is_empty());
        let full_key = FullKey::from_user_key(user_key, epoch);
        self.add_full_key(full_key.as_slice(), value, true).await?;
        Ok(())
    }

    /// Adds a key-value pair to the underlying builders.
    ///
    /// If `allow_split` and the current builder reaches its capacity, this function will create a
    /// new one with the configuration generated by the closure provided earlier.
    ///
    /// Note that in some cases like compaction of the same user key, automatic splitting is not
    /// allowed, where `allow_split` should be `false`.
    pub async fn add_full_key(
        &mut self,
        full_key: FullKey<&[u8]>,
        value: HummockValue<&[u8]>,
        allow_split: bool,
    ) -> HummockResult<()> {
        if let Some(builder) = self.current_builder.as_ref() {
            if allow_split && builder.reach_capacity() {
                self.seal_current();
            }
        }

        if self.current_builder.is_none() {
            let (tracker, builder) = self.builder_factory.open_builder().await?;
            self.current_builder = Some(builder);
            self.tracker = Some(tracker);
        }

        let builder = self.current_builder.as_mut().unwrap();
        builder.add(full_key.into_inner(), value);
        Ok(())
    }

    /// Marks the current builder as sealed. Next call of `add` will always create a new table.
    ///
    /// If there's no builder created, or current one is already sealed before, then this function
    /// will be no-op.
    pub fn seal_current(&mut self) {
        if let Some(builder) = self.current_builder.take() {
            let (sst_id, data, meta, table_ids) = builder.finish();
            let sstable_store = self.sstable_store.clone();
            let bloom_filter_size = meta.bloom_filter.len();

            if bloom_filter_size != 0 {
                self.stats
                    .sstable_bloom_filter_size
                    .observe(bloom_filter_size as _);
            }

            self.stats
                .sstable_meta_size
                .observe(meta.encoded_size() as _);

            let sst_info = SstableInfo {
                id: sst_id,
                key_range: Some(risingwave_pb::hummock::KeyRange {
                    left: meta.smallest_key.clone(),
                    right: meta.largest_key.clone(),
                    inf: false,
                }),
                file_size: meta.estimated_size as u64,
                table_ids,
            };
            let policy = self.policy;
            let mut tracker = self.tracker.take().unwrap();
            let task_progress = self.task_progress.clone();
            let upload_join_handle = tokio::spawn(async move {
                if !tracker.try_increase_memory(data.capacity() as u64 + meta.encoded_size() as u64)
                {
                    tracing::debug!("failed to allocate increase memory for meta file, sst id: {}, file size: {}, meta size: {}",
                        sst_id, data.capacity(), meta.encoded_size());
                }
                let ret = sstable_store.put_sst(sst_id, meta, data, policy).await;
                drop(tracker);
                if let Some(progress_tracker) = task_progress {
                    progress_tracker.inc_blocks_uploaded();
                }
                ret
            });
            self.sealed_builders.push(SealedSstableBuilder {
                sst_info,
                upload_join_handle,
                bloom_filter_size,
            });
            if let Some(progress_tracker) = &self.task_progress {
                progress_tracker.inc_blocks_sealed();
            }
        }
    }

    /// Finalizes all the tables to be ids, blocks and metadata.
    pub fn finish(mut self) -> Vec<SealedSstableBuilder> {
        self.seal_current();
        self.sealed_builders
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::sstable::utils::CompressionAlgorithm;
    use crate::hummock::test_utils::default_builder_opt_for_test;
    use crate::hummock::{MemoryLimiter, SstableBuilderOptions, DEFAULT_RESTART_INTERVAL};

    pub struct LocalTableBuilderFactory {
        next_id: AtomicU64,
        options: SstableBuilderOptions,
        limiter: MemoryLimiter,
    }

    impl LocalTableBuilderFactory {
        pub fn new(next_id: u64, options: SstableBuilderOptions) -> Self {
            Self {
                limiter: MemoryLimiter::new(1000000),
                next_id: AtomicU64::new(next_id),
                options,
            }
        }
    }

    #[async_trait::async_trait]
    impl TableBuilderFactory for LocalTableBuilderFactory {
        async fn open_builder(&self) -> HummockResult<(MemoryTracker, SstableBuilder)> {
            let id = self.next_id.fetch_add(1, SeqCst);
            let builder = SstableBuilder::new_for_test(id, self.options.clone());
            let tracker = self.limiter.require_memory(1).await.unwrap();
            Ok((tracker, builder))
        }
    }

    #[tokio::test]
    async fn test_empty() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = LocalTableBuilderFactory::new(
            1001,
            SstableBuilderOptions {
                capacity: table_capacity,
                block_capacity: block_size,
                restart_interval: DEFAULT_RESTART_INTERVAL,
                bloom_false_positive: 0.1,
                compression_algorithm: CompressionAlgorithm::None,
                estimate_bloom_filter_capacity: 0,
            },
        );
        let builder = CapacitySplitTableBuilder::new_for_test(
            get_id_and_builder,
            CachePolicy::NotFill,
            mock_sstable_store(),
        );
        let results = builder.finish();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_lots_of_tables() {
        let block_size = 1 << 10;
        let table_capacity = 4 * block_size;
        let get_id_and_builder = LocalTableBuilderFactory::new(
            1001,
            SstableBuilderOptions {
                capacity: table_capacity,
                block_capacity: block_size,
                restart_interval: DEFAULT_RESTART_INTERVAL,
                bloom_false_positive: 0.1,
                compression_algorithm: CompressionAlgorithm::None,
                ..Default::default()
            },
        );
        let mut builder = CapacitySplitTableBuilder::new_for_test(
            get_id_and_builder,
            CachePolicy::NotFill,
            mock_sstable_store(),
        );

        for i in 0..table_capacity {
            builder
                .add_user_key(
                    b"key".to_vec(),
                    HummockValue::put(b"value"),
                    (table_capacity - i) as u64,
                )
                .await
                .unwrap();
        }

        let results = builder.finish();
        assert!(results.len() > 1);
    }

    #[tokio::test]
    async fn test_table_seal() {
        let mut builder = CapacitySplitTableBuilder::new_for_test(
            LocalTableBuilderFactory::new(1001, default_builder_opt_for_test()),
            CachePolicy::NotFill,
            mock_sstable_store(),
        );
        let mut epoch = 100;

        macro_rules! add {
            () => {
                epoch -= 1;
                builder
                    .add_user_key(b"k".to_vec(), HummockValue::put(b"v"), epoch)
                    .await
                    .unwrap();
            };
        }

        assert_eq!(builder.len(), 0);
        builder.seal_current();
        assert_eq!(builder.len(), 0);
        add!();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 1);
        builder.seal_current();
        assert_eq!(builder.len(), 1);
        add!();
        assert_eq!(builder.len(), 2);
        builder.seal_current();
        assert_eq!(builder.len(), 2);
        builder.seal_current();
        assert_eq!(builder.len(), 2);

        let results = builder.finish();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_initial_not_allowed_split() {
        let mut builder = CapacitySplitTableBuilder::new_for_test(
            LocalTableBuilderFactory::new(1001, default_builder_opt_for_test()),
            CachePolicy::NotFill,
            mock_sstable_store(),
        );

        builder
            .add_full_key(
                FullKey::from_user_key_slice(b"k", 233).as_slice(),
                HummockValue::put(b"v"),
                false,
            )
            .await
            .unwrap();
    }
}
