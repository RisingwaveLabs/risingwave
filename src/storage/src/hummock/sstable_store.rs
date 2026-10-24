// Copyright 2022 RisingWave Labs
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

use std::clone::Clone;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use foyer::{
    Cache, CacheBuilder, CacheEntry, EventListener, Hint, HybridCache, HybridCacheBuilder,
    HybridCacheEntry,
};
use futures::{StreamExt, future};
use prost::Message;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::vector_index::{HnswGraphFileInfo, VectorFileInfo};
use risingwave_hummock_sdk::{
    HummockHnswGraphFileId, HummockObjectId, HummockRawObjectId, HummockSstableObjectId,
    HummockVectorFileId, SST_OBJECT_SUFFIX,
};
use risingwave_hummock_trace::TracedCachePolicy;
use risingwave_object_store::object::{
    ObjectMetadataIter, ObjectResult, ObjectStoreRef, ObjectStreamingUploader,
};
use risingwave_pb::hummock::PbHnswGraph;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::Instant;

use super::{
    BatchUploadWriter, Block, BlockMeta, RecentFilter, Sstable, SstableMeta, SstableWriterOptions,
};
use crate::hummock::block_stream::BlockDataStream;
use crate::hummock::none::NoneRecentFilter;
use crate::hummock::pin_cache::PinCache;
use crate::hummock::vector::file::{VectorBlock, VectorBlockMeta, VectorFileMeta};
use crate::hummock::vector::monitor::VectorStoreCacheStats;
use crate::hummock::{HummockError, HummockResult};
use crate::monitor::{HummockStateStoreMetrics, StoreLocalStatistic};

mod data_read;

macro_rules! impl_vector_index_meta_file {
    ($($type_name:ident),+) => {
        pub enum HummockVectorIndexMetaFile {
            $(
                $type_name(Pin<Box<$type_name>>),
            )+
        }

        $(
            impl From<$type_name> for HummockVectorIndexMetaFile {
                fn from(v: $type_name) -> Self {
                    Self::$type_name(Box::pin(v))
                }
            }

            unsafe impl Send for VectorMetaFileHolder<$type_name> {}

            impl VectorMetaFileHolder<$type_name> {
                fn try_from_entry(
                    entry: CacheEntry<HummockRawObjectId, HummockVectorIndexMetaFile>,
                    object_id: HummockRawObjectId
                ) -> HummockResult<Self> {
                    let HummockVectorIndexMetaFile::$type_name(file_meta) = &*entry else {
                        return Err(HummockError::decode_error(format!(
                            "expect {} for object {}",
                            stringify!($type_name),
                            object_id
                        )));
                    };
                    let ptr = file_meta.as_ref().get_ref() as *const _;
                    Ok(VectorMetaFileHolder {
                        _cache_entry: entry,
                        ptr,
                    })
                }
            }
        )+
    };
}

impl_vector_index_meta_file!(VectorFileMeta, PbHnswGraph);

pub struct VectorMetaFileHolder<T> {
    _cache_entry: CacheEntry<HummockRawObjectId, HummockVectorIndexMetaFile>,
    ptr: *const T,
}

impl<T> Deref for VectorMetaFileHolder<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: VectorFileHolder is exposed only as immutable, and `VectorFileMeta` is pinned via box
        unsafe { &*self.ptr }
    }
}

pub type TableHolder = HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>;

pub type VectorBlockHolder = CacheEntry<(HummockVectorFileId, usize), Box<VectorBlock>>;

pub type VectorFileHolder = VectorMetaFileHolder<VectorFileMeta>;
pub type HnswGraphFileHolder = VectorMetaFileHolder<PbHnswGraph>;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

pub struct BlockCacheEventListener {
    metrics: Arc<HummockStateStoreMetrics>,
}

impl BlockCacheEventListener {
    pub fn new(metrics: Arc<HummockStateStoreMetrics>) -> Self {
        Self { metrics }
    }
}

impl EventListener for BlockCacheEventListener {
    type Key = SstableBlockIndex;
    type Value = Box<Block>;

    fn on_leave(&self, _reason: foyer::Event, _key: &Self::Key, value: &Self::Value)
    where
        Self::Key: foyer::Key,
        Self::Value: foyer::Value,
    {
        self.metrics
            .block_efficiency_histogram
            .observe(value.efficiency());
    }
}

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(Hint),
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(Hint::Normal)
    }
}

impl From<TracedCachePolicy> for CachePolicy {
    fn from(policy: TracedCachePolicy) -> Self {
        match policy {
            TracedCachePolicy::Disable => Self::Disable,
            TracedCachePolicy::Fill(priority) => Self::Fill(priority.into()),
            TracedCachePolicy::NotFill => Self::NotFill,
        }
    }
}

impl From<CachePolicy> for TracedCachePolicy {
    fn from(policy: CachePolicy) -> Self {
        match policy {
            CachePolicy::Disable => Self::Disable,
            CachePolicy::Fill(priority) => Self::Fill(priority.into()),
            CachePolicy::NotFill => Self::NotFill,
        }
    }
}

pub struct SstableStoreConfig {
    pub store: ObjectStoreRef,
    pub path: String,

    pub prefetch_buffer_capacity: usize,
    pub max_prefetch_block_number: usize,
    pub recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub use_new_object_prefix_strategy: bool,
    pub skip_bloom_filter_in_serde: bool,

    pub meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    pub block_cache: HybridCache<SstableBlockIndex, Box<Block>>,

    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    pin_cache: Option<Arc<PinCache>>,

    meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,

    /// Recent filter for `(sst_obj_id, blk_idx)`.
    ///
    /// `blk_idx == USIZE::MAX` stands for `sst_obj_id` only entry.
    recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    prefetch_buffer_usage: Arc<AtomicUsize>,
    prefetch_buffer_capacity: usize,
    max_prefetch_block_number: usize,
    /// Whether the object store is divided into prefixes depends on two factors:
    ///   1. The specific object store type.
    ///   2. Whether the existing cluster is a new cluster.
    ///
    /// The value of `use_new_object_prefix_strategy` is determined by the `use_new_object_prefix_strategy` field in the system parameters.
    /// For a new cluster, `use_new_object_prefix_strategy` is set to True.
    /// For an old cluster, `use_new_object_prefix_strategy` is set to False.
    /// The final decision of whether to divide prefixes is based on this field and the specific object store type, this approach is implemented to ensure backward compatibility.
    use_new_object_prefix_strategy: bool,

    /// sst serde happens when a sst meta is written to meta disk cache.
    /// Excluding the SST filter from serde can reduce the meta disk cache entry size
    /// and reduce disk IO throughput at the cost of making the SST filter useless.
    skip_bloom_filter_in_serde: bool,
}

impl SstableStore {
    pub fn new(config: SstableStoreConfig) -> Self {
        // TODO: We should validate path early. Otherwise object store won't report invalid path
        // error until first write attempt.

        Self {
            path: config.path,
            store: config.store,
            pin_cache: None,

            meta_cache: config.meta_cache,
            block_cache: config.block_cache,
            vector_meta_cache: config.vector_meta_cache,
            vector_block_cache: config.vector_block_cache,

            recent_filter: config.recent_filter,
            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: config.prefetch_buffer_capacity,
            max_prefetch_block_number: config.max_prefetch_block_number,
            use_new_object_prefix_strategy: config.use_new_object_prefix_strategy,
            skip_bloom_filter_in_serde: config.skip_bloom_filter_in_serde,
        }
    }

    /// Attaches a recovered backend while constructing the store, before sharing it.
    #[must_use]
    pub(crate) fn with_pin_cache(mut self, pin_cache: Arc<PinCache>) -> Self {
        self.pin_cache = Some(pin_cache);
        self
    }

    /// For compactor, we do not need a high concurrency load for cache. Instead, we need the cache
    ///  can be evict more effective.
    #[expect(clippy::borrowed_box)]
    pub async fn for_compactor(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
        use_new_object_prefix_strategy: bool,
    ) -> HummockResult<Self> {
        let meta_cache = HybridCacheBuilder::new()
            .memory(meta_cache_capacity)
            .with_shards(1)
            .with_weighter(|_: &HummockSstableObjectId, value: &Box<Sstable>| {
                std::mem::size_of::<HummockSstableObjectId>()
                    + value.estimated_meta_cache_memory_weight()
            })
            .storage()
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        let block_cache = HybridCacheBuilder::new()
            .memory(block_cache_capacity)
            .with_shards(1)
            .with_weighter(|_: &SstableBlockIndex, value: &Box<Block>| {
                std::mem::size_of::<SstableBlockIndex>() + value.estimated_memory_weight()
            })
            .storage()
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        Ok(Self {
            path,
            store,
            pin_cache: None,

            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: block_cache_capacity,
            max_prefetch_block_number: 16, /* compactor won't use this parameter, so just assign a default value. */
            recent_filter: Arc::new(NoneRecentFilter::default().into()),
            use_new_object_prefix_strategy,
            skip_bloom_filter_in_serde: false,

            meta_cache,
            block_cache,
            vector_meta_cache: CacheBuilder::new(1 << 10).build(),
            vector_block_cache: CacheBuilder::new(1 << 10).build(),
        })
    }

    pub async fn delete(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        self.store
            .delete(self.get_sst_data_path(object_id).as_str())
            .await?;
        self.meta_cache.remove(&object_id);
        // TODO(MrCroxx): support group remove in foyer.
        Ok(())
    }

    pub(crate) fn pin_cache(&self) -> Option<&Arc<PinCache>> {
        self.pin_cache.as_ref()
    }

    pub fn delete_cache(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        self.meta_cache.remove(&object_id);
        Ok(())
    }

    pub(crate) async fn put_sst_data(
        &self,
        object_id: HummockSstableObjectId,
        data: Bytes,
    ) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(object_id);
        self.store
            .upload(&data_path, data)
            .await
            .map_err(Into::into)
    }

    pub async fn get_vector_file_meta(
        &self,
        vector_file: &VectorFileInfo,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<VectorFileHolder> {
        let store = self.store.clone();
        let path = self.get_object_data_path(HummockObjectId::VectorFile(vector_file.object_id));
        let meta_offset = vector_file.meta_offset;
        let entry = self
            .vector_meta_cache
            .get_or_fetch(&vector_file.object_id.as_raw(), || async move {
                let encoded_footer = store.read(&path, meta_offset..).await?;
                let meta = VectorFileMeta::decode_footer(&encoded_footer)?;
                Ok::<_, anyhow::Error>(HummockVectorIndexMetaFile::from(meta))
            })
            .await?;
        stats.file_meta_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.file_meta_miss += 1;
        }
        VectorFileHolder::try_from_entry(entry, vector_file.object_id.as_raw())
    }

    pub async fn get_vector_block(
        &self,
        vector_file: &VectorFileInfo,
        block_idx: usize,
        block_meta: &VectorBlockMeta,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<VectorBlockHolder> {
        let store = self.store.clone();
        let path = self.get_object_data_path(HummockObjectId::VectorFile(vector_file.object_id));
        let start_offset = block_meta.offset;
        let end_offset = start_offset + block_meta.block_size;
        let entry = self
            .vector_block_cache
            .get_or_fetch(&(vector_file.object_id, block_idx), || async move {
                let encoded_block = store.read(&path, start_offset..end_offset).await?;
                let block = VectorBlock::decode(&encoded_block)?;
                Ok::<_, anyhow::Error>(Box::new(block))
            })
            .await
            .map_err(HummockError::foyer_error)?;

        stats.file_block_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.file_block_miss += 1;
        }
        Ok(entry)
    }

    pub fn insert_vector_cache(
        &self,
        object_id: HummockVectorFileId,
        meta: VectorFileMeta,
        blocks: Vec<VectorBlock>,
    ) {
        self.vector_meta_cache
            .insert(object_id.as_raw(), meta.into());
        for (idx, block) in blocks.into_iter().enumerate() {
            self.vector_block_cache
                .insert((object_id, idx), Box::new(block));
        }
    }

    pub fn insert_hnsw_graph_cache(&self, object_id: HummockHnswGraphFileId, graph: PbHnswGraph) {
        self.vector_meta_cache
            .insert(object_id.as_raw(), graph.into());
    }

    pub async fn get_hnsw_graph(
        &self,
        graph_file: &HnswGraphFileInfo,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<HnswGraphFileHolder> {
        let store = self.store.clone();
        let graph_file_path =
            self.get_object_data_path(HummockObjectId::HnswGraphFile(graph_file.object_id));
        let entry = self
            .vector_meta_cache
            .get_or_fetch(&graph_file.object_id.as_raw(), || async move {
                let encoded_graph = store.read(&graph_file_path, ..).await?;
                let graph = PbHnswGraph::decode(encoded_graph.as_ref())?;
                Ok::<_, anyhow::Error>(HummockVectorIndexMetaFile::from(graph))
            })
            .await
            .map_err(HummockError::foyer_error)?;
        stats.hnsw_graph_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.hnsw_graph_miss += 1;
        }
        HnswGraphFileHolder::try_from_entry(entry, graph_file.object_id.as_raw())
    }

    pub fn get_sst_data_path(&self, object_id: impl Into<HummockSstableObjectId>) -> String {
        self.get_object_data_path(HummockObjectId::Sstable(object_id.into()))
    }

    pub fn get_object_data_path(&self, object_id: HummockObjectId) -> String {
        let obj_prefix = self.store.get_object_prefix(
            object_id.as_raw().as_raw_id(),
            self.use_new_object_prefix_strategy,
        );
        risingwave_hummock_sdk::get_object_data_path(&obj_prefix, &self.path, object_id)
    }

    pub fn get_object_id_from_path(path: &str) -> HummockObjectId {
        risingwave_hummock_sdk::get_object_id_from_path(path)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn clear_block_cache(&self) -> HummockResult<()> {
        self.block_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn clear_meta_cache(&self) -> HummockResult<()> {
        self.meta_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)
    }

    pub async fn sstable_cached(
        &self,
        sst_obj_id: HummockSstableObjectId,
    ) -> HummockResult<Option<HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>>> {
        self.meta_cache
            .get(&sst_obj_id)
            .await
            .map_err(HummockError::foyer_error)
    }

    /// Returns `table_holder`
    pub async fn sstable(
        &self,
        sstable_info_ref: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        let object_id = sstable_info_ref.object_id;
        let store = self.store.clone();
        let meta_path = self.get_sst_data_path(object_id);
        let stats_ptr = stats.remote_io_time.clone();
        let range = sstable_info_ref.meta_offset as usize..;
        let skip_bloom_filter_in_serde = self.skip_bloom_filter_in_serde;

        let fetch = self.meta_cache.get_or_fetch(&object_id, || async move {
            let now = Instant::now();
            let buf = store
                .read(&meta_path, range)
                .instrument_await("get_meta_response".verbose())
                .await?;
            let meta = SstableMeta::decode(&buf[..])?;

            let sst = Sstable::new(object_id, meta, skip_bloom_filter_in_serde);
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
            Ok::<_, anyhow::Error>(Box::new(sst))
        });

        stats.cache_meta_block_total += 1;
        let entry = fetch
            .instrument_await("fetch_meta".verbose())
            .await
            .map_err(HummockError::foyer_error);
        if let Ok(ref entry) = entry
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_meta_block_miss += 1;
        }
        entry
    }

    pub async fn list_sst_object_metadata_from_object_store(
        &self,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> HummockResult<ObjectMetadataIter> {
        let list_path = format!("{}/{}", self.path, prefix.unwrap_or("".into()));
        let raw_iter = self.store.list(&list_path, start_after, limit).await?;
        let iter = raw_iter.filter(|r| match r {
            Ok(i) => future::ready(i.key.ends_with(&format!(".{}", SST_OBJECT_SUFFIX))),
            Err(_) => future::ready(true),
        });
        Ok(Box::pin(iter))
    }

    pub fn create_sst_writer(
        self: Arc<Self>,
        object_id: impl Into<HummockSstableObjectId>,
        options: SstableWriterOptions,
    ) -> BatchUploadWriter {
        BatchUploadWriter::new(object_id, self, options)
    }

    pub fn insert_meta_cache(&self, object_id: HummockSstableObjectId, meta: SstableMeta) {
        let sst = Sstable::new(object_id, meta, self.skip_bloom_filter_in_serde);
        self.meta_cache.insert(object_id, Box::new(sst));
    }

    pub fn insert_block_cache(
        &self,
        object_id: HummockSstableObjectId,
        block_index: u64,
        block: Box<Block>,
    ) {
        self.block_cache.insert(
            SstableBlockIndex {
                sst_id: object_id,
                block_idx: block_index,
            },
            block,
        );
    }

    pub fn get_prefetch_memory_usage(&self) -> usize {
        self.prefetch_buffer_usage.load(Ordering::Acquire)
    }

    pub async fn get_stream_for_blocks(
        &self,
        object_id: HummockSstableObjectId,
        metas: &[BlockMeta],
    ) -> HummockResult<BlockDataStream> {
        fail_point!("get_stream_err");
        let data_path = self.get_sst_data_path(object_id);
        let store = self.store();
        let block_meta = &metas[0];
        let start_pos = block_meta.offset as usize;
        let end_pos = metas.iter().map(|meta| meta.len as usize).sum::<usize>() + start_pos;
        let range = start_pos..end_pos;
        // spawn to tokio pool because the object-storage sdk may not be safe to cancel.
        // Compaction may copy raw blocks without decoding them. Always stream from the
        // authoritative store so local cache corruption cannot reach a new SST.
        let ret = tokio::spawn(async move { store.streaming_read(&data_path, range).await }).await;

        let reader = match ret {
            Ok(Ok(reader)) => reader,
            Ok(Err(e)) => return Err(HummockError::from(e)),
            Err(e) => {
                return Err(HummockError::other(format!(
                    "failed to get result, this read request may be canceled: {}",
                    e.as_report()
                )));
            }
        };
        Ok(BlockDataStream::new(reader, metas))
    }

    pub fn meta_cache(&self) -> &HybridCache<HummockSstableObjectId, Box<Sstable>> {
        &self.meta_cache
    }

    pub fn block_cache(&self) -> &HybridCache<SstableBlockIndex, Box<Block>> {
        &self.block_cache
    }

    pub fn recent_filter(&self) -> &Arc<RecentFilter<(HummockSstableObjectId, usize)>> {
        &self.recent_filter
    }

    pub async fn create_streaming_uploader(
        &self,
        path: &str,
    ) -> ObjectResult<ObjectStreamingUploader> {
        self.store.streaming_upload(path).await
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use foyer::{
        BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCacheBuilder, Location,
        PsyncIoEngineConfig,
    };
    use futures::StreamExt;
    use risingwave_hummock_sdk::HummockObjectId;
    use risingwave_hummock_sdk::sstable_info::SstableInfo;

    use super::{SstableBlockIndex, SstableStoreRef, SstableWriterOptions};
    use crate::hummock::block_stream::PrefetchLookup;
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::pin_cache::PinCache;
    use crate::hummock::pin_cache::test_utils::{
        download_and_publish_for_test, in_memory_object_store, publish_pin_cache,
    };
    use crate::hummock::sstable::SstableIteratorReadOptions;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, gen_test_sstable_data, put_sst,
        test_key_of,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{Block, CachePolicy, SstableIterator, SstableMeta, SstableStore};
    use crate::monitor::StoreLocalStatistic;

    const SST_ID: u64 = 1;

    #[tokio::test]
    async fn test_pin_read_cache_policy_and_recent_filter() {
        use std::time::Duration;

        use crate::hummock::RecentFilterTrait;
        use crate::hummock::recent_filter::simple::SimpleRecentFilter;

        let store = mock_sstable_store().await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        let (mut store, _) =
            publish_pin_cache(store, info.object_id, in_memory_object_store()).await;

        // Both a cold Pin read and its subsequent RAM hit must record recent access.
        for _ in 0..2 {
            let recent = Arc::new(SimpleRecentFilter::new(2, Duration::from_secs(60)).into());
            Arc::get_mut(&mut store).unwrap().recent_filter = recent;
            assert!(
                !store
                    .recent_filter()
                    .contains(&(info.object_id, usize::MAX))
            );
            assert!(!store.recent_filter().contains(&(info.object_id, 0)));
            store
                .get(
                    &sst,
                    0,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .await
                .unwrap();
            assert!(
                store
                    .recent_filter()
                    .contains(&(info.object_id, usize::MAX))
            );
            assert!(store.recent_filter().contains(&(info.object_id, 0)));
        }

        #[cfg(feature = "failpoints")]
        {
            use crate::hummock::BlockEntry;

            fail::cfg("disable_block_cache", "return").unwrap();
            let _cleanup = scopeguard::guard((), |_| fail::remove("disable_block_cache"));
            // The failpoint must bypass an existing RAM entry and prevent a new fill.
            let key = SstableBlockIndex {
                sst_id: info.object_id,
                block_idx: 0,
            };
            for cached in [true, false] {
                assert_eq!(store.block_cache().memory().get(&key).is_some(), cached);
                let block = store
                    .get(
                        &sst,
                        0,
                        CachePolicy::default(),
                        &mut StoreLocalStatistic::default(),
                    )
                    .await
                    .unwrap();
                assert!(matches!(block.entry(), BlockEntry::Owned(_)));
                assert_eq!(store.block_cache().memory().get(&key).is_some(), cached);
                store.clear_block_cache().await.unwrap();
            }
        }
    }

    fn get_hummock_value(x: usize) -> HummockValue<Vec<u8>> {
        HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec())
    }

    async fn validate_sst(
        sstable_store: SstableStoreRef,
        info: &SstableInfo,
        mut meta: SstableMeta,
        x_range: Range<usize>,
    ) {
        let mut stats = StoreLocalStatistic::default();
        let holder = sstable_store.sstable(info, &mut stats).await.unwrap();
        std::mem::take(&mut meta.bloom_filter);
        assert_eq!(holder.meta, meta);
        let holder = sstable_store.sstable(info, &mut stats).await.unwrap();
        assert_eq!(holder.meta, meta);
        let mut iter = SstableIterator::new(
            holder,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            info,
        );
        iter.rewind().await.unwrap();
        for i in x_range {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(key, iterator_test_key_of(i).to_ref());
            assert_eq!(value, get_hummock_value(i).as_slice());
            iter.next().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_batch_upload() {
        let sstable_store = mock_sstable_store().await;
        let x_range = 0..100;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range
                .clone()
                .map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let writer_opts = SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Disable,
        };
        let info = put_sst(
            SST_ID,
            data.clone(),
            meta.clone(),
            sstable_store.clone(),
            writer_opts,
            vec![0],
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[tokio::test]
    async fn test_streaming_upload() {
        // Generate test data.
        let sstable_store = mock_sstable_store().await;
        let x_range = 0..100;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range
                .clone()
                .map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let writer_opts = SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Disable,
        };
        let info = put_sst(
            SST_ID,
            data.clone(),
            meta.clone(),
            sstable_store.clone(),
            writer_opts,
            vec![0],
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[tokio::test]
    async fn test_empty_prefetch_falls_back_to_block_get() {
        let mut sstable_store = mock_sstable_store().await;
        // Direct SstableStoreConfig construction can bypass the serde nonzero check.
        Arc::get_mut(&mut sstable_store)
            .unwrap()
            .max_prefetch_block_number = 0;
        let (sstable, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        let mut iter = SstableIterator::new(
            sstable,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: true,
                cache_policy: CachePolicy::Disable,
                ..Default::default()
            }),
            &info,
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), iter.rewind())
            .await
            .expect("empty prefetch must not cause an unbounded refill loop")
            .unwrap();
        assert_eq!(iter.key(), test_key_of(0).to_ref());
        let mut stats = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut stats);
        assert_eq!(stats.cache_data_prefetch_count, 1);
        assert_eq!(stats.cache_data_block_total, 1);
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_prefetch_failure_falls_back_and_releases_tracker() {
        for truncate in [true, false] {
            let sstable_store = mock_sstable_store().await;
            let (sstable, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                    .await;
            assert!(sstable.meta.block_metas.len() > 1);
            sstable_store.clear_block_cache().await.unwrap();
            let path = sstable_store.get_sst_data_path(sstable.id);
            let mut data = sstable_store.store.read(&path, ..).await.unwrap().to_vec();
            let second_offset = sstable.meta.block_metas[1].offset as usize;
            if truncate {
                // A multi-block read fails, but reading block 0 still succeeds.
                data.truncate(second_offset);
            } else {
                // Decoding block 1 fails its checksum after block 0 was already decoded.
                data[second_offset] ^= 1;
            }
            sstable_store
                .store
                .upload(&path, data.into())
                .await
                .unwrap();
            let mut iter = SstableIterator::new(
                sstable.clone(),
                sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions {
                    prefetch: true,
                    cache_policy: CachePolicy::Disable,
                    ..Default::default()
                }),
                &info,
            );
            iter.rewind().await.unwrap();
            assert_eq!(iter.key(), test_key_of(0).to_ref());
            assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
            let mut stats = StoreLocalStatistic::default();
            iter.collect_local_statistic(&mut stats);
            assert_eq!(stats.cache_data_prefetch_count, 1);
            assert_eq!(stats.cache_data_block_total, 1);
            // If the single-block read also fails, the error must still reach the caller.
            let second_key = risingwave_hummock_sdk::key::FullKey::decode(
                &sstable.meta.block_metas[1].smallest_key,
            );
            assert!(iter.seek(second_key).await.is_err());
            assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
        }
    }

    #[tokio::test]
    async fn test_prefetch_releases_old_budget_before_refill() {
        let mut sstable_store = mock_sstable_store().await;
        // The first prefetch may start at zero usage. A retained old tracker would force
        // the next prefetch into the single-block path instead of reading another batch.
        Arc::get_mut(&mut sstable_store)
            .unwrap()
            .prefetch_buffer_capacity = 0;
        let (sstable, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        let next_batch = sstable_store.max_prefetch_block_number;
        assert!(sstable.meta.block_metas.len() > next_batch + 1);
        let mut iter = SstableIterator::new(
            sstable.clone(),
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: true,
                cache_policy: CachePolicy::Disable,
                ..Default::default()
            }),
            &info,
        );
        for idx in [0, next_batch] {
            let key = risingwave_hummock_sdk::key::FullKey::decode(
                &sstable.meta.block_metas[idx].smallest_key,
            );
            iter.seek(key).await.unwrap();
            assert_eq!(iter.key(), key);
            assert!(sstable_store.get_prefetch_memory_usage() > 0);
        }
        let mut stats = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut stats);
        assert_eq!(stats.cache_data_prefetch_count, 2);
        assert_eq!(stats.cache_data_block_total, 0);
        drop(iter);
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_prefetch_producer_large_limit() {
        let mut sstable_store = mock_sstable_store().await;
        Arc::get_mut(&mut sstable_store)
            .unwrap()
            .max_prefetch_block_number = usize::MAX;
        let (sstable, _) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        let mut stats = StoreLocalStatistic::default();
        let mut stream = sstable_store
            .prefetch_blocks(&sstable, 1, 3, CachePolicy::Disable, &mut stats)
            .await
            .unwrap();
        assert_eq!(stats.cache_data_prefetch_block_count, 2);
        for idx in [1, 2] {
            assert!(matches!(
                stream.take_block(idx),
                crate::hummock::block_stream::PrefetchLookup::Hit(_)
            ));
        }
        assert!(matches!(
            stream.take_block(3),
            crate::hummock::block_stream::PrefetchLookup::Exhausted
        ));
        drop(stream);
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_prefetch_producer_respects_disabled_cache() {
        let sstable_store = mock_sstable_store().await;
        let (sstable, _) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        // A cached block in the requested range must not shorten a cache-disabled prefetch.
        sstable_store
            .get(
                &sstable,
                1,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
        let mut stats = StoreLocalStatistic::default();
        let stream = sstable_store
            .prefetch_blocks(&sstable, 0, 3, CachePolicy::Disable, &mut stats)
            .await
            .unwrap();
        assert_eq!(stats.cache_data_prefetch_block_count, 3);
        drop(stream);
        assert!(
            !sstable_store
                .block_cache
                .contains(&super::SstableBlockIndex {
                    sst_id: sstable.id,
                    block_idx: 0,
                })
        );

        // With block 0 cached and the object removed, NotFill may succeed but Disable must fail.
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
        sstable_store.delete(sstable.id).await.unwrap();
        assert!(
            sstable_store
                .prefetch_blocks(
                    &sstable,
                    0,
                    3,
                    CachePolicy::NotFill,
                    &mut StoreLocalStatistic::default()
                )
                .await
                .is_ok()
        );
        assert!(
            sstable_store
                .prefetch_blocks(
                    &sstable,
                    0,
                    3,
                    CachePolicy::Disable,
                    &mut StoreLocalStatistic::default()
                )
                .await
                .is_err()
        );
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_basic() {
        let sstable_store = mock_sstable_store().await;
        let object_id = 123;
        let data_path = sstable_store.get_sst_data_path(object_id);
        assert_eq!(data_path, "test/123.data");
        assert_eq!(
            SstableStore::get_object_id_from_path(&data_path),
            HummockObjectId::Sstable(object_id.into())
        );
    }

    #[tokio::test]
    async fn test_memory_fill_respects_pin_route() {
        const MB: usize = 1 << 20;
        let cache_dir = tempfile::tempdir().unwrap();
        let device = FsDeviceBuilder::new(cache_dir.path())
            .with_capacity(16 * MB)
            .build()
            .unwrap();
        let block_cache = HybridCacheBuilder::new()
            .with_name("pin-memory-only-test")
            .memory(MB)
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::new())
            .with_engine_config(BlockEngineConfig::new(device).with_block_size(MB))
            .build()
            .await
            .unwrap();
        let mut store = mock_sstable_store().await;
        Arc::get_mut(&mut store).unwrap().block_cache = block_cache;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        let (store, _) = publish_pin_cache(store, info.object_id, in_memory_object_store()).await;
        let cache = store.block_cache();
        let key = SstableBlockIndex {
            sst_id: info.object_id,
            block_idx: 0,
        };

        let mut stats = StoreLocalStatistic::default();
        let block = store
            .get(&sst, 0, CachePolicy::default(), &mut stats)
            .await
            .unwrap();
        assert!(block.loaded_from_pin_cache);
        assert_eq!(
            cache.memory().get(&key).unwrap().properties().location(),
            Location::InMem
        );
        drop(block);
        store
            .get(&sst, 0, CachePolicy::NotFill, &mut stats)
            .await
            .unwrap();
        assert_eq!(stats.cache_data_block_total, 2);
        assert_eq!(stats.cache_data_block_miss, 0);
        assert_eq!(stats.pin_cache_data_block_total, 2);
        assert_eq!(stats.pin_cache_data_block_hit, 2);
        assert_eq!(stats.pin_cache_data_block_memory_hit, 1);
        cache.memory().evict_all();
        cache.storage().wait().await;
        assert!(cache.storage().load(&key).await.unwrap().is_miss());

        // A preexisting Foyer disk entry must not take precedence over a published Pin file.
        let (range, capacity) = sst.calculate_block_info(0);
        let data = store
            .store()
            .read(&store.get_sst_data_path(info.object_id), range)
            .await
            .unwrap();
        cache.insert(key, Box::new(Block::decode(data, capacity).unwrap()));
        cache.memory().evict_all();
        cache.storage().wait().await;
        assert!(cache.storage().load(&key).await.unwrap().entry().is_some());
        for policy in [
            CachePolicy::NotFill,
            CachePolicy::Disable,
            CachePolicy::default(),
        ] {
            let block = store
                .get(&sst, 0, policy, &mut StoreLocalStatistic::default())
                .await
                .unwrap();
            assert!(block.loaded_from_pin_cache);
            if !matches!(policy, CachePolicy::Fill(_)) {
                assert!(!cache.memory().contains(&key));
            }
            drop(block);
            // Exercise the prefetch producer even after a Fill point read.
            cache.memory().evict_all();
            let mut stats = StoreLocalStatistic::default();
            let mut stream = store
                .prefetch_blocks(&sst, 0, sst.block_count(), policy, &mut stats)
                .await
                .unwrap();
            let PrefetchLookup::Hit(block) = stream.take_block(0) else {
                panic!("prefetch must return the first block");
            };
            assert!(block.loaded_from_pin_cache);
            assert!(stats.pin_cache_data_block_total > 0);
            assert_eq!(
                stats.pin_cache_data_block_hit,
                stats.pin_cache_data_block_total
            );
            if let CachePolicy::Fill(_) = policy {
                assert_eq!(
                    cache.memory().get(&key).unwrap().properties().location(),
                    Location::InMem
                );
            } else {
                assert!(!cache.memory().contains(&key));
            }
        }
    }

    #[tokio::test]
    async fn test_pin_fetch_shares_entry_and_rechecks_memory() {
        use risingwave_common::config::ObjectStoreConfig;
        use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};

        use crate::monitor::ObjectStoreMetrics;

        let store = mock_sstable_store().await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        let metrics = Arc::new(ObjectStoreMetrics {
            read_bytes: prometheus::IntCounter::new("pin_test_read_bytes", "Local read bytes")
                .unwrap(),
            ..ObjectStoreMetrics::unused()
        });
        let local = Arc::new(ObjectStoreImpl::InMem(
            InMemObjectStore::for_test()
                .monitored(metrics.clone(), Arc::new(ObjectStoreConfig::default())),
        ));
        let (store, pin) = publish_pin_cache(store, info.object_id, local).await;
        let handle = pin.get(info.object_id).unwrap();
        let before = metrics.read_bytes.get();
        let first = store.get_pinned_block(&sst, 0, CachePolicy::default(), handle.clone());
        let second = store.get_pinned_block(&sst, 0, CachePolicy::default(), handle.clone());
        let other = store.get_pinned_block(&sst, 1, CachePolicy::default(), handle.clone());
        futures::pin_mut!(first, second, other);
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());
        assert!(futures::poll!(&mut other).is_pending());
        let (first, second, other) = futures::join!(first, second, other);
        let (first, second, other) = (first.unwrap(), second.unwrap(), other.unwrap());
        assert!(std::ptr::eq(&*first, &*second));
        assert!(!std::ptr::eq(&*first, &*other));
        assert_eq!(
            metrics.read_bytes.get() - before,
            (sst.meta.block_metas[0].len + sst.meta.block_metas[1].len) as u64
        );

        store.clear_block_cache().await.unwrap();
        let delayed = store.get_pinned_block(&sst, 0, CachePolicy::default(), handle);
        futures::pin_mut!(delayed);
        assert!(futures::poll!(&mut delayed).is_pending());
        // Fill memory after the fast lookup misses, before the producer gets to run.
        let inserted = store.block_cache().memory().insert_with_properties(
            SstableBlockIndex {
                sst_id: info.object_id,
                block_idx: 0,
            },
            Box::new((*first).clone()),
            foyer::HybridCacheProperties::default().with_location(Location::InMem),
        );
        let before = metrics.read_bytes.get();
        let block = delayed.await.unwrap();
        assert!(std::ptr::eq(&*block, inserted.value().as_ref()));
        assert_eq!(metrics.read_bytes.get(), before);
    }

    #[tokio::test]
    async fn test_pin_shared_fetch_failure_preserves_new_publication() {
        for corrupt in [false, true] {
            let store = mock_sstable_store().await;
            let (sst, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
            store.clear_block_cache().await.unwrap();
            let local = in_memory_object_store();
            let (store, pin) = publish_pin_cache(store, info.object_id, local.clone()).await;
            let old = pin.get(info.object_id).unwrap();
            let old_path = local
                .list("", None, None)
                .await
                .unwrap()
                .next()
                .await
                .unwrap()
                .unwrap()
                .key;
            if corrupt {
                let size = local.metadata(&old_path).await.unwrap().total_size;
                local
                    .upload(&old_path, Bytes::from(vec![0; size as usize]))
                    .await
                    .unwrap();
            } else {
                local.delete(&old_path).await.unwrap();
            }
            old.invalidate();
            download_and_publish_for_test(
                &pin,
                store.store(),
                store.get_sst_data_path(info.object_id),
                info.object_id,
                info.file_size,
            )
            .await
            .unwrap();
            let current = pin.get(info.object_id).unwrap();

            // The new handle joins the old producer's logical block fetch. Both share its
            // I/O or decode failure, but only the producer's old file may be invalidated.
            let first = store.get_pinned_block(&sst, 0, CachePolicy::default(), old);
            let second = store.get_pinned_block(&sst, 0, CachePolicy::default(), current);
            futures::pin_mut!(first, second);
            assert!(futures::poll!(&mut first).is_pending());
            assert!(futures::poll!(&mut second).is_pending());
            let (first, second) = futures::join!(first, second);
            assert!(first.is_err());
            assert!(second.is_err());
            assert!(pin.get(info.object_id).is_some());
        }
    }

    #[tokio::test]
    async fn test_pin_read_does_not_wait_for_foyer_fetch() {
        let sstable_store = mock_sstable_store().await;
        let (sstable, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        let object_id = info.object_id;
        let (sstable_store, pin_cache) =
            publish_pin_cache(sstable_store, object_id, in_memory_object_store()).await;

        // A slow ordinary fetch must not delay a published Pin read.
        let idx = SstableBlockIndex {
            sst_id: object_id,
            block_idx: 0,
        };
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let leader = sstable_store
            .block_cache
            .get_or_fetch(&idx, move || async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Err::<(Box<crate::hummock::Block>, foyer::HybridCacheProperties), _>(
                    anyhow::anyhow!("injected failure from a different read route"),
                )
            });
        started_rx.await.unwrap();
        let mut stats = StoreLocalStatistic::default();
        let block = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sstable_store.get(&sstable, 0, CachePolicy::default(), &mut stats),
        )
        .await
        .expect("Pin must complete while the ordinary fetch is still blocked")
        .unwrap();
        assert!(block.loaded_from_pin_cache);
        // The synchronous memory insertion can also satisfy Foyer's pending waiters.
        assert!(leader.await.unwrap().value().loaded_from_pin_cache);
        release_tx.send(()).unwrap();
        assert_eq!(stats.pin_cache_data_block_hit, 1);
        assert_eq!(stats.cache_data_block_miss, 0);
        assert!(pin_cache.get(object_id).is_some());
    }

    #[tokio::test]
    async fn test_pin_block_and_prefetch_read_fallback() {
        for (prefetch, failure) in [
            (false, "missing"),
            (false, "checksum"),
            (false, "truncated"),
            (true, "missing"),
            (true, "checksum"),
            (true, "truncated"),
        ] {
            let store = mock_sstable_store().await;
            let (sst, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
            store.clear_block_cache().await.unwrap();
            let local = in_memory_object_store();
            let path = format!("{}-0.sst", info.object_id.as_raw_id());
            let mut data = store
                .store()
                .read(&store.get_sst_data_path(info.object_id), ..)
                .await
                .unwrap()
                .to_vec();
            let corrupt = failure == "checksum";
            if corrupt {
                // Prefetch admits block 0 before decoding the corrupt block 1.
                let (range, _) = sst.calculate_block_info(usize::from(prefetch));
                data[range].fill(0);
            } else if failure == "truncated" {
                let (range, _) = sst.calculate_block_info(usize::from(prefetch));
                data.truncate(range.start + 1);
            }
            local.upload(&path, Bytes::from(data)).await.unwrap();
            // Recovery restores the route without reading or validating the local contents.
            let pin = PinCache::new(local.clone(), u64::MAX, 1, [info.object_id])
                .await
                .unwrap();
            assert!(pin.get(info.object_id).is_some());
            let store = Arc::new(Arc::into_inner(store).unwrap().with_pin_cache(pin.clone()));
            if failure == "missing" {
                local.delete(&path).await.unwrap();
            }
            let mut stats = StoreLocalStatistic::default();
            if prefetch {
                let mut iter = SstableIterator::new(
                    sst.clone(),
                    store.clone(),
                    Arc::new(SstableIteratorReadOptions {
                        prefetch: true,
                        cache_policy: CachePolicy::default(),
                        ..Default::default()
                    }),
                    &info,
                );
                iter.rewind().await.unwrap();
                assert_eq!(iter.key(), test_key_of(0).to_ref());
                iter.collect_local_statistic(&mut stats);
                assert_eq!(stats.cache_data_prefetch_count, 1);
                assert_eq!(stats.cache_data_block_total, 1);
                assert_eq!(store.get_prefetch_memory_usage(), 0);
                if corrupt {
                    let memory = store.block_cache().memory();
                    let first = memory
                        .get(&SstableBlockIndex {
                            sst_id: info.object_id,
                            block_idx: 0,
                        })
                        .unwrap();
                    assert!(first.value().loaded_from_pin_cache);
                    assert_eq!(first.properties().location(), Location::InMem);
                    assert!(!memory.contains(&SstableBlockIndex {
                        sst_id: info.object_id,
                        block_idx: 1
                    }));
                    let block = store
                        .get(
                            &sst,
                            1,
                            CachePolicy::default(),
                            &mut StoreLocalStatistic::default(),
                        )
                        .await
                        .unwrap();
                    assert!(!block.loaded_from_pin_cache);
                }
            } else {
                let block = store
                    .get(&sst, 0, CachePolicy::default(), &mut stats)
                    .await
                    .unwrap();
                assert!(block.len() > 0);
                assert_eq!(stats.cache_data_block_miss, 1);
            }
            assert!(pin.get(info.object_id).is_none());
            assert_eq!(stats.pin_cache_data_block_total, 1);
            assert_eq!(stats.pin_cache_data_block_hit, 0);
        }
    }

    #[tokio::test]
    async fn test_clear_file_cache_and_refetch() {
        let sstable_store = mock_sstable_store().await;
        let x_range = 0..10;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range.map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let info = put_sst(
            SST_ID,
            data,
            meta,
            sstable_store.clone(),
            SstableWriterOptions {
                capacity_hint: None,
                tracker: None,
                policy: CachePolicy::Fill(foyer::Hint::Normal),
            },
            vec![0],
        )
        .await
        .unwrap();

        let mut stats = StoreLocalStatistic::default();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_some()
        );
        let sstable = sstable_store.sstable(&info, &mut stats).await.unwrap();
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut stats,
            )
            .await
            .unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_some()
        );

        sstable_store.clear_meta_cache().await.unwrap();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_none()
        );
        sstable_store.sstable(&info, &mut stats).await.unwrap();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_some()
        );

        sstable_store.clear_block_cache().await.unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_none()
        );
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut stats,
            )
            .await
            .unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_some()
        );
    }
    #[tokio::test]
    async fn test_unpublished_pin_reads_use_foyer() {
        let store = mock_sstable_store().await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, store.clone()).await;
        let local = in_memory_object_store();
        let pin = PinCache::new(local.clone(), u64::MAX, 1, []).await.unwrap();
        let store = Arc::new(Arc::into_inner(store).unwrap().with_pin_cache(pin.clone()));
        let path = store.get_sst_data_path(info.object_id);
        pin.register_objects([info.object_id]);
        // Desired but not published: both read paths must use Foyer without filling Pin.
        assert!(pin.get(info.object_id).is_none());
        let key = SstableBlockIndex {
            sst_id: info.object_id,
            block_idx: 0,
        };
        for location in [Location::Default, Location::InMem] {
            store.clear_block_cache().await.unwrap();
            let remote = store.store();
            let path = path.clone();
            let (range, capacity) = sst.calculate_block_info(0);
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = tokio::sync::oneshot::channel();
            let leader = store.block_cache.get_or_fetch(&key, move || async move {
                started_tx.send(()).unwrap();
                release_rx.await.unwrap();
                let bytes = remote.read(&path, range).await?;
                let block = crate::hummock::Block::decode(bytes, capacity)?;
                // InMem is also used for remote results after a throttled Foyer disk load.
                let props = foyer::HybridCacheProperties::default().with_location(location);
                Ok::<_, anyhow::Error>((Box::new(block), props))
            });
            started_rx.await.unwrap();
            let mut stats = StoreLocalStatistic::default();
            {
                let read = store.get(&sst, 0, CachePolicy::default(), &mut stats);
                futures::pin_mut!(read);
                assert!(futures::poll!(&mut read).is_pending());
                release_tx.send(()).unwrap();
                leader.await.unwrap();
                read.await.unwrap();
            }
            assert!(pin.get(info.object_id).is_none());
            let observed = (
                stats.pin_cache_data_block_total,
                stats.pin_cache_data_block_hit,
                stats.pin_cache_data_block_memory_hit,
                stats.cache_data_block_miss,
            );
            assert_eq!(observed, (1, 0, 0, 1));
        }

        // Force a batch read rather than a first-block cache hit.
        store.clear_block_cache().await.unwrap();
        let mut stats = StoreLocalStatistic::default();
        store
            .prefetch_blocks(&sst, 0, sst.block_count(), CachePolicy::NotFill, &mut stats)
            .await
            .unwrap();
        assert!(stats.cache_data_prefetch_count > 0);
        assert_eq!(stats.pin_cache_data_block_hit, 0);
        assert!(pin.get(info.object_id).is_none());
        assert!(
            local
                .list("", None, None)
                .await
                .unwrap()
                .next()
                .await
                .is_none()
        );
    }
}
