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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};
use futures::Future;
use risingwave_common::cache::{CachableEntry, LruCache, LruCacheEventListener};
use risingwave_hummock_sdk::HummockSstableId;

use super::{
    Block, HummockResult, TieredCache, TieredCacheEntry, TieredCacheKey, TieredCacheValue,
};
use crate::hummock::HummockError;

const MIN_BUFFER_SIZE_PER_SHARD: usize = 32 * 1024 * 1024;

enum BlockEntry {
    Cache(CachableEntry<(HummockSstableId, u64), Box<Block>>),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: CachableEntry<(HummockSstableId, u64), Box<Block>>) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }

    pub fn from_tiered_cache(entry: TieredCacheEntry<(HummockSstableId, u64), Box<Block>>) -> Self {
        match entry {
            TieredCacheEntry::Cache(entry) => Self::from_cached_block(entry),
            TieredCacheEntry::Owned(block) => Self::from_owned_block(*block),
        }
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

impl TieredCacheKey for (HummockSstableId, u64) {
    fn encoded_len() -> usize {
        16
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
        buf.put_u64(self.1);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let sst_id = buf.get_u64();
        let block_idx = buf.get_u64();
        (sst_id, block_idx)
    }
}

impl TieredCacheValue for Box<Block> {
    fn len(&self) -> usize {
        Block::len(self)
    }

    fn encoded_len(&self) -> usize {
        self.len()
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_slice(self.raw_data());
    }

    fn decode(buf: Vec<u8>) -> Self {
        Box::new(Block::decode_from_raw(Bytes::from(buf)))
    }
}

pub struct MemoryBlockCacheEventListener {
    tiered_cache: TieredCache<(HummockSstableId, u64), Box<Block>>,
}

impl LruCacheEventListener for MemoryBlockCacheEventListener {
    type K = (HummockSstableId, u64);
    type T = Box<Block>;

    fn on_release(&self, key: Self::K, value: Self::T) {
        // TODO(MrCroxx): handle error?
        self.tiered_cache.insert(key, value).unwrap();
    }
}

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<LruCache<(HummockSstableId, u64), Box<Block>>>,

    tiered_cache: TieredCache<(HummockSstableId, u64), Box<Block>>,
}

impl BlockCache {
    pub fn new(
        capacity: usize,
        mut max_shard_bits: usize,
        tiered_cache: TieredCache<(HummockSstableId, u64), Box<Block>>,
    ) -> Self {
        if capacity == 0 {
            panic!("block cache capacity == 0");
        }
        while (capacity >> max_shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && max_shard_bits > 0 {
            max_shard_bits -= 1;
        }

        let listener = Arc::new(MemoryBlockCacheEventListener {
            tiered_cache: tiered_cache.clone(),
        });

        let cache = LruCache::with_event_listener(max_shard_bits, capacity, listener);

        Self {
            inner: Arc::new(cache),

            tiered_cache,
        }
    }

    pub async fn get(&self, sst_id: HummockSstableId, block_idx: u64) -> Option<BlockHolder> {
        if let Some(block) = self
            .inner
            .lookup(Self::hash(sst_id, block_idx), &(sst_id, block_idx))
            .map(BlockHolder::from_cached_block)
        {
            return Some(block);
        }

        // TODO(MrCroxx): handle error
        if let Some(holder) = self.tiered_cache.get(&(sst_id, block_idx)).await.unwrap() {
            return Some(BlockHolder::from_tiered_cache(holder.into_inner()));
        }

        None
    }

    pub fn insert(
        &self,
        sst_id: HummockSstableId,
        block_idx: u64,
        block: Box<Block>,
    ) -> BlockHolder {
        BlockHolder::from_cached_block(self.inner.insert(
            (sst_id, block_idx),
            Self::hash(sst_id, block_idx),
            block.len(),
            block,
        ))
    }

    pub async fn get_or_insert_with<F, Fut>(
        &self,
        sst_id: HummockSstableId,
        block_idx: u64,
        f: F,
    ) -> HummockResult<BlockHolder>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = HummockResult<Box<Block>>> + Send + 'static,
    {
        let h = Self::hash(sst_id, block_idx);
        let key = (sst_id, block_idx);
        let entry = self
            .inner
            .lookup_with_request_dedup::<_, HummockError, _>(h, key, || {
                let tiered_cache = self.tiered_cache.clone();
                let f = f();
                async move {
                    if let Some(holder) = tiered_cache
                        .get(&key)
                        .await
                        .map_err(HummockError::tiered_cache)?
                    {
                        // TODO(MrCroxx): `TieredCacheEntryHolder::into_owned()` may perform copy,
                        // eliminate it later.
                        let block = holder.into_owned();
                        let len = block.len();
                        return Ok((block, len));
                    }

                    let block = f.await?;
                    let len = block.len();
                    Ok((block, len))
                }
            })
            .await
            .map_err(|e| {
                HummockError::other(format!(
                    "block cache lookup request dedup get cancel: {:?}",
                    e,
                ))
            })??;
        Ok(BlockHolder::from_cached_block(entry))
    }

    fn hash(sst_id: HummockSstableId, block_idx: u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        sst_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }

    pub fn size(&self) -> usize {
        self.inner.get_memory_usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        self.inner.clear();
    }
}
