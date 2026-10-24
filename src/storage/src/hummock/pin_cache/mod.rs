// Copyright 2026 RisingWave Labs
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

//! Local whole-SST storage with separate read, membership, and refill protocols.
//!
//! Callers register referenced objects and unregister obsolete ones at their chosen lifecycle
//! boundaries. The backend object registry does not track versions; the membership module
//! independently computes version-derived logical SST references.
//! A refill captures a `PinCacheRefillToken` with `prepare_refill` before it is queued;
//! the caller checks it before I/O, and `publish` checks it atomically with the index update.
//! Each object has an optional published file. The executor owns running attempts; downloads leave the
//! index unchanged until publication. Revocation changes the admission identity; unregistering
//! invalidates all object tokens.
//! Unregistering an object prevents new lookups; existing read handles retain their file.
//! Reads use `get` and never create refill work. Recovery completes before sharing the cache.
//! GC reclaims withdrawn files after their last reader releases them.
//! `storage` owns file registration and capacity; `gc` selects and deletes files using that state.
//! They share one storage lock, separate from the shard locks used by the read index.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectResult, ObjectStoreRef};

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "GC scheduling is not wired yet")
)]
mod gc;
pub(crate) mod membership;
mod read;
mod recovery;
mod refill;
mod storage;
pub(crate) use refill::PinCacheDownloadError;
#[cfg(test)]
pub(super) mod test_utils;
#[cfg(test)]
mod tests;

use self::storage::PinCacheStorageState;
use crate::hummock::SstableBlockIndex;
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

/// One download's local file, shared by the index and readers after publication.
struct PinCacheFile {
    path: String,
    size: u64,
}

/// One registered SST object; registration alone does not make it readable or start refill.
struct PinCacheObject {
    // Admission identity, shared by queued work and its download. Stable across retries;
    // replaced on revocation. It is not a version ID or a persisted file sequence.
    generation: u64,
    published: Option<Arc<PinCacheFile>>,
}

impl PinCacheObject {
    /// Withdraws the read route and returns the exact file to enqueue for deletion.
    /// Membership and refill admission remain valid.
    /// The caller must hand the returned reference to GC outside the shard lock.
    fn unpublish(&mut self) -> Option<Arc<PinCacheFile>> {
        let file = self.published.take()?;
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(file.size));
        Some(file)
    }

    fn publish(&mut self, file: Arc<PinCacheFile>) {
        assert!(self.published.is_none());
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(file.size));
        self.published = Some(file);
    }
}

/// One shard's object state and read flights, each protected by its own lock.
/// Hold at most one shard lock at a time. Release it before I/O, spawning tasks,
/// invoking callbacks, or acquiring the cache's storage lock.
#[derive(Default)]
struct PinCacheShard {
    state: RwLock<PinCacheShardState>,
    read_requests: Mutex<HashMap<SstableBlockIndex, read::ReadRequest>>,
}

/// One shard's object index and refill generation allocator.
#[derive(Default)]
struct PinCacheShardState {
    objects: HashMap<HummockSstableObjectId, PinCacheObject>,
    // Never reset on removal: an old token must not match a reintroduced object.
    next_generation: u64,
}

impl PinCacheShardState {
    fn register_object(&mut self, id: HummockSstableObjectId) {
        self.objects.entry(id).or_insert_with(|| {
            self.next_generation += 1;
            PinCacheObject {
                generation: self.next_generation,
                published: None,
            }
        });
    }

    fn revoke_refill(&mut self, object_id: HummockSstableObjectId) {
        if let Some(object) = self.objects.get_mut(&object_id) {
            self.next_generation += 1;
            object.generation = self.next_generation;
        }
    }

    /// Finds the registered object whose ID and generation both match the token.
    /// Returns None after unregistration or token revocation, including removal followed by
    /// re-registration of the same ID. This does not check whether a file is already published.
    fn object_matching_token(&mut self, token: PinCacheRefillToken) -> Option<&mut PinCacheObject> {
        self.objects
            .get_mut(&token.object_id)
            .filter(|object| object.generation == token.generation)
    }
}

/// A local whole-SST cache. The remote object store remains authoritative.
/// Each object's state is protected by its shard lock. Reads, refills, and membership updates
/// may run concurrently; batch membership updates are not atomic across objects.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    shards: Box<[PinCacheShard]>,
    capacity: u64,
    // Leaf lock: never acquire a shard lock or perform I/O while holding it.
    // Upload protection and capacity accounting change together under this lock.
    storage: Mutex<PinCacheStorageState>,
    next_path_id: AtomicU64,
}

/// Admission captured before queuing a refill. Its identity is private to the issuing cache.
/// This token reserves no disk space, starts no I/O, and needs no cleanup when discarded.
/// Copies preserve the same admission for retries; revocation invalidates every copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PinCacheRefillToken {
    object_id: HummockSstableObjectId,
    generation: u64,
}

impl PinCacheRefillToken {
    /// The SST whose remote path the caller should use for this refill.
    pub(crate) fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
}

/// A reference to one published file, retained even after its object leaves the index.
/// The file stays on disk until the last handle releases it. Reads never look up the object again.
/// Callers must support fallback if the selected local file becomes unavailable.
#[derive(Clone)]
pub(crate) struct PinCacheReadHandle {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    file: Arc<PinCacheFile>,
}

impl PinCache {
    /// Recovers local files selected by the initial pin-policy/version membership before sharing.
    /// An incomplete inventory fails initialization; no partially recovered cache is returned.
    pub(crate) async fn new(
        store: ObjectStoreRef,
        capacity: u64,
        shard_num: usize,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) -> ObjectResult<Arc<Self>> {
        assert!(
            shard_num > 0,
            "pin cache shard count must be greater than zero"
        );
        GLOBAL_PIN_CACHE_METRICS
            .capacity_bytes
            .set(metric_bytes(capacity));
        GLOBAL_PIN_CACHE_METRICS.accounted_bytes.set(0);
        let mut pin_cache = Self {
            store,
            capacity,
            storage: Mutex::default(),
            shards: (0..shard_num).map(|_| PinCacheShard::default()).collect(),
            next_path_id: AtomicU64::new(rand::random()),
        };
        for id in objects {
            let state = pin_cache.shards[Self::shard_index(id, shard_num)]
                .state
                .get_mut();
            state.register_object(id);
        }
        let metrics = &*GLOBAL_PIN_CACHE_METRICS;
        let objects = pin_cache.store.list("", None, None).await;
        let recovered = pin_cache
            .recover_local_files(objects)
            .await
            .inspect_err(|_| {
                metrics.recovery_failures.inc();
            })?;
        metrics
            .published_objects
            .set(metric_bytes(recovered.objects));
        metrics.published_bytes.set(metric_bytes(recovered.bytes));
        metrics.recovery_ready.set(1);
        Ok(Arc::new(pin_cache))
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheReadHandle> {
        let state = self.shard(object_id).state.read();
        let file = Arc::clone(state.objects.get(&object_id)?.published.as_ref()?);
        Some(PinCacheReadHandle {
            pin_cache: Arc::clone(self),
            object_id,
            file,
        })
    }

    fn shard_index(object_id: HummockSstableObjectId, shard_num: usize) -> usize {
        xxhash_rust::xxh64::xxh64(&object_id.as_raw_id().to_le_bytes(), 0) as usize % shard_num
    }

    fn shard(&self, object_id: HummockSstableObjectId) -> &PinCacheShard {
        &self.shards[Self::shard_index(object_id, self.shards.len())]
    }
}
