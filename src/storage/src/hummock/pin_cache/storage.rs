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

//! File ownership and logical capacity for the local cache.
//!
//! Capacity covers indexed files and in-flight reservations. Failed or cancelled uploads
//! release their reservation immediately; their physical leftovers are discovered by full GC.
//! Indexed files remain owned here until confirmed absent or deleted, even after publication
//! is withdrawn. Publications, readers and GC selections hold external file leases.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{PinCache, PinCacheFile, metric_bytes};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

#[derive(Default)]
pub(super) struct PinCacheStorageState {
    pub(super) files: HashMap<String, Arc<PinCacheFile>>,
    // Target paths only. Backend-generated temporary paths are discovered by LIST.
    pub(super) uploads: HashMap<String, u64>,
    // A subset of `files`: exact paths of explicitly discarded files.
    // Keep entries until deletion succeeds, including while readers or selections hold them.
    pub(super) pending_deletes: HashSet<String>,
    pub(super) accounted_bytes: u64,
}

impl PinCacheStorageState {
    fn account(&mut self, size: u64) {
        self.accounted_bytes = self.accounted_bytes.saturating_add(size);
        self.report();
    }

    /// Forget a confirmed absent file and release its accounted bytes at most once.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "GC scheduling is not wired yet")
    )]
    pub(super) fn remove_file(&mut self, path: &str) {
        if let Some(file) = self.files.remove(path) {
            self.pending_deletes.remove(path);
            self.accounted_bytes -= file.size;
            self.report();
        }
    }

    fn report(&self) {
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(self.accounted_bytes));
    }
}

impl PinCache {
    /// Reserve logical capacity and protect the target before starting any I/O.
    pub(super) fn try_reserve(&self, file: &PinCacheFile) -> Result<(), u64> {
        let mut state = self.storage.lock();
        if state
            .accounted_bytes
            .checked_add(file.size)
            .is_none_or(|total| total > self.capacity)
        {
            return Err(state.accounted_bytes);
        }
        assert!(
            !state.files.contains_key(&file.path) && !state.uploads.contains_key(&file.path),
            "a cache path must not be reused"
        );
        state.uploads.insert(file.path.clone(), file.size);
        state.account(file.size);
        Ok(())
    }

    /// Failed writes return their logical reservation. Physical leftovers belong to full GC.
    pub(super) fn abort_upload(&self, path: &str) {
        let mut state = self.storage.lock();
        let size = state
            .uploads
            .remove(path)
            .expect("upload owns a reservation");
        state.accounted_bytes -= size;
        state.report();
    }

    /// Transfer a successful write into the completed-object index without changing capacity.
    pub(super) fn commit_upload(&self, file: PinCacheFile) -> Arc<PinCacheFile> {
        let mut state = self.storage.lock();
        let size = state
            .uploads
            .remove(&file.path)
            .expect("upload owns a reservation");
        assert_eq!(size, file.size);
        let file = Arc::new(file);
        state.files.insert(file.path.clone(), file.clone());
        file
    }

    /// Startup inventory accounts each local object file once, before any GC pass can run.
    pub(super) fn account_existing(&self, path: String, size: u64) -> Arc<PinCacheFile> {
        let mut state = self.storage.lock();
        assert!(
            !state.files.contains_key(&path),
            "startup must inventory each path once"
        );
        let file = Arc::new(PinCacheFile {
            path: path.clone(),
            size,
        });
        state.files.insert(path, file.clone());
        state.account(size);
        file
    }

    /// Hand off an obsolete file after releasing its shard lock. This only records
    /// deletion intent; readers may still hold the file, and no GC pass is started here.
    pub(super) fn enqueue_delete(&self, file: Arc<PinCacheFile>) {
        let mut state = self.storage.lock();
        debug_assert!(
            state
                .files
                .get(&file.path)
                .is_some_and(|entry| Arc::ptr_eq(entry, &file))
        );
        state.pending_deletes.insert(file.path.clone());
    }
}
