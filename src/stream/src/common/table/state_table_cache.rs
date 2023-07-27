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

use std::cmp::Reverse;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DefaultOrdered, ScalarImpl};
use crate::common::cache::{StateCache, TopNStateCache};

/// The watermark cache key is just an OwnedRow wrapped in `DefaultOrdered`.
/// This is because we want to use the `DefaultOrdered` implementation of `Ord`.
/// The assumption is that the watermark column is the first column in the row.
/// So it should automatically be ordered by the watermark column.
/// We disregard the ordering of the remaining PK datums.
type WatermarkCacheKey = Reverse<DefaultOrdered<OwnedRow>>;

/// Updates to Cache:
/// -----------------
/// INSERT
///    A. Cache evicted. Update cached value.
///    B. Cache uninitialized. Initialize cache, insert into TopNCache.
///    C. Cache not empty. Insert into TopNCache.
///
/// DELETE
///    A. Matches lowest value pk. Remove lowest value. Mark cache as Evicted.
///       Later on Barrier we will refresh the cache with table scan.
///       Since on barrier we will clean up all values before watermark,
///       We have less rows to scan.
///    B. Does not match. Do nothing.
///
/// UPDATE
///    A. Do delete then insert.
///
/// BARRIER
///    State table commit. See below.
///
/// STATE TABLE COMMIT
///    Update `prev_cleaned_watermark`.
///
/// Using the Cache:
/// ----------------
/// When state cleaning,
/// if watermark_to_be_cleaned < smallest val OR no value in cache + cache is synced: No need issue delete range.
/// if watermark_to_be_cleaned => smallest val OR cache not synced: Issue delete ranges.
///
/// Refreshing the cache:
/// ---------------------
/// On barrier, do table scan from `[most_recently_cleaned_watermark, +inf)`.
/// Take the Top N rows and insert into cache.
/// This has to be implemented in `state_table`.
///
/// We don't need to store any values, just the keys.
/// TODO(kwannoel): Optimization: We can use cache to do point delete rather than range delete.
/// FIXME(kwannoel): This should be a trait.
/// Then only when building state table with watermark we will initialize it.
/// Otherwise point it to a no-op implementation.
/// TODO(kwannoel): Add tests for it.
#[derive(Clone)]
pub(crate) struct StateTableWatermarkCache {
    pk_indices: Vec<usize>,
    inner: TopNStateCache<WatermarkCacheKey, ()>,
}

pub(crate) enum StateTableWatermarkCacheEntry {
    /// There is some lowest value.
    Lowest(ScalarImpl),
    /// Cache out of sync, we can't rely on it.
    NotSynced,
    /// No values in cache
    Empty,
}

impl StateTableWatermarkCacheEntry {
    pub fn not_synced(&self) -> bool {
        matches!(self, Self::NotSynced)
    }
}

impl StateTableWatermarkCache {
    pub fn new(pk_indices: Vec<usize>) -> Self {
        Self {
            pk_indices,
            inner: TopNStateCache::new(2048), // TODO: This number is arbitrary
        }
    }

    /// Handle inserts / updates.
    /// TODO: Ignore rows with null in watermark col.
    pub fn handle_insert(&mut self, row: impl Row) {
        todo!()
    }

    /// Handle inserts / updates.
    /// TODO: Ignore rows with null in watermark col.
    pub fn handle_delete(&mut self, row: impl Row) {
        todo!()
    }

    /// Handle inserts / updates.
    pub fn handle_commit(&mut self, row: impl Row) {
        todo!()
    }

    /// Get the lowest value.
    pub fn get_lowest(&self) -> StateTableWatermarkCacheEntry {
        todo!()
    }

    /// Insert a new value.
    /// FIXME: WE should just copy the whole interface of StateCAche impl
    /// for top n state cache.
    pub fn insert(&mut self, key: impl Row) -> Option<()> {
        self.inner.insert(Reverse(DefaultOrdered(key.into_owned_row())), ())
    }
}

