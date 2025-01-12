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

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foyer::CacheHint;
use futures::{pin_mut, Stream, StreamExt};
use parking_lot::Mutex;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::config::StorageMemoryConfig;
use risingwave_expr::codegen::try_stream;
use risingwave_hummock_sdk::can_concat;
use risingwave_hummock_sdk::compaction_group::StateTableId;
use risingwave_hummock_sdk::key::{
    bound_table_key_range, EmptySliceRef, FullKey, TableKey, UserKey,
};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use tokio::sync::oneshot::{channel, Receiver, Sender};

use super::{HummockError, HummockResult, SstableStoreRef};
use crate::error::{StorageError, StorageResult};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::CachePolicy;
use crate::mem_table::{KeyOp, MemTableError};
use crate::monitor::MemoryCollector;
use crate::store::{OpConsistencyLevel, ReadOptions, StateStoreKeyedRow, StateStoreRead};

pub fn range_overlap<R, B>(
    search_key_range: &R,
    inclusive_start_key: &B,
    end_key: Bound<&B>,
) -> bool
where
    R: RangeBounds<B>,
    B: Ord,
{
    let (start_bound, end_bound) = (search_key_range.start_bound(), search_key_range.end_bound());

    //        RANGE
    // TABLE
    let too_left = match (start_bound, end_key) {
        (Included(range_start), Included(inclusive_end_key)) => range_start > inclusive_end_key,
        (Included(range_start), Excluded(end_key))
        | (Excluded(range_start), Included(end_key))
        | (Excluded(range_start), Excluded(end_key)) => range_start >= end_key,
        (Unbounded, _) | (_, Unbounded) => false,
    };
    // RANGE
    //        TABLE
    let too_right = match end_bound {
        Included(range_end) => range_end < inclusive_start_key,
        Excluded(range_end) => range_end <= inclusive_start_key,
        Unbounded => false,
    };

    !too_left && !too_right
}

pub fn filter_single_sst<R, B>(info: &SstableInfo, table_id: TableId, table_key_range: &R) -> bool
where
    R: RangeBounds<TableKey<B>>,
    B: AsRef<[u8]> + EmptySliceRef,
{
    let table_range = &info.key_range;
    let table_start = FullKey::decode(table_range.left.as_ref()).user_key;
    let table_end = FullKey::decode(table_range.right.as_ref()).user_key;
    let (left, right) = bound_table_key_range(table_id, table_key_range);
    let left: Bound<UserKey<&[u8]>> = left.as_ref().map(|key| key.as_ref());
    let right: Bound<UserKey<&[u8]>> = right.as_ref().map(|key| key.as_ref());
    range_overlap(
        &(left, right),
        &table_start,
        if table_range.right_exclusive {
            Bound::Excluded(&table_end)
        } else {
            Bound::Included(&table_end)
        },
    ) && info.table_ids.binary_search(&table_id.table_id()).is_ok()
}

/// Search the SST containing the specified key within a level, using binary search.
pub(crate) fn search_sst_idx(ssts: &[SstableInfo], key: UserKey<&[u8]>) -> usize {
    ssts.partition_point(|table| {
        let ord = FullKey::decode(&table.key_range.left).user_key.cmp(&key);
        ord == Ordering::Less || ord == Ordering::Equal
    })
}

/// Prune overlapping SSTs that does not overlap with a specific key range or does not overlap with
/// a specific table id. Returns the sst ids after pruning.
pub fn prune_overlapping_ssts<'a, R, B>(
    ssts: &'a [SstableInfo],
    table_id: TableId,
    table_key_range: &'a R,
) -> impl DoubleEndedIterator<Item = &'a SstableInfo>
where
    R: RangeBounds<TableKey<B>>,
    B: AsRef<[u8]> + EmptySliceRef,
{
    ssts.iter()
        .filter(move |info| filter_single_sst(info, table_id, table_key_range))
}

/// Prune non-overlapping SSTs that does not overlap with a specific key range or does not overlap
/// with a specific table id. Returns the sst ids after pruning.
#[allow(clippy::type_complexity)]
pub fn prune_nonoverlapping_ssts<'a>(
    ssts: &'a [SstableInfo],
    user_key_range: (Bound<UserKey<&'a [u8]>>, Bound<UserKey<&'a [u8]>>),
    table_id: StateTableId,
) -> impl DoubleEndedIterator<Item = &'a SstableInfo> {
    debug_assert!(can_concat(ssts));
    let start_table_idx = match user_key_range.0 {
        Included(key) | Excluded(key) => search_sst_idx(ssts, key).saturating_sub(1),
        _ => 0,
    };
    let end_table_idx = match user_key_range.1 {
        Included(key) | Excluded(key) => search_sst_idx(ssts, key).saturating_sub(1),
        _ => ssts.len().saturating_sub(1),
    };
    ssts[start_table_idx..=end_table_idx]
        .iter()
        .filter(move |sst| sst.table_ids.binary_search(&table_id).is_ok())
}

type RequestQueue = VecDeque<(Sender<MemoryTracker>, u64)>;
enum MemoryRequest {
    Ready(MemoryTracker),
    Pending(Receiver<MemoryTracker>),
}

struct MemoryLimiterInner {
    total_size: AtomicU64,
    controller: Mutex<RequestQueue>,
    has_waiter: AtomicBool,
    quota: u64,
}

impl MemoryLimiterInner {
    fn release_quota(&self, quota: u64) {
        self.total_size.fetch_sub(quota, AtomicOrdering::SeqCst);
    }

    fn add_memory(&self, quota: u64) {
        self.total_size.fetch_add(quota, AtomicOrdering::SeqCst);
    }

    fn may_notify_waiters(self: &Arc<Self>) {
        // check `has_waiter` to avoid access lock every times drop `MemoryTracker`.
        if !self.has_waiter.load(AtomicOrdering::Acquire) {
            return;
        }
        let mut notify_waiters = vec![];
        {
            let mut waiters = self.controller.lock();
            while let Some((_, quota)) = waiters.front() {
                if !self.try_require_memory(*quota) {
                    break;
                }
                let (tx, quota) = waiters.pop_front().unwrap();
                notify_waiters.push((tx, quota));
            }

            if waiters.is_empty() {
                self.has_waiter.store(false, AtomicOrdering::Release);
            }
        }

        for (tx, quota) in notify_waiters {
            let _ = tx.send(MemoryTracker::new(self.clone(), quota));
        }
    }

    fn try_require_memory(&self, quota: u64) -> bool {
        let mut current_quota = self.total_size.load(AtomicOrdering::Acquire);
        while self.permit_quota(current_quota, quota) {
            match self.total_size.compare_exchange(
                current_quota,
                current_quota + quota,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(old_quota) => {
                    current_quota = old_quota;
                }
            }
        }
        false
    }

    fn require_memory(self: &Arc<Self>, quota: u64) -> MemoryRequest {
        let mut waiters = self.controller.lock();
        let first_req = waiters.is_empty();
        if first_req {
            // When this request is the first waiter but the previous `MemoryTracker` is just release a large quota, it may skip notifying this waiter because it has checked `has_waiter` and found it was false. So we must set it one and retry `try_require_memory` again to avoid deadlock.
            self.has_waiter.store(true, AtomicOrdering::Release);
        }
        // We must require again with lock because some other MemoryTracker may drop just after this thread gets mutex but before it enters queue.
        if self.try_require_memory(quota) {
            if first_req {
                self.has_waiter.store(false, AtomicOrdering::Release);
            }
            return MemoryRequest::Ready(MemoryTracker::new(self.clone(), quota));
        }
        let (tx, rx) = channel();
        waiters.push_back((tx, quota));
        MemoryRequest::Pending(rx)
    }

    fn permit_quota(&self, current_quota: u64, _request_quota: u64) -> bool {
        current_quota <= self.quota
    }
}

pub struct MemoryLimiter {
    inner: Arc<MemoryLimiterInner>,
}

impl Debug for MemoryLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryLimiter")
            .field("quota", &self.inner.quota)
            .field("usage", &self.inner.total_size)
            .finish()
    }
}

pub struct MemoryTracker {
    limiter: Arc<MemoryLimiterInner>,
    quota: Option<u64>,
}
impl MemoryTracker {
    fn new(limiter: Arc<MemoryLimiterInner>, quota: u64) -> Self {
        Self {
            limiter,
            quota: Some(quota),
        }
    }
}

impl Debug for MemoryTracker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryTracker")
            .field("quota", &self.quota)
            .finish()
    }
}

impl MemoryLimiter {
    pub fn unlimit() -> Arc<Self> {
        Arc::new(Self::new(u64::MAX))
    }

    pub fn new(quota: u64) -> Self {
        Self {
            inner: Arc::new(MemoryLimiterInner {
                total_size: AtomicU64::new(0),
                controller: Mutex::new(VecDeque::default()),
                has_waiter: AtomicBool::new(false),
                quota,
            }),
        }
    }

    pub fn try_require_memory(&self, quota: u64) -> Option<MemoryTracker> {
        if self.inner.try_require_memory(quota) {
            Some(MemoryTracker::new(self.inner.clone(), quota))
        } else {
            None
        }
    }

    pub fn get_memory_usage(&self) -> u64 {
        self.inner.total_size.load(AtomicOrdering::Acquire)
    }

    pub fn quota(&self) -> u64 {
        self.inner.quota
    }

    pub fn must_require_memory(&self, quota: u64) -> MemoryTracker {
        if !self.inner.try_require_memory(quota) {
            self.inner.add_memory(quota);
        }

        MemoryTracker::new(self.inner.clone(), quota)
    }
}

impl MemoryLimiter {
    pub async fn require_memory(&self, quota: u64) -> MemoryTracker {
        match self.inner.require_memory(quota) {
            MemoryRequest::Ready(tracker) => tracker,
            MemoryRequest::Pending(rx) => rx.await.unwrap(),
        }
    }
}

impl MemoryTracker {
    pub fn try_increase_memory(&mut self, target: u64) -> bool {
        let quota = self.quota.unwrap();
        if quota >= target {
            return true;
        }
        if self.limiter.try_require_memory(target - quota) {
            self.quota = Some(target);
            true
        } else {
            false
        }
    }
}

// We must notify waiters outside `MemoryTracker` to avoid dead-lock and loop-owner.
impl Drop for MemoryTracker {
    fn drop(&mut self) {
        if let Some(quota) = self.quota.take() {
            self.limiter.release_quota(quota);
            self.limiter.may_notify_waiters();
        }
    }
}

/// Check whether the items in `sub_iter` is a subset of the items in `full_iter`, and meanwhile
/// preserve the order.
pub fn check_subset_preserve_order<T: Eq>(
    sub_iter: impl Iterator<Item = T>,
    mut full_iter: impl Iterator<Item = T>,
) -> bool {
    for sub_iter_item in sub_iter {
        let mut found = false;
        for full_iter_item in full_iter.by_ref() {
            if sub_iter_item == full_iter_item {
                found = true;
                break;
            }
        }
        if !found {
            return false;
        }
    }
    true
}

static SANITY_CHECK_ENABLED: AtomicBool = AtomicBool::new(cfg!(debug_assertions));

/// This function is intended to be called during compute node initialization if the storage
/// sanity check is not desired. This controls a global flag so only need to be called once
/// if need to disable the sanity check.
pub fn disable_sanity_check() {
    SANITY_CHECK_ENABLED.store(false, AtomicOrdering::Release);
}

pub(crate) fn sanity_check_enabled() -> bool {
    SANITY_CHECK_ENABLED.load(AtomicOrdering::Acquire)
}

/// Make sure the key to insert should not exist in storage.
pub(crate) async fn do_insert_sanity_check(
    key: &TableKey<Bytes>,
    value: &Bytes,
    inner: &impl StateStoreRead,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    if let OpConsistencyLevel::Inconsistent = op_consistency_level {
        return Ok(());
    }
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CacheHint::Normal),
        ..Default::default()
    };
    let stored_value = inner.get(key.clone(), read_options).await?;

    if let Some(stored_value) = stored_value {
        return Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Insert(stored_value),
            new: KeyOp::Insert(value.clone()),
        })
        .into());
    }
    Ok(())
}

/// Make sure that the key to delete should exist in storage and the value should be matched.
pub(crate) async fn do_delete_sanity_check(
    key: &TableKey<Bytes>,
    old_value: &Bytes,
    inner: &impl StateStoreRead,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    let OpConsistencyLevel::ConsistentOldValue {
        check_old_value: old_value_checker,
        ..
    } = op_consistency_level
    else {
        return Ok(());
    };
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CacheHint::Normal),
        ..Default::default()
    };
    match inner.get(key.clone(), read_options).await? {
        None => Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Delete(Bytes::default()),
            new: KeyOp::Delete(old_value.clone()),
        })
        .into()),
        Some(stored_value) => {
            if !old_value_checker(&stored_value, old_value) {
                Err(Box::new(MemTableError::InconsistentOperation {
                    key: key.clone(),
                    prev: KeyOp::Insert(stored_value),
                    new: KeyOp::Delete(old_value.clone()),
                })
                .into())
            } else {
                Ok(())
            }
        }
    }
}

/// Make sure that the key to update should exist in storage and the value should be matched
pub(crate) async fn do_update_sanity_check(
    key: &TableKey<Bytes>,
    old_value: &Bytes,
    new_value: &Bytes,
    inner: &impl StateStoreRead,
    table_id: TableId,
    table_option: TableOption,
    op_consistency_level: &OpConsistencyLevel,
) -> StorageResult<()> {
    let OpConsistencyLevel::ConsistentOldValue {
        check_old_value: old_value_checker,
        ..
    } = op_consistency_level
    else {
        return Ok(());
    };
    let read_options = ReadOptions {
        retention_seconds: table_option.retention_seconds,
        table_id,
        cache_policy: CachePolicy::Fill(CacheHint::Normal),
        ..Default::default()
    };

    match inner.get(key.clone(), read_options).await? {
        None => Err(Box::new(MemTableError::InconsistentOperation {
            key: key.clone(),
            prev: KeyOp::Delete(Bytes::default()),
            new: KeyOp::Update((old_value.clone(), new_value.clone())),
        })
        .into()),
        Some(stored_value) => {
            if !old_value_checker(&stored_value, old_value) {
                Err(Box::new(MemTableError::InconsistentOperation {
                    key: key.clone(),
                    prev: KeyOp::Insert(stored_value),
                    new: KeyOp::Update((old_value.clone(), new_value.clone())),
                })
                .into())
            } else {
                Ok(())
            }
        }
    }
}

pub fn cmp_delete_range_left_bounds(a: Bound<&Bytes>, b: Bound<&Bytes>) -> Ordering {
    match (a, b) {
        // only right bound of delete range can be `Unbounded`
        (Unbounded, _) | (_, Unbounded) => unreachable!(),
        (Included(x), Included(y)) | (Excluded(x), Excluded(y)) => x.cmp(y),
        (Included(x), Excluded(y)) => x.cmp(y).then(Ordering::Less),
        (Excluded(x), Included(y)) => x.cmp(y).then(Ordering::Greater),
    }
}

pub(crate) fn validate_delete_range(left: &Bound<Bytes>, right: &Bound<Bytes>) -> bool {
    match (left, right) {
        // only right bound of delete range can be `Unbounded`
        (Unbounded, _) => unreachable!(),
        (_, Unbounded) => true,
        (Included(x), Included(y)) => x <= y,
        (Included(x), Excluded(y)) | (Excluded(x), Included(y)) | (Excluded(x), Excluded(y)) => {
            x < y
        }
    }
}

#[expect(dead_code)]
pub(crate) fn filter_with_delete_range<'a>(
    kv_iter: impl Iterator<Item = (TableKey<Bytes>, KeyOp)> + 'a,
    mut delete_ranges_iter: impl Iterator<Item = &'a (Bound<Bytes>, Bound<Bytes>)> + 'a,
) -> impl Iterator<Item = (TableKey<Bytes>, KeyOp)> + 'a {
    let mut range = delete_ranges_iter.next();
    if let Some((range_start, range_end)) = range {
        assert!(
            validate_delete_range(range_start, range_end),
            "range_end {:?} smaller than range_start {:?}",
            range_start,
            range_end
        );
    }
    kv_iter.filter(move |(ref key, _)| {
        if let Some(range_bound) = range {
            if cmp_delete_range_left_bounds(Included(&key.0), range_bound.0.as_ref())
                == Ordering::Less
            {
                true
            } else if range_bound.contains(key.as_ref()) {
                false
            } else {
                // Key has exceeded the current key range. Advance to the next range.
                loop {
                    range = delete_ranges_iter.next();
                    if let Some(range_bound) = range {
                        assert!(
                            validate_delete_range(&range_bound.0, &range_bound.1),
                            "range_end {:?} smaller than range_start {:?}",
                            range_bound.0,
                            range_bound.1
                        );
                        if cmp_delete_range_left_bounds(Included(key), range_bound.0.as_ref())
                            == Ordering::Less
                        {
                            // Not fall in the next delete range
                            break true;
                        } else if range_bound.contains(key.as_ref()) {
                            // Fall in the next delete range
                            break false;
                        } else {
                            // Exceed the next delete range. Go to the next delete range if there is
                            // any in the next loop
                            continue;
                        }
                    } else {
                        // No more delete range.
                        break true;
                    }
                }
            }
        } else {
            true
        }
    })
}

/// Wait for the `committed_epoch` of `table_id` to reach `wait_epoch`.
///
/// When the `table_id` does not exist in the latest version, we assume that
/// the table is not created yet, and will wait until the table is created.
pub(crate) async fn wait_for_epoch(
    notifier: &tokio::sync::watch::Sender<PinnedVersion>,
    wait_epoch: u64,
    table_id: TableId,
) -> StorageResult<()> {
    let mut prev_committed_epoch = None;
    let prev_committed_epoch = &mut prev_committed_epoch;
    wait_for_update(
        notifier,
        |version| {
            let committed_epoch = version.table_committed_epoch(table_id);
            let ret = if let Some(committed_epoch) = committed_epoch {
                if committed_epoch >= wait_epoch {
                    Ok(true)
                } else {
                    Ok(false)
                }
            } else if prev_committed_epoch.is_none() {
                Ok(false)
            } else {
                Err(HummockError::wait_epoch(format!(
                    "table {} has been dropped",
                    table_id
                )))
            };
            *prev_committed_epoch = committed_epoch;
            ret
        },
        || {
            format!(
                "wait_for_epoch: epoch: {}, table_id: {}",
                wait_epoch, table_id
            )
        },
    )
    .await?;
    Ok(())
}

pub(crate) async fn wait_for_update(
    notifier: &tokio::sync::watch::Sender<PinnedVersion>,
    mut inspect_fn: impl FnMut(&PinnedVersion) -> HummockResult<bool>,
    mut periodic_debug_info: impl FnMut() -> String,
) -> HummockResult<()> {
    let mut receiver = notifier.subscribe();
    if inspect_fn(&receiver.borrow_and_update())? {
        return Ok(());
    }
    let start_time = Instant::now();
    loop {
        match tokio::time::timeout(Duration::from_secs(30), receiver.changed()).await {
            Err(_) => {
                // The reason that we need to retry here is batch scan in
                // chain/rearrange_chain is waiting for an
                // uncommitted epoch carried by the CreateMV barrier, which
                // can take unbounded time to become committed and propagate
                // to the CN. We should consider removing the retry as well as wait_epoch
                // for chain/rearrange_chain if we enforce
                // chain/rearrange_chain to be scheduled on the same
                // CN with the same distribution as the upstream MV.
                // See #3845 for more details.
                tracing::warn!(
                    info = periodic_debug_info(),
                    elapsed = ?start_time.elapsed(),
                    "timeout when waiting for version update",
                );
                continue;
            }
            Ok(Err(_)) => {
                return Err(HummockError::wait_epoch("tx dropped"));
            }
            Ok(Ok(_)) => {
                if inspect_fn(&receiver.borrow_and_update())? {
                    return Ok(());
                }
            }
        }
    }
}

pub struct HummockMemoryCollector {
    sstable_store: SstableStoreRef,
    limiter: Arc<MemoryLimiter>,
    storage_memory_config: StorageMemoryConfig,
}

impl HummockMemoryCollector {
    pub fn new(
        sstable_store: SstableStoreRef,
        limiter: Arc<MemoryLimiter>,
        storage_memory_config: StorageMemoryConfig,
    ) -> Self {
        Self {
            sstable_store,
            limiter,
            storage_memory_config,
        }
    }
}

impl MemoryCollector for HummockMemoryCollector {
    fn get_meta_memory_usage(&self) -> u64 {
        self.sstable_store.meta_cache().memory().usage() as _
    }

    fn get_data_memory_usage(&self) -> u64 {
        self.sstable_store.block_cache().memory().usage() as _
    }

    fn get_uploading_memory_usage(&self) -> u64 {
        self.limiter.get_memory_usage()
    }

    fn get_prefetch_memory_usage(&self) -> usize {
        self.sstable_store.get_prefetch_memory_usage()
    }

    fn get_meta_cache_memory_usage_ratio(&self) -> f64 {
        self.sstable_store.meta_cache().memory().usage() as f64
            / self.sstable_store.meta_cache().memory().capacity() as f64
    }

    fn get_block_cache_memory_usage_ratio(&self) -> f64 {
        self.sstable_store.block_cache().memory().usage() as f64
            / self.sstable_store.block_cache().memory().capacity() as f64
    }

    fn get_shared_buffer_usage_ratio(&self) -> f64 {
        self.limiter.get_memory_usage() as f64
            / (self.storage_memory_config.shared_buffer_capacity_mb * 1024 * 1024) as f64
    }
}

#[try_stream(ok = StateStoreKeyedRow, error = StorageError)]
pub(crate) async fn merge_stream<'a>(
    mem_table_iter: impl Iterator<Item = (&'a TableKey<Bytes>, &'a KeyOp)> + 'a,
    inner_stream: impl Stream<Item = StorageResult<StateStoreKeyedRow>> + 'static,
    table_id: TableId,
    epoch: u64,
    rev: bool,
) {
    let inner_stream = inner_stream.peekable();
    pin_mut!(inner_stream);

    let mut mem_table_iter = mem_table_iter.fuse().peekable();

    loop {
        match (inner_stream.as_mut().peek().await, mem_table_iter.peek()) {
            (None, None) => break,
            // The mem table side has come to an end, return data from the shared storage.
            (Some(_), None) => {
                let (key, value) = inner_stream.next().await.unwrap()?;
                yield (key, value)
            }
            // The stream side has come to an end, return data from the mem table.
            (None, Some(_)) => {
                let (key, key_op) = mem_table_iter.next().unwrap();
                match key_op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => {
                        yield (FullKey::new(table_id, key.clone(), epoch), value.clone())
                    }
                    _ => {}
                }
            }
            (Some(Ok((inner_key, _))), Some((mem_table_key, _))) => {
                debug_assert_eq!(inner_key.user_key.table_id, table_id);
                let mut ret = inner_key.user_key.table_key.cmp(mem_table_key);
                if rev {
                    ret = ret.reverse();
                }
                match ret {
                    Ordering::Less => {
                        // yield data from storage
                        let (key, value) = inner_stream.next().await.unwrap()?;
                        yield (key, value);
                    }
                    Ordering::Equal => {
                        // both memtable and storage contain the key, so we advance both
                        // iterators and return the data in memory.

                        let (_, key_op) = mem_table_iter.next().unwrap();
                        let (key, old_value_in_inner) = inner_stream.next().await.unwrap()?;
                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (key.clone(), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update((old_value, new_value)) => {
                                debug_assert!(old_value == &old_value_in_inner);

                                yield (key, new_value.clone());
                            }
                        }
                    }
                    Ordering::Greater => {
                        // yield data from mem table
                        let (key, key_op) = mem_table_iter.next().unwrap();

                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (FullKey::new(table_id, key.clone(), epoch), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update(_) => unreachable!(
                                "memtable update should always be paired with a storage key"
                            ),
                        }
                    }
                }
            }
            (Some(Err(_)), Some(_)) => {
                // Throw the error.
                return Err(inner_stream.next().await.unwrap().unwrap_err());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::{poll_fn, Future};
    use std::sync::Arc;
    use std::task::Poll;

    use futures::future::join_all;
    use futures::FutureExt;
    use rand::random;

    use crate::hummock::utils::MemoryLimiter;

    async fn assert_pending(future: &mut (impl Future + Unpin)) {
        for _ in 0..10 {
            assert!(poll_fn(|cx| Poll::Ready(future.poll_unpin(cx)))
                .await
                .is_pending());
        }
    }

    #[tokio::test]
    async fn test_loose_memory_limiter() {
        let quota = 5;
        let memory_limiter = MemoryLimiter::new(quota);
        drop(memory_limiter.require_memory(6).await);
        let tracker1 = memory_limiter.require_memory(3).await;
        assert_eq!(3, memory_limiter.get_memory_usage());
        let tracker2 = memory_limiter.require_memory(4).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        let mut future = memory_limiter.require_memory(5).boxed();
        assert_pending(&mut future).await;
        assert_eq!(7, memory_limiter.get_memory_usage());
        drop(tracker1);
        let tracker3 = future.await;
        assert_eq!(9, memory_limiter.get_memory_usage());
        drop(tracker2);
        assert_eq!(5, memory_limiter.get_memory_usage());
        drop(tracker3);
        assert_eq!(0, memory_limiter.get_memory_usage());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_multi_thread_acquire_memory() {
        const QUOTA: u64 = 10;
        let memory_limiter = Arc::new(MemoryLimiter::new(200));
        let mut handles = vec![];
        for _ in 0..40 {
            let limiter = memory_limiter.clone();
            let h = tokio::spawn(async move {
                let mut buffers = vec![];
                let mut current_buffer_usage = (random::<usize>() % 8) + 2;
                for _ in 0..1000 {
                    if buffers.len() < current_buffer_usage
                        && let Some(tracker) = limiter.try_require_memory(QUOTA)
                    {
                        buffers.push(tracker);
                    } else {
                        buffers.clear();
                        current_buffer_usage = (random::<usize>() % 8) + 2;
                        let req = limiter.require_memory(QUOTA);
                        match tokio::time::timeout(std::time::Duration::from_millis(1), req).await {
                            Ok(tracker) => {
                                buffers.push(tracker);
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }
                    let sleep_time = random::<u64>() % 3 + 1;
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_time)).await;
                }
            });
            handles.push(h);
        }
        let h = join_all(handles);
        let _ = h.await;
    }
}
