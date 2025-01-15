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

use std::ops::Bound::*;

use bytes::Bytes;
use more_asserts::debug_assert_le;
use risingwave_hummock_sdk::key::{FullKey, UserKey, UserKeyRange};
use risingwave_hummock_sdk::{EpochWithGap, HummockEpoch};

use crate::hummock::iterator::{Backward, HummockIterator};
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::value::HummockValue;
use crate::hummock::HummockResult;
use crate::monitor::StoreLocalStatistic;

/// [`BackwardUserIterator`] can be used by user directly.
pub struct BackwardUserIterator<I: HummockIterator<Direction = Backward>> {
    /// Inner table iterator.
    iterator: I,

    /// We just met a new key
    just_met_new_key: bool,

    /// Last user key
    last_key: FullKey<Bytes>,

    /// Last user value
    last_val: Bytes,

    /// Last user key value is deleted
    last_delete: bool,

    /// Flag for whether the iterator reaches over the right end of the range.
    out_of_range: bool,

    /// Start and end bounds of user key.
    key_range: UserKeyRange,

    /// Only reads values if `epoch <= self.read_epoch`.
    read_epoch: HummockEpoch,

    /// Only reads values if `ts > self.min_epoch`. use for ttl
    min_epoch: HummockEpoch,

    /// Ensures the SSTs needed by `iterator` won't be vacuumed.
    _version: Option<PinnedVersion>,

    /// Store scan statistic
    stats: StoreLocalStatistic,
}

impl<I: HummockIterator<Direction = Backward>> BackwardUserIterator<I> {
    /// Creates [`BackwardUserIterator`] with given `read_epoch`.
    pub fn new(
        iterator: I,
        key_range: UserKeyRange,
        read_epoch: u64,
        min_epoch: u64,
        version: Option<PinnedVersion>,
    ) -> Self {
        Self {
            iterator,
            out_of_range: false,
            key_range,
            just_met_new_key: false,
            last_key: FullKey::default(),
            last_val: Bytes::new(),
            last_delete: true,
            read_epoch,
            min_epoch,
            stats: StoreLocalStatistic::default(),
            _version: version,
        }
    }

    fn out_of_range(&self, key: UserKey<&[u8]>) -> bool {
        match &self.key_range.0 {
            Included(begin_key) => key < begin_key.as_ref(),
            Excluded(begin_key) => key <= begin_key.as_ref(),
            Unbounded => false,
        }
    }

    fn reset(&mut self) {
        self.last_key = FullKey::default();
        self.just_met_new_key = false;
        self.last_delete = true;
        self.out_of_range = false;
    }

    /// Gets the iterator move to the next step.
    ///
    /// Returned result:
    /// - if `Ok(())` is returned, it means that the iterator successfully move to the next position
    ///   (may reach to the end and thus not valid)
    /// - if `Err(_) ` is returned, it means that some error happened.
    pub async fn next(&mut self) -> HummockResult<()> {
        // We need to deal with three cases:
        // 1. current key == last key.
        //    Since current key must have an epoch newer than the one of the last key,
        //    we assign current kv as the new last kv and also inherit its status of deletion, and
        // continue.
        //
        // 2. current key != last key.
        //    We have to make a decision for the last key.
        //    a. If it is not deleted, we stop.
        //    b. Otherwise, we continue to find the next new key.
        //
        // 3. `self.iterator` invalid. The case is the same as 2. However, option b is invalid now.
        // We just stop. Without further `next`, `BackwardUserIterator` is still valid.

        // We remark that whether `self.iterator` is valid and `BackwardUserIterator` is valid can
        // be different even if we leave `out_of_range` out of consideration. This diffs
        // from `UserIterator` because we always make a decision about the past key only
        // when we enter a new state, such as encountering a new key, or `self.iterator`
        // turning invalid.

        if !self.iterator.is_valid() {
            // We abuse `last_delete` to indicate that we are indeed invalid now, i.e. run out of kv
            // pairs.
            self.last_delete = true;
            return Ok(());
        }

        while self.iterator.is_valid() {
            let full_key = self.iterator.key();
            let epoch = full_key.epoch_with_gap.pure_epoch();
            let key = &full_key.user_key;

            if epoch > self.min_epoch && epoch <= self.read_epoch {
                if self.just_met_new_key {
                    self.last_key = full_key.copy_into();
                    self.just_met_new_key = false;
                    // If we encounter an out-of-range key, stop early.
                    if self.out_of_range(self.last_key.user_key.as_ref()) {
                        self.out_of_range = true;
                        break;
                    }
                } else if self.last_key.user_key.as_ref() != *key {
                    if !self.last_delete {
                        // We remark that we don't check `out_of_range` here as the other two cases
                        // covered all situation. 2(a)
                        self.just_met_new_key = true;
                        self.stats.processed_key_count += 1;
                        return Ok(());
                    } else {
                        // 2(b)
                        self.last_key = full_key.copy_into();
                        // If we encounter an out-of-range key, stop early.
                        if self.out_of_range(self.last_key.user_key.as_ref()) {
                            self.out_of_range = true;
                            break;
                        }
                    }
                } else {
                    self.stats.skip_multi_version_key_count += 1;
                }
                // TODO: Since the real world workload may follow power law or 20/80 rule, or
                // whatever name. We may directly seek to the next key if we have
                // been seeing the same key for too many times.

                // 1 and 2(a)
                match self.iterator.value() {
                    HummockValue::Put(val) => {
                        // TODO: unconditionally set the last key may lead to redundant copies
                        self.last_key = full_key.copy_into();
                        self.last_val = Bytes::copy_from_slice(val);
                        self.last_delete = false;
                    }
                    HummockValue::Delete => {
                        self.last_delete = true;
                    }
                }
            }
            self.iterator.next().await?;
        }
        Ok(()) // not valid, EOF
    }

    /// Returns the key with the newest version. Thus no version in it, and only the `user_key` will
    /// be returned.
    ///
    /// The returned key is de-duplicated and thus it will not output the same key, unless the
    /// `rewind` or `seek` methods are called.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn key(&self) -> FullKey<&[u8]> {
        assert!(self.is_valid());
        self.last_key.to_ref()
    }

    /// The returned value is in the form of user value.
    ///
    /// Note: before call the function you need to ensure that the iterator is valid.
    pub fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        &self.last_val
    }

    /// Resets the iterating position to the beginning.
    pub async fn rewind(&mut self) -> HummockResult<()> {
        // Handle range scan
        match &self.key_range.1 {
            Included(end_key) | Excluded(end_key) => {
                let full_key = FullKey {
                    user_key: end_key.as_ref(),
                    epoch_with_gap: EpochWithGap::new_min_epoch(),
                };
                self.iterator.seek(full_key).await?;
            }
            Unbounded => self.iterator.rewind().await?,
        };

        // Handle multi-version
        self.reset();
        // Handle range scan when key < begin_key
        self.next().await?;
        if let Excluded(end_key) = &self.key_range.1
            && self.is_valid()
            && self.key().user_key == end_key.as_ref()
        {
            self.next().await?;
        }
        Ok(())
    }

    /// Resets the iterating position to the first position where the key >= provided key.
    pub async fn seek(&mut self, user_key: UserKey<&[u8]>) -> HummockResult<()> {
        // Handle range scan when key > end_key
        let seek_key = match &self.key_range.1 {
            Included(end_key) | Excluded(end_key) => {
                let end_key = end_key.as_ref();
                if end_key < user_key {
                    end_key
                } else {
                    user_key
                }
            }
            Unbounded => user_key,
        };
        let full_key = FullKey {
            user_key: seek_key,
            epoch_with_gap: EpochWithGap::new_min_epoch(),
        };
        self.iterator.seek(full_key).await?;

        // Handle multi-version
        self.reset();
        // Handle range scan when key < begin_key
        self.next().await?;
        if let Excluded(end_key) = &self.key_range.1
            && self.is_valid()
            && self.key().user_key == end_key.as_ref()
        {
            debug_assert_le!(end_key.as_ref(), user_key);
            self.next().await?;
        }
        Ok(())
    }

    /// Indicates whether the iterator can be used.
    pub fn is_valid(&self) -> bool {
        // Handle range scan
        // key <= end_key is guaranteed by seek/rewind function
        // We remark that there are only three cases out of four combinations:
        // (iterator valid && last_delete false) is impossible
        let has_enough_input = self.iterator.is_valid() || !self.last_delete;
        has_enough_input && (!self.out_of_range)
    }

    pub fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
        self.iterator.collect_local_statistic(stats);
    }
}

#[cfg(test)]
impl<I: HummockIterator<Direction = Backward>> BackwardUserIterator<I> {
    /// Creates [`BackwardUserIterator`] with maximum epoch.
    pub(crate) fn for_test(iterator: I, key_range: UserKeyRange) -> Self {
        Self::new(iterator, key_range, HummockEpoch::MAX, 0, None)
    }

    /// Creates [`BackwardUserIterator`] with maximum epoch.
    pub(crate) fn with_min_epoch(
        iterator: I,
        key_range: UserKeyRange,
        min_epoch: HummockEpoch,
    ) -> Self {
        Self::new(iterator, key_range, HummockEpoch::MAX, min_epoch, None)
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Reverse;
    use std::collections::BTreeMap;
    use std::ops::Bound::{self, *};

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::{test_epoch, EpochExt};
    use risingwave_hummock_sdk::key::prev_key;
    use risingwave_hummock_sdk::sstable_info::SstableInfo;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base,
        gen_iterator_test_sstable_from_kv_pair, gen_iterator_test_sstable_with_incr_epoch,
        iterator_test_bytes_key_of, iterator_test_bytes_key_of_epoch,
        iterator_test_bytes_user_key_of, iterator_test_user_key_of, iterator_test_value_of,
        mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::MergeIterator;
    use crate::hummock::test_utils::gen_test_sstable;
    use crate::hummock::{BackwardSstableIterator, SstableStoreRef, TableHolder};

    #[tokio::test]
    async fn test_backward_user_basic() {
        let sstable_store = mock_sstable_store().await;
        let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let (table2, sstable_info_2) = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;

        let backward_iters = vec![
            BackwardSstableIterator::new(table0, sstable_store.clone(), &sstable_info_0),
            BackwardSstableIterator::new(table1, sstable_store.clone(), &sstable_info_1),
            BackwardSstableIterator::new(table2, sstable_store, &sstable_info_2),
        ];

        let mi = MergeIterator::new(backward_iters);
        let mut ui = BackwardUserIterator::for_test(mi, (Unbounded, Unbounded));
        let mut i = 3 * TEST_KEYS_COUNT;
        ui.rewind().await.unwrap();
        while ui.is_valid() {
            let key = ui.key();
            let val = ui.value();
            assert_eq!(key, iterator_test_bytes_key_of(i).to_ref());
            assert_eq!(val, iterator_test_value_of(i).as_slice());
            i -= 1;
            ui.next().await.unwrap();
            if i == 0 {
                assert!(!ui.is_valid());
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_backward_user_seek() {
        let sstable_store = mock_sstable_store().await;
        let (table0, sstable_info_0) = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let (table1, sstable_info_1) = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let (table2, sstable_info_2) = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let backward_iters = vec![
            BackwardSstableIterator::new(table0, sstable_store.clone(), &sstable_info_0),
            BackwardSstableIterator::new(table1, sstable_store.clone(), &sstable_info_1),
            BackwardSstableIterator::new(table2, sstable_store, &sstable_info_2),
        ];

        let bmi = MergeIterator::new(backward_iters);
        let mut bui = BackwardUserIterator::for_test(bmi, (Unbounded, Unbounded));

        // right edge case
        bui.seek(iterator_test_user_key_of(0).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // normal case
        bui.seek(iterator_test_user_key_of(TEST_KEYS_COUNT + 4).as_ref())
            .await
            .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(v, iterator_test_value_of(TEST_KEYS_COUNT + 4).as_slice());
        assert_eq!(k, iterator_test_bytes_key_of(TEST_KEYS_COUNT + 4).to_ref());
        bui.seek(iterator_test_user_key_of(2 * TEST_KEYS_COUNT + 5).as_ref())
            .await
            .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(
            v,
            iterator_test_value_of(2 * TEST_KEYS_COUNT + 5).as_slice()
        );
        assert_eq!(
            k,
            iterator_test_bytes_key_of(2 * TEST_KEYS_COUNT + 5).to_ref()
        );

        // left edge case
        bui.seek(iterator_test_user_key_of(3 * TEST_KEYS_COUNT).as_ref())
            .await
            .unwrap();
        let k = bui.key();
        let v = bui.value();
        assert_eq!(v, iterator_test_value_of(3 * TEST_KEYS_COUNT).as_slice());
        assert_eq!(k, iterator_test_bytes_key_of(3 * TEST_KEYS_COUNT).to_ref());
    }

    #[tokio::test]
    async fn test_backward_user_delete() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (1, 300, HummockValue::delete()),
            (2, 100, HummockValue::put(iterator_test_value_of(2))),
        ];
        let (table0, sstable_info_0) =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;

        let kv_pairs = vec![
            (1, 400, HummockValue::put(iterator_test_value_of(1))),
            (2, 200, HummockValue::delete()),
        ];
        let (table1, sstable_info_1) =
            gen_iterator_test_sstable_from_kv_pair(1, kv_pairs, sstable_store.clone()).await;
        let backward_iters = vec![
            BackwardSstableIterator::new(table0, sstable_store.clone(), &sstable_info_0),
            BackwardSstableIterator::new(table1, sstable_store, &sstable_info_1),
        ];
        let bmi = MergeIterator::new(backward_iters);
        let mut bui = BackwardUserIterator::for_test(bmi, (Unbounded, Unbounded));

        bui.rewind().await.unwrap();

        // verify
        let k = bui.key();
        let v = bui.value();

        assert_eq!(k, iterator_test_bytes_key_of_epoch(1, 400).to_ref());
        assert_eq!(v, iterator_test_value_of(1).as_slice());

        // only one valid kv pair
        bui.next().await.unwrap();
        assert!(!bui.is_valid());
    }

    // left..=end
    #[tokio::test]
    async fn test_backward_user_range_inclusive() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 400, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::put(iterator_test_value_of(2))),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 300, HummockValue::put(iterator_test_value_of(7))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let (sstable, sstable_info) =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let backward_iters = vec![BackwardSstableIterator::new(
            sstable,
            sstable_store,
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);

        let begin_key = Included(iterator_test_bytes_user_key_of(2));
        let end_key = Included(iterator_test_bytes_user_key_of(7));

        let mut bui = BackwardUserIterator::for_test(bmi, (begin_key, end_key));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 300).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-end-range iterate -----
        bui.seek(iterator_test_user_key_of(8).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 300).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(iterator_test_user_key_of(7).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 300).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(iterator_test_user_key_of(2).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // ----- before-begin-range iterate -----
        bui.seek(iterator_test_user_key_of(1).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());
    }

    // left..end
    #[tokio::test]
    async fn test_backward_user_range() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let (sstable, sstable_info) =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let backward_iters = vec![BackwardSstableIterator::new(
            sstable.clone(),
            sstable_store.clone(),
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);

        let begin_key = Excluded(iterator_test_bytes_user_key_of(2));
        let end_key = Included(iterator_test_bytes_user_key_of(7));

        let mut bui = BackwardUserIterator::for_test(bmi, (begin_key, end_key));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-bend-range iterate -----
        bui.seek(iterator_test_user_key_of(8).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(iterator_test_user_key_of(7).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(7, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(iterator_test_user_key_of(2).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());

        // ----- begin-begin-range iterate -----
        bui.seek(iterator_test_user_key_of(1).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());

        let backward_iters = vec![BackwardSstableIterator::new(
            sstable,
            sstable_store,
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);

        let begin_key = Excluded(iterator_test_bytes_user_key_of(2));
        let end_key = Excluded(iterator_test_bytes_user_key_of(7));

        let mut bui = BackwardUserIterator::for_test(bmi, (begin_key, end_key));
        bui.rewind().await.unwrap();
        assert!(bui.is_valid());
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        // ----- end-range iterate -----
        bui.seek(iterator_test_user_key_of(7).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.seek(iterator_test_user_key_of(5).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
    }

    // ..=right
    #[tokio::test]
    async fn test_backward_user_range_to_inclusive() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let (sstable, sstable_info) =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let backward_iters = vec![BackwardSstableIterator::new(
            sstable.clone(),
            sstable_store.clone(),
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);
        let end_key = Included(iterator_test_bytes_user_key_of(7));

        let mut bui = BackwardUserIterator::for_test(
            bmi,
            (Included(iterator_test_bytes_user_key_of(2)), end_key),
        );

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(iterator_test_user_key_of(7).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- in-range iterate -----
        bui.seek(iterator_test_user_key_of(6).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(iterator_test_user_key_of(0).as_ref())
            .await
            .unwrap();
        assert!(!bui.is_valid());

        let end_key = Excluded(iterator_test_bytes_user_key_of(6));
        let backward_iters = vec![BackwardSstableIterator::new(
            sstable,
            sstable_store,
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);
        let mut bui = BackwardUserIterator::for_test(
            bmi,
            (Excluded(iterator_test_bytes_user_key_of(2)), end_key),
        );
        // ----- basic iterate -----
        bui.seek(iterator_test_user_key_of(6).as_ref())
            .await
            .unwrap();
        assert!(bui.is_valid());
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());
        bui.seek(iterator_test_user_key_of(7).as_ref())
            .await
            .unwrap();
        assert!(bui.is_valid());
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
    }

    // left..
    #[tokio::test]
    async fn test_backward_user_range_from() {
        let sstable_store = mock_sstable_store().await;
        // key=[idx, epoch], value
        let kv_pairs = vec![
            (0, 200, HummockValue::delete()),
            (0, 100, HummockValue::put(iterator_test_value_of(0))),
            (1, 200, HummockValue::put(iterator_test_value_of(1))),
            (1, 100, HummockValue::delete()),
            (2, 300, HummockValue::put(iterator_test_value_of(2))),
            (2, 200, HummockValue::delete()),
            (2, 100, HummockValue::delete()),
            (3, 100, HummockValue::put(iterator_test_value_of(3))),
            (5, 200, HummockValue::delete()),
            (5, 100, HummockValue::put(iterator_test_value_of(5))),
            (6, 100, HummockValue::put(iterator_test_value_of(6))),
            (7, 200, HummockValue::delete()),
            (7, 100, HummockValue::put(iterator_test_value_of(7))),
            (8, 100, HummockValue::put(iterator_test_value_of(8))),
        ];
        let (handle, sstable_info) =
            gen_iterator_test_sstable_from_kv_pair(0, kv_pairs, sstable_store.clone()).await;
        let backward_iters = vec![BackwardSstableIterator::new(
            handle,
            sstable_store,
            &sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);
        let begin_key = Included(iterator_test_bytes_user_key_of(2));

        let mut bui = BackwardUserIterator::for_test(bmi, (begin_key, Unbounded));

        // ----- basic iterate -----
        bui.rewind().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(8, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- begin-range iterate -----
        bui.seek(iterator_test_user_key_of(2).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- in-range iterate -----
        bui.seek(iterator_test_user_key_of(5).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- end-range iterate -----
        bui.seek(iterator_test_user_key_of(8).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(8, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());

        // ----- after-end-range iterate -----
        bui.seek(iterator_test_user_key_of(9).as_ref())
            .await
            .unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(8, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(6, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(3, 100).to_ref());
        bui.next().await.unwrap();
        assert_eq!(bui.key(), iterator_test_bytes_key_of_epoch(2, 300).to_ref());
        bui.next().await.unwrap();
        assert!(!bui.is_valid());
    }

    fn key_from_num(num: usize) -> UserKey<Vec<u8>> {
        let width = 20;
        UserKey::for_test(
            TableId::default(),
            format!("{:0width$}", num, width = width)
                .as_bytes()
                .to_vec(),
        )
    }

    #[allow(clippy::mutable_key_type)]
    async fn chaos_test_case(
        handle: TableHolder,
        start_bound: Bound<UserKey<Bytes>>,
        end_bound: Bound<UserKey<Bytes>>,
        truth: &ChaosTestTruth,
        sstable_store: SstableStoreRef,
        sstable_info: &SstableInfo,
    ) {
        let start_key = match &start_bound {
            Bound::Included(b) => {
                UserKey::for_test(b.table_id, Bytes::from(prev_key(&b.table_key.clone())))
            }
            Bound::Excluded(b) => b.clone(),
            Unbounded => key_from_num(0).into_bytes(),
        };
        let end_key = match &end_bound {
            Bound::Included(b) => b.clone(),
            Unbounded => key_from_num(999999999999).into_bytes(),
            _ => unimplemented!(),
        };

        let backward_iters = vec![BackwardSstableIterator::new(
            handle,
            sstable_store,
            sstable_info,
        )];
        let bmi = MergeIterator::new(backward_iters);
        let mut bui = BackwardUserIterator::for_test(bmi, (start_bound, end_bound));
        let num_puts: usize = truth
            .iter()
            .map(|(key, inserts)| {
                if *key > end_key || *key <= start_key {
                    return 0;
                }
                match inserts.first_key_value().unwrap().1 {
                    HummockValue::Put(_) => 1,
                    HummockValue::Delete => 0,
                }
            })
            .reduce(|accum, item| accum + item)
            .unwrap();
        let mut num_kvs = 0;
        bui.rewind().await.unwrap();
        for (key, value) in truth.iter().rev() {
            if *key > end_key || *key <= start_key {
                continue;
            }
            let (_, value) = value.first_key_value().unwrap();
            if let HummockValue::Delete = value {
                continue;
            }
            assert!(bui.is_valid(), "num_kvs:{}", num_kvs);
            assert_eq!(bui.key().user_key, key.as_ref(), "num_kvs:{}", num_kvs);
            if let HummockValue::Put(bytes) = &value {
                assert_eq!(bui.value(), bytes, "num_kvs:{}", num_kvs);
            }
            bui.next().await.unwrap();
            num_kvs += 1;
        }
        assert!(!bui.is_valid());
        assert_eq!(num_kvs, num_puts);
    }

    type ChaosTestTruth =
        BTreeMap<UserKey<Bytes>, BTreeMap<Reverse<HummockEpoch>, HummockValue<Bytes>>>;

    async fn generate_chaos_test_data() -> (
        usize,
        TableHolder,
        ChaosTestTruth,
        SstableStoreRef,
        SstableInfo,
    ) {
        // We first generate the key value pairs.
        let mut rng = thread_rng();
        #[allow(clippy::mutable_key_type)]
        let mut truth: ChaosTestTruth = BTreeMap::new();
        let mut prev_key_number: usize = 1;
        let number_of_keys = 5000;
        for _ in 0..number_of_keys {
            let key: usize = rng.gen_range(prev_key_number..=(prev_key_number + 10));
            prev_key_number = key + 1;
            let key_bytes = key_from_num(key).into_bytes();
            let mut prev_time = 500;
            let num_updates = rng.gen_range(1..10usize);
            for _ in 0..num_updates {
                let time: HummockEpoch = test_epoch(rng.gen_range(prev_time..=(prev_time + 1000)));
                let is_delete = rng.gen_range(0..=1usize) < 1usize;
                match is_delete {
                    true => {
                        truth
                            .entry(key_bytes.clone())
                            .or_default()
                            .insert(Reverse(time), HummockValue::delete());
                    }
                    false => {
                        let value_size = rng.gen_range(100..=200);
                        let value: String = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(value_size)
                            .map(char::from)
                            .collect();
                        truth
                            .entry(key_bytes.clone())
                            .or_default()
                            .insert(Reverse(time), HummockValue::put(Bytes::from(value)));
                    }
                }
                prev_time = time.next_epoch();
            }
        }
        let sstable_store = mock_sstable_store().await;
        let (sst, sstable_info) = gen_test_sstable(
            default_builder_opt_for_test(),
            0,
            truth.iter().flat_map(|(key, inserts)| {
                inserts.iter().map(|(time, value)| {
                    let full_key = FullKey {
                        user_key: key.clone(),
                        epoch_with_gap: EpochWithGap::new_from_epoch(time.0),
                    };
                    (full_key, value.clone())
                })
            }),
            sstable_store.clone(),
        )
        .await;

        (prev_key_number, sst, truth, sstable_store, sstable_info)
    }

    #[tokio::test]
    async fn test_backward_user_chaos_unbounded_unbounded() {
        let (_prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            chaos_test_case(
                sst.clone(),
                Unbounded,
                Unbounded,
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_unbounded_included() {
        let (prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key).into_bytes();
            chaos_test_case(
                sst.clone(),
                Unbounded,
                Included(end_key_bytes.clone()),
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_included_unbounded() {
        let (prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key).into_bytes();
            chaos_test_case(
                sst.clone(),
                Included(begin_key_bytes.clone()),
                Unbounded,
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_excluded_unbounded() {
        let (prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key).into_bytes();
            chaos_test_case(
                sst.clone(),
                Excluded(begin_key_bytes.clone()),
                Unbounded,
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_included_included() {
        let (prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key).into_bytes();
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key).into_bytes();
            chaos_test_case(
                sst.clone(),
                Included(begin_key_bytes.clone()),
                Included(end_key_bytes.clone()),
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_backward_user_chaos_excluded_included() {
        let (prev_key_number, sst, truth, sstable_store, sstable_info) =
            generate_chaos_test_data().await;
        let repeat = 20;
        for _ in 0..repeat {
            let mut rng = thread_rng();
            let end_key: usize = rng.gen_range(2..=prev_key_number);
            let end_key_bytes = key_from_num(end_key).into_bytes();
            let begin_key: usize = rng.gen_range(1..=end_key);
            let begin_key_bytes = key_from_num(begin_key).into_bytes();
            chaos_test_case(
                sst.clone(),
                Excluded(begin_key_bytes),
                Included(end_key_bytes),
                &truth,
                sstable_store.clone(),
                &sstable_info,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn test_min_epoch() {
        let sstable_store = mock_sstable_store().await;
        let (table0, sstable_info_0) = gen_iterator_test_sstable_with_incr_epoch(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
            1,
        )
        .await;

        let backward_iters = vec![BackwardSstableIterator::new(
            table0,
            sstable_store,
            &sstable_info_0,
        )];

        let min_count = (TEST_KEYS_COUNT / 5) as u64;
        let min_epoch = test_epoch(min_count);
        let mi = MergeIterator::new(backward_iters);
        let mut ui = BackwardUserIterator::with_min_epoch(mi, (Unbounded, Unbounded), min_epoch);
        ui.rewind().await.unwrap();

        let mut i = 0;
        while ui.is_valid() {
            let key = ui.key();
            let key_epoch = key.epoch_with_gap.pure_epoch();
            assert!(key_epoch > min_epoch);

            i += 1;
            ui.next().await.unwrap();
        }

        let expect_count = TEST_KEYS_COUNT - min_count as usize;
        assert_eq!(i, expect_count);
    }
}
