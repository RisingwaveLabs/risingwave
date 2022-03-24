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

use super::variants::FORWARD;
use crate::hummock::iterator::merge_inner::MergeIteratorInner;

pub type MergeIterator<'a> = MergeIteratorInner<'a, FORWARD>;

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        default_builder_opt_for_test, gen_iterator_test_sstable_base, iterator_test_key_of,
        iterator_test_value_of, mock_sstable_store, TEST_KEYS_COUNT,
    };
    use crate::hummock::iterator::{BoxedHummockIterator, HummockIterator};
    use crate::hummock::sstable::SSTableIterator;
    use crate::monitor::StateStoreMetrics;

    #[tokio::test]
    async fn test_merge_basic() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(Arc::new(table2), sstable_store)),
        ];

        let mut iter = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));
        let mut i = 0;
        iter.rewind().await.unwrap();
        while iter.is_valid() {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, iterator_test_key_of(i).as_slice());
            assert_eq!(
                val.into_put_value().unwrap(),
                iterator_test_value_of(i).as_slice()
            );
            i += 1;
            iter.next().await.unwrap();
            if i == TEST_KEYS_COUNT * 3 {
                assert!(!iter.is_valid());
                break;
            }
        }
        assert!(i >= TEST_KEYS_COUNT * 3);
    }

    #[tokio::test]
    async fn test_merge_seek() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x * 3,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| x * 3 + 1,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table2 = gen_iterator_test_sstable_base(
            2,
            default_builder_opt_for_test(),
            |x| x * 3 + 2,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(
                Arc::new(table1),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(Arc::new(table2), sstable_store)),
        ];

        let mut mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        // right edge case
        mi.seek(iterator_test_key_of(TEST_KEYS_COUNT * 3).as_slice())
            .await
            .unwrap();
        assert!(!mi.is_valid());

        // normal case
        mi.seek(iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).as_slice())
            .await
            .unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT * 2 + 5).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT * 2 + 5).as_slice());

        mi.seek(iterator_test_key_of(17).as_slice()).await.unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(TEST_KEYS_COUNT + 7).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(TEST_KEYS_COUNT + 7).as_slice());

        // left edge case
        mi.seek(iterator_test_key_of(0).as_slice()).await.unwrap();
        let k = mi.key();
        let v = mi.value();
        assert_eq!(
            v.into_put_value().unwrap(),
            iterator_test_value_of(0).as_slice()
        );
        assert_eq!(k, iterator_test_key_of(0).as_slice());
    }

    #[tokio::test]
    async fn test_merge_invalidate_reset() {
        let sstable_store = mock_sstable_store();
        let table0 = gen_iterator_test_sstable_base(
            0,
            default_builder_opt_for_test(),
            |x| x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let table1 = gen_iterator_test_sstable_base(
            1,
            default_builder_opt_for_test(),
            |x| TEST_KEYS_COUNT + x,
            sstable_store.clone(),
            TEST_KEYS_COUNT,
        )
        .await;
        let iters: Vec<BoxedHummockIterator> = vec![
            Box::new(SSTableIterator::new(
                Arc::new(table0),
                sstable_store.clone(),
            )),
            Box::new(SSTableIterator::new(Arc::new(table1), sstable_store)),
        ];

        let mut mi = MergeIterator::new(iters, Arc::new(StateStoreMetrics::unused()));

        mi.rewind().await.unwrap();
        let mut count = 0;
        while mi.is_valid() {
            count += 1;
            mi.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);

        mi.rewind().await.unwrap();
        let mut count = 0;
        while mi.is_valid() {
            count += 1;
            mi.next().await.unwrap();
        }
        assert_eq!(count, TEST_KEYS_COUNT * 2);
    }
}
