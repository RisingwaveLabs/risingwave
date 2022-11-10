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

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::future::Future;
use std::iter::Fuse;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockReadEpoch;

use crate::error::StorageResult;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_state_store_associated_type, define_state_store_read_associated_type,
    define_state_store_write_associated_type, StateStore, StateStoreIter,
};

mod batched_iter {
    use itertools::Itertools;

    use super::*;

    /// A utility struct for iterating over a range of keys in a locked `BTreeMap`, which will batch
    /// some records to make a trade-off between the copying overhead and the times of acquiring
    /// the lock.
    ///
    /// Therefore, it's not guaranteed that we're iterating over a consistent snapshot of the map.
    /// Users should handle MVCC by themselves.
    pub struct Iter<K, V> {
        inner: Arc<RwLock<BTreeMap<K, V>>>,
        range: (Bound<K>, Bound<K>),
        current: std::vec::IntoIter<(K, V)>,
    }

    impl<K, V> Iter<K, V> {
        pub fn new(inner: Arc<RwLock<BTreeMap<K, V>>>, range: (Bound<K>, Bound<K>)) -> Self {
            Self {
                inner,
                range,
                current: Vec::new().into_iter(),
            }
        }
    }

    impl<K, V> Iter<K, V>
    where
        K: Ord + Clone,
        V: Clone,
    {
        const BATCH_SIZE: usize = 256;

        /// Get the next batch of records and fill the `current` buffer.
        fn refill(&mut self) {
            assert!(self.current.is_empty());

            let batch: Vec<(K, V)> = self
                .inner
                .read()
                .range((self.range.0.as_ref(), self.range.1.as_ref()))
                .take(Self::BATCH_SIZE)
                .map(|(k, v)| (K::clone(k), V::clone(v)))
                .collect_vec();

            if let Some((last_key, _)) = batch.last() {
                self.range.0 = Bound::Excluded(K::clone(last_key));
            }
            self.current = batch.into_iter();
        }
    }

    impl<K, V> Iterator for Iter<K, V>
    where
        K: Ord + Clone,
        V: Clone,
    {
        type Item = (K, V);

        fn next(&mut self) -> Option<Self::Item> {
            match self.current.next() {
                Some(r) => Some(r),
                None => {
                    self.refill();
                    self.current.next()
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use rand::Rng;

        use super::*;

        #[test]
        fn test_iter_chaos() {
            let key_range = 1..=10000;
            let map: BTreeMap<i32, Arc<str>> = key_range
                .clone()
                .map(|k| (k, k.to_string().into()))
                .collect();
            let map = Arc::new(RwLock::new(map));

            let rand_bound = || {
                let key = rand::thread_rng().gen_range(key_range.clone());
                match rand::thread_rng().gen_range(1..=5) {
                    1 | 2 => Bound::Included(key),
                    3 | 4 => Bound::Excluded(key),
                    _ => Bound::Unbounded,
                }
            };

            for _ in 0..1000 {
                let range = loop {
                    let range = (rand_bound(), rand_bound());
                    let (start, end) = (range.start_bound(), range.end_bound());

                    // Filter out invalid ranges. Code migrated from `BTreeMap::range`.
                    match (start, end) {
                        (Bound::Excluded(s), Bound::Excluded(e)) if s == e => {
                            continue;
                        }
                        (
                            Bound::Included(s) | Bound::Excluded(s),
                            Bound::Included(e) | Bound::Excluded(e),
                        ) if s > e => {
                            continue;
                        }
                        _ => break range,
                    }
                };

                let v1 = Iter::new(map.clone(), range).collect_vec();
                let v2 = map
                    .read()
                    .range(range)
                    .map(|(&k, v)| (k, v.clone()))
                    .collect_vec();

                // Items iterated from the batched iterator should be the same as normal iterator.
                assert_eq!(v1, v2);
            }
        }
    }
}

type KeyWithEpoch = (Bytes, Reverse<u64>);

/// An in-memory state store
///
/// The in-memory state store is a [`BTreeMap`], which maps (key, epoch) to value. It never does GC,
/// so the memory usage will be high. Therefore, in-memory state store should never be used in
/// production.
#[derive(Clone, Default)]
pub struct MemoryStateStore {
    /// Stores (key, epoch) -> user value.
    inner: Arc<RwLock<BTreeMap<KeyWithEpoch, Option<Bytes>>>>,
}

fn to_bytes_range<R, B>(range: R) -> (Bound<KeyWithEpoch>, Bound<KeyWithEpoch>)
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match range.start_bound() {
        Included(k) => Included((Bytes::copy_from_slice(k.as_ref()), Reverse(u64::MAX))),
        Excluded(k) => Excluded((Bytes::copy_from_slice(k.as_ref()), Reverse(0))),
        Unbounded => Unbounded,
    };
    let end = match range.end_bound() {
        Included(k) => Included((Bytes::copy_from_slice(k.as_ref()), Reverse(0))),
        Excluded(k) => Excluded((Bytes::copy_from_slice(k.as_ref()), Reverse(u64::MAX))),
        Unbounded => Unbounded,
    };
    (start, end)
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn shared() -> Self {
        static STORE: LazyLock<MemoryStateStore> = LazyLock::new(MemoryStateStore::new);
        STORE.clone()
    }

    fn scan(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        limit: Option<usize>,
    ) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let inner = self.inner.read();

        let mut last_key = None;
        for ((key, Reverse(key_epoch)), value) in inner.range(to_bytes_range(key_range)) {
            if *key_epoch > epoch {
                continue;
            }
            if Some(key) != last_key {
                if let Some(value) = value {
                    data.push((key.clone(), value.clone()));
                }
                last_key = Some(key);
            }
            if let Some(limit) = limit && data.len() >= limit {
                    break;
                }
        }
        Ok(data)
    }
}

impl StateStoreRead for MemoryStateStore {
    type Iter = MemoryStateStoreIter;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        _read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            let range_bounds = (Bound::Included(key.to_vec()), Bound::Included(key.to_vec()));
            // We do not really care about vnodes here, so we just use the default value.
            let res = self.scan(range_bounds, epoch, Some(1))?;

            Ok(match res.as_slice() {
                [] => None,
                [(_, value)] => Some(value.clone()),
                _ => unreachable!(),
            })
        }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        _read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            Ok(MemoryStateStoreIter::new(
                batched_iter::Iter::new(self.inner.clone(), to_bytes_range(key_range)),
                epoch,
            ))
        }
    }
}

impl StateStoreWrite for MemoryStateStore {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        _delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let epoch = write_options.epoch;
            let mut inner = self.inner.write();
            let mut size: usize = 0;
            for (key, value) in kv_pairs {
                size += key.len() + value.size();
                inner.insert((key, Reverse(epoch)), value.user_value);
            }
            Ok(size)
        }
    }
}

impl LocalStateStore for MemoryStateStore {}

impl StateStore for MemoryStateStore {
    type Local = Self;

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, _epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            // memory backend doesn't need to wait for epoch, so this is a no-op.
            Ok(())
        }
    }

    fn sync(&self, _epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            // memory backend doesn't need to push to S3, so this is a no-op
            Ok(SyncResult {
                ..Default::default()
            })
        }
    }

    fn seal_epoch(&self, _epoch: u64, _is_checkpoint: bool) {}

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move { Ok(()) }
    }

    fn new_local(&self, _table_id: TableId) -> Self::NewLocalFuture<'_> {
        async { self.clone() }
    }
}

pub struct MemoryStateStoreIter {
    inner: Fuse<batched_iter::Iter<KeyWithEpoch, Option<Bytes>>>,

    epoch: u64,

    last_key: Option<Bytes>,
}

impl MemoryStateStoreIter {
    pub fn new(inner: batched_iter::Iter<KeyWithEpoch, Option<Bytes>>, epoch: u64) -> Self {
        Self {
            inner: inner.fuse(),
            epoch,
            last_key: None,
        }
    }
}

impl StateStoreIter for MemoryStateStoreIter {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            for ((key, Reverse(key_epoch)), value) in self.inner.by_ref() {
                if key_epoch > self.epoch {
                    continue;
                }
                if Some(&key) != self.last_key.as_ref() {
                    self.last_key = Some(key.clone());
                    if let Some(value) = value {
                        return Ok(Some((key, value)));
                    }
                }
            }
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_snapshot_isolation() {
        let state_store = MemoryStateStore::new();
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), StorageValue::new_put(b"v1".to_vec())),
                    (b"b".to_vec().into(), StorageValue::new_put(b"v1".to_vec())),
                ],
                vec![],
                WriteOptions {
                    epoch: 0,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        state_store
            .ingest_batch(
                vec![
                    (b"a".to_vec().into(), StorageValue::new_put(b"v2".to_vec())),
                    (b"b".to_vec().into(), StorageValue::new_delete()),
                ],
                vec![],
                WriteOptions {
                    epoch: 1,
                    table_id: Default::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(b"a".to_vec()),
                        Bound::Included(b"b".to_vec()),
                    ),
                    0,
                    None,
                )
                .unwrap(),
            vec![
                (b"a".to_vec().into(), b"v1".to_vec().into()),
                (b"b".to_vec().into(), b"v1".to_vec().into())
            ]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(b"a".to_vec()),
                        Bound::Included(b"b".to_vec()),
                    ),
                    0,
                    Some(1),
                )
                .unwrap(),
            vec![(b"a".to_vec().into(), b"v1".to_vec().into())]
        );
        assert_eq!(
            state_store
                .scan(
                    (
                        Bound::Included(b"a".to_vec()),
                        Bound::Included(b"b".to_vec()),
                    ),
                    1,
                    None,
                )
                .unwrap(),
            vec![(b"a".to_vec().into(), b"v2".to_vec().into())]
        );
        assert_eq!(
            state_store
                .get(b"a", 0, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(b"b", 0, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(b"v1".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(b"c", 0, ReadOptions::default(),)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(b"a", 1, ReadOptions::default(),)
                .await
                .unwrap(),
            Some(b"v2".to_vec().into())
        );
        assert_eq!(
            state_store
                .get(b"b", 1, ReadOptions::default(),)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            state_store
                .get(b"c", 1, ReadOptions::default())
                .await
                .unwrap(),
            None
        );
    }
}
