use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::HummockStorage;
use crate::hummock::iterator::DirectedUserIterator;
use crate::hummock::key::{next_key, prev_key};
use crate::{StateStore, StateStoreIter};

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    pub storage: HummockStorage,
}

impl HummockStateStore {
    pub fn new(storage: HummockStorage) -> Self {
        Self { storage }
    }
}

// Note(eric): How about removing HummockStateStore and just impl StateStore for HummockStorage?
#[async_trait]
impl StateStore for HummockStateStore {
    type Iter = HummockStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let value = self.storage.get(key).await?;
        let value = value.map(Bytes::from);
        Ok(value)
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, epoch: u64) -> Result<()> {
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
                epoch,
            )
            .await?;

        Ok(())
    }

    async fn iter(&self, prefix: &[u8]) -> Result<Self::Iter> {
        let timer = self.storage.stats.iter_seek_latency.start_timer();
        let range = prefix.to_owned()..next_key(prefix);
        let inner = self.storage.range_scan(range).await?;
        self.storage.stats.iter_counts.inc();
        let mut res = DirectedUserIterator::Forward(inner);
        res.rewind().await?;
        timer.observe_duration();
        Ok(HummockStateStoreIter(res))
    }

    async fn reverse_iter(&self, prefix: &[u8]) -> Result<Self::Iter> {
        let range = prefix.to_owned()..prev_key(prefix);
        let inner = self.storage.reverse_range_scan(range).await?;
        let mut res = DirectedUserIterator::Backward(inner);
        res.rewind().await?;
        Ok(HummockStateStoreIter(res))
    }
}

pub struct HummockStateStoreIter(DirectedUserIterator);

#[async_trait]
impl StateStoreIter for HummockStateStoreIter {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let iter = &mut self.0;

        if iter.is_valid() {
            let kv = (
                Bytes::copy_from_slice(iter.key()),
                Bytes::copy_from_slice(iter.value()),
            );
            iter.next().await?;
            Ok(Some(kv))
        } else {
            Ok(None)
        }
    }
}
