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

use std::ops::{Bound, Deref};
use std::sync::atomic::{AtomicU64, Ordering};

use bincode::error::{DecodeError, EncodeError};
use bincode::{Decode, Encode};
use bytes::Bytes;
use prost::Message;
use risingwave_pb::meta::SubscribeResponse;

use crate::{
    LocalStorageId, StorageType, TracedHummockReadEpoch, TracedInitOptions, TracedNewLocalOptions,
    TracedReadOptions, TracedSealCurrentEpochOptions, TracedTryWaitEpochOptions,
};

pub type RecordId = u64;

pub type RecordIdGenerator = UniqueIdGenerator<AtomicU64>;
pub type ConcurrentIdGenerator = UniqueIdGenerator<AtomicU64>;

pub trait UniqueId {
    type Type;
    fn inc(&self) -> Self::Type;
}

impl UniqueId for AtomicU64 {
    type Type = u64;

    fn inc(&self) -> Self::Type {
        self.fetch_add(1, Ordering::Relaxed)
    }
}

pub struct UniqueIdGenerator<T> {
    id: T,
}

impl<T: UniqueId> UniqueIdGenerator<T> {
    pub fn new(id: T) -> Self {
        Self { id }
    }

    pub fn next(&self) -> T::Type {
        self.id.inc()
    }
}

#[derive(Encode, Decode, Debug, PartialEq, Clone)]
pub struct Record {
    pub storage_type: StorageType,
    pub record_id: RecordId,
    pub operation: Operation,
}

impl Record {
    pub fn new(storage_type: StorageType, record_id: RecordId, operation: Operation) -> Self {
        Self {
            storage_type,
            record_id,
            operation,
        }
    }

    pub fn storage_type(&self) -> &StorageType {
        &self.storage_type
    }

    pub fn record_id(&self) -> RecordId {
        self.record_id
    }

    pub fn operation(&self) -> &Operation {
        &self.operation
    }

    pub fn is_iter_related(&self) -> bool {
        matches!(
            self.operation(),
            Operation::Iter { .. } | Operation::IterNext(_)
        )
    }

    #[cfg(test)]
    pub(crate) fn new_local_none(record_id: RecordId, operation: Operation) -> Self {
        Self::new(StorageType::Global, record_id, operation)
    }
}

pub type TracedIterRange = (Bound<TracedBytes>, Bound<TracedBytes>);

/// Operations represents Hummock operations
#[derive(Encode, Decode, PartialEq, Debug, Clone)]
pub enum Operation {
    /// Get operation of Hummock.
    Get {
        /// Key to retrieve.
        key: TracedBytes,
        /// Optional epoch value.
        epoch: Option<u64>,
        /// Read options for the operation.
        read_options: TracedReadOptions,
    },

    /// Insert operation of Hummock.
    Insert {
        /// Key to insert.
        key: TracedBytes,
        /// New value to insert.
        new_val: TracedBytes,
        /// Optional old value to replace.
        old_val: Option<TracedBytes>,
    },

    /// Delete operation of Hummock.
    Delete {
        /// Key to delete.
        key: TracedBytes,
        /// Value to match for deletion.
        old_val: TracedBytes,
    },

    /// Iter operation of Hummock.
    Iter {
        /// Key range for iteration.
        key_range: TracedIterRange,
        /// Optional epoch value.
        epoch: Option<u64>,
        /// Read options for the operation.
        read_options: TracedReadOptions,
    },

    /// Iter.next operation of Hummock.
    IterNext(RecordId),

    /// Sync operation of Hummock.
    Sync(Vec<(u64, Vec<u32>)>),

    /// `MetaMessage` operation of Hummock.
    MetaMessage(Box<TracedSubResp>),

    /// Result operation of Hummock.
    Result(OperationResult),

    /// `NewLocalStorage` operation of Hummock.
    NewLocalStorage(TracedNewLocalOptions, LocalStorageId),

    /// `DropLocalStorage` operation of Hummock.
    DropLocalStorage,

    /// Init of a local storage
    LocalStorageInit(TracedInitOptions),

    /// Try wait epoch
    TryWaitEpoch(TracedHummockReadEpoch, TracedTryWaitEpochOptions),

    /// Seal current epoch
    SealCurrentEpoch {
        epoch: u64,
        opts: TracedSealCurrentEpochOptions,
    },

    LocalStorageEpoch,

    LocalStorageIsDirty,

    TryFlush,

    Flush,
    /// Finish operation of Hummock.
    Finish,
}

impl Operation {
    pub fn get(key: Bytes, epoch: Option<u64>, read_options: TracedReadOptions) -> Operation {
        Operation::Get {
            key: key.into(),
            epoch,
            read_options,
        }
    }

    pub fn insert(key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> Operation {
        Operation::Insert {
            key: key.into(),
            new_val: new_val.into(),
            old_val: old_val.map(|v| v.into()),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct TracedBytes(Bytes);

impl Deref for TracedBytes {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Encode for TracedBytes {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        Encode::encode(&self.0.as_ref(), encoder)
    }
}

impl Decode for TracedBytes {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let buf: Vec<u8> = Decode::decode(decoder)?;
        let bytes = Bytes::from(buf);
        Ok(Self(bytes))
    }
}

impl<'de> bincode::BorrowDecode<'de> for TracedBytes {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let buf: Vec<u8> = Decode::decode(decoder)?;
        let bytes = Bytes::from(buf);
        Ok(Self(bytes))
    }
}

impl From<Vec<u8>> for TracedBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<Bytes> for TracedBytes {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<TracedBytes> for Bytes {
    fn from(value: TracedBytes) -> Self {
        value.0
    }
}
/// `TraceResult` discards Error and only traces whether succeeded or not.
/// Use Option rather than Result because it's overhead to serialize Error.
#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TraceResult<T> {
    Ok(T),
    Err,
}

impl<T> TraceResult<T> {
    pub fn is_ok(&self) -> bool {
        matches!(*self, Self::Ok(_))
    }
}

impl<T, E> From<std::result::Result<T, E>> for TraceResult<T> {
    fn from(value: std::result::Result<T, E>) -> Self {
        match value {
            Ok(v) => Self::Ok(v),
            Err(_) => Self::Err, // discard error
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum OperationResult {
    Get(TraceResult<Option<TracedBytes>>),
    Insert(TraceResult<()>),
    Delete(TraceResult<()>),
    TryFlush(TraceResult<()>),
    Flush(TraceResult<usize>),
    Iter(TraceResult<()>),
    IterNext(TraceResult<Option<(TracedBytes, TracedBytes)>>),
    Sync(TraceResult<usize>),
    NotifyHummock(TraceResult<()>),
    TryWaitEpoch(TraceResult<()>),
    LocalStorageEpoch(TraceResult<u64>),
    LocalStorageIsDirty(TraceResult<bool>),
}

#[derive(PartialEq, Debug, Clone)]
pub struct TracedSubResp(pub SubscribeResponse);

impl Encode for TracedSubResp {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        // SubscribeResponse and its implementation of Serialize is generated
        // by prost and pbjson for protobuf mapping.
        // Serialization methods like Bincode may not correctly serialize it.
        // So we use prost::Message::encode
        let mut buf = vec![];
        self.0
            .encode(&mut buf)
            .map_err(|_| EncodeError::Other("failed to encode subscribeResponse"))?;
        Encode::encode(&buf, encoder)
    }
}

impl Decode for TracedSubResp {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let buf: Vec<u8> = Decode::decode(decoder)?;
        let resp = Message::decode(&buf[..]).map_err(|_| {
            DecodeError::OtherString("failed to decode subscribeResponse".to_owned())
        })?;
        Ok(Self(resp))
    }
}

impl<'de> bincode::BorrowDecode<'de> for TracedSubResp {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let buf: Vec<u8> = Decode::decode(decoder)?;
        let resp = Message::decode(&buf[..]).map_err(|_| {
            DecodeError::OtherString("failed to decode subscribeResponse".to_owned())
        })?;
        Ok(Self(resp))
    }
}

impl From<SubscribeResponse> for TracedSubResp {
    fn from(value: SubscribeResponse) -> Self {
        Self(value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use parking_lot::Mutex;

    use super::*;

    // test atomic id
    #[tokio::test(flavor = "multi_thread")]
    async fn test_atomic_id() {
        let gen = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));
        let mut handles = Vec::new();
        let ids_lock = Arc::new(Mutex::new(HashSet::new()));
        let count: u64 = 5000;

        for _ in 0..count {
            let ids = ids_lock.clone();
            let gen = gen.clone();
            handles.push(tokio::spawn(async move {
                let id = gen.next();
                ids.lock().insert(id);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let ids = ids_lock.lock();

        for i in 0..count {
            assert!(ids.contains(&i));
        }
    }

    #[test]
    fn test_record_is_iter_related() {
        let iter_operation = Operation::Iter {
            key_range: (Bound::Unbounded, Bound::Unbounded),
            epoch: None,
            read_options: TracedReadOptions::for_test(0),
        };
        let get_operation = Operation::Get {
            key: TracedBytes(Bytes::from("test")),
            epoch: None,
            read_options: TracedReadOptions::for_test(0),
        };

        let iter_record = Record::new(StorageType::Global, 1, iter_operation);
        let get_record = Record::new(StorageType::Global, 2, get_operation);

        assert!(iter_record.is_iter_related());
        assert!(!get_record.is_iter_related());
    }
}
