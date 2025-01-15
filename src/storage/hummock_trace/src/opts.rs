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

use bincode::{Decode, Encode};
use foyer::CacheHint;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::{HummockReadEpoch, HummockVersionId};
use risingwave_pb::common::PbBuffer;

use crate::TracedBytes;

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedPrefetchOptions {
    pub prefetch: bool,
    pub for_large_query: bool,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TracedCachePolicy {
    Disable,
    Fill(TracedCachePriority),
    NotFill,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TracedCachePriority {
    High,
    Low,
}

impl From<CachePriority> for TracedCachePriority {
    fn from(value: CachePriority) -> Self {
        match value {
            CachePriority::High => Self::High,
            CachePriority::Low => Self::Low,
        }
    }
}

impl From<TracedCachePriority> for CachePriority {
    fn from(value: TracedCachePriority) -> Self {
        match value {
            TracedCachePriority::High => Self::High,
            TracedCachePriority::Low => Self::Low,
        }
    }
}

impl From<CacheHint> for TracedCachePriority {
    fn from(value: CacheHint) -> Self {
        match value {
            CacheHint::Normal => Self::High,
            CacheHint::Low => Self::Low,
        }
    }
}

impl From<TracedCachePriority> for CacheHint {
    fn from(value: TracedCachePriority) -> Self {
        match value {
            TracedCachePriority::High => Self::Normal,
            TracedCachePriority::Low => Self::Low,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedTableId {
    pub table_id: u32,
}

impl From<TableId> for TracedTableId {
    fn from(value: TableId) -> Self {
        Self {
            table_id: value.table_id,
        }
    }
}

impl From<TracedTableId> for TableId {
    fn from(value: TracedTableId) -> Self {
        Self {
            table_id: value.table_id,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedReadOptions {
    pub prefix_hint: Option<TracedBytes>,
    pub prefetch_options: TracedPrefetchOptions,
    pub cache_policy: TracedCachePolicy,

    pub retention_seconds: Option<u32>,
    pub table_id: TracedTableId,
    pub read_version_from_backup: bool,
    pub read_committed: bool,
}

impl TracedReadOptions {
    pub fn for_test(table_id: u32) -> Self {
        Self {
            prefix_hint: Some(TracedBytes::from(vec![0])),
            prefetch_options: TracedPrefetchOptions {
                prefetch: true,
                for_large_query: true,
            },
            cache_policy: TracedCachePolicy::Disable,
            retention_seconds: None,
            table_id: TracedTableId { table_id },
            read_version_from_backup: false,
            read_committed: false,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedWriteOptions {
    pub epoch: u64,
    pub table_id: TracedTableId,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedTableOption {
    pub retention_seconds: Option<u32>,
}

impl From<TableOption> for TracedTableOption {
    fn from(value: TableOption) -> Self {
        Self {
            retention_seconds: value.retention_seconds,
        }
    }
}

impl From<TracedTableOption> for TableOption {
    fn from(value: TracedTableOption) -> Self {
        Self {
            retention_seconds: value.retention_seconds,
        }
    }
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub enum TracedOpConsistencyLevel {
    Inconsistent,
    ConsistentOldValue,
}

#[derive(Encode, Decode, PartialEq, Eq, Debug, Clone)]
pub struct TracedNewLocalOptions {
    pub table_id: TracedTableId,
    pub op_consistency_level: TracedOpConsistencyLevel,
    pub table_option: TracedTableOption,
    pub is_replicated: bool,
    pub vnodes: TracedBitmap,
}

#[derive(Encode, Decode, PartialEq, Debug, Clone)]
pub struct TracedTryWaitEpochOptions {
    pub table_id: TracedTableId,
}

#[cfg(test)]
impl TracedNewLocalOptions {
    pub(crate) fn for_test(table_id: u32) -> Self {
        use risingwave_common::hash::VirtualNode;

        Self {
            table_id: TracedTableId { table_id },
            op_consistency_level: TracedOpConsistencyLevel::Inconsistent,
            table_option: TracedTableOption {
                retention_seconds: None,
            },
            is_replicated: false,
            vnodes: TracedBitmap::from(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        }
    }
}

pub type TracedHummockEpoch = u64;

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub enum TracedHummockReadEpoch {
    Committed(TracedHummockEpoch),
    BatchQueryReadCommitted(TracedHummockEpoch, u64),
    NoWait(TracedHummockEpoch),
    Backup(TracedHummockEpoch),
    TimeTravel(TracedHummockEpoch),
}

impl From<HummockReadEpoch> for TracedHummockReadEpoch {
    fn from(value: HummockReadEpoch) -> Self {
        match value {
            HummockReadEpoch::Committed(epoch) => Self::Committed(epoch),
            HummockReadEpoch::BatchQueryCommitted(epoch, version_id) => {
                Self::BatchQueryReadCommitted(epoch, version_id.to_u64())
            }
            HummockReadEpoch::NoWait(epoch) => Self::NoWait(epoch),
            HummockReadEpoch::Backup(epoch) => Self::Backup(epoch),
            HummockReadEpoch::TimeTravel(epoch) => Self::TimeTravel(epoch),
        }
    }
}

impl From<TracedHummockReadEpoch> for HummockReadEpoch {
    fn from(value: TracedHummockReadEpoch) -> Self {
        match value {
            TracedHummockReadEpoch::Committed(epoch) => Self::Committed(epoch),
            TracedHummockReadEpoch::BatchQueryReadCommitted(epoch, version_id) => {
                Self::BatchQueryCommitted(epoch, HummockVersionId::new(version_id))
            }
            TracedHummockReadEpoch::NoWait(epoch) => Self::NoWait(epoch),
            TracedHummockReadEpoch::Backup(epoch) => Self::Backup(epoch),
            TracedHummockReadEpoch::TimeTravel(epoch) => Self::TimeTravel(epoch),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct TracedEpochPair {
    pub curr: TracedHummockEpoch,
    pub prev: TracedHummockEpoch,
}

impl From<EpochPair> for TracedEpochPair {
    fn from(value: EpochPair) -> Self {
        TracedEpochPair {
            curr: value.curr,
            prev: value.prev,
        }
    }
}

impl From<TracedEpochPair> for EpochPair {
    fn from(value: TracedEpochPair) -> Self {
        EpochPair {
            curr: value.curr,
            prev: value.prev,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct TracedInitOptions {
    pub epoch: TracedEpochPair,
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct TracedSealCurrentEpochOptions {
    // The watermark is serialized into protobuf
    pub table_watermarks: Option<(bool, Vec<Vec<u8>>, bool)>,
    pub switch_op_consistency_level: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct TracedBitmap {
    pub compression: i32,
    pub body: Vec<u8>,
}

impl From<Bitmap> for TracedBitmap {
    fn from(value: Bitmap) -> Self {
        let pb = value.to_protobuf();
        Self {
            compression: pb.compression,
            body: pb.body,
        }
    }
}

impl From<TracedBitmap> for Bitmap {
    fn from(value: TracedBitmap) -> Self {
        let pb = PbBuffer {
            compression: value.compression,
            body: value.body,
        };
        Bitmap::from(&pb)
    }
}
