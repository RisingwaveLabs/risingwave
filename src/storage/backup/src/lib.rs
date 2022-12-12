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

#![allow(clippy::derive_partial_eq_without_eq)]
#![feature(trait_alias)]
#![feature(binary_heap_drain_sorted)]
#![feature(option_result_contains)]
#![feature(type_alias_impl_trait)]
#![feature(drain_filter)]
#![feature(custom_test_frameworks)]
#![feature(lint_reasons)]
#![feature(map_try_insert)]
#![feature(hash_drain_filter)]
#![feature(is_some_and)]
#![feature(btree_drain_filter)]
#![feature(result_option_inspect)]
#![feature(once_cell)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(provide_any)]
#![cfg_attr(coverage, feature(no_coverage))]
#![allow(dead_code)]

pub mod error;
pub mod meta_snapshot;
pub mod storage;

use std::hash::Hasher;

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionExt;
use risingwave_hummock_sdk::{HummockSstableId, HummockVersionId};
use risingwave_pb::hummock::HummockVersion;
use serde::{Deserialize, Serialize};

use crate::error::{BackupError, BackupResult};

pub type MetaSnapshotId = u64;
pub type MetaBackupJobId = u64;

/// `MetaSnapshotMetadata` is metadata of `MetaSnapshot`.
#[derive(Serialize, Deserialize, Clone)]
pub struct MetaSnapshotMetadata {
    pub id: MetaSnapshotId,
    pub hummock_version_id: HummockVersionId,
    pub ssts: Vec<HummockSstableId>,
}

impl MetaSnapshotMetadata {
    pub fn new(id: MetaSnapshotId, v: &HummockVersion) -> Self {
        Self {
            id,
            hummock_version_id: v.id,
            ssts: v.get_sst_ids(),
        }
    }
}

// Code is copied from storage crate. TODO #6482: extract method.
pub fn xxhash64_checksum(data: &[u8]) -> u64 {
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(data);
    hasher.finish()
}

pub fn xxhash64_verify(data: &[u8], checksum: u64) -> BackupResult<()> {
    let data_checksum = xxhash64_checksum(data);
    if data_checksum != checksum {
        return Err(BackupError::ChecksumMismatch {
            expected: checksum,
            found: data_checksum,
        });
    }
    Ok(())
}
