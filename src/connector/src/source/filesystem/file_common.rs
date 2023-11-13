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
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use anyhow::anyhow;
use aws_sdk_s3::types::Object;
use risingwave_common::types::{JsonbVal, Timestamp};
use serde::{Deserialize, Serialize};

use crate::source::{SplitId, SplitMetaData};

///  [`FsSplit`] Describes a file or a split of a file. A file is a generic concept,
/// and can be a local file, a distributed file system, or am object in S3 bucket.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct FsSplit {
    pub name: String,
    pub offset: usize,
    pub size: usize,
}

impl From<&Object> for FsSplit {
    fn from(value: &Object) -> Self {
        Self {
            name: value.key().unwrap().to_owned(),
            offset: 0,
            size: value.size() as usize,
        }
    }
}

impl SplitMetaData for FsSplit {
    fn id(&self) -> SplitId {
        self.name.as_str().into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let offset = start_offset.parse().unwrap();
        self.offset = offset;
        Ok(())
    }
}

impl FsSplit {
    pub fn new(name: String, start: usize, size: usize) -> Self {
        Self {
            name,
            offset: start,
            size,
        }
    }
}

///  [`OpendalFsSplit`] Describes a file or a split of a file. A file is a generic concept,
/// and can be a local file, a distributed file system, or am object in S3 bucket.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OpendalFsSplit<C: OpenDALConnectorTypeMarker>
where
    C: OpenDALConnectorTypeMarker + std::marker::Send,
{
    pub name: String,
    pub offset: usize,
    pub size: usize,
    marker: PhantomData<C>,
}

impl<C: OpenDALConnectorTypeMarker> From<&Object> for OpendalFsSplit<C>
where
    C: OpenDALConnectorTypeMarker + std::marker::Send,
{
    fn from(value: &Object) -> Self {
        Self {
            name: value.key().unwrap().to_owned(),
            offset: 0,
            size: value.size() as usize,
            marker: PhantomData,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OpenDALConnectorType {
    OpenDalS3,
    Gcs,
}

/// A marker trait for different conventions, used for enforcing type safety.
///
/// Implementors are [`S3`], [`Gcs`].
pub trait OpenDALConnectorTypeMarker: 'static + Sized {
    /// The extra fields in the [`PlanBase`] of this convention.
    // type Extra: 'static + Eq + Hash + Clone + Debug;

    /// Get the [`OpenDALConnectorType`] enum value.
    fn value() -> OpenDALConnectorType;
}

/// The marker for batch convention.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Gcs;
impl OpenDALConnectorTypeMarker for Gcs {
    // type Extra = Gcs;

    fn value() -> OpenDALConnectorType {
        OpenDALConnectorType::Gcs
    }
}

/// The marker for batch convention.
#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OpenDalS3;
impl OpenDALConnectorTypeMarker for OpenDalS3 {
    // type Extra = OpenDalS3;

    fn value() -> OpenDALConnectorType {
        OpenDALConnectorType::OpenDalS3
    }
}

impl<C: OpenDALConnectorTypeMarker> SplitMetaData for OpendalFsSplit<C>
where
    C: OpenDALConnectorTypeMarker + std::marker::Send,
{
    fn id(&self) -> SplitId {
        self.name.as_str().into()
    }

    fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_with_offset(&mut self, start_offset: String) -> anyhow::Result<()> {
        let offset = start_offset.parse().unwrap();
        self.offset = offset;
        Ok(())
    }
}

impl<C: OpenDALConnectorTypeMarker> OpendalFsSplit<C>
where
    C: OpenDALConnectorTypeMarker + std::marker::Send,
{
    pub fn new(name: String, start: usize, size: usize) -> Self {
        Self {
            name,
            offset: start,
            size,
            marker: PhantomData,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FsPageItem {
    pub name: String,
    pub size: i64,
    pub timestamp: Timestamp,
}

pub type FsPage = Vec<FsPageItem>;

impl From<&Object> for FsPageItem {
    fn from(value: &Object) -> Self {
        let aws_ts = value.last_modified().unwrap();
        Self {
            name: value.key().unwrap().to_owned(),
            size: value.size(),
            timestamp: Timestamp::from_timestamp_uncheck(aws_ts.secs(), aws_ts.subsec_nanos()),
        }
    }
}
