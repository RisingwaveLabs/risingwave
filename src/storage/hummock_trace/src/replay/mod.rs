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

mod runner;
mod worker;

use std::ops::Bound;

use futures::stream::BoxStream;
use futures::Stream;
#[cfg(test)]
use futures_async_stream::try_stream;
#[cfg(test)]
use mockall::{automock, mock};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
pub use runner::*;
pub(crate) use worker::*;

use crate::error::Result;
#[cfg(test)]
use crate::TraceError;
use crate::{
    LocalStorageId, Record, TracedBytes, TracedInitOptions, TracedNewLocalOptions,
    TracedReadOptions, TracedSealCurrentEpochOptions, TracedTryWaitEpochOptions,
};

pub type ReplayItem = (TracedBytes, TracedBytes);
pub trait ReplayItemStream = Stream<Item = ReplayItem> + Send;

type ReplayGroup = Record;

#[derive(Debug)]
enum WorkerResponse {
    Continue,
    Shutdown,
}

pub(crate) type ReplayRequest = Option<ReplayGroup>;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub(crate) enum WorkerId {
    // local storage worker
    Local(u64, LocalStorageId),
    // for global store
    OneShot(u64),
}

#[async_trait::async_trait]
pub trait LocalReplay: LocalReplayRead + ReplayWrite + Send + Sync {
    async fn init(&mut self, options: TracedInitOptions) -> Result<()>;
    fn seal_current_epoch(&mut self, next_epoch: u64, opts: TracedSealCurrentEpochOptions);
    fn is_dirty(&self) -> bool;
    fn epoch(&self) -> u64;
    async fn try_flush(&mut self) -> Result<()>;
    async fn flush(&mut self) -> Result<usize>;
}
pub trait GlobalReplay: ReplayRead + ReplayStateStore + Send + Sync {}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait LocalReplayRead {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>>;
    async fn get(
        &self,
        key: TracedBytes,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayRead {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>>;
    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayWrite {
    fn insert(
        &mut self,
        key: TracedBytes,
        new_val: TracedBytes,
        old_val: Option<TracedBytes>,
    ) -> Result<()>;
    fn delete(&mut self, key: TracedBytes, old_val: TracedBytes) -> Result<()>;
}

#[cfg_attr(test, automock)]
#[async_trait::async_trait]
pub trait ReplayStateStore {
    async fn sync(&self, sync_table_epochs: Vec<(u64, Vec<u32>)>) -> Result<usize>;
    async fn notify_hummock(&self, info: Info, op: RespOperation, version: u64) -> Result<u64>;
    async fn new_local(&self, opts: TracedNewLocalOptions) -> Box<dyn LocalReplay>;
    async fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
        options: TracedTryWaitEpochOptions,
    ) -> Result<()>;
}

// define mock trait for replay interfaces
// We need to do this since the mockall crate does not support async_trait
#[cfg(test)]
mock! {
    pub GlobalReplayInterface{}
    #[async_trait::async_trait]
    impl ReplayRead for GlobalReplayInterface{
        async fn iter(
            &self,
            key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
            epoch: u64,
            read_options: TracedReadOptions,
        ) -> Result<BoxStream<'static, Result<ReplayItem>>>;
        async fn get(
            &self,
            key: TracedBytes,
            epoch: u64,
            read_options: TracedReadOptions,
        ) -> Result<Option<TracedBytes>>;
    }
    #[async_trait::async_trait]
    impl ReplayStateStore for GlobalReplayInterface{
        async fn sync(&self, sync_table_epochs: Vec<(u64, Vec<u32>)>) -> Result<usize>;
        async fn notify_hummock(&self, info: Info, op: RespOperation, version: u64,
        ) -> Result<u64>;
        async fn new_local(&self, opts: TracedNewLocalOptions) -> Box<dyn LocalReplay>;
        async fn try_wait_epoch(&self, epoch: HummockReadEpoch,options: TracedTryWaitEpochOptions) -> Result<()>;
    }
    impl GlobalReplay for GlobalReplayInterface{}
}

// define mock trait for local replay interfaces
#[cfg(test)]
mock! {
    pub LocalReplayInterface{}
    #[async_trait::async_trait]
    impl LocalReplayRead for LocalReplayInterface{
        async fn iter(
            &self,
            key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
            read_options: TracedReadOptions,
        ) -> Result<BoxStream<'static, Result<ReplayItem>>>;
        async fn get(
            &self,
            key: TracedBytes,
            read_options: TracedReadOptions,
        ) -> Result<Option<TracedBytes>>;
    }
    #[async_trait::async_trait]
    impl ReplayWrite for LocalReplayInterface{
        fn insert(&mut self, key: TracedBytes, new_val: TracedBytes, old_val: Option<TracedBytes>) -> Result<()>;
        fn delete(&mut self, key: TracedBytes, old_val: TracedBytes) -> Result<()>;
    }
    #[async_trait::async_trait]
    impl LocalReplay for LocalReplayInterface{
        async fn init(&mut self, options: TracedInitOptions) -> Result<()>;
        fn seal_current_epoch(&mut self, next_epoch: u64, opts: TracedSealCurrentEpochOptions);
        fn is_dirty(&self) -> bool;
        fn epoch(&self) -> u64;
        async fn flush(&mut self) -> Result<usize>;
        async fn try_flush(&mut self) -> Result<()>;
    }
}

#[cfg(test)]
pub(crate) struct MockReplayIterStream {
    items: Vec<ReplayItem>,
}
#[cfg(test)]
impl MockReplayIterStream {
    pub(crate) fn new(items: Vec<ReplayItem>) -> Self {
        Self { items }
    }

    #[try_stream(ok = ReplayItem, error = TraceError)]

    pub(crate) async fn into_stream(self) {
        for (key, value) in self.items {
            yield (key, value)
        }
    }
}
