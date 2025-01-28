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

use std::ops::Bound;

use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::{Channel, NotificationClient, ObserverError};
use risingwave_hummock_sdk::key::TableKey;
use risingwave_hummock_sdk::{HummockReadEpoch, HummockVersionId, SyncResult};
use risingwave_hummock_trace::{
    GlobalReplay, LocalReplay, LocalReplayRead, ReplayItem, ReplayRead, ReplayStateStore,
    ReplayWrite, Result, TraceError, TracedBytes, TracedInitOptions, TracedNewLocalOptions,
    TracedReadOptions, TracedSealCurrentEpochOptions, TracedSubResp, TracedTryWaitEpochOptions,
};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation as RespOperation};
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_storage::hummock::store::LocalHummockStorage;
use risingwave_storage::hummock::test_utils::*;
use risingwave_storage::hummock::HummockStorage;
use risingwave_storage::store::{to_owned_item, LocalStateStore, StateStoreIterExt};
use risingwave_storage::{StateStore, StateStoreIter, StateStoreReadIter};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};

pub(crate) struct GlobalReplayIter<S>
where
    S: StateStoreReadIter,
{
    inner: S,
}

impl<S> GlobalReplayIter<S>
where
    S: StateStoreReadIter,
{
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }

    pub(crate) fn into_stream(self) -> impl Stream<Item = Result<ReplayItem>> {
        self.inner.into_stream(to_owned_item).map(|item_res| {
            item_res
                .map(|(key, value)| (key.user_key.table_key.0.into(), value.into()))
                .map_err(|_| TraceError::IterFailed("iter failed to retrieve item".to_owned()))
        })
    }
}

pub(crate) struct LocalReplayIter {
    inner: Vec<ReplayItem>,
}

impl LocalReplayIter {
    pub(crate) async fn new(iter: impl StateStoreIter) -> Self {
        let inner = iter
            .into_stream(to_owned_item)
            .map_ok(|value| (value.0.user_key.table_key.0.into(), value.1.into()))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        Self { inner }
    }

    #[try_stream(ok = ReplayItem, error = TraceError)]
    pub(crate) async fn into_stream(self) {
        for (key, value) in self.inner {
            yield (key, value)
        }
    }
}

pub(crate) struct GlobalReplayImpl {
    store: HummockStorage,
    notifier: NotificationManagerRef,
}

impl GlobalReplayImpl {
    pub(crate) fn new(store: HummockStorage, notifier: NotificationManagerRef) -> Self {
        Self { store, notifier }
    }
}

impl GlobalReplay for GlobalReplayImpl {}

#[async_trait::async_trait]
impl ReplayRead for GlobalReplayImpl {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>> {
        let key_range = (
            key_range.0.map(TracedBytes::into).map(TableKey),
            key_range.1.map(TracedBytes::into).map(TableKey),
        );

        let iter = self
            .store
            .iter(key_range, epoch, read_options.into())
            .await
            .unwrap();
        let stream = GlobalReplayIter::new(iter).into_stream().boxed();
        Ok(stream)
    }

    async fn get(
        &self,
        key: TracedBytes,
        epoch: u64,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(self
            .store
            .get(TableKey(key.into()), epoch, read_options.into())
            .await
            .unwrap()
            .map(TracedBytes::from))
    }
}

#[async_trait::async_trait]
impl ReplayStateStore for GlobalReplayImpl {
    async fn sync(&self, sync_table_epochs: Vec<(u64, Vec<u32>)>) -> Result<usize> {
        let result: SyncResult = self
            .store
            .sync(
                sync_table_epochs
                    .into_iter()
                    .map(|(epoch, table_ids)| {
                        (epoch, table_ids.into_iter().map(TableId::new).collect())
                    })
                    .collect(),
            )
            .await
            .map_err(|e| TraceError::SyncFailed(format!("{e}")))?;
        Ok(result.sync_size)
    }

    async fn notify_hummock(&self, info: Info, op: RespOperation, version: u64) -> Result<u64> {
        let prev_version_id = match &info {
            Info::HummockVersionDeltas(deltas) => deltas.version_deltas.last().map(|d| d.prev_id),
            _ => None,
        };

        self.notifier
            .notify_hummock_with_version(op, info, Some(version));

        // wait till version updated
        if let Some(prev_version_id) = prev_version_id {
            self.store
                .wait_version_update(HummockVersionId::new(prev_version_id))
                .await;
        }
        Ok(version)
    }

    async fn new_local(&self, options: TracedNewLocalOptions) -> Box<dyn LocalReplay> {
        let local_storage = self.store.new_local(options.into()).await;
        Box::new(LocalReplayImpl(local_storage))
    }

    async fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
        options: TracedTryWaitEpochOptions,
    ) -> Result<()> {
        self.store
            .try_wait_epoch(epoch, options.into())
            .await
            .map_err(|_| TraceError::TryWaitEpochFailed)?;
        Ok(())
    }
}
pub(crate) struct LocalReplayImpl(LocalHummockStorage);

#[async_trait::async_trait]
impl LocalReplay for LocalReplayImpl {
    async fn init(&mut self, options: TracedInitOptions) -> Result<()> {
        self.0
            .init(options.into())
            .await
            .map_err(|_| TraceError::Other("init failed"))
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: TracedSealCurrentEpochOptions) {
        self.0.seal_current_epoch(next_epoch, opts.into());
    }

    fn epoch(&self) -> u64 {
        self.0.epoch()
    }

    async fn flush(&mut self) -> Result<usize> {
        self.0.flush().await.map_err(|_| TraceError::FlushFailed)
    }

    fn is_dirty(&self) -> bool {
        self.0.is_dirty()
    }

    async fn try_flush(&mut self) -> Result<()> {
        self.0
            .try_flush()
            .await
            .map_err(|_| TraceError::TryFlushFailed)
    }
}

#[async_trait::async_trait]
impl LocalReplayRead for LocalReplayImpl {
    async fn iter(
        &self,
        key_range: (Bound<TracedBytes>, Bound<TracedBytes>),
        read_options: TracedReadOptions,
    ) -> Result<BoxStream<'static, Result<ReplayItem>>> {
        let key_range = (
            key_range.0.map(|b| TableKey(b.into())),
            key_range.1.map(|b| TableKey(b.into())),
        );

        let iter = LocalStateStore::iter(&self.0, key_range, read_options.into())
            .await
            .unwrap();

        let stream = LocalReplayIter::new(iter).await.into_stream().boxed();
        Ok(stream)
    }

    async fn get(
        &self,
        key: TracedBytes,
        read_options: TracedReadOptions,
    ) -> Result<Option<TracedBytes>> {
        Ok(
            LocalStateStore::get(&self.0, TableKey(key.into()), read_options.into())
                .await
                .unwrap()
                .map(TracedBytes::from),
        )
    }
}

#[async_trait::async_trait]
impl ReplayWrite for LocalReplayImpl {
    fn insert(
        &mut self,
        key: TracedBytes,
        new_val: TracedBytes,
        old_val: Option<TracedBytes>,
    ) -> Result<()> {
        LocalStateStore::insert(
            &mut self.0,
            TableKey(key.into()),
            new_val.into(),
            old_val.map(|b| b.into()),
        )
        .unwrap();
        Ok(())
    }

    fn delete(&mut self, key: TracedBytes, old_val: TracedBytes) -> Result<()> {
        LocalStateStore::delete(&mut self.0, TableKey(key.into()), old_val.into()).unwrap();
        Ok(())
    }
}

pub struct ReplayNotificationClient {
    addr: HostAddr,
    notification_manager: NotificationManagerRef,
    first_resp: Box<TracedSubResp>,
}

impl ReplayNotificationClient {
    pub fn new(
        addr: HostAddr,
        notification_manager: NotificationManagerRef,
        first_resp: Box<TracedSubResp>,
    ) -> Self {
        Self {
            addr,
            notification_manager,
            first_resp,
        }
    }
}

#[async_trait::async_trait]
impl NotificationClient for ReplayNotificationClient {
    type Channel = ReplayChannel<SubscribeResponse>;

    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> std::result::Result<Self::Channel, ObserverError> {
        let (tx, rx) = unbounded_channel();

        self.notification_manager
            .insert_sender(subscribe_type, WorkerKey(self.addr.to_protobuf()), tx)
            .await;

        // send the first snapshot message
        let op = self.first_resp.0.operation();
        let info = self.first_resp.0.info.clone();

        self.notification_manager
            .notify_hummock(op, info.unwrap())
            .await;

        Ok(ReplayChannel(rx))
    }
}

pub fn get_replay_notification_client(
    env: MetaSrvEnv,
    worker_node: WorkerNode,
    first_resp: Box<TracedSubResp>,
) -> ReplayNotificationClient {
    ReplayNotificationClient::new(
        worker_node.get_host().unwrap().into(),
        env.notification_manager_ref(),
        first_resp,
    )
}

pub struct ReplayChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for ReplayChannel<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}
