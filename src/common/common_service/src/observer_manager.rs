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

use std::time::Duration;

use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_rpc_client::error::RpcError;
use risingwave_rpc_client::MetaClient;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tonic::{Status, Streaming};

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// We can write the notification logic by implementing `ObserverNodeImpl`.
pub struct ObserverManager<T: NotificationClient, S: ObserverState> {
    rx: T::Channel,
    client: T,
    observer_states: S,
}

pub trait ObserverState: Send + 'static {
    fn subscribe_type() -> SubscribeType;
    /// modify data after receiving notification from meta
    fn handle_notification(&mut self, resp: SubscribeResponse);

    /// Initialize data from the meta. It will be called at start or resubscribe
    fn handle_initialization_notification(&mut self, resp: SubscribeResponse);
}

impl<S: ObserverState> ObserverManager<RpcNotificationClient, S> {
    pub async fn new_with_meta_client(meta_client: MetaClient, observer_states: S) -> Self {
        let client = RpcNotificationClient { meta_client };
        Self::new(client, observer_states).await
    }
}

/// Error type for [`ObserverManager`].
#[derive(thiserror::Error, Debug)]
pub enum ObserverError {
    #[error("notification channel closed")]
    ChannelClosed,

    #[error(transparent)]
    Rpc(
        #[from]
        #[backtrace]
        RpcError,
    ),
}

impl From<tonic::Status> for ObserverError {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(RpcError::from_meta_status(status))
    }
}

impl<T, S> ObserverManager<T, S>
where
    T: NotificationClient,
    S: ObserverState,
{
    pub async fn new(client: T, observer_states: S) -> Self {
        let rx = client.subscribe(S::subscribe_type()).await.unwrap();
        Self {
            rx,
            client,
            observer_states,
        }
    }

    async fn wait_init_notification(&mut self) -> Result<(), ObserverError> {
        let mut notification_vec = Vec::new();
        let init_notification = loop {
            // notification before init notification must be received successfully.
            match self.rx.message().await? {
                Some(notification) => {
                    if !matches!(notification.info.as_ref().unwrap(), &Info::Snapshot(_)) {
                        notification_vec.push(notification);
                    } else {
                        break notification;
                    }
                }
                None => return Err(ObserverError::ChannelClosed),
            }
        };

        let Info::Snapshot(info) = init_notification.info.as_ref().unwrap() else {
            unreachable!();
        };

        notification_vec.retain_mut(|notification| match notification.info.as_ref().unwrap() {
            Info::Database(_)
            | Info::Schema(_)
            | Info::ObjectGroup(_)
            | Info::User(_)
            | Info::Connection(_)
            | Info::Secret(_)
            | Info::Function(_) => {
                notification.version > info.version.as_ref().unwrap().catalog_version
            }
            Info::Node(_) => {
                notification.version > info.version.as_ref().unwrap().worker_node_version
            }
            Info::HummockVersionDeltas(version_delta) => {
                version_delta.version_deltas[0].id > info.hummock_version.as_ref().unwrap().id
            }
            Info::MetaBackupManifestId(_) => true,
            Info::SystemParams(_) | Info::SessionParam(_) => true,
            Info::Snapshot(_) | Info::HummockWriteLimits(_) => unreachable!(),
            Info::HummockStats(_) => true,
            Info::Recovery(_) => true,
            Info::StreamingWorkerSlotMapping(_) => {
                notification.version
                    > info
                        .version
                        .as_ref()
                        .unwrap()
                        .streaming_worker_slot_mapping_version
            }
            Info::ServingWorkerSlotMappings(_) => true,
        });

        self.observer_states
            .handle_initialization_notification(init_notification);

        for notification in notification_vec {
            self.observer_states.handle_notification(notification);
        }

        Ok(())
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// call the `handle_initialization_notification` and `handle_notification` to update node data.
    pub async fn start(mut self) -> JoinHandle<()> {
        if let Err(err) = self.wait_init_notification().await {
            tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
            self.re_subscribe().await;
        }

        tokio::spawn(async move {
            loop {
                match self.rx.message().await {
                    Ok(resp) => {
                        if resp.is_none() {
                            tracing::warn!("Stream of notification terminated.");
                            self.re_subscribe().await;
                            continue;
                        }
                        self.observer_states.handle_notification(resp.unwrap());
                    }
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
                        self.re_subscribe().await;
                    }
                }
            }
        })
    }

    /// `re_subscribe` is used to re-subscribe to the meta's notification.
    async fn re_subscribe(&mut self) {
        loop {
            match self.client.subscribe(S::subscribe_type()).await {
                Ok(rx) => {
                    tracing::debug!("re-subscribe success");
                    self.rx = rx;
                    if let Err(err) = self.wait_init_notification().await {
                        tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
                        continue;
                    } else {
                        break;
                    }
                }
                Err(_) => {
                    tokio::time::sleep(RE_SUBSCRIBE_RETRY_INTERVAL).await;
                }
            }
        }
    }
}

const RE_SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[async_trait::async_trait]
pub trait Channel: Send + 'static {
    type Item;
    async fn message(&mut self) -> std::result::Result<Option<Self::Item>, Status>;
}

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for Streaming<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, Status> {
        self.message().await
    }
}

#[async_trait::async_trait]
pub trait NotificationClient: Send + Sync + 'static {
    type Channel: Channel<Item = SubscribeResponse>;
    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Self::Channel, ObserverError>;
}

pub struct RpcNotificationClient {
    meta_client: MetaClient,
}

impl RpcNotificationClient {
    pub fn new(meta_client: MetaClient) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl NotificationClient for RpcNotificationClient {
    type Channel = Streaming<SubscribeResponse>;

    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Self::Channel, ObserverError> {
        self.meta_client
            .subscribe(subscribe_type)
            .await
            .map_err(Into::into)
    }
}
