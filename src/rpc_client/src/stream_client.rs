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

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::TryStreamExt;
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::monitor::{EndpointExt, TcpConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest;
use risingwave_pb::stream_service::streaming_control_stream_response::InitResponse;
use risingwave_pb::stream_service::*;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Endpoint;

use crate::channel::{Channel, WrappedChannelExt};
use crate::error::{Result, RpcError};
use crate::{stream_rpc_client_method_impl, RpcClient, RpcClientPool, UnboundedBidiStreamHandle};

#[derive(Clone)]
pub struct StreamClient(StreamServiceClient<Channel>);

#[async_trait]
impl RpcClient for StreamClient {
    async fn new_client(host_addr: HostAddr) -> Result<Self> {
        Self::new(host_addr).await
    }
}

impl StreamClient {
    async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(5))
            .monitored_connect(
                "grpc-stream-client",
                TcpConfig {
                    tcp_nodelay: true,
                    ..Default::default()
                },
            )
            .await?
            .wrapped();

        Ok(Self(
            StreamServiceClient::new(channel).max_decoding_message_size(usize::MAX),
        ))
    }
}

pub type StreamClientPool = RpcClientPool<StreamClient>;
pub type StreamClientPoolRef = Arc<StreamClientPool>;

macro_rules! for_all_stream_rpc {
    ($macro:ident) => {
        $macro! {
            { 0, get_min_uncommitted_sst_id, GetMinUncommittedSstIdRequest, GetMinUncommittedSstIdResponse }
        }
    };
}

impl StreamClient {
    for_all_stream_rpc! { stream_rpc_client_method_impl }
}

pub type StreamingControlHandle =
    UnboundedBidiStreamHandle<StreamingControlStreamRequest, StreamingControlStreamResponse>;

impl StreamClient {
    pub async fn start_streaming_control(
        &self,
        init_request: PbInitRequest,
    ) -> Result<StreamingControlHandle> {
        let first_request = StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::Init(
                init_request,
            )),
        };
        let mut client = self.0.to_owned();
        let (handle, first_rsp) =
            UnboundedBidiStreamHandle::initialize(first_request, |rx| async move {
                client
                    .streaming_control_stream(UnboundedReceiverStream::new(rx))
                    .await
                    .map(|response| response.into_inner().map_err(RpcError::from_stream_status))
                    .map_err(RpcError::from_stream_status)
            })
            .await?;
        match first_rsp {
            StreamingControlStreamResponse {
                response: Some(streaming_control_stream_response::Response::Init(InitResponse {})),
            } => {}
            other => {
                return Err(anyhow!("expect InitResponse but get {:?}", other).into());
            }
        };
        Ok(handle)
    }
}
