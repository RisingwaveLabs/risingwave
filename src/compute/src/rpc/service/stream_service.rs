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

use futures::{Stream, StreamExt, TryStreamExt};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::*;
use risingwave_stream::task::{LocalStreamManager, StreamEnvironment};
use tokio::sync::mpsc::unbounded_channel;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone)]
pub struct StreamServiceImpl {
    pub mgr: LocalStreamManager,
    pub env: StreamEnvironment,
}

impl StreamServiceImpl {
    pub fn new(mgr: LocalStreamManager, env: StreamEnvironment) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    type StreamingControlStreamStream =
        impl Stream<Item = std::result::Result<StreamingControlStreamResponse, tonic::Status>>;

    async fn streaming_control_stream(
        &self,
        request: Request<Streaming<StreamingControlStreamRequest>>,
    ) -> Result<Response<Self::StreamingControlStreamStream>, Status> {
        let mut stream = request.into_inner().boxed();
        let first_request = stream.try_next().await?;
        let Some(StreamingControlStreamRequest {
            request: Some(streaming_control_stream_request::Request::Init(init_request)),
        }) = first_request
        else {
            return Err(Status::invalid_argument(format!(
                "unexpected first request: {:?}",
                first_request
            )));
        };
        let (tx, rx) = unbounded_channel();
        self.mgr.handle_new_control_stream(tx, stream, init_request);
        Ok(Response::new(UnboundedReceiverStream::new(rx)))
    }

    async fn get_min_uncommitted_sst_id(
        &self,
        _request: Request<GetMinUncommittedSstIdRequest>,
    ) -> Result<Response<GetMinUncommittedSstIdResponse>, Status> {
        let min_uncommitted_sst_id = if let Some(hummock) = self.mgr.env.state_store().as_hummock()
        {
            hummock
                .min_uncommitted_sst_id()
                .await
                .unwrap_or(HummockSstableObjectId::MAX)
        } else {
            HummockSstableObjectId::MAX
        };
        Ok(Response::new(GetMinUncommittedSstIdResponse {
            min_uncommitted_sst_id,
        }))
    }
}
