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

use std::net::SocketAddr;
use std::sync::Arc;

use either::Either;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_batch::task::BatchManager;
use risingwave_common::catalog::DatabaseId;
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{
    permits, GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse, PbPermits,
};
use risingwave_stream::executor::exchange::permit::{MessageWithPermits, Receiver};
use risingwave_stream::executor::DispatcherMessageBatch;
use risingwave_stream::task::LocalStreamManager;
use thiserror_ext::AsReport;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::rpc::service::exchange_metrics::ExchangeServiceMetrics;

/// Buffer size of the receiver of the remote channel.
const BATCH_EXCHANGE_BUFFER_SIZE: usize = 1024;

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: LocalStreamManager,
    metrics: Arc<ExchangeServiceMetrics>,
}

pub type BatchDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;
pub type StreamDataStream = impl Stream<Item = std::result::Result<GetStreamResponse, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = BatchDataStream;
    type GetStreamStream = StreamDataStream;

    #[cfg_attr(coverage, coverage(off))]
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let pb_task_output_id = request
            .into_inner()
            .task_output_id
            .expect("Failed to get task output id.");
        let (tx, rx) = tokio::sync::mpsc::channel(BATCH_EXCHANGE_BUFFER_SIZE);
        if let Err(e) = self.batch_mgr.get_data(tx, peer_addr, &pb_task_output_id) {
            error!(
                %peer_addr,
                error = %e.as_report(),
                "Failed to serve exchange RPC"
            );
            return Err(e.into());
        }

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream(
        &self,
        request: Request<Streaming<GetStreamRequest>>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        use risingwave_pb::task_service::get_stream_request::*;

        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;

        let mut request_stream: Streaming<GetStreamRequest> = request.into_inner();

        // Extract the first `Get` request from the stream.
        let Get {
            up_actor_id,
            down_actor_id,
            up_fragment_id,
            down_fragment_id,
            database_id,
        } = {
            let req = request_stream
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("get_stream request is empty"))??;
            match req.value.unwrap() {
                Value::Get(get) => get,
                Value::AddPermits(_) => unreachable!("the first message must be `Get`"),
            }
        };

        let receiver = self
            .stream_mgr
            .take_receiver(DatabaseId::new(database_id), (up_actor_id, down_actor_id))
            .await?;

        // Map the remaining stream to add-permits.
        let add_permits_stream = request_stream.map_ok(|req| match req.value.unwrap() {
            Value::Get(_) => unreachable!("the following messages must be `AddPermits`"),
            Value::AddPermits(add_permits) => add_permits.value.unwrap(),
        });

        Ok(Response::new(Self::get_stream_impl(
            self.metrics.clone(),
            peer_addr,
            receiver,
            add_permits_stream,
            (up_fragment_id, down_fragment_id),
        )))
    }
}

impl ExchangeServiceImpl {
    pub fn new(
        mgr: Arc<BatchManager>,
        stream_mgr: LocalStreamManager,
        metrics: Arc<ExchangeServiceMetrics>,
    ) -> Self {
        ExchangeServiceImpl {
            batch_mgr: mgr,
            stream_mgr,
            metrics,
        }
    }

    #[try_stream(ok = GetStreamResponse, error = Status)]
    async fn get_stream_impl(
        metrics: Arc<ExchangeServiceMetrics>,
        peer_addr: SocketAddr,
        mut receiver: Receiver,
        add_permits_stream: impl Stream<Item = std::result::Result<permits::Value, tonic::Status>>,
        up_down_fragment_ids: (u32, u32),
    ) {
        tracing::debug!(target: "events::compute::exchange", peer_addr = %peer_addr, "serve stream exchange RPC");
        let up_fragment_id = up_down_fragment_ids.0.to_string();
        let down_fragment_id = up_down_fragment_ids.1.to_string();

        let permits = receiver.permits();

        // Select from the permits back from the downstream and the upstream receiver.
        let select_stream = futures::stream::select(
            add_permits_stream.map_ok(Either::Left),
            #[try_stream]
            async move {
                while let Some(m) = receiver.recv_raw().await {
                    yield Either::Right(m);
                }
            },
        );
        pin_mut!(select_stream);

        while let Some(r) = select_stream.try_next().await? {
            match r {
                Either::Left(permits_to_add) => {
                    permits.add_permits(permits_to_add);
                }
                Either::Right(MessageWithPermits { message, permits }) => {
                    let proto = message.to_protobuf();
                    // forward the acquired permit to the downstream
                    let response = GetStreamResponse {
                        message: Some(proto),
                        permits: Some(PbPermits { value: permits }),
                    };
                    let bytes = DispatcherMessageBatch::get_encoded_len(&response);

                    yield response;

                    metrics
                        .stream_fragment_exchange_bytes
                        .with_label_values(&[&up_fragment_id, &down_fragment_id])
                        .inc_by(bytes as u64);
                }
            }
        }
    }
}
