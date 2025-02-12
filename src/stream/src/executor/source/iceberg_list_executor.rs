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


use anyhow::{anyhow, Context};
use either::Either;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_connector::source::iceberg::{IcebergProperties, IcebergSplit};
use risingwave_connector::source::prelude::IcebergSplitEnumerator;
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::{ConnectorProperties, SplitMetaData};
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{barrier_to_message_stream, StreamSourceCore};
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

pub struct IcebergListExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Metrics for monitor.
    #[expect(dead_code)]
    metrics: Arc<StreamingMetrics>,

    /// Receiver of barrier channel.
    barrier_receiver: Option<UnboundedReceiver<Barrier>>,

    /// System parameter reader to read barrier interval
    #[expect(dead_code)]
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    #[expect(dead_code)]
    rate_limit_rps: Option<u32>,
}

impl<S: StateStore> IcebergListExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: Option<StreamSourceCore<S>>,
        metrics: Arc<StreamingMetrics>,
        barrier_receiver: UnboundedReceiver<Barrier>,
        system_params: SystemParamsReaderRef,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core,
            metrics,
            barrier_receiver: Some(barrier_receiver),
            system_params,
            rate_limit_rps,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut barrier_receiver = self.barrier_receiver.take().unwrap();
        let barrier = barrier_receiver
            .recv()
            .instrument_await("source_recv_first_barrier")
            .await
            .ok_or_else(|| {
                anyhow!(
                    "failed to receive the first barrier, actor_id: {:?}, source_id: {:?}",
                    self.actor_ctx.id,
                    self.stream_source_core.as_ref().unwrap().source_id
                )
            })?;

        let mut core = self.stream_source_core.unwrap();

        // Build source description from the builder.
        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = Some(core);

        let properties = source_desc_builder.with_properties();
        let config = ConnectorProperties::extract(properties, false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            unreachable!()
        };

        yield Message::Barrier(barrier);
        let barrier_stream = barrier_to_message_stream(barrier_receiver).boxed();

        let mut chunk_builder = StreamChunkBuilder::new(1024, vec![DataType::Varchar, DataType::Jsonb]);

        // TODO: persist last position in state
        

        // discover initial splits
        // do a regular scan, then switch to incremental scan
        // TODO: we may support starting from a specific snapshot/timestamp later
        let table = iceberg_properties.load_table().await?;
        // FIXME: start snapshot might be empty
        let start_snapshot = table.metadata().current_snapshot().unwrap();
        let last_position = start_snapshot.snapshot_id();
        let snapshot_scan = table
            .scan()
            .snapshot_id(start_snapshot.snapshot_id())
            //   .select(require_names)
            .build()
            .context("failed to build iceberg scan")?;
        let splits = IcebergSplitEnumerator::scan_to_splits(
            table.metadata_ref(),
            snapshot_scan,
            IcebergScanType::DataScan,
            10,
        )
        .await?;
        for split in splits {
            if let Some(chunk) = chunk_builder.append_row(
                Op::Insert,
                &[
                    Some(ScalarImpl::Utf8(split.id().to_string().into())),
                    Some(ScalarImpl::Jsonb(split.encode_to_json())),
                ],
            ) {
                yield Message::Chunk(chunk);
            }
        }
        if let Some(chunk) = chunk_builder.take() {
            yield Message::Chunk(chunk);
        }

        let incremental_scan_stream =
            incremental_scan_stream(*iceberg_properties, last_position, 1).map(|res| match res {
                Ok(split) => {
                    let row = (
                        Op::Insert,
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(split.id().to_string().into())),
                            Some(ScalarImpl::Jsonb(split.encode_to_json())),
                        ]),
                    );
                    Ok(StreamChunk::from_rows(
                        &[row],
                        &[DataType::Varchar, DataType::Jsonb],
                    ))
                }
                Err(e) => Err(e),
            });

        let mut stream =
            StreamReaderWithPause::<true, _>::new(barrier_stream, incremental_scan_stream);

        // if barrier.is_pause_on_startup() {
        //     stream.pause_stream();
        // }

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "encountered an error");
                }
                Ok(msg) => match msg {
                    // Barrier arrives.
                    Either::Left(msg) => match &msg {
                        Message::Barrier(barrier) => {
                            if let Some(mutation) = barrier.mutation.as_deref() {
                                match mutation {
                                    Mutation::Pause => stream.pause_stream(),
                                    Mutation::Resume => stream.resume_stream(),
                                    _ => (),
                                }
                            }

                            // Propagate the barrier.
                            yield msg;
                        }
                        // Only barrier can be received.
                        _ => unreachable!(),
                    },
                    // Chunked FsPage arrives.
                    Either::Right(chunk) => {
                        yield Message::Chunk(chunk);
                    }
                },
            }
        }
    }
}

#[try_stream(boxed, ok = IcebergSplit, error = StreamExecutorError)]
async fn incremental_scan_stream(
    iceberg_properties: IcebergProperties,
    mut last_position: i64,
    list_interval_sec: u64,
) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(list_interval_sec)).await;

        // XXX: should we use sth like table.refresh() instead of reload the table every time?
        // iceberg-java does this, but iceberg-rust doesn't have this API now.
        let table = iceberg_properties.load_table().await?;

        let Some(current_snapshot) = table.metadata().current_snapshot() else {
            tracing::info!("Skip incremental scan because table is empty");
            continue;
        };

        if current_snapshot.snapshot_id() == last_position {
            tracing::info!(
                "Current table snapshot is already enumerated: {}, no new snapshot available",
                last_position
            );
            continue;
        }

        let incremental_scan = table
            .scan()
            .from_snapshot_id(last_position)
            .to_snapshot_id(current_snapshot.snapshot_id())
            //   .select(require_names)
            .build()
            .context("failed to build iceberg scan")?;

        let splits = IcebergSplitEnumerator::scan_to_splits(
            table.metadata_ref(),
            incremental_scan,
            IcebergScanType::DataScan,
            10, // TODO
        )
        .await?;

        for split in splits {
            yield split;
        }

        last_position = current_snapshot.snapshot_id();
    }
}

impl<S: StateStore> Execute for IcebergListExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergListExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("IcebergListExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("IcebergListExecutor").finish()
        }
    }
}
