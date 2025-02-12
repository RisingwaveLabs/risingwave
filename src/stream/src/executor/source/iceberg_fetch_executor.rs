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

use std::collections::HashMap;
use std::ops::Bound;

use either::Either;
use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::ScalarRef;
use risingwave_connector::source::iceberg::{
    scan_task_to_chunk, IcebergFileScanTask, IcebergSplit,
};
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{SourceContext, SourceCtrlOpts, SplitMetaData};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{
    get_split_offset_col_idx, get_split_offset_mapping_from_chunk, prune_additional_cols,
    SourceStateTableHandler, StreamSourceCore,
};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const SPLIT_BATCH_SIZE: usize = 1000;

pub struct IcebergFetchExecutor<S: StateStore> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<Executor>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,
}

impl<S: StateStore> IcebergFetchExecutor<S> {
    pub fn new(
        actor_ctx: ActorContextRef,
        stream_source_core: StreamSourceCore<S>,
        upstream: Executor,
        rate_limit_rps: Option<u32>,
    ) -> Self {
        Self {
            actor_ctx,
            stream_source_core: Some(stream_source_core),
            upstream: Some(upstream),
            rate_limit_rps,
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: SourceDesc,
        stream: &mut StreamReaderWithPause<BIASED, StreamChunk>,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<()> {
        let mut batch = Vec::with_capacity(SPLIT_BATCH_SIZE);
        'vnodes: for vnode in state_store_handler.state_table.vnodes().iter_vnodes() {
            let table_iter = state_store_handler
                .state_table
                .iter_with_vnode(
                    vnode,
                    &(Bound::<OwnedRow>::Unbounded, Bound::<OwnedRow>::Unbounded),
                    // This usage is similar with `backfill`. So we only need to fetch a large data rather than establish a connection for a whole object.
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(table_iter);
            let properties = source_desc.source.config.clone();
            while let Some(item) = table_iter.next().await {
                let row = item?;
                let split = match row.datum_at(1) {
                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => match properties {
                        risingwave_connector::source::ConnectorProperties::Iceberg(_) => {
                            IcebergSplit::restore_from_json(jsonb_ref.to_owned_scalar())?
                        }
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                };
                batch.push(split);

                if batch.len() >= SPLIT_BATCH_SIZE {
                    break 'vnodes;
                }
            }
        }
        if batch.is_empty() {
            stream.replace_data_stream(stream::pending().boxed());
        } else {
            *splits_on_fetch += batch.len();
            let batch_reader = Self::build_batched_stream_reader(
                column_ids,
                source_ctx,
                source_desc,
                batch,
                rate_limit_rps,
            )
            .map_err(StreamExecutorError::connector_error);
            stream.replace_data_stream(batch_reader);
        }

        Ok(())
    }

    #[try_stream(ok = StreamChunk, error = StreamExecutorError)]
    async fn build_batched_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: SourceDesc,
        batch: Vec<IcebergSplit>,
        rate_limit_rps: Option<u32>,
    ) {
        // let (stream, _) = source_desc
        //     .source
        //     .build_stream(batch, column_ids, Arc::new(source_ctx), false)
        //     .await
        //     .map_err(StreamExecutorError::connector_error)?;
        // Ok(apply_rate_limit(stream, rate_limit_rps).boxed())
        let properties = source_desc.source.config.clone();
        let properties = match properties {
            risingwave_connector::source::ConnectorProperties::Iceberg(iceberg_properties) => {
                iceberg_properties
            }
            _ => unreachable!(),
        };
        let table = properties.load_table().await?;
        for data_file_scan_task in batch {
            let tasks = match data_file_scan_task.task {
                IcebergFileScanTask::Data(tasks) => tasks,
                IcebergFileScanTask::EqualityDelete(_) | IcebergFileScanTask::PositionDelete(_) => {
                    unreachable!()
                }
            };

            for task in tasks {
                #[for_await]
                for chunk in scan_task_to_chunk(
                    table.clone(),
                    task,
                    1024,
                    // self.schema,
                    false,
                    true,
                ) {
                    let chunk = chunk?;
                    yield StreamChunk::from_parts(
                        itertools::repeat_n(Op::Insert, chunk.cardinality()).collect_vec(),
                        chunk,
                    );
                }
            }
        }
    }

    fn build_source_ctx(
        &self,
        source_desc: &SourceDesc,
        source_id: TableId,
        source_name: &str,
    ) -> SourceContext {
        SourceContext::new(
            self.actor_ctx.id,
            source_id,
            self.actor_ctx.fragment_id,
            source_name.to_owned(),
            source_desc.metrics.clone(),
            SourceCtrlOpts {
                chunk_size: limited_chunk_size(self.rate_limit_rps),
                split_txn: self.rate_limit_rps.is_some(), // when rate limiting, we may split txn
            },
            source_desc.source.config.clone(),
            None,
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(mut self) {
        let mut upstream = self.upstream.take().unwrap().execute();
        let barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = barrier.epoch;
        let is_pause_on_startup = barrier.is_pause_on_startup();
        yield Message::Barrier(barrier);

        let mut core = self.stream_source_core.take().unwrap();
        let mut state_store_handler = core.split_state_store;

        // Build source description from the builder.
        let source_desc_builder = core.source_desc_builder.take().unwrap();

        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;

        // let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        // else {
        //     unreachable!("Partition and offset columns must be set.");
        // };
        // See extract_iceberg_columns. 3 hidden columns that are not available are added in the source schema
        // and row_id is also added
        // and offset & split are 2 hidden include columns.
        let split_idx = source_desc.columns.len() - 3 - 1 - 2;
        let offset_idx = source_desc.columns.len() - 3 - 1 - 2 + 1;
        println!(
            "source_desc.columns: {:#?}, split_idx: {}, offset_idx: {}",
            source_desc.columns, split_idx, offset_idx
        );
        // Initialize state table.
        state_store_handler.init_epoch(first_epoch).await?;

        let mut splits_on_fetch: usize = 0;
        let mut stream =
            StreamReaderWithPause::<true, StreamChunk>::new(upstream, stream::pending().boxed());

        if is_pause_on_startup {
            stream.pause_stream();
        }

        // If it is a recovery startup,
        // there can be file assignments in the state table.
        // Hence we try building a reader first.
        Self::replace_with_new_batch_reader(
            &mut splits_on_fetch,
            &state_store_handler, // move into the function
            core.column_ids.clone(),
            self.build_source_ctx(&source_desc, core.source_id, &core.source_name),
            source_desc.clone(),
            &mut stream,
            self.rate_limit_rps,
        )
        .await?;

        while let Some(msg) = stream.next().await {
            match msg {
                Err(e) => {
                    tracing::error!(error = %e.as_report(), "Fetch Error");
                    splits_on_fetch = 0;
                }
                Ok(msg) => {
                    match msg {
                        // This branch will be preferred.
                        Either::Left(msg) => {
                            match &msg {
                                Message::Barrier(barrier) => {
                                    let mut need_rebuild_reader = false;

                                    if let Some(mutation) = barrier.mutation.as_deref() {
                                        match mutation {
                                            Mutation::Pause => stream.pause_stream(),
                                            Mutation::Resume => stream.resume_stream(),
                                            Mutation::Throttle(actor_to_apply) => {
                                                if let Some(new_rate_limit) =
                                                    actor_to_apply.get(&self.actor_ctx.id)
                                                    && *new_rate_limit != self.rate_limit_rps
                                                {
                                                    tracing::debug!(
                                                        "updating rate limit from {:?} to {:?}",
                                                        self.rate_limit_rps,
                                                        *new_rate_limit
                                                    );
                                                    self.rate_limit_rps = *new_rate_limit;
                                                    need_rebuild_reader = true;
                                                }
                                            }
                                            _ => (),
                                        }
                                    }

                                    state_store_handler
                                        .state_table
                                        .commit(barrier.epoch)
                                        .await?;

                                    if let Some(vnode_bitmap) =
                                        barrier.as_update_vnode_bitmap(self.actor_ctx.id)
                                    {
                                        // if _cache_may_stale, we must rebuild the stream to adjust vnode mappings
                                        let (_prev_vnode_bitmap, cache_may_stale) =
                                            state_store_handler
                                                .state_table
                                                .update_vnode_bitmap(vnode_bitmap);

                                        if cache_may_stale {
                                            splits_on_fetch = 0;
                                        }
                                    }

                                    if splits_on_fetch == 0 || need_rebuild_reader {
                                        Self::replace_with_new_batch_reader(
                                            &mut splits_on_fetch,
                                            &state_store_handler,
                                            core.column_ids.clone(),
                                            self.build_source_ctx(
                                                &source_desc,
                                                core.source_id,
                                                &core.source_name,
                                            ),
                                            source_desc.clone(),
                                            &mut stream,
                                            self.rate_limit_rps,
                                        )
                                        .await?;
                                    }

                                    // Propagate the barrier.
                                    yield msg;
                                }
                                // Receiving file assignments from upstream list executor,
                                // store into state table.
                                Message::Chunk(chunk) => {
                                    let file_assignment = chunk
                                        .data_chunk()
                                        .rows()
                                        .map(|row| {
                                            let split = row.datum_at(1).unwrap().into_jsonb();
                                            IcebergSplit::restore_from_json(split.to_owned_scalar())
                                                .unwrap()
                                        })
                                        .collect();
                                    state_store_handler.set_states(file_assignment).await?;
                                    state_store_handler.state_table.try_flush().await?;
                                }
                                _ => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        Either::Right(chunk) => {
                            println!(
                                "chunk:\n {:}, split_idx: {}, offset_idx: {}",
                                chunk.to_pretty(),
                                split_idx,
                                offset_idx
                            );
                            // let mapping =
                            //     get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx)
                            //         .unwrap();
                            let mut mapping = HashMap::new();
                            // All rows (including those visible or invisible) will be used to update the source offset.
                            for i in 0..chunk.capacity() {
                                let (_, row, _) = chunk.row_at(i);
                                let split_id: Arc<str> =
                                    row.datum_at(split_idx).unwrap().into_utf8().into();
                                let offset = row.datum_at(offset_idx).unwrap().into_int64();
                                mapping.insert(split_id, offset.to_owned());
                            }
                            debug_assert_eq!(mapping.len(), 1);
                            if let Some((split_id, offset)) = mapping.into_iter().next() {
                                let row = state_store_handler
                                    .get(split_id.clone())
                                    .await?
                                    .expect("The fs_split should be in the state table.");
                                let fs_split = match row.datum_at(1) {
                                    Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                        IcebergSplit::restore_from_json(
                                            jsonb_ref.to_owned_scalar(),
                                        )?
                                    }
                                    _ => unreachable!(),
                                };
                                // FIXME: not sure how to update state table yet.
                                // we pass split ()

                                
                                // FIXME(rc): Here we compare `offset` with `fs_split.size` to determine
                                // whether the file is finished, where the `offset` is the starting position
                                // of the NEXT message line in the file. However, In other source connectors,
                                // we use the word `offset` to represent the offset of the current message.
                                // We have to be careful about this semantical inconsistency.
                                // if offset.parse::<usize>().unwrap() >= fs_split.size {
                                //     splits_on_fetch -= 1;
                                //     state_store_handler.delete(split_id).await?;
                                // } else {
                                // state_store_handler
                                //     .set(split_id, fs_split.encode_to_json())
                                //     .await?;
                                // }
                            }

                            let chunk = prune_additional_cols(
                                &chunk,
                                split_idx,
                                offset_idx,
                                &source_desc.columns,
                            );
                            yield Message::Chunk(chunk);
                        }
                    }
                }
            }
        }
    }
}

impl<S: StateStore> Execute for IcebergFetchExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore> Debug for IcebergFetchExecutor<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("IcebergFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("IcebergFetchExecutor").finish()
        }
    }
}
