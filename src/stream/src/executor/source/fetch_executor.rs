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

use std::marker::PhantomData;
use std::ops::Bound;

use either::Either;
use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::types::ScalarRef;
use risingwave_connector::parser::parquet_parser::get_total_row_nums_for_parquet_file;
use risingwave_connector::parser::EncodingProperties;
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalAzblob, OpendalGcs, OpendalPosixFs, OpendalS3, OpendalSource,
};
use risingwave_connector::source::filesystem::OpendalFsSplit;
use risingwave_connector::source::reader::desc::SourceDesc;
use risingwave_connector::source::{
    BoxSourceChunkStream, SourceContext, SourceCtrlOpts, SplitImpl, SplitMetaData,
};
use risingwave_storage::store::PrefetchOptions;
use thiserror_ext::AsReport;

use super::{
    apply_rate_limit, get_split_offset_col_idx, get_split_offset_mapping_from_chunk,
    prune_additional_cols, SourceStateTableHandler, StreamSourceCore,
};
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::prelude::*;
use crate::executor::stream_reader::StreamReaderWithPause;

const SPLIT_BATCH_SIZE: usize = 1000;

type SplitBatch = Option<Vec<SplitImpl>>;

pub struct FsFetchExecutor<S: StateStore, Src: OpendalSource> {
    actor_ctx: ActorContextRef,

    /// Streaming source for external
    stream_source_core: Option<StreamSourceCore<S>>,

    /// Upstream list executor.
    upstream: Option<Executor>,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    _marker: PhantomData<Src>,
}

impl<S: StateStore, Src: OpendalSource> FsFetchExecutor<S, Src> {
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
            _marker: PhantomData,
        }
    }

    async fn replace_with_new_batch_reader<const BIASED: bool>(
        splits_on_fetch: &mut usize,
        state_store_handler: &SourceStateTableHandler<S>,
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
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
                        risingwave_connector::source::ConnectorProperties::Gcs(_) => {
                            let split: OpendalFsSplit<OpendalGcs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::OpendalS3(_) => {
                            let split: OpendalFsSplit<OpendalS3> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::Azblob(_) => {
                            let split: OpendalFsSplit<OpendalAzblob> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
                        }
                        risingwave_connector::source::ConnectorProperties::PosixFs(_) => {
                            let split: OpendalFsSplit<OpendalPosixFs> =
                                OpendalFsSplit::restore_from_json(jsonb_ref.to_owned_scalar())?;
                            SplitImpl::from(split)
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
                Some(batch),
                rate_limit_rps,
            )
            .await?
            .map_err(StreamExecutorError::connector_error);
            stream.replace_data_stream(batch_reader);
        }

        Ok(())
    }

    async fn build_batched_stream_reader(
        column_ids: Vec<ColumnId>,
        source_ctx: SourceContext,
        source_desc: &SourceDesc,
        batch: SplitBatch,
        rate_limit_rps: Option<u32>,
    ) -> StreamExecutorResult<BoxSourceChunkStream> {
        let (stream, _) = source_desc
            .source
            .build_stream(batch, column_ids, Arc::new(source_ctx), false)
            .await
            .map_err(StreamExecutorError::connector_error)?;
        Ok(apply_rate_limit(stream, rate_limit_rps).boxed())
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

        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };
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
            &source_desc,
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
                                            &source_desc,
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
                                    // For Parquet encoding, the offset indicates the current row being read.
                                    // Therefore, to determine if the end of a Parquet file has been reached, we need to compare its offset with the total number of rows.
                                    // We directly obtain the total row count and set the size in `OpendalFsSplit` to this value.
                                    let file_assignment = if let EncodingProperties::Parquet =
                                        source_desc.source.parser_config.encoding_config
                                    {
                                        let filename_list: Vec<_> = chunk
                                            .data_chunk()
                                            .rows()
                                            .map(|row| {
                                                let filename = row.datum_at(0).unwrap().into_utf8();
                                                filename.to_owned()
                                            })
                                            .collect();
                                        let mut parquet_file_assignment = vec![];
                                        for filename in &filename_list {
                                            let total_row_num =
                                                get_total_row_nums_for_parquet_file(
                                                    filename,
                                                    source_desc.clone(),
                                                )
                                                .await?;
                                            parquet_file_assignment.push(
                                                OpendalFsSplit::<Src>::new(
                                                    filename.to_owned(),
                                                    0,
                                                    total_row_num - 1, // -1 because offset start from 0.
                                                ),
                                            )
                                        }
                                        parquet_file_assignment
                                    } else {
                                        chunk
                                            .data_chunk()
                                            .rows()
                                            .map(|row| {
                                                let filename = row.datum_at(0).unwrap().into_utf8();

                                                let size = row.datum_at(2).unwrap().into_int64();
                                                OpendalFsSplit::<Src>::new(
                                                    filename.to_owned(),
                                                    0,
                                                    size as usize,
                                                )
                                            })
                                            .collect()
                                    };
                                    state_store_handler.set_states(file_assignment).await?;
                                    state_store_handler.state_table.try_flush().await?;
                                }
                                _ => unreachable!(),
                            }
                        }
                        // StreamChunk from FsSourceReader, and the reader reads only one file.
                        Either::Right(chunk) => {
                            let mapping =
                                get_split_offset_mapping_from_chunk(&chunk, split_idx, offset_idx)
                                    .unwrap();
                            debug_assert_eq!(mapping.len(), 1);
                            if let Some((split_id, offset)) = mapping.into_iter().next() {
                                // When `offset = usize::MAX` , it indicates that reading the current file (split_id) is finished.
                                // At this point, it can be deleted from the state table.
                                // This chunk serves only as an EOF marker, so it will be skipped and not yielded to downstream.
                                if offset.parse::<usize>().unwrap() == usize::MAX {
                                    splits_on_fetch -= 1;
                                    state_store_handler.delete(split_id).await?;
                                } else {
                                    let row = state_store_handler.get(split_id.clone()).await?
                                    .unwrap_or_else(|| {
                                        panic!("The fs_split (file_name) {:?} should be in the state table.",
                                      split_id)
                                    });
                                    let fs_split = match row.datum_at(1) {
                                        Some(ScalarRefImpl::Jsonb(jsonb_ref)) => {
                                            OpendalFsSplit::<Src>::restore_from_json(
                                                jsonb_ref.to_owned_scalar(),
                                            )?
                                        }
                                        _ => unreachable!(),
                                    };
                                    state_store_handler
                                        .set(split_id, fs_split.encode_to_json())
                                        .await?;
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
    }
}

impl<S: StateStore, Src: OpendalSource> Execute for FsFetchExecutor<S, Src> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }
}

impl<S: StateStore, Src: OpendalSource> Debug for FsFetchExecutor<S, Src> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(core) = &self.stream_source_core {
            f.debug_struct("FsFetchExecutor")
                .field("source_id", &core.source_id)
                .field("column_ids", &core.column_ids)
                .finish()
        } else {
            f.debug_struct("FsFetchExecutor").finish()
        }
    }
}
