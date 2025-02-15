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

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Once;
use std::time::Instant;

use anyhow::anyhow;
use either::Either;
use futures::stream::{select_with_strategy, PollNext};
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::metrics::{LabelGuardedIntCounter, GLOBAL_ERROR_METRICS};
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::types::JsonbVal;
use risingwave_connector::source::reader::desc::{SourceDesc, SourceDescBuilder};
use risingwave_connector::source::{BackfillInfo, SplitId, SplitImpl, SplitMetaData};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::TryWaitEpochOptions;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;

use super::executor_core::StreamSourceCore;
use super::get_split_offset_col_idx;
use super::source_backfill_state_table::BackfillStateTableHandler;
use crate::executor::prelude::*;
use crate::executor::source::source_executor::{StreamReaderBuilder, WAIT_BARRIER_MULTIPLE_TIMES};
use crate::executor::source::GetLatestSplitInfoFn;
use crate::executor::UpdateMutation;
use crate::task::CreateMviewProgressReporter;

type SharedBackfillStage = Arc<RwLock<BackfillStage>>;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum BackfillState {
    /// `None` means not started yet. It's the initial state.
    /// XXX: perhaps we can also set to low-watermark instead of `None`
    Backfilling(Option<String>),
    /// Backfill is stopped at this offset (inclusive). Source needs to filter out messages before this offset.
    SourceCachingUp(String),
    Finished,
}
pub type BackfillStates = HashMap<SplitId, BackfillStateWithProgress>;

/// Only `state` field is the real state for fail-over.
/// Other fields are for observability (but we still need to persist them).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct BackfillStateWithProgress {
    pub state: BackfillState,
    pub num_consumed_rows: u64,
    /// The latest offset from upstream (inclusive). After we reach this offset, we can stop backfilling.
    /// This is initialized with the latest available offset in the connector (if the connector provides the ability to fetch it)
    /// so that we can finish backfilling even when upstream doesn't emit any data.
    pub target_offset: Option<String>,
}

impl BackfillStateWithProgress {
    pub fn encode_to_json(self) -> JsonbVal {
        serde_json::to_value(self).unwrap().into()
    }

    pub fn restore_from_json(value: JsonbVal) -> anyhow::Result<Self> {
        serde_json::from_value(value.take()).map_err(|e| anyhow!(e))
    }
}

pub struct SourceBackfillExecutor<S: StateStore> {
    pub inner: SourceBackfillExecutorInner<S>,
    /// Upstream changelog stream which may contain metadata columns, e.g. `_rw_offset`
    pub input: Executor,
}

pub struct SourceBackfillExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Streaming source for external
    // FIXME: some fields e.g. its state table is not used. We might need to refactor. Even latest_split_info is not used.
    stream_source_core: StreamSourceCore<S>,
    backfill_state_store: BackfillStateTableHandler<S>,

    /// Metrics for monitor.
    metrics: Arc<StreamingMetrics>,
    source_split_change_count: LabelGuardedIntCounter<4>,

    // /// Receiver of barrier channel.
    // barrier_receiver: Option<UnboundedReceiver<Barrier>>,
    /// System parameter reader to read barrier interval
    system_params: SystemParamsReaderRef,

    /// Rate limit in rows/s.
    rate_limit_rps: Option<u32>,

    progress: CreateMviewProgressReporter,
}

/// Local variables used in the backfill stage.
///
/// See <https://github.com/risingwavelabs/risingwave/issues/18299> for a state diagram about how it works.
///
/// Note: all off the fields should contain all available splits, and we can `unwrap()` safely when `get()`.
#[derive(Debug)]
struct BackfillStage {
    states: BackfillStates,
    /// A copy of all splits (incl unfinished and finished ones) assigned to the actor.
    ///
    /// Note: the offsets are not updated. Should use `state`'s offset to update before using it (`get_latest_unfinished_splits`).
    splits: Vec<SplitImpl>,
}

impl BackfillStage {
    fn total_backfilled_rows(&self) -> u64 {
        self.states.values().map(|s| s.num_consumed_rows).sum()
    }

    fn debug_assert_consistent(&self) {
        if cfg!(debug_assertions) {
            let all_splits: HashSet<_> =
                self.splits.iter().map(|split| split.id().clone()).collect();
            assert_eq!(
                self.states.keys().cloned().collect::<HashSet<_>>(),
                all_splits
            );
        }
    }

    /// Get unfinished splits with latest offsets according to the backfill states.
    fn get_latest_unfinished_splits(&self) -> StreamExecutorResult<Vec<SplitImpl>> {
        let mut unfinished_splits = Vec::new();
        for split in &self.splits {
            let state = &self.states.get(split.id().as_ref()).unwrap().state;
            match state {
                BackfillState::Backfilling(Some(offset)) => {
                    let mut updated_split = split.clone();
                    updated_split.update_in_place(offset.clone())?;
                    unfinished_splits.push(updated_split);
                }
                BackfillState::Backfilling(None) => unfinished_splits.push(split.clone()),
                BackfillState::SourceCachingUp(_) | BackfillState::Finished => {}
            }
        }
        Ok(unfinished_splits)
    }

    /// Updates backfill states and `target_offsets` and returns whether the row from upstream `SourceExecutor` is visible.
    fn handle_upstream_row(&mut self, split_id: &str, offset: &str) -> bool {
        let mut vis = false;
        let state = self.states.get_mut(split_id).unwrap();
        let state_inner = &mut state.state;
        match state_inner {
            BackfillState::Backfilling(None) => {
                // backfilling for this split is not started yet. Ignore this row
            }
            BackfillState::Backfilling(Some(backfill_offset)) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // continue backfilling. Ignore this row
                    }
                    Ordering::Equal => {
                        // backfilling for this split is finished just right.
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // backfilling for this split produced more data than current source's progress.
                        // We should stop backfilling, and filter out rows from upstream with offset <= backfill_offset.
                        *state_inner = BackfillState::SourceCachingUp(backfill_offset.clone());
                    }
                }
            }
            BackfillState::SourceCachingUp(backfill_offset) => {
                match compare_kafka_offset(backfill_offset, offset) {
                    Ordering::Less => {
                        // Source caught up, but doesn't contain the last backfilled row.
                        // This may happen e.g., if Kafka performed compaction.
                        vis = true;
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Equal => {
                        // Source just caught up with backfilling.
                        *state_inner = BackfillState::Finished;
                    }
                    Ordering::Greater => {
                        // Source is still behind backfilling.
                    }
                }
            }
            BackfillState::Finished => {
                vis = true;
                // This split's backfilling is finished, we are waiting for other splits
            }
        }
        if matches!(state_inner, BackfillState::Backfilling(_)) {
            state.target_offset = Some(offset.to_owned());
        }
        if vis {
            debug_assert_eq!(*state_inner, BackfillState::Finished);
        }
        vis
    }

    /// Updates backfill states and returns whether the row backfilled from external system is visible.
    fn handle_backfill_row(&mut self, split_id: &str, offset: &str) -> bool {
        let state = self.states.get_mut(split_id).unwrap();
        state.num_consumed_rows += 1;
        let state_inner = &mut state.state;
        match state_inner {
            BackfillState::Backfilling(_old_offset) => {
                if let Some(target_offset) = &state.target_offset
                    && compare_kafka_offset(offset, target_offset).is_ge()
                {
                    // Note1: If target_offset = offset, it seems we can mark the state as Finished without waiting for upstream to catch up
                    // and dropping duplicated messages.
                    // But it's not true if target_offset is fetched from other places, like Kafka high watermark.
                    // In this case, upstream hasn't reached the target_offset yet.
                    //
                    // Note2: after this, all following rows in the current chunk will be invisible.
                    //
                    // Note3: if target_offset is None (e.g., when upstream doesn't emit messages at all), we will
                    // keep backfilling.
                    *state_inner = BackfillState::SourceCachingUp(offset.to_owned());
                } else {
                    *state_inner = BackfillState::Backfilling(Some(offset.to_owned()));
                }
                true
            }
            BackfillState::SourceCachingUp(_) | BackfillState::Finished => {
                // backfilling stopped. ignore
                false
            }
        }
    }
}

impl<S: StateStore> SourceBackfillExecutorInner<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        actor_ctx: ActorContextRef,
        info: ExecutorInfo,
        stream_source_core: StreamSourceCore<S>,
        metrics: Arc<StreamingMetrics>,
        system_params: SystemParamsReaderRef,
        backfill_state_store: BackfillStateTableHandler<S>,
        rate_limit_rps: Option<u32>,
        progress: CreateMviewProgressReporter,
    ) -> Self {
        let source_split_change_count = metrics
            .source_split_change_count
            .with_guarded_label_values(&[
                &stream_source_core.source_id.to_string(),
                &stream_source_core.source_name,
                &actor_ctx.id.to_string(),
                &actor_ctx.fragment_id.to_string(),
            ]);

        Self {
            actor_ctx,
            info,
            stream_source_core,
            backfill_state_store,
            metrics,
            source_split_change_count,
            system_params,
            rate_limit_rps,
            progress,
        }
    }

    fn stream_reader_builder(
        &self,
        source_desc: SourceDesc,
        get_latest_split_info_fn: GetLatestSplitInfoFn,
    ) -> StreamReaderBuilder {
        StreamReaderBuilder {
            source_desc,
            rate_limit: self.rate_limit_rps,
            source_id: self.stream_source_core.source_id,
            source_name: self.stream_source_core.source_name.clone(),
            actor_ctx: self.actor_ctx.clone(),
            is_auto_schema_change_enable: false,
            get_latest_split_info_fn,
            reader_stream: None,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute(mut self, input: Executor) {
        let mut input = input.execute();

        // Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        let owned_splits = barrier
            .initial_split_assignment(self.actor_ctx.id)
            .unwrap_or(&[])
            .to_vec();
        let is_pause_on_startup = barrier.is_pause_on_startup();
        let is_initialize = barrier.is_newly_added(self.actor_ctx.id);
        yield Message::Barrier(barrier);

        let mut core = self.stream_source_core;

        let source_desc_builder: SourceDescBuilder = core.source_desc_builder.take().unwrap();
        let source_desc = source_desc_builder
            .build()
            .map_err(StreamExecutorError::connector_error)?;
        let (Some(split_idx), Some(offset_idx)) = get_split_offset_col_idx(&source_desc.columns)
        else {
            unreachable!("Partition and offset columns must be set.");
        };

        self.backfill_state_store.init_epoch(first_epoch).await?;

        let mut backfill_states: BackfillStates = HashMap::new();
        for split in &owned_splits {
            let split_id = split.id();
            let backfill_state = self
                .backfill_state_store
                .try_recover_from_state_store(&split_id)
                .await?
                .unwrap_or(BackfillStateWithProgress {
                    state: BackfillState::Backfilling(None),
                    num_consumed_rows: 0,
                    target_offset: None, // init with None
                });
            backfill_states.insert(split_id, backfill_state);
        }
        let backfill_stage = Arc::new(RwLock::new(BackfillStage {
            states: backfill_states,
            splits: owned_splits,
        }));
        backfill_stage.read().await.debug_assert_consistent();

        // Return the ownership of `stream_source_core` to the source executor.
        self.stream_source_core = core;

        let latest_split_info_weak = Arc::downgrade(&backfill_stage);
        let get_latest_split_info_fn: GetLatestSplitInfoFn = Arc::new(move || {
            let weak = latest_split_info_weak.clone();
            Box::pin(async move {
                let Some(backfill_stage_ref) = weak.upgrade() else {
                    return Err(StreamExecutorError::connector_error(anyhow!(
                        "backfill_stage is already dropped, retry later"
                    )));
                };
                let backfill_stage_guard = backfill_stage_ref.read().await;
                backfill_stage_guard.get_latest_unfinished_splits()
            })
        });
        let mut stream_reader_builder =
            self.stream_reader_builder(source_desc.clone(), get_latest_split_info_fn.clone());

        if is_initialize {
            let mut backfill_stage_guard = backfill_stage.write().await;
            let backfill_info = stream_reader_builder
                .fetch_latest_splits(
                    Some(backfill_stage_guard.get_latest_unfinished_splits()?),
                    false,
                )
                .await?
                .backfill_info;
            for (split_id, info) in &backfill_info {
                let state = backfill_stage_guard.states.get_mut(split_id).unwrap();
                match info {
                    BackfillInfo::NoDataToBackfill => {
                        state.state = BackfillState::Finished;
                    }
                    BackfillInfo::HasDataToBackfill { latest_offset } => {
                        // Note: later we will override it with the offset from the source message, and it's possible to become smaller than this value.
                        state.target_offset = Some(latest_offset.clone());
                    }
                }
            }
        }
        let retry_reader_stream = stream_reader_builder
            .into_retry_stream(
                Some(backfill_stage.read().await.get_latest_unfinished_splits()?),
                false, // do not use the latest, because we need to backfill from the beginning
            )
            .boxed();
        tracing::debug!(?backfill_stage, "source backfill started");

        fn select_strategy(_: &mut ()) -> PollNext {
            futures::stream::PollNext::Left
        }

        // We choose "preferring upstream" strategy here, because:
        // - When the upstream source's workload is high (i.e., Kafka has new incoming data), it just makes backfilling slower.
        //   For chunks from upstream, they are simply dropped, so there's no much overhead.
        //   So possibly this can also affect other running jobs less.
        // - When the upstream Source's becomes less busy, SourceBackfill can begin to catch up.
        let mut backfill_stream = select_with_strategy(
            input.by_ref().map(Either::Left),
            retry_reader_stream.map(Either::Right),
            select_strategy,
        );

        type PausedReader = Option<impl Stream>;
        let mut paused_reader: PausedReader = None;
        let mut command_paused = false;

        macro_rules! pause_reader {
            () => {
                let (left, right) = backfill_stream.into_inner();
                backfill_stream = select_with_strategy(
                    left,
                    futures::stream::pending().boxed().map(Either::Right),
                    select_strategy,
                );
                // XXX: do we have to store the original reader? Can we simply rebuild the reader later?
                paused_reader = Some(right);
            };
        }

        // If the first barrier requires us to pause on startup, pause the stream.
        if is_pause_on_startup {
            command_paused = true;
            pause_reader!();
        }

        let state_store = self.backfill_state_store.state_store.state_store().clone();
        let table_id = self.backfill_state_store.state_store.table_id().into();
        static STATE_TABLE_INITIALIZED: Once = Once::new();
        tokio::spawn(async move {
            // This is for self.backfill_finished() to be safe.
            // We wait for 1st epoch's curr, i.e., the 2nd epoch's prev.
            let epoch = first_epoch.curr;
            tracing::info!("waiting for epoch: {}", epoch);
            state_store
                .try_wait_epoch(
                    HummockReadEpoch::Committed(epoch),
                    TryWaitEpochOptions { table_id },
                )
                .await
                .expect("failed to wait epoch");
            STATE_TABLE_INITIALIZED.call_once(|| ());
            tracing::info!("finished waiting for epoch: {}", epoch);
        });

        {
            let source_backfill_row_count = self
                .metrics
                .source_backfill_row_count
                .with_guarded_label_values(&[
                    &self.stream_source_core.source_id.to_string(),
                    &self.stream_source_core.source_name,
                    &self.actor_ctx.id.to_string(),
                    &self.actor_ctx.fragment_id.to_string(),
                ]);

            // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` * `expected_barrier_latency_ms`
            // milliseconds, considering some other latencies like network and cost in Meta.
            let mut max_wait_barrier_time_ms = self.system_params.load().barrier_interval_ms()
                as u128
                * WAIT_BARRIER_MULTIPLE_TIMES;
            let mut last_barrier_time = Instant::now();
            let mut self_paused = false;

            // The main logic of the loop is in handle_upstream_row and handle_backfill_row.
            'backfill_loop: while let Some(either) = backfill_stream.next().await {
                match either {
                    // Upstream
                    Either::Left(msg) => {
                        let Ok(msg) = msg else {
                            let e = msg.unwrap_err();
                            let core = &self.stream_source_core;
                            tracing::warn!(
                                error = ?e.as_report(),
                                source_id = %core.source_id,
                                "stream source reader error",
                            );
                            GLOBAL_ERROR_METRICS.user_source_error.report([
                                "SourceReaderError".to_owned(),
                                core.source_id.to_string(),
                                core.source_name.to_owned(),
                                self.actor_ctx.fragment_id.to_string(),
                            ]);

                            let reader = self
                                .stream_reader_builder(
                                    source_desc.clone(),
                                    get_latest_split_info_fn.clone(),
                                )
                                .into_retry_stream(
                                    Some(
                                        backfill_stage
                                            .read()
                                            .await
                                            .get_latest_unfinished_splits()?,
                                    ),
                                    false,
                                )
                                .boxed();

                            backfill_stream = select_with_strategy(
                                input.by_ref().map(Either::Left),
                                reader.map(Either::Right),
                                select_strategy,
                            );
                            continue;
                        };
                        match msg {
                            Message::Barrier(barrier) => {
                                last_barrier_time = Instant::now();

                                if self_paused {
                                    // command_paused has a higher priority.
                                    if !command_paused {
                                        backfill_stream = select_with_strategy(
                                            input.by_ref().map(Either::Left),
                                            paused_reader
                                                .take()
                                                .expect("no paused reader to resume"),
                                            select_strategy,
                                        );
                                    }
                                    self_paused = false;
                                }

                                let mut split_changed = false;
                                if let Some(ref mutation) = barrier.mutation.as_deref() {
                                    match mutation {
                                        Mutation::Pause => {
                                            // pause_reader should not be invoked consecutively more than once.
                                            if !command_paused {
                                                pause_reader!();
                                                command_paused = true;
                                            } else {
                                                tracing::warn!(command_paused, "unexpected pause");
                                            }
                                        }
                                        Mutation::Resume => {
                                            // pause_reader.take should not be invoked consecutively more than once.
                                            if command_paused {
                                                backfill_stream = select_with_strategy(
                                                    input.by_ref().map(Either::Left),
                                                    paused_reader
                                                        .take()
                                                        .expect("no paused reader to resume"),
                                                    select_strategy,
                                                );
                                                command_paused = false;
                                            } else {
                                                tracing::warn!(command_paused, "unexpected resume");
                                            }
                                        }
                                        Mutation::SourceChangeSplit(actor_splits) => {
                                            tracing::info!(
                                                actor_splits = ?actor_splits,
                                                "source change split received"
                                            );
                                            split_changed = self
                                                .apply_split_change(
                                                    actor_splits,
                                                    &backfill_stage,
                                                    true,
                                                )
                                                .await?;
                                        }
                                        Mutation::Update(UpdateMutation {
                                            actor_splits, ..
                                        }) => {
                                            split_changed = self
                                                .apply_split_change(
                                                    actor_splits,
                                                    &backfill_stage,
                                                    false,
                                                )
                                                .await?;
                                        }
                                        Mutation::Throttle(actor_to_apply) => {
                                            if let Some(new_rate_limit) =
                                                actor_to_apply.get(&self.actor_ctx.id)
                                                && *new_rate_limit != self.rate_limit_rps
                                            {
                                                tracing::info!(
                                                    "updating rate limit from {:?} to {:?}",
                                                    self.rate_limit_rps,
                                                    *new_rate_limit
                                                );
                                                self.rate_limit_rps = *new_rate_limit;
                                                // rebuild reader
                                                let reader = self
                                                    .stream_reader_builder(
                                                        source_desc.clone(),
                                                        get_latest_split_info_fn.clone(),
                                                    )
                                                    .into_retry_stream(
                                                        Some(
                                                            backfill_stage
                                                                .read()
                                                                .await
                                                                .get_latest_unfinished_splits()?,
                                                        ),
                                                        false,
                                                    )
                                                    .boxed();

                                                backfill_stream = select_with_strategy(
                                                    input.by_ref().map(Either::Left),
                                                    reader.map(Either::Right),
                                                    select_strategy,
                                                );
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                if split_changed {
                                    // rebuild backfill_stream
                                    // Note: we don't put this part in a method, due to some complex lifetime issues.

                                    let latest_unfinished_splits = backfill_stage
                                        .read()
                                        .await
                                        .get_latest_unfinished_splits()?;
                                    tracing::info!(
                                        "actor {:?} apply source split change to {:?}",
                                        self.actor_ctx.id,
                                        latest_unfinished_splits
                                    );

                                    let reader = self
                                        .stream_reader_builder(
                                            source_desc.clone(),
                                            get_latest_split_info_fn.clone(),
                                        )
                                        .into_retry_stream(Some(latest_unfinished_splits), false)
                                        .boxed();

                                    backfill_stream = select_with_strategy(
                                        input.by_ref().map(Either::Left),
                                        reader.map(Either::Right),
                                        select_strategy,
                                    );
                                }

                                self.backfill_state_store
                                    .set_states(backfill_stage.read().await.states.clone())
                                    .await?;
                                self.backfill_state_store
                                    .state_store
                                    .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                                    .await?;

                                if self.should_report_finished(&backfill_stage.read().await.states)
                                {
                                    // drop the backfill kafka consumers
                                    backfill_stream = select_with_strategy(
                                        input.by_ref().map(Either::Left),
                                        futures::stream::pending().boxed().map(Either::Right),
                                        select_strategy,
                                    );

                                    self.progress.finish(
                                        barrier.epoch,
                                        backfill_stage.read().await.total_backfilled_rows(),
                                    );
                                    // yield barrier after reporting progress
                                    yield Message::Barrier(barrier);

                                    // After we reported finished, we still don't exit the loop.
                                    // Because we need to handle split migration.
                                    if STATE_TABLE_INITIALIZED.is_completed() && {
                                        let state = &backfill_stage.read().await.states;
                                        self.backfill_finished(state).await?
                                    } {
                                        break 'backfill_loop;
                                    }
                                } else {
                                    self.progress.update_for_source_backfill(
                                        barrier.epoch,
                                        backfill_stage.read().await.total_backfilled_rows(),
                                    );
                                    // yield barrier after reporting progress
                                    yield Message::Barrier(barrier);
                                }
                            }
                            Message::Chunk(chunk) => {
                                // We need to iterate over all rows because there might be multiple splits in a chunk.
                                // Note: We assume offset from the source is monotonically increasing for the algorithm to work correctly.
                                let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                                for (i, (_, row)) in chunk.rows().enumerate() {
                                    let split = row.datum_at(split_idx).unwrap().into_utf8();
                                    let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                                    let vis = backfill_stage
                                        .write()
                                        .await
                                        .handle_upstream_row(split, offset);
                                    new_vis.set(i, vis);
                                }
                                // emit chunk if vis is not empty. i.e., some splits finished backfilling.
                                let new_vis = new_vis.finish();
                                if new_vis.count_ones() != 0 {
                                    let new_chunk = chunk.clone_with_vis(new_vis);
                                    yield Message::Chunk(new_chunk);
                                }
                            }
                            Message::Watermark(_) => {
                                // Ignore watermark during backfill.
                            }
                        }
                    }
                    // backfill
                    Either::Right(msg) => {
                        let chunk = msg?;

                        if last_barrier_time.elapsed().as_millis() > max_wait_barrier_time_ms {
                            assert!(!command_paused, "command_paused should be false");
                            // pause_reader should not be invoked consecutively more than once.
                            if !self_paused {
                                pause_reader!();
                            } else {
                                tracing::warn!(self_paused, "unexpected self pause");
                            }
                            // Exceeds the max wait barrier time, the source will be paused.
                            // Currently we can guarantee the
                            // source is not paused since it received stream
                            // chunks.
                            self_paused = true;
                            tracing::warn!(
                                "source {} paused, wait barrier for {:?}",
                                self.info.identity,
                                last_barrier_time.elapsed()
                            );

                            // Only update `max_wait_barrier_time_ms` to capture
                            // `barrier_interval_ms`
                            // changes here to avoid frequently accessing the shared
                            // `system_params`.
                            max_wait_barrier_time_ms =
                                self.system_params.load().barrier_interval_ms() as u128
                                    * WAIT_BARRIER_MULTIPLE_TIMES;
                        }
                        let mut new_vis = BitmapBuilder::zeroed(chunk.visibility().len());

                        for (i, (_, row)) in chunk.rows().enumerate() {
                            let split_id = row.datum_at(split_idx).unwrap().into_utf8();
                            let offset = row.datum_at(offset_idx).unwrap().into_utf8();
                            let vis = backfill_stage
                                .write()
                                .await
                                .handle_backfill_row(split_id, offset);
                            new_vis.set(i, vis);
                        }

                        let new_vis = new_vis.finish();
                        let card = new_vis.count_ones();
                        if card != 0 {
                            let new_chunk = chunk.clone_with_vis(new_vis);
                            yield Message::Chunk(new_chunk);
                            source_backfill_row_count.inc_by(card as u64);
                        }
                    }
                }
            }
        }

        std::mem::drop(backfill_stream);
        let mut states = backfill_stage.read().await.states.clone();

        // backfill_stage is done, we just need persist the states but not use the lock.
        drop(backfill_stage);

        // Make sure `Finished` state is persisted.
        self.backfill_state_store.set_states(states.clone()).await?;

        // All splits finished backfilling. Now we only forward the source data.
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Barrier(barrier) => {
                    if let Some(ref mutation) = barrier.mutation.as_deref() {
                        match mutation {
                            Mutation::Pause | Mutation::Resume => {
                                // We don't need to do anything. Handled by upstream.
                            }
                            Mutation::SourceChangeSplit(actor_splits) => {
                                tracing::info!(
                                    actor_splits = ?actor_splits,
                                    "source change split received"
                                );
                                self.apply_split_change_forward_stage(
                                    actor_splits,
                                    &mut states,
                                    true,
                                )
                                .await?;
                            }
                            Mutation::Update(UpdateMutation { actor_splits, .. }) => {
                                self.apply_split_change_forward_stage(
                                    actor_splits,
                                    &mut states,
                                    false,
                                )
                                .await?;
                            }
                            _ => {}
                        }
                    }
                    self.backfill_state_store
                        .state_store
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    yield Message::Barrier(barrier);
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }

    /// When we should call `progress.finish()` to let blocking DDL return.
    /// We report as soon as `SourceCachingUp`. Otherwise the DDL might be blocked forever until upstream messages come.
    ///
    /// Note: split migration (online scaling) is related with progress tracking.
    /// - For foreground DDL, scaling is not allowed before progress is finished.
    /// - For background DDL, scaling is skipped when progress is not finished, and can be triggered by recreating actors during recovery.
    ///
    /// See <https://github.com/risingwavelabs/risingwave/issues/18300> for more details.
    fn should_report_finished(&self, states: &BackfillStates) -> bool {
        states.values().all(|state| {
            matches!(
                state.state,
                BackfillState::Finished | BackfillState::SourceCachingUp(_)
            )
        })
    }

    /// All splits entered `Finished` state.
    ///
    /// We check all splits for the source, including other actors' splits here, before going to the forward stage.
    /// Otherwise if we `break` early, but after rescheduling, an unfinished split is migrated to
    /// this actor, we still need to backfill it.
    ///
    /// Note: at the beginning, the actor will only read the state written by itself.
    /// It needs to _wait until it can read all actors' written data_.
    /// i.e., wait for the first checkpoint has been available.
    ///
    /// See <https://github.com/risingwavelabs/risingwave/issues/18300> for more details.
    async fn backfill_finished(&self, states: &BackfillStates) -> StreamExecutorResult<bool> {
        Ok(states
            .values()
            .all(|state| matches!(state.state, BackfillState::Finished))
            && self
                .backfill_state_store
                .scan()
                .await?
                .into_iter()
                .all(|state| matches!(state.state, BackfillState::Finished)))
    }

    /// For newly added splits, we do not need to backfill and can directly forward from upstream.
    async fn apply_split_change(
        &mut self,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
        stage: &SharedBackfillStage,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        self.source_split_change_count.inc();
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            if self
                .update_state_if_changed(target_splits, stage, should_trim_state)
                .await?
            {
                // Note: we don't rebuild backfill_stream here, due to some complex lifetime issues.
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns `true` if split changed. Otherwise `false`.
    async fn update_state_if_changed(
        &mut self,
        target_splits: Vec<SplitImpl>,
        stage: &SharedBackfillStage,
        should_trim_state: bool,
    ) -> StreamExecutorResult<bool> {
        let mut target_state: BackfillStates = HashMap::with_capacity(target_splits.len());

        let mut split_changed = false;
        // Take out old states (immutable, only used to build target_state and check for added/dropped splits).
        // Will be set to target_state in the end.
        let old_states = std::mem::take(&mut stage.write().await.states);
        // Iterate over the target (assigned) splits
        // - check if any new splits are added
        // - build target_state
        for split in &target_splits {
            let split_id = split.id();
            if let Some(s) = old_states.get(&split_id) {
                target_state.insert(split_id, s.clone());
            } else {
                split_changed = true;

                let backfill_state = self
                    .backfill_state_store
                    .try_recover_from_state_store(&split_id)
                    .await?;
                match backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill.
                        // Note that this branch is different from the initial barrier (BackfillStateInner::Backfilling(None) there).
                        target_state.insert(
                            split_id,
                            BackfillStateWithProgress {
                                state: BackfillState::Finished,
                                num_consumed_rows: 0,
                                target_offset: None,
                            },
                        );
                    }
                    Some(backfill_state) => {
                        // Migrated split. Backfill if unfinished.
                        target_state.insert(split_id, backfill_state);
                    }
                }
            }
        }

        // Checks dropped splits
        for existing_split_id in old_states.keys() {
            if !target_state.contains_key(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        let mut stage_write_guard = stage.write().await;
        if split_changed {
            let dropped_splits = stage_write_guard
                .states
                .extract_if(|split_id, _| !target_state.contains_key(split_id))
                .map(|(split_id, _)| split_id);

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store.trim_state(dropped_splits).await?;
            }
            tracing::info!(old_state=?old_states, new_state=?target_state, "finish split change");
        } else {
            debug_assert_eq!(old_states, target_state);
        }
        stage_write_guard.states = target_state;
        stage_write_guard.splits = target_splits;
        stage_write_guard.debug_assert_consistent();
        Ok(split_changed)
    }

    /// For split change during forward stage, all newly added splits should be already finished.
    // We just need to update the state store if necessary.
    async fn apply_split_change_forward_stage(
        &mut self,
        split_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
        states: &mut BackfillStates,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        self.source_split_change_count.inc();
        if let Some(target_splits) = split_assignment.get(&self.actor_ctx.id).cloned() {
            self.update_state_if_changed_forward_stage(target_splits, states, should_trim_state)
                .await?;
        }

        Ok(())
    }

    async fn update_state_if_changed_forward_stage(
        &mut self,
        target_splits: Vec<SplitImpl>,
        states: &mut BackfillStates,
        should_trim_state: bool,
    ) -> StreamExecutorResult<()> {
        let target_splits: HashSet<SplitId> = target_splits
            .into_iter()
            .map(|split| (split.id()))
            .collect();

        let mut split_changed = false;
        let mut newly_added_splits = vec![];

        // Checks added splits
        for split_id in &target_splits {
            if !states.contains_key(split_id) {
                split_changed = true;

                let backfill_state = self
                    .backfill_state_store
                    .try_recover_from_state_store(split_id)
                    .await?;
                match &backfill_state {
                    None => {
                        // Newly added split. We don't need to backfill!
                        newly_added_splits.push(split_id.clone());
                    }
                    Some(backfill_state) => {
                        // Migrated split. It should also be finished since we are in forwarding stage.
                        match backfill_state.state {
                            BackfillState::Finished => {}
                            _ => {
                                return Err(anyhow::anyhow!(
                                    "Unexpected backfill state: {:?}",
                                    backfill_state
                                )
                                .into());
                            }
                        }
                    }
                }
                states.insert(
                    split_id.clone(),
                    backfill_state.unwrap_or(BackfillStateWithProgress {
                        state: BackfillState::Finished,
                        num_consumed_rows: 0,
                        target_offset: None,
                    }),
                );
            }
        }

        // Checks dropped splits
        for existing_split_id in states.keys() {
            if !target_splits.contains(existing_split_id) {
                tracing::info!("split dropping detected: {}", existing_split_id);
                split_changed = true;
            }
        }

        if split_changed {
            tracing::info!(
                target_splits = ?target_splits,
                "apply split change"
            );

            let dropped_splits = states.extract_if(|split_id, _| !target_splits.contains(split_id));

            if should_trim_state {
                // trim dropped splits' state
                self.backfill_state_store
                    .trim_state(dropped_splits.map(|(k, _v)| k))
                    .await?;
            }

            // For migrated splits, and existing splits, we do not need to update
            // state store, but only for newly added splits.
            self.backfill_state_store
                .set_states(
                    newly_added_splits
                        .into_iter()
                        .map(|split_id| {
                            (
                                split_id,
                                BackfillStateWithProgress {
                                    state: BackfillState::Finished,
                                    num_consumed_rows: 0,
                                    target_offset: None,
                                },
                            )
                        })
                        .collect(),
                )
                .await?;
        }

        Ok(())
    }
}

fn compare_kafka_offset(a: &str, b: &str) -> Ordering {
    let a = a.parse::<i64>().unwrap();
    let b = b.parse::<i64>().unwrap();
    a.cmp(&b)
}

impl<S: StateStore> Execute for SourceBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.inner.execute(self.input).boxed()
    }
}

impl<S: StateStore> Debug for SourceBackfillExecutorInner<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let core = &self.stream_source_core;
        f.debug_struct("SourceBackfillExecutor")
            .field("source_id", &core.source_id)
            .field("column_ids", &core.column_ids)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}
