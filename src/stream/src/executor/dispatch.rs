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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::iter::repeat_with;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use futures::{FutureExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::hash::{ActorMapping, ExpandedActorMapping, VirtualNode};
use risingwave_common::metrics::LabelGuardedIntCounter;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::stream_plan::update_mutation::PbDispatcherUpdate;
use risingwave_pb::stream_plan::PbDispatcher;
use smallvec::{smallvec, SmallVec};
use tokio::time::Instant;
use tokio_stream::adapters::Peekable;
use tokio_stream::StreamExt;
use tracing::{event, Instrument};

use super::exchange::output::{new_output, BoxedOutput};
use super::{
    AddMutation, DispatcherBarriers, DispatcherMessageBatch, MessageBatch, TroublemakerExecutor,
    UpdateMutation,
};
use crate::executor::prelude::*;
use crate::executor::StreamConsumer;
use crate::task::{DispatcherId, SharedContext};

/// [`DispatchExecutor`] consumes messages and send them into downstream actors. Usually,
/// data chunks will be dispatched with some specified policy, while control message
/// such as barriers will be distributed to all receivers.
pub struct DispatchExecutor {
    input: Executor,
    inner: DispatchExecutorInner,
}

struct DispatcherWithMetrics {
    dispatcher: DispatcherImpl,
    actor_output_buffer_blocking_duration_ns: LabelGuardedIntCounter<3>,
}

impl DispatcherWithMetrics {
    pub fn record_output_buffer_blocking_duration(&self, duration: Duration) {
        let ns = duration.as_nanos() as u64;
        self.actor_output_buffer_blocking_duration_ns.inc_by(ns);
    }
}

impl Debug for DispatcherWithMetrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.dispatcher.fmt(f)
    }
}

impl Deref for DispatcherWithMetrics {
    type Target = DispatcherImpl;

    fn deref(&self) -> &Self::Target {
        &self.dispatcher
    }
}

impl DerefMut for DispatcherWithMetrics {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.dispatcher
    }
}

struct DispatchExecutorMetrics {
    actor_id_str: String,
    fragment_id_str: String,
    metrics: Arc<StreamingMetrics>,
    actor_out_record_cnt: LabelGuardedIntCounter<2>,
}

impl DispatchExecutorMetrics {
    fn monitor_dispatcher(&self, dispatcher: DispatcherImpl) -> DispatcherWithMetrics {
        DispatcherWithMetrics {
            actor_output_buffer_blocking_duration_ns: self
                .metrics
                .actor_output_buffer_blocking_duration_ns
                .with_guarded_label_values(&[
                    &self.actor_id_str,
                    &self.fragment_id_str,
                    dispatcher.dispatcher_id_str(),
                ]),
            dispatcher,
        }
    }
}

struct DispatchExecutorInner {
    dispatchers: Vec<DispatcherWithMetrics>,
    actor_id: u32,
    context: Arc<SharedContext>,
    metrics: DispatchExecutorMetrics,
}

impl DispatchExecutorInner {
    async fn dispatch(&mut self, msg: MessageBatch) -> StreamResult<()> {
        let limit = (self.context.config.developer).exchange_concurrent_dispatchers;
        // Only barrier can be batched for now.
        match msg {
            MessageBatch::BarrierBatch(barrier_batch) => {
                if barrier_batch.is_empty() {
                    return Ok(());
                }
                // Only the first barrier in a batch can be mutation.
                let mutation = barrier_batch[0].mutation.clone();
                self.pre_mutate_dispatchers(&mutation)?;
                futures::stream::iter(self.dispatchers.iter_mut())
                    .map(Ok)
                    .try_for_each_concurrent(limit, |dispatcher| async {
                        let start_time = Instant::now();
                        dispatcher
                            .dispatch_barriers(
                                barrier_batch
                                    .iter()
                                    .cloned()
                                    .map(|b| b.into_dispatcher())
                                    .collect(),
                            )
                            .await?;
                        dispatcher.record_output_buffer_blocking_duration(start_time.elapsed());
                        StreamResult::Ok(())
                    })
                    .await?;
                self.post_mutate_dispatchers(&mutation)?;
            }
            MessageBatch::Watermark(watermark) => {
                futures::stream::iter(self.dispatchers.iter_mut())
                    .map(Ok)
                    .try_for_each_concurrent(limit, |dispatcher| async {
                        let start_time = Instant::now();
                        dispatcher.dispatch_watermark(watermark.clone()).await?;
                        dispatcher.record_output_buffer_blocking_duration(start_time.elapsed());
                        StreamResult::Ok(())
                    })
                    .await?;
            }
            MessageBatch::Chunk(chunk) => {
                futures::stream::iter(self.dispatchers.iter_mut())
                    .map(Ok)
                    .try_for_each_concurrent(limit, |dispatcher| async {
                        let start_time = Instant::now();
                        dispatcher.dispatch_data(chunk.clone()).await?;
                        dispatcher.record_output_buffer_blocking_duration(start_time.elapsed());
                        StreamResult::Ok(())
                    })
                    .await?;

                self.metrics
                    .actor_out_record_cnt
                    .inc_by(chunk.cardinality() as _);
            }
        }
        Ok(())
    }

    /// Add new dispatchers to the executor. Will check whether their ids are unique.
    fn add_dispatchers<'a>(
        &mut self,
        new_dispatchers: impl IntoIterator<Item = &'a PbDispatcher>,
    ) -> StreamResult<()> {
        let new_dispatchers: Vec<_> = new_dispatchers
            .into_iter()
            .map(|d| {
                DispatcherImpl::new(&self.context, self.actor_id, d)
                    .map(|dispatcher| self.metrics.monitor_dispatcher(dispatcher))
            })
            .try_collect()?;

        self.dispatchers.extend(new_dispatchers);

        assert!(
            self.dispatchers
                .iter()
                .map(|d| d.dispatcher_id())
                .all_unique(),
            "dispatcher ids must be unique: {:?}",
            self.dispatchers
        );

        Ok(())
    }

    fn find_dispatcher(&mut self, dispatcher_id: DispatcherId) -> &mut DispatcherImpl {
        self.dispatchers
            .iter_mut()
            .find(|d| d.dispatcher_id() == dispatcher_id)
            .unwrap_or_else(|| panic!("dispatcher {}:{} not found", self.actor_id, dispatcher_id))
    }

    /// Update the dispatcher BEFORE we actually dispatch this barrier. We'll only add the new
    /// outputs.
    fn pre_update_dispatcher(&mut self, update: &PbDispatcherUpdate) -> StreamResult<()> {
        let outputs: Vec<_> = update
            .added_downstream_actor_id
            .iter()
            .map(|&id| new_output(&self.context, self.actor_id, id))
            .try_collect()?;

        let dispatcher = self.find_dispatcher(update.dispatcher_id);
        dispatcher.add_outputs(outputs);

        Ok(())
    }

    /// Update the dispatcher AFTER we dispatch this barrier. We'll remove some outputs and finally
    /// update the hash mapping.
    fn post_update_dispatcher(&mut self, update: &PbDispatcherUpdate) -> StreamResult<()> {
        let ids = update.removed_downstream_actor_id.iter().copied().collect();

        let dispatcher = self.find_dispatcher(update.dispatcher_id);
        dispatcher.remove_outputs(&ids);

        // The hash mapping is only used by the hash dispatcher.
        //
        // We specify a single upstream hash mapping for scaling the downstream fragment. However,
        // it's possible that there're multiple upstreams with different exchange types, for
        // example, the `Broadcast` inner side of the dynamic filter. There're too many combinations
        // to handle here, so we just ignore the `hash_mapping` field for any other exchange types.
        if let DispatcherImpl::Hash(dispatcher) = dispatcher {
            dispatcher.hash_mapping =
                ActorMapping::from_protobuf(update.get_hash_mapping()?).to_expanded();
        }

        Ok(())
    }

    /// For `Add` and `Update`, update the dispatchers before we dispatch the barrier.
    fn pre_mutate_dispatchers(&mut self, mutation: &Option<Arc<Mutation>>) -> StreamResult<()> {
        let Some(mutation) = mutation.as_deref() else {
            return Ok(());
        };

        match mutation {
            Mutation::Add(AddMutation { adds, .. }) => {
                if let Some(new_dispatchers) = adds.get(&self.actor_id) {
                    self.add_dispatchers(new_dispatchers)?;
                }
            }
            Mutation::Update(UpdateMutation {
                dispatchers,
                actor_new_dispatchers: actor_dispatchers,
                ..
            }) => {
                if let Some(new_dispatchers) = actor_dispatchers.get(&self.actor_id) {
                    self.add_dispatchers(new_dispatchers)?;
                }

                if let Some(updates) = dispatchers.get(&self.actor_id) {
                    for update in updates {
                        self.pre_update_dispatcher(update)?;
                    }
                }
            }
            Mutation::AddAndUpdate(
                AddMutation { adds, .. },
                UpdateMutation {
                    dispatchers,
                    actor_new_dispatchers: actor_dispatchers,
                    ..
                },
            ) => {
                if let Some(new_dispatchers) = adds.get(&self.actor_id) {
                    self.add_dispatchers(new_dispatchers)?;
                }

                if let Some(new_dispatchers) = actor_dispatchers.get(&self.actor_id) {
                    self.add_dispatchers(new_dispatchers)?;
                }

                if let Some(updates) = dispatchers.get(&self.actor_id) {
                    for update in updates {
                        self.pre_update_dispatcher(update)?;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// For `Stop` and `Update`, update the dispatchers after we dispatch the barrier.
    fn post_mutate_dispatchers(&mut self, mutation: &Option<Arc<Mutation>>) -> StreamResult<()> {
        let Some(mutation) = mutation.as_deref() else {
            return Ok(());
        };

        match mutation {
            Mutation::Stop(stops) => {
                // Remove outputs only if this actor itself is not to be stopped.
                if !stops.contains(&self.actor_id) {
                    for dispatcher in &mut self.dispatchers {
                        dispatcher.remove_outputs(stops);
                    }
                }
            }
            Mutation::Update(UpdateMutation {
                dispatchers,
                dropped_actors,
                ..
            })
            | Mutation::AddAndUpdate(
                _,
                UpdateMutation {
                    dispatchers,
                    dropped_actors,
                    ..
                },
            ) => {
                if let Some(updates) = dispatchers.get(&self.actor_id) {
                    for update in updates {
                        self.post_update_dispatcher(update)?;
                    }
                }

                if !dropped_actors.contains(&self.actor_id) {
                    for dispatcher in &mut self.dispatchers {
                        dispatcher.remove_outputs(dropped_actors);
                    }
                }
            }
            _ => {}
        };

        // After stopping the downstream mview, the outputs of some dispatcher might be empty and we
        // should clean up them.
        self.dispatchers.retain(|d| !d.is_empty());

        Ok(())
    }
}

impl DispatchExecutor {
    pub fn new(
        mut input: Executor,
        dispatchers: Vec<DispatcherImpl>,
        actor_id: u32,
        fragment_id: u32,
        context: Arc<SharedContext>,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
    ) -> Self {
        if crate::consistency::insane() {
            // make some trouble before dispatching to avoid generating invalid dist key.
            let mut info = input.info().clone();
            info.identity = format!("{} (embedded trouble)", info.identity);
            let troublemaker = TroublemakerExecutor::new(input, chunk_size);
            input = (info, troublemaker).into();
        }

        let actor_id_str = actor_id.to_string();
        let fragment_id_str = fragment_id.to_string();
        let actor_out_record_cnt = metrics
            .actor_out_record_cnt
            .with_guarded_label_values(&[&actor_id_str, &fragment_id_str]);
        let metrics = DispatchExecutorMetrics {
            actor_id_str,
            fragment_id_str,
            metrics,
            actor_out_record_cnt,
        };
        let dispatchers = dispatchers
            .into_iter()
            .map(|dispatcher| metrics.monitor_dispatcher(dispatcher))
            .collect();
        Self {
            input,
            inner: DispatchExecutorInner {
                dispatchers,
                actor_id,
                context,
                metrics,
            },
        }
    }
}

impl StreamConsumer for DispatchExecutor {
    type BarrierStream = impl Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(mut self: Box<Self>) -> Self::BarrierStream {
        let max_barrier_count_per_batch =
            self.inner.context.config.developer.max_barrier_batch_size;
        #[try_stream]
        async move {
            let mut input = self.input.execute().peekable();
            loop {
                let Some(message) =
                    try_batch_barriers(max_barrier_count_per_batch, &mut input).await?
                else {
                    // end_of_stream
                    break;
                };
                match message {
                    chunk @ MessageBatch::Chunk(_) => {
                        self.inner
                            .dispatch(chunk)
                            .instrument(tracing::info_span!("dispatch_chunk"))
                            .instrument_await("dispatch_chunk")
                            .await?;
                    }
                    MessageBatch::BarrierBatch(barrier_batch) => {
                        assert!(!barrier_batch.is_empty());
                        self.inner
                            .dispatch(MessageBatch::BarrierBatch(barrier_batch.clone()))
                            .instrument(tracing::info_span!("dispatch_barrier_batch"))
                            .instrument_await("dispatch_barrier_batch")
                            .await?;
                        self.inner
                            .metrics
                            .metrics
                            .barrier_batch_size
                            .observe(barrier_batch.len() as f64);
                        for barrier in barrier_batch {
                            yield barrier;
                        }
                    }
                    watermark @ MessageBatch::Watermark(_) => {
                        self.inner
                            .dispatch(watermark)
                            .instrument(tracing::info_span!("dispatch_watermark"))
                            .instrument_await("dispatch_watermark")
                            .await?;
                    }
                }
            }
        }
    }
}

/// Tries to batch up to `max_barrier_count_per_batch` consecutive barriers within a single message batch.
///
/// Returns the message batch.
///
/// Returns None if end of stream.
async fn try_batch_barriers(
    max_barrier_count_per_batch: u32,
    input: &mut Peekable<BoxedMessageStream>,
) -> StreamResult<Option<MessageBatch>> {
    let Some(msg) = input.next().await else {
        // end_of_stream
        return Ok(None);
    };
    let mut barrier_batch = vec![];
    let msg: Message = msg?;
    let max_peek_attempts = match msg {
        Message::Chunk(c) => {
            return Ok(Some(MessageBatch::Chunk(c)));
        }
        Message::Watermark(w) => {
            return Ok(Some(MessageBatch::Watermark(w)));
        }
        Message::Barrier(b) => {
            let peek_more_barrier = b.mutation.is_none();
            barrier_batch.push(b);
            if peek_more_barrier {
                max_barrier_count_per_batch.saturating_sub(1)
            } else {
                0
            }
        }
    };
    // Try to peek more consecutive non-mutation barriers.
    for _ in 0..max_peek_attempts {
        let peek = input.peek().now_or_never();
        let Some(peek) = peek else {
            break;
        };
        let Some(msg) = peek else {
            // end_of_stream
            break;
        };
        let Ok(Message::Barrier(barrier)) = msg else {
            break;
        };
        if barrier.mutation.is_some() {
            break;
        }
        let msg: Message = input.next().await.unwrap()?;
        let Message::Barrier(ref barrier) = msg else {
            unreachable!("must be a barrier");
        };
        barrier_batch.push(barrier.clone());
    }
    Ok(Some(MessageBatch::BarrierBatch(barrier_batch)))
}

#[derive(Debug)]
pub enum DispatcherImpl {
    Hash(HashDataDispatcher),
    Broadcast(BroadcastDispatcher),
    Simple(SimpleDispatcher),
    RoundRobin(RoundRobinDataDispatcher),
}

impl DispatcherImpl {
    pub fn new(
        context: &SharedContext,
        actor_id: ActorId,
        dispatcher: &PbDispatcher,
    ) -> StreamResult<Self> {
        let outputs = dispatcher
            .downstream_actor_id
            .iter()
            .map(|&down_id| new_output(context, actor_id, down_id))
            .collect::<StreamResult<Vec<_>>>()?;

        let output_indices = dispatcher
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        use risingwave_pb::stream_plan::DispatcherType::*;
        let dispatcher_impl = match dispatcher.get_type()? {
            Hash => {
                assert!(!outputs.is_empty());
                let dist_key_indices = dispatcher
                    .dist_key_indices
                    .iter()
                    .map(|i| *i as usize)
                    .collect();

                let hash_mapping =
                    ActorMapping::from_protobuf(dispatcher.get_hash_mapping()?).to_expanded();

                DispatcherImpl::Hash(HashDataDispatcher::new(
                    outputs,
                    dist_key_indices,
                    output_indices,
                    hash_mapping,
                    dispatcher.dispatcher_id,
                ))
            }
            Broadcast => DispatcherImpl::Broadcast(BroadcastDispatcher::new(
                outputs,
                output_indices,
                dispatcher.dispatcher_id,
            )),
            Simple | NoShuffle => {
                let [output]: [_; 1] = outputs.try_into().unwrap();
                DispatcherImpl::Simple(SimpleDispatcher::new(
                    output,
                    output_indices,
                    dispatcher.dispatcher_id,
                ))
            }
            Unspecified => unreachable!(),
        };

        Ok(dispatcher_impl)
    }
}

macro_rules! impl_dispatcher {
    ($( { $variant_name:ident } ),*) => {
        impl DispatcherImpl {
            pub async fn dispatch_data(&mut self, chunk: StreamChunk) -> StreamResult<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.dispatch_data(chunk).await, )*
                }
            }

            pub async fn dispatch_barriers(&mut self, barriers: DispatcherBarriers) -> StreamResult<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.dispatch_barriers(barriers).await, )*
                }
            }

            pub async fn dispatch_watermark(&mut self, watermark: Watermark) -> StreamResult<()> {
                match self {
                    $( Self::$variant_name(inner) => inner.dispatch_watermark(watermark).await, )*
                }
            }

            pub fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
                match self {
                    $(Self::$variant_name(inner) => inner.add_outputs(outputs), )*
                }
            }

            pub fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
                match self {
                    $(Self::$variant_name(inner) => inner.remove_outputs(actor_ids), )*
                }
            }

            pub fn dispatcher_id(&self) -> DispatcherId {
                match self {
                    $(Self::$variant_name(inner) => inner.dispatcher_id(), )*
                }
            }

            pub fn dispatcher_id_str(&self) -> &str {
                match self {
                    $(Self::$variant_name(inner) => inner.dispatcher_id_str(), )*
                }
            }

            pub fn is_empty(&self) -> bool {
                match self {
                    $(Self::$variant_name(inner) => inner.is_empty(), )*
                }
            }
        }
    }
}

macro_rules! for_all_dispatcher_variants {
    ($macro:ident) => {
        $macro! {
            { Hash },
            { Broadcast },
            { Simple },
            { RoundRobin }
        }
    };
}

for_all_dispatcher_variants! { impl_dispatcher }

pub trait DispatchFuture<'a> = Future<Output = StreamResult<()>> + Send;

pub trait Dispatcher: Debug + 'static {
    /// Dispatch a data chunk to downstream actors.
    fn dispatch_data(&mut self, chunk: StreamChunk) -> impl DispatchFuture<'_>;
    /// Dispatch barriers to downstream actors, generally by broadcasting it.
    fn dispatch_barriers(&mut self, barrier: DispatcherBarriers) -> impl DispatchFuture<'_>;
    /// Dispatch a watermark to downstream actors, generally by broadcasting it.
    fn dispatch_watermark(&mut self, watermark: Watermark) -> impl DispatchFuture<'_>;

    /// Add new outputs to the dispatcher.
    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>);
    /// Remove outputs to `actor_ids` from the dispatcher.
    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>);

    /// The ID of the dispatcher. A [`DispatchExecutor`] may have multiple dispatchers with
    /// different IDs.
    ///
    /// Note that the dispatcher id is always equal to the downstream fragment id.
    /// See also `proto/stream_plan.proto`.
    fn dispatcher_id(&self) -> DispatcherId;

    /// Dispatcher id in string. See [`Dispatcher::dispatcher_id`].
    fn dispatcher_id_str(&self) -> &str;

    /// Whether the dispatcher has no outputs. If so, it'll be cleaned up from the
    /// [`DispatchExecutor`].
    fn is_empty(&self) -> bool;
}

/// Concurrently broadcast a message to all outputs.
///
/// Note that this does not follow `concurrent_dispatchers` in the config and the concurrency is
/// always unlimited.
async fn broadcast_concurrent(
    outputs: impl IntoIterator<Item = &'_ mut BoxedOutput>,
    message: DispatcherMessageBatch,
) -> StreamResult<()> {
    futures::future::try_join_all(
        outputs
            .into_iter()
            .map(|output| output.send(message.clone())),
    )
    .await?;
    Ok(())
}

#[derive(Debug)]
pub struct RoundRobinDataDispatcher {
    outputs: Vec<BoxedOutput>,
    output_indices: Vec<usize>,
    cur: usize,
    dispatcher_id: DispatcherId,
    dispatcher_id_str: String,
}

impl RoundRobinDataDispatcher {
    pub fn new(
        outputs: Vec<BoxedOutput>,
        output_indices: Vec<usize>,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            outputs,
            output_indices,
            cur: 0,
            dispatcher_id,
            dispatcher_id_str: dispatcher_id.to_string(),
        }
    }
}

impl Dispatcher for RoundRobinDataDispatcher {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> StreamResult<()> {
        let chunk = if self.output_indices.len() < chunk.columns().len() {
            chunk
                .project(&self.output_indices)
                .eliminate_adjacent_noop_update()
        } else {
            chunk.project(&self.output_indices)
        };

        self.outputs[self.cur]
            .send(DispatcherMessageBatch::Chunk(chunk))
            .await?;
        self.cur += 1;
        self.cur %= self.outputs.len();
        Ok(())
    }

    async fn dispatch_barriers(&mut self, barriers: DispatcherBarriers) -> StreamResult<()> {
        // always broadcast barrier
        broadcast_concurrent(
            &mut self.outputs,
            DispatcherMessageBatch::BarrierBatch(barriers),
        )
        .await
    }

    async fn dispatch_watermark(&mut self, watermark: Watermark) -> StreamResult<()> {
        if let Some(watermark) = watermark.transform_with_indices(&self.output_indices) {
            // always broadcast watermark
            broadcast_concurrent(
                &mut self.outputs,
                DispatcherMessageBatch::Watermark(watermark),
            )
            .await?;
        }
        Ok(())
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(outputs);
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .extract_if(|output| actor_ids.contains(&output.actor_id()))
            .count();
        self.cur = self.cur.min(self.outputs.len() - 1);
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn dispatcher_id_str(&self) -> &str {
        &self.dispatcher_id_str
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

pub struct HashDataDispatcher {
    outputs: Vec<BoxedOutput>,
    keys: Vec<usize>,
    output_indices: Vec<usize>,
    /// Mapping from virtual node to actor id, used for hash data dispatcher to dispatch tasks to
    /// different downstream actors.
    hash_mapping: ExpandedActorMapping,
    dispatcher_id: DispatcherId,
    dispatcher_id_str: String,
}

impl Debug for HashDataDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashDataDispatcher")
            .field("outputs", &self.outputs)
            .field("keys", &self.keys)
            .field("dispatcher_id", &self.dispatcher_id)
            .finish_non_exhaustive()
    }
}

impl HashDataDispatcher {
    pub fn new(
        outputs: Vec<BoxedOutput>,
        keys: Vec<usize>,
        output_indices: Vec<usize>,
        hash_mapping: ExpandedActorMapping,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            outputs,
            keys,
            output_indices,
            hash_mapping,
            dispatcher_id,
            dispatcher_id_str: dispatcher_id.to_string(),
        }
    }
}

impl Dispatcher for HashDataDispatcher {
    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(outputs);
    }

    async fn dispatch_barriers(&mut self, barriers: DispatcherBarriers) -> StreamResult<()> {
        // always broadcast barrier
        broadcast_concurrent(
            &mut self.outputs,
            DispatcherMessageBatch::BarrierBatch(barriers),
        )
        .await
    }

    async fn dispatch_watermark(&mut self, watermark: Watermark) -> StreamResult<()> {
        if let Some(watermark) = watermark.transform_with_indices(&self.output_indices) {
            // always broadcast watermark
            broadcast_concurrent(
                &mut self.outputs,
                DispatcherMessageBatch::Watermark(watermark),
            )
            .await?;
        }
        Ok(())
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> StreamResult<()> {
        // A chunk can be shuffled into multiple output chunks that to be sent to downstreams.
        // In these output chunks, the only difference are visibility map, which is calculated
        // by the hash value of each line in the input chunk.
        let num_outputs = self.outputs.len();

        // get hash value of every line by its key
        let vnode_count = self.hash_mapping.len();
        let vnodes = VirtualNode::compute_chunk(chunk.data_chunk(), &self.keys, vnode_count);

        tracing::debug!(target: "events::stream::dispatch::hash", "\n{}\n keys {:?} => {:?}", chunk.to_pretty(), self.keys, vnodes);

        let mut vis_maps = repeat_with(|| BitmapBuilder::with_capacity(chunk.capacity()))
            .take(num_outputs)
            .collect_vec();
        let mut last_vnode_when_update_delete = None;
        let mut new_ops: Vec<Op> = Vec::with_capacity(chunk.capacity());

        // Apply output indices after calculating the vnode.
        let chunk = if self.output_indices.len() < chunk.columns().len() {
            chunk
                .project(&self.output_indices)
                .eliminate_adjacent_noop_update()
        } else {
            chunk.project(&self.output_indices)
        };

        for ((vnode, &op), visible) in vnodes
            .iter()
            .copied()
            .zip_eq_fast(chunk.ops())
            .zip_eq_fast(chunk.visibility().iter())
        {
            // Build visibility map for every output chunk.
            for (output, vis_map) in self.outputs.iter().zip_eq_fast(vis_maps.iter_mut()) {
                vis_map.append(visible && self.hash_mapping[vnode.to_index()] == output.actor_id());
            }

            if !visible {
                assert!(
                    last_vnode_when_update_delete.is_none(),
                    "invisible row between U- and U+, op = {op:?}",
                );
                new_ops.push(op);
                continue;
            }

            // The 'update' message, noted by an `UpdateDelete` and a successive `UpdateInsert`,
            // need to be rewritten to common `Delete` and `Insert` if they were dispatched to
            // different actors.
            if op == Op::UpdateDelete {
                last_vnode_when_update_delete = Some(vnode);
            } else if op == Op::UpdateInsert {
                if vnode
                    != last_vnode_when_update_delete
                        .take()
                        .expect("missing U- before U+")
                {
                    new_ops.push(Op::Delete);
                    new_ops.push(Op::Insert);
                } else {
                    new_ops.push(Op::UpdateDelete);
                    new_ops.push(Op::UpdateInsert);
                }
            } else {
                new_ops.push(op);
            }
        }
        assert!(
            last_vnode_when_update_delete.is_none(),
            "missing U+ after U-"
        );

        let ops = new_ops;

        // individually output StreamChunk integrated with vis_map
        futures::future::try_join_all(
            vis_maps
                .into_iter()
                .zip_eq_fast(self.outputs.iter_mut())
                .map(|(vis_map, output)| async {
                    let vis_map = vis_map.finish();
                    // columns is not changed in this function
                    let new_stream_chunk =
                        StreamChunk::with_visibility(ops.clone(), chunk.columns().into(), vis_map);
                    if new_stream_chunk.cardinality() > 0 {
                        event!(
                            tracing::Level::TRACE,
                            msg = "chunk",
                            downstream = output.actor_id(),
                            "send = \n{:#?}",
                            new_stream_chunk
                        );
                        output
                            .send(DispatcherMessageBatch::Chunk(new_stream_chunk))
                            .await?;
                    }
                    StreamResult::Ok(())
                }),
        )
        .await?;

        Ok(())
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .extract_if(|output| actor_ids.contains(&output.actor_id()))
            .count();
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn dispatcher_id_str(&self) -> &str {
        &self.dispatcher_id_str
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

/// `BroadcastDispatcher` dispatches message to all outputs.
#[derive(Debug)]
pub struct BroadcastDispatcher {
    outputs: HashMap<ActorId, BoxedOutput>,
    output_indices: Vec<usize>,
    dispatcher_id: DispatcherId,
    dispatcher_id_str: String,
}

impl BroadcastDispatcher {
    pub fn new(
        outputs: impl IntoIterator<Item = BoxedOutput>,
        output_indices: Vec<usize>,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            outputs: Self::into_pairs(outputs).collect(),
            output_indices,
            dispatcher_id,
            dispatcher_id_str: dispatcher_id.to_string(),
        }
    }

    fn into_pairs(
        outputs: impl IntoIterator<Item = BoxedOutput>,
    ) -> impl Iterator<Item = (ActorId, BoxedOutput)> {
        outputs
            .into_iter()
            .map(|output| (output.actor_id(), output))
    }
}

impl Dispatcher for BroadcastDispatcher {
    async fn dispatch_data(&mut self, chunk: StreamChunk) -> StreamResult<()> {
        let chunk = if self.output_indices.len() < chunk.columns().len() {
            chunk
                .project(&self.output_indices)
                .eliminate_adjacent_noop_update()
        } else {
            chunk.project(&self.output_indices)
        };
        broadcast_concurrent(
            self.outputs.values_mut(),
            DispatcherMessageBatch::Chunk(chunk),
        )
        .await
    }

    async fn dispatch_barriers(&mut self, barriers: DispatcherBarriers) -> StreamResult<()> {
        // always broadcast barrier
        broadcast_concurrent(
            self.outputs.values_mut(),
            DispatcherMessageBatch::BarrierBatch(barriers),
        )
        .await
    }

    async fn dispatch_watermark(&mut self, watermark: Watermark) -> StreamResult<()> {
        if let Some(watermark) = watermark.transform_with_indices(&self.output_indices) {
            // always broadcast watermark
            broadcast_concurrent(
                self.outputs.values_mut(),
                DispatcherMessageBatch::Watermark(watermark),
            )
            .await?;
        }
        Ok(())
    }

    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.outputs.extend(Self::into_pairs(outputs));
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.outputs
            .extract_if(|actor_id, _| actor_ids.contains(actor_id))
            .count();
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn dispatcher_id_str(&self) -> &str {
        &self.dispatcher_id_str
    }

    fn is_empty(&self) -> bool {
        self.outputs.is_empty()
    }
}

/// `SimpleDispatcher` dispatches message to a single output.
#[derive(Debug)]
pub struct SimpleDispatcher {
    /// In most cases, there is exactly one output. However, in some cases of configuration change,
    /// the field needs to be temporarily set to 0 or 2 outputs.
    ///
    /// - When dropping a materialized view, the output will be removed and this field becomes
    ///   empty. The [`DispatchExecutor`] will immediately clean-up this empty dispatcher before
    ///   finishing processing the current mutation.
    /// - When migrating a singleton fragment, the new output will be temporarily added in `pre`
    ///   stage and this field becomes multiple, which is for broadcasting this configuration
    ///   change barrier to both old and new downstream actors. In `post` stage, the old output
    ///   will be removed and this field becomes single again.
    ///
    /// Therefore, when dispatching data, we assert that there's exactly one output by
    /// `Self::output`.
    output: SmallVec<[BoxedOutput; 2]>,
    output_indices: Vec<usize>,
    dispatcher_id: DispatcherId,
    dispatcher_id_str: String,
}

impl SimpleDispatcher {
    pub fn new(
        output: BoxedOutput,
        output_indices: Vec<usize>,
        dispatcher_id: DispatcherId,
    ) -> Self {
        Self {
            output: smallvec![output],
            output_indices,
            dispatcher_id,
            dispatcher_id_str: dispatcher_id.to_string(),
        }
    }
}

impl Dispatcher for SimpleDispatcher {
    fn add_outputs(&mut self, outputs: impl IntoIterator<Item = BoxedOutput>) {
        self.output.extend(outputs);
        assert!(self.output.len() <= 2);
    }

    async fn dispatch_barriers(&mut self, barriers: DispatcherBarriers) -> StreamResult<()> {
        // Only barrier is allowed to be dispatched to multiple outputs during migration.
        for output in &mut self.output {
            output
                .send(DispatcherMessageBatch::BarrierBatch(barriers.clone()))
                .await?;
        }
        Ok(())
    }

    async fn dispatch_data(&mut self, chunk: StreamChunk) -> StreamResult<()> {
        let output = self
            .output
            .iter_mut()
            .exactly_one()
            .expect("expect exactly one output");

        let chunk = if self.output_indices.len() < chunk.columns().len() {
            chunk
                .project(&self.output_indices)
                .eliminate_adjacent_noop_update()
        } else {
            chunk.project(&self.output_indices)
        };
        output.send(DispatcherMessageBatch::Chunk(chunk)).await
    }

    async fn dispatch_watermark(&mut self, watermark: Watermark) -> StreamResult<()> {
        let output = self
            .output
            .iter_mut()
            .exactly_one()
            .expect("expect exactly one output");

        if let Some(watermark) = watermark.transform_with_indices(&self.output_indices) {
            output
                .send(DispatcherMessageBatch::Watermark(watermark))
                .await?;
        }
        Ok(())
    }

    fn remove_outputs(&mut self, actor_ids: &HashSet<ActorId>) {
        self.output
            .retain(|output| !actor_ids.contains(&output.actor_id()));
    }

    fn dispatcher_id(&self) -> DispatcherId {
        self.dispatcher_id
    }

    fn dispatcher_id_str(&self) -> &str {
        &self.dispatcher_id_str
    }

    fn is_empty(&self) -> bool {
        self.output.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::Mutex;

    use async_trait::async_trait;
    use futures::pin_mut;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::{Array, ArrayBuilder, I32ArrayBuilder};
    use risingwave_common::config;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::hash_util::Crc32FastBuilder;
    use risingwave_pb::stream_plan::DispatcherType;

    use super::*;
    use crate::executor::exchange::output::Output;
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::receiver::ReceiverExecutor;
    use crate::executor::{BarrierInner as Barrier, MessageInner as Message};
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;
    use crate::task::test_utils::helper_make_local_actor;

    #[derive(Debug)]
    pub struct MockOutput {
        actor_id: ActorId,
        data: Arc<Mutex<Vec<DispatcherMessageBatch>>>,
    }

    impl MockOutput {
        pub fn new(actor_id: ActorId, data: Arc<Mutex<Vec<DispatcherMessageBatch>>>) -> Self {
            Self { actor_id, data }
        }
    }

    #[async_trait]
    impl Output for MockOutput {
        async fn send(&mut self, message: DispatcherMessageBatch) -> StreamResult<()> {
            self.data.lock().unwrap().push(message);
            Ok(())
        }

        fn actor_id(&self) -> ActorId {
            self.actor_id
        }
    }

    // TODO: this test contains update being shuffled to different partitions, which is not
    // supported for now.
    #[tokio::test]
    async fn test_hash_dispatcher_complex() {
        test_hash_dispatcher_complex_inner().await
    }

    async fn test_hash_dispatcher_complex_inner() {
        // This test only works when vnode count is 256.
        assert_eq!(VirtualNode::COUNT_FOR_TEST, 256);

        let num_outputs = 2; // actor id ranges from 1 to 2
        let key_indices = &[0, 2];
        let output_data_vecs = (0..num_outputs)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect::<Vec<_>>();
        let outputs = output_data_vecs
            .iter()
            .enumerate()
            .map(|(actor_id, data)| {
                Box::new(MockOutput::new(1 + actor_id as u32, data.clone())) as BoxedOutput
            })
            .collect::<Vec<_>>();
        let mut hash_mapping = (1..num_outputs + 1)
            .flat_map(|id| vec![id as ActorId; VirtualNode::COUNT_FOR_TEST / num_outputs])
            .collect_vec();
        hash_mapping.resize(VirtualNode::COUNT_FOR_TEST, num_outputs as u32);
        let mut hash_dispatcher = HashDataDispatcher::new(
            outputs,
            key_indices.to_vec(),
            vec![0, 1, 2],
            hash_mapping,
            0,
        );

        let chunk = StreamChunk::from_pretty(
            "  I I I
            +  4 6 8
            +  5 7 9
            +  0 0 0
            -  1 1 1 D
            U- 2 0 2
            U+ 2 0 2
            U- 3 3 2
            U+ 3 3 4",
        );
        hash_dispatcher.dispatch_data(chunk).await.unwrap();

        assert_eq!(
            *output_data_vecs[0].lock().unwrap()[0].as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I
                +  4 6 8
                +  5 7 9
                +  0 0 0
                -  1 1 1 D
                U- 2 0 2
                U+ 2 0 2
                -  3 3 2 D  // Should rewrite UpdateDelete to Delete
                +  3 3 4    // Should rewrite UpdateInsert to Insert",
            )
        );
        assert_eq!(
            *output_data_vecs[1].lock().unwrap()[0].as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I
                +  4 6 8 D
                +  5 7 9 D
                +  0 0 0 D
                -  1 1 1 D  // Should keep original invisible mark
                U- 2 0 2 D  // Should keep UpdateDelete
                U+ 2 0 2 D  // Should keep UpdateInsert
                -  3 3 2    // Should rewrite UpdateDelete to Delete
                +  3 3 4 D  // Should rewrite UpdateInsert to Insert",
            )
        );
    }

    #[tokio::test]
    async fn test_configuration_change() {
        let _schema = Schema { fields: vec![] };
        let (tx, rx) = channel_for_test();
        let actor_id = 233;
        let fragment_id = 666;
        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let ctx = Arc::new(SharedContext::for_test());
        let metrics = Arc::new(StreamingMetrics::unused());

        let (untouched, old, new) = (234, 235, 238); // broadcast downstream actors
        let (old_simple, new_simple) = (114, 514); // simple downstream actors

        // 1. Register info in context.
        ctx.add_actors(
            [actor_id, untouched, old, new, old_simple, new_simple]
                .into_iter()
                .map(helper_make_local_actor),
        );
        // actor_id -> untouched, old, new, old_simple, new_simple

        let broadcast_dispatcher_id = 666;
        let broadcast_dispatcher = DispatcherImpl::new(
            &ctx,
            actor_id,
            &PbDispatcher {
                r#type: DispatcherType::Broadcast as _,
                dispatcher_id: broadcast_dispatcher_id,
                downstream_actor_id: vec![untouched, old],
                ..Default::default()
            },
        )
        .unwrap();

        let simple_dispatcher_id = 888;
        let simple_dispatcher = DispatcherImpl::new(
            &ctx,
            actor_id,
            &PbDispatcher {
                r#type: DispatcherType::Simple as _,
                dispatcher_id: simple_dispatcher_id,
                downstream_actor_id: vec![old_simple],
                ..Default::default()
            },
        )
        .unwrap();

        let dispatcher_updates = maplit::hashmap! {
            actor_id => vec![PbDispatcherUpdate {
                actor_id,
                dispatcher_id: broadcast_dispatcher_id,
                added_downstream_actor_id: vec![new],
                removed_downstream_actor_id: vec![old],
                hash_mapping: Default::default(),
            }]
        };
        let b1 = Barrier::new_test_barrier(test_epoch(1)).with_mutation(Mutation::Update(
            UpdateMutation {
                dispatchers: dispatcher_updates,
                merges: Default::default(),
                vnode_bitmaps: Default::default(),
                dropped_actors: Default::default(),
                actor_splits: Default::default(),
                actor_new_dispatchers: Default::default(),
            },
        ));
        barrier_test_env.inject_barrier(&b1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let input = Executor::new(
            Default::default(),
            ReceiverExecutor::for_test(
                actor_id,
                rx,
                barrier_test_env.shared_context.clone(),
                barrier_test_env.local_barrier_manager.clone(),
            )
            .boxed(),
        );
        let executor = Box::new(DispatchExecutor::new(
            input,
            vec![broadcast_dispatcher, simple_dispatcher],
            actor_id,
            fragment_id,
            ctx.clone(),
            metrics,
            config::default::developer::stream_chunk_size(),
        ))
        .execute();
        pin_mut!(executor);

        // 2. Take downstream receivers.
        let mut rxs = [untouched, old, new, old_simple, new_simple]
            .into_iter()
            .map(|id| (id, ctx.take_receiver((actor_id, id)).unwrap()))
            .collect::<HashMap<_, _>>();
        macro_rules! try_recv {
            ($down_id:expr) => {
                rxs.get_mut(&$down_id).unwrap().try_recv()
            };
        }

        // 3. Send a chunk.
        tx.send(Message::Chunk(StreamChunk::default()).into())
            .await
            .unwrap();

        tx.send(Message::Barrier(b1.clone().into_dispatcher()).into())
            .await
            .unwrap();
        executor.next().await.unwrap().unwrap();

        // 5. Check downstream.
        try_recv!(untouched).unwrap().as_chunk().unwrap();
        try_recv!(untouched).unwrap().as_barrier_batch().unwrap();

        try_recv!(old).unwrap().as_chunk().unwrap();
        try_recv!(old).unwrap().as_barrier_batch().unwrap(); // It should still receive the barrier even if it's to be removed.

        try_recv!(new).unwrap().as_barrier_batch().unwrap(); // Since it's just added, it won't receive the chunk.

        try_recv!(old_simple).unwrap().as_chunk().unwrap();
        try_recv!(old_simple).unwrap().as_barrier_batch().unwrap(); // Untouched.

        // 6. Send another barrier.
        let b2 = Barrier::new_test_barrier(test_epoch(2));
        barrier_test_env.inject_barrier(&b2, [actor_id]);
        tx.send(Message::Barrier(b2.into_dispatcher()).into())
            .await
            .unwrap();
        executor.next().await.unwrap().unwrap();

        // 7. Check downstream.
        try_recv!(untouched).unwrap().as_barrier_batch().unwrap();
        try_recv!(old).unwrap_err(); // Since it's stopped, we can't receive the new messages.
        try_recv!(new).unwrap().as_barrier_batch().unwrap();

        try_recv!(old_simple).unwrap().as_barrier_batch().unwrap(); // Untouched.
        try_recv!(new_simple).unwrap_err(); // Untouched.

        // 8. Send another chunk.
        tx.send(Message::Chunk(StreamChunk::default()).into())
            .await
            .unwrap();

        // 9. Send a configuration change barrier for simple dispatcher.
        let dispatcher_updates = maplit::hashmap! {
            actor_id => vec![PbDispatcherUpdate {
                actor_id,
                dispatcher_id: simple_dispatcher_id,
                added_downstream_actor_id: vec![new_simple],
                removed_downstream_actor_id: vec![old_simple],
                hash_mapping: Default::default(),
            }]
        };
        let b3 = Barrier::new_test_barrier(test_epoch(3)).with_mutation(Mutation::Update(
            UpdateMutation {
                dispatchers: dispatcher_updates,
                merges: Default::default(),
                vnode_bitmaps: Default::default(),
                dropped_actors: Default::default(),
                actor_splits: Default::default(),
                actor_new_dispatchers: Default::default(),
            },
        ));
        barrier_test_env.inject_barrier(&b3, [actor_id]);
        tx.send(Message::Barrier(b3.into_dispatcher()).into())
            .await
            .unwrap();
        executor.next().await.unwrap().unwrap();

        // 10. Check downstream.
        try_recv!(old_simple).unwrap().as_chunk().unwrap();
        try_recv!(old_simple).unwrap().as_barrier_batch().unwrap(); // It should still receive the barrier even if it's to be removed.

        try_recv!(new_simple).unwrap().as_barrier_batch().unwrap(); // Since it's just added, it won't receive the chunk.

        // 11. Send another barrier.
        let b4 = Barrier::new_test_barrier(test_epoch(4));
        barrier_test_env.inject_barrier(&b4, [actor_id]);
        tx.send(Message::Barrier(b4.into_dispatcher()).into())
            .await
            .unwrap();
        executor.next().await.unwrap().unwrap();

        // 12. Check downstream.
        try_recv!(old_simple).unwrap_err(); // Since it's stopped, we can't receive the new messages.
        try_recv!(new_simple).unwrap().as_barrier_batch().unwrap();
    }

    #[tokio::test]
    async fn test_hash_dispatcher() {
        // This test only works when vnode count is 256.
        assert_eq!(VirtualNode::COUNT_FOR_TEST, 256);

        let num_outputs = 5; // actor id ranges from 1 to 5
        let cardinality = 10;
        let dimension = 4;
        let key_indices = &[0, 2];
        let output_data_vecs = (0..num_outputs)
            .map(|_| Arc::new(Mutex::new(Vec::new())))
            .collect::<Vec<_>>();
        let outputs = output_data_vecs
            .iter()
            .enumerate()
            .map(|(actor_id, data)| {
                Box::new(MockOutput::new(1 + actor_id as u32, data.clone())) as BoxedOutput
            })
            .collect::<Vec<_>>();
        let mut hash_mapping = (1..num_outputs + 1)
            .flat_map(|id| vec![id as ActorId; VirtualNode::COUNT_FOR_TEST / num_outputs])
            .collect_vec();
        hash_mapping.resize(VirtualNode::COUNT_FOR_TEST, num_outputs as u32);
        let mut hash_dispatcher = HashDataDispatcher::new(
            outputs,
            key_indices.to_vec(),
            (0..dimension).collect(),
            hash_mapping.clone(),
            0,
        );

        let mut ops = Vec::new();
        for idx in 0..cardinality {
            if idx % 2 == 0 {
                ops.push(Op::Insert);
            } else {
                ops.push(Op::Delete);
            }
        }

        let mut start = 19260817i32..;
        let mut builders = (0..dimension)
            .map(|_| I32ArrayBuilder::new(cardinality))
            .collect_vec();
        let mut output_cols = vec![vec![vec![]; dimension]; num_outputs];
        let mut output_ops = vec![vec![]; num_outputs];
        for op in &ops {
            let hash_builder = Crc32FastBuilder;
            let mut hasher = hash_builder.build_hasher();
            let one_row = (0..dimension).map(|_| start.next().unwrap()).collect_vec();
            for key_idx in key_indices {
                let val = one_row[*key_idx];
                let bytes = val.to_le_bytes();
                hasher.update(&bytes);
            }
            let output_idx =
                hash_mapping[hasher.finish() as usize % VirtualNode::COUNT_FOR_TEST] as usize - 1;
            for (builder, val) in builders.iter_mut().zip_eq_fast(one_row.iter()) {
                builder.append(Some(*val));
            }
            output_cols[output_idx]
                .iter_mut()
                .zip_eq_fast(one_row.iter())
                .for_each(|(each_column, val)| each_column.push(*val));
            output_ops[output_idx].push(op);
        }

        let columns = builders
            .into_iter()
            .map(|builder| {
                let array = builder.finish();
                array.into_ref()
            })
            .collect();

        let chunk = StreamChunk::new(ops, columns);
        hash_dispatcher.dispatch_data(chunk).await.unwrap();

        for (output_idx, output) in output_data_vecs.into_iter().enumerate() {
            let guard = output.lock().unwrap();
            // It is possible that there is no chunks, as a key doesn't belong to any hash bucket.
            assert!(guard.len() <= 1);
            if guard.is_empty() {
                assert!(output_cols[output_idx].iter().all(|x| { x.is_empty() }));
            } else {
                let message = guard.first().unwrap();
                let real_chunk = match message {
                    DispatcherMessageBatch::Chunk(chunk) => chunk,
                    _ => panic!(),
                };
                real_chunk
                    .columns()
                    .iter()
                    .zip_eq_fast(output_cols[output_idx].iter())
                    .for_each(|(real_col, expect_col)| {
                        let real_vals = real_chunk
                            .visibility()
                            .iter_ones()
                            .map(|row_idx| real_col.as_int32().value_at(row_idx).unwrap())
                            .collect::<Vec<_>>();
                        assert_eq!(real_vals.len(), expect_col.len());
                        assert_eq!(real_vals, *expect_col);
                    });
            }
        }
    }
}
