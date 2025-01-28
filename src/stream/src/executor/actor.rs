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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::join_all;
use futures::FutureExt;
use hytra::TrAdder;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::config::StreamingConfig;
use risingwave_common::hash::VirtualNode;
use risingwave_common::log::LogSuppresser;
use risingwave_common::metrics::{IntGaugeExt, GLOBAL_ERROR_METRICS};
use risingwave_common::util::epoch::EpochPair;
use risingwave_expr::expr_context::{expr_context_scope, FRAGMENT_ID, VNODE_COUNT};
use risingwave_expr::ExprError;
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::stream_plan::PbStreamActor;
use risingwave_pb::stream_service::inject_barrier_request::build_actor_info::UpstreamActors;
use risingwave_rpc_client::MetaClient;
use thiserror_ext::AsReport;
use tokio_stream::StreamExt;
use tracing::Instrument;

use super::monitor::StreamingMetrics;
use super::subtask::SubtaskHandle;
use super::StreamConsumer;
use crate::error::StreamResult;
use crate::task::{ActorId, FragmentId, LocalBarrierManager};

/// Shared by all operators of an actor.
pub struct ActorContext {
    pub id: ActorId,
    pub fragment_id: u32,
    pub vnode_count: usize,
    pub mview_definition: String,

    // TODO(eric): these seem to be useless now?
    last_mem_val: Arc<AtomicUsize>,
    cur_mem_val: Arc<AtomicUsize>,
    total_mem_val: Arc<TrAdder<i64>>,

    pub streaming_metrics: Arc<StreamingMetrics>,

    /// This is the number of dispatchers when the actor is created. It will not be updated during runtime when new downstreams are added.
    pub initial_dispatch_num: usize,
    // mv_table_id to subscription id
    pub related_subscriptions: Arc<HashMap<TableId, HashSet<u32>>>,
    pub initial_upstream_actors: HashMap<FragmentId, UpstreamActors>,

    // Meta client. currently used for auto schema change. `None` for test only
    pub meta_client: Option<MetaClient>,

    pub streaming_config: Arc<StreamingConfig>,
}

pub type ActorContextRef = Arc<ActorContext>;

impl ActorContext {
    pub fn for_test(id: ActorId) -> ActorContextRef {
        Arc::new(Self {
            id,
            fragment_id: 0,
            vnode_count: VirtualNode::COUNT_FOR_TEST,
            mview_definition: "".to_owned(),
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val: Arc::new(TrAdder::new()),
            streaming_metrics: Arc::new(StreamingMetrics::unused()),
            // Set 1 for test to enable sanity check on table
            initial_dispatch_num: 1,
            related_subscriptions: HashMap::new().into(),
            initial_upstream_actors: Default::default(),
            meta_client: None,
            streaming_config: Arc::new(StreamingConfig::default()),
        })
    }

    #[expect(clippy::too_many_arguments)]
    pub fn create(
        stream_actor: &PbStreamActor,
        total_mem_val: Arc<TrAdder<i64>>,
        streaming_metrics: Arc<StreamingMetrics>,
        initial_dispatch_num: usize,
        related_subscriptions: Arc<HashMap<TableId, HashSet<u32>>>,
        initial_upstream_actors: HashMap<FragmentId, UpstreamActors>,
        meta_client: Option<MetaClient>,
        streaming_config: Arc<StreamingConfig>,
    ) -> ActorContextRef {
        Arc::new(Self {
            id: stream_actor.actor_id,
            fragment_id: stream_actor.fragment_id,
            mview_definition: stream_actor.mview_definition.clone(),
            vnode_count: (stream_actor.vnode_bitmap.as_ref())
                // An unset `vnode_bitmap` means the actor is a singleton,
                // where only `SINGLETON_VNODE` is set.
                .map_or(1, |b| Bitmap::from(b).len()),
            cur_mem_val: Arc::new(0.into()),
            last_mem_val: Arc::new(0.into()),
            total_mem_val,
            streaming_metrics,
            initial_dispatch_num,
            related_subscriptions,
            initial_upstream_actors,
            meta_client,
            streaming_config,
        })
    }

    pub fn on_compute_error(&self, err: ExprError, identity: &str) {
        static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);
        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
            tracing::error!(identity, error = %err.as_report(), suppressed_count, "failed to evaluate expression");
        }

        let executor_name = identity.split(' ').next().unwrap_or("name_not_found");
        GLOBAL_ERROR_METRICS.user_compute_error.report([
            "ExprError".to_owned(),
            executor_name.to_owned(),
            self.fragment_id.to_string(),
        ]);
    }

    pub fn store_mem_usage(&self, val: usize) {
        // Record the last mem val.
        // Calculate the difference between old val and new value, and apply the diff to total
        // memory usage value.
        let old_value = self.cur_mem_val.load(Ordering::Relaxed);
        self.last_mem_val.store(old_value, Ordering::Relaxed);
        let diff = val as i64 - old_value as i64;

        self.total_mem_val.inc(diff);

        self.cur_mem_val.store(val, Ordering::Relaxed);
    }

    pub fn mem_usage(&self) -> usize {
        self.cur_mem_val.load(Ordering::Relaxed)
    }
}

/// `Actor` is the basic execution unit in the streaming framework.
pub struct Actor<C> {
    /// The [`StreamConsumer`] of the actor.
    consumer: C,
    /// The subtasks to execute concurrently.
    subtasks: Vec<SubtaskHandle>,

    pub actor_context: ActorContextRef,
    expr_context: ExprContext,
    barrier_manager: LocalBarrierManager,
}

impl<C> Actor<C>
where
    C: StreamConsumer,
{
    pub fn new(
        consumer: C,
        subtasks: Vec<SubtaskHandle>,
        _metrics: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
        expr_context: ExprContext,
        barrier_manager: LocalBarrierManager,
    ) -> Self {
        Self {
            consumer,
            subtasks,
            actor_context,
            expr_context,
            barrier_manager,
        }
    }

    #[inline(always)]
    pub async fn run(mut self) -> StreamResult<()> {
        let expr_context = self.expr_context.clone();
        let fragment_id = self.actor_context.fragment_id;
        let vnode_count = self.actor_context.vnode_count;

        let run = async move {
            tokio::join!(
                // Drive the subtasks concurrently.
                join_all(std::mem::take(&mut self.subtasks)),
                self.run_consumer(),
            )
            .1
        }
        .boxed();

        // Attach contexts to the future.
        let run = expr_context_scope(expr_context, run);
        let run = FRAGMENT_ID::scope(fragment_id, run);
        let run = VNODE_COUNT::scope(vnode_count, run);

        run.await
    }

    async fn run_consumer(self) -> StreamResult<()> {
        fail::fail_point!("start_actors_err", |_| Err(anyhow::anyhow!(
            "intentional start_actors_err"
        )
        .into()));

        let id = self.actor_context.id;
        let span_name = format!("Actor {id}");

        let new_span = |epoch: Option<EpochPair>| {
            tracing::info_span!(
                parent: None,
                "actor",
                "otel.name" = span_name,
                actor_id = id,
                prev_epoch = epoch.map(|e| e.prev),
                curr_epoch = epoch.map(|e| e.curr),
            )
        };
        let mut span = new_span(None);

        let actor_count = self
            .actor_context
            .streaming_metrics
            .actor_count
            .with_guarded_label_values(&[&self.actor_context.fragment_id.to_string()]);
        let _actor_count_guard = actor_count.inc_guard();

        let current_epoch = self
            .actor_context
            .streaming_metrics
            .actor_current_epoch
            .with_guarded_label_values(&[
                &self.actor_context.id.to_string(),
                &self.actor_context.fragment_id.to_string(),
            ]);

        let mut last_epoch: Option<EpochPair> = None;
        let mut stream = Box::pin(Box::new(self.consumer).execute());

        // Drive the streaming task with an infinite loop
        let result = loop {
            let barrier = match stream
                .try_next()
                .instrument(span.clone())
                .instrument_await(
                    last_epoch.map_or("Epoch <initial>".into(), |e| format!("Epoch {}", e.curr)),
                )
                .await
            {
                Ok(Some(barrier)) => barrier,
                Ok(None) => break Err(anyhow!("actor exited unexpectedly").into()),
                Err(err) => break Err(err),
            };

            fail::fail_point!("collect_actors_err", id == 10, |_| Err(anyhow::anyhow!(
                "intentional collect_actors_err"
            )
            .into()));

            // Then stop this actor if asked
            if barrier.is_stop(id) {
                debug!(actor_id = id, epoch = ?barrier.epoch, "stop at barrier");
                break Ok(barrier);
            }

            current_epoch.set(barrier.epoch.curr as i64);

            // Collect barriers to local barrier manager
            self.barrier_manager.collect(id, &barrier);

            // Tracing related work
            last_epoch = Some(barrier.epoch);
            span = barrier.tracing_context().attach(new_span(last_epoch));
        };

        spawn_blocking_drop_stream(stream).await;

        let result = result.map(|stop_barrier| {
            // Collect the stop barrier after the stream has been dropped to ensure that all resources
            self.barrier_manager.collect(id, &stop_barrier);
        });

        tracing::debug!(actor_id = id, ok = result.is_ok(), "actor exit");
        result
    }
}

/// Drop the stream in a blocking task to avoid interfering with other actors.
///
/// Logically the actor is dropped after we send the barrier with `Drop` mutation to the
/// downstream, thus making the `drop`'s progress asynchronous. However, there might be a
/// considerable amount of data in the executors' in-memory cache, dropping these structures might
/// be a CPU-intensive task. This may lead to the runtime being unable to schedule other actors if
/// the `drop` is called on the current thread.
pub async fn spawn_blocking_drop_stream<T: Send + 'static>(stream: T) {
    let _ = tokio::task::spawn_blocking(move || drop(stream))
        .instrument_await("drop_stream")
        .await;
}
