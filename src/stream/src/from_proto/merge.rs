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

use risingwave_pb::stream_plan::{DispatcherType, MergeNode};

use super::*;
use crate::executor::exchange::input::new_input;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, MergeExecutor, MergeExecutorInput, MergeExecutorUpstream};
use crate::task::SharedContext;

pub struct MergeExecutorBuilder;

impl MergeExecutorBuilder {
    pub(crate) fn new_input(
        shared_context: Arc<SharedContext>,
        executor_stats: Arc<StreamingMetrics>,
        actor_context: ActorContextRef,
        info: ExecutorInfo,
        node: &MergeNode,
        chunk_size: usize,
    ) -> StreamResult<MergeExecutorInput> {
        let upstream_fragment_id = node.get_upstream_fragment_id();

        let inputs: Vec<_> = actor_context
            .initial_upstream_actors
            .get(&node.upstream_fragment_id)
            .map(|actors| actors.actors.iter())
            .into_iter()
            .flatten()
            .map(|&upstream_actor_id| {
                new_input(
                    &shared_context,
                    executor_stats.clone(),
                    actor_context.id,
                    actor_context.fragment_id,
                    upstream_actor_id,
                    upstream_fragment_id,
                )
            })
            .try_collect()?;

        // If there's always only one upstream, we can use `ReceiverExecutor`. Note that it can't
        // scale to multiple upstreams.
        let always_single_input = match node.get_upstream_dispatcher_type()? {
            DispatcherType::Unspecified => unreachable!(),
            DispatcherType::Hash | DispatcherType::Broadcast => false,
            // There could be arbitrary number of upstreams with simple dispatcher.
            DispatcherType::Simple => false,
            // There should be always only one upstream with no-shuffle dispatcher.
            DispatcherType::NoShuffle => true,
        };

        let upstreams = if always_single_input {
            MergeExecutorUpstream::Singleton(inputs.into_iter().exactly_one().unwrap())
        } else {
            MergeExecutorUpstream::Merge(MergeExecutor::new_select_receiver(
                inputs,
                &executor_stats,
                &actor_context,
            ))
        };
        Ok(MergeExecutorInput::new(
            upstreams,
            actor_context,
            upstream_fragment_id,
            shared_context,
            executor_stats,
            info,
            chunk_size,
        ))
    }
}

impl ExecutorBuilder for MergeExecutorBuilder {
    type Node = MergeNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        _store: impl StateStore,
    ) -> StreamResult<Executor> {
        let barrier_rx = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);
        Ok(Self::new_input(
            params.shared_context,
            params.executor_stats,
            params.actor_context,
            params.info,
            node,
            params.env.config().developer.chunk_size,
        )?
        .into_executor(barrier_rx))
    }
}
