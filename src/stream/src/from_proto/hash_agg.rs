// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Global Streaming Hash Aggregators

use std::marker::PhantomData;
use std::sync::Arc;

use risingwave_common::hash::{calc_hash_key_kind, HashKey, HashKeyDispatcher};

use super::agg_common::{build_agg_call_from_prost, build_agg_state_tables_from_proto};
use super::*;
use crate::executor::aggregation::{AggCall, AggStateTable};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, HashAggExecutor, PkIndices};

pub struct HashAggExecutorDispatcher<S: StateStore>(PhantomData<S>);

pub struct HashAggExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    input: BoxedExecutor,
    agg_calls: Vec<AggCall>,
    agg_state_tables: Vec<Option<AggStateTable<S>>>,
    key_indices: Vec<usize>,
    pk_indices: PkIndices,
    group_by_cache_size: usize,
    extreme_cache_size: usize,
    executor_id: u64,
    metrics: Arc<StreamingMetrics>,
}

impl<S: StateStore> HashKeyDispatcher for HashAggExecutorDispatcher<S> {
    type Input = HashAggExecutorDispatcherArgs<S>;
    type Output = StreamResult<BoxedExecutor>;

    fn dispatch<K: HashKey>(args: Self::Input) -> Self::Output {
        Ok(HashAggExecutor::<K, S>::new(
            args.ctx,
            args.input,
            args.agg_calls,
            args.agg_state_tables,
            args.pk_indices,
            args.executor_id,
            args.key_indices,
            args.group_by_cache_size,
            args.extreme_cache_size,
            args.metrics,
        )?
        .boxed())
    }
}

pub struct HashAggExecutorBuilder;

impl ExecutorBuilder for HashAggExecutorBuilder {
    fn new_boxed_executor(
        params: ExecutorParams,
        node: &StreamNode,
        store: impl StateStore,
        stream: &mut LocalStreamManagerCore,
    ) -> StreamResult<BoxedExecutor> {
        let node = try_match_expand!(node.get_node_body().unwrap(), NodeBody::HashAgg)?;
        let key_indices = node
            .get_group_key()
            .iter()
            .map(|key| *key as usize)
            .collect::<Vec<_>>();
        let [input]: [_; 1] = params.input.try_into().unwrap();
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].data_type())
            .collect_vec();
        let kind = calc_hash_key_kind(&keys);

        let agg_calls: Vec<AggCall> = node
            .get_agg_calls()
            .iter()
            .map(|agg_call| build_agg_call_from_prost(node.is_append_only, agg_call))
            .try_collect()?;

        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for hash agg"));
        let agg_state_tables =
            build_agg_state_tables_from_proto(store, node.get_agg_call_states(), Some(vnodes));

        let args = HashAggExecutorDispatcherArgs {
            ctx: params.actor_context,
            input,
            agg_calls,
            agg_state_tables,
            key_indices,
            pk_indices: params.pk_indices,
            group_by_cache_size: stream.config.developer.unsafe_hash_agg_cache_size,
            extreme_cache_size: stream.config.developer.unsafe_extreme_cache_size,
            executor_id: params.executor_id,
            metrics: params.executor_stats,
        };
        HashAggExecutorDispatcher::dispatch_by_kind(kind, args)
    }
}
