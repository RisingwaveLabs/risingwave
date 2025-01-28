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

use std::cmp::min;
use std::sync::Arc;

use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{
    build_func_non_strict, build_non_strict_from_prost, InputRefExpression, NonStrictExpression,
};
use risingwave_pb::plan_common::JoinType as JoinTypeProto;
use risingwave_pb::stream_plan::HashJoinNode;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::hash_join::*;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{ActorContextRef, JoinType};
use crate::task::AtomicU64Ref;

pub struct HashJoinExecutorBuilder;

impl ExecutorBuilder for HashJoinExecutorBuilder {
    type Node = HashJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let is_append_only = node.is_append_only;
        let vnodes = Arc::new(params.vnode_bitmap.expect("vnodes not set for hash join"));

        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        let table_l = node.get_left_table()?;
        let degree_table_l = node.get_left_degree_table()?;

        let table_r = node.get_right_table()?;
        let degree_table_r = node.get_right_degree_table()?;

        let params_l = JoinParams::new(
            node.get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_left_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let params_r = JoinParams::new(
            node.get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
            node.get_right_deduped_input_pk_indices()
                .iter()
                .map(|key| *key as usize)
                .collect_vec(),
        );
        let null_safe = node.get_null_safe().to_vec();
        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(build_non_strict_from_prost(
                cond_prost,
                params.eval_error_report.clone(),
            )?),
            Err(_) => None,
        };
        trace!("Join non-equi condition: {:?}", condition);
        let mut inequality_pairs = Vec::with_capacity(node.get_inequality_pairs().len());
        for inequality_pair in node.get_inequality_pairs() {
            let key_required_larger = inequality_pair.get_key_required_larger() as usize;
            let key_required_smaller = inequality_pair.get_key_required_smaller() as usize;
            inequality_pairs.push((
                key_required_larger,
                key_required_smaller,
                inequality_pair.get_clean_state(),
                if let Some(delta_expression) = inequality_pair.delta_expression.as_ref() {
                    let data_type = source_l.schema().fields
                        [min(key_required_larger, key_required_smaller)]
                    .data_type();
                    Some(build_func_non_strict(
                        delta_expression.delta_type(),
                        data_type.clone(),
                        vec![
                            Box::new(InputRefExpression::new(data_type, 0)),
                            build_non_strict_from_prost(
                                delta_expression.delta.as_ref().unwrap(),
                                params.eval_error_report.clone(),
                            )?
                            .into_inner(),
                        ],
                        params.eval_error_report.clone(),
                    )?)
                } else {
                    None
                },
            ));
        }

        let join_key_data_types = params_l
            .join_key_indices
            .iter()
            .map(|idx| source_l.schema().fields[*idx].data_type())
            .collect_vec();

        let state_table_l =
            StateTable::from_table_catalog(table_l, store.clone(), Some(vnodes.clone())).await;
        let degree_state_table_l =
            StateTable::from_table_catalog(degree_table_l, store.clone(), Some(vnodes.clone()))
                .await;

        let state_table_r =
            StateTable::from_table_catalog(table_r, store.clone(), Some(vnodes.clone())).await;
        let degree_state_table_r =
            StateTable::from_table_catalog(degree_table_r, store, Some(vnodes)).await;

        let args = HashJoinExecutorDispatcherArgs {
            ctx: params.actor_context,
            info: params.info.clone(),
            source_l,
            source_r,
            params_l,
            params_r,
            null_safe,
            output_indices,
            cond: condition,
            inequality_pairs,
            state_table_l,
            degree_state_table_l,
            state_table_r,
            degree_state_table_r,
            lru_manager: params.watermark_epoch,
            is_append_only,
            metrics: params.executor_stats,
            join_type_proto: node.get_join_type()?,
            join_key_data_types,
            chunk_size: params.env.config().developer.chunk_size,
            high_join_amplification_threshold: params
                .env
                .config()
                .developer
                .high_join_amplification_threshold,
        };

        let exec = args.dispatch()?;
        Ok((params.info, exec).into())
    }
}

struct HashJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    source_l: Executor,
    source_r: Executor,
    params_l: JoinParams,
    params_r: JoinParams,
    null_safe: Vec<bool>,
    output_indices: Vec<usize>,
    cond: Option<NonStrictExpression>,
    inequality_pairs: Vec<(usize, usize, bool, Option<NonStrictExpression>)>,
    state_table_l: StateTable<S>,
    degree_state_table_l: StateTable<S>,
    state_table_r: StateTable<S>,
    degree_state_table_r: StateTable<S>,
    lru_manager: AtomicU64Ref,
    is_append_only: bool,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
    chunk_size: usize,
    high_join_amplification_threshold: usize,
}

impl<S: StateStore> HashKeyDispatcher for HashJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident) => {
                HashJoinExecutor::<K, S, { JoinType::$join_type }>::new(
                    self.ctx,
                    self.info,
                    self.source_l,
                    self.source_r,
                    self.params_l,
                    self.params_r,
                    self.null_safe,
                    self.output_indices,
                    self.cond,
                    self.inequality_pairs,
                    self.state_table_l,
                    self.degree_state_table_l,
                    self.state_table_r,
                    self.degree_state_table_r,
                    self.lru_manager,
                    self.is_append_only,
                    self.metrics,
                    self.chunk_size,
                    self.high_join_amplification_threshold,
                )
                .boxed()
            };
        }
        let executor = match self.join_type_proto {
            JoinTypeProto::AsofInner
            | JoinTypeProto::AsofLeftOuter
            | JoinTypeProto::Unspecified => unreachable!(),
            JoinTypeProto::Inner => build!(Inner),
            JoinTypeProto::LeftOuter => build!(LeftOuter),
            JoinTypeProto::RightOuter => build!(RightOuter),
            JoinTypeProto::FullOuter => build!(FullOuter),
            JoinTypeProto::LeftSemi => build!(LeftSemi),
            JoinTypeProto::LeftAnti => build!(LeftAnti),
            JoinTypeProto::RightSemi => build!(RightSemi),
            JoinTypeProto::RightAnti => build!(RightAnti),
        };
        Ok(executor)
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}
