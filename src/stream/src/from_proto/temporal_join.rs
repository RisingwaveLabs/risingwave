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

use risingwave_common::catalog::ColumnId;
use risingwave_common::hash::{HashKey, HashKeyDispatcher};
use risingwave_common::types::DataType;
use risingwave_expr::expr::{build_non_strict_from_prost, NonStrictExpression};
use risingwave_pb::plan_common::{JoinType as JoinTypeProto, StorageTableDesc};
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::common::table::state_table::StateTable;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    ActorContextRef, JoinType, NestedLoopTemporalJoinExecutor, TemporalJoinExecutor,
};
use crate::task::AtomicU64Ref;

pub struct TemporalJoinExecutorBuilder;

impl ExecutorBuilder for TemporalJoinExecutorBuilder {
    type Node = TemporalJoinNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let table_desc: &StorageTableDesc = node.get_table_desc()?;
        let condition = match node.get_condition() {
            Ok(cond_prost) => Some(build_non_strict_from_prost(
                cond_prost,
                params.eval_error_report,
            )?),
            Err(_) => None,
        };

        let table_output_indices = node
            .get_table_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();

        let output_indices = node
            .get_output_indices()
            .iter()
            .map(|&x| x as usize)
            .collect_vec();
        let [source_l, source_r]: [_; 2] = params.input.try_into().unwrap();

        if node.get_is_nested_loop() {
            let right_table = BatchTable::new_partial(
                store.clone(),
                table_output_indices
                    .iter()
                    .map(|&x| ColumnId::new(table_desc.columns[x].column_id))
                    .collect_vec(),
                params.vnode_bitmap.clone().map(Into::into),
                table_desc,
            );

            let dispatcher_args = NestedLoopTemporalJoinExecutorDispatcherArgs {
                ctx: params.actor_context,
                info: params.info.clone(),
                left: source_l,
                right: source_r,
                right_table,
                condition,
                output_indices,
                chunk_size: params.env.config().developer.chunk_size,
                metrics: params.executor_stats,
                join_type_proto: node.get_join_type()?,
            };
            Ok((params.info, dispatcher_args.dispatch()?).into())
        } else {
            let table = {
                let column_ids = table_desc
                    .columns
                    .iter()
                    .map(|x| ColumnId::new(x.column_id))
                    .collect_vec();

                BatchTable::new_partial(
                    store.clone(),
                    column_ids,
                    params.vnode_bitmap.clone().map(Into::into),
                    table_desc,
                )
            };

            let table_stream_key_indices = table_desc
                .stream_key
                .iter()
                .map(|&k| k as usize)
                .collect_vec();

            let left_join_keys = node
                .get_left_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec();

            let right_join_keys = node
                .get_right_key()
                .iter()
                .map(|key| *key as usize)
                .collect_vec();

            let null_safe = node.get_null_safe().to_vec();

            let join_key_data_types = left_join_keys
                .iter()
                .map(|idx| source_l.schema().fields[*idx].data_type())
                .collect_vec();

            let memo_table = node.get_memo_table();
            let memo_table = match memo_table {
                Ok(memo_table) => {
                    let vnodes = Arc::new(
                        params
                            .vnode_bitmap
                            .expect("vnodes not set for temporal join"),
                    );
                    Some(
                        StateTable::from_table_catalog(
                            memo_table,
                            store.clone(),
                            Some(vnodes.clone()),
                        )
                        .await,
                    )
                }
                Err(_) => None,
            };
            let append_only = memo_table.is_none();

            let dispatcher_args = TemporalJoinExecutorDispatcherArgs {
                ctx: params.actor_context,
                info: params.info.clone(),
                left: source_l,
                right: source_r,
                right_table: table,
                left_join_keys,
                right_join_keys,
                null_safe,
                condition,
                output_indices,
                table_output_indices,
                table_stream_key_indices,
                watermark_epoch: params.watermark_epoch,
                chunk_size: params.env.config().developer.chunk_size,
                metrics: params.executor_stats,
                join_type_proto: node.get_join_type()?,
                join_key_data_types,
                memo_table,
                append_only,
            };

            Ok((params.info, dispatcher_args.dispatch()?).into())
        }
    }
}

struct TemporalJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: BatchTable<S>,
    left_join_keys: Vec<usize>,
    right_join_keys: Vec<usize>,
    null_safe: Vec<bool>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    table_output_indices: Vec<usize>,
    table_stream_key_indices: Vec<usize>,
    watermark_epoch: AtomicU64Ref,
    chunk_size: usize,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
    join_key_data_types: Vec<DataType>,
    memo_table: Option<StateTable<S>>,
    append_only: bool,
}

impl<S: StateStore> HashKeyDispatcher for TemporalJoinExecutorDispatcherArgs<S> {
    type Output = StreamResult<Box<dyn Execute>>;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident, $append_only:ident) => {
                Ok(Box::new(TemporalJoinExecutor::<
                    K,
                    S,
                    { JoinType::$join_type },
                    { $append_only },
                >::new(
                    self.ctx,
                    self.info,
                    self.left,
                    self.right,
                    self.right_table,
                    self.left_join_keys,
                    self.right_join_keys,
                    self.null_safe,
                    self.condition,
                    self.output_indices,
                    self.table_output_indices,
                    self.table_stream_key_indices,
                    self.watermark_epoch,
                    self.metrics,
                    self.chunk_size,
                    self.join_key_data_types,
                    self.memo_table,
                )))
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Inner => {
                if self.append_only {
                    build!(Inner, true)
                } else {
                    build!(Inner, false)
                }
            }
            JoinTypeProto::LeftOuter => {
                if self.append_only {
                    build!(LeftOuter, true)
                } else {
                    build!(LeftOuter, false)
                }
            }
            _ => unreachable!(),
        }
    }

    fn data_types(&self) -> &[DataType] {
        &self.join_key_data_types
    }
}

struct NestedLoopTemporalJoinExecutorDispatcherArgs<S: StateStore> {
    ctx: ActorContextRef,
    info: ExecutorInfo,
    left: Executor,
    right: Executor,
    right_table: BatchTable<S>,
    condition: Option<NonStrictExpression>,
    output_indices: Vec<usize>,
    chunk_size: usize,
    metrics: Arc<StreamingMetrics>,
    join_type_proto: JoinTypeProto,
}

impl<S: StateStore> NestedLoopTemporalJoinExecutorDispatcherArgs<S> {
    fn dispatch(self) -> StreamResult<Box<dyn Execute>> {
        /// This macro helps to fill the const generic type parameter.
        macro_rules! build {
            ($join_type:ident) => {
                Ok(Box::new(NestedLoopTemporalJoinExecutor::<
                    S,
                    { JoinType::$join_type },
                >::new(
                    self.ctx,
                    self.info,
                    self.left,
                    self.right,
                    self.right_table,
                    self.condition,
                    self.output_indices,
                    self.metrics,
                    self.chunk_size,
                )))
            };
        }
        match self.join_type_proto {
            JoinTypeProto::Inner => {
                build!(Inner)
            }
            JoinTypeProto::LeftOuter => {
                build!(LeftOuter)
            }
            _ => unreachable!(),
        }
    }
}
