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
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::BasicSerde;
use risingwave_pb::plan_common::StorageTableDesc;
use risingwave_pb::stream_plan::{StreamScanNode, StreamScanType};
use risingwave_storage::table::batch_table::BatchTable;

use super::*;
use crate::common::table::state_table::{ReplicatedStateTable, StateTable};
use crate::executor::{
    ArrangementBackfillExecutor, BackfillExecutor, ChainExecutor, RearrangedChainExecutor,
    TroublemakerExecutor,
};

pub struct StreamScanExecutorBuilder;

impl ExecutorBuilder for StreamScanExecutorBuilder {
    type Node = StreamScanNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        state_store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream, snapshot]: [_; 2] = params.input.try_into().unwrap();
        // For reporting the progress.
        let progress = params
            .local_barrier_manager
            .register_create_mview_progress(params.actor_context.id);

        let output_indices = node
            .output_indices
            .iter()
            .map(|&i| i as usize)
            .collect_vec();

        let exec = match node.stream_scan_type() {
            StreamScanType::Chain | StreamScanType::UpstreamOnly => {
                let upstream_only = matches!(node.stream_scan_type(), StreamScanType::UpstreamOnly);
                ChainExecutor::new(snapshot, upstream, progress, upstream_only).boxed()
            }
            StreamScanType::Rearrange => {
                RearrangedChainExecutor::new(snapshot, upstream, progress).boxed()
            }

            StreamScanType::Backfill => {
                let table_desc: &StorageTableDesc = node.get_table_desc()?;

                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);

                let state_table = if let Ok(table) = node.get_state_table() {
                    Some(
                        StateTable::from_table_catalog(table, state_store.clone(), vnodes.clone())
                            .await,
                    )
                } else {
                    None
                };

                let upstream_table =
                    BatchTable::new_partial(state_store.clone(), column_ids, vnodes, table_desc);

                BackfillExecutor::new(
                    upstream_table,
                    upstream,
                    state_table,
                    output_indices,
                    progress,
                    params.executor_stats.clone(),
                    params.env.config().developer.chunk_size,
                    node.rate_limit.into(),
                )
                .boxed()
            }
            StreamScanType::ArrangementBackfill => {
                let column_ids = node
                    .upstream_column_ids
                    .iter()
                    .map(ColumnId::from)
                    .collect_vec();

                let vnodes = params.vnode_bitmap.map(Arc::new);

                let state_table = node.get_state_table().unwrap();
                let state_table = StateTable::from_table_catalog(
                    state_table,
                    state_store.clone(),
                    vnodes.clone(),
                )
                .await;

                let upstream_table = node.get_arrangement_table().unwrap();
                let versioned = upstream_table.get_version().is_ok();

                macro_rules! new_executor {
                    ($SD:ident) => {{
                        let upstream_table =
                            ReplicatedStateTable::<_, $SD>::from_table_catalog_with_output_column_ids(
                                upstream_table,
                                state_store.clone(),
                                vnodes,
                                column_ids,
                            )
                            .await;
                        ArrangementBackfillExecutor::<_, $SD>::new(
                            upstream_table,
                            upstream,
                            state_table,
                            output_indices,
                            progress,
                            params.executor_stats.clone(),
                            params.env.config().developer.chunk_size,
                            node.rate_limit.into()
                        )
                        .boxed()
                    }};
                }
                if versioned {
                    new_executor!(ColumnAwareSerde)
                } else {
                    new_executor!(BasicSerde)
                }
            }
            StreamScanType::SnapshotBackfill => {
                unreachable!("SnapshotBackfillExecutor is handled specially when in `StreamActorManager::create_nodes_inner`")
            }
            StreamScanType::Unspecified => unreachable!(),
        };

        if crate::consistency::insane() {
            let mut info = params.info.clone();
            info.identity = format!("{} (troubled)", info.identity);
            Ok((
                params.info,
                TroublemakerExecutor::new(
                    (info, exec).into(),
                    params.env.config().developer.chunk_size,
                ),
            )
                .into())
        } else {
            Ok((params.info, exec).into())
        }
    }
}
