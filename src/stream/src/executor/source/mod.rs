// Copyright 2024 RisingWave Labs
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

pub mod executor_core;
use std::collections::HashMap;

use await_tree::InstrumentAwait;
pub use executor_core::StreamSourceCore;
mod fs_source_executor;
#[expect(deprecated)]
pub use fs_source_executor::*;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::row::Row;
use risingwave_connector::source::{SourceColumnDesc, SplitId};
use risingwave_pb::plan_common::additional_column::ColumnType;
use risingwave_pb::plan_common::AdditionalColumn;
pub use state_table_handler::*;
pub mod fetch_executor;
pub use fetch_executor::*;

pub mod source_executor;

pub mod list_executor;
pub mod state_table_handler;
use futures_async_stream::try_stream;
pub use list_executor::*;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Barrier, Message};

/// Receive barriers from barrier manager with the channel, error on channel close.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn barrier_to_message_stream(mut rx: UnboundedReceiver<Barrier>) {
    while let Some(barrier) = rx.recv().instrument_await("receive_barrier").await {
        yield Message::Barrier(barrier);
    }
    bail!("barrier reader closed unexpectedly");
}

pub fn get_split_offset_mapping_from_chunk(
    chunk: &StreamChunk,
    split_idx: usize,
    offset_idx: usize,
) -> Option<HashMap<SplitId, String>> {
    let mut split_offset_mapping = HashMap::new();
    for (_, row) in chunk.rows() {
        let split_id = row.datum_at(split_idx).unwrap().into_utf8().into();
        let offset = row.datum_at(offset_idx).unwrap().into_utf8();
        split_offset_mapping.insert(split_id, offset.to_string());
    }
    Some(split_offset_mapping)
}

pub fn get_split_offset_col_idx(
    column_descs: &[SourceColumnDesc],
) -> (Option<usize>, Option<usize>) {
    let mut split_idx = None;
    let mut offset_idx = None;
    for (idx, column) in column_descs.iter().enumerate() {
        match column.additional_column {
            AdditionalColumn {
                column_type: Some(ColumnType::Partition(_) | ColumnType::Filename(_)),
            } => {
                split_idx = Some(idx);
            }
            AdditionalColumn {
                column_type: Some(ColumnType::Offset(_)),
            } => {
                offset_idx = Some(idx);
            }
            _ => (),
        }
    }
    (split_idx, offset_idx)
}

pub fn prune_additional_cols(
    chunk: &StreamChunk,
    split_idx: usize,
    offset_idx: usize,
    column_descs: &[SourceColumnDesc],
) -> StreamChunk {
    chunk.project(
        &(0..chunk.dimension())
            .filter(|&idx| {
                (idx != split_idx && idx != offset_idx) || column_descs[idx].is_visible()
            })
            .collect_vec(),
    )
}
