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
use std::ops::Deref;
use std::sync::Arc;

use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use prometheus::Histogram;
use risingwave_common::array::DataChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnId, Schema};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::common::BatchQueryEpoch;
use risingwave_pb::plan_common::as_of::AsOfType;
use risingwave_pb::plan_common::{as_of, PbAsOf, StorageTableDesc};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::{dispatch_state_store, StateStore};

use super::ScanRange;
use crate::error::{BatchError, Result};
use crate::executor::{
    build_scan_ranges_from_pb, BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor,
    ExecutorBuilder,
};
use crate::monitor::BatchMetrics;

/// Executor that scans data from row table
pub struct RowSeqScanExecutor<S: StateStore> {
    chunk_size: usize,
    identity: String,

    /// Batch metrics.
    /// None: Local mode don't record mertics.
    metrics: Option<BatchMetrics>,

    table: StorageTable<S>,
    scan_ranges: Vec<ScanRange>,
    ordered: bool,
    epoch: BatchQueryEpoch,
    limit: Option<u64>,
    as_of: Option<AsOf>,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AsOf {
    pub timestamp: i64,
}

impl TryFrom<&PbAsOf> for AsOf {
    type Error = BatchError;

    fn try_from(pb: &PbAsOf) -> std::result::Result<Self, Self::Error> {
        match pb.as_of_type.as_ref().unwrap() {
            AsOfType::Timestamp(ts) => Ok(Self {
                timestamp: ts.timestamp,
            }),
            AsOfType::ProcessTime(_) | AsOfType::Version(_) => Err(BatchError::TimeTravel(
                anyhow::anyhow!("batch query does not support as of process time or version"),
            )),
        }
    }
}

impl From<&AsOf> for PbAsOf {
    fn from(v: &AsOf) -> Self {
        PbAsOf {
            as_of_type: Some(AsOfType::Timestamp(as_of::Timestamp {
                timestamp: v.timestamp,
            })),
        }
    }
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    pub fn new(
        table: StorageTable<S>,
        scan_ranges: Vec<ScanRange>,
        ordered: bool,
        epoch: BatchQueryEpoch,
        chunk_size: usize,
        identity: String,
        limit: Option<u64>,
        metrics: Option<BatchMetrics>,
        as_of: Option<AsOf>,
    ) -> Self {
        Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            ordered,
            epoch,
            limit,
            as_of,
        }
    }
}

pub struct RowSeqScanExecutorBuilder {}

impl BoxedExecutorBuilder for RowSeqScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Row sequential scan should not have input executor!"
        );
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_desc: &StorageTableDesc = seq_scan_node.get_table_desc()?;
        let column_ids = seq_scan_node
            .column_ids
            .iter()
            .copied()
            .map(ColumnId::from)
            .collect();
        let vnodes = match &seq_scan_node.vnode_bitmap {
            Some(vnodes) => Some(Bitmap::from(vnodes).into()),
            // This is possible for dml. vnode_bitmap is not filled by scheduler.
            // Or it's single distribution, e.g., distinct agg. We scan in a single executor.
            None => Some(Bitmap::ones(table_desc.vnode_count()).into()),
        };

        let scan_ranges = build_scan_ranges_from_pb(&seq_scan_node.scan_ranges, table_desc)?;

        let ordered = seq_scan_node.ordered;

        let epoch = source.epoch();
        let limit = seq_scan_node.limit;
        let as_of = seq_scan_node
            .as_of
            .as_ref()
            .map(AsOf::try_from)
            .transpose()?;
        let chunk_size = if let Some(limit) = seq_scan_node.limit {
            (limit as u32).min(source.context().get_config().developer.chunk_size as u32)
        } else {
            source.context().get_config().developer.chunk_size as u32
        };
        let metrics = source.context().batch_metrics();

        dispatch_state_store!(source.context().state_store(), state_store, {
            let table = StorageTable::new_partial(state_store, column_ids, vnodes, table_desc);
            Ok(Box::new(RowSeqScanExecutor::new(
                table,
                scan_ranges,
                ordered,
                epoch,
                chunk_size as usize,
                source.plan_node().get_identity().clone(),
                limit,
                metrics,
                as_of,
            )))
        })
    }
}

impl<S: StateStore> Executor for RowSeqScanExecutor<S> {
    fn schema(&self) -> &Schema {
        self.table.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl<S: StateStore> RowSeqScanExecutor<S> {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let Self {
            chunk_size,
            identity,
            metrics,
            table,
            scan_ranges,
            ordered,
            epoch,
            limit,
            as_of,
        } = *self;
        let table = Arc::new(table);
        // as_of takes precedence
        let query_epoch = as_of
            .map(|a| {
                let epoch = unix_timestamp_sec_to_epoch(a.timestamp).0;
                tracing::debug!(epoch, identity, "time travel");
                risingwave_pb::common::BatchQueryEpoch {
                    epoch: Some(risingwave_pb::common::batch_query_epoch::Epoch::TimeTravel(
                        epoch,
                    )),
                }
            })
            .unwrap_or_else(|| epoch);

        // Create collector.
        let histogram = metrics
            .as_ref()
            .map(|metrics| &metrics.executor_metrics().row_seq_scan_next_duration);

        if ordered {
            // Currently we execute range-scans concurrently so the order is not guaranteed if
            // there're multiple ranges.
            // TODO: reserve the order for multiple ranges.
            assert_eq!(scan_ranges.len(), 1);
        }

        let (point_gets, range_scans): (Vec<ScanRange>, Vec<ScanRange>) = scan_ranges
            .into_iter()
            .partition(|x| x.pk_prefix.len() == table.pk_indices().len());

        // the number of rows have been returned as execute result
        let mut returned = 0;
        if let Some(limit) = &limit
            && returned >= *limit
        {
            return Ok(());
        }
        let mut data_chunk_builder = DataChunkBuilder::new(table.schema().data_types(), chunk_size);
        // Point Get
        for point_get in point_gets {
            let table = table.clone();
            if let Some(row) =
                Self::execute_point_get(table, point_get, query_epoch, histogram).await?
            {
                if let Some(chunk) = data_chunk_builder.append_one_row(row) {
                    returned += chunk.cardinality() as u64;
                    yield chunk;
                    if let Some(limit) = &limit
                        && returned >= *limit
                    {
                        return Ok(());
                    }
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            returned += chunk.cardinality() as u64;
            yield chunk;
            if let Some(limit) = &limit
                && returned >= *limit
            {
                return Ok(());
            }
        }

        // Range Scan
        // WARN: DO NOT use `select` to execute range scans concurrently
        //       it can consume too much memory if there're too many ranges.
        for range in range_scans {
            let stream = Self::execute_range(
                table.clone(),
                range,
                ordered,
                query_epoch,
                chunk_size,
                limit,
                histogram,
            );
            #[for_await]
            for chunk in stream {
                let chunk = chunk?;
                returned += chunk.cardinality() as u64;
                yield chunk;
                if let Some(limit) = &limit
                    && returned >= *limit
                {
                    return Ok(());
                }
            }
        }
    }

    async fn execute_point_get(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        epoch: BatchQueryEpoch,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) -> Result<Option<OwnedRow>> {
        let pk_prefix = scan_range.pk_prefix;
        assert!(pk_prefix.len() == table.pk_indices().len());

        let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

        // Point Get.
        let row = table.get_row(&pk_prefix, epoch.into()).await?;

        if let Some(timer) = timer {
            timer.observe_duration()
        }

        Ok(row)
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn execute_range(
        table: Arc<StorageTable<S>>,
        scan_range: ScanRange,
        ordered: bool,
        epoch: BatchQueryEpoch,
        chunk_size: usize,
        limit: Option<u64>,
        histogram: Option<impl Deref<Target = Histogram>>,
    ) {
        let pk_prefix = scan_range.pk_prefix.clone();
        let range_bounds = scan_range.convert_to_range_bounds(&table);
        // Range Scan.
        assert!(pk_prefix.len() < table.pk_indices().len());
        let iter = table
            .batch_chunk_iter_with_pk_bounds(
                epoch.into(),
                &pk_prefix,
                range_bounds,
                ordered,
                chunk_size,
                PrefetchOptions::new(limit.is_none(), true),
            )
            .await?;

        pin_mut!(iter);
        loop {
            let timer = histogram.as_ref().map(|histogram| histogram.start_timer());

            let chunk = iter.next().await.transpose().map_err(BatchError::from)?;

            if let Some(timer) = timer {
                timer.observe_duration()
            }

            if let Some(chunk) = chunk {
                yield chunk
            } else {
                break;
            }
        }
    }
}

pub fn unix_timestamp_sec_to_epoch(ts: i64) -> risingwave_common::util::epoch::Epoch {
    let ts = ts.checked_add(1).unwrap();
    risingwave_common::util::epoch::Epoch::from_unix_millis_or_earliest(
        u64::try_from(ts).unwrap_or(0).checked_mul(1000).unwrap(),
    )
}
