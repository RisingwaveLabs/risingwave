use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_storage::table::{ScannableTable, TableIterRef};

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

/// Executor that scans data from row table
pub struct RowSeqScanExecutor {
    table: Arc<dyn ScannableTable>,
    /// An iterator to scan StateStore.
    iter: Option<TableIterRef>,
    primary: bool,

    column_ids: Vec<i32>,
    column_indices: Vec<usize>,
    chunk_size: usize,
    schema: Schema,
    identity: String,
}

impl RowSeqScanExecutor {
    // TODO: decide the chunk size for row seq scan
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;

    pub fn new(
        table: Arc<dyn ScannableTable>,
        column_ids: Vec<i32>,
        chunk_size: usize,
        primary: bool,
        identity: String,
    ) -> Self {
        // Currently row_id for table_v2 is totally a mess, we override this function to match the
        // behavior of column ids of mviews.
        // FIXME: remove this hack
        let column_indices = column_ids.iter().map(|&id| id as usize).collect_vec();
        // let column_indices = table.column_indices(&column_ids);

        let table_schema = table.schema();
        let schema = Schema::new(
            column_indices
                .iter()
                .map(|idx| table_schema.fields().get(*idx).cloned().unwrap())
                .collect_vec(),
        );

        Self {
            table,
            iter: None,
            primary,
            column_ids,
            column_indices,
            chunk_size,
            schema,
            identity,
        }
    }
}

impl BoxedExecutorBuilder for RowSeqScanExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::RowSeqScan
        )?;

        let table_id = TableId::from(&seq_scan_node.table_ref_id);

        let table = source
            .global_task_env()
            .table_manager()
            .get_table(&table_id)?;

        let column_ids = seq_scan_node.get_column_ids().clone();

        Ok(Box::new(Self::new(
            table,
            column_ids,
            Self::DEFAULT_CHUNK_SIZE,
            source.task_id.task_id == 0,
            source.plan_node().get_identity().clone(),
        )))
    }
}

#[async_trait::async_trait]
impl Executor for RowSeqScanExecutor {
    async fn open(&mut self) -> Result<()> {
        if !self.primary {
            info!("non-primary row seq scan, ignored");
            return Ok(());
        }

        self.iter = Some(self.table.iter(u64::MAX).await?);
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if !self.primary {
            return Ok(None);
        }

        let iter = self.iter.as_mut().expect("executor not open");

        self.table
            .collect_from_iter(iter, &self.column_indices, Some(self.chunk_size))
            .await
    }

    async fn close(&mut self) -> Result<()> {
        info!("Table scan closed.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}
