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

pub mod aggregation;
mod delete;
mod expand;
mod fast_insert;
mod filter;
mod generic_exchange;
mod group_top_n;
mod hash_agg;
mod hop_window;
mod iceberg_scan;
mod insert;
mod join;
mod limit;
mod log_row_seq_scan;
mod managed;
mod max_one_row;
mod merge_sort;
mod merge_sort_exchange;
mod mysql_query;
mod order_by;
mod postgres_query;
mod project;
mod project_set;
mod row_seq_scan;
mod s3_file_scan;
mod sort_agg;
mod sort_over_window;
mod source;
mod sys_row_seq_scan;
mod table_function;
pub mod test_utils;
mod top_n;
mod union;
mod update;
mod utils;
mod values;

use anyhow::Context;
use async_recursion::async_recursion;
pub use delete::*;
pub use expand::*;
pub use fast_insert::*;
pub use filter::*;
use futures::stream::BoxStream;
pub use generic_exchange::*;
pub use group_top_n::*;
pub use hash_agg::*;
pub use hop_window::*;
pub use iceberg_scan::*;
pub use insert::*;
pub use join::*;
pub use limit::*;
pub use managed::*;
pub use max_one_row::*;
pub use merge_sort::*;
pub use merge_sort_exchange::*;
pub use mysql_query::*;
pub use order_by::*;
pub use postgres_query::*;
pub use project::*;
pub use project_set::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::PlanNode;
use risingwave_pb::common::BatchQueryEpoch;
pub use row_seq_scan::*;
pub use sort_agg::*;
pub use sort_over_window::SortOverWindowExecutor;
pub use source::*;
pub use table_function::*;
use thiserror_ext::AsReport;
pub use top_n::TopNExecutor;
pub use union::*;
pub use update::*;
pub use utils::*;
pub use values::*;

use self::log_row_seq_scan::LogStoreRowSeqScanExecutorBuilder;
use self::test_utils::{BlockExecutorBuilder, BusyLoopExecutorBuilder};
use crate::error::Result;
use crate::executor::s3_file_scan::FileScanExecutorBuilder;
use crate::executor::sys_row_seq_scan::SysRowSeqScanExecutorBuilder;
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedDataChunkStream = BoxStream<'static, Result<DataChunk>>;

pub struct ExecutorInfo {
    pub schema: Schema,
    pub id: String,
}

/// Refactoring of `Executor` using `Stream`.
pub trait Executor: Send + 'static {
    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Executes to return the data chunk stream.
    ///
    /// The implementation should guaranteed that each `DataChunk`'s cardinality is not zero.
    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

impl std::fmt::Debug for BoxedExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.identity())
    }
}

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor`
/// from proto and global environment.
#[async_trait::async_trait]
pub trait BoxedExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor>;
}

pub struct ExecutorBuilder<'a, C> {
    pub plan_node: &'a PlanNode,
    pub task_id: &'a TaskId,
    context: C,
    epoch: BatchQueryEpoch,
    shutdown_rx: ShutdownToken,
}

macro_rules! build_executor {
    ($source: expr, $inputs: expr, $($proto_type_name:path => $data_type:ty),* $(,)?) => {
        match $source.plan_node().get_node_body().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::new_boxed_executor($source, $inputs)
                },
            )*
        }
    }
}

impl<'a, C: Clone> ExecutorBuilder<'a, C> {
    pub fn new(
        plan_node: &'a PlanNode,
        task_id: &'a TaskId,
        context: C,
        epoch: BatchQueryEpoch,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        Self {
            plan_node,
            task_id,
            context,
            epoch,
            shutdown_rx,
        }
    }

    #[must_use]
    pub fn clone_for_plan(&self, plan_node: &'a PlanNode) -> Self {
        ExecutorBuilder::new(
            plan_node,
            self.task_id,
            self.context.clone(),
            self.epoch,
            self.shutdown_rx.clone(),
        )
    }

    pub fn plan_node(&self) -> &PlanNode {
        self.plan_node
    }

    pub fn context(&self) -> &C {
        &self.context
    }

    pub fn epoch(&self) -> BatchQueryEpoch {
        self.epoch
    }
}

impl<'a, C: BatchTaskContext> ExecutorBuilder<'a, C> {
    pub async fn build(&self) -> Result<BoxedExecutor> {
        self.try_build()
            .await
            .inspect_err(|e| {
                let plan_node = self.plan_node.get_node_body();
                error!(error = %e.as_report(), ?plan_node, "failed to build executor");
            })
            .context("failed to build executor")
            .map_err(Into::into)
    }

    #[async_recursion]
    async fn try_build(&self) -> Result<BoxedExecutor> {
        let mut inputs = Vec::with_capacity(self.plan_node.children.len());
        for input_node in &self.plan_node.children {
            let input = self.clone_for_plan(input_node).build().await?;
            inputs.push(input);
        }

        let real_executor = build_executor! { self, inputs,
            NodeBody::RowSeqScan => RowSeqScanExecutorBuilder,
            NodeBody::Insert => InsertExecutor,
            NodeBody::Delete => DeleteExecutor,
            NodeBody::Exchange => GenericExchangeExecutorBuilder,
            NodeBody::Update => UpdateExecutor,
            NodeBody::Filter => FilterExecutor,
            NodeBody::Project => ProjectExecutor,
            NodeBody::SortAgg => SortAggExecutor,
            NodeBody::Sort => SortExecutor,
            NodeBody::TopN => TopNExecutor,
            NodeBody::GroupTopN => GroupTopNExecutorBuilder,
            NodeBody::Limit => LimitExecutor,
            NodeBody::Values => ValuesExecutor,
            NodeBody::NestedLoopJoin => NestedLoopJoinExecutor,
            NodeBody::HashJoin => HashJoinExecutor<()>,
            // NodeBody::SortMergeJoin => SortMergeJoinExecutor,
            NodeBody::HashAgg => HashAggExecutorBuilder,
            NodeBody::MergeSortExchange => MergeSortExchangeExecutorBuilder,
            NodeBody::TableFunction => TableFunctionExecutorBuilder,
            NodeBody::HopWindow => HopWindowExecutor,
            NodeBody::SysRowSeqScan => SysRowSeqScanExecutorBuilder,
            NodeBody::Expand => ExpandExecutor,
            NodeBody::LocalLookupJoin => LocalLookupJoinExecutorBuilder,
            NodeBody::DistributedLookupJoin => DistributedLookupJoinExecutorBuilder,
            NodeBody::ProjectSet => ProjectSetExecutor,
            NodeBody::Union => UnionExecutor,
            NodeBody::Source => SourceExecutor,
            NodeBody::SortOverWindow => SortOverWindowExecutor,
            NodeBody::MaxOneRow => MaxOneRowExecutor,
            NodeBody::FileScan => FileScanExecutorBuilder,
            NodeBody::IcebergScan => IcebergScanExecutorBuilder,
            NodeBody::PostgresQuery => PostgresQueryExecutorBuilder,
            NodeBody::MysqlQuery => MySqlQueryExecutorBuilder,
            // Follow NodeBody only used for test
            NodeBody::BlockExecutor => BlockExecutorBuilder,
            NodeBody::BusyLoopExecutor => BusyLoopExecutorBuilder,
            NodeBody::LogRowSeqScan => LogStoreRowSeqScanExecutorBuilder,
        }
        .await?;

        Ok(Box::new(ManagedExecutor::new(
            real_executor,
            self.shutdown_rx.clone(),
        )) as BoxedExecutor)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::test_batch_query_epoch;
    use risingwave_pb::batch_plan::PlanNode;

    use crate::executor::ExecutorBuilder;
    use crate::task::{ComputeNodeContext, ShutdownToken, TaskId};

    #[tokio::test]
    async fn test_clone_for_plan() {
        let plan_node = PlanNode::default();
        let task_id = &TaskId {
            task_id: 1,
            stage_id: 1,
            query_id: "test_query_id".to_string(),
        };
        let builder = ExecutorBuilder::new(
            &plan_node,
            task_id,
            ComputeNodeContext::for_test(),
            test_batch_query_epoch(),
            ShutdownToken::empty(),
        );
        let child_plan = &PlanNode::default();
        let cloned_builder = builder.clone_for_plan(child_plan);
        assert_eq!(builder.task_id, cloned_builder.task_id);
    }
}
