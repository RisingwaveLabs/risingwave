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

mod fast_insert;
mod managed;
pub mod test_utils;

use std::future::Future;
use std::sync::Arc;

use anyhow::Context;
use async_recursion::async_recursion;
pub use fast_insert::*;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
pub use managed::*;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBodyDiscriminants;
use risingwave_pb::batch_plan::PlanNode;
use risingwave_pb::common::BatchQueryEpoch;
use thiserror_ext::AsReport;

use crate::error::Result;
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
pub trait BoxedExecutorBuilder {
    fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> impl Future<Output = Result<BoxedExecutor>> + Send;
}

pub struct ExecutorBuilder<'a> {
    pub plan_node: &'a PlanNode,
    pub task_id: &'a TaskId,
    context: Arc<dyn BatchTaskContext>,
    epoch: BatchQueryEpoch,
    shutdown_rx: ShutdownToken,
}

impl<'a> ExecutorBuilder<'a> {
    pub fn new(
        plan_node: &'a PlanNode,
        task_id: &'a TaskId,
        context: Arc<dyn BatchTaskContext>,
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

    pub fn context(&self) -> &Arc<dyn BatchTaskContext> {
        &self.context
    }

    pub fn epoch(&self) -> BatchQueryEpoch {
        self.epoch
    }

    pub fn shutdown_rx(&self) -> &ShutdownToken {
        &self.shutdown_rx
    }
}

/// Descriptor for executor builder.
///
/// We will call `builder` to build the executor if the `node_body` matches.
pub struct ExecutorBuilderDescriptor {
    pub node_body: NodeBodyDiscriminants,

    /// Typically from [`BoxedExecutorBuilder::new_boxed_executor`].
    pub builder: for<'a> fn(
        source: &'a ExecutorBuilder<'a>,
        inputs: Vec<BoxedExecutor>,
    ) -> BoxFuture<'a, Result<BoxedExecutor>>,
}

/// All registered executor builders.
#[linkme::distributed_slice]
pub static BUILDER_DESCS: [ExecutorBuilderDescriptor];

/// Register an executor builder so that it can be used to build the executor from protobuf.
#[macro_export]
macro_rules! register_executor {
    ($node_body:ident, $builder:ty) => {
        const _: () = {
            use futures::FutureExt;
            use risingwave_batch::executor::{ExecutorBuilderDescriptor, BUILDER_DESCS};
            use risingwave_pb::batch_plan::plan_node::NodeBodyDiscriminants;

            #[linkme::distributed_slice(BUILDER_DESCS)]
            static BUILDER: ExecutorBuilderDescriptor = ExecutorBuilderDescriptor {
                node_body: NodeBodyDiscriminants::$node_body,
                builder: |a, b| <$builder>::new_boxed_executor(a, b).boxed(),
            };
        };
    };
}
pub use register_executor;

impl<'a> ExecutorBuilder<'a> {
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

        let node_body_discriminants: NodeBodyDiscriminants =
            self.plan_node.get_node_body().unwrap().into();

        let builder = BUILDER_DESCS
            .iter()
            .find(|x| x.node_body == node_body_discriminants)
            .with_context(|| {
                format!(
                    "no executor builder found for {:?}",
                    node_body_discriminants
                )
            })?
            .builder;

        let real_executor = builder(self, inputs).await?;

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
            query_id: "test_query_id".to_owned(),
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
