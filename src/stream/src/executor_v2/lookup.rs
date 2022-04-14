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

use async_trait::async_trait;
use futures::StreamExt;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::try_match_expand;
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use crate::executor::{Executor as ExecutorV1, ExecutorBuilder};
use crate::executor_v2::{Barrier, BoxedMessageStream, Executor, PkIndices, PkIndicesRef};
use crate::task::{ExecutorParams, LocalStreamManagerCore};

mod sides;
use self::sides::*;
mod impl_;

pub use impl_::LookupExecutorParams;

use super::ExecutorV1AsV2;

#[cfg(test)]
mod tests;

/// `LookupExecutor` takes one input stream and one arrangement. It joins the input stream with the
/// arrangement. Currently, it only supports inner join. See `LookupExecutorParams` for more
/// information.
///
/// The output schema is `| stream columns | arrangement columns |`.
pub struct LookupExecutor<S: StateStore> {
    /// the data types of the formed new columns
    output_data_types: Vec<DataType>,

    /// The schema of the lookup executor
    schema: Schema,

    /// The primary key indices of the schema
    pk_indices: PkIndices,

    /// The join side of the arrangement
    arrangement: ArrangeJoinSide<S>,

    /// The join side of the stream
    stream: StreamJoinSide,

    /// The executor for arrangement.
    arrangement_executor: Option<Box<dyn Executor>>,

    /// The executor for stream.
    stream_executor: Option<Box<dyn Executor>>,

    /// The last received barrier.
    last_barrier: Option<Barrier>,
}

#[async_trait]
impl<S: StateStore> Executor for LookupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "<unknown>"
    }
}

pub struct LookupExecutorBuilder {}

impl ExecutorBuilder for LookupExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut LocalStreamManagerCore,
    ) -> Result<Box<dyn ExecutorV1>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::LookupNode)?;

        let stream = params.input.remove(1);
        let stream = Box::new(ExecutorV1AsV2(stream));
        let arrangement = params.input.remove(0);
        let arrangement = Box::new(ExecutorV1AsV2(arrangement));

        Ok(Box::new(
            Box::new(LookupExecutor::new(LookupExecutorParams {
                arrangement,
                stream,
                arrangement_keyspace: Keyspace::shared_executor_root(store, u64::MAX),
                arrangement_col_descs: vec![], // TODO: fill this field
                arrangement_order_rules: vec![], // TODO: fill this field
                pk_indices: params.pk_indices,
                use_current_epoch: node.use_current_epoch,
                stream_join_key_indices: node.stream_key.iter().map(|x| *x as usize).collect(),
                arrange_join_key_indices: node.arrange_key.iter().map(|x| *x as usize).collect(),
            }))
            .v1(),
        ))
    }
}
