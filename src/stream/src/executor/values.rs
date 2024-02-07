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

use std::vec;

use await_tree::InstrumentAwait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::ensure;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::NonStrictExpression;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{
    ActorContextRef, Barrier, BoxedMessageStream, Executor, ExecutorInfo, Message, PkIndicesRef,
    StreamExecutorError,
};
use crate::task::CreateMviewProgress;

const DEFAULT_CHUNK_SIZE: usize = 1024;

/// Execute `values` in stream. As is a leaf, current workaround holds a `barrier_executor`.
/// May refractor with `BarrierRecvExecutor` in the near future.
pub struct ValuesExecutor {
    ctx: ActorContextRef,
    info: ExecutorInfo,

    // Receiver of barrier channel.
    barrier_receiver: UnboundedReceiver<Barrier>,
    progress: CreateMviewProgress,

    rows: vec::IntoIter<Vec<NonStrictExpression>>,
}

impl ValuesExecutor {
    /// Currently hard-code the `pk_indices` as the last column.
    pub fn new(
        ctx: ActorContextRef,
        info: ExecutorInfo,
        progress: CreateMviewProgress,
        rows: Vec<Vec<NonStrictExpression>>,
        barrier_receiver: UnboundedReceiver<Barrier>,
    ) -> Self {
        Self {
            ctx,
            info,
            progress,
            barrier_receiver,
            rows: rows.into_iter(),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn into_stream(self) {
        let Self {
            info,
            mut progress,
            mut barrier_receiver,
            mut rows,
            ..
        } = self;
        let barrier = barrier_receiver
            .recv()
            .instrument_await("values_executor_recv_first_barrier")
            .await
            .unwrap();

        let emit = barrier.is_newly_added(self.ctx.id);
        let paused_on_startup = barrier.is_pause_on_startup();

        yield Message::Barrier(barrier);

        // If it's failover, do not evaluate rows (assume they have been yielded)
        if emit {
            if paused_on_startup {
                // Wait for the data stream to be resumed before yielding the chunks.
                while let Some(barrier) = barrier_receiver.recv().await {
                    let is_resume = barrier.is_resume();
                    yield Message::Barrier(barrier);
                    if is_resume {
                        break;
                    }
                }
            }

            let cardinality = info.schema.len();
            ensure!(cardinality > 0);
            while !rows.is_empty() {
                // We need a one row chunk rather than an empty chunk because constant
                // expression's eval result is same size as input chunk
                // cardinality.
                let one_row_chunk = DataChunk::new_dummy(1);

                let chunk_size = DEFAULT_CHUNK_SIZE.min(rows.len());
                let mut array_builders = info.schema.create_array_builders(chunk_size);
                for row in rows.by_ref().take(chunk_size) {
                    for (expr, builder) in row.into_iter().zip_eq_fast(&mut array_builders) {
                        let out = expr.eval_infallible(&one_row_chunk).await;
                        builder.append_array(&out);
                    }
                }

                let columns: Vec<_> = array_builders
                    .into_iter()
                    .map(|b| b.finish().into())
                    .collect();

                let chunk = DataChunk::new(columns, chunk_size);
                let ops = vec![Op::Insert; chunk_size];

                let stream_chunk = StreamChunk::from_parts(ops, chunk);
                yield Message::Chunk(stream_chunk);
            }
        }

        while let Some(barrier) = barrier_receiver.recv().await {
            if emit {
                progress.finish(barrier.epoch.curr, 0);
            }
            yield Message::Barrier(barrier);
        }
    }
}

impl Executor for ValuesExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.into_stream().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;
    use risingwave_common::array::{
        Array, ArrayImpl, I16Array, I32Array, I64Array, StructArray, StructValue,
    };
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl, StructType};
    use risingwave_expr::expr::{BoxedExpression, LiteralExpression, NonStrictExpression};
    use risingwave_hummock_sdk::EpochWithGap;
    use tokio::sync::mpsc::unbounded_channel;

    use super::ValuesExecutor;
    use crate::executor::test_utils::StreamExecutorTestExt;
    use crate::executor::{ActorContext, AddMutation, Barrier, Executor, ExecutorInfo, Mutation};
    use crate::task::{CreateMviewProgress, LocalBarrierManager};

    #[tokio::test]
    async fn test_values() {
        let barrier_manager = LocalBarrierManager::for_test();
        let progress = CreateMviewProgress::for_test(barrier_manager);
        let actor_id = progress.actor_id();
        let (tx, barrier_receiver) = unbounded_channel();
        let value = StructValue::new(vec![Some(1.into()), Some(2.into()), Some(3.into())]);
        let exprs = vec![
            Box::new(LiteralExpression::new(
                DataType::Int16,
                Some(ScalarImpl::Int16(1)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(2)),
            )),
            Box::new(LiteralExpression::new(
                DataType::Int64,
                Some(ScalarImpl::Int64(3)),
            )),
            Box::new(LiteralExpression::new(
                DataType::new_struct(
                    vec![DataType::Int32, DataType::Int32, DataType::Int32],
                    vec![],
                ),
                Some(ScalarImpl::Struct(value)),
            )),
            Box::new(LiteralExpression::new(
                DataType::Int64,
                Some(ScalarImpl::Int64(0)),
            )),
        ];
        let schema = exprs
            .iter() // for each column
            .map(|col| Field::unnamed(col.return_type()))
            .collect::<Schema>();
        let pk_indices = vec![schema.len() - 1];
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: "ValuesExecutor".to_string(),
        };
        let values_executor_struct = ValuesExecutor::new(
            ActorContext::create(actor_id),
            info,
            progress,
            vec![exprs
                .into_iter()
                .map(NonStrictExpression::for_test)
                .collect()],
            barrier_receiver,
        );
        let mut values_executor = Box::new(values_executor_struct).execute();

        // Init barrier
        let first_message =
            Barrier::new_test_barrier(EpochWithGap::new_for_test(1).as_u64_for_test())
                .with_mutation(Mutation::Add(AddMutation {
                    adds: Default::default(),
                    added_actors: maplit::hashset! {actor_id},
                    splits: Default::default(),
                    pause: false,
                }));
        tx.send(first_message).unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));

        // Consume the barrier
        let values_msg = values_executor.next().await.unwrap().unwrap();

        let result = values_msg
            .into_chunk()
            .unwrap()
            .compact()
            .data_chunk()
            .to_owned();

        let array: ArrayImpl = StructArray::new(
            StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]),
            vec![
                I32Array::from_iter([1]).into_ref(),
                I32Array::from_iter([2]).into_ref(),
                I32Array::from_iter([3]).into_ref(),
            ],
            [true].into_iter().collect(),
        )
        .into();

        assert_eq!(*result.column_at(0), I16Array::from_iter([1]).into_ref());
        assert_eq!(*result.column_at(1), I32Array::from_iter([2]).into_ref());
        assert_eq!(*result.column_at(2), I64Array::from_iter([3]).into_ref());
        assert_eq!(*result.column_at(3), array.into());
        assert_eq!(*result.column_at(4), I64Array::from_iter([0]).into_ref());

        // ValueExecutor should simply forward following barriers
        tx.send(Barrier::new_test_barrier(
            EpochWithGap::new_for_test(2).as_u64_for_test(),
        ))
        .unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));

        tx.send(Barrier::new_test_barrier(
            EpochWithGap::new_for_test(3).as_u64_for_test(),
        ))
        .unwrap();

        assert!(matches!(
            values_executor.next_unwrap_ready_barrier().unwrap(),
            Barrier { .. }
        ));
    }
}
