use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

pub use actor::Actor;
pub use aggregation::*;
use async_trait::async_trait;
pub use batch_query::*;
pub use chain::*;
pub use debug::*;
pub use dispatch::*;
pub use filter::*;
pub use global_simple_agg::*;
pub use hash_agg::*;
pub use hash_join::*;
pub use local_simple_agg::*;
pub use merge::*;
pub use monitor::*;
pub use mview::*;
pub use project::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, DataChunk, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::common::ActorInfo;
use risingwave_pb::data::barrier::Mutation as ProstMutation;
use risingwave_pb::data::stream_message::StreamMessage;
use risingwave_pb::data::{
    Actors as MutationActors, AddMutation, Barrier as ProstBarrier, NothingMutation, StopMutation,
    StreamMessage as ProstStreamMessage, UpdateMutation,
};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::StateStore;
use smallvec::SmallVec;
pub use source::*;
pub use top_n::*;
pub use top_n_appendonly::*;
use tracing::trace_span;

use crate::task::{ExecutorParams, StreamManagerCore, ENABLE_BARRIER_AGGREGATION};

mod actor;
mod aggregation;
mod barrier_align;
mod batch_query;
mod chain;
mod debug;
mod dispatch;
mod filter;
mod global_simple_agg;
mod hash_agg;
mod hash_join;
mod local_simple_agg;
mod managed_state;
mod merge;
mod monitor;
mod mview;
mod project;
mod source;
mod top_n;
mod top_n_appendonly;

#[cfg(test)]
mod integration_tests;

#[cfg(test)]
mod test_utils;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    Stop(HashSet<u32>),
    UpdateOutputs(HashMap<u32, Vec<ActorInfo>>),
    AddOutput(HashMap<u32, Vec<ActorInfo>>),
}

#[derive(Debug, Clone)]
pub struct Barrier {
    pub epoch: u64,
    pub mutation: Option<Arc<Mutation>>,
    pub span: tracing::Span,
}

impl Default for Barrier {
    fn default() -> Self {
        Self {
            span: tracing::Span::none(),
            epoch: 0,
            mutation: None,
        }
    }
}

impl Barrier {
    /// Create a plain barrier.
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_mutation(self, mutation: Mutation) -> Self {
        Self {
            mutation: Some(Arc::new(mutation)),
            ..self
        }
    }

    // TODO: The barrier should always contain trace info after we migrated barrier generation to
    // meta service.
    #[must_use]
    pub fn with_span(self, span: tracing::span::Span) -> Self {
        Self { span, ..self }
    }

    pub fn is_stop_mutation(&self) -> bool {
        self.mutation.as_ref().map(|m| m.is_stop()).unwrap_or(false)
    }
}

impl PartialEq for Barrier {
    fn eq(&self, other: &Self) -> bool {
        self.epoch == other.epoch && self.mutation == other.mutation
    }
}

impl Mutation {
    /// Return true if the mutation is stop.
    pub fn is_stop(&self) -> bool {
        matches!(self, Mutation::Stop(_))
    }
}

impl Barrier {
    fn to_protobuf(&self) -> ProstBarrier {
        let Barrier {
            epoch, mutation, ..
        }: Barrier = self.clone();
        ProstBarrier {
            epoch,
            mutation: match mutation.as_deref() {
                None => Some(ProstMutation::Nothing(NothingMutation {})),
                Some(Mutation::Stop(actors)) => Some(ProstMutation::Stop(StopMutation {
                    actors: actors.iter().cloned().collect::<Vec<_>>(),
                })),
                Some(Mutation::UpdateOutputs(updates)) => {
                    Some(ProstMutation::Update(UpdateMutation {
                        actors: updates
                            .iter()
                            .map(|(&f, actors)| {
                                (
                                    f,
                                    MutationActors {
                                        info: actors.clone(),
                                    },
                                )
                            })
                            .collect(),
                    }))
                }
                Some(Mutation::AddOutput(adds)) => Some(ProstMutation::Add(AddMutation {
                    actors: adds
                        .iter()
                        .map(|(&id, actors)| {
                            (
                                id,
                                MutationActors {
                                    info: actors.clone(),
                                },
                            )
                        })
                        .collect(),
                })),
            },
            span: vec![],
        }
    }

    pub fn from_protobuf(prost: &ProstBarrier) -> Result<Self> {
        let mutation = match prost.get_mutation()? {
            ProstMutation::Nothing(_) => (None),
            ProstMutation::Stop(stop) => {
                Some(Mutation::Stop(HashSet::from_iter(stop.get_actors().clone())).into())
            }
            ProstMutation::Update(update) => Some(
                Mutation::UpdateOutputs(
                    update
                        .actors
                        .iter()
                        .map(|(&f, actors)| (f, actors.get_info().clone()))
                        .collect::<HashMap<u32, Vec<ActorInfo>>>(),
                )
                .into(),
            ),
            ProstMutation::Add(adds) => Some(
                Mutation::AddOutput(
                    adds.actors
                        .iter()
                        .map(|(&id, actors)| (id, actors.get_info().clone()))
                        .collect::<HashMap<u32, Vec<ActorInfo>>>(),
                )
                .into(),
            ),
        };
        let epoch = prost.get_epoch();
        Ok(Barrier {
            span: if ENABLE_BARRIER_AGGREGATION {
                trace_span!("barrier", epoch = %epoch, mutation = ?mutation)
            } else {
                tracing::Span::none()
            },
            epoch,
            mutation,
        })
    }
}

#[derive(Debug)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(Barrier),
}

impl Message {
    /// Return true if the message is a stop barrier, meaning the stream
    /// will not continue, false otherwise.
    pub fn is_terminate(&self) -> bool {
        matches!(
          self,
          Message::Barrier(Barrier {
            mutation,
            ..
          }) if mutation.as_deref().unwrap().is_stop()
        )
    }

    pub fn to_protobuf(&self) -> Result<ProstStreamMessage> {
        let prost = match self {
            Self::Chunk(stream_chunk) => {
                let prost_stream_chunk = stream_chunk.to_protobuf()?;
                StreamMessage::StreamChunk(prost_stream_chunk)
            }
            Self::Barrier(barrier) => StreamMessage::Barrier(barrier.clone().to_protobuf()),
        };
        let prost_stream_msg = ProstStreamMessage {
            stream_message: Some(prost),
        };
        Ok(prost_stream_msg)
    }

    pub fn from_protobuf(prost: &ProstStreamMessage) -> Result<Self> {
        let res = match prost.get_stream_message()? {
            StreamMessage::StreamChunk(ref stream_chunk) => {
                Message::Chunk(StreamChunk::from_protobuf(stream_chunk)?)
            }
            StreamMessage::Barrier(ref barrier) => {
                Message::Barrier(Barrier::from_protobuf(barrier)?)
            }
        };
        Ok(res)
    }
}

/// `Executor` supports handling of control messages.
#[async_trait]
pub trait Executor: Send + Debug + 'static {
    async fn next(&mut self) -> Result<Message>;

    /// Return the schema of the OUTPUT of the executor.
    fn schema(&self) -> &Schema;

    /// Return the primary key indices of the OUTPUT of the executor.
    /// Schema is used by both OLAP and streaming, therefore
    /// pk indices are maintained independently.
    fn pk_indices(&self) -> PkIndicesRef;

    fn pk_data_types(&self) -> PkDataTypes {
        let schema = self.schema();
        self.pk_indices()
            .iter()
            .map(|idx| schema.fields[*idx].data_type.clone())
            .collect()
    }

    /// Identity string of the executor.
    fn identity(&self) -> &str;

    /// Logical Operator Information of the executor
    fn logical_operator_info(&self) -> &str;

    /// Clears the in-memory cache of the executor. It's no-op by default.
    fn clear_cache(&mut self) -> Result<()> {
        Ok(())
    }

    fn init(&mut self, _epoch: u64) -> Result<()> {
        unreachable!()
    }
}

pub type PkIndices = Vec<usize>;
pub type PkIndicesRef<'a> = &'a [usize];
pub type PkDataTypes = SmallVec<[DataType; 1]>;

/// Get inputs by given `pk_indices` from `columns`.
pub fn pk_input_arrays<'a>(pk_indices: PkIndicesRef, columns: &'a [Column]) -> Vec<&'a ArrayImpl> {
    pk_indices
        .iter()
        .map(|pk_idx| columns[*pk_idx].array_ref())
        .collect()
}

pub trait ExecutorBuilder {
    fn create_executor(
        executor_params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        steam: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>>;
}

macro_rules! build_executor {
    ($source: expr,$a: expr,$b: expr,$c: expr, $($proto_type_name:path => $data_type:ty),*) => {
        match $a.get_node().unwrap() {
            $(
                $proto_type_name(..) => {
                    <$data_type>::create_executor($source,$a,$b,$c)
                },
            )*
        }
    }
}

pub fn create_executor(
    executor_params: ExecutorParams,
    steam: &mut StreamManagerCore,
    node: &stream_plan::StreamNode,
    store: impl StateStore,
) -> Result<Box<dyn Executor>> {
    let real_executor = build_executor! { executor_params,node,store,steam,
      Node::SourceNode => SourceExecutorCreater,
      Node::ProjectNode => ProjectExecutorCreater,
      Node::TopNNode => TopNExecutorCreater,
      Node::AppendOnlyTopNNode => AppendOnlyTopNExecutorCreater,
      Node::LocalSimpleAggNode => LocalSimpleAggExecutorCreater,
      Node::GlobalSimpleAggNode => SimpleAggExecutorCreater,
      Node::HashAggNode => HashAggExecutorCreater,
      Node::HashJoinNode => HashJoinExecutorCreater,
      Node::ChainNode => ChainExecutorCreater,
      Node::BatchPlanNode => BatchQueryExecutorCreater,
      Node::ExchangeNode => MaterializeExecutorCreater,
      Node::MergeNode => MergeExecutorCreater,
      Node::MviewNode => MaterializeExecutorCreater,
      Node::FilterNode => FilterExecutorCreater
    }?;
    Ok(real_executor)
}

/// `SimpleExecutor` accepts a single chunk as input.
pub trait SimpleExecutor: Executor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message>;
    fn input(&mut self) -> &mut dyn Executor;
}

/// Most executors don't care about the control messages, and therefore
/// this method provides a default implementation helper for them.
async fn simple_executor_next<E: SimpleExecutor>(executor: &mut E) -> Result<Message> {
    match executor.input().next().await {
        Ok(message) => match message {
            Message::Chunk(chunk) => executor.consume_chunk(chunk),
            Message::Barrier(_) => Ok(message),
        },
        Err(e) => Err(e),
    }
}

/// `StreamConsumer` is the last step in an actor
#[async_trait]
pub trait StreamConsumer: Send + Debug + 'static {
    /// Run next stream chunk, returns whether the chunk is a barrier.
    async fn next(&mut self) -> Result<Option<Barrier>>;
}
