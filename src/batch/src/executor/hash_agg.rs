// Copyright 2023 RisingWave Labs
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

use std::collections::HashMap;
use std::marker::PhantomData;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, StreamChunk, Vis};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::hash::{HashKey, HashKeyDispatcher, PrecomputedBuildHasher};
use risingwave_common::memory::MemoryContext;
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::agg::{AggCall, BoxedAggState};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::HashAggNode;

use crate::executor::aggregation::build as build_agg;
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::{BatchTaskContext, ShutdownToken, TaskId};

type AggHashMap<K, A> = hashbrown::HashMap<K, Vec<BoxedAggState>, PrecomputedBuildHasher, A>;

/// A dispatcher to help create specialized hash agg executor.
impl HashKeyDispatcher for HashAggExecutorBuilder {
    type Output = BoxedExecutor;

    fn dispatch_impl<K: HashKey>(self) -> Self::Output {
        Box::new(HashAggExecutor::<K>::new(
            self.agg_init_states,
            self.group_key_columns,
            self.group_key_types,
            self.schema,
            self.child,
            self.identity,
            self.chunk_size,
            self.mem_context,
            self.shutdown_rx,
        ))
    }

    fn data_types(&self) -> &[DataType] {
        &self.group_key_types
    }
}

pub struct HashAggExecutorBuilder {
    agg_init_states: Vec<BoxedAggState>,
    group_key_columns: Vec<usize>,
    group_key_types: Vec<DataType>,
    child: BoxedExecutor,
    schema: Schema,
    task_id: TaskId,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    shutdown_rx: ShutdownToken,
}

impl HashAggExecutorBuilder {
    fn deserialize(
        hash_agg_node: &HashAggNode,
        child: BoxedExecutor,
        task_id: TaskId,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        shutdown_rx: ShutdownToken,
    ) -> Result<BoxedExecutor> {
        let agg_init_states: Vec<_> = hash_agg_node
            .get_agg_calls()
            .iter()
            .map(|agg| AggCall::from_protobuf(agg).and_then(|agg| build_agg(&agg)))
            .try_collect()?;

        let group_key_columns = hash_agg_node
            .get_group_key()
            .iter()
            .map(|x| *x as usize)
            .collect_vec();

        let child_schema = child.schema();

        let group_key_types = group_key_columns
            .iter()
            .map(|i| child_schema.fields[*i].data_type.clone())
            .collect_vec();

        let fields = group_key_types
            .iter()
            .cloned()
            .chain(agg_init_states.iter().map(|e| e.return_type()))
            .map(Field::unnamed)
            .collect::<Vec<Field>>();

        let builder = HashAggExecutorBuilder {
            agg_init_states,
            group_key_columns,
            group_key_types,
            child,
            schema: Schema { fields },
            task_id,
            identity,
            chunk_size,
            mem_context,
            shutdown_rx,
        };

        Ok(builder.dispatch())
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for HashAggExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let hash_agg_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::HashAgg
        )?;

        let identity = source.plan_node().get_identity();

        Self::deserialize(
            hash_agg_node,
            child,
            source.task_id.clone(),
            identity.clone(),
            source.context.get_config().developer.chunk_size,
            source.context.create_executor_mem_context(identity),
            source.shutdown_rx.clone(),
        )
    }
}

/// `HashAggExecutor` implements the hash aggregate algorithm.
pub struct HashAggExecutor<K> {
    /// Factories to construct aggregator for each groups
    agg_init_states: Vec<BoxedAggState>,
    /// Column indexes that specify a group
    group_key_columns: Vec<usize>,
    /// Data types of group key columns
    group_key_types: Vec<DataType>,
    /// Output schema
    schema: Schema,
    child: BoxedExecutor,
    identity: String,
    chunk_size: usize,
    mem_context: MemoryContext,
    shutdown_rx: ShutdownToken,
    _phantom: PhantomData<K>,
}

impl<K> HashAggExecutor<K> {
    pub fn new(
        agg_init_states: Vec<BoxedAggState>,
        group_key_columns: Vec<usize>,
        group_key_types: Vec<DataType>,
        schema: Schema,
        child: BoxedExecutor,
        identity: String,
        chunk_size: usize,
        mem_context: MemoryContext,
        shutdown_rx: ShutdownToken,
    ) -> Self {
        HashAggExecutor {
            agg_init_states,
            group_key_columns,
            group_key_types,
            schema,
            child,
            identity,
            chunk_size,
            mem_context,
            shutdown_rx,
            _phantom: PhantomData,
        }
    }
}

impl<K: HashKey + Send + Sync> Executor for HashAggExecutor<K> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl<K: HashKey + Send + Sync> HashAggExecutor<K> {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        // hash map for each agg groups
        let mut groups = AggHashMap::<K, _>::with_hasher_in(
            PrecomputedBuildHasher,
            self.mem_context.global_allocator(),
        );

        // consume all chunks to compute the agg result
        #[for_await]
        for chunk in self.child.execute() {
            let chunk = StreamChunk::from(chunk?.compact());
            let keys = K::build(self.group_key_columns.as_slice(), &chunk)?;
            let group_visibilities = Self::get_group_visibilities(keys, chunk.vis());

            let mut memory_usage_diff = 0;
            for (key, visibility) in group_visibilities {
                let mut new_group = false;
                let states = groups.entry(key).or_insert_with(|| {
                    new_group = true;
                    self.agg_init_states.clone()
                });
                let masked_chunk = chunk.with_visibility(visibility.into());

                for state in states {
                    if !new_group {
                        memory_usage_diff -= state.estimated_size() as i64;
                    }
                    state.update(&masked_chunk).await?;
                    memory_usage_diff += state.estimated_size() as i64;
                }
            }
            // update memory usage
            self.mem_context.add(memory_usage_diff);
        }

        // Don't use `into_iter` here, it may cause memory leak.
        let mut result = groups.iter_mut();
        let cardinality = self.chunk_size;
        loop {
            let mut group_builders: Vec<_> = self
                .group_key_types
                .iter()
                .map(|datatype| datatype.create_array_builder(cardinality))
                .collect();

            let mut agg_builders: Vec<_> = self
                .agg_init_states
                .iter()
                .map(|agg| agg.return_type().create_array_builder(cardinality))
                .collect();

            let mut has_next = false;
            let mut array_len = 0;
            for (key, states) in result.by_ref().take(cardinality) {
                self.shutdown_rx.check()?;
                has_next = true;
                array_len += 1;
                key.deserialize_to_builders(&mut group_builders[..], &self.group_key_types)?;
                for (aggregator, builder) in states.iter_mut().zip_eq_fast(&mut agg_builders) {
                    builder.append(aggregator.output()?);
                }
            }
            if !has_next {
                break; // exit loop
            }

            let columns = group_builders
                .into_iter()
                .chain(agg_builders)
                .map(|b| b.finish().into())
                .collect::<Vec<_>>();

            let output = DataChunk::new(columns, array_len);
            yield output;
        }
    }

    /// Get visibilities that mask rows in the chunk for each group. The returned visibility
    /// is a `Bitmap` rather than `Option<Bitmap>` because it's likely to have multiple groups
    /// in one chunk.
    ///
    /// * `keys`: Hash Keys of rows.
    /// * `base_visibility`: Visibility of rows.
    fn get_group_visibilities(keys: Vec<K>, base_visibility: &Vis) -> Vec<(K, Bitmap)> {
        let n_rows = keys.len();
        let mut vis_builders =
            HashMap::with_capacity_and_hasher(keys.len(), PrecomputedBuildHasher);
        for (row_idx, key) in keys
            .into_iter()
            .enumerate()
            .filter(|(row_idx, _)| base_visibility.is_set(*row_idx))
        {
            vis_builders
                .entry(key)
                .or_insert_with(|| BitmapBuilder::zeroed(n_rows))
                .set(row_idx, true);
        }
        vis_builders
            .into_iter()
            .map(|(key, vis_builder)| (key, vis_builder.finish()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::alloc::{AllocError, Allocator, Global, Layout};
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use futures_async_stream::for_await;
    use prometheus::IntGauge;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::test_prelude::DataChunkTestExt;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::expr::agg_call::Type;
    use risingwave_pb::expr::{AggCall, InputRef};

    use super::*;
    use crate::executor::test_utils::{diff_executor_output, MockExecutor};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn execute_int32_grouped() {
        let parent_mem = MemoryContext::root(IntGauge::new("root_memory_usage", " ").unwrap());
        {
            let src_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i i
                 0 1 1
                 1 1 1
                 0 0 1
                 1 1 2
                 1 0 1
                 0 0 2
                 1 1 3
                 0 1 2",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));

            let agg_call = AggCall {
                r#type: Type::Sum as i32,
                args: vec![InputRef {
                    index: 2,
                    r#type: Some(PbDataType {
                        type_name: TypeName::Int32 as i32,
                        ..Default::default()
                    }),
                }],
                return_type: Some(PbDataType {
                    type_name: TypeName::Int64 as i32,
                    ..Default::default()
                }),
                distinct: false,
                order_by: vec![],
                filter: None,
                direct_args: vec![],
            };

            let agg_prost = HashAggNode {
                group_key: vec![0, 1],
                agg_calls: vec![agg_call],
            };

            let mem_context = MemoryContext::new(
                Some(parent_mem.clone()),
                IntGauge::new("memory_usage", " ").unwrap(),
            );
            let actual_exec = HashAggExecutorBuilder::deserialize(
                &agg_prost,
                src_exec,
                TaskId::default(),
                "HashAggExecutor".to_string(),
                CHUNK_SIZE,
                mem_context.clone(),
                ShutdownToken::empty(),
            )
            .unwrap();

            // TODO: currently the order is fixed unless the hasher is changed
            let expect_exec = Box::new(MockExecutor::with_chunk(
                DataChunk::from_pretty(
                    "i i I
                 1 0 1
                 0 0 3
                 0 1 3
                 1 1 6",
                ),
                Schema::new(vec![
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int32),
                    Field::unnamed(DataType::Int64),
                ]),
            ));
            diff_executor_output(actual_exec, expect_exec).await;

            // check estimated memory usage = 4 groups x state size
            assert_eq!(mem_context.get_bytes_used() as usize, 4 * 72);
        }

        // Ensure that agg memory counter has been dropped.
        assert_eq!(0, parent_mem.get_bytes_used());
    }

    #[tokio::test]
    async fn execute_count_star() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i
                 0
                 1
                 0
                 1
                 1
                 0
                 1
                 0",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32)]),
        );

        let agg_call = AggCall {
            r#type: Type::Count as i32,
            args: vec![],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
        };

        let agg_prost = HashAggNode {
            group_key: vec![],
            agg_calls: vec![agg_call],
        };

        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
            ShutdownToken::empty(),
        )
        .unwrap();

        let expect_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "I
                 8",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int64)]),
        );
        diff_executor_output(actual_exec, Box::new(expect_exec)).await;
    }

    /// A test to verify that `HashMap` may leak memory counter when using `into_iter`.
    #[test]
    fn test_hashmap_into_iter_bug() {
        let dropped: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        {
            struct MyAllocInner {
                drop_flag: Arc<AtomicBool>,
            }

            #[derive(Clone)]
            struct MyAlloc {
                inner: Arc<MyAllocInner>,
            }

            impl Drop for MyAllocInner {
                fn drop(&mut self) {
                    println!("MyAlloc freed.");
                    self.drop_flag.store(true, Ordering::SeqCst);
                }
            }

            unsafe impl Allocator for MyAlloc {
                fn allocate(
                    &self,
                    layout: Layout,
                ) -> std::result::Result<NonNull<[u8]>, AllocError> {
                    let g = Global;
                    g.allocate(layout)
                }

                unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
                    let g = Global;
                    g.deallocate(ptr, layout)
                }
            }

            let mut map = hashbrown::HashMap::with_capacity_in(
                10,
                MyAlloc {
                    inner: Arc::new(MyAllocInner {
                        drop_flag: dropped.clone(),
                    }),
                },
            );
            for i in 0..10 {
                map.entry(i).or_insert_with(|| "i".to_string());
            }

            for (k, v) in map {
                println!("{}, {}", k, v);
            }
        }

        assert!(!dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_shutdown() {
        let src_exec = MockExecutor::with_chunk(
            DataChunk::from_pretty(
                "i i i
                 0 1 1",
            ),
            Schema::new(vec![Field::unnamed(DataType::Int32); 3]),
        );

        let agg_call = AggCall {
            r#type: Type::Sum as i32,
            args: vec![InputRef {
                index: 2,
                r#type: Some(PbDataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
            }],
            return_type: Some(PbDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            distinct: false,
            order_by: vec![],
            filter: None,
            direct_args: vec![],
        };

        let agg_prost = HashAggNode {
            group_key: vec![0, 1],
            agg_calls: vec![agg_call],
        };

        let (shutdown_tx, shutdown_rx) = ShutdownToken::new();
        let actual_exec = HashAggExecutorBuilder::deserialize(
            &agg_prost,
            Box::new(src_exec),
            TaskId::default(),
            "HashAggExecutor".to_string(),
            CHUNK_SIZE,
            MemoryContext::none(),
            shutdown_rx,
        )
        .unwrap();

        shutdown_tx.cancel();

        #[for_await]
        for data in actual_exec.execute() {
            assert!(data.is_err());
            break;
        }
    }
}
