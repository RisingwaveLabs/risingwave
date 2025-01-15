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

use std::vec;

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

/// [`ValuesExecutor`] implements Values executor.
pub struct ValuesExecutor {
    rows: vec::IntoIter<Vec<BoxedExpression>>,
    schema: Schema,
    identity: String,
    chunk_size: usize,
}

impl ValuesExecutor {
    pub(crate) fn new(
        rows: Vec<Vec<BoxedExpression>>,
        schema: Schema,
        identity: String,
        chunk_size: usize,
    ) -> Self {
        Self {
            rows: rows.into_iter(),
            schema,
            identity,
            chunk_size,
        }
    }
}

impl Executor for ValuesExecutor {
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

impl ValuesExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        if !self.rows.is_empty() {
            let cardinality = self.rows.len();
            ensure!(cardinality > 0);

            while !self.rows.is_empty() {
                // We need a one row chunk rather than an empty chunk because constant
                // expression's eval result is same size as input chunk
                // cardinality.
                let one_row_chunk = DataChunk::new_dummy(1);

                let chunk_size = self.chunk_size.min(self.rows.len());
                let mut array_builders = self.schema.create_array_builders(chunk_size);
                for row in self.rows.by_ref().take(chunk_size) {
                    for (expr, builder) in row.into_iter().zip_eq_fast(&mut array_builders) {
                        let out = expr.eval(&one_row_chunk).await?;
                        builder.append_array(&out);
                    }
                }

                let columns: Vec<_> = array_builders
                    .into_iter()
                    .map(|b| b.finish().into())
                    .collect();

                let chunk = DataChunk::new(columns, chunk_size);

                yield chunk
            }
        }
    }
}

impl BoxedExecutorBuilder for ValuesExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.is_empty(), "ValuesExecutor should have no child!");
        let value_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Values
        )?;

        let mut rows: Vec<Vec<BoxedExpression>> = Vec::with_capacity(value_node.get_tuples().len());
        for row in value_node.get_tuples() {
            let expr_row: Vec<_> = row.get_cells().iter().map(build_from_prost).try_collect()?;
            rows.push(expr_row);
        }

        let fields = value_node
            .get_fields()
            .iter()
            .map(Field::from)
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            rows: rows.into_iter(),
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
            chunk_size: source.context().get_config().developer.chunk_size,
        }))
    }
}

#[cfg(test)]
mod tests {

    use futures::stream::StreamExt;
    use risingwave_common::array::{
        Array, ArrayImpl, I16Array, I32Array, I64Array, StructArray, StructValue,
    };
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataType, ScalarImpl, StructType};
    use risingwave_expr::expr::{BoxedExpression, LiteralExpression};

    use crate::executor::{Executor, ValuesExecutor};

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_values_executor() {
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
                StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]).into(),
                Some(ScalarImpl::Struct(value)),
            )) as BoxedExpression,
        ];

        let fields = exprs
            .iter() // for each column
            .map(|col| Field::unnamed(col.return_type()))
            .collect::<Vec<Field>>();

        let values_executor = Box::new(ValuesExecutor {
            rows: vec![exprs].into_iter(),
            schema: Schema { fields },
            identity: "ValuesExecutor2".to_owned(),
            chunk_size: CHUNK_SIZE,
        });

        let fields = &values_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int16);
        assert_eq!(fields[1].data_type, DataType::Int32);
        assert_eq!(fields[2].data_type, DataType::Int64);
        assert_eq!(
            fields[3].data_type,
            StructType::unnamed(vec![DataType::Int32, DataType::Int32, DataType::Int32]).into()
        );

        let mut stream = values_executor.execute();
        let result = stream.next().await.unwrap();
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

        if let Ok(result) = result {
            assert_eq!(*result.column_at(0), I16Array::from_iter([1]).into_ref());
            assert_eq!(*result.column_at(1), I32Array::from_iter([2]).into_ref());
            assert_eq!(*result.column_at(2), I64Array::from_iter([3]).into_ref());
            assert_eq!(*result.column_at(3), array.into());
        }
    }

    #[tokio::test]
    async fn test_chunk_split_size() {
        let rows = [
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(1)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(2)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(3)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(4)),
            )) as BoxedExpression,
        ]
        .into_iter()
        .map(|expr| vec![expr])
        .collect::<Vec<_>>();

        let fields = vec![Field::unnamed(DataType::Int32)];

        let values_executor = Box::new(ValuesExecutor::new(
            rows,
            Schema { fields },
            "ValuesExecutor2".to_owned(),
            3,
        ));
        let mut stream = values_executor.execute();
        assert_eq!(stream.next().await.unwrap().unwrap().cardinality(), 3);
        assert_eq!(stream.next().await.unwrap().unwrap().cardinality(), 1);
        assert!(stream.next().await.is_none());
    }

    // Handle the possible case of ValuesNode([[]])
    #[tokio::test]
    async fn test_no_column_values_executor() {
        let values_executor = Box::new(ValuesExecutor::new(
            vec![vec![]],
            Schema::default(),
            "ValuesExecutor2".to_owned(),
            CHUNK_SIZE,
        ));
        let mut stream = values_executor.execute();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.cardinality(), 1);
        assert_eq!(result.dimension(), 0);

        assert!(stream.next().await.is_none());
    }
}
