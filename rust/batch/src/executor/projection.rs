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

use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_expr::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub(super) struct ProjectionExecutor {
    expr: Vec<BoxedExpression>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
}

#[async_trait::async_trait]
impl Executor for ProjectionExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let child_output = self.child.next().await?;
        match child_output {
            Some(child_chunk) => {
                let arrays: Vec<Column> = self
                    .expr
                    .iter_mut()
                    .map(|expr| expr.eval(&child_chunk).map(Column::new))
                    .collect::<Result<Vec<_>>>()?;
                let ret = DataChunk::builder().columns(arrays).build();
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for ProjectionExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_children().len() == 1);

        let project_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Project
        )?;

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child_node = source.clone_for_plan(proto_child).build()?;

        let project_exprs = project_node
            .get_select_list()
            .iter()
            .map(build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        let fields = project_exprs
            .iter()
            .map(|expr| Field::unnamed(expr.return_type()))
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            expr: project_exprs,
            child: child_node,
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::{InputRefExpression, LiteralExpression};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::values::ValuesExecutor;
    use crate::*;

    #[tokio::test]
    async fn test_project_executor() -> Result<()> {
        let col1 = column_nonnull! {I32Array, [1, 2, 33333, 4, 5]};
        let col2 = column_nonnull! {I32Array, [7, 8, 66666, 4, 3]};
        let chunk = DataChunk::builder().columns(vec![col1, col2]).build();

        let expr1 = InputRefExpression::new(DataType::Int32, 0);
        let expr_vec = vec![Box::new(expr1) as BoxedExpression];

        let schema = schema_unnamed! { DataType::Int32, DataType::Int32 };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(chunk);

        let fields = expr_vec
            .iter()
            .map(|expr| Field::unnamed(expr.return_type()))
            .collect::<Vec<Field>>();

        let mut proj_executor = ProjectionExecutor {
            expr: expr_vec,
            child: Box::new(mock_executor),
            schema: Schema { fields },
            identity: "ProjectionExecutor".to_string(),
        };
        proj_executor.open().await.unwrap();

        let fields = &proj_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int32);

        let result_chunk = proj_executor.next().await?.unwrap();
        proj_executor.close().await.unwrap();
        assert_eq!(result_chunk.dimension(), 1);
        assert_eq!(
            result_chunk
                .column_at(0)
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(33333), Some(4), Some(5)]
        );

        proj_executor.close().await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_project_dummy_chunk() {
        let literal = LiteralExpression::new(DataType::Int32, Some(1_i32.into()));

        let values_executor = ValuesExecutor::new(
            vec![vec![]], // One single row with no column.
            Schema::default(),
            "ValuesExecutor".to_string(),
            1024,
        );

        let mut proj_executor = ProjectionExecutor {
            expr: vec![Box::new(literal)],
            child: Box::new(values_executor),
            schema: schema_unnamed!(DataType::Int32),
            identity: "ProjectionExecutor".to_string(),
        };

        proj_executor.open().await.unwrap();
        let chunk = proj_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *chunk.column_at(0).array(),
            array_nonnull!(I32Array, [1]).into()
        );
    }
}
