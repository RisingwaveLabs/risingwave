// Copyright 2024 RisingWave Labs
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

//! This module contains the expression for computing the iceberg partition value.
//! spec ref: <https://iceberg.apache.org/spec/#partition-transforms>
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use icelake::types::{create_transform_function, BoxedTransformFunction, Transform};
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::ensure;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::BoxedExpression;
use risingwave_expr::{build_function, ExprError, Result};

pub struct IcebergTransform {
    child: BoxedExpression,
    transform: BoxedTransformFunction,
    return_type: DataType,
}

impl std::fmt::Debug for IcebergTransform {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergTransform")
            .field("child", &self.child)
            .field("return_type", &self.return_type)
            .finish()
    }
}

#[async_trait::async_trait]
impl risingwave_expr::expr::Expression for IcebergTransform {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        // Get the child array
        let array = self.child.eval(data_chunk).await?;
        // Convert to arrow array
        let arrow_array = array.as_ref().try_into().unwrap();
        // Transform
        let res_array = self.transform.transform(arrow_array).unwrap();
        // Convert back to array ref and return it
        Ok(Arc::new((&res_array).try_into().unwrap()))
    }

    async fn eval_row(&self, _row: &OwnedRow) -> Result<Datum> {
        Err(ExprError::Internal(anyhow!(
            "eval_row in iceberg_transform is not supported yet"
        )))
    }
}

#[build_function("iceberg_transform(varchar, any) -> any")]
fn build(return_type: DataType, mut children: Vec<BoxedExpression>) -> Result<BoxedExpression> {
    let transform_type = {
        let datum = children[0].eval_const()?.unwrap();
        let str = datum.as_utf8();
        Transform::from_str(str).map_err(|_| ExprError::InvalidParam {
            name: "transform type in icberg_transform",
            reason: format!("Fail to parse {str} as iceberg transform type").into(),
        })?
    };

    // Check type
    match &transform_type {
        Transform::Bucket(_) => IcebergTransform::check_bucket(&return_type, &children)?,
        Transform::Truncate(_) => IcebergTransform::check_truncate(&return_type, &children)?,
        Transform::Year | Transform::Month | Transform::Day | Transform::Hour => {
            IcebergTransform::check_time(&return_type, &children)?
        }
        Transform::Identity | Transform::Void => {
            return Err(ExprError::Internal(anyhow!(
                "identity or void type should not be used in iceberg_transform"
            )))
        }
    }

    Ok(Box::new(IcebergTransform {
        child: children.remove(1),
        transform: create_transform_function(&transform_type)
            .map_err(|err| ExprError::Internal(err.into()))?,
        return_type,
    }))
}

impl IcebergTransform {
    fn check_bucket(return_type: &DataType, children: &Vec<BoxedExpression>) -> Result<()> {
        ensure!(matches!(
            children[1].return_type(),
            DataType::Int32
                | DataType::Int64
                | DataType::Decimal
                | DataType::Date
                | DataType::Time
                | DataType::Timestamp
                | DataType::Timestamptz
                | DataType::Varchar
                | DataType::Bytea
        ));
        ensure!(*return_type == DataType::Int32);
        Ok(())
    }

    fn check_truncate(return_type: &DataType, children: &Vec<BoxedExpression>) -> Result<()> {
        ensure!(matches!(
            children[1].return_type(),
            DataType::Int32 | DataType::Int64 | DataType::Decimal | DataType::Varchar
        ));
        ensure!(*return_type == children[1].return_type());
        Ok(())
    }

    fn check_time(return_type: &DataType, children: &Vec<BoxedExpression>) -> Result<()> {
        ensure!(matches!(
            children[1].return_type(),
            DataType::Date | DataType::Timestamp | DataType::Timestamptz
        ));
        ensure!(*return_type == DataType::Int32);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_expr::expr::build_from_pretty;

    #[tokio::test]
    async fn test_bucket() {
        let (input, expected) = DataChunk::from_pretty(
            "i   i
             34  1373",
        )
        .split_column_at(1);
        let expr = build_from_pretty("(iceberg_transform:int4 bucket[2017]:varchar $0:int)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));
    }

    #[tokio::test]
    async fn test_truncate() {
        let (input, expected) = DataChunk::from_pretty(
            "T         T
            iceberg   ice
            risingwave ris
            delta     del",
        )
        .split_column_at(1);
        let expr = build_from_pretty("(iceberg_transform:varchar truncate[3]:varchar $0:varchar)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));
    }

    #[tokio::test]
    async fn test_year_month_day_hour() {
        let (input, expected) = DataChunk::from_pretty(
            "TZ                                  i i i i
            1970-01-01T00:00:00.000000000+00:00  0 0 0 0
            1971-02-01T01:00:00.000000000+00:00  1 13 396 9505
            1972-03-01T02:00:00.000000000+00:00  2 26 790 18962
            1970-05-01T06:00:00.000000000+00:00  0 4 120 2886
            1970-06-01T07:00:00.000000000+00:00  0 5 151 3631",
        )
        .split_column_at(1);

        // year
        let expr = build_from_pretty("(iceberg_transform:int4 year:varchar $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(0));

        // month
        let expr = build_from_pretty("(iceberg_transform:int4 month:varchar $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(1));

        // day
        let expr = build_from_pretty("(iceberg_transform:int4 day:varchar $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(2));

        // hour
        let expr = build_from_pretty("(iceberg_transform:int4 hour:varchar $0:timestamptz)");
        let res = expr.eval(&input).await.unwrap();
        assert_eq!(res, *expected.column_at(3));
    }
}
