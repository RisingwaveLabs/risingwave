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

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use tokio_postgres;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, DataChunk, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

/// S3 file scan executor. Currently only support parquet file format.
pub struct PostgresQueryExecutor {
    schema: Schema,
    host: String,
    port: String,
    username: String,
    password: String,
    database: String,
    query: String,
    identity: String,
}

impl Executor for PostgresQueryExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

pub fn postgres_row_to_owned_row(
    row: tokio_postgres::Row,
    schema: &Schema,
) -> Result<OwnedRow, BatchError> {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = postgres_cell_to_scalar_impl(&row, &rw_field.data_type, i, name)?;
        datums.push(datum);
    }
    Ok(OwnedRow::new(datums))
}

// TODO(kwannoel): Support more types, see postgres connector's ScalarAdapter.
fn postgres_cell_to_scalar_impl(
    row: &tokio_postgres::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> Result<Datum, BatchError> {
    // We observe several incompatibility issue in Debezium's Postgres connector. We summarize them here:
    // Issue #1. The null of enum list is not supported in Debezium. An enum list contains `NULL` will fallback to `NULL`.
    // Issue #2. In our parser, when there's inf, -inf, nan or invalid item in a list, the whole list will fallback null.
    let datum = match data_type {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Date
        | DataType::Time
        | DataType::Timestamp
        | DataType::Timestamptz
        | DataType::Jsonb
        | DataType::Interval
        | DataType::Bytea => {
            // ScalarAdapter is also fine. But ScalarImpl is more efficient
            row.try_get::<_, Option<ScalarImpl>>(i)?
        }
        DataType::Decimal => {
            // Decimal is more efficient than PgNumeric in ScalarAdapter
            let val = row.try_get::<_, Option<Decimal>>(i)?;
            val.map(ScalarImpl::from)
        }
        _ => {
            tracing::warn!(name, ?data_type, "unsupported data type, set to null");
            None
        }
    };
    Ok(datum)
}

impl PostgresQueryExecutor {
    pub fn new(
        schema: Schema,
        host: String,
        port: String,
        username: String,
        password: String,
        database: String,
        query: String,
        identity: String,
    ) -> Self {
        Self {
            schema,
            host,
            port,
            username,
            password,
            database,
            query,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        tracing::debug!("postgres_query_executor: started");
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.username, self.password, self.database
        );
        let (client, _conn) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
        // TODO(kwannoel): Use pagination using CURSOR.
        let rows = client.query(&self.query, &[]).await?;
        let mut builder = DataChunkBuilder::new(self.schema.data_types(), 1024);
        tracing::debug!("postgres_query_executor: query executed, start deserializing rows");
        // deserialize the rows
        for row in rows {
            let owned_row = postgres_row_to_owned_row(row, &self.schema)?;
            if let Some(chunk) = builder.append_one_row(owned_row) {
                yield chunk;
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
        return Ok(());
    }
}

pub struct PostgresQueryExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for PostgresQueryExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let postgres_query_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::PostgresQuery
        )?;

        Ok(Box::new(PostgresQueryExecutor::new(
            Schema::from_iter(postgres_query_node.columns.iter().map(Field::from)),
            postgres_query_node.hostname.clone(),
            postgres_query_node.port.clone(),
            postgres_query_node.username.clone(),
            postgres_query_node.password.clone(),
            postgres_query_node.database.clone(),
            postgres_query_node.query.clone(),
            source.plan_node().get_identity().clone(),
        )))
    }
}
