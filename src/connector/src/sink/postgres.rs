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

use std::collections::{BTreeMap, HashSet};

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{Row, RowExt};
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use simd_json::prelude::ArrayTrait;
use thiserror_ext::AsReport;
use tokio_postgres::types::Type as PgType;

use super::{
    LogSinker, SinkError, SinkLogReader, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::connector_common::{create_pg_client, PostgresExternalTable, SslMode};
use crate::parser::scalar_adapter::{validate_pg_type_to_rw_type, ScalarAdapter};
use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriterParam};

pub const POSTGRES_SINK: &str = "postgres";

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub table: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "Default::default")]
    pub ssl_mode: SslMode,
    #[serde(rename = "ssl.root.cert")]
    pub ssl_root_cert: Option<String>,
    #[serde(default = "default_max_batch_rows")]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
}

fn default_max_batch_rows() -> usize {
    1024
}

fn default_schema() -> String {
    "public".to_owned()
}

impl PostgresConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<PostgresConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

#[derive(Debug)]
pub struct PostgresSink {
    pub config: PostgresConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl PostgresSink {
    pub fn new(
        config: PostgresConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for PostgresSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = PostgresConfig::from_btreemap(param.properties)?;
        PostgresSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl Sink for PostgresSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = PostgresSinkWriter;

    const SINK_NAME: &'static str = POSTGRES_SINK;

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert Postgres sink (please define in `primary_key` field)")));
        }

        // Verify our sink schema is compatible with Postgres
        {
            let pg_table = PostgresExternalTable::connect(
                &self.config.user,
                &self.config.password,
                &self.config.host,
                self.config.port,
                &self.config.database,
                &self.config.schema,
                &self.config.table,
                &self.config.ssl_mode,
                &self.config.ssl_root_cert,
                self.is_append_only,
            )
            .await?;

            // Check that names and types match, order of columns doesn't matter.
            {
                let pg_columns = pg_table.column_descs();
                let sink_columns = self.schema.fields();
                if pg_columns.len() < sink_columns.len() {
                    return Err(SinkError::Config(anyhow!(
                    "Column count mismatch: Postgres table has {} columns, but sink schema has {} columns, sink should have less or equal columns to the Postgres table",
                    pg_columns.len(),
                    sink_columns.len()
                )));
                }

                let pg_columns_lookup = pg_columns
                    .iter()
                    .map(|c| (c.name.clone(), c.data_type.clone()))
                    .collect::<BTreeMap<_, _>>();
                for sink_column in sink_columns {
                    let pg_column = pg_columns_lookup.get(&sink_column.name);
                    match pg_column {
                        None => {
                            return Err(SinkError::Config(anyhow!(
                                "Column `{}` not found in Postgres table `{}`",
                                sink_column.name,
                                self.config.table
                            )))
                        }
                        Some(pg_column) => {
                            if !validate_pg_type_to_rw_type(pg_column, &sink_column.data_type()) {
                                return Err(SinkError::Config(anyhow!(
                                "Column `{}` in Postgres table `{}` has type `{}`, but sink schema defines it as type `{}`",
                                sink_column.name,
                                self.config.table,
                                pg_column,
                                sink_column.data_type()
                            )));
                            }
                        }
                    }
                }
            }

            // check that pk matches
            {
                let pg_pk_names = pg_table.pk_names();
                let sink_pk_names = self
                    .pk_indices
                    .iter()
                    .map(|i| &self.schema.fields()[*i].name)
                    .collect::<HashSet<_>>();
                if pg_pk_names.len() != sink_pk_names.len() {
                    return Err(SinkError::Config(anyhow!(
                    "Primary key mismatch: Postgres table has primary key on columns {:?}, but sink schema defines primary key on columns {:?}",
                    pg_pk_names,
                    sink_pk_names
                )));
                }
                for name in pg_pk_names {
                    if !sink_pk_names.contains(name) {
                        return Err(SinkError::Config(anyhow!(
                        "Primary key mismatch: Postgres table has primary key on column `{}`, but sink schema does not define it as a primary key",
                        name
                    )));
                    }
                }
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        PostgresSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await
    }
}

struct ParameterBuffer<'a> {
    /// A set of parameters to be inserted/deleted.
    /// Each set is a flattened 2d-array.
    parameters: Vec<Vec<Option<ScalarAdapter>>>,
    /// the column dimension (fixed).
    column_length: usize,
    /// schema types to serialize into `ScalarAdapter`
    schema_types: &'a [PgType],
    /// estimated number of parameters that can be sent in a single query.
    estimated_parameter_size: usize,
    /// current parameter buffer to be filled.
    current_parameter_buffer: Vec<Option<ScalarAdapter>>,
}

impl<'a> ParameterBuffer<'a> {
    /// The maximum number of parameters that can be sent in a single query.
    /// See: <https://www.postgresql.org/docs/current/limits.html>
    /// and <https://github.com/sfackler/rust-postgres/issues/356>
    const MAX_PARAMETERS: usize = 32768;

    /// `flattened_chunk_size` is the number of datums in a single chunk.
    fn new(schema_types: &'a [PgType], flattened_chunk_size: usize) -> Self {
        let estimated_parameter_size = usize::min(Self::MAX_PARAMETERS, flattened_chunk_size);
        Self {
            parameters: vec![],
            column_length: schema_types.len(),
            schema_types,
            estimated_parameter_size,
            current_parameter_buffer: Vec::with_capacity(estimated_parameter_size),
        }
    }

    fn add_row(&mut self, row: impl Row) {
        if self.current_parameter_buffer.len() + self.column_length >= Self::MAX_PARAMETERS {
            self.new_buffer();
        }
        for (i, datum_ref) in row.iter().enumerate() {
            let pg_datum = datum_ref.map(|s| {
                let ty = &self.schema_types[i];
                match ScalarAdapter::from_scalar(s, ty) {
                    Ok(scalar) => Some(scalar),
                    Err(e) => {
                        tracing::error!(error=%e.as_report(), scalar=?s, "Failed to convert scalar to pg value");
                        None
                    }
                }
            });
            self.current_parameter_buffer.push(pg_datum.flatten());
        }
    }

    fn new_buffer(&mut self) {
        let filled_buffer = std::mem::replace(
            &mut self.current_parameter_buffer,
            Vec::with_capacity(self.estimated_parameter_size),
        );
        self.parameters.push(filled_buffer);
    }

    fn into_parts(self) -> (Vec<Vec<Option<ScalarAdapter>>>, Vec<Option<ScalarAdapter>>) {
        (self.parameters, self.current_parameter_buffer)
    }
}

pub struct PostgresSinkWriter {
    config: PostgresConfig,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    client: tokio_postgres::Client,
    schema_types: Vec<PgType>,
    schema: Schema,
}

impl PostgresSinkWriter {
    async fn new(
        config: PostgresConfig,
        mut schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = create_pg_client(
            &config.user,
            &config.password,
            &config.host,
            &config.port.to_string(),
            &config.database,
            &config.ssl_mode,
            &config.ssl_root_cert,
        )
        .await?;

        // Rewrite schema types for serialization
        let schema_types = {
            let pg_table = PostgresExternalTable::connect(
                &config.user,
                &config.password,
                &config.host,
                config.port,
                &config.database,
                &config.schema,
                &config.table,
                &config.ssl_mode,
                &config.ssl_root_cert,
                is_append_only,
            )
            .await?;
            let name_to_type = pg_table.column_name_to_pg_type();
            let mut schema_types = Vec::with_capacity(schema.fields.len());
            for field in &mut schema.fields[..] {
                let field_name = &field.name;
                let actual_data_type = name_to_type.get(field_name).map(|t| (*t).clone());
                let actual_data_type = actual_data_type
                    .ok_or_else(|| {
                        SinkError::Config(anyhow!(
                            "Column `{}` not found in sink schema",
                            field_name
                        ))
                    })?
                    .clone();
                schema_types.push(actual_data_type);
            }
            schema_types
        };

        let writer = Self {
            config,
            pk_indices,
            is_append_only,
            client,
            schema_types,
            schema,
        };
        Ok(writer)
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        // https://www.postgresql.org/docs/current/limits.html
        // We have a limit of 65,535 parameters in a single query, as restricted by the PostgreSQL protocol.
        if self.is_append_only {
            self.write_batch_append_only(chunk).await
        } else {
            self.write_batch_non_append_only(chunk).await
        }
    }

    async fn write_batch_append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut transaction = self.client.transaction().await?;
        // 1d flattened array of parameters to be inserted.
        let mut parameter_buffer = ParameterBuffer::new(
            &self.schema_types,
            chunk.cardinality() * chunk.data_types().len(),
        );
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    parameter_buffer.add_row(row);
                }
                Op::UpdateInsert | Op::Delete | Op::UpdateDelete => {
                    bail!("append-only sink should not receive update insert, update delete and delete operations")
                }
            }
        }
        let (parameters, remaining) = parameter_buffer.into_parts();
        Self::execute_parameter(
            Op::Insert,
            &mut transaction,
            &self.schema,
            &self.config.table,
            &self.pk_indices,
            parameters,
            remaining,
        )
        .await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn write_batch_non_append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut transaction = self.client.transaction().await?;
        // 1d flattened array of parameters to be inserted.
        let mut insert_parameter_buffer = ParameterBuffer::new(
            &self.schema_types,
            chunk.cardinality() * chunk.data_types().len(),
        );
        let mut delete_parameter_buffer = ParameterBuffer::new(
            &self.schema_types,
            chunk.cardinality() * self.pk_indices.len(),
        );
        // 1d flattened array of parameters to be deleted.
        for (op, row) in chunk.rows() {
            match op {
                Op::UpdateInsert | Op::Insert => {
                    insert_parameter_buffer.add_row(row);
                }
                Op::UpdateDelete | Op::Delete => {
                    delete_parameter_buffer.add_row(row.project(&self.pk_indices));
                }
            }
        }

        let (delete_parameters, delete_remaining_parameter) = delete_parameter_buffer.into_parts();
        Self::execute_parameter(
            Op::Delete,
            &mut transaction,
            &self.schema,
            &self.config.table,
            &self.pk_indices,
            delete_parameters,
            delete_remaining_parameter,
        )
        .await?;
        let (insert_parameters, insert_remaining_parameter) = insert_parameter_buffer.into_parts();
        Self::execute_parameter(
            Op::Insert,
            &mut transaction,
            &self.schema,
            &self.config.table,
            &self.pk_indices,
            insert_parameters,
            insert_remaining_parameter,
        )
        .await?;
        transaction.commit().await?;

        Ok(())
    }

    async fn execute_parameter(
        op: Op,
        transaction: &mut tokio_postgres::Transaction<'_>,
        schema: &Schema,
        table_name: &str,
        pk_indices: &[usize],
        parameters: Vec<Vec<Option<ScalarAdapter>>>,
        remaining_parameter: Vec<Option<ScalarAdapter>>,
    ) -> Result<()> {
        let column_length = match op {
            Op::Insert => schema.len(),
            Op::Delete => pk_indices.len(),
            _ => unreachable!(),
        };
        if !parameters.is_empty() {
            let parameter_length = parameters[0].len();
            let rows_length = parameter_length / column_length;
            assert_eq!(
                parameter_length % column_length,
                0,
                "flattened parameters are unaligned, parameters={:#?} columns={:#?}",
                parameters,
                schema.fields(),
            );
            let statement = match op {
                Op::Insert => create_insert_sql(schema, table_name, rows_length),
                Op::Delete => create_delete_sql(schema, table_name, pk_indices, rows_length),
                _ => unreachable!(),
            };
            let statement = transaction.prepare(&statement).await?;
            for parameter in parameters {
                transaction.execute_raw(&statement, parameter).await?;
            }
        }
        if !remaining_parameter.is_empty() {
            let rows_length = remaining_parameter.len() / column_length;
            assert_eq!(
                remaining_parameter.len() % column_length,
                0,
                "flattened parameters are unaligned"
            );
            let statement = match op {
                Op::Insert => create_insert_sql(schema, table_name, rows_length),
                Op::Delete => create_delete_sql(schema, table_name, pk_indices, rows_length),
                _ => unreachable!(),
            };
            tracing::trace!("binding statement: {:?}", statement);
            let statement = transaction.prepare(&statement).await?;
            tracing::trace!("binding parameters: {:?}", remaining_parameter);
            transaction
                .execute_raw(&statement, remaining_parameter)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl LogSinker for PostgresSinkWriter {
    async fn consume_log_and_sink(mut self, log_reader: &mut impl SinkLogReader) -> Result<!> {
        loop {
            let (epoch, item) = log_reader.next_item().await?;
            match item {
                LogStoreReadItem::StreamChunk { chunk, chunk_id } => {
                    self.write_batch(chunk).await?;
                    log_reader.truncate(TruncateOffset::Chunk { epoch, chunk_id })?;
                }
                LogStoreReadItem::Barrier { .. } => {
                    log_reader.truncate(TruncateOffset::Barrier { epoch })?;
                }
                LogStoreReadItem::UpdateVnodeBitmap(_) => {}
            }
        }
    }
}

fn create_insert_sql(schema: &Schema, table_name: &str, number_of_rows: usize) -> String {
    let number_of_columns = schema.len();
    let columns: String = schema
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .join(", ");
    let parameters: String = (0..number_of_rows)
        .map(|i| {
            let row_parameters = (0..number_of_columns)
                .map(|j| format!("${}", i * number_of_columns + j + 1))
                .join(", ");
            format!("({row_parameters})")
        })
        .collect_vec()
        .join(", ");
    format!("INSERT INTO {table_name} ({columns}) VALUES {parameters}")
}

fn create_delete_sql(
    schema: &Schema,
    table_name: &str,
    pk_indices: &[usize],
    number_of_rows: usize,
) -> String {
    let number_of_pk = pk_indices.len();
    let pk = {
        let pk_symbols = pk_indices
            .iter()
            .map(|pk_index| &schema.fields()[*pk_index].name)
            .join(", ");
        format!("({})", pk_symbols)
    };
    let parameters: String = (0..number_of_rows)
        .map(|i| {
            let row_parameters: String = (0..pk_indices.len())
                .map(|j| format!("${}", i * number_of_pk + j + 1))
                .join(", ");
            format!("({row_parameters})")
        })
        .collect_vec()
        .join(", ");
    format!("DELETE FROM {table_name} WHERE {pk} in ({parameters})")
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use expect_test::{expect, Expect};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;

    fn check(actual: impl Display, expect: Expect) {
        let actual = actual.to_string();
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_create_insert_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let table_name = "test_table";
        let sql = create_insert_sql(&schema, table_name, 3);
        check(
            sql,
            expect!["INSERT INTO test_table (a, b) VALUES ($1, $2), ($3, $4), ($5, $6)"],
        );
    }

    #[test]
    fn test_create_delete_sql() {
        let schema = Schema::new(vec![
            Field {
                data_type: DataType::Int32,
                name: "a".to_owned(),
            },
            Field {
                data_type: DataType::Int32,
                name: "b".to_owned(),
            },
        ]);
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, table_name, &[1], 3);
        check(
            sql,
            expect!["DELETE FROM test_table WHERE (b) in (($1), ($2), ($3))"],
        );
        let table_name = "test_table";
        let sql = create_delete_sql(&schema, table_name, &[0, 1], 3);
        check(
            sql,
            expect!["DELETE FROM test_table WHERE (a, b) in (($1, $2), ($3, $4), ($5, $6))"],
        );
    }
}
