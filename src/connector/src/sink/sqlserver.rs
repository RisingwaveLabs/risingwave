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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Decimal};
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use simd_json::prelude::ArrayTrait;
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use with_options::WithOptions;

use super::{
    SinkError, SinkWriterMetrics, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};
use crate::sink::writer::{LogSinkerOf, SinkWriter, SinkWriterExt};
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkParam, SinkWriterParam};

pub const SQLSERVER_SINK: &str = "sqlserver";

fn default_max_batch_rows() -> usize {
    1024
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SqlServerConfig {
    #[serde(rename = "sqlserver.host")]
    pub host: String,
    #[serde(rename = "sqlserver.port")]
    #[serde_as(as = "DisplayFromStr")]
    pub port: u16,
    #[serde(rename = "sqlserver.user")]
    pub user: String,
    #[serde(rename = "sqlserver.password")]
    pub password: String,
    #[serde(rename = "sqlserver.database")]
    pub database: String,
    #[serde(rename = "sqlserver.table")]
    pub table: String,
    #[serde(
        rename = "sqlserver.max_batch_rows",
        default = "default_max_batch_rows"
    )]
    #[serde_as(as = "DisplayFromStr")]
    pub max_batch_rows: usize,
    pub r#type: String, // accept "append-only" or "upsert"
}

impl SqlServerConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SqlServerConfig>(serde_json::to_value(properties).unwrap())
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
pub struct SqlServerSink {
    pub config: SqlServerConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl SqlServerSink {
    pub fn new(
        mut config: SqlServerConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        // Rewrite config because tiberius allows a maximum of 2100 params in one query request.
        const TIBERIUS_PARAM_MAX: usize = 2000;
        let params_per_op = schema.fields().len();
        let tiberius_max_batch_rows = if params_per_op == 0 {
            config.max_batch_rows
        } else {
            ((TIBERIUS_PARAM_MAX as f64 / params_per_op as f64).floor()) as usize
        };
        if tiberius_max_batch_rows == 0 {
            return Err(SinkError::SqlServer(anyhow!(format!(
                "too many column {}",
                params_per_op
            ))));
        }
        config.max_batch_rows = std::cmp::min(config.max_batch_rows, tiberius_max_batch_rows);
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl TryFrom<SinkParam> for SqlServerSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SqlServerConfig::from_btreemap(param.properties)?;
        SqlServerSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl Sink for SqlServerSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<SqlServerSinkWriter>;

    const SINK_NAME: &'static str = SQLSERVER_SINK;

    async fn validate(&self) -> Result<()> {
        risingwave_common::license::Feature::SqlServerSink
            .check_available()
            .map_err(|e| anyhow::anyhow!(e))?;

        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert SQL Server sink (please define in `primary_key` field)")));
        }

        for f in self.schema.fields() {
            check_data_type_compatibility(&f.data_type)?;
        }

        // Query table metadata from SQL Server.
        let mut sql_server_table_metadata = HashMap::new();
        let mut sql_client = SqlServerClient::new(&self.config).await?;
        let query_table_metadata_error = || {
            SinkError::SqlServer(anyhow!(format!(
                "SQL Server table {} metadata error",
                self.config.table
            )))
        };
        static QUERY_TABLE_METADATA: &str = r#"
SELECT
    col.name AS ColumnName,
    pk.index_id AS PkIndex
FROM
    sys.columns col
LEFT JOIN
    sys.index_columns ic ON ic.object_id = col.object_id AND ic.column_id = col.column_id
LEFT JOIN
    sys.indexes pk ON pk.object_id = col.object_id AND pk.index_id = ic.index_id AND pk.is_primary_key = 1
WHERE
    col.object_id = OBJECT_ID(@P1)
ORDER BY
    col.column_id;"#;
        let rows = sql_client
            .inner_client
            .query(QUERY_TABLE_METADATA, &[&self.config.table])
            .await?
            .into_results()
            .await?;
        for row in rows.into_iter().flatten() {
            let mut iter = row.into_iter();
            let ColumnData::String(Some(col_name)) =
                iter.next().ok_or_else(query_table_metadata_error)?
            else {
                return Err(query_table_metadata_error());
            };
            let ColumnData::I32(col_pk_index) =
                iter.next().ok_or_else(query_table_metadata_error)?
            else {
                return Err(query_table_metadata_error());
            };
            sql_server_table_metadata.insert(col_name.into_owned(), col_pk_index.is_some());
        }

        // Validate Column name and Primary Key
        for (idx, col) in self.schema.fields().iter().enumerate() {
            let rw_is_pk = self.pk_indices.contains(&idx);
            match sql_server_table_metadata.get(&col.name) {
                None => {
                    return Err(SinkError::SqlServer(anyhow!(format!(
                        "column {} not found in the downstream SQL Server table {}",
                        col.name, self.config.table
                    ))));
                }
                Some(sql_server_is_pk) => {
                    if self.is_append_only {
                        continue;
                    }
                    if rw_is_pk && !*sql_server_is_pk {
                        return Err(SinkError::SqlServer(anyhow!(format!(
                            "column {} specified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name, self.config.table,
                        ))));
                    }
                    if !rw_is_pk && *sql_server_is_pk {
                        return Err(SinkError::SqlServer(anyhow!(format!(
                            "column {} unspecified in primary_key mismatches with the downstream SQL Server table {} PK",
                            col.name, self.config.table,
                        ))));
                    }
                }
            }
        }

        if !self.is_append_only {
            let sql_server_pk_count = sql_server_table_metadata
                .values()
                .filter(|is_pk| **is_pk)
                .count();
            if sql_server_pk_count != self.pk_indices.len() {
                return Err(SinkError::SqlServer(anyhow!(format!(
                    "primary key does not match between RisingWave sink ({}) and SQL Server table {} ({})",
                    self.pk_indices.len(),
                    self.config.table,
                    sql_server_pk_count,
                ))));
            }
        }

        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SqlServerSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?
        .into_log_sinker(SinkWriterMetrics::new(&writer_param)))
    }
}

enum SqlOp {
    Insert(OwnedRow),
    Merge(OwnedRow),
    Delete(OwnedRow),
}

pub struct SqlServerSinkWriter {
    config: SqlServerConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    sql_client: SqlServerClient,
    ops: Vec<SqlOp>,
}

impl SqlServerSinkWriter {
    async fn new(
        config: SqlServerConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let sql_client = SqlServerClient::new(&config).await?;
        let writer = Self {
            config,
            schema,
            pk_indices,
            is_append_only,
            sql_client,
            ops: vec![],
        };
        Ok(writer)
    }

    async fn delete_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Delete(row.into_owned_row()));
        Ok(())
    }

    async fn upsert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Merge(row.into_owned_row()));
        Ok(())
    }

    async fn insert_one(&mut self, row: RowRef<'_>) -> Result<()> {
        if self.ops.len() + 1 >= self.config.max_batch_rows {
            self.flush().await?;
        }
        self.ops.push(SqlOp::Insert(row.into_owned_row()));
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        use std::fmt::Write;
        if self.ops.is_empty() {
            return Ok(());
        }
        let mut query_str = String::new();
        let col_num = self.schema.fields.len();
        let mut next_param_id = 1;
        let non_pk_col_indices = (0..col_num)
            .filter(|idx| !self.pk_indices.contains(idx))
            .collect::<Vec<usize>>();
        let all_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let all_source_col_names = self
            .schema
            .fields
            .iter()
            .map(|f| format!("[SOURCE].[{}]", f.name))
            .collect::<Vec<_>>()
            .join(",");
        let pk_match = self
            .pk_indices
            .iter()
            .map(|idx| {
                format!(
                    "[SOURCE].[{}]=[TARGET].[{}]",
                    &self.schema[*idx].name, &self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let param_placeholders = |param_id: &mut usize| {
            let params = (*param_id..(*param_id + col_num))
                .map(|i| format!("@P{}", i))
                .collect::<Vec<_>>()
                .join(",");
            *param_id += col_num;
            params
        };
        let set_all_source_col = non_pk_col_indices
            .iter()
            .map(|idx| {
                format!(
                    "[{}]=[SOURCE].[{}]",
                    &self.schema[*idx].name, &self.schema[*idx].name
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        // TODO: avoid repeating the SQL
        for op in &self.ops {
            match op {
                SqlOp::Insert(_) => {
                    write!(
                        &mut query_str,
                        "INSERT INTO [{}] ({}) VALUES ({});",
                        self.config.table,
                        all_col_names,
                        param_placeholders(&mut next_param_id),
                    )
                    .unwrap();
                }
                SqlOp::Merge(_) => {
                    write!(
                        &mut query_str,
                        r#"MERGE [{}] AS [TARGET]
                        USING (VALUES ({})) AS [SOURCE] ({})
                        ON {}
                        WHEN MATCHED THEN UPDATE SET {}
                        WHEN NOT MATCHED THEN INSERT ({}) VALUES ({});"#,
                        self.config.table,
                        param_placeholders(&mut next_param_id),
                        all_col_names,
                        pk_match,
                        set_all_source_col,
                        all_col_names,
                        all_source_col_names,
                    )
                    .unwrap();
                }
                SqlOp::Delete(_) => {
                    write!(
                        &mut query_str,
                        r#"DELETE FROM [{}] WHERE {};"#,
                        self.config.table,
                        self.pk_indices
                            .iter()
                            .map(|idx| {
                                let condition =
                                    format!("[{}]=@P{}", self.schema[*idx].name, next_param_id);
                                next_param_id += 1;
                                condition
                            })
                            .collect::<Vec<_>>()
                            .join(" AND "),
                    )
                    .unwrap();
                }
            }
        }

        let mut query = Query::new(query_str);
        for op in self.ops.drain(..) {
            match op {
                SqlOp::Insert(row) => {
                    bind_params(&mut query, row, &self.schema, 0..col_num)?;
                }
                SqlOp::Merge(row) => {
                    bind_params(&mut query, row, &self.schema, 0..col_num)?;
                }
                SqlOp::Delete(row) => {
                    bind_params(
                        &mut query,
                        row,
                        &self.schema,
                        self.pk_indices.iter().copied(),
                    )?;
                }
            }
        }
        query.execute(&mut self.sql_client.inner_client).await?;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SqlServerSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            match op {
                Op::Insert => {
                    if self.is_append_only {
                        self.insert_one(row).await?;
                    } else {
                        self.upsert_one(row).await?;
                    }
                }
                Op::UpdateInsert => {
                    debug_assert!(!self.is_append_only);
                    self.upsert_one(row).await?;
                }
                Op::Delete => {
                    debug_assert!(!self.is_append_only);
                    self.delete_one(row).await?;
                }
                Op::UpdateDelete => {}
            }
        }
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            self.flush().await?;
        }
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct SqlServerClient {
    pub inner_client: Client<tokio_util::compat::Compat<TcpStream>>,
}

impl SqlServerClient {
    async fn new(msconfig: &SqlServerConfig) -> Result<Self> {
        let mut config = Config::new();
        config.host(&msconfig.host);
        config.port(msconfig.port);
        config.authentication(AuthMethod::sql_server(&msconfig.user, &msconfig.password));
        config.database(&msconfig.database);
        config.trust_cert();
        Self::new_with_config(config).await
    }

    pub async fn new_with_config(mut config: Config) -> Result<Self> {
        let tcp = TcpStream::connect(config.get_addr())
            .await
            .context("failed to connect to sql server")
            .map_err(SinkError::SqlServer)?;
        tcp.set_nodelay(true)
            .context("failed to setting nodelay when connecting to sql server")
            .map_err(SinkError::SqlServer)?;

        let client = match Client::connect(config.clone(), tcp.compat_write()).await {
            // Connection successful.
            Ok(client) => client,
            // The server wants us to redirect to a different address
            Err(tiberius::error::Error::Routing { host, port }) => {
                config.host(&host);
                config.port(port);
                let tcp = TcpStream::connect(config.get_addr())
                    .await
                    .context("failed to connect to sql server after routing")
                    .map_err(SinkError::SqlServer)?;
                tcp.set_nodelay(true)
                    .context(
                        "failed to setting nodelay when connecting to sql server after routing",
                    )
                    .map_err(SinkError::SqlServer)?;
                // we should not have more than one redirect, so we'll short-circuit here.
                Client::connect(config, tcp.compat_write()).await?
            }
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            inner_client: client,
        })
    }
}

fn bind_params(
    query: &mut Query<'_>,
    row: impl Row,
    schema: &Schema,
    col_indices: impl Iterator<Item = usize>,
) -> Result<()> {
    use risingwave_common::types::ScalarRefImpl;
    for col_idx in col_indices {
        match row.datum_at(col_idx) {
            Some(data_ref) => match data_ref {
                ScalarRefImpl::Int16(v) => query.bind(v),
                ScalarRefImpl::Int32(v) => query.bind(v),
                ScalarRefImpl::Int64(v) => query.bind(v),
                ScalarRefImpl::Float32(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Float64(v) => query.bind(v.into_inner()),
                ScalarRefImpl::Utf8(v) => query.bind(v.to_owned()),
                ScalarRefImpl::Bool(v) => query.bind(v),
                ScalarRefImpl::Decimal(v) => match v {
                    Decimal::Normalized(d) => {
                        query.bind(decimal_to_sql(&d));
                    }
                    Decimal::NaN | Decimal::PositiveInf | Decimal::NegativeInf => {
                        tracing::warn!(
                            "Inf, -Inf, Nan in RisingWave decimal is converted into SQL Server null!"
                        );
                        query.bind(None as Option<Numeric>);
                    }
                },
                ScalarRefImpl::Date(v) => query.bind(v.0),
                ScalarRefImpl::Timestamp(v) => query.bind(v.0),
                ScalarRefImpl::TimestampNanosecond(v) => query.bind(v.0),
                ScalarRefImpl::Timestamptz(v) => query.bind(v.timestamp_micros()),
                ScalarRefImpl::Time(v) => query.bind(v.0),
                ScalarRefImpl::Bytea(v) => query.bind(v.to_vec()),
                ScalarRefImpl::Interval(_) => return Err(data_type_not_supported("Interval")),
                ScalarRefImpl::Jsonb(_) => return Err(data_type_not_supported("Jsonb")),
                ScalarRefImpl::Struct(_) => return Err(data_type_not_supported("Struct")),
                ScalarRefImpl::List(_) => return Err(data_type_not_supported("List")),
                ScalarRefImpl::Int256(_) => return Err(data_type_not_supported("Int256")),
                ScalarRefImpl::Serial(_) => return Err(data_type_not_supported("Serial")),
                ScalarRefImpl::Map(_) => return Err(data_type_not_supported("Map")),
            },
            None => match schema[col_idx].data_type {
                DataType::Boolean => {
                    query.bind(None as Option<bool>);
                }
                DataType::Int16 => {
                    query.bind(None as Option<i16>);
                }
                DataType::Int32 => {
                    query.bind(None as Option<i32>);
                }
                DataType::Int64 => {
                    query.bind(None as Option<i64>);
                }
                DataType::Float32 => {
                    query.bind(None as Option<f32>);
                }
                DataType::Float64 => {
                    query.bind(None as Option<f64>);
                }
                DataType::Decimal => {
                    query.bind(None as Option<Numeric>);
                }
                DataType::Date => {
                    query.bind(None as Option<chrono::NaiveDate>);
                }
                DataType::Time => {
                    query.bind(None as Option<chrono::NaiveTime>);
                }
                DataType::Timestamp | DataType::TimestampNanosecond => {
                    query.bind(None as Option<chrono::NaiveDateTime>);
                }
                DataType::Timestamptz => {
                    query.bind(None as Option<i64>);
                }
                DataType::Varchar => {
                    query.bind(None as Option<String>);
                }
                DataType::Bytea => {
                    query.bind(None as Option<Vec<u8>>);
                }
                DataType::Interval => return Err(data_type_not_supported("Interval")),
                DataType::Struct(_) => return Err(data_type_not_supported("Struct")),
                DataType::List(_) => return Err(data_type_not_supported("List")),
                DataType::Jsonb => return Err(data_type_not_supported("Jsonb")),
                DataType::Serial => return Err(data_type_not_supported("Serial")),
                DataType::Int256 => return Err(data_type_not_supported("Int256")),
                DataType::Map(_) => return Err(data_type_not_supported("Map")),
            },
        };
    }
    Ok(())
}

fn data_type_not_supported(data_type_name: &str) -> SinkError {
    SinkError::SqlServer(anyhow!(format!(
        "{data_type_name} is not supported in SQL Server"
    )))
}

fn check_data_type_compatibility(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal
        | DataType::Date
        | DataType::Varchar
        | DataType::Time
        | DataType::Timestamp
        | DataType::TimestampNanosecond
        | DataType::Timestamptz
        | DataType::Bytea => Ok(()),
        DataType::Interval => Err(data_type_not_supported("Interval")),
        DataType::Struct(_) => Err(data_type_not_supported("Struct")),
        DataType::List(_) => Err(data_type_not_supported("List")),
        DataType::Jsonb => Err(data_type_not_supported("Jsonb")),
        DataType::Serial => Err(data_type_not_supported("Serial")),
        DataType::Int256 => Err(data_type_not_supported("Int256")),
        DataType::Map(_) => Err(data_type_not_supported("Map")),
    }
}

/// The implementation is copied from tiberius crate.
fn decimal_to_sql(decimal: &rust_decimal::Decimal) -> Numeric {
    let unpacked = decimal.unpack();

    let mut value = (((unpacked.hi as u128) << 64)
        + ((unpacked.mid as u128) << 32)
        + unpacked.lo as u128) as i128;

    if decimal.is_sign_negative() {
        value = -value;
    }

    Numeric::new_with_scale(value, decimal.scale() as u8)
}
