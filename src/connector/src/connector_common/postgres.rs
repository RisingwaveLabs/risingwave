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

use std::collections::HashMap;
use std::fmt;

use anyhow::anyhow;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use risingwave_common::bail;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::types::{DataType, ScalarImpl, StructType};
use sea_schema::postgres::def::{ColumnType, TableInfo, Type as SeaType};
use sea_schema::postgres::discovery::SchemaDiscovery;
use serde_derive::Deserialize;
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use sqlx::PgPool;
use thiserror_ext::AsReport;
use tokio_postgres::types::Kind as PgKind;
use tokio_postgres::{Client as PgClient, NoTls};

#[cfg(not(madsim))]
use super::maybe_tls_connector::MaybeMakeTlsConnector;
use crate::error::ConnectorResult;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    #[serde(alias = "disable")]
    Disabled,
    #[serde(alias = "prefer")]
    Preferred,
    #[serde(alias = "require")]
    Required,
    /// verify that the server is trustworthy by checking the certificate chain
    /// up to the root certificate stored on the client.
    #[serde(alias = "verify-ca")]
    VerifyCa,
    /// Besides verify the certificate, will also verify that the serverhost name
    /// matches the name stored in the server certificate.
    #[serde(alias = "verify-full")]
    VerifyFull,
}

impl Default for SslMode {
    fn default() -> Self {
        Self::Preferred
    }
}

pub struct PostgresExternalTable {
    column_descs: Vec<ColumnDesc>,
    pk_names: Vec<String>,
    column_name_to_pg_type: HashMap<String, tokio_postgres::types::Type>,
}

impl PostgresExternalTable {
    pub async fn connect(
        username: &str,
        password: &str,
        host: &str,
        port: u16,
        database: &str,
        schema: &str,
        table: &str,
        ssl_mode: &SslMode,
        ssl_root_cert: &Option<String>,
        is_append_only: bool,
    ) -> ConnectorResult<Self> {
        tracing::debug!("connect to postgres external table");
        let mut options = PgConnectOptions::new()
            .username(username)
            .password(password)
            .host(host)
            .port(port)
            .database(database)
            .ssl_mode(match ssl_mode {
                SslMode::Disabled => PgSslMode::Disable,
                SslMode::Preferred => PgSslMode::Prefer,
                SslMode::Required => PgSslMode::Require,
                SslMode::VerifyCa => PgSslMode::VerifyCa,
                SslMode::VerifyFull => PgSslMode::VerifyFull,
            });

        if *ssl_mode == SslMode::VerifyCa || *ssl_mode == SslMode::VerifyFull {
            if let Some(ref root_cert) = ssl_root_cert {
                options = options.ssl_root_cert(root_cert.as_str());
            }
        }

        let connection = PgPool::connect_with(options).await?;
        let schema_discovery = SchemaDiscovery::new(connection, schema);
        // fetch column schema and primary key
        let empty_map = HashMap::new();
        let table_schema = schema_discovery
            .discover_table(
                TableInfo {
                    name: table.to_owned(),
                    of_type: None,
                },
                &empty_map,
            )
            .await?;

        let mut column_name_to_pg_type = HashMap::new();
        let mut column_descs = vec![];
        for col in &table_schema.columns {
            let data_type = type_to_rw_type(&col.col_type)?;
            let column_desc = if let Some(ref default_expr) = col.default {
                // parse the value of "column_default" field in information_schema.columns,
                // non number data type will be stored as "'value'::type"
                let val_text = default_expr
                    .0
                    .split("::")
                    .map(|s| s.trim_matches('\''))
                    .next()
                    .expect("default value expression");

                match ScalarImpl::from_text(val_text, &data_type) {
                    Ok(scalar) => ColumnDesc::named_with_default_value(
                        col.name.clone(),
                        ColumnId::placeholder(),
                        data_type.clone(),
                        Some(scalar),
                    ),
                    Err(err) => {
                        tracing::warn!(error=%err.as_report(), "failed to parse postgres default value expression, only constant is supported");
                        ColumnDesc::named(col.name.clone(), ColumnId::placeholder(), data_type)
                    }
                }
            } else {
                ColumnDesc::named(col.name.clone(), ColumnId::placeholder(), data_type)
            };
            {
                let pg_type = Self::discovered_type_to_pg_type(&col.col_type)?;
                column_name_to_pg_type.insert(col.name.clone(), pg_type);
            }
            column_descs.push(column_desc);
        }

        if !is_append_only && table_schema.primary_key_constraints.is_empty() {
            return Err(anyhow!(
                "Postgres table should define the primary key for non-append-only tables"
            )
            .into());
        }
        let mut pk_names = vec![];
        table_schema.primary_key_constraints.iter().for_each(|pk| {
            pk_names.extend(pk.columns.clone());
        });

        Ok(Self {
            column_descs,
            pk_names,
            column_name_to_pg_type,
        })
    }

    pub fn column_descs(&self) -> &Vec<ColumnDesc> {
        &self.column_descs
    }

    pub fn pk_names(&self) -> &Vec<String> {
        &self.pk_names
    }

    pub fn column_name_to_pg_type(&self) -> &HashMap<String, tokio_postgres::types::Type> {
        &self.column_name_to_pg_type
    }

    // We use `sea-schema` for table schema discovery.
    // So we have to map `sea-schema` pg types
    // to `tokio-postgres` pg types (which we use for query binding).
    fn discovered_type_to_pg_type(
        discovered_type: &SeaType,
    ) -> anyhow::Result<tokio_postgres::types::Type> {
        use tokio_postgres::types::Type as PgType;
        match discovered_type {
            SeaType::SmallInt => Ok(PgType::INT2),
            SeaType::Integer => Ok(PgType::INT4),
            SeaType::BigInt => Ok(PgType::INT8),
            SeaType::Decimal(_) => Ok(PgType::NUMERIC),
            SeaType::Numeric(_) => Ok(PgType::NUMERIC),
            SeaType::Real => Ok(PgType::FLOAT4),
            SeaType::DoublePrecision => Ok(PgType::FLOAT8),
            SeaType::Varchar(_) => Ok(PgType::VARCHAR),
            SeaType::Char(_) => Ok(PgType::CHAR),
            SeaType::Text => Ok(PgType::TEXT),
            SeaType::Bytea => Ok(PgType::BYTEA),
            SeaType::Timestamp(_) => Ok(PgType::TIMESTAMP),
            SeaType::TimestampWithTimeZone(_) => Ok(PgType::TIMESTAMPTZ),
            SeaType::Date => Ok(PgType::DATE),
            SeaType::Time(_) => Ok(PgType::TIME),
            SeaType::TimeWithTimeZone(_) => Ok(PgType::TIMETZ),
            SeaType::Interval(_) => Ok(PgType::INTERVAL),
            SeaType::Boolean => Ok(PgType::BOOL),
            SeaType::Point => Ok(PgType::POINT),
            SeaType::Uuid => Ok(PgType::UUID),
            SeaType::Json | SeaType::JsonBinary => Ok(PgType::JSONB),
            SeaType::Array(t) => {
                let Some(t) = t.col_type.as_ref() else {
                    bail!("missing array type")
                };
                match t.as_ref() {
                    // RW only supports 1 level of nesting.
                    SeaType::SmallInt => Ok(PgType::INT2_ARRAY),
                    SeaType::Integer => Ok(PgType::INT4_ARRAY),
                    SeaType::BigInt => Ok(PgType::INT8_ARRAY),
                    SeaType::Decimal(_) => Ok(PgType::NUMERIC_ARRAY),
                    SeaType::Numeric(_) => Ok(PgType::NUMERIC_ARRAY),
                    SeaType::Real => Ok(PgType::FLOAT4_ARRAY),
                    SeaType::DoublePrecision => Ok(PgType::FLOAT8_ARRAY),
                    SeaType::Varchar(_) => Ok(PgType::VARCHAR_ARRAY),
                    SeaType::Char(_) => Ok(PgType::CHAR_ARRAY),
                    SeaType::Text => Ok(PgType::TEXT_ARRAY),
                    SeaType::Bytea => Ok(PgType::BYTEA_ARRAY),
                    SeaType::Timestamp(_) => Ok(PgType::TIMESTAMP_ARRAY),
                    SeaType::TimestampWithTimeZone(_) => Ok(PgType::TIMESTAMPTZ_ARRAY),
                    SeaType::Date => Ok(PgType::DATE_ARRAY),
                    SeaType::Time(_) => Ok(PgType::TIME_ARRAY),
                    SeaType::TimeWithTimeZone(_) => Ok(PgType::TIMETZ_ARRAY),
                    SeaType::Interval(_) => Ok(PgType::INTERVAL_ARRAY),
                    SeaType::Boolean => Ok(PgType::BOOL_ARRAY),
                    SeaType::Point => Ok(PgType::POINT_ARRAY),
                    SeaType::Uuid => Ok(PgType::UUID_ARRAY),
                    SeaType::Json | SeaType::JsonBinary => Ok(PgType::JSONB_ARRAY),
                    SeaType::Array(_) => bail!("nested array type is not supported"),
                    SeaType::Unknown(name) => {
                        // Treat as enum type
                        Ok(PgType::new(
                            name.clone(),
                            0,
                            PgKind::Array(PgType::new(
                                name.clone(),
                                0,
                                PgKind::Enum(vec![]),
                                "".into(),
                            )),
                            "".into(),
                        ))
                    }
                    _ => bail!("unsupported array type: {:?}", t),
                }
            }
            SeaType::Unknown(name) => {
                // Treat as enum type
                Ok(PgType::new(
                    name.clone(),
                    0,
                    PgKind::Enum(vec![]),
                    "".into(),
                ))
            }
            _ => bail!("unsupported type: {:?}", discovered_type),
        }
    }
}

impl fmt::Display for SslMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            SslMode::Disabled => "disabled",
            SslMode::Preferred => "preferred",
            SslMode::Required => "required",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        })
    }
}

pub async fn create_pg_client(
    user: &str,
    password: &str,
    host: &str,
    port: &str,
    database: &str,
    ssl_mode: &SslMode,
    ssl_root_cert: &Option<String>,
) -> anyhow::Result<PgClient> {
    let mut pg_config = tokio_postgres::Config::new();
    pg_config
        .user(user)
        .password(password)
        .host(host)
        .port(port.parse::<u16>().unwrap())
        .dbname(database);

    let (_verify_ca, verify_hostname) = match ssl_mode {
        SslMode::VerifyCa => (true, false),
        SslMode::VerifyFull => (true, true),
        _ => (false, false),
    };

    #[cfg(not(madsim))]
    let connector = match ssl_mode {
        SslMode::Disabled => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            MaybeMakeTlsConnector::NoTls(NoTls)
        }
        SslMode::Preferred => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Prefer);
            match SslConnector::builder(SslMethod::tls()) {
                Ok(mut builder) => {
                    // disable certificate verification for `prefer`
                    builder.set_verify(SslVerifyMode::NONE);
                    MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "SSL connector error");
                    MaybeMakeTlsConnector::NoTls(NoTls)
                }
            }
        }
        SslMode::Required => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            // disable certificate verification for `require`
            builder.set_verify(SslVerifyMode::NONE);
            MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
        }

        SslMode::VerifyCa | SslMode::VerifyFull => {
            pg_config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let mut builder = SslConnector::builder(SslMethod::tls())?;
            if let Some(ssl_root_cert) = ssl_root_cert {
                builder.set_ca_file(ssl_root_cert).map_err(|e| {
                    anyhow!(format!("bad ssl root cert error: {}", e.to_report_string()))
                })?;
            }
            let mut connector = MakeTlsConnector::new(builder.build());
            if !verify_hostname {
                connector.set_callback(|config, _| {
                    config.set_verify_hostname(false);
                    Ok(())
                });
            }
            MaybeMakeTlsConnector::Tls(connector)
        }
    };
    #[cfg(madsim)]
    let connector = NoTls;

    let (client, connection) = pg_config.connect(connector).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e.as_report(), "postgres connection error");
        }
    });

    Ok(client)
}

pub fn type_to_rw_type(col_type: &ColumnType) -> ConnectorResult<DataType> {
    let dtype = match col_type {
        ColumnType::SmallInt | ColumnType::SmallSerial => DataType::Int16,
        ColumnType::Integer | ColumnType::Serial => DataType::Int32,
        ColumnType::BigInt | ColumnType::BigSerial => DataType::Int64,
        ColumnType::Money | ColumnType::Decimal(_) | ColumnType::Numeric(_) => DataType::Decimal,
        ColumnType::Real => DataType::Float32,
        ColumnType::DoublePrecision => DataType::Float64,
        ColumnType::Varchar(_) | ColumnType::Char(_) | ColumnType::Text => DataType::Varchar,
        ColumnType::Bytea => DataType::Bytea,
        ColumnType::Timestamp(_) => DataType::Timestamp,
        ColumnType::TimestampWithTimeZone(_) => DataType::Timestamptz,
        ColumnType::Date => DataType::Date,
        ColumnType::Time(_) | ColumnType::TimeWithTimeZone(_) => DataType::Time,
        ColumnType::Interval(_) => DataType::Interval,
        ColumnType::Boolean => DataType::Boolean,
        ColumnType::Point => DataType::Struct(StructType::new(vec![
            ("x", DataType::Float32),
            ("y", DataType::Float32),
        ])),
        ColumnType::Uuid => DataType::Varchar,
        ColumnType::Xml => DataType::Varchar,
        ColumnType::Json => DataType::Jsonb,
        ColumnType::JsonBinary => DataType::Jsonb,
        ColumnType::Array(def) => {
            let item_type = match def.col_type.as_ref() {
                Some(ty) => type_to_rw_type(ty.as_ref())?,
                None => {
                    return Err(anyhow!("ARRAY type missing element type").into());
                }
            };

            DataType::List(Box::new(item_type))
        }
        ColumnType::PgLsn => DataType::Int64,
        ColumnType::Cidr
        | ColumnType::Inet
        | ColumnType::MacAddr
        | ColumnType::MacAddr8
        | ColumnType::Int4Range
        | ColumnType::Int8Range
        | ColumnType::NumRange
        | ColumnType::TsRange
        | ColumnType::TsTzRange
        | ColumnType::DateRange
        | ColumnType::Enum(_) => DataType::Varchar,

        ColumnType::Line => {
            return Err(anyhow!("LINE type not supported").into());
        }
        ColumnType::Lseg => {
            return Err(anyhow!("LSEG type not supported").into());
        }
        ColumnType::Box => {
            return Err(anyhow!("BOX type not supported").into());
        }
        ColumnType::Path => {
            return Err(anyhow!("PATH type not supported").into());
        }
        ColumnType::Polygon => {
            return Err(anyhow!("POLYGON type not supported").into());
        }
        ColumnType::Circle => {
            return Err(anyhow!("CIRCLE type not supported").into());
        }
        ColumnType::Bit(_) => {
            return Err(anyhow!("BIT type not supported").into());
        }
        ColumnType::VarBit(_) => {
            return Err(anyhow!("VARBIT type not supported").into());
        }
        ColumnType::TsVector => {
            return Err(anyhow!("TSVECTOR type not supported").into());
        }
        ColumnType::TsQuery => {
            return Err(anyhow!("TSQUERY type not supported").into());
        }
        ColumnType::Unknown(name) => {
            // NOTES: user-defined enum type is classified as `Unknown`
            tracing::warn!("Unknown Postgres data type: {name}, map to varchar");
            DataType::Varchar
        }
    };

    Ok(dtype)
}
