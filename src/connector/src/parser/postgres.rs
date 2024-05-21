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

use std::sync::LazyLock;

use risingwave_common::catalog::Schema;
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Decimal, ListValue, ScalarImpl};
use thiserror_ext::AsReport;
use tokio_postgres::types::{Kind, Type};

use crate::parser::scalar_adapter::ScalarAdapter;
use crate::parser::util::log_error;

static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);

macro_rules! handle_list_data_type {
    ($row:expr, $i:expr, $name:expr, $dtype:expr, $type:ty) => {{
        let res = $row.try_get::<_, Option<Vec<Option<$type>>>>($i);
        match res {
            Ok(val) => val.map(|v| {
                let mut builder = $dtype.create_array_builder(0);
                v.into_iter()
                    .for_each(|val| builder.append(val.map(ScalarImpl::from)));
                ScalarImpl::from(ListValue::new(builder.finish()))
            }),
            Err(err) => {
                log_error!($name, err, "parse column failed");
                None
            }
        }
    }};
}

macro_rules! handle_data_type {
    ($row:expr, $i:expr, $name:expr, $type:ty) => {{
        let res = $row.try_get::<_, Option<$type>>($i);
        match res {
            Ok(val) => val.map(|v| ScalarImpl::from(v)),
            Err(err) => {
                log_error!($name, err, "parse column failed");
                None
            }
        }
    }};
}

pub fn postgres_row_to_owned_row(row: tokio_postgres::Row, schema: &Schema) -> OwnedRow {
    let mut datums = vec![];
    for i in 0..schema.fields.len() {
        let rw_field = &schema.fields[i];
        let name = rw_field.name.as_str();
        let datum = postgres_cell_to_scalar_impl(&row, &rw_field.data_type, i, name);
        datums.push(datum);
    }
    OwnedRow::new(datums)
}

fn postgres_cell_to_scalar_impl(
    row: &tokio_postgres::Row,
    data_type: &DataType,
    i: usize,
    name: &str,
) -> Option<ScalarImpl> {
    // We observe several incompatibility issue in Debezium's Postgres connector. We summarize them here:
    // Issue #1. The null of enum list is not supported in Debezium. An enum list contains `NULL` will fallback to `NULL`.
    // Issue #2. In our parser, when there's inf, -inf, nan or invalid item in a list, the whole list will fallback null.
    match data_type {
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
        | DataType::Bytea
        | DataType::Int256 => {
            let res = row.try_get::<_, Option<ScalarImpl>>(i);
            match res {
                Ok(val) => val,
                Err(err) => {
                    log_error!(name, err, "parse column failed");
                    None
                }
            }
        }
        DataType::Decimal => {
            handle_data_type!(row, i, name, Decimal)
        }
        DataType::Varchar => {
            let res = row.try_get::<_, Option<ScalarAdapter>>(i);
            match res {
                Ok(val) => val.and_then(|v| v.into_scalar(data_type)),
                Err(err) => {
                    log_error!(name, err, "parse column failed");
                    None
                }
            }
        }
        DataType::List(dtype) => {
            // enum list needs to be handled separately
            if let Kind::Array(item_type) = row.columns()[i].type_().kind()
                && let Kind::Enum(_) = item_type.kind()
            {
                // Issue #1, we use ScalarAdaptertead of Option<ScalarAdapter
                let res = row.try_get::<_, Option<Vec<ScalarAdapter>>>(i);
                match res {
                    Ok(val) => val.map(|val| {
                        let mut builder = dtype.create_array_builder(0);
                        val.into_iter()
                            .for_each(|v| builder.append(v.into_scalar(&DataType::Varchar)));
                        ScalarImpl::from(ListValue::new(builder.finish()))
                    }),
                    Err(err) => {
                        log_error!(name, err, "parse enum column failed");
                        None
                    }
                }
            } else {
                match **dtype {
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
                    | DataType::Bytea
                    | DataType::Int256
                    | DataType::Decimal
                    | DataType::Varchar => {
                        let res = row.try_get::<_, Option<ScalarAdapter>>(i);
                        match res {
                            Ok(val) => val.and_then(|v| v.into_scalar(data_type)),
                            Err(err) => {
                                log_error!(name, err, "parse list column failed");
                                None
                            }
                        }
                    }
                    DataType::Struct(_) | DataType::List(_) | DataType::Serial => {
                        tracing::warn!(
                            "unsupported List data type {:?}, set the List to empty",
                            **dtype
                        );
                        None
                    }
                }
            }
        }
        DataType::Struct(_) | DataType::Serial => {
            // Interval and Struct are not supported
            tracing::warn!(name, ?data_type, "unsupported data type, set to null");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio_postgres::NoTls;

    use crate::parser::scalar_adapter::EnumString;
    const DB: &str = "postgres";
    const USER: &str = "kexiang";

    #[ignore]
    #[tokio::test]
    async fn enum_string_integration_test() {
        let connect = format!(
            "host=localhost port=5432 user={} password={} dbname={}",
            USER, DB, DB
        );
        let (client, connection) = tokio_postgres::connect(connect.as_str(), NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // allow type existed
        let _ = client
            .execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')", &[])
            .await;
        client
            .execute(
                "CREATE TABLE IF NOT EXISTS person(id int PRIMARY KEY, current_mood mood)",
                &[],
            )
            .await
            .unwrap();
        client.execute("DELETE FROM person;", &[]).await.unwrap();
        client
            .execute("INSERT INTO person VALUES (1, 'happy')", &[])
            .await
            .unwrap();

        // test from_sql
        let got: EnumString = client
            .query_one("SELECT * FROM person", &[])
            .await
            .unwrap()
            .get::<usize, Option<EnumString>>(1)
            .unwrap();
        assert_eq!("happy", got.0.as_str());

        client.execute("DELETE FROM person", &[]).await.unwrap();

        // test to_sql
        client
            .execute("INSERT INTO person VALUES (2, $1)", &[&got])
            .await
            .unwrap();

        let got_new: EnumString = client
            .query_one("SELECT * FROM person", &[])
            .await
            .unwrap()
            .get::<usize, Option<EnumString>>(1)
            .unwrap();
        assert_eq!("happy", got_new.0.as_str());
        client.execute("DROP TABLE person", &[]).await.unwrap();
        client.execute("DROP TYPE mood", &[]).await.unwrap();
    }
}
