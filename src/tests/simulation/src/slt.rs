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

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use rand::{thread_rng, Rng, SeedableRng};
use rand_chacha::ChaChaRng;
use sqllogictest::{DefaultColumnType, ParallelTestError, Record};

use crate::client::RisingWave;
use crate::cluster::{Cluster, KillOpts};
use crate::utils::TimedExt;

fn is_create_table_as(sql: &str) -> bool {
    let parts: Vec<String> = sql.split_whitespace().map(|s| s.to_lowercase()).collect();

    parts.len() >= 4 && parts[0] == "create" && parts[1] == "table" && parts[3] == "as"
}

#[derive(Debug, PartialEq, Eq)]
enum SqlCmd {
    /// Other create statements.
    Create {
        is_create_table_as: bool,
    },
    /// Create Materialized views
    CreateMaterializedView {
        name: String,
    },
    /// Set background ddl
    SetBackgroundDdl {
        enable: bool,
    },
    Drop,
    Dml,
    Flush,
    Alter,
    Others,
}

impl SqlCmd {
    // We won't kill during insert/update/delete/alter since the atomicity is not guaranteed.
    // Notice that `create table as` is also not atomic in our system.
    // TODO: For `SqlCmd::Alter`, since table fragment and catalog commit for table schema change
    // are not transactional, we can't kill during `alter table add/drop columns` for now, will
    // remove it until transactional commit of table fragment and catalog is supported.
    fn ignore_kill(&self) -> bool {
        matches!(
            self,
            SqlCmd::Dml
                | SqlCmd::Flush
                | SqlCmd::Alter
                | SqlCmd::Create {
                    is_create_table_as: true
                }
        )
    }
}

fn extract_sql_command(sql: &str) -> SqlCmd {
    let sql = sql.to_lowercase();
    let tokens = sql.split_whitespace();
    let mut tokens = tokens.multipeek();
    let first_token = tokens.next().unwrap_or("");

    match first_token {
        // Note(kwannoel):
        // It's entirely possible for a malformed command to be parsed as a `create table as`
        // BUT an error should be expected for such a test.
        // So it should still succeed.
        "create" => {
            let result: Option<SqlCmd> = try {
                match tokens.next()? {
                    "materialized" => {
                        // view
                        tokens.next()?;

                        // if not exists | name
                        let next = *tokens.peek()?;
                        if "if" == next
                            && let Some("not") = tokens.peek().cloned()
                            && let Some("exists") = tokens.peek().cloned() {
                            tokens.next();
                            tokens.next();
                            tokens.next();
                            let name = tokens.next()?.to_string();
                            SqlCmd::CreateMaterializedView { name }
                        } else {
                            let name = next.to_string();
                            SqlCmd::CreateMaterializedView { name }
                        }
                    }
                    _ => SqlCmd::Create {
                        is_create_table_as: is_create_table_as(&sql),
                    },
                }
            };
            result.unwrap_or(SqlCmd::Others)
        }
        "set" => {
            if sql.contains("background_ddl") {
                let enable = sql.contains("true");
                SqlCmd::SetBackgroundDdl { enable }
            } else {
                SqlCmd::Others
            }
        }
        "drop" => SqlCmd::Drop,
        "insert" | "update" | "delete" => SqlCmd::Dml,
        "flush" => SqlCmd::Flush,
        "alter" => SqlCmd::Alter,
        _ => SqlCmd::Others,
    }
}

const KILL_IGNORE_FILES: &[&str] = &[
    // TPCH queries are too slow for recovery.
    "tpch_snapshot.slt",
    "tpch_upstream.slt",
    // Drop is not retryable in search path test.
    "search_path.slt",
    // Transaction statements are not retryable.
    "transaction/now.slt",
    "transaction/read_only_multi_conn.slt",
    "transaction/read_only.slt",
    "transaction/tolerance.slt",
];

/// Run the sqllogictest files in `glob`.
pub async fn run_slt_task(
    cluster: Arc<Cluster>,
    glob: &str,
    opts: &KillOpts,
    // Probability of background_ddl being set to true per ddl record.
    background_ddl_weight: f64,
) {
    tracing::info!("background_ddl_weight: {}", background_ddl_weight);
    let seed = std::env::var("MADSIM_TEST_SEED")
        .unwrap_or("0".to_string())
        .parse::<u64>()
        .unwrap();
    let mut rng = ChaChaRng::seed_from_u64(seed);
    let kill = opts.kill_compute || opts.kill_meta || opts.kill_frontend || opts.kill_compactor;
    let files = glob::glob(glob).expect("failed to read glob pattern");
    for file in files {
        // use a session per file
        let mut tester =
            sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));

        let file = file.unwrap();
        let path = file.as_path();
        println!("{}", path.display());
        if kill && KILL_IGNORE_FILES.iter().any(|s| path.ends_with(s)) {
            continue;
        }
        // XXX: hack for kafka source test
        let tempfile = (path.ends_with("kafka.slt") || path.ends_with("kafka_batch.slt"))
            .then(|| hack_kafka_test(path));
        let path = tempfile.as_ref().map(|p| p.path()).unwrap_or(path);

        // NOTE(kwannoel): For background ddl
        let mut background_ddl_enabled = false;

        for record in sqllogictest::parse_file(path).expect("failed to parse file") {
            // uncomment to print metrics for task counts
            // let metrics = madsim::runtime::Handle::current().metrics();
            // println!("{:#?}", metrics);
            // println!("{}", metrics.num_tasks_by_node_by_spawn());
            if let sqllogictest::Record::Halt { .. } = record {
                break;
            }

            // For normal records.
            if !kill {
                match tester
                    .run_async(record.clone())
                    .timed(|_res, elapsed| {
                        tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                    })
                    .await
                {
                    Ok(_) => continue,
                    Err(e) => panic!("{}", e),
                }
            }

            // For kill enabled.
            let cmd = match &record {
                sqllogictest::Record::Statement { sql, .. }
                | sqllogictest::Record::Query { sql, .. } => extract_sql_command(sql),
                _ => SqlCmd::Others,
            };
            tracing::debug!(?cmd, "Running");

            if matches!(cmd, SqlCmd::SetBackgroundDdl { .. }) && background_ddl_weight > 0.0 {
                panic!("We cannot run background_ddl statement with background_ddl_weight > 0.0, since it could be reset");
            }

            // Our background ddl strategy is as follows:
            // 1. For each background ddl compatible statement, provide a chance for background_ddl=true.
            // 2. Then later reset background_ddl after the statement is executed.
            let reset_background_ddl_record = if let Record::Statement {
                loc,
                conditions,
                connection,
                ..
            } = &record
                && matches!(cmd, SqlCmd::CreateMaterializedView { .. })
                && rng.gen_bool(background_ddl_weight) {
                let enable_background_ddl = Record::Statement {
                    loc: loc.clone(),
                    conditions: conditions.clone(),
                    connection: connection.clone(),
                    expected_error: None,
                    sql: "SET BACKGROUND_DDL=true;".to_string(),
                    expected_count: None,
                };
                let disable_background_ddl: Record<DefaultColumnType> = Record::Statement {
                    loc: loc.clone(),
                    conditions: conditions.clone(),
                    connection: connection.clone(),
                    expected_error: None,
                    sql: "SET BACKGROUND_DDL=false;".to_string(),
                    expected_count: None,
                };
                tester.run_async(enable_background_ddl).await.unwrap();
                background_ddl_enabled = true;
                Some(disable_background_ddl)
            } else {
                None
            };

            if cmd.ignore_kill() {
                for i in 0usize.. {
                    let delay = Duration::from_secs(1 << i);
                    if let Err(err) = tester
                        .run_async(record.clone())
                        .timed(|_res, elapsed| {
                            tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                        })
                        .await
                    {
                        // cluster could be still under recovering if killed before, retry if
                        // meets `no reader for dml in table with id {}`.
                        let should_retry =
                            err.to_string().contains("no reader for dml in table") && i < 5;
                        if !should_retry {
                            panic!("{}", err);
                        }
                        tracing::error!("failed to run test: {err}\nretry after {delay:?}");
                    } else {
                        break;
                    }
                    tokio::time::sleep(delay).await;
                }
                continue;
            }

            let should_kill = thread_rng().gen_bool(opts.kill_rate as f64);
            // spawn a background task to kill nodes
            let handle = if should_kill {
                let cluster = cluster.clone();
                let opts = *opts;
                Some(tokio::spawn(async move {
                    let t = thread_rng().gen_range(Duration::default()..Duration::from_secs(1));
                    tokio::time::sleep(t).await;
                    cluster.kill_node(&opts).await;
                    tokio::time::sleep(Duration::from_secs(15)).await;
                }))
            } else {
                None
            };

            // retry up to 5 times until it succeed,
            // unless it's CREATE MATERIALIZED VIEW (in background).
            // For CREATE MATERIALIZED VIEW, we retry up to 10 times since it needs to:
            // 1. Wait for recovery to complete if cluster goes down.
            // 2. Wait for background ddl to finish.
            // Other cases just need to wait for 1., so they are faster to complete.
            let max_retry =
                if matches!(cmd, SqlCmd::CreateMaterializedView { .. }) && background_ddl_enabled {
                    10
                } else {
                    5
                };
            for i in 0usize.. {
                tracing::debug!(iteration = i, "retry count");
                let delay = Duration::from_secs(1 << i);
                match tester
                    .run_async(record.clone())
                    .timed(|_res, elapsed| {
                        tracing::debug!("Record {:?} finished in {:?}", record, elapsed)
                    })
                    .await
                {
                    Ok(_) => {
                        tracing::debug!(iteration = i, "retry count (OK)");
                        // For background ddl
                        if let SqlCmd::CreateMaterializedView { ref name } = cmd && background_ddl_enabled
                            && matches!(record, Record::Statement { expected_error: None, .. } | Record::Query { expected_error: None, ..})
                        {
                            tracing::debug!(iteration=i, "Retry for background ddl");
                            // wait for background ddl to finish and succeed.
                            let Ok(rw) =
                                RisingWave::connect("frontend".into(), "dev".into()).await
                            else {
                                tracing::debug!(iteration=i, name, "failed to run test: background_mv not created, retry after {delay:?}");
                                if i >= max_retry {
                                    panic!("failed to run test after retry {i} times, while waiting for background ddl");
                                }
                                continue;
                            };
                            let client = rw.pg_client();
                            if client.simple_query("WAIT;").await.is_ok()
                                && let Ok(result) = client
                                    .query(
                                        "select count(*) from pg_matviews where matviewname=$1;",
                                        &[&name],
                                    )
                                    .await
                                && let Ok(1) = result[0].try_get::<_, i64>(0)
                            {
                                if let Some(record) = reset_background_ddl_record {
                                    tracing::debug!("Record with background_ddl {:?} finished", record);
                                    if tester.run_async(record).await.is_ok() {
                                        // Try to reset, but if the cluster killed again before reset,
                                        // it is fine, we can leave it to the next mview to reset it.
                                        background_ddl_enabled = false;
                                    }
                                }
                                tracing::debug!("Record with background_ddl {:?} finished", record);
                                break;
                            }
                            // If fail, recreate mv again.
                            tracing::debug!(iteration=i, name, "failed to run test: background_mv not created, retry after {delay:?}");
                            if i >= max_retry {
                                panic!("failed to run test after retry {i} times, while waiting for background ddl");
                            }
                            continue;
                        }
                        break;
                    }
                    Err(e) => {
                        tracing::debug!(iteration = i, "retry count (ERR)");
                        match cmd {
                            // allow 'table exists' error when retry CREATE statement
                            SqlCmd::Create {
                                is_create_table_as: false,
                            }
                            | SqlCmd::CreateMaterializedView { .. }
                                if i != 0
                                    && e.to_string().contains("exists")
                                    && e.to_string().contains("Catalog error") =>
                            {
                                break
                            }
                            // allow 'not found' error when retry DROP statement
                            SqlCmd::Drop
                                if i != 0
                                    && e.to_string().contains("not found")
                                    && e.to_string().contains("Catalog error") =>
                            {
                                break
                            }

                            // Keep i >= max_retry for other errors. Since these errors indicate that the MV might not yet be created.
                            _ if i >= max_retry => {
                                panic!("failed to run test after retry {i} times: {e}")
                            }
                            SqlCmd::CreateMaterializedView { ref name }
                                if i != 0
                                    && e.to_string().contains("table is in creating procedure")
                                    && background_ddl_enabled =>
                            {
                                tracing::debug!(iteration = i, name, "Retry for background ddl");
                                // wait for background ddl to finish and succeed.
                                let Ok(rw) =
                                    RisingWave::connect("frontend".into(), "dev".into()).await
                                else {
                                    tracing::debug!(iteration=i, name, "failed to run test: background_mv not created, retry after {delay:?}");
                                    if i >= max_retry {
                                        panic!("failed to run test after retry {i} times, while waiting for background ddl");
                                    }
                                    continue;
                                };
                                let client = rw.pg_client();
                                if client.simple_query("WAIT;").await.is_ok()
                                    && let Ok(result) = client
                                    .query(
                                        "select count(*) from pg_matviews where matviewname=$1;",
                                        &[&name],
                                    )
                                    .await
                                    && let Ok(1) = result[0].try_get::<_, i64>(0)
                                    {
                                        if let Some(record) = reset_background_ddl_record {
                                            tester.run_async(record).await.unwrap();
                                            background_ddl_enabled = false;
                                        }
                                        tracing::debug!("Record with background_ddl {:?} finished", record);
                                        break;
                                    }

                                // If fail, recreate mv again.
                                tracing::info!(iteration=i, name, "failed to run test: background_mv not created, retry after {delay:?}");
                                if i >= max_retry {
                                    panic!("failed to run test after retry {i} times, while waiting for background ddl");
                                }
                                continue;
                            }
                            _ => tracing::error!(
                                iteration = i,
                                "failed to run test: {e}\nretry after {delay:?}"
                            ),
                        }
                    }
                }
                tokio::time::sleep(delay).await;
            }
            if let SqlCmd::SetBackgroundDdl { enable } = cmd {
                background_ddl_enabled = enable;
            };
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
        }
    }
}

pub async fn run_parallel_slt_task(glob: &str, jobs: usize) -> Result<(), ParallelTestError> {
    let mut tester =
        sqllogictest::Runner::new(|| RisingWave::connect("frontend".into(), "dev".into()));
    tester
        .run_parallel_async(
            glob,
            vec!["frontend".into()],
            |host, dbname| async move { RisingWave::connect(host, dbname).await.unwrap() },
            jobs,
        )
        .await
        .map_err(|e| panic!("{e}"))
}

/// Replace some strings in kafka.slt and write to a new temp file.
fn hack_kafka_test(path: &Path) -> tempfile::NamedTempFile {
    let content = std::fs::read_to_string(path).expect("failed to read file");
    let simple_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/simple-schema.avsc")
            .expect("failed to get schema path");
    let complex_avsc_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.avsc")
            .expect("failed to get schema path");
    let proto_full_path = std::fs::canonicalize("src/connector/src/test_data/complex-schema")
        .expect("failed to get schema path");
    let json_schema_full_path =
        std::fs::canonicalize("src/connector/src/test_data/complex-schema.json")
            .expect("failed to get schema path");
    let content = content
        .replace("127.0.0.1:29092", "192.168.11.1:29092")
        .replace(
            "/risingwave/avro-simple-schema.avsc",
            simple_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/avro-complex-schema.avsc",
            complex_avsc_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/proto-complex-schema",
            proto_full_path.to_str().unwrap(),
        )
        .replace(
            "/risingwave/json-complex-schema",
            json_schema_full_path.to_str().unwrap(),
        );
    let file = tempfile::NamedTempFile::new().expect("failed to create temp file");
    std::fs::write(file.path(), content).expect("failed to write file");
    println!("created a temp file for kafka test: {:?}", file.path());
    file
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use expect_test::{expect, Expect};

    use super::*;

    fn check(actual: impl Debug, expect: Expect) {
        let actual = format!("{:#?}", actual);
        expect.assert_eq(&actual);
    }

    #[test]
    fn test_is_create_table_as() {
        assert!(is_create_table_as("     create     table xx  as select 1;"));
        assert!(!is_create_table_as(
            "     create table xx not  as select 1;"
        ));
        assert!(!is_create_table_as("     create view xx as select 1;"));
    }

    #[test]
    fn test_extract_sql_command() {
        check(
            extract_sql_command("create  table  t as select 1;"),
            expect![[r#"
                Create {
                    is_create_table_as: true,
                }"#]],
        );
        check(
            extract_sql_command("  create table  t (a int);"),
            expect![[r#"
                Create {
                    is_create_table_as: false,
                }"#]],
        );
        check(
            extract_sql_command(" create materialized   view  m_1 as select 1;"),
            expect![[r#"
                CreateMaterializedView {
                    name: "m_1",
                }"#]],
        );
        check(
            extract_sql_command("set background_ddl= true;"),
            expect![[r#"
                SetBackgroundDdl {
                    enable: true,
                }"#]],
        );
        check(
            extract_sql_command("SET BACKGROUND_DDL=true;"),
            expect![[r#"
                SetBackgroundDdl {
                    enable: true,
                }"#]],
        )
    }
}
