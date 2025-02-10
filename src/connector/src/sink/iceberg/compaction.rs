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
use std::pin::pin;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{anyhow, Context};
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_emrserverless::types::builders::SparkSubmitBuilder;
use aws_sdk_emrserverless::types::{JobDriver, JobRunState};
use aws_sdk_emrserverless::Client;
use aws_types::region::Region;
use futures::future::select;
use itertools::Itertools;
use thiserror_ext::AsReport;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_retry::strategy::ExponentialBackoff;
use tracing::{error, info, warn};

use crate::sink::iceberg::IcebergConfig;

pub struct IcebergCompactionConfig {
    region: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
    execution_role_arn: String,
    application_id: String,
    entrypoint: String,
    compact_frequency: usize,
    min_compact_gap_duration_sec: Duration,
}

impl IcebergCompactionConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        use std::env::var;
        Ok(IcebergCompactionConfig {
            region: var("ICEBERG_COMPACTION_REGION").ok(),
            access_key: var("ICEBERG_COMPACTION_ACCESS_KEY").ok(),
            secret_key: var("ICEBERG_COMPACTION_SECRET_KEY").ok(),
            execution_role_arn: var("ICEBERG_COMPACTION_EXECUTION_ROLE_ARN")
                .map_err(|_| anyhow!("ICEBERG_COMPACTION_EXECUTION_ROLE_ARN not set in env var"))?,
            application_id: var("ICEBERG_COMPACTION_APPLICATION_ID")
                .map_err(|_| anyhow!("ICEBERG_COMPACTION_APPLICATION_ID not set in env var"))?,
            entrypoint: var("ICEBERG_COMPACTION_ENTRYPOINT")
                .map_err(|_| anyhow!("ICEBERG_COMPACTION_ENTRYPOINT not set in env var"))?,
            compact_frequency: var("ICEBERG_COMPACTION_FREQUENCY")
                .map_err(|_| anyhow!("ICEBERG_COMPACTION_FREQUENCY not set in env var"))?
                .parse::<usize>()
                .context("invalid ICEBERG_COMPACTION_FREQUENCY")?,
            min_compact_gap_duration_sec: Duration::from_secs(
                var("ICEBERG_MIN_COMPACTION_GAP_DURATION_SEC")
                    .map_err(|_| {
                        anyhow!("ICEBERG_MIN_COMPACTION_GAP_DURATION_SEC not set in env var")
                    })?
                    .parse::<u64>()
                    .context("invalid ICEBERG_MIN_COMPACTION_GAP_DURATION_SEC")?,
            ),
        })
    }
}

async fn run_compact(
    catalog: String,
    database: String,
    table: String,
    catalog_config: HashMap<String, String>,
    mut commit_rx: mpsc::UnboundedReceiver<()>,
) {
    let config = match IcebergCompactionConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            error!(catalog, database, table, e = ?e.as_report(), "failed to start compact worker");
            return;
        }
    };
    let compact_frequency = config.compact_frequency;
    let min_compact_gap_duration_sec = config.min_compact_gap_duration_sec;
    let mut pending_commit_num = 0;
    let client = IcebergCompactionClient::new(config).await;
    let new_backoff = || {
        ExponentialBackoff::from_millis(1000)
            .factor(2)
            .max_delay(Duration::from_secs(60))
    };

    let mut prev_compact_success_time = Instant::now();
    let mut error_backoff = new_backoff();
    let mut error_count = 0;

    loop {
        if pending_commit_num >= compact_frequency
            && Instant::now().duration_since(prev_compact_success_time)
                > min_compact_gap_duration_sec
        {
            let compact_start_time = Instant::now();
            if let Err(e) = client
                .compact(
                    catalog.clone(),
                    database.clone(),
                    table.clone(),
                    &catalog_config,
                )
                .await
            {
                let backoff_duration = error_backoff.next().expect("should exist");
                error_count += 1;
                error!(
                    err = ?e.as_report(),
                    ?backoff_duration,
                    error_count,
                    catalog, database, table, "failed to compact"
                );
                sleep(backoff_duration).await;
            } else {
                info!(catalog, database, table, elapsed = ?compact_start_time.elapsed(),  "compact success");
                pending_commit_num = 0;
                error_backoff = new_backoff();
                error_count = 0;
                prev_compact_success_time = Instant::now();
            }
        }
        if commit_rx.recv().await.is_some() {
            pending_commit_num += 1;
        } else {
            break;
        }
    }
}

fn get_catalog_config(config: &IcebergConfig) -> anyhow::Result<HashMap<String, String>> {
    match config.common.catalog_type.as_deref() {
        Some("storage") | None => Ok(HashMap::from_iter([
            ("type".to_owned(), "hadoop".to_owned()),
            (
                "warehouse".to_owned(),
                config
                    .common
                    .warehouse_path
                    .clone()
                    .ok_or_else(|| anyhow!("warehouse unspecified for jdbc catalog"))?,
            ),
        ])),
        Some("jdbc") => {
            let mut result = HashMap::new();
            result.insert("type".to_owned(), "jdbc".to_owned());
            result.insert(
                "warehouse".to_owned(),
                config
                    .common
                    .warehouse_path
                    .clone()
                    .ok_or_else(|| anyhow!("warehouse unspecified for jdbc catalog"))?,
            );
            result.insert(
                "uri".to_owned(),
                config
                    .common
                    .catalog_uri
                    .clone()
                    .ok_or_else(|| anyhow!("uri unspecified for jdbc catalog"))?,
            );
            if let Some(region) = &config.common.region {
                result.insert(
                    "s3.endpoint".to_owned(),
                    format!("https://s3.{}.amazonaws.com", region),
                );
            }
            if let Some(endpoint) = &config.common.endpoint {
                result.insert("s3.endpoint".to_owned(), endpoint.to_owned());
            }
            if let Some(access_key) = &config.common.access_key {
                result.insert("s3.access-key-id".to_owned(), access_key.to_owned());
            }
            if let Some(secret_key) = &config.common.secret_key {
                result.insert("s3.secret-access-key".to_owned(), secret_key.to_owned());
            }
            if let Some(path_style_access) = &config.common.path_style_access {
                result.insert(
                    "s3.path-style-access".to_owned(),
                    path_style_access.to_string(),
                );
            }
            result.insert(
                "io-impl".to_owned(),
                "risingwave.shaded.org.apache.iceberg.aws.s3.S3FileIO".to_owned(),
            );
            config
                .java_catalog_props
                .iter()
                .filter(|(key, _)| key.starts_with("jdbc."))
                .for_each(|(k, v)| {
                    result.insert(k.clone(), v.clone());
                });
            Ok(result)
        }
        Some(other) => Err(anyhow!("unsupported catalog type {} in compaction", other)),
    }
}

pub fn spawn_compaction_client(
    config: &IcebergConfig,
) -> anyhow::Result<(mpsc::UnboundedSender<()>, oneshot::Sender<()>)> {
    let catalog = config
        .common
        .catalog_name
        .clone()
        .ok_or_else(|| anyhow!("should specify catalog name"))?;
    let database = config
        .common
        .database_name
        .clone()
        .ok_or_else(|| anyhow!("should specify database"))?;
    let table = config.common.table_name.clone();
    let (commit_tx, commit_rx) = mpsc::unbounded_channel();
    let (finish_tx, finish_rx) = oneshot::channel();

    let catalog_config = get_catalog_config(config)?;

    let _join_handle = tokio::spawn(async move {
        select(
            finish_rx,
            pin!(run_compact(
                catalog.clone(),
                database.clone(),
                table.clone(),
                catalog_config,
                commit_rx,
            )),
        )
        .await;
        warn!(catalog, database, table, "compact worker exits");
    });
    Ok((commit_tx, finish_tx))
}

pub struct IcebergCompactionClient {
    client: Client,
    config: IcebergCompactionConfig,
}

impl IcebergCompactionClient {
    pub async fn new(config: IcebergCompactionConfig) -> Self {
        let config_loader = aws_config::from_env();
        let config_loader = if let Some(region) = &config.region {
            config_loader.region(Region::new(region.clone()))
        } else {
            config_loader
        };
        let config_loader = if let (Some(access_key), Some(secret_key)) =
            (&config.access_key, &config.secret_key)
        {
            config_loader.credentials_provider(SharedCredentialsProvider::new(
                aws_credential_types::Credentials::from_keys(
                    access_key.clone(),
                    secret_key.clone(),
                    None,
                ),
            ))
        } else {
            config_loader
        };
        let sdk_config = config_loader.load().await;
        Self {
            client: Client::new(&sdk_config),
            config,
        }
    }

    async fn wait_job_finish(&self, job_run_id: String) -> anyhow::Result<()> {
        let start_time = Instant::now();
        let success_job_run = loop {
            let output = self
                .client
                .get_job_run()
                .job_run_id(&job_run_id)
                .application_id(self.config.application_id.clone())
                .send()
                .await?;
            let job_run = output.job_run.ok_or_else(|| anyhow!("empty job run"))?;
            match &job_run.state {
                JobRunState::Cancelled | JobRunState::Cancelling | JobRunState::Failed => {
                    return Err(anyhow!(
                        "fail state: {}. Detailed: {}",
                        job_run.state,
                        job_run.state_details
                    ));
                }
                JobRunState::Pending
                | JobRunState::Queued
                | JobRunState::Running
                | JobRunState::Scheduled
                | JobRunState::Submitted => {
                    info!(
                        elapsed = ?start_time.elapsed(),
                        job_status = ?job_run.state,
                        "waiting job."
                    );
                    sleep(Duration::from_secs(5)).await;
                }
                JobRunState::Success => {
                    break job_run;
                }
                state => {
                    return Err(anyhow!("unhandled state: {:?}", state));
                }
            };
        };
        info!(
            job_run_id,
            details = success_job_run.state_details,
            elapsed = ?start_time.elapsed(),
            "job run finish"
        );
        Ok(())
    }

    pub async fn compact(
        &self,
        catalog: String,
        db: String,
        table: String,
        catalog_config: &HashMap<String, String>,
    ) -> anyhow::Result<()> {
        let start_result = self
            .client
            .start_job_run()
            .name(format!(
                "job-run-{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            ))
            .application_id(self.config.application_id.clone())
            .execution_role_arn(self.config.execution_role_arn.clone())
            .job_driver(JobDriver::SparkSubmit(
                SparkSubmitBuilder::default()
                    .entry_point(self.config.entrypoint.clone())
                    .entry_point_arguments(catalog.clone())
                    .entry_point_arguments(db)
                    .entry_point_arguments(table)
                    .spark_submit_parameters(
                        catalog_config
                            .iter()
                            .map(|(key, value)| {
                                format!("--conf spark.sql.catalog.{}.{}={}", catalog, key, value)
                            })
                            .join(" "),
                    )
                    .build()
                    .unwrap(),
            ))
            .send()
            .await
            .context("start job")?;
        info!(job_run_id = start_result.job_run_id, "job started");
        self.wait_job_finish(start_result.job_run_id).await
    }
}

#[tokio::test]
#[ignore]
async fn trigger_compaction() {
    tracing_subscriber::fmt().init();
    let warehouse = "s3://iceberg-spark/iceberg/";
    let catalog = "catalog";
    let db = "db";
    let table = "table";

    let config = IcebergCompactionConfig::from_env().unwrap();
    let client = IcebergCompactionClient::new(config).await;
    let result = client
        .compact(
            catalog.to_owned(),
            db.to_owned(),
            table.to_owned(),
            &HashMap::from_iter([
                ("type".to_owned(), "hadoop".to_owned()),
                ("warehouse".to_owned(), warehouse.to_owned()),
            ]),
        )
        .await;

    info!(?result, "job result");
}
