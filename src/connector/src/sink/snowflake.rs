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

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;
use with_options::WithOptions;

use super::encoder::{
    JsonEncoder, RowEncoder, TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::snowflake_connector::{SnowflakeHttpClient, SnowflakeS3Client};
use super::writer::LogSinkerOf;
use super::{SinkError, SinkParam};
use crate::sink::writer::SinkWriterExt;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const SNOWFLAKE_SINK: &str = "snowflake";

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SnowflakeCommon {
    /// The snowflake database used for sinking
    #[serde(rename = "snowflake.database")]
    pub database: String,

    /// The corresponding schema where sink table exists
    #[serde(rename = "snowflake.schema")]
    pub schema: String,

    /// The created pipe object, will be used as `insertFiles` target
    #[serde(rename = "snowflake.pipe")]
    pub pipe: String,

    /// The unique, snowflake provided `account_identifier`
    /// NOTE: please use the form `<orgname>-<account_name>`
    /// For detailed guidance, reference: <https://docs.snowflake.com/en/user-guide/admin-account-identifier>
    #[serde(rename = "snowflake.account_identifier")]
    pub account_identifier: String,

    /// The user that owns the table to be sinked
    /// NOTE: the user should've been granted corresponding *role*
    /// reference: <https://docs.snowflake.com/en/sql-reference/sql/grant-role>
    #[serde(rename = "snowflake.user")]
    pub user: String,

    /// The public key fingerprint used when generating custom `jwt_token`
    /// reference: <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating>
    #[serde(rename = "snowflake.rsa_public_key_fp")]
    pub rsa_public_key_fp: String,

    /// The rsa pem key *without* encryption
    #[serde(rename = "snowflake.private_key")]
    pub private_key: String,

    /// The s3 bucket where intermediate sink files will be stored
    #[serde(rename = "snowflake.s3_bucket")]
    pub s3_bucket: String,

    /// s3 credentials
    #[serde(rename = "snowflake.aws_access_key_id")]
    pub aws_access_key_id: String,

    /// s3 credentials
    #[serde(rename = "snowflake.aws_secret_access_key")]
    pub aws_secret_access_key: String,

    /// The s3 region, e.g., us-east-2
    #[serde(rename = "snowflake.aws_region")]
    pub aws_region: String,

    /// The configurable max row(s) to batch,
    /// which should be explicitly specified by user(s)
    #[serde(rename = "snowflake.max_batch_row_num")]
    pub max_batch_row_num: String,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub common: SnowflakeCommon,
}

impl SnowflakeConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct SnowflakeSink {
    pub config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl Sink for SnowflakeSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SnowflakeSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::Config(
                anyhow!("SnowflakeSink only supports append-only mode at present, please change the configuration accordingly to enable sinking.")
            ));
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SnowflakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeConfig::from_hashmap(param.properties)?;
        Ok(SnowflakeSink {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

pub struct SnowflakeSinkWriter {
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    /// the client used to send `insertFiles` post request
    http_client: SnowflakeHttpClient,
    /// the client to insert file to external storage (i.e., s3)
    s3_client: SnowflakeS3Client,
    row_encoder: JsonEncoder,
    row_counter: u32,
    payload: String,
    /// the threshold for sinking to s3
    max_batch_row_num: u32,
    /// The current epoch, used in naming the sink files
    epoch: u64,
    sink_file_suffix: u32,
}

impl SnowflakeSinkWriter {
    pub async fn new(
        config: SnowflakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Self {
        let http_client = SnowflakeHttpClient::new(
            config.common.account_identifier.clone(),
            config.common.user.clone(),
            config.common.database.clone(),
            config.common.schema.clone(),
            config.common.pipe.clone(),
            config.common.rsa_public_key_fp.clone(),
            config.common.private_key.clone(),
            HashMap::new(),
        );

        let s3_client = SnowflakeS3Client::new(
            config.common.s3_bucket.clone(),
            config.common.aws_access_key_id.clone(),
            config.common.aws_secret_access_key.clone(),
            config.common.aws_region.clone(),
        )
        .await;

        let max_batch_row_num = config
            .common
            .max_batch_row_num
            .clone()
            .parse::<u32>()
            .expect("failed to parse `snowflake.max_batch_row_num` as a `u32`");

        Self {
            config,
            schema: schema.clone(),
            pk_indices,
            is_append_only,
            http_client,
            s3_client,
            row_encoder: JsonEncoder::new(
                schema,
                None,
                super::encoder::DateHandlingMode::String,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcString,
                TimeHandlingMode::String,
            ),
            row_counter: 0,
            payload: String::new(),
            max_batch_row_num,
            epoch: 0,
            // Start from 0, i.e., `RW_SNOWFLAKE_S3_SINK_FILE_0`
            sink_file_suffix: 0,
        }
    }

    /// Note that we shall NOT reset the `sink_file_suffix`
    /// since we need to incrementally keep the sink
    /// file *unique*, otherwise snowflake will not
    /// sink it from external stage (i.e., s3)
    fn reset(&mut self) {
        self.payload.clear();
        self.row_counter = 0;
    }

    fn at_sink_threshold(&self) -> bool {
        self.row_counter >= self.max_batch_row_num
    }

    fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            assert_eq!(op, Op::Insert, "expect all `op(s)` to be `Op::Insert`");
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.payload.push_str(&row_json_string);
            self.row_counter += 1;
        }
        Ok(())
    }

    fn update_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Construct the *unique* file suffix for the sink
    fn file_suffix(&self) -> String {
        format!("{}_{}", self.epoch, self.sink_file_suffix)
    }

    /// Sink `payload` to s3, then trigger corresponding `insertFiles` post request
    /// to snowflake, to finish the overall sinking pipeline.
    async fn sink_payload(&mut self) -> Result<()> {
        if self.payload.is_empty() {
            return Ok(());
        }
        // first sink to the external stage provided by user (i.e., s3)
        self.s3_client
            .sink_to_s3(self.payload.clone().into(), self.file_suffix())
            .await?;
        // then trigger `insertFiles` post request to snowflake
        self.http_client.send_request(self.file_suffix()).await?;
        // reset `payload` & `row_counter`
        self.reset();
        // to ensure s3 sink file unique
        self.sink_file_suffix += 1;
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        self.update_epoch(epoch);
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        if is_checkpoint {
            // sink all the row(s) currently batched in `self.payload`
            self.sink_payload().await?;
        }
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.append_only(chunk)?;

        // When the number of row exceeds `MAX_BATCH_ROW_NUM`
        if self.at_sink_threshold() {
            self.sink_payload().await?;
        }

        Ok(())
    }
}
