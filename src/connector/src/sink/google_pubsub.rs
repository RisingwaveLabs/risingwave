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
use std::usize;

use anyhow::{anyhow, Context};
use google_cloud_gax::conn::Environment;
use google_cloud_googleapis::pubsub::v1::PubsubMessage;
use google_cloud_pubsub::apiv1;
use google_cloud_pubsub::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::google_cloud_auth::project;
use google_cloud_pubsub::client::google_cloud_auth::token::DefaultTokenSourceProvider;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::publisher::Publisher;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tonic::Status;
use with_options::WithOptions;

use super::catalog::desc::SinkDesc;
use super::catalog::SinkFormatDesc;
use super::formatter::SinkFormatterImpl;
use super::log_store::DeliveryFutureManagerAddFuture;
use super::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use super::{DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriterParam};
use crate::dispatch_sink_formatter_str_key_impl;

pub const PUBSUB_SINK: &str = "google_pubsub";

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct GooglePubSubConfig {
    /// The Google Pub/Sub Project ID
    #[serde(rename = "pubsub.project_id")]
    pub project_id: String,

    /// Specifies the Pub/Sub topic to publish messages
    #[serde(rename = "pubsub.topic")]
    pub topic: String,

    /// The Google Pub/Sub endpoint URL
    #[serde(rename = "pubsub.endpoint")]
    pub endpoint: String,

    /// use the connector with a pubsub emulator
    /// <https://cloud.google.com/pubsub/docs/emulator>
    #[serde(rename = "pubsub.emulator_host")]
    pub emulator_host: Option<String>,

    /// A JSON string containing the service account credentials for authorization,
    /// see the [service-account](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) credentials guide.
    /// The provided account credential must have the
    /// `pubsub.publisher` [role](https://cloud.google.com/pubsub/docs/access-control#roles)
    #[serde(rename = "pubsub.credentials")]
    pub credentials: Option<String>,

    // accept "append-only"
    pub r#type: String,
}

impl GooglePubSubConfig {
    fn from_hashmap(values: HashMap<String, String>) -> Result<Self> {
        serde_json::from_value::<GooglePubSubConfig>(serde_json::to_value(values).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))
    }
}

#[derive(Clone, Debug)]
pub struct GooglePubSubSink {
    pub config: GooglePubSubConfig,
    is_append_only: bool,

    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl Sink for GooglePubSubSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = AsyncTruncateLogSinkerOf<GooglePubSubSinkWriter>;

    const SINK_NAME: &'static str = PUBSUB_SINK;

    fn is_sink_decouple(desc: &SinkDesc, user_specified: &SinkDecouple) -> Result<bool> {
        match user_specified {
            SinkDecouple::Default => Ok(desc.sink_type.is_append_only()),
            SinkDecouple::Disable => Ok(false),
            SinkDecouple::Enable => Ok(true),
        }
    }

    async fn validate(&self) -> Result<()> {
        if !self.is_append_only {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Google Pub/Sub sink only support append-only mode"
            )));
        }

        let conf = &self.config;
        if matches!((&conf.emulator_host, &conf.credentials), (None, None)) {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Configure at least one of `pubsub.emulator_host` and `pubsub.credentials` in the Google Pub/Sub sink"
            )));
        }

        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(GooglePubSubSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(usize::MAX))
    }
}

impl TryFrom<SinkParam> for GooglePubSubSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = GooglePubSubConfig::from_hashmap(param.properties)?;

        let format_desc = param
            .format_desc
            .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?;
        Ok(Self {
            config,
            is_append_only: param.sink_type.is_append_only(),

            schema,
            pk_indices: param.downstream_pk,
            format_desc,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

struct GooglePubSubPayloadWriter {
    publisher: Publisher,
}

impl GooglePubSubSinkWriter {
    pub async fn new(
        config: GooglePubSubConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let environment = if let Some(ref cred) = config.credentials {
            let auth_config = project::Config {
                audience: Some(apiv1::conn_pool::AUDIENCE),
                scopes: Some(&apiv1::conn_pool::SCOPES),
                sub: None,
            };
            let cred_file = CredentialsFile::new_from_str(cred).await.map_err(|e| {
                SinkError::GooglePubSub(
                    anyhow!(e).context("Failed to create Google Cloud Pub/Sub credentials file"),
                )
            })?;
            let provider =
                DefaultTokenSourceProvider::new_with_credentials(auth_config, Box::new(cred_file))
                    .await
                    .map_err(|e| {
                        SinkError::GooglePubSub(
                            anyhow!(e).context(
                                "Failed to create Google Cloud Pub/Sub token source provider",
                            ),
                        )
                    })?;
            Environment::GoogleCloud(Box::new(provider))
        } else if let Some(emu_host) = config.emulator_host {
            Environment::Emulator(emu_host)
        } else {
            return Err(SinkError::GooglePubSub(anyhow!(
                "Missing emulator_host or credentials in Google Pub/Sub sink"
            )));
        };

        let client_config = ClientConfig {
            endpoint: config.endpoint,
            project_id: Some(config.project_id),
            environment,
            ..Default::default()
        };
        let client = Client::new(client_config)
            .await
            .map_err(|e| SinkError::GooglePubSub(anyhow!(e)))?;

        let topic = async {
            let topic = client.topic(&config.topic);
            if !topic.exists(None).await? {
                topic.create(None, None).await?;
            }
            Ok(topic)
        }
        .await
        .map_err(|e: Status| SinkError::GooglePubSub(anyhow!(e)))?;

        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema,
            pk_indices,
            db_name,
            sink_from_name,
            topic.fully_qualified_name(),
        )
        .await?;

        let publisher = topic.new_publisher(None);
        let payload_writer = GooglePubSubPayloadWriter { publisher };

        Ok(Self {
            payload_writer,
            formatter,
        })
    }
}

pub struct GooglePubSubSinkWriter {
    payload_writer: GooglePubSubPayloadWriter,
    formatter: SinkFormatterImpl,
}

impl AsyncTruncateSinkWriter for GooglePubSubSinkWriter {
    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        _add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        dispatch_sink_formatter_str_key_impl!(
            &self.formatter,
            formatter,
            self.payload_writer.write_chunk(chunk, formatter).await
        )
    }
}

impl FormattedSink for GooglePubSubPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        let ordering_key = k.unwrap_or_default();
        match v {
            Some(data) => {
                let msg = PubsubMessage {
                    data,
                    ordering_key,
                    ..Default::default()
                };
                let awaiter = self.publisher.publish(msg).await;
                awaiter
                    .get()
                    .await
                    .context("Google Pub/Sub sink error")
                    .map_err(SinkError::GooglePubSub)
                    .map(|_| ())
            }
            None => Err(SinkError::GooglePubSub(anyhow!(
                "Google Pub/Sub sink error: missing value to publish"
            ))),
        }
    }
}
