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

use core::mem;
use core::time::Duration;
use std::collections::HashMap;
use std::convert::Infallible;

use anyhow::Context;
use base64::engine::general_purpose;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use reqwest::{redirect, Body, Client, RequestBuilder, StatusCode};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use url::Url;

use super::{Result, SinkError};

const BUFFER_SIZE: usize = 64 * 1024;
const MIN_CHUNK_SIZE: usize = BUFFER_SIZE - 1024;
pub(crate) const DORIS_SUCCESS_STATUS: [&str; 2] = ["Success", "Publish Timeout"];
pub(crate) const DORIS_DELETE_SIGN: &str = "__DORIS_DELETE_SIGN__";
pub(crate) const STARROCKS_DELETE_SIGN: &str = "__op";

const WAIT_HANDDLE_TIMEOUT: Duration = Duration::from_secs(10);
pub(crate) const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const LOCALHOST: &str = "localhost";
const LOCALHOST_IP: &str = "127.0.0.1";
pub struct HeaderBuilder {
    header: HashMap<String, String>,
}
impl Default for HeaderBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl HeaderBuilder {
    pub fn new() -> Self {
        Self {
            header: HashMap::default(),
        }
    }

    pub fn add_common_header(mut self) -> Self {
        self.header
            .insert("expect".to_string(), "100-continue".to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Doris will generate a default, non-repeating label.
    pub fn set_label(mut self, label: String) -> Self {
        self.header.insert("label".to_string(), label);
        self
    }

    pub fn set_columns_name(mut self, columns_name: Vec<&str>) -> Self {
        let columns_name_str = columns_name.join(",");
        self.header.insert("columns".to_string(), columns_name_str);
        self
    }

    /// This method is only called during upsert operations.
    pub fn add_hidden_column(mut self) -> Self {
        self.header
            .insert("hidden_columns".to_string(), DORIS_DELETE_SIGN.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn enable_2_pc(mut self) -> Self {
        self.header
            .insert("two_phase_commit".to_string(), "true".to_string());
        self
    }

    pub fn set_user_password(mut self, user: String, password: String) -> Self {
        let auth = format!(
            "Basic {}",
            general_purpose::STANDARD.encode(format!("{}:{}", user, password))
        );
        self.header.insert("Authorization".to_string(), auth);
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn set_txn_id(mut self, txn_id: i64) -> Self {
        self.header
            .insert("txn_operation".to_string(), txn_id.to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_commit(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "commit".to_string());
        self
    }

    /// The method is temporarily not in use, reserved for later use in 2PC.
    /// Only use in Doris
    pub fn add_abort(mut self) -> Self {
        self.header
            .insert("txn_operation".to_string(), "abort".to_string());
        self
    }

    pub fn add_json_format(mut self) -> Self {
        self.header.insert("format".to_string(), "json".to_string());
        self
    }

    /// Only use in Doris
    pub fn add_read_json_by_line(mut self) -> Self {
        self.header
            .insert("read_json_by_line".to_string(), "true".to_string());
        self
    }

    /// Only use in Starrocks
    pub fn add_strip_outer_array(mut self) -> Self {
        self.header
            .insert("strip_outer_array".to_string(), "true".to_string());
        self
    }

    pub fn set_partial_update(mut self, partial_update: Option<String>) -> Self {
        self.header.insert(
            "partial_update".to_string(),
            partial_update.unwrap_or_else(|| "false".to_string()),
        );
        self
    }

    pub fn build(self) -> HashMap<String, String> {
        self.header
    }
}

pub struct InserterInnerBuilder {
    url: String,
    header: HashMap<String, String>,
    #[expect(dead_code)]
    sender: Option<Sender>,
    fe_host: String,
}
impl InserterInnerBuilder {
    pub fn new(
        url: String,
        db: String,
        table: String,
        header: HashMap<String, String>,
    ) -> Result<Self> {
        let fe_host = Url::parse(&url)
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
            .host_str()
            .ok_or_else(|| {
                SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get fe host from url"))
            })?
            .to_string();
        let url = format!("{}/api/{}/{}/_stream_load", url, db, table);

        Ok(Self {
            url,
            sender: None,
            header,
            fe_host,
        })
    }

    fn build_request(&self, uri: String) -> RequestBuilder {
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .redirect(redirect::Policy::none()) // we handle redirect by ourselves
            .build()
            .unwrap();

        let mut builder = client.put(uri);
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }
        builder
    }

    pub async fn build(&self) -> Result<InserterInner> {
        let builder = self.build_request(self.url.clone());
        let resp = builder
            .send()
            .await
            .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
        // TODO: shall we let `reqwest` handle the redirect?
        let mut be_url = if resp.status() == StatusCode::TEMPORARY_REDIRECT {
            resp.headers()
                .get("location")
                .ok_or_else(|| {
                    SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                        "Can't get doris BE url in header",
                    ))
                })?
                .to_str()
                .context("Can't get doris BE url in header")
                .map_err(SinkError::DorisStarrocksConnect)?
                .to_string()
        } else {
            return Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "Can't get doris BE url",
            )));
        };

        if self.fe_host != LOCALHOST && self.fe_host != LOCALHOST_IP {
            let mut parsed_be_url =
                Url::parse(&be_url).map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
            let be_host = parsed_be_url.host_str().ok_or_else(|| {
                SinkError::DorisStarrocksConnect(anyhow::anyhow!("Can't get be host from url"))
            })?;

            if be_host == LOCALHOST || be_host == LOCALHOST_IP {
                // if be host is 127.0.0.1, we may can't connect to it directly,
                // so replace it with fe host
                parsed_be_url
                    .set_host(Some(self.fe_host.as_str()))
                    .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
                be_url = parsed_be_url.as_str().into();
            }
        }

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let body = Body::wrap_stream(
            tokio_stream::wrappers::UnboundedReceiverStream::new(receiver).map(Ok::<_, Infallible>),
        );
        let builder = self.build_request(be_url).body(body);

        let handle: JoinHandle<Result<Vec<u8>>> = tokio::spawn(async move {
            let response = builder
                .send()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?;
            let status = response.status();
            let raw = response
                .bytes()
                .await
                .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                .into();

            if status == StatusCode::OK {
                Ok(raw)
            } else {
                Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                    "Failed connection {:?},{:?}",
                    status,
                    String::from_utf8(raw)
                        .map_err(|err| SinkError::DorisStarrocksConnect(err.into()))?
                )))
            }
        });
        Ok(InserterInner::new(sender, handle))
    }
}

type Sender = UnboundedSender<Bytes>;

pub struct InserterInner {
    sender: Option<Sender>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    buffer: BytesMut,
}
impl InserterInner {
    pub fn new(sender: Sender, join_handle: JoinHandle<Result<Vec<u8>>>) -> Self {
        Self {
            sender: Some(sender),
            join_handle: Some(join_handle),
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
    }

    async fn send_chunk(&mut self) -> Result<()> {
        if self.sender.is_none() {
            return Ok(());
        }

        let chunk = mem::replace(&mut self.buffer, BytesMut::with_capacity(BUFFER_SIZE));

        if let Err(_e) = self.sender.as_mut().unwrap().send(chunk.freeze()) {
            self.sender.take();
            self.wait_handle().await?;

            Err(SinkError::DorisStarrocksConnect(anyhow::anyhow!(
                "channel closed"
            )))
        } else {
            Ok(())
        }
    }

    pub async fn write(&mut self, data: Bytes) -> Result<()> {
        self.buffer.put_slice(&data);
        if self.buffer.len() >= MIN_CHUNK_SIZE {
            self.send_chunk().await?;
        }
        Ok(())
    }

    async fn wait_handle(&mut self) -> Result<Vec<u8>> {
        let res =
            match tokio::time::timeout(WAIT_HANDDLE_TIMEOUT, self.join_handle.as_mut().unwrap())
                .await
            {
                Ok(res) => res.map_err(|err| SinkError::DorisStarrocksConnect(err.into()))??,
                Err(err) => return Err(SinkError::DorisStarrocksConnect(err.into())),
            };
        Ok(res)
    }

    pub async fn finish(mut self) -> Result<Vec<u8>> {
        if !self.buffer.is_empty() {
            self.send_chunk().await?;
        }
        self.sender = None;
        self.wait_handle().await
    }
}
