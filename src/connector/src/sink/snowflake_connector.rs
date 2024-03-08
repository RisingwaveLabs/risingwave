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

use bytes::{BufMut, Bytes, BytesMut};
use http::request::Builder;
use hyper::body::{Body, Sender};
use hyper::client::HttpConnector;
use hyper::{body, Client, Request, StatusCode};
use hyper_tls::HttpsConnector;
use tokio::task::JoinHandle;

use super::doris_starrocks_connector::POOL_IDLE_TIMEOUT;
use super::{Result, SinkError};

const SNOWFLAKE_HOST_ADDR: &str = "snowflakecomputing.com";
const SNOWFLAKE_REQUEST_ID: &str = "RW_SNOWFLAKE_SINK";

#[derive(Debug)]
pub struct SnowflakeHttpClient {
    url: String,
    s3: String,
    header: HashMap<String, String>,
}

impl SnowflakeHttpClient {
    pub fn new(
        account: String,
        db: String,
        schema: String,
        pipe: String,
        s3: String,
        header: HashMap<String, String>,
    ) -> Self {
        // TODO: ensure if we need user to *explicitly* provide the request id
        let url = format!(
            "https://{}.{}/v1/data/pipes/{}.{}.{}/insertFiles?request_id={}",
            account, SNOWFLAKE_HOST_ADDR, db, schema, pipe, SNOWFLAKE_REQUEST_ID
        );

        Self { url, s3, header }
    }

    fn build_request_and_client(&self) -> (Builder, Client<HttpsConnector<HttpConnector>>) {
        let mut builder = Request::post(self.url.clone());
        for (k, v) in &self.header {
            builder = builder.header(k, v);
        }

        let connector = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        (builder, client)
    }

    /// NOTE: this function should ONLY be called after
    /// uploading files to remote external staged storage, e.g., AWS S3
    pub async fn send_request(&self) -> Result<()> {
        let (builder, client) = self.build_request_and_client();
        let request = builder
            .body(Body::from(self.s3.clone()))
            .map_err(|err| SinkError::Snowflake(err.to_string()))?;
        let response = client.request(request).await.map_err(|err| SinkError::Snowflake(err.to_string()))?;
        if response.status() != StatusCode::OK {
            return Err(SinkError::Snowflake(format!("failed to make http request, error code: {}", response.status())));
        }
        Ok(())
    }
}
