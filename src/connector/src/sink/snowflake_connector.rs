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
use std::time::{SystemTime, UNIX_EPOCH};

use aws_config;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use bytes::Bytes;
use http::header;
use http::request::Builder;
use hyper::body::Body;
use hyper::client::HttpConnector;
use hyper::{Client, Request, StatusCode};
use hyper_tls::HttpsConnector;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

use super::doris_starrocks_connector::POOL_IDLE_TIMEOUT;
use super::{Result, SinkError};

const SNOWFLAKE_HOST_ADDR: &str = "snowflakecomputing.com";
const SNOWFLAKE_REQUEST_ID: &str = "RW_SNOWFLAKE_SINK";
const S3_INTERMEDIATE_FILE_NAME: &str = "RW_SNOWFLAKE_S3_SINK_FILE";

/// The helper function to generate the *global unique* s3 file name.
fn generate_s3_file_name(s3_path: Option<String>, suffix: String) -> String {
    match s3_path {
        Some(path) => format!("{}/{}_{}", path, S3_INTERMEDIATE_FILE_NAME, suffix),
        None => format!("{}_{}", S3_INTERMEDIATE_FILE_NAME, suffix),
    }
}

/// Claims is used when constructing `jwt_token`
/// with payload specified.
/// reference: <https://docs.snowflake.com/en/developer-guide/sql-api/authenticating>
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    iat: usize,
    exp: usize,
}

#[derive(Debug)]
pub struct SnowflakeHttpClient {
    url: String,
    rsa_public_key_fp: String,
    account: String,
    user: String,
    private_key: String,
    header: HashMap<String, String>,
    s3_path: Option<String>,
}

impl SnowflakeHttpClient {
    pub fn new(
        account: String,
        user: String,
        db: String,
        schema: String,
        pipe: String,
        rsa_public_key_fp: String,
        private_key: String,
        header: HashMap<String, String>,
        s3_path: Option<String>,
    ) -> Self {
        // todo: ensure if we need user to *explicitly* provide the `request_id`
        // currently it seems that this is not important.
        // reference to the snowpipe rest api is as below, i.e.,
        // <https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis>
        let url = format!(
            "https://{}.{}/v1/data/pipes/{}.{}.{}/insertFiles?requestId={}",
            account.clone(),
            SNOWFLAKE_HOST_ADDR,
            db,
            schema,
            pipe,
            SNOWFLAKE_REQUEST_ID
        );

        Self {
            url,
            rsa_public_key_fp,
            account,
            user,
            private_key,
            header,
            s3_path,
        }
    }

    /// Generate a 59-minutes valid `jwt_token` for authentication of snowflake side
    /// And please note that we will NOT strictly counting the time interval
    /// of `jwt_token` expiration.
    /// Which essentially means that this method should be called *every time* we want
    /// to send `insertFiles` request to snowflake server.
    fn generate_jwt_token(&self) -> Result<String> {
        let header = Header::new(Algorithm::RS256);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let lifetime = 59 * 60;

        // Ensure the account and username are uppercase
        let account = self.account.to_uppercase();
        let user = self.user.to_uppercase();

        // Construct the fully qualified username
        let qualified_username = format!("{}.{}", account, user);

        let claims = Claims {
            iss: format!("{}.{}", qualified_username.clone(), self.rsa_public_key_fp),
            sub: qualified_username,
            iat: now,
            exp: now + lifetime,
        };

        let jwt_token = encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(self.private_key.as_ref()).map_err(|err| {
                SinkError::Snowflake(format!(
                    "failed to encode from provided rsa pem key, error: {}",
                    err
                ))
            })?,
        )
        .map_err(|err| {
            SinkError::Snowflake(format!("failed to encode jwt_token, error: {}", err))
        })?;
        Ok(jwt_token)
    }

    fn build_request_and_client(&self) -> (Builder, Client<HttpsConnector<HttpConnector>>) {
        let builder = Request::post(self.url.clone());

        let connector = HttpsConnector::new();
        let client = Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);

        (builder, client)
    }

    /// NOTE: this function should ONLY be called *after*
    /// uploading files to remote external staged storage, i.e., AWS S3
    pub async fn send_request(&self, file_suffix: String) -> Result<()> {
        let (builder, client) = self.build_request_and_client();

        // Generate the jwt_token
        let jwt_token = self.generate_jwt_token()?;
        let builder = builder
            .header(header::CONTENT_TYPE, "text/plain")
            .header("Authorization", format!("Bearer {}", jwt_token))
            .header(
                "X-Snowflake-Authorization-Token-Type".to_string(),
                "KEYPAIR_JWT",
            );

        let request = builder
            .body(Body::from(generate_s3_file_name(
                self.s3_path.clone(),
                file_suffix,
            )))
            .map_err(|err| SinkError::Snowflake(err.to_string()))?;

        let response = client
            .request(request)
            .await
            .map_err(|err| SinkError::Snowflake(err.to_string()))?;

        if response.status() != StatusCode::OK {
            return Err(SinkError::Snowflake(format!(
                "failed to make http request, error code: {}\ndetailed response: {:#?}",
                response.status(),
                response,
            )));
        }

        Ok(())
    }
}

/// todo: refactor this part after s3 sink is available
pub struct SnowflakeS3Client {
    s3_bucket: String,
    s3_path: Option<String>,
    s3_client: S3Client,
}

impl SnowflakeS3Client {
    pub async fn new(
        s3_bucket: String,
        s3_path: Option<String>,
        aws_access_key_id: String,
        aws_secret_access_key: String,
        aws_region: String,
    ) -> Self {
        let credentials = Credentials::new(
            aws_access_key_id,
            aws_secret_access_key,
            // we don't allow temporary credentials
            None,
            None,
            "rw_sink_to_s3_credentials",
        );

        let region = RegionProviderChain::first_try(Region::new(aws_region)).or_default_provider();

        let config = aws_config::from_env()
            .credentials_provider(credentials)
            .region(region)
            .load()
            .await;

        // create the brand new s3 client used to sink files to s3
        let s3_client = S3Client::new(&config);

        Self {
            s3_bucket,
            s3_path,
            s3_client,
        }
    }

    pub async fn sink_to_s3(&self, data: Bytes, file_suffix: String) -> Result<()> {
        self.s3_client
            .put_object()
            .bucket(self.s3_bucket.clone())
            .key(generate_s3_file_name(self.s3_path.clone(), file_suffix))
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|err| {
                SinkError::Snowflake(format!("failed to sink data to S3, error: {}", err))
            })?;

        Ok(())
    }
}
