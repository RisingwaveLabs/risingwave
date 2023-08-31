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

use std::fmt::Debug;
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use reqwest::Method;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use url::Url;

/// extract the magic number and `schema_id` at the front of payload
///
/// 0 -> magic number
/// 1-4 -> schema id
/// 5-... -> message payload
pub(crate) fn extract_schema_id(payload: &[u8]) -> Result<(i32, &[u8])> {
    let header_len = 5;

    if payload.len() < header_len {
        return Err(RwError::from(InternalError(format!(
            "confluent kafka message need 5 bytes header, but payload len is {}",
            payload.len()
        ))));
    }
    let magic = payload[0];
    let schema_id = BigEndian::read_i32(&payload[1..5]);

    if magic != 0 {
        return Err(RwError::from(InternalError(
            "confluent kafka message must have a zero magic byte".to_owned(),
        )));
    }

    Ok((schema_id, &payload[header_len..]))
}

pub(crate) struct SchemaRegistryCtx {
    pub username: Option<String>,
    pub password: Option<String>,
    pub client: reqwest::Client,
    pub path: Vec<String>,
}

pub(crate) async fn req_inner<T>(ctx: Arc<SchemaRegistryCtx>, url: Url, method: Method) -> Result<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    url.clone()
        .path_segments_mut()
        .expect("constructor validated URL can be a base")
        .clear()
        .extend(&ctx.path);

    let mut request_builder = ctx.client.request(method, url);

    if let Some(ref username) = ctx.username {
        request_builder = request_builder.basic_auth(username, ctx.password.as_ref());
    }
    request(request_builder).await
}

async fn request<T>(req: reqwest::RequestBuilder) -> Result<T>
where
    T: DeserializeOwned,
{
    let res = req.send().await.map_err(|e| {
        RwError::from(ProtocolError(format!(
            "confluent registry send req error {}",
            e
        )))
    })?;
    let status = res.status();
    if status.is_success() {
        res.json().await.map_err(|e| {
            RwError::from(ProtocolError(format!(
                "confluent registry parse resp error {}",
                e
            )))
        })
    } else {
        let res = res.json::<ErrorResp>().await.map_err(|e| {
            RwError::from(ProtocolError(format!(
                "confluent registry resp error {}",
                e
            )))
        })?;
        Err(RwError::from(ProtocolError(format!(
            "confluent registry resp error, code: {}, msg {}",
            res.error_code, res.message
        ))))
    }
}

/// `Schema` format of confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct ConfluentSchema {
    /// The id of the schema
    pub id: i32,
    /// The raw text of the schema def
    pub content: String,
}

/// `Subject` stored in confluent schema registry
#[derive(Debug, Eq, PartialEq)]
pub struct Subject {
    /// The version of the current schema
    pub version: i32,
    /// The name of the schema
    pub name: String,
    /// The schema corresponding to that `version`
    pub schema: ConfluentSchema,
}

/// One schema can reference another schema
/// (e.g., import "other.proto" in protobuf)
#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaReference {
    /// The name of the reference.
    pub name: String,
    /// The subject that the referenced schema belongs to
    pub subject: String,
    /// The version of the referenced schema
    pub version: i32,
}

#[derive(Debug, Deserialize)]
pub struct GetByIdResp {
    pub schema: String,
}

#[derive(Debug, Deserialize)]
pub struct GetBySubjectResp {
    pub id: i32,
    pub schema: String,
    pub version: i32,
    pub subject: String,
    // default to empty/non-reference
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

#[derive(Debug, Deserialize)]
struct ErrorResp {
    error_code: i32,
    message: String,
}

#[derive(Debug)]
enum ReqResp<T> {
    Succeed(T),
    Failed(ErrorResp),
}
