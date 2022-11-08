// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::path::Path;

use apache_avro::Schema;
use crossbeam_skiplist::map::Entry;
use crossbeam_skiplist::SkipMap;
use risingwave_common::error::ErrorCode::{
    InternalError, InvalidConfigValue, InvalidParameterValue, ProtocolError,
};
use risingwave_common::error::{Result, RwError};
use risingwave_connector::aws_utils::{default_conn_config, s3_client, AwsConfigV2};
use url::Url;

use crate::parser::schema_registry::Client;

const AVRO_SCHEMA_LOCATION_S3_REGION: &str = "region";

/// Read schema from s3 bucket.
/// S3 file location format: <s3://bucket_name/file_name>
pub(super) async fn read_schema_from_s3(
    url: &Url,
    properties: HashMap<String, String>,
) -> Result<String> {
    let bucket = url
        .domain()
        .ok_or_else(|| RwError::from(InternalError(format!("Illegal Avro schema path {}", url))))?;
    if properties.get(AVRO_SCHEMA_LOCATION_S3_REGION).is_none() {
        return Err(RwError::from(InvalidConfigValue {
            config_entry: AVRO_SCHEMA_LOCATION_S3_REGION.to_string(),
            config_value: "NONE".to_string(),
        }));
    }
    let key = url.path().replace('/', "");
    let config = AwsConfigV2::from(properties.clone());
    let sdk_config = config.load_config(None).await;
    let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
    let response = s3_client
        .get_object()
        .bucket(bucket.to_string())
        .key(key)
        .send()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;
    let body_bytes = response.body.collect().await.map_err(|e| {
        RwError::from(InternalError(format!(
            "Read Avro schema file from s3 {}",
            e
        )))
    })?;
    let schema_bytes = body_bytes.into_bytes().to_vec();
    String::from_utf8(schema_bytes)
        .map_err(|e| RwError::from(InternalError(format!("Avro schema not valid utf8 {}", e))))
}

/// Read avro schema file from local file.For on-premise or testing.
pub(super) fn read_schema_from_local(path: impl AsRef<Path>) -> Result<String> {
    std::fs::read_to_string(path.as_ref()).map_err(|e| e.into())
}

/// Read avro schema file from local file.For common usage.
pub(super) async fn read_schema_from_https(location: &Url) -> Result<String> {
    let res = reqwest::get(location.clone()).await.map_err(|e| {
        InvalidParameterValue(format!(
            "failed to make request to URL: {}, err: {}",
            location, e
        ))
    })?;
    if !res.status().is_success() {
        return Err(RwError::from(InvalidParameterValue(format!(
            "Http request err, URL: {}, status code: {}",
            location,
            res.status()
        ))));
    }
    let body = res
        .bytes()
        .await
        .map_err(|e| InvalidParameterValue(format!("failed to read HTTP body: {}", e)))?;

    String::from_utf8(body.into()).map_err(|e| {
        RwError::from(InternalError(format!(
            "read schema string from https failed {}",
            e
        )))
    })
}

#[derive(Debug)]
pub struct ConfluentSchemaResolver {
    writer_schemas: SkipMap<i32, Schema>,
    confluent_client: Client,
}

impl ConfluentSchemaResolver {
    // return the reader schema and a new `SchemaResolver`
    pub async fn new(subject_name: &str, client: Client) -> Result<(Schema, Self)> {
        let cf_schema = client.get_schema_by_subject(subject_name).await?;
        let schema = Schema::parse_str(&cf_schema.raw)
            .map_err(|e| RwError::from(ProtocolError(format!("Avro schema parse error {}", e))))?;
        let resolver = ConfluentSchemaResolver {
            writer_schemas: SkipMap::new(),
            confluent_client: client,
        };
        resolver.writer_schemas.insert(cf_schema.id, schema.clone());
        Ok((schema, resolver))
    }

    // get the writer schema by id
    pub async fn get(&self, schema_id: i32) -> Result<Entry<'_, i32, Schema>> {
        if let Some(entry) = self.writer_schemas.get(&schema_id) {
            Ok(entry)
        } else {
            let cf_schema = self.confluent_client.get_schema_by_id(schema_id).await?;

            let schema = Schema::parse_str(&cf_schema.raw).map_err(|e| {
                RwError::from(ProtocolError(format!("Avro schema parse error {}", e)))
            })?;
            Ok(self.writer_schemas.get_or_insert(schema_id, schema))
        }
    }
}
