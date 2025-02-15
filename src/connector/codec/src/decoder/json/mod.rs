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
use std::{fs, mem};

use anyhow::Context;
use risingwave_pb::plan_common::ColumnDesc;
use serde_json::Value;
use thiserror::Error;
use url::Url;

use super::avro::{avro_schema_to_column_descs, MapHandling};

#[derive(Debug, Error, thiserror_ext::ContextInto)]
pub enum Error {
    #[error("could not open schema from {filename}")]
    SchemaFromFile {
        filename: String,
        source: std::io::Error,
    },
    #[error("parse error for url {url}")]
    UrlParse {
        url: String,
        source: url::ParseError,
    },
    #[error("schema from {url} not valid JSON")]
    SchemaNotJson { url: String, source: std::io::Error },
    #[error("request error")]
    Request { url: String, source: reqwest::Error },
    #[error("schema from {url} not valid JSON")]
    SchemaNotJsonSerde {
        url: String,
        source: serde_json::Error,
    },
    #[error("ref `{ref_string}` can not be resolved as pointer, `{ref_fragment}` can not be found in the schema")]
    JsonRefPointerNotFound {
        ref_string: String,
        ref_fragment: String,
    },
    #[error("json ref error")]
    JsonRef {
        #[from]
        source: std::io::Error,
    },
    #[error("need url to be a file or a http based, got {url}")]
    UnsupportedUrl { url: String },
    #[error(transparent)]
    Uncategorized(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct JsonRef {
    schema_cache: HashMap<String, Value>,
    reference_key: Option<String>,
}

impl JsonRef {
    fn new() -> JsonRef {
        JsonRef {
            schema_cache: HashMap::new(),
            reference_key: Some("__reference__".to_owned()),
        }
    }

    async fn deref_value(&mut self, value: &mut Value, retrieval_url: &Url) -> Result<()> {
        self.schema_cache
            .insert(retrieval_url.to_string(), value.clone());
        self.deref(value, retrieval_url, &vec![]).await?;
        Ok(())
    }

    async fn deref(
        &mut self,
        value: &mut Value,
        base_url: &Url,
        used_refs: &Vec<String>,
    ) -> Result<()> {
        if let Some(obj) = value.as_object_mut()
            && let Some(ref_value) = obj.remove("$ref")
            && let Some(ref_string) = ref_value.as_str()
        {
            let ref_url = base_url.join(ref_string).into_url_parse(ref_string)?;
            let mut ref_url_no_fragment = ref_url.clone();
            ref_url_no_fragment.set_fragment(None);
            let url_schema = ref_url_no_fragment.scheme();
            let ref_no_fragment = ref_url_no_fragment.to_string();

            let mut schema = match self.schema_cache.get(&ref_no_fragment) {
                Some(cached_schema) => cached_schema.clone(),
                None => {
                    if url_schema == "http" || url_schema == "https" {
                        reqwest::get(ref_url_no_fragment.clone())
                            .await
                            .into_request(&ref_no_fragment)?
                            .json()
                            .await
                            .into_request(&ref_no_fragment)?
                    } else if url_schema == "file" {
                        let file_path = ref_url_no_fragment.to_file_path().map_err(|_| {
                            anyhow::anyhow!(
                                "could not convert url {} to file path",
                                &ref_url_no_fragment
                            )
                        })?;
                        let file =
                            fs::File::open(file_path).into_schema_from_file(&ref_no_fragment)?;
                        serde_json::from_reader(file)
                            .into_schema_not_json_serde(ref_no_fragment.clone())?
                    } else {
                        return Err(Error::UnsupportedUrl {
                            url: ref_no_fragment,
                        });
                    }
                }
            };

            if !self.schema_cache.contains_key(&ref_no_fragment) {
                self.schema_cache
                    .insert(ref_no_fragment.clone(), schema.clone());
            }

            let ref_url_string = ref_url.to_string();
            if let Some(ref_fragment) = ref_url.fragment() {
                schema = schema
                    .pointer(ref_fragment)
                    .ok_or(Error::JsonRefPointerNotFound {
                        ref_string: ref_string.to_owned(),
                        ref_fragment: ref_fragment.to_owned(),
                    })?
                    .clone();
            }
            // Do not deref a url twice to prevent infinite loops
            if used_refs.contains(&ref_url_string) {
                return Ok(());
            }
            let mut new_used_refs = used_refs.clone();
            new_used_refs.push(ref_url_string);

            Box::pin(self.deref(&mut schema, &ref_url_no_fragment, &new_used_refs)).await?;
            let old_value = mem::replace(value, schema);

            if let Some(reference_key) = &self.reference_key {
                if let Some(new_obj) = value.as_object_mut() {
                    new_obj.insert(reference_key.clone(), old_value);
                }
            }
        }

        if let Some(obj) = value.as_object_mut() {
            for obj_value in obj.values_mut() {
                Box::pin(self.deref(obj_value, base_url, used_refs)).await?
            }
        }
        Ok(())
    }
}

impl crate::JsonSchema {
    /// FIXME: when the JSON schema is invalid, it will panic.
    ///
    /// ## Notes on type conversion
    /// Map will be used when an object doesn't have `properties` but has `additionalProperties`.
    /// When an object has `properties` and `additionalProperties`, the latter will be ignored.
    /// <https://github.com/mozilla/jsonschema-transpiler/blob/fb715c7147ebd52427e0aea09b2bba2d539850b1/src/jsonschema.rs#L228-L280>
    ///
    /// TODO: examine other stuff like `oneOf`, `patternProperties`, etc.
    pub async fn json_schema_to_columns(
        &mut self,
        retrieval_url: Url,
    ) -> anyhow::Result<Vec<ColumnDesc>> {
        JsonRef::new()
            .deref_value(&mut self.0, &retrieval_url)
            .await?;
        let avro_schema = jst::convert_avro(&self.0, jst::Context::default()).to_string();
        let schema =
            apache_avro::Schema::parse_str(&avro_schema).context("failed to parse avro schema")?;
        avro_schema_to_column_descs(&schema, Some(MapHandling::Jsonb)).map_err(Into::into)
    }
}
