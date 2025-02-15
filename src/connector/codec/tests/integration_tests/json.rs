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

use std::fs;

use risingwave_common::catalog::ColumnDesc;
use risingwave_connector_codec::JsonSchema;
use url::Url;

use crate::utils::{ColumnDescTestDisplay, *};

#[tokio::test]
async fn test_json_schema_parse() {
    let test_id_type_file_path = fs::canonicalize("tests/test_data/id_type.json")
        .unwrap()
        .to_string_lossy()
        .to_string();
    let test_id_type_http_url = "https://gist.githubusercontent.com/yuhao-su/a1b23e4073b4f1ca4e614c89a785575d/raw/ec8ccd6b3bcf6fafe5c57173a4fdf4129c63625d/idType.txt";

    let schema = format!(
        r##" {{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "definitions": {{
            "stringType": {{
              "type": "string"
            }},
            "marketObj": {{
              "type": "object",
              "additionalProperties": {{
                "$ref": "{test_id_type_http_url}#/definitions/idType"
              }}
            }},
            "marketArray": {{
              "type": "object",
              "additionalProperties": {{
                "type": "array",
                "items": {{
                  "type": "string"
                }}
              }}
            }}
          }},
          "properties": {{
            "id": {{
              "$ref": "file://{test_id_type_file_path}#/definitions/idType"
            }},
            "name": {{
              "$ref": "#/definitions/marketObj",
              "description": "Name of the market subject"
            }},
            "cats": {{
              "$ref": "#/definitions/marketArray"
            }},
            "meta": {{
              "type": "object",
              "properties": {{
                "active": {{
                  "$ref": "#/definitions/marketObj"
                }},
                "tags": {{
                  "$ref": "#/definitions/marketArray"
                }}
              }}
            }}
          }},
          "required": [
            "id",
            "name"
          ]
        }}"##
    );

    let mut json_schema = JsonSchema::parse_str(&schema).unwrap();

    let columns = json_schema
        .json_schema_to_columns(Url::parse("http://test_schema_uri.test").unwrap())
        .await
        .unwrap()
        .into_iter()
        .map(|c| ColumnDesc::from(c))
        .collect_vec();
    let column_display = columns
        .iter()
        .map(|c| ColumnDescTestDisplay(c))
        .collect_vec();

    expect![[r#"
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "definitions": {
            "marketArray": {
              "additionalProperties": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "type": "object"
            },
            "marketObj": {
              "additionalProperties": {
                "__reference__": {},
                "type": "string"
              },
              "type": "object"
            },
            "stringType": {
              "type": "string"
            }
          },
          "properties": {
            "cats": {
              "__reference__": {},
              "additionalProperties": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "type": "object"
            },
            "id": {
              "__reference__": {},
              "type": "string"
            },
            "meta": {
              "properties": {
                "active": {
                  "__reference__": {},
                  "additionalProperties": {
                    "__reference__": {},
                    "type": "string"
                  },
                  "type": "object"
                },
                "tags": {
                  "__reference__": {},
                  "additionalProperties": {
                    "items": {
                      "type": "string"
                    },
                    "type": "array"
                  },
                  "type": "object"
                }
              },
              "type": "object"
            },
            "name": {
              "__reference__": {
                "description": "Name of the market subject"
              },
              "additionalProperties": {
                "__reference__": {},
                "type": "string"
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "name"
          ],
          "type": "object"
        }"#]]
    .assert_eq(&serde_json::to_string_pretty(&json_schema.0).unwrap());

    expect![[r#"
            [
                cats(#1): Jsonb,
                id(#2): Varchar,
                meta(#3): Struct {
                    active: Jsonb,
                    tags: Jsonb,
                },
                name(#4): Jsonb,
            ]
        "#]]
    .assert_debug_eq(&column_display);
}
