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

use risingwave_common::catalog::SecretId;
use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::user::grant_privilege::Object;
use serde_json::{json, Map as JsonMap};

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::{get_acl_items, SysCatalogReaderImpl};
use crate::error::Result;
use crate::handler::create_source::UPSTREAM_SOURCE_KEY;
use crate::user::has_access_to_object;
use crate::WithOptionsSecResolved;

#[derive(Fields)]
struct RwSource {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    connector: String,
    columns: Vec<String>,
    format: Option<String>,
    row_encode: Option<String>,
    append_only: bool,
    associated_table_id: Option<i32>,
    connection_id: Option<i32>,
    definition: String,
    acl: Vec<String>,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
    is_shared: bool,
    // connector properties in json format
    connector_props: JsonbVal,
    // format-encode options in json format
    format_encode_options: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_sources")]
fn read_rw_sources_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwSource>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema
                .iter_source()
                .filter(|s| has_access_to_object(current_user, &schema.name, s.id, s.owner))
                .map(|source| {
                    let format_encode_props_with_secrets = WithOptionsSecResolved::new(
                        source.info.format_encode_options.clone(),
                        source.info.format_encode_secret_refs.clone(),
                    );
                    RwSource {
                        id: source.id as i32,
                        name: source.name.clone(),
                        schema_id: schema.id() as i32,
                        owner: source.owner as i32,
                        connector: source
                            .with_properties
                            .get(UPSTREAM_SOURCE_KEY)
                            .cloned()
                            .unwrap_or("".to_owned())
                            .to_uppercase(),
                        columns: source.columns.iter().map(|c| c.name().into()).collect(),
                        format: source
                            .info
                            .get_format()
                            .ok()
                            .map(|format| format.as_str_name().into()),
                        row_encode: source
                            .info
                            .get_row_encode()
                            .ok()
                            .map(|row_encode| row_encode.as_str_name().into()),
                        append_only: source.append_only,
                        associated_table_id: source
                            .associated_table_id
                            .map(|id| id.table_id as i32),
                        connection_id: source.connection_id.map(|id| id as i32),
                        definition: source.create_sql_purified(),
                        acl: get_acl_items(
                            &Object::SourceId(source.id),
                            false,
                            &users,
                            username_map,
                        ),
                        initialized_at: source.initialized_at_epoch.map(|e| e.as_timestamptz()),
                        created_at: source.created_at_epoch.map(|e| e.as_timestamptz()),
                        initialized_at_cluster_version: source
                            .initialized_at_cluster_version
                            .clone(),
                        created_at_cluster_version: source.created_at_cluster_version.clone(),
                        is_shared: source.info.is_shared(),

                        connector_props: serialize_props_with_secret(
                            schema,
                            source.with_properties.clone(),
                        )
                        .into(),
                        format_encode_options: serialize_props_with_secret(
                            schema,
                            format_encode_props_with_secrets,
                        )
                        .into(),
                    }
                })
        })
        .collect())
}

pub fn serialize_props_with_secret(
    schema: &SchemaCatalog,
    props_with_secret: WithOptionsSecResolved,
) -> jsonbb::Value {
    let (inner, secret_ref) = props_with_secret.into_parts();
    // if not secret, {"some key": {"type": "plaintext", "value": "xxxx"}}
    // if secret, {"some key": {"type": "secret", "value": {"value": "<secret name>"}}}
    let mut result: JsonMap<String, serde_json::Value> = JsonMap::new();

    for (k, v) in inner {
        result.insert(k, json!({"type": "plaintext", "value": v}));
    }
    for (k, v) in secret_ref {
        let secret_name = schema
            .get_secret_by_id(&SecretId(v.secret_id))
            .unwrap()
            .name
            .clone();
        result.insert(
            k,
            json!({"type": "secret", "value": {"value": secret_name}}),
        );
    }

    jsonbb::Value::from(serde_json::Value::Object(result))
}
