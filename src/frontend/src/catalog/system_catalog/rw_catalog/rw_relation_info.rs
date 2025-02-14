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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::catalog::OwnedByUserCatalog;
use crate::error::Result;
use crate::user::has_access_to_object;

// TODO: `rw_relation_info` contains some extra streaming meta info that's only meaningful for
// streaming jobs, we'd better query relation infos from `rw_relations` and move these streaming
// infos into anther system table.
#[derive(Fields)]
#[primary_key(schemaname, relationname)]
struct RwRelationInfo {
    schemaname: String,
    relationname: String,
    relationowner: i32,
    definition: String,
    relationtype: String,
    relationid: i32,
    relationtimezone: String, // The timezone used to interpret ambiguous dates/timestamps as tstz
    fragments: Option<String>, // fragments is json encoded fragment infos.
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_relation_info")]
async fn read_relation_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwRelationInfo>> {
    let mut table_ids = Vec::new();
    {
        let catalog_reader = reader.catalog_reader.read_guard();
        let schemas = catalog_reader.get_all_schema_names(&reader.auth_context.database)?;
        let user_reader = reader.user_info_reader.read_guard();
        let current_user = user_reader
            .get_user_by_name(&reader.auth_context.user_name)
            .expect("user not found");
        for schema in &schemas {
            let schema_catalog =
                catalog_reader.get_schema_by_name(&reader.auth_context.database, schema)?;

            schema_catalog
                .iter_created_mvs()
                .filter(|m| {
                    has_access_to_object(current_user, &schema_catalog.name, m.id.table_id, m.owner)
                })
                .for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

            schema_catalog
                .iter_user_table()
                .filter(|t| {
                    has_access_to_object(current_user, &schema_catalog.name, t.id.table_id, t.owner)
                })
                .for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

            schema_catalog
                .iter_source()
                .filter(|s| has_access_to_object(current_user, &schema_catalog.name, s.id, s.owner))
                .filter(|s| s.info.is_shared())
                .for_each(|s| {
                    table_ids.push(s.id);
                });

            schema_catalog
                .iter_sink()
                .filter(|s| {
                    has_access_to_object(
                        current_user,
                        &schema_catalog.name,
                        s.id.sink_id,
                        s.owner.user_id,
                    )
                })
                .for_each(|t| {
                    table_ids.push(t.id.sink_id);
                });

            schema_catalog
                .iter_index()
                .filter(|idx| {
                    has_access_to_object(
                        current_user,
                        &schema_catalog.name,
                        idx.id.index_id,
                        idx.owner(),
                    )
                })
                .for_each(|t| {
                    table_ids.push(t.index_table.id.table_id);
                });
        }
    }

    let table_fragments = reader.meta_client.list_table_fragments(&table_ids).await?;
    let mut rows = Vec::new();
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.get_all_schema_names(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    for schema in &schemas {
        let schema_catalog =
            catalog_reader.get_schema_by_name(&reader.auth_context.database, schema)?;
        schema_catalog
            .iter_created_mvs()
            .filter(|m| {
                has_access_to_object(current_user, &schema_catalog.name, m.id.table_id, m.owner)
            })
            .for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(RwRelationInfo {
                        schemaname: schema.clone(),
                        relationname: t.name.clone(),
                        relationowner: t.owner as i32,
                        definition: t.create_sql(),
                        relationtype: "MATERIALIZED VIEW".into(),
                        relationid: t.id.table_id as i32,
                        relationtimezone: fragments.get_ctx().unwrap().get_timezone().clone(),
                        fragments: Some(json!(fragments.get_fragments()).to_string()),
                        initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                        created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                        initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                        created_at_cluster_version: t.created_at_cluster_version.clone(),
                    });
                }
            });

        schema_catalog
            .iter_user_table()
            .filter(|t| {
                has_access_to_object(current_user, &schema_catalog.name, t.id.table_id, t.owner)
            })
            .for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(RwRelationInfo {
                        schemaname: schema.clone(),
                        relationname: t.name.clone(),
                        relationowner: t.owner as i32,
                        definition: t.create_sql_purified(),
                        relationtype: "TABLE".into(),
                        relationid: t.id.table_id as i32,
                        relationtimezone: fragments.get_ctx().unwrap().get_timezone().clone(),
                        fragments: Some(json!(fragments.get_fragments()).to_string()),
                        initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                        created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                        initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                        created_at_cluster_version: t.created_at_cluster_version.clone(),
                    });
                }
            });

        schema_catalog
            .iter_sink()
            .filter(|s| {
                has_access_to_object(
                    current_user,
                    &schema_catalog.name,
                    s.id.sink_id,
                    s.owner.user_id,
                )
            })
            .for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.sink_id) {
                    rows.push(RwRelationInfo {
                        schemaname: schema.clone(),
                        relationname: t.name.clone(),
                        relationowner: t.owner.user_id as i32,
                        definition: t.definition.clone(),
                        relationtype: "SINK".into(),
                        relationid: t.id.sink_id as i32,
                        relationtimezone: fragments.get_ctx().unwrap().get_timezone().clone(),
                        fragments: Some(json!(fragments.get_fragments()).to_string()),
                        initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                        created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                        initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                        created_at_cluster_version: t.created_at_cluster_version.clone(),
                    });
                }
            });

        schema_catalog
            .iter_index()
            .filter(|idx| {
                has_access_to_object(
                    current_user,
                    &schema_catalog.name,
                    idx.id.index_id,
                    idx.owner(),
                )
            })
            .for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.index_table.id.table_id) {
                    rows.push(RwRelationInfo {
                        schemaname: schema.clone(),
                        relationname: t.name.clone(),
                        relationowner: t.index_table.owner as i32,
                        definition: t.index_table.create_sql(),
                        relationtype: "INDEX".into(),
                        relationid: t.index_table.id.table_id as i32,
                        relationtimezone: fragments.get_ctx().unwrap().get_timezone().clone(),
                        fragments: Some(json!(fragments.get_fragments()).to_string()),
                        initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                        created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                        initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                        created_at_cluster_version: t.created_at_cluster_version.clone(),
                    });
                }
            });

        // Sources have no fragments.
        schema_catalog
            .iter_source()
            .filter(|s| has_access_to_object(current_user, &schema_catalog.name, s.id, s.owner))
            .for_each(|t| {
                let (timezone, fragments) = if t.info.is_shared()
                    && let Some(fragments) = table_fragments.get(&t.id)
                {
                    (
                        fragments.get_ctx().unwrap().get_timezone().clone(),
                        Some(json!(fragments.get_fragments()).to_string()),
                    )
                } else {
                    ("".into(), None)
                };

                rows.push(RwRelationInfo {
                    schemaname: schema.clone(),
                    relationname: t.name.clone(),
                    relationowner: t.owner as i32,
                    definition: t.create_sql_purified(),
                    relationtype: "SOURCE".into(),
                    relationid: t.id as i32,
                    relationtimezone: timezone,
                    fragments,
                    initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                    created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                    initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                    created_at_cluster_version: t.created_at_cluster_version.clone(),
                });
            });

        schema_catalog
            .iter_subscription()
            .filter(|s| {
                has_access_to_object(
                    current_user,
                    &schema_catalog.name,
                    s.id.subscription_id,
                    s.owner.user_id,
                )
            })
            .for_each(|t| {
                rows.push(RwRelationInfo {
                    schemaname: schema.clone(),
                    relationname: t.name.clone(),
                    relationowner: t.owner.user_id as i32,
                    definition: t.definition.clone(),
                    relationtype: "SUBSCRIPTION".into(),
                    relationid: t.id.subscription_id as i32,
                    relationtimezone: "".into(),
                    fragments: None,
                    initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                    created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                    initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                    created_at_cluster_version: t.created_at_cluster_version.clone(),
                });
            });
    }

    Ok(rows)
}
