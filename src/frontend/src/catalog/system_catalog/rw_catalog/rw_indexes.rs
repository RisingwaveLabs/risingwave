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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwIndex {
    #[primary_key]
    id: i32,
    name: String,
    primary_table_id: i32,
    indkey: Vec<i16>,
    schema_id: i32,
    owner: i32,
    definition: String,
    acl: String,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_indexes")]
fn read_rw_indexes(reader: &SysCatalogReaderImpl) -> Result<Vec<RwIndex>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_index().map(|index| RwIndex {
                id: index.id.index_id as i32,
                name: index.name.clone(),
                primary_table_id: index.primary_table.id().table_id as i32,
                indkey: index
                    .index_item
                    .iter()
                    .take(index.index_columns_len as usize)
                    .map(|index| {
                        let ind = if let Some(input_ref) = index.as_input_ref() {
                            input_ref.index() + 1
                        } else {
                            0
                        };
                        ind as i16
                    })
                    .collect(),
                schema_id: schema.id() as i32,
                owner: index.index_table.owner as i32,
                definition: index.index_table.create_sql(),
                acl: "".into(),
                initialized_at: index.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: index.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: index.initialized_at_cluster_version.clone(),
                created_at_cluster_version: index.created_at_cluster_version.clone(),
            })
        })
        .collect())
}
