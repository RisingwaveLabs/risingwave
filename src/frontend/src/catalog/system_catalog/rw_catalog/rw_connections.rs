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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwConnection {
    #[primary_key]
    id: i32,
    name: String,
    schema_id: i32,
    owner: i32,
    type_: String,
    provider: String,
    acl: String,
}

#[system_catalog(table, "rw_catalog.rw_connections")]
fn read_rw_connections(reader: &SysCatalogReaderImpl) -> Result<Vec<RwConnection>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_connections().map(|conn| RwConnection {
                id: conn.id as i32,
                name: conn.name.clone(),
                schema_id: schema.id() as i32,
                owner: conn.owner as i32,
                type_: conn.connection_type().into(),
                provider: conn.provider().into(),
                acl: "".into(),
            })
        })
        .collect())
}
