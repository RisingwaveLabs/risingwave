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

use itertools::Itertools;
use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwActorInfo {
    #[primary_key]
    actor_id: i32,
    fragment_id: i32,
    dispatcher: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_actor_infos")]
async fn read_rw_actors(reader: &SysCatalogReaderImpl) -> Result<Vec<RwActorInfo>> {
    let table_ids = reader
        .meta_client
        .list_streaming_job_states()
        .await?
        .into_iter()
        .map(|fragment| fragment.table_id)
        .collect_vec();
    let table_fragments = reader.meta_client.list_table_fragments(&table_ids).await?;
    Ok(table_fragments
        .into_iter()
        .flat_map(|(_, fragment_info)| {
            fragment_info.fragments.into_iter().flat_map(|fragment| {
                let fragment_id = fragment.id;
                fragment.actors.into_iter().map(move |actor| RwActorInfo {
                    actor_id: actor.id as _,
                    fragment_id: fragment_id as _,
                    dispatcher: json!(actor.dispatcher).into(),
                })
            })
        })
        .collect())
}
