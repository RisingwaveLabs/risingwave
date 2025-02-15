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

use crate::catalog::system_catalog::{extract_parallelism_from_table_state, SysCatalogReaderImpl};
use crate::error::Result;

#[derive(Fields)]
struct RwStreamingJob {
    #[primary_key]
    id: i32,
    name: String,
    status: String,
    parallelism: String,
    max_parallelism: i32,
    resource_group: String,
}

#[system_catalog(table, "rw_catalog.rw_streaming_jobs")]
async fn read_rw_streaming_jobs(reader: &SysCatalogReaderImpl) -> Result<Vec<RwStreamingJob>> {
    let states = reader.meta_client.list_streaming_job_states().await?;

    Ok(states
        .into_iter()
        .map(|state| {
            let parallelism = extract_parallelism_from_table_state(&state);
            RwStreamingJob {
                id: state.table_id as i32,
                status: state.state().as_str_name().into(),
                name: state.name,
                parallelism: parallelism.to_uppercase(),
                max_parallelism: state.max_parallelism as i32,
                resource_group: state.resource_group,
            }
        })
        .collect())
}
