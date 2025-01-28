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

#[system_catalog(
    view,
    "rw_catalog.rw_streaming_parallelism",
    "WITH all_relations AS (
        SELECT id, name, 'table' as relation_type FROM rw_tables
        UNION ALL
        SELECT id, name, 'materialized view' as relation_type FROM rw_materialized_views
        UNION ALL
        SELECT id, name, 'sink' as relation_type FROM rw_sinks
        UNION ALL
        SELECT id, name, 'index' as relation_type FROM rw_indexes
        UNION ALL
        SELECT id, name, 'source' as relation_type FROM rw_sources WHERE is_shared = true
    )
    SELECT
        relation.id,
        relation.name,
        relation.relation_type,
        job.parallelism,
        job.max_parallelism
    FROM all_relations relation
    INNER JOIN rw_streaming_jobs job ON relation.id = job.id
    ORDER BY relation.id"
)]
#[derive(Fields)]
struct RwStreamingParallelism {
    id: i32,
    name: String,
    relation_type: String,
    parallelism: String,
    max_parallelism: i32,
}
