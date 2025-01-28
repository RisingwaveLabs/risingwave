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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::stream_plan::FragmentTypeFlag;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwFragment {
    #[primary_key]
    fragment_id: i32,
    table_id: i32,
    distribution_type: String,
    state_table_ids: Vec<i32>,
    upstream_fragment_ids: Vec<i32>,
    flags: Vec<String>,
    parallelism: i32,
    max_parallelism: i32,
    node: JsonbVal,
}

pub(super) fn extract_fragment_type_flag(mask: u32) -> Vec<FragmentTypeFlag> {
    let mut result = vec![];
    for i in 0..32 {
        let bit = 1 << i;
        if mask & bit != 0 {
            match FragmentTypeFlag::try_from(bit as i32) {
                Err(_) => continue,
                Ok(flag) => result.push(flag),
            };
        }
    }
    result
}

#[system_catalog(table, "rw_catalog.rw_fragments")]
async fn read_rw_fragment(reader: &SysCatalogReaderImpl) -> Result<Vec<RwFragment>> {
    let distributions = reader.meta_client.list_fragment_distribution().await?;

    Ok(distributions
        .into_iter()
        .map(|distribution| RwFragment {
            fragment_id: distribution.fragment_id as i32,
            table_id: distribution.table_id as i32,
            distribution_type: distribution.distribution_type().as_str_name().into(),
            state_table_ids: distribution
                .state_table_ids
                .into_iter()
                .map(|id| id as i32)
                .collect(),
            upstream_fragment_ids: distribution
                .upstream_fragment_ids
                .into_iter()
                .map(|id| id as i32)
                .collect(),
            flags: extract_fragment_type_flag(distribution.fragment_type_mask)
                .into_iter()
                .flat_map(|t| t.as_str_name().strip_prefix("FRAGMENT_TYPE_FLAG_"))
                .map(|s| s.into())
                .collect(),
            parallelism: distribution.parallelism as i32,
            max_parallelism: distribution.vnode_count as i32,
            node: json!(distribution.node).into(),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use risingwave_pb::stream_plan::FragmentTypeFlag;

    use super::extract_fragment_type_flag;

    #[test]
    fn test_extract_mask() {
        let mask = (FragmentTypeFlag::Source as u32) | (FragmentTypeFlag::StreamScan as u32);
        let result = extract_fragment_type_flag(mask);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&FragmentTypeFlag::Source));
        assert!(result.contains(&FragmentTypeFlag::StreamScan))
    }
}
