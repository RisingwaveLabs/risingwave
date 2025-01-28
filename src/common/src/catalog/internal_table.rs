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

use std::any::type_name;
use std::fmt::Debug;

use anyhow::anyhow;
use itertools::Itertools;

pub const RW_INTERNAL_TABLE_FUNCTION_NAME: &str = "rw_table";

pub fn generate_internal_table_name_with_type(
    job_name: &str,
    fragment_id: u32,
    table_id: u32,
    table_type: &str,
) -> String {
    format!(
        "__internal_{}_{}_{}_{}",
        job_name,
        fragment_id,
        table_type.to_lowercase(),
        table_id
    )
}

/// TODO(kwannoel): Test if there's underscores in the job name
pub fn internal_table_name_to_parts(table_name: &str) -> Option<(&str, u32, &str, u32)> {
    let parts: Vec<&str> = table_name.split('_').collect();
    println!("parts: {:?}", parts);
    if parts.len() == 7 && parts[2] == "internal" {
        let job_name = parts[3];
        let fragment_id = parts[4].parse().ok()?;
        let table_type = parts[5];
        let table_id = parts[6].parse().ok()?;
        Some((job_name, fragment_id, table_type, table_id))
    } else {
        None
    }
}

pub fn get_dist_key_in_pk_indices<I: Eq + Copy + Debug, O: TryFrom<usize>>(
    dist_key_indices: &[I],
    pk_indices: &[I],
) -> anyhow::Result<Vec<O>> {
    dist_key_indices
        .iter()
        .map(|&di| {
            pk_indices
                .iter()
                .position(|&pi| di == pi)
                .ok_or_else(|| {
                    anyhow!(
                        "distribution key {:?} must be a subset of primary key {:?}",
                        dist_key_indices,
                        pk_indices
                    )
                })
                .map(|idx| match O::try_from(idx) {
                    Ok(idx) => idx,
                    Err(_) => unreachable!("failed to cast {} to {}", idx, type_name::<O>()),
                })
        })
        .try_collect()
}

/// Get distribution key start index in pk, and return None if `dist_key_in_pk_indices` is not empty
/// or continuous.
/// Note that `dist_key_in_pk_indices` may be shuffled, the start index should be the
/// minimum value.
pub fn get_dist_key_start_index_in_pk(dist_key_in_pk_indices: &[usize]) -> Option<usize> {
    let mut sorted_dist_key = dist_key_in_pk_indices.iter().sorted();
    if let Some(min_idx) = sorted_dist_key.next() {
        let mut prev_idx = min_idx;
        for idx in sorted_dist_key {
            if *idx != prev_idx + 1 {
                return None;
            }
            prev_idx = idx;
        }
        Some(*min_idx)
    } else {
        None
    }
}
