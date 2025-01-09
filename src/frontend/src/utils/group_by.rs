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

use auto_enums::auto_enum;

use crate::expr::ExprImpl;

#[derive(Debug, Clone)]
pub enum GroupBy {
    GroupKey(Vec<ExprImpl>),
    GroupingSets(Vec<Vec<ExprImpl>>),
    Rollup(Vec<Vec<ExprImpl>>),
    Cube(Vec<Vec<ExprImpl>>),
}

impl GroupBy {
    pub fn is_empty(&self) -> bool {
        match self {
            GroupBy::GroupKey(group_key) => group_key.is_empty(),
            GroupBy::GroupingSets(grouping_sets) => grouping_sets.is_empty(),
            GroupBy::Rollup(rollup) => rollup.is_empty(),
            GroupBy::Cube(cube) => cube.is_empty(),
        }
    }

    #[auto_enum(Iterator)]
    pub fn iter(&self) -> impl Iterator<Item = &ExprImpl> {
        match self {
            GroupBy::GroupKey(group_key) => group_key.iter(),
            GroupBy::GroupingSets(grouping_sets) => grouping_sets.iter().flat_map(|v| v.iter()),
            GroupBy::Rollup(rollup) => rollup.iter().flat_map(|v| v.iter()),
            GroupBy::Cube(cube) => cube.iter().flat_map(|v| v.iter()),
        }
    }

    #[auto_enum(Iterator)]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ExprImpl> {
        match self {
            GroupBy::GroupKey(group_key) => group_key.iter_mut(),
            GroupBy::GroupingSets(grouping_sets) => {
                grouping_sets.iter_mut().flat_map(|v| v.iter_mut())
            }
            GroupBy::Rollup(rollup) => rollup.iter_mut().flat_map(|v| v.iter_mut()),
            GroupBy::Cube(cube) => cube.iter_mut().flat_map(|v| v.iter_mut()),
        }
    }
}
