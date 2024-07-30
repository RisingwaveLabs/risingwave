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

/// Get the rate-limited max chunk size.
pub(crate) fn limited_chunk_size(rate_limit: Option<u32>) -> usize {
    let config_chunk_size = crate::config::chunk_size();
    rate_limit
        .map(|limit| config_chunk_size.min(limit as usize))
        .unwrap_or(config_chunk_size)
}
