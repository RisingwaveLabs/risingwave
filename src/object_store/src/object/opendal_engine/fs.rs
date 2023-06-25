// Copyright 2023 RisingWave Labs
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

use std::time::Duration;

use opendal::services::Fs;
use opendal::Operator;

use super::{config_retry, EngineType, OpendalObjectStore};
use crate::object::ObjectResult;
impl OpendalObjectStore {
    /// create opendal fs engine.
    pub fn new_fs_engine(root: String) -> ObjectResult<Self> {
        // Create fs backend builder.
        let mut builder = Fs::default();

        builder.root(&root);

        let op: Operator =
            config_retry(Operator::new(builder)?, 2.0, Duration::from_secs(1), 3).finish();
        Ok(Self {
            op,
            engine_type: EngineType::Fs,
        })
    }
}
