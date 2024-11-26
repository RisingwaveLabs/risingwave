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

use risingwave_common::types::Timestamptz;
use risingwave_common::util::epoch::Epoch;
use risingwave_expr::{function, Result};

#[function("rw_epoch_to_ts(int8) -> timestamptz")]
fn rw_epoch_to_ts(epoch: i64) -> Result<Timestamptz> {
    Ok(Epoch(epoch as u64).as_timestamptz())
}
