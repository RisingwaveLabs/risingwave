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

use risingwave_common::bail;
use risingwave_expr_macro::aggregate;
use serde_json::Value;

#[aggregate("jsonb_agg(boolean) -> jsonb", state = "Value")]
#[aggregate("jsonb_agg(*int) -> jsonb", state = "Value")]
#[aggregate("jsonb_agg(*float) -> jsonb", state = "Value")]
#[aggregate("jsonb_agg(varchar) -> jsonb", state = "Value")]
#[aggregate("jsonb_agg(jsonb) -> jsonb", state = "Value")]
fn jsonb_agg(state: Option<Value>, input: Option<impl Into<Value>>) -> Value {
    let mut array = match state {
        Some(Value::Array(a)) => a,
        None => Vec::with_capacity(1),
        _ => unreachable!("invalid jsonb state"),
    };
    array.push(input.map_or(Value::Null, Into::into));
    Value::Array(array)
}
