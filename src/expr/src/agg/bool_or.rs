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

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::ScalarImpl;
use risingwave_expr_macro::aggregate;

/// Returns true if any non-null input value is true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b1 boolean, b2 boolean, b3 boolean, b4 boolean);
///
/// query T
/// select bool_or(b1) from t;
/// ----
/// NULL
///
/// statement ok
/// insert into t values
///     (true,  null, false, null),
///     (false, true, null,  null),
///     (null,  true, false, null);
///
/// query TTTTTT
/// select
///     bool_or(b1),
///     bool_or(b2),
///     bool_or(b3),
///     bool_or(b4),
///     bool_or(NOT b2),
///     bool_or(NOT b3)
/// FROM t;
/// ----
/// t t f NULL f t
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_or(boolean) -> boolean")]
fn bool_or_append_only(state: bool, input: bool) -> bool {
    state || input
}

/// Returns true if any non-null input value is true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b boolean);
///
/// statement ok
/// create materialized view mv as select bool_or(b) from t;
///
/// query T
/// select * from mv;
/// ----
/// NULL
///
/// statement ok
/// insert into t values (true), (false), (null);
///
/// query T
/// select * from mv;
/// ----
/// t
///
/// statement ok
/// delete from t where b is true;
///
/// query T
/// select * from mv;
/// ----
/// f
///
/// statement ok
/// drop materialized view mv;
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_or(boolean) -> boolean", state = "BoolOrState")]
fn bool_or(mut state: BoolOrState, input: bool, retract: bool) -> BoolOrState {
    if input == true {
        if retract {
            state.true_count -= 1;
        } else {
            state.true_count += 1;
        }
    }
    state
}

/// State for retractable `bool_or` aggregate.
#[derive(Debug, Default, Clone, EstimateSize)]
struct BoolOrState {
    true_count: i64,
}

// state -> result
impl From<BoolOrState> for bool {
    fn from(state: BoolOrState) -> Self {
        state.true_count == 0
    }
}

// first input -> state
impl From<bool> for BoolOrState {
    fn from(b: bool) -> Self {
        Self {
            true_count: if b { 1 } else { 0 },
        }
    }
}

impl From<ScalarImpl> for BoolOrState {
    fn from(d: ScalarImpl) -> Self {
        let ScalarImpl::Int64(v) = d else {
            panic!("unexpected state for `bool_or`: {:?}", d);
        };
        Self { true_count: v }
    }
}

impl From<BoolOrState> for ScalarImpl {
    fn from(state: BoolOrState) -> Self {
        ScalarImpl::Int64(state.true_count)
    }
}
