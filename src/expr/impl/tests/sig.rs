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

use std::collections::HashMap;

risingwave_expr_impl::enable!();

use itertools::Itertools;
use risingwave_expr::sig::{FuncName, FuncSign, SigDataType, FUNCTION_REGISTRY};
#[test]
fn test_func_sig_map() {
    // convert FUNC_SIG_MAP to a more convenient map for testing
    let mut new_map: HashMap<FuncName, HashMap<Vec<SigDataType>, Vec<FuncSign>>> = HashMap::new();
    for sig in FUNCTION_REGISTRY.iter_scalars() {
        // exclude deprecated functions
        if sig.deprecated {
            continue;
        }

        new_map
            .entry(sig.name.clone())
            .or_default()
            .entry(sig.inputs_type.to_vec())
            .or_default()
            .push(sig.clone());
    }

    let mut duplicated: Vec<_> = new_map
        .into_values()
        .flat_map(|funcs_with_same_name| {
            funcs_with_same_name.into_values().filter_map(|v| {
                if v.len() > 1 {
                    Some(format!(
                        "{}({}) -> {}",
                        v[0].name.as_str_name().to_ascii_lowercase(),
                        v[0].inputs_type.iter().format(", "),
                        v.iter().map(|sig| &sig.ret_type).format("/")
                    ))
                } else {
                    None
                }
            })
        })
        .collect();
    duplicated.sort();

    // This snapshot shows the function signatures without a unique match. Frontend has to
    // handle them specially without relying on FuncSigMap.
    let expected = expect_test::expect![[r#"
        [
            "cast(anyarray) -> character varying/anyarray",
            "cast(bigint) -> rw_int256/serial/integer/smallint/numeric/double precision/real/character varying",
            "cast(boolean) -> integer/character varying",
            "cast(character varying) -> timestamp_ns without time zone/jsonb/interval/timestamp without time zone/time without time zone/date/rw_int256/real/double precision/numeric/smallint/integer/bigint/character varying/boolean/bytea/anyarray",
            "cast(date) -> timestamp_ns without time zone/timestamp without time zone/character varying",
            "cast(double precision) -> numeric/real/bigint/integer/smallint/character varying",
            "cast(integer) -> rw_int256/smallint/numeric/double precision/real/bigint/boolean/character varying",
            "cast(interval) -> time without time zone/character varying",
            "cast(jsonb) -> boolean/double precision/real/numeric/bigint/integer/smallint/character varying",
            "cast(numeric) -> double precision/real/bigint/integer/smallint/character varying",
            "cast(real) -> numeric/bigint/integer/smallint/double precision/character varying",
            "cast(rw_int256) -> double precision/character varying",
            "cast(smallint) -> rw_int256/numeric/double precision/real/bigint/integer/character varying",
            "cast(time without time zone) -> interval/character varying",
            "cast(timestamp without time zone) -> timestamp_ns without time zone/time without time zone/date/character varying",
            "cast(timestamp_ns without time zone) -> timestamp without time zone/time without time zone/date/character varying",
            "greatest() -> bytea/character varying/timestamp with time zone/timestamp without time zone/interval/time without time zone/date/rw_int256/serial/real/double precision/numeric/smallint/integer/bigint/boolean",
            "least() -> bytea/character varying/timestamp with time zone/timestamp without time zone/interval/time without time zone/date/rw_int256/serial/real/double precision/numeric/smallint/integer/bigint/boolean",
        ]
    "#]];
    expected.assert_debug_eq(&duplicated);
}
