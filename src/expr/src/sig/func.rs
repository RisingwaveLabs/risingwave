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

//! Function signatures.

use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::sync::LazyLock;

use risingwave_common::types::{DataType, DataTypeName};
use risingwave_pb::expr::expr_node::PbType;

use super::FuncSigDebug;
use crate::error::Result;
use crate::expr::BoxedExpression;

pub static FUNC_SIG_MAP: LazyLock<FuncSigMap> = LazyLock::new(|| unsafe {
    let mut map = FuncSigMap::default();
    tracing::info!("{} function signatures loaded.", FUNC_SIG_MAP_INIT.len());
    for desc in FUNC_SIG_MAP_INIT.drain(..) {
        map.insert(desc);
    }
    map
});

/// The table of function signatures.
pub fn func_sigs() -> impl Iterator<Item = &'static FuncSign> {
    FUNC_SIG_MAP.0.values().flatten()
}

#[derive(Default, Clone, Debug)]
pub struct FuncSigMap(HashMap<(PbType, usize), Vec<FuncSign>>);

impl FuncSigMap {
    /// Inserts a function signature.
    pub fn insert(&mut self, desc: FuncSign) {
        self.0
            .entry((desc.func, desc.inputs_type.len()))
            .or_default()
            .push(desc)
    }

    /// Returns a function signature with the same type, argument types and return type.
    pub fn get(&self, ty: PbType, args: &[DataTypeName], ret: DataTypeName) -> Option<&FuncSign> {
        let v = self.0.get(&(ty, args.len()))?;
        v.iter()
            .find(|d| d.inputs_type == args && d.ret_type == ret)
    }

    /// Returns all function signatures with the same type and number of arguments.
    pub fn get_with_arg_nums(&self, ty: PbType, nargs: usize) -> &[FuncSign] {
        self.0.get(&(ty, nargs)).map_or(&[], Deref::deref)
    }
}

/// A function signature.
#[derive(Clone)]
pub struct FuncSign {
    pub func: PbType,
    pub inputs_type: &'static [DataTypeName],
    pub ret_type: DataTypeName,
    pub build: fn(return_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression>,
}

impl fmt::Debug for FuncSign {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        FuncSigDebug {
            func: self.func.as_str_name(),
            inputs_type: self.inputs_type,
            ret_type: self.ret_type,
            set_returning: false,
        }
        .fmt(f)
    }
}

/// Register a function into global registry.
///
/// # Safety
///
/// This function must be called sequentially.
///
/// It is designed to be used by `#[function]` macro.
/// Users SHOULD NOT call this function.
#[doc(hidden)]
pub unsafe fn _register(desc: FuncSign) {
    FUNC_SIG_MAP_INIT.push(desc)
}

/// The global registry of function signatures on initialization.
///
/// `#[function]` macro will generate a `#[ctor]` function to register the signature into this
/// vector. The calls are guaranteed to be sequential. The vector will be drained and moved into
/// `FUNC_SIG_MAP` on the first access of `FUNC_SIG_MAP`.
static mut FUNC_SIG_MAP_INIT: Vec<FuncSign> = Vec::new();

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_func_sig_map() {
        // convert FUNC_SIG_MAP to a more convenient map for testing
        let mut new_map: BTreeMap<PbType, BTreeMap<Vec<DataTypeName>, Vec<FuncSign>>> =
            BTreeMap::new();
        for ((func, num_args), sigs) in FUNC_SIG_MAP.0.iter() {
            for sig in sigs {
                // validate the FUNC_SIG_MAP is consistent
                assert_eq!(func, &sig.func);
                assert_eq!(num_args, &sig.inputs_type.len());

                new_map
                    .entry(*func)
                    .or_default()
                    .entry(sig.inputs_type.to_vec())
                    .or_default()
                    .push(sig.clone());
            }
        }

        let duplicated: BTreeMap<_, Vec<_>> = new_map
            .into_iter()
            .filter_map(|(k, funcs_with_same_name)| {
                let funcs_with_same_name_type: Vec<_> = funcs_with_same_name
                    .into_values()
                    .filter(|v| v.len() > 1)
                    .collect();
                if !funcs_with_same_name_type.is_empty() {
                    Some((k, funcs_with_same_name_type.into_iter().flatten().collect()))
                } else {
                    None
                }
            })
            .collect();

        // This snapshot shows the function signatures without a unique match. Frontend has to
        // handle them specially without relying on FuncSigMap.
        let expected = expect_test::expect![[r#"
            {
                Cast: [
                    cast(boolean) -> int32,
                    cast(boolean) -> varchar,
                    cast(int16) -> int256,
                    cast(int16) -> decimal,
                    cast(int16) -> float64,
                    cast(int16) -> float32,
                    cast(int16) -> int64,
                    cast(int16) -> int32,
                    cast(int16) -> varchar,
                    cast(int32) -> int256,
                    cast(int32) -> int16,
                    cast(int32) -> decimal,
                    cast(int32) -> float64,
                    cast(int32) -> float32,
                    cast(int32) -> int64,
                    cast(int32) -> boolean,
                    cast(int32) -> varchar,
                    cast(int64) -> int256,
                    cast(int64) -> int32,
                    cast(int64) -> int16,
                    cast(int64) -> decimal,
                    cast(int64) -> float64,
                    cast(int64) -> float32,
                    cast(int64) -> varchar,
                    cast(float32) -> decimal,
                    cast(float32) -> int64,
                    cast(float32) -> int32,
                    cast(float32) -> int16,
                    cast(float32) -> float64,
                    cast(float32) -> varchar,
                    cast(float64) -> decimal,
                    cast(float64) -> float32,
                    cast(float64) -> int64,
                    cast(float64) -> int32,
                    cast(float64) -> int16,
                    cast(float64) -> varchar,
                    cast(decimal) -> float64,
                    cast(decimal) -> float32,
                    cast(decimal) -> int64,
                    cast(decimal) -> int32,
                    cast(decimal) -> int16,
                    cast(decimal) -> varchar,
                    cast(date) -> timestamp,
                    cast(date) -> varchar,
                    cast(varchar) -> date,
                    cast(varchar) -> time,
                    cast(varchar) -> timestamp,
                    cast(varchar) -> jsonb,
                    cast(varchar) -> interval,
                    cast(varchar) -> int256,
                    cast(varchar) -> float32,
                    cast(varchar) -> float64,
                    cast(varchar) -> decimal,
                    cast(varchar) -> int16,
                    cast(varchar) -> int32,
                    cast(varchar) -> int64,
                    cast(varchar) -> varchar,
                    cast(varchar) -> boolean,
                    cast(varchar) -> bytea,
                    cast(varchar) -> list,
                    cast(time) -> interval,
                    cast(time) -> varchar,
                    cast(timestamp) -> date,
                    cast(timestamp) -> time,
                    cast(timestamp) -> varchar,
                    cast(interval) -> time,
                    cast(interval) -> varchar,
                    cast(list) -> varchar,
                    cast(list) -> list,
                    cast(jsonb) -> boolean,
                    cast(jsonb) -> float64,
                    cast(jsonb) -> float32,
                    cast(jsonb) -> decimal,
                    cast(jsonb) -> int64,
                    cast(jsonb) -> int32,
                    cast(jsonb) -> int16,
                    cast(jsonb) -> varchar,
                    cast(int256) -> float64,
                    cast(int256) -> varchar,
                ],
                ArrayAccess: [
                    array_access(list, int32) -> boolean,
                    array_access(list, int32) -> int16,
                    array_access(list, int32) -> int32,
                    array_access(list, int32) -> int64,
                    array_access(list, int32) -> int256,
                    array_access(list, int32) -> float32,
                    array_access(list, int32) -> float64,
                    array_access(list, int32) -> decimal,
                    array_access(list, int32) -> serial,
                    array_access(list, int32) -> date,
                    array_access(list, int32) -> time,
                    array_access(list, int32) -> timestamp,
                    array_access(list, int32) -> timestamptz,
                    array_access(list, int32) -> interval,
                    array_access(list, int32) -> varchar,
                    array_access(list, int32) -> bytea,
                    array_access(list, int32) -> jsonb,
                    array_access(list, int32) -> list,
                    array_access(list, int32) -> struct,
                ],
                ArrayLength: [
                    array_length(list) -> int64,
                    array_length(list) -> int32,
                ],
                Cardinality: [
                    cardinality(list) -> int64,
                    cardinality(list) -> int32,
                ],
            }
        "#]];
        expected.assert_debug_eq(&duplicated);
    }
}
