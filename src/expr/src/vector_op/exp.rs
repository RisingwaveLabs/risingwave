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

use num_traits::{Float, Zero};
use risingwave_common::types::OrderedF64;

use crate::{ExprError, Result};

pub fn exp_f64(input: OrderedF64) -> Result<OrderedF64> {
    // The cases where the exponent value is Inf or NaN can be handled explicitly and without
    // executing the `exp` operator.
    if input.is_nan() {
        Ok(input)
    } else if input.is_infinite() {
        if input.is_sign_negative() {
            Ok(0.into())
        } else {
            Ok(input)
        }
    } else {
        let res = input.exp();

        // If the argument passed to `exp` is not `inf` or `-inf` then a result that is `inf` or `0`
        // means that the operation had an overflow or an underflow, and the appropriate
        // error should be returned.
        if res.is_infinite() {
            Err(ExprError::FloatOverflow)
        } else if res.is_zero() {
            Err(ExprError::FloatUnderflow)
        } else {
            Ok(res)
        }
    }
}
