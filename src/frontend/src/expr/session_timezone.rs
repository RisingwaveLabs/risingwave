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

use risingwave_common::types::DataType;
pub use risingwave_pb::expr::expr_node::Type as ExprType;

pub use crate::expr::expr_rewriter::ExprRewriter;
pub use crate::expr::function_call::FunctionCall;
use crate::expr::{Expr, ExprImpl, ExprVisitor};
use crate::session::current;

/// `SessionTimezone` will be used to resolve session
/// timezone-dependent casts, comparisons or arithmetic.
pub struct SessionTimezone {
    timezone: String,
    /// Whether or not the session timezone was used
    used: bool,
}

impl ExprRewriter for SessionTimezone {
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let (func_type, inputs, ret) = func_call.decompose();
        let inputs: Vec<ExprImpl> = inputs
            .into_iter()
            .map(|expr| self.rewrite_expr(expr))
            .collect();
        if let Some(expr) = self.with_timezone(func_type, &inputs, ret.clone()) {
            self.mark_used();
            expr
        } else {
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }
}

impl SessionTimezone {
    pub fn new(timezone: String) -> Self {
        Self {
            timezone,
            used: false,
        }
    }

    pub fn timezone(&self) -> String {
        self.timezone.clone()
    }

    pub fn used(&self) -> bool {
        self.used
    }

    fn mark_used(&mut self) {
        if !self.used {
            self.used = true;
            current::notice_to_user(format!(
                "Your session timezone is {}. It was used in the interpretation of timestamps and dates in your query. If this is unintended, \
                change your timezone to match that of your data's with `set timezone = [timezone]` or \
                rewrite your query with an explicit timezone conversion, e.g. with `AT TIME ZONE`.\n",
                self.timezone
            ));
        }
    }

    // Inlines conversions based on session timezone if required by the function
    fn with_timezone(
        &self,
        func_type: ExprType,
        inputs: &[ExprImpl],
        return_type: DataType,
    ) -> Option<ExprImpl> {
        match func_type {
            // `input_timestamptz::varchar`
            // => `cast_with_time_zone(input_timestamptz, zone_string)`
            // `input_varchar::timestamptz`
            // => `cast_with_time_zone(input_varchar, zone_string)`
            // `input_date::timestamptz`
            // => `input_date::timestamp AT TIME ZONE zone_string`
            // `input_timestamp::timestamptz`
            // => `input_timestamp AT TIME ZONE zone_string`
            // `input_timestamptz::date`
            // => `(input_timestamptz AT TIME ZONE zone_string)::date`
            // `input_timestamptz::time`
            // => `(input_timestamptz AT TIME ZONE zone_string)::time`
            // `input_timestamptz::timestamp`
            // => `input_timestamptz AT TIME ZONE zone_string`
            // `input_timestamptz::timestamp_ns`
            // => `input_timestamptz AT TIME ZONE zone_string`
            ExprType::Cast => {
                assert_eq!(inputs.len(), 1);
                let mut input = inputs[0].clone();
                let input_type = input.return_type();
                match (input_type, return_type.clone()) {
                    (DataType::Timestamptz, DataType::Varchar)
                    | (DataType::Varchar, DataType::Timestamptz) => {
                        Some(self.cast_with_timezone(input, return_type))
                    }
                    (DataType::Date, DataType::Timestamptz)
                    | (DataType::Timestamp, DataType::Timestamptz)
                    | (DataType::TimestampNanosecond, DataType::Timestamptz) => {
                        input = input.cast_explicit(DataType::Timestamp).unwrap();
                        Some(self.at_timezone(input))
                    }
                    (DataType::Timestamptz, DataType::Date)
                    | (DataType::Timestamptz, DataType::Time)
                    | (DataType::Timestamptz, DataType::Timestamp)
                    | (DataType::Timestamptz, DataType::TimestampNanosecond) => {
                        input = self.at_timezone(input);
                        input = input.cast_explicit(return_type).unwrap();
                        Some(input)
                    }
                    _ => None,
                }
            }
            // `lhs_date CMP rhs_timestamptz`
            // => `(lhs_date::timestamp AT TIME ZONE zone_string) CMP rhs_timestamptz`
            // `lhs_timestamp CMP rhs_timestamptz`
            // => `(lhs_timestamp AT TIME ZONE zone_string) CMP rhs_timestamptz`
            // `lhs_timestamptz CMP rhs_date`
            // => `lhs_timestamptz CMP (rhs_date::timestamp AT TIME ZONE zone_string)`
            // `lhs_timestamptz CMP rhs_timestamp`
            // => `lhs_timestamptz CMP (rhs_timestamp AT TIME ZONE zone_string)`
            ExprType::Equal
            | ExprType::NotEqual
            | ExprType::LessThan
            | ExprType::LessThanOrEqual
            | ExprType::GreaterThan
            | ExprType::GreaterThanOrEqual
            | ExprType::IsDistinctFrom
            | ExprType::IsNotDistinctFrom => {
                assert_eq!(inputs.len(), 2);
                let mut inputs = inputs.to_vec();
                for idx in 0..2 {
                    if matches!(inputs[(idx + 1) % 2].return_type(), DataType::Timestamptz)
                        && matches!(
                            inputs[idx % 2].return_type(),
                            DataType::Date | DataType::Timestamp | DataType::TimestampNanosecond
                        )
                    {
                        let mut to_cast = inputs[idx % 2].clone();
                        // Cast to `Timestamp` first, then use `AT TIME ZONE` to convert to
                        // `Timestamptz`
                        to_cast = to_cast.cast_explicit(DataType::Timestamp).unwrap();
                        inputs[idx % 2] = self.at_timezone(to_cast);
                        return Some(
                            FunctionCall::new_unchecked(func_type, inputs, return_type).into(),
                        );
                    }
                }
                None
            }
            // `add(lhs_interval, rhs_timestamptz)`
            // => `add_with_time_zone(rhs_timestamptz, lhs_interval, zone_string)`
            // `add(lhs_timestamptz, rhs_interval)`
            // => `add_with_time_zone(lhs_timestamptz, rhs_interval, zone_string)`
            // `subtract(lhs_timestamptz, rhs_interval)`
            // => `subtract_with_time_zone(lhs_timestamptz, rhs_interval, zone_string)`
            ExprType::Subtract | ExprType::Add => {
                assert_eq!(inputs.len(), 2);
                let canonical_match = matches!(inputs[0].return_type(), DataType::Timestamptz)
                    && matches!(inputs[1].return_type(), DataType::Interval);
                let inverse_match = matches!(inputs[1].return_type(), DataType::Timestamptz)
                    && matches!(inputs[0].return_type(), DataType::Interval);
                assert!(!(inverse_match && func_type == ExprType::Subtract)); // This should never have been parsed.
                if canonical_match || inverse_match {
                    let (orig_timestamptz, interval) =
                        if func_type == ExprType::Add && inverse_match {
                            (inputs[1].clone(), inputs[0].clone())
                        } else {
                            (inputs[0].clone(), inputs[1].clone())
                        };
                    let new_type = match func_type {
                        ExprType::Add => ExprType::AddWithTimeZone,
                        ExprType::Subtract => ExprType::SubtractWithTimeZone,
                        _ => unreachable!(),
                    };
                    let rewritten_expr = FunctionCall::new(
                        new_type,
                        vec![
                            orig_timestamptz,
                            interval,
                            ExprImpl::literal_varchar(self.timezone()),
                        ],
                    )
                    .unwrap()
                    .into();
                    return Some(rewritten_expr);
                }
                None
            }
            // `date_trunc(field_string, input_timestamptz)`
            // => `date_trunc(field_string, input_timestamptz, zone_string)`
            ExprType::DateTrunc | ExprType::Extract | ExprType::DatePart => {
                if !(inputs.len() == 2 && inputs[1].return_type() == DataType::Timestamptz) {
                    return None;
                }
                assert_eq!(inputs[0].return_type(), DataType::Varchar);
                if let ExprImpl::Literal(lit) = &inputs[0]
                    && matches!(func_type, ExprType::Extract | ExprType::DatePart)
                    && lit
                        .get_data()
                        .as_ref()
                        .map_or(true, |v| v.as_utf8().eq_ignore_ascii_case("epoch"))
                {
                    // No need to rewrite when field is `null` or `epoch`.
                    // This is optional but avoids false warning in common case.
                    return None;
                }
                let mut new_inputs = inputs.to_vec();
                new_inputs.push(ExprImpl::literal_varchar(self.timezone()));
                Some(FunctionCall::new(func_type, new_inputs).unwrap().into())
            }
            // `char_to_timestamptz(input_string, format_string)`
            // => `char_to_timestamptz(input_string, format_string, zone_string)`
            ExprType::CharToTimestamptz => {
                if !(inputs.len() == 2
                    && inputs[0].return_type() == DataType::Varchar
                    && inputs[1].return_type() == DataType::Varchar)
                {
                    return None;
                }
                let mut new_inputs = inputs.to_vec();
                new_inputs.push(ExprImpl::literal_varchar(self.timezone()));
                Some(FunctionCall::new(func_type, new_inputs).unwrap().into())
            }
            // `to_char(input_timestamptz, format_string)`
            // => `to_char(input_timestamptz, format_string, zone_string)`
            ExprType::ToChar => {
                if !(inputs.len() == 2
                    && inputs[0].return_type() == DataType::Timestamptz
                    && inputs[1].return_type() == DataType::Varchar)
                {
                    return None;
                }
                let mut new_inputs = inputs.to_vec();
                new_inputs.push(ExprImpl::literal_varchar(self.timezone()));
                Some(FunctionCall::new(func_type, new_inputs).unwrap().into())
            }
            _ => None,
        }
    }

    fn at_timezone(&self, input: ExprImpl) -> ExprImpl {
        FunctionCall::new(
            ExprType::AtTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
        )
        .unwrap()
        .into()
    }

    fn cast_with_timezone(&self, input: ExprImpl, return_type: DataType) -> ExprImpl {
        FunctionCall::new_unchecked(
            ExprType::CastWithTimeZone,
            vec![input, ExprImpl::literal_varchar(self.timezone.clone())],
            return_type,
        )
        .into()
    }
}

#[derive(Default)]
pub struct TimestamptzExprFinder {
    has: bool,
}

impl TimestamptzExprFinder {
    pub fn has(&self) -> bool {
        self.has
    }
}

impl ExprVisitor for TimestamptzExprFinder {
    fn visit_function_call(&mut self, func_call: &FunctionCall) {
        if func_call.return_type() == DataType::Timestamptz {
            self.has = true;
            return;
        }

        for input in &func_call.inputs {
            if input.return_type() == DataType::Timestamptz {
                self.has = true;
                return;
            }
        }

        func_call
            .inputs()
            .iter()
            .for_each(|expr| self.visit_expr(expr));
    }
}
