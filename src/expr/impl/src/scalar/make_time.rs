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

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use risingwave_common::types::{
    Date, FloatExt, Time, Timestamp, TimestampNanosecond, Timestamptz, F64,
};
use risingwave_expr::expr_context::TIME_ZONE;
use risingwave_expr::{capture_context, function, ExprError, Result};

use crate::scalar::timestamptz::timestamp_at_time_zone;

pub fn make_naive_date(mut year: i32, month: i32, day: i32) -> Result<NaiveDate> {
    if year == 0 {
        return Err(ExprError::InvalidParam {
            name: "year, month, day",
            reason: format!("invalid date: {}-{}-{}", year, month, day).into(),
        });
    }
    if year < 0 {
        year += 1
    }
    NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| ExprError::InvalidParam {
        name: "year, month, day",
        reason: format!("invalid date: {}-{}-{}", year, month, day).into(),
    })
}

fn make_naive_time_ns(hour: i32, min: i32, sec: F64) -> Result<NaiveTime> {
    if !sec.is_finite() || sec.0.is_sign_negative() {
        return Err(ExprError::InvalidParam {
            name: "sec",
            reason: format!("invalid sec: {}", sec).into(),
        });
    }
    let sec_u32 = sec.0.trunc() as u32;
    let nanosecond_u32 = ((sec.0 - sec.0.trunc()) * 1_000_000_000.0).round_ties_even() as u32;
    NaiveTime::from_hms_nano_opt(hour as u32, min as u32, sec_u32, nanosecond_u32).ok_or_else(
        || ExprError::InvalidParam {
            name: "hour, min, sec",
            reason: format!("invalid time: {}:{}:{}", hour, min, sec).into(),
        },
    )
}

fn make_naive_time_ms(hour: i32, min: i32, sec: F64) -> Result<NaiveTime> {
    if !sec.is_finite() || sec.0.is_sign_negative() {
        return Err(ExprError::InvalidParam {
            name: "sec",
            reason: format!("invalid sec: {}", sec).into(),
        });
    }
    let sec_u32 = sec.0.trunc() as u32;
    let microsecond_u32 = ((sec.0 - sec.0.trunc()) * 1_000_000.0).round_ties_even() as u32;
    NaiveTime::from_hms_micro_opt(hour as u32, min as u32, sec_u32, microsecond_u32).ok_or_else(
        || ExprError::InvalidParam {
            name: "hour, min, sec",
            reason: format!("invalid time: {}:{}:{}", hour, min, sec).into(),
        },
    )
}

// year int, month int, day int
#[function("make_date(int4, int4, int4) -> date")]
pub fn make_date(year: i32, month: i32, day: i32) -> Result<Date> {
    Ok(Date(make_naive_date(year, month, day)?))
}

// hour int, min int, sec double precision
#[function("make_time(int4, int4, float8) -> time")]
pub fn make_time(hour: i32, min: i32, sec: F64) -> Result<Time> {
    Ok(Time(make_naive_time_ns(hour, min, sec)?))
}

// year int, month int, day int, hour int, min int, sec double precision
#[function("make_timestamp(int4, int4, int4, int4, int4, float8) -> timestamp")]
pub fn make_timestamp(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamp> {
    Ok(Timestamp(NaiveDateTime::new(
        make_naive_date(year, month, day)?,
        make_naive_time_ms(hour, min, sec)?,
    )))
}

#[function("make_timestamp_ns(int4, int4, int4, int4, int4, float8) -> timestamp_ns")]
pub fn make_timestamp_ns(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<TimestampNanosecond> {
    Ok(TimestampNanosecond(NaiveDateTime::new(
        make_naive_date(year, month, day)?,
        make_naive_time_ns(hour, min, sec)?,
    )))
}

// year int, month int, day int, hour int, min int, sec double precision
#[function("make_timestamptz(int4, int4, int4, int4, int4, float8) -> timestamptz")]
pub fn make_timestamptz(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamptz> {
    make_timestamptz_impl_captured(year, month, day, hour, min, sec)
}

// year int, month int, day int, hour int, min int, sec double precision, timezone text
#[function("make_timestamptz(int4, int4, int4, int4, int4, float8, varchar) -> timestamptz")]
pub fn make_timestamptz_with_time_zone(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
    time_zone: &str,
) -> Result<Timestamptz> {
    make_timestamptz_impl(time_zone, year, month, day, hour, min, sec)
}

#[capture_context(TIME_ZONE)]
fn make_timestamptz_impl(
    time_zone: &str,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamptz> {
    let naive_date_time = NaiveDateTime::new(
        make_naive_date(year, month, day)?,
        make_naive_time_ms(hour, min, sec)?,
    );
    timestamp_at_time_zone(Timestamp(naive_date_time), time_zone)
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use risingwave_common::types::{Date, Timestamp};

    #[test]
    fn test_naive_date_and_time() {
        let year = -1973;
        let month = 2;
        let day = 2;
        let hour = 12;
        let min = 34;
        let sec: f64 = 56.789;
        let naive_date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap();
        let naive_time = NaiveTime::from_hms_micro_opt(
            hour as u32,
            min as u32,
            sec.trunc() as u32,
            ((sec - sec.trunc()) * 1_000_000.0).round() as u32,
        )
        .unwrap();
        assert_eq!(naive_date.to_string(), String::from("-1973-02-02"));
        let date = Date(naive_date);
        assert_eq!(date.to_string(), String::from("1974-02-02 BC"));
        assert_eq!(naive_time.to_string(), String::from("12:34:56.789"));
        let date_time = Timestamp(NaiveDateTime::new(naive_date, naive_time));
        assert_eq!(
            date_time.to_string(),
            String::from("1974-02-02 12:34:56.789 BC")
        );
    }
}
