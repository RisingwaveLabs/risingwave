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

use std::str::FromStr;

use chrono::{Datelike, NaiveTime, Timelike};
use risingwave_common::types::{
    Date, Decimal, Interval, Time, Timestamp, TimestampNanosecond, Timestamptz, F64,
};
use risingwave_expr::{function, ExprError, Result};

use self::Unit::*;
use crate::scalar::timestamptz::time_zone_err;

/// Extract field from `Datelike`.
fn extract_from_datelike(date: impl Datelike, unit: Unit) -> Decimal {
    match unit {
        Millennium => ((date.year() - 1) / 1000 + 1).into(),
        Century => ((date.year() - 1) / 100 + 1).into(),
        Decade => (date.year() / 10).into(),
        Year => date.year().into(),
        IsoYear => date.iso_week().year().into(),
        Quarter => ((date.month() - 1) / 3 + 1).into(),
        Month => date.month().into(),
        Week => date.iso_week().week().into(),
        Day => date.day().into(),
        Doy => date.ordinal().into(),
        Dow => date.weekday().num_days_from_sunday().into(),
        IsoDow => date.weekday().number_from_monday().into(),
        u => unreachable!("invalid unit {:?} for date", u),
    }
}

/// Extract field from `Timelike`.
fn extract_from_timelike(time: impl Timelike, unit: Unit) -> Decimal {
    let nanos = || time.second() as u64 * 1_000_000_000 + time.nanosecond() as u64;
    match unit {
        Hour => time.hour().into(),
        Minute => time.minute().into(),
        Second => Decimal::from_i128_with_scale(nanos() as i128, 9),
        Millisecond => Decimal::from_i128_with_scale(nanos() as i128, 6),
        Microsecond => Decimal::from_i128_with_scale(nanos() as i128, 3),
        Nanosecond => nanos().into(),
        Epoch => {
            let nanos =
                time.num_seconds_from_midnight() as u64 * 1_000_000_000 + time.nanosecond() as u64;
            Decimal::from_i128_with_scale(nanos as i128, 9)
        }
        u => unreachable!("invalid unit {:?} for time", u),
    }
}

#[function(
    "extract(varchar, date) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_date()?"
)]
fn extract_from_date(date: Date, unit: &Unit) -> Decimal {
    match unit {
        Epoch => {
            let epoch = date.0.and_time(NaiveTime::default()).and_utc().timestamp();
            epoch.into()
        }
        Julian => {
            const UNIX_EPOCH_DAY: i32 = 719_163;
            let julian = date.0.num_days_from_ce() - UNIX_EPOCH_DAY + 2_440_588;
            julian.into()
        }
        _ => extract_from_datelike(date.0, *unit),
    }
}

#[function(
    "extract(varchar, time) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_time()?"
)]
fn extract_from_time(time: Time, unit: &Unit) -> Decimal {
    extract_from_timelike(time.0, *unit)
}

#[function(
    "extract(varchar, timestamp) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_timestamp()?"
)]
fn extract_from_timestamp(timestamp: Timestamp, unit: &Unit) -> Decimal {
    match unit {
        Epoch => {
            let micros = timestamp.0.and_utc().timestamp_micros();
            Decimal::from_i128_with_scale(micros as i128, 6)
        }
        Julian => {
            let epoch =
                Decimal::from_i128_with_scale(timestamp.0.and_utc().timestamp_micros() as i128, 6);
            epoch / (24 * 60 * 60).into() + 2_440_588.into()
        }
        _ if unit.is_date_unit() => extract_from_datelike(timestamp.0.date(), *unit),
        _ if unit.is_time_unit() => extract_from_timelike(timestamp.0.time(), *unit),
        u => unreachable!("invalid unit {:?} for timestamp", u),
    }
}

#[function(
    "extract(varchar, timestamp_ns) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_timestamp()?"
)]
fn extract_from_timestamp_ns(timestamp_nano: TimestampNanosecond, unit: &Unit) -> Decimal {
    match unit {
        Epoch => {
            if let Some(nanos) = timestamp_nano.0.and_utc().timestamp_nanos_opt() {
                Decimal::from_i128_with_scale(nanos as i128, 9)
            } else {
                let micros = timestamp_nano.0.and_utc().timestamp_micros();
                Decimal::from_i128_with_scale(micros as i128, 6)
            }
        }
        Julian => {
            let epoch = if let Some(nanos) = timestamp_nano.0.and_utc().timestamp_nanos_opt() {
                Decimal::from_i128_with_scale(nanos as i128, 9)
            } else {
                let epoch = timestamp_nano.0.and_utc().timestamp_micros();
                Decimal::from_i128_with_scale(epoch as i128, 6)
            };
            epoch / (24 * 60 * 60).into() + 2_440_588.into()
        }
        _ if unit.is_date_unit() => extract_from_datelike(timestamp_nano.0.date(), *unit),
        _ if unit.is_time_unit() => extract_from_timelike(timestamp_nano.0.time(), *unit),
        u => unreachable!("invalid unit {:?} for timestamp", u),
    }
}

#[function(
    "extract(varchar, timestamptz) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_timestamptz()?"
)]
fn extract_from_timestamptz(tz: Timestamptz, unit: &Unit) -> Decimal {
    match unit {
        Epoch => Decimal::from_i128_with_scale(tz.timestamp_micros() as _, 6),
        // TODO(#5826): all other units depend on implicit session TimeZone
        u => unreachable!("invalid unit {u:?} for timestamp with time zone"),
    }
}

#[function(
    "extract(varchar, timestamptz, varchar) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_timestamptz_at_timezone()?"
)]
fn extract_from_timestamptz_at_timezone(
    input: Timestamptz,
    timezone: &str,
    unit: &Unit,
) -> Result<Decimal> {
    use chrono::Offset as _;

    let time_zone = Timestamptz::lookup_time_zone(timezone).map_err(time_zone_err)?;
    let instant_local = input.to_datetime_in_zone(time_zone);

    Ok(match unit {
        Epoch => Decimal::from_i128_with_scale(instant_local.timestamp_micros() as _, 6),
        Timezone => {
            let east_secs = instant_local.offset().fix().local_minus_utc();
            east_secs.into()
        }
        Timezone_Hour => {
            let east_secs = instant_local.offset().fix().local_minus_utc();
            (east_secs / 3600).into()
        }
        Timezone_Minute => {
            let east_secs = instant_local.offset().fix().local_minus_utc();
            (east_secs % 3600 / 60).into()
        }
        _ => extract_from_timestamp(instant_local.naive_local().into(), unit),
    })
}

#[function(
    "extract(varchar, interval) -> decimal",
    prebuild = "Unit::from_str($0)?.ensure_interval()?"
)]
fn extract_from_interval(interval: Interval, unit: &Unit) -> Decimal {
    match unit {
        Millennium => (interval.years_field() / 1000).into(),
        Century => (interval.years_field() / 100).into(),
        Decade => (interval.years_field() / 10).into(),
        Year => interval.years_field().into(),
        Quarter => (interval.months_field() / 3 + 1).into(),
        Month => interval.months_field().into(),
        Day => interval.days_field().into(),
        Hour => interval.hours_field().into(),
        Minute => interval.minutes_field().into(),
        Second => Decimal::from_i128_with_scale(interval.seconds_in_micros() as i128, 6),
        Millisecond => Decimal::from_i128_with_scale(interval.seconds_in_micros() as i128, 3),
        Microsecond => interval.seconds_in_micros().into(),
        Epoch => Decimal::from_i128_with_scale(interval.epoch_in_micros(), 6),
        u => unreachable!("invalid unit {:?} for interval", u),
    }
}

#[function(
    "date_part(varchar, date) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_date()?"
)]
fn date_part_from_date(date: Date, unit: &Unit) -> Result<F64> {
    // date_part of date manually cast to timestamp
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/catalog/system_functions.sql#L123
    extract_from_timestamp(date.into(), unit)
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function(
    "date_part(varchar, time) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_time()?"
)]
fn date_part_from_time(time: Time, unit: &Unit) -> Result<F64> {
    extract_from_time(time, unit)
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function(
    "date_part(varchar, timestamp) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_timestamp()?"
)]
fn date_part_from_timestamp(timestamp: Timestamp, unit: &Unit) -> Result<F64> {
    extract_from_timestamp(timestamp, unit)
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function(
    "date_part(varchar, timestamptz) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_timestamptz()?"
)]
fn date_part_from_timestamptz(input: Timestamptz, unit: &Unit) -> Result<F64> {
    extract_from_timestamptz(input, unit)
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function(
    "date_part(varchar, timestamptz, varchar) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_timestamptz_at_timezone()?"
)]
fn date_part_from_timestamptz_at_timezone(
    input: Timestamptz,
    timezone: &str,
    unit: &Unit,
) -> Result<F64> {
    extract_from_timestamptz_at_timezone(input, timezone, unit)?
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

#[function(
    "date_part(varchar, interval) -> float8",
    prebuild = "Unit::from_str($0)?.ensure_interval()?"
)]
fn date_part_from_interval(interval: Interval, unit: &Unit) -> Result<F64> {
    extract_from_interval(interval, unit)
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

/// Define an enum and its `FromStr` impl.
macro_rules! define_unit {
    ($(#[ $attr:meta ])* enum $name:ident { $($variant:ident,)* }) => {
        $(#[$attr])*
        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        enum $name {
            $($variant,)*
        }

        impl FromStr for $name {
            type Err = ExprError;

            fn from_str(s: &str) -> Result<Self> {
                $(
                    if s.eq_ignore_ascii_case(stringify!($variant)) {
                        return Ok(Self::$variant);
                    }
                )*
                Err(invalid_unit(s))
            }
        }
    };
}

define_unit! {
    /// Datetime units.
    #[allow(non_camel_case_types)]
    enum Unit {
        Millennium,
        Century,
        Decade,
        Year,
        IsoYear,
        Quarter,
        Month,
        Week,
        Day,
        Doy,
        Dow,
        IsoDow,
        Hour,
        Minute,
        Second,
        Millisecond,
        Microsecond,
        Nanosecond,
        Epoch,
        Julian,
        Timezone,
        Timezone_Hour,
        Timezone_Minute,
    }
}

impl Unit {
    /// Whether the unit is a valid date unit.
    #[rustfmt::skip]
    const fn is_date_unit(self) -> bool {
        matches!(
            self,
            Millennium | Century | Decade | Year | IsoYear | Quarter | Month | Week
            | Day | Doy | Dow | IsoDow | Epoch | Julian
        )
    }

    /// Whether the unit is a valid time unit.
    const fn is_time_unit(self) -> bool {
        matches!(
            self,
            Hour | Minute | Second | Millisecond | Microsecond | Nanosecond | Epoch
        )
    }

    /// Whether the unit is a valid timestamp unit.
    const fn is_timestamp_unit(self) -> bool {
        self.is_date_unit() || self.is_time_unit()
    }

    /// Whether the unit is a valid timestamptz unit.
    const fn is_timestamptz_unit(self) -> bool {
        matches!(self, Epoch)
    }

    /// Whether the unit is a valid timestamptz at timezone unit.
    const fn is_timestamptz_at_timezone_unit(self) -> bool {
        self.is_timestamp_unit() || matches!(self, Timezone | Timezone_Hour | Timezone_Minute)
    }

    /// Whether the unit is a valid interval unit.
    #[rustfmt::skip]
    const fn is_interval_unit(self) -> bool {
        matches!(
            self,
            Millennium | Century | Decade | Year | Quarter | Month | Day | Hour | Minute
            | Second | Millisecond | Microsecond | Epoch
        )
    }

    /// Ensure the unit is a valid date unit.
    fn ensure_date(self) -> Result<Self> {
        if self.is_date_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "date"))
        }
    }

    /// Ensure the unit is a valid time unit.
    fn ensure_time(self) -> Result<Self> {
        if self.is_time_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "time"))
        }
    }

    /// Ensure the unit is a valid timestamp unit.
    fn ensure_timestamp(self) -> Result<Self> {
        if self.is_timestamp_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "timestamp"))
        }
    }

    /// Ensure the unit is a valid timestamptz unit.
    fn ensure_timestamptz(self) -> Result<Self> {
        if self.is_timestamptz_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "timestamp with time zone"))
        }
    }

    /// Ensure the unit is a valid timestamptz unit.
    fn ensure_timestamptz_at_timezone(self) -> Result<Self> {
        if self.is_timestamptz_at_timezone_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "timestamp with time zone"))
        }
    }

    /// Ensure the unit is a valid interval unit.
    fn ensure_interval(self) -> Result<Self> {
        if self.is_interval_unit() {
            Ok(self)
        } else {
            Err(unsupported_unit(self, "interval"))
        }
    }
}

fn invalid_unit(unit: &str) -> ExprError {
    ExprError::InvalidParam {
        name: "unit",
        reason: format!("unit \"{unit}\" not recognized").into(),
    }
}

fn unsupported_unit(unit: Unit, type_: &str) -> ExprError {
    ExprError::InvalidParam {
        name: "unit",
        reason: format!("unit \"{unit:?}\" not supported for type {type_}").into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime};

    use super::*;

    #[test]
    fn test_extract_from_date() {
        let date = Date::new(NaiveDate::parse_from_str("2021-11-22", "%Y-%m-%d").unwrap());
        let extract = |i| extract_from_date(date, &i).to_string();
        assert_eq!(extract(Day), "22");
        assert_eq!(extract(Month), "11");
        assert_eq!(extract(Year), "2021");
        assert_eq!(extract(Dow), "1");
        assert_eq!(extract(Doy), "326");
        assert_eq!(extract(Millennium), "3");
        assert_eq!(extract(Century), "21");
        assert_eq!(extract(Decade), "202");
        assert_eq!(extract(IsoYear), "2021");
        assert_eq!(extract(Quarter), "4");
        assert_eq!(extract(Week), "47");
        assert_eq!(extract(IsoDow), "1");
        assert_eq!(extract(Epoch), "1637539200");
        assert_eq!(extract(Julian), "2459541");
    }

    #[test]
    fn test_extract_from_time() {
        let time: Time = "23:22:57.123450".parse().unwrap();
        let extract = |unit| extract_from_time(time, &unit).to_string();
        assert_eq!(extract(Hour), "23");
        assert_eq!(extract(Minute), "22");
        assert_eq!(extract(Second), "57.123450000");
        assert_eq!(extract(Millisecond), "57123.450000");
        assert_eq!(extract(Microsecond), "57123450.000");
        assert_eq!(extract(Epoch), "84177.123450000");
    }

    #[test]
    fn test_extract_from_timestamp() {
        let ts = Timestamp::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2.575400", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
        );
        let extract = |unit| extract_from_timestamp(ts, &unit).to_string();
        assert_eq!(extract(Millennium), "3");
        assert_eq!(extract(Century), "21");
        assert_eq!(extract(Decade), "202");
        assert_eq!(extract(IsoYear), "2021");
        assert_eq!(extract(Year), "2021");
        assert_eq!(extract(Quarter), "4");
        assert_eq!(extract(Month), "11");
        assert_eq!(extract(Week), "47");
        assert_eq!(extract(Day), "22");
        assert_eq!(extract(Dow), "1");
        assert_eq!(extract(IsoDow), "1");
        assert_eq!(extract(Doy), "326");
        assert_eq!(extract(Hour), "12");
        assert_eq!(extract(Minute), "4");
        assert_eq!(extract(Second), "2.575400000");
        assert_eq!(extract(Millisecond), "2575.400000");
        assert_eq!(extract(Microsecond), "2575400.000");
        assert_eq!(extract(Epoch), "1637582642.575400");
        assert_eq!(extract(Julian), "2459541.5028075856481481481481");
    }

    #[test]
    fn test_extract_from_timestamptz() {
        let ts: Timestamptz = "2023-06-01 00:00:00Z".parse().unwrap();
        let extract = |unit| {
            extract_from_timestamptz_at_timezone(ts, "pst8pdt", &unit)
                .unwrap()
                .to_string()
        };
        assert_eq!(extract(Timezone), "-25200");
        assert_eq!(extract(Timezone_Hour), "-7");
        assert_eq!(extract(Timezone_Minute), "0");
    }

    #[test]
    fn test_extract_from_interval() {
        let interval: Interval = "2345 years 1 mon 250 days 23:22:57.123450".parse().unwrap();
        let extract = |unit| extract_from_interval(interval, &unit).to_string();
        assert_eq!(extract(Millennium), "2");
        assert_eq!(extract(Century), "23");
        assert_eq!(extract(Decade), "234");
        assert_eq!(extract(Year), "2345");
        assert_eq!(extract(Month), "1");
        assert_eq!(extract(Day), "250");
        assert_eq!(extract(Hour), "23");
        assert_eq!(extract(Minute), "22");
        assert_eq!(extract(Second), "57.123450");
        assert_eq!(extract(Millisecond), "57123.450");
        assert_eq!(extract(Microsecond), "57123450");
        assert_eq!(extract(Epoch), "74026848177.123450");

        let interval: Interval = "-2345 years -1 mon -250 days -23:22:57.123450"
            .parse()
            .unwrap();
        let extract = |unit| extract_from_interval(interval, &unit).to_string();
        assert_eq!(extract(Millennium), "-2");
        assert_eq!(extract(Century), "-23");
        assert_eq!(extract(Decade), "-234");
        assert_eq!(extract(Year), "-2345");
        assert_eq!(extract(Month), "-1");
        assert_eq!(extract(Day), "-250");
        assert_eq!(extract(Hour), "-23");
        assert_eq!(extract(Minute), "-22");
        assert_eq!(extract(Second), "-57.123450");
        assert_eq!(extract(Millisecond), "-57123.450");
        assert_eq!(extract(Microsecond), "-57123450");
        assert_eq!(extract(Epoch), "-74026848177.123450");
    }
}
