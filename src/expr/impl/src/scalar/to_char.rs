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

use std::fmt::{Debug, Write};
use std::sync::LazyLock;

use aho_corasick::{AhoCorasick, AhoCorasickBuilder};
use chrono::format::{Item, StrftimeItems};
use chrono::{Datelike, NaiveDate};
use risingwave_common::types::{Interval, Timestamp, TimestampNanosecond, Timestamptz};
use risingwave_expr::{function, ExprError, Result};

use super::timestamptz::time_zone_err;
use crate::scalar::arithmetic_op::timestamp_interval_add;

type Pattern<'a> = Vec<chrono::format::Item<'a>>;

#[inline(always)]
fn invalid_pattern_err() -> ExprError {
    ExprError::InvalidParam {
        name: "pattern",
        reason: "invalid format specification for an interval value, HINT: Intervals are not tied to specific calendar dates.".into(),
    }
}

self_cell::self_cell! {
    pub struct ChronoPattern {
        owner: String,
        #[covariant]
        dependent: Pattern,
    }
}

impl Debug for ChronoPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChronoPattern")
            .field("tmpl", self.borrow_owner())
            .finish()
    }
}

impl ChronoPattern {
    /// Compile the pg pattern to chrono pattern.
    // TODO: Chrono can not fully support the pg format, so consider using other implementations
    // later.
    pub fn compile(tmpl: &str) -> ChronoPattern {
        // mapping from pg pattern to chrono pattern
        // pg pattern: https://www.postgresql.org/docs/current/functions-formatting.html
        // chrono pattern: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        const PATTERNS: &[(&str, &str)] = &[
            ("HH24", "%H"),
            ("hh24", "%H"),
            ("HH12", "%I"),
            ("hh12", "%I"),
            ("HH", "%I"),
            ("hh", "%I"),
            ("AM", "%p"),
            ("PM", "%p"),
            ("am", "%P"),
            ("pm", "%P"),
            ("MI", "%M"),
            ("mi", "%M"),
            ("SS", "%S"),
            ("ss", "%S"),
            ("YYYY", "%Y"),
            ("yyyy", "%Y"),
            ("YY", "%y"),
            ("yy", "%y"),
            ("IYYY", "%G"),
            ("iyyy", "%G"),
            ("IY", "%g"),
            ("iy", "%g"),
            ("MM", "%m"),
            ("mm", "%m"),
            ("Month", "%B"),
            ("Mon", "%b"),
            ("DD", "%d"),
            ("dd", "%d"),
            ("NS", "%9f"),
            ("ns", "%9f"),
            ("US", "%6f"),
            ("us", "%6f"),
            ("MS", "%3f"),
            ("ms", "%3f"),
            ("TZH:TZM", "%:z"),
            ("tzh:tzm", "%:z"),
            ("TZHTZM", "%z"),
            ("tzhtzm", "%z"),
            ("TZH", "%#z"),
            ("tzh", "%#z"),
        ];
        // build an Aho-Corasick automaton for fast matching
        static AC: LazyLock<AhoCorasick> = LazyLock::new(|| {
            AhoCorasickBuilder::new()
                .ascii_case_insensitive(false)
                .match_kind(aho_corasick::MatchKind::LeftmostLongest)
                .build(PATTERNS.iter().map(|(k, _)| k))
                .expect("failed to build an Aho-Corasick automaton")
        });

        ChronoPattern::compile_inner(tmpl, PATTERNS, &AC)
    }

    pub fn compile_for_interval(tmpl: &str) -> ChronoPattern {
        // mapping from pg pattern to chrono pattern
        // pg pattern: https://www.postgresql.org/docs/current/functions-formatting.html
        // chrono pattern: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
        const PATTERNS: &[(&str, &str)] = &[
            ("HH24", "%H"),
            ("hh24", "%H"),
            ("HH12", "%I"),
            ("hh12", "%I"),
            ("HH", "%I"),
            ("hh", "%I"),
            ("AM", "%p"),
            ("PM", "%p"),
            ("am", "%P"),
            ("pm", "%P"),
            ("MI", "%M"),
            ("mi", "%M"),
            ("SS", "%S"),
            ("ss", "%S"),
            ("YYYY", "%Y"),
            ("yyyy", "%Y"),
            ("YY", "%y"),
            ("yy", "%y"),
            ("IYYY", "%G"),
            ("iyyy", "%G"),
            ("IY", "%g"),
            ("iy", "%g"),
            ("MM", "%m"),
            ("mm", "%m"),
            ("Month", "%B"),
            ("Mon", "%b"),
            ("DD", "%d"),
            ("dd", "%d"),
            ("US", "%.6f"), /* "%6f" and "%3f" are converted to private data structures in chrono, so we use "%.6f" and "%.3f" instead. */
            ("us", "%.6f"),
            ("MS", "%.3f"),
            ("ms", "%.3f"),
            ("TZH:TZM", "%:z"),
            ("tzh:tzm", "%:z"),
            ("TZHTZM", "%z"),
            ("tzhtzm", "%z"),
            ("TZH", "%#z"),
            ("tzh", "%#z"),
        ];
        // build an Aho-Corasick automaton for fast matching
        static AC: LazyLock<AhoCorasick> = LazyLock::new(|| {
            AhoCorasickBuilder::new()
                .ascii_case_insensitive(false)
                .match_kind(aho_corasick::MatchKind::LeftmostLongest)
                .build(PATTERNS.iter().map(|(k, _)| k))
                .expect("failed to build an Aho-Corasick automaton")
        });
        ChronoPattern::compile_inner(tmpl, PATTERNS, &AC)
    }

    fn compile_inner(
        tmpl: &str,
        patterns: &[(&str, &str)],
        ac: &LazyLock<AhoCorasick>,
    ) -> ChronoPattern {
        // replace all pg patterns with chrono patterns
        let mut chrono_tmpl = String::new();
        ac.replace_all_with(tmpl, &mut chrono_tmpl, |mat, _, dst| {
            dst.push_str(patterns[mat.pattern()].1);
            true
        });
        tracing::debug!(tmpl, chrono_tmpl, "compile_pattern_to_chrono");
        ChronoPattern::new(chrono_tmpl, |tmpl| {
            StrftimeItems::new(tmpl).collect::<Vec<_>>()
        })
    }
}

#[function(
    "to_char(timestamp, varchar) -> varchar",
    prebuild = "ChronoPattern::compile($1)"
)]
fn timestamp_to_char(data: Timestamp, pattern: &ChronoPattern, writer: &mut impl Write) {
    let format = data.0.format_with_items(pattern.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
}

#[function(
    "to_char(timestamp_ns, varchar) -> varchar",
    prebuild = "ChronoPattern::compile($1)"
)]
fn timestamp_ns_to_char(
    data: TimestampNanosecond,
    pattern: &ChronoPattern,
    writer: &mut impl Write,
) {
    let format = data.0.format_with_items(pattern.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
}

#[function("to_char(timestamptz, varchar) -> varchar", rewritten)]
fn _timestamptz_to_char() {}

#[function(
    "to_char(timestamptz, varchar, varchar) -> varchar",
    prebuild = "ChronoPattern::compile($1)"
)]
fn timestamptz_to_char3(
    data: Timestamptz,
    zone: &str,
    tmpl: &ChronoPattern,
    writer: &mut impl Write,
) -> Result<()> {
    let format = data
        .to_datetime_in_zone(Timestamptz::lookup_time_zone(zone).map_err(time_zone_err)?)
        .format_with_items(tmpl.borrow_dependent().iter());
    write!(writer, "{}", format).unwrap();
    Ok(())
}

#[function(
    "to_char(interval, varchar) -> varchar",
    prebuild = "ChronoPattern::compile_for_interval($1)"
)]
fn interval_to_char(
    interval: Interval,
    pattern: &ChronoPattern,
    writer: &mut impl Write,
) -> Result<()> {
    for iter in pattern.borrow_dependent() {
        format_inner(writer, interval, iter)?;
    }
    Ok(())
}

fn adjust_to_iso_year(interval: Interval) -> Result<i32> {
    let start = risingwave_common::types::Timestamp(
        NaiveDate::from_ymd_opt(0, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    let interval = Interval::from_month_day_usec(interval.months(), interval.days(), 0);
    let date = timestamp_interval_add(start, interval)?;
    Ok(date.0.iso_week().year())
}

fn format_inner(w: &mut impl Write, interval: Interval, item: &Item<'_>) -> Result<()> {
    match *item {
        Item::Literal(s) | Item::Space(s) => {
            w.write_str(s).unwrap();
            Ok(())
        }
        Item::OwnedLiteral(ref s) | Item::OwnedSpace(ref s) => {
            w.write_str(s).unwrap();
            Ok(())
        }
        Item::Numeric(ref spec, _) => {
            use chrono::format::Numeric::*;
            match *spec {
                Year => {
                    let year = interval.years_field();
                    if year < 0 {
                        write!(w, "{:+05}", year).unwrap();
                    } else {
                        write!(w, "{:04}", year).unwrap();
                    }
                }
                YearMod100 => {
                    let year = interval.years_field();
                    if year % 100 < 0 {
                        let year = -((-year) % 100);
                        write!(w, "{:+03}", year).unwrap();
                    } else {
                        let year = year % 100;
                        write!(w, "{:02}", year).unwrap();
                    }
                }
                IsoYear => {
                    let iso_year = adjust_to_iso_year(interval)?;
                    if interval.years_field() < 0 {
                        write!(w, "{:+05}", iso_year).unwrap();
                    } else {
                        write!(w, "{:04}", iso_year).unwrap();
                    }
                }
                IsoYearMod100 => {
                    let iso_year = adjust_to_iso_year(interval)?;
                    if interval.years_field() % 100 < 0 {
                        let iso_year = -((-iso_year) % 100);
                        write!(w, "{:+03}", iso_year).unwrap();
                    } else {
                        let iso_year = iso_year % 100;
                        write!(w, "{:02}", iso_year).unwrap();
                    }
                }
                Month => {
                    let month = interval.months_field();
                    if month < 0 {
                        write!(w, "{:+03}", month).unwrap();
                    } else {
                        write!(w, "{:02}", month).unwrap();
                    }
                }
                Day => {
                    let day = interval.days_field();
                    if day < 0 {
                        write!(w, "{:+02}", day).unwrap();
                    } else {
                        write!(w, "{:02}", day).unwrap();
                    }
                }
                Hour => {
                    let hour = interval.hours_field();
                    if hour < 0 {
                        write!(w, "{:+03}", hour).unwrap();
                    } else {
                        write!(w, "{:02}", hour).unwrap();
                    }
                }
                Hour12 => {
                    let hour = interval.hours_field();
                    if hour < 0 {
                        // here to align with postgres, we format -0 as 012.
                        let hour = -(-hour) % 12;
                        if hour == 0 {
                            w.write_str("012").unwrap();
                        } else {
                            write!(w, "{:+03}", hour).unwrap();
                        }
                    } else {
                        let hour = if hour % 12 == 0 { 12 } else { hour % 12 };
                        write!(w, "{:02}", hour).unwrap();
                    }
                }
                Minute => {
                    let minute = interval.usecs() / 1_000_000 / 60;
                    if minute % 60 < 0 {
                        let minute = -((-minute) % 60);
                        write!(w, "{:+03}", minute).unwrap();
                    } else {
                        let minute = minute % 60;
                        write!(w, "{:02}", minute).unwrap();
                    }
                }
                Second => {
                    let second = interval.usecs() / 1_000_000;
                    if second % 60 < 0 {
                        let second = -((-second) % 60);
                        write!(w, "{:+03}", second).unwrap();
                    } else {
                        let second = second % 60;
                        write!(w, "{:02}", second).unwrap();
                    }
                }
                Nanosecond | Ordinal | WeekdayFromMon | NumDaysFromSun | IsoWeek | WeekFromSun
                | WeekFromMon | IsoYearDiv100 | Timestamp | YearDiv100 | Internal(_) => {
                    unreachable!()
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        Item::Fixed(ref spec) => {
            use chrono::format::Fixed::*;
            match *spec {
                LowerAmPm => {
                    if interval.hours_field() % 24 >= 12 {
                        w.write_str("pm").unwrap();
                    } else {
                        w.write_str("am").unwrap();
                    }
                    Ok(())
                }
                UpperAmPm => {
                    if interval.hours_field() % 24 >= 12 {
                        w.write_str("PM").unwrap();
                    } else {
                        w.write_str("AM").unwrap();
                    }
                    Ok(())
                }
                Nanosecond3 => {
                    let usec = interval.usecs() % 1_000_000;
                    write!(w, "{:03}", usec / 1000).unwrap();
                    Ok(())
                }
                Nanosecond6 => {
                    let usec = interval.usecs() % 1_000_000;
                    write!(w, "{:06}", usec).unwrap();
                    Ok(())
                }
                Internal(_) | ShortMonthName | LongMonthName | TimezoneOffset | TimezoneOffsetZ
                | TimezoneOffsetColon => Err(invalid_pattern_err()),
                ShortWeekdayName
                | LongWeekdayName
                | TimezoneName
                | TimezoneOffsetDoubleColon
                | TimezoneOffsetTripleColon
                | TimezoneOffsetColonZ
                | Nanosecond
                | Nanosecond9
                | RFC2822
                | RFC3339 => unreachable!(),
                _ => unreachable!(),
            }
        }
        Item::Error => Err(invalid_pattern_err()),
    }
}
