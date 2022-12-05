// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Write;
use std::num::FpCategory;

use num_traits::ToPrimitive;

use super::{DatumRef, ScalarRefImpl};

// Used to convert ScalarRef to text format
pub trait ToText {
    fn to_text(&self) -> String;
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty } ),*) => {
        $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    self.to_string()
                }
            }
        )*
    };
}

macro_rules! implement_using_itoa {
    ($({ $scalar_type:ty } ),*) => {
        $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    itoa::Buffer::new().format(*self).to_owned()
                }
            }
        )*
    };
}

implement_using_to_string! {
    { String },
    { &str }
}

implement_using_itoa! {
    { i16 },
    { i32 },
    { i64 }
}

macro_rules! implement_using_ryu {
    ($({ $scalar_type:ty, $to_std_type:ident } ),*) => {
            $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    match self.classify() {
                        FpCategory::Infinite if self.is_sign_negative() => "-Infinity".to_owned(),
                        FpCategory::Infinite => "Infinity".to_owned(),
                        FpCategory::Zero if self.is_sign_negative() => "-0".to_owned(),
                        FpCategory::Nan => "NaN".to_owned(),
                        _ => match self.$to_std_type() {
                            Some(v) => {
                                let mut buf = ryu::Buffer::new();
                                let mut s = buf.format_finite(v);
                                if let Some(trimmed) = s.strip_suffix(".0") {
                                    s = trimmed;
                                }
                                let mut s_chars = s.chars().peekable();
                                let mut s_owned = s.to_owned();
                                let mut index = 0;
                                while let Some(c) = s_chars.next() {
                                    index += 1;
                                    if c == 'e' {
                                        if s_chars.peek() != Some(&'-') {
                                            s_owned.insert(index, '+');
                                        } else {
                                            index += 1;
                                        }

                                        if index + 1 == s.len() {
                                            s_owned.insert(index,'0');
                                        }
                                        break;
                                    }
                                }
                                s_owned
                            }
                            None => "NaN".to_owned(),
                        },
                    }
                }
            }
        )*
    };
}

implement_using_ryu! {
    { crate::types::OrderedF32, to_f32 },
    { crate::types::OrderedF64, to_f64 }
}

impl ToText for bool {
    fn to_text(&self) -> String {
        if *self {
            "t".to_string()
        } else {
            "f".to_string()
        }
    }
}

/// Convert bytes in `Bytea` type to String.
pub fn format_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 * bytes.len());
    write!(s, "\\x{}", hex::encode(bytes)).unwrap();
    s
}

impl ToText for ScalarRefImpl<'_> {
    fn to_text(&self) -> String {
        match self {
            ScalarRefImpl::Bool(b) => b.to_text(),
            ScalarRefImpl::Int16(i) => i.to_text(),
            ScalarRefImpl::Int32(i) => i.to_text(),
            ScalarRefImpl::Int64(i) => i.to_text(),
            ScalarRefImpl::Float32(f) => f.to_text(),
            ScalarRefImpl::Float64(f) => f.to_text(),
            ScalarRefImpl::Decimal(d) => d.to_text(),
            ScalarRefImpl::Interval(i) => i.to_text(),
            ScalarRefImpl::NaiveDate(d) => d.to_text(),
            ScalarRefImpl::NaiveTime(t) => t.to_text(),
            ScalarRefImpl::NaiveDateTime(dt) => dt.to_text(),
            ScalarRefImpl::List(l) => l.to_text(),
            ScalarRefImpl::Struct(s) => s.to_text(),
            ScalarRefImpl::Utf8(v) => v.to_text(),
            ScalarRefImpl::Bytea(v) => format_bytes(v),
        }
    }
}

impl ToText for DatumRef<'_> {
    fn to_text(&self) -> String {
        match self {
            Some(data) => data.to_text(),
            None => "NULL".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ordered_float::OrderedFloat;
    use crate::types::to_text::ToText;

    #[test]
    fn test_float_to_text() {
        // f64 -> text.
        let ret: OrderedFloat<f64> = OrderedFloat::<f64>::from(1.234567890123456);
        tracing::info!("ret: {}", ret.to_text());
        assert_eq!("1.234567890123456".to_string(), ret.to_text());

        // f32 -> text.
        let ret: OrderedFloat<f32> = OrderedFloat::<f32>::from(1.234567);
        assert_eq!("1.234567".to_string(), ret.to_text());
    }
}
