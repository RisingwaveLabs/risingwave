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

use std::fmt::{Result, Write};
use std::num::FpCategory;

use super::{DataType, DatumRef, ScalarRefImpl};
use crate::dispatch_scalar_ref_variants;

// Used to convert ScalarRef to text format
pub trait ToText {
    /// Write the text to the writer *regardless* of its data type
    ///
    /// See `ToText::to_text` for more details.
    fn write<W: Write>(&self, f: &mut W) -> Result;

    /// Write the text to the writer according to its data type
    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> Result;

    /// Convert to text according to its data type
    fn to_text_with_type(&self, ty: &DataType) -> String {
        let mut s = String::new();
        self.write_with_type(ty, &mut s).unwrap();
        s
    }

    /// `to_text` is a special version of `to_text_with_type`, it convert the scalar to default type
    /// text. E.g. for Int64, it will convert to text as a Int64 type.
    /// We should prefer to use `to_text_with_type` because it's more clear and readable.
    ///
    /// Following is the relationship between scalar and default type:
    /// - `ScalarRefImpl::Int16` -> `DataType::Int16`
    /// - `ScalarRefImpl::Int32` -> `DataType::Int32`
    /// - `ScalarRefImpl::Int64` -> `DataType::Int64`
    /// - `ScalarRefImpl::Int256` -> `DataType::Int256`
    /// - `ScalarRefImpl::Float32` -> `DataType::Float32`
    /// - `ScalarRefImpl::Float64` -> `DataType::Float64`
    /// - `ScalarRefImpl::Decimal` -> `DataType::Decimal`
    /// - `ScalarRefImpl::Boolean` -> `DataType::Boolean`
    /// - `ScalarRefImpl::Utf8` -> `DataType::Varchar`
    /// - `ScalarRefImpl::Bytea` -> `DataType::Bytea`
    /// - `ScalarRefImpl::Date` -> `DataType::Date`
    /// - `ScalarRefImpl::Time` -> `DataType::Time`
    /// - `ScalarRefImpl::Timestamp` -> `DataType::Timestamp`
    /// - `ScalarRefImpl::Interval` -> `DataType::Interval`
    /// - `ScalarRefImpl::List` -> `DataType::List`
    /// - `ScalarRefImpl::Struct` -> `DataType::Struct`
    fn to_text(&self) -> String {
        let mut s = String::new();
        self.write(&mut s).unwrap();
        s
    }
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{self}")
                }
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

macro_rules! implement_using_itoa {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{}", itoa::Buffer::new().format(*self))
                }
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_to_string! {
    { String ,Varchar },
    { &str ,Varchar}
}

implement_using_itoa! {
    { i16, Int16 },
    { i32, Int32 },
    { i64, Int64 }
}

macro_rules! implement_using_ryu {
    ($({ $scalar_type:ty, $data_type:ident } ),*) => {
            $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    let inner = self.0;
                    match inner.classify() {
                        FpCategory::Infinite if inner.is_sign_negative() => write!(f, "-Infinity"),
                        FpCategory::Infinite => write!(f, "Infinity"),
                        FpCategory::Zero if inner.is_sign_negative() => write!(f, "-0"),
                        FpCategory::Nan => write!(f, "NaN"),
                        _ => {
                            let mut buf = ryu::Buffer::new();
                            let mut s = buf.format_finite(self.0);
                            if let Some(trimmed) = s.strip_suffix(".0") {
                                s = trimmed;
                            }
                            if let Some(mut idx) = s.as_bytes().iter().position(|x| *x == b'e') {
                                idx += 1;
                                write!(f, "{}", &s[..idx])?;
                                if s.as_bytes()[idx] == b'-' {
                                    write!(f, "-")?;
                                    idx += 1;
                                } else {
                                    write!(f, "+")?;
                                }
                                if idx + 1 == s.len() {
                                    write!(f, "0")?;
                                }
                                write!(f, "{}", &s[idx..])?;
                            } else {
                                write!(f, "{}", s)?;
                            }
                            Ok(())
                        }
                    }
                }
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_ryu! {
    { crate::types::F32, Float32 },
    { crate::types::F64, Float64 }
}

impl ToText for bool {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        if *self {
            write!(f, "t")
        } else {
            write!(f, "f")
        }
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match ty {
            DataType::Boolean => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for &[u8] {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        write!(f, "\\x{}", hex::encode(self))
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match ty {
            DataType::Bytea => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for ScalarRefImpl<'_> {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        dispatch_scalar_ref_variants!(self, v, { v.write(f) })
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        dispatch_scalar_ref_variants!(self, v, { v.write_with_type(ty, f) })
    }
}

impl ToText for DatumRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        match self {
            Some(data) => data.write(f),
            None => write!(f, "NULL"),
        }
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match self {
            Some(data) => data.write_with_type(ty, f),
            None => write!(f, "NULL"),
        }
    }
}

/// Double quote a string if it contains any special characters.
pub fn quote(input: &str, writer: &mut impl Write) -> std::fmt::Result {
    if !input.is_empty() // non-empty
        && input.trim() == input // no leading or trailing whitespace
        && !input.contains(&['(', ')', ',', '"', '\\'][..])
    {
        return writer.write_str(input);
    }

    writer.write_char('"')?;

    for ch in input.chars() {
        match ch {
            '"' => writer.write_str("\"\"")?,
            '\\' => writer.write_str("\\\\")?,
            _ => writer.write_char(ch)?,
        }
    }

    writer.write_char('"')
}

/// Remove double quotes from a string.
/// This is the reverse of [`quote`].
pub fn unquote(input: &str, writer: &mut impl Write) -> std::fmt::Result {
    if !(input.starts_with('"') && input.ends_with('"')) {
        return writer.write_str(input);
    }

    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    writer.write_char('"')?;
                }
            }
            '\\' => {
                if let Some(next_char) = chars.next() {
                    writer.write_char(next_char)?;
                }
            }
            _ => writer.write_char(ch)?,
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ordered_float::OrderedFloat;
    use crate::types::ToText;

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

    #[test]
    fn test_quote() {
        fn test(input: &str, quoted: &str) {
            let mut actual = String::new();
            quote(input, &mut actual).unwrap();
            assert_eq!(quoted, actual);

            let mut actual = String::new();
            unquote(quoted, &mut actual).unwrap();
            assert_eq!(input, actual);
        }
        test("abc", "abc");
        test("", r#""""#);
        test(" x ", r#"" x ""#);
        test(r#"a"bc"#, r#""a""bc""#);
        test(r#"a\bc"#, r#""a\\bc""#);
        test("{1}", "{1}");
        test("{1,2}", r#""{1,2}""#);
        test(r#"{"f": 1}"#, r#""{""f"": 1}""#);
    }
}
