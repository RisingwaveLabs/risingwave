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

use std::fmt::{Display, Formatter, Write};
use std::hash::Hasher;
use std::io::Read;
use std::mem;
use std::num::ParseIntError;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use bytes::Bytes;
use ethnum::{i256, I256};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, FromPrimitive,
    ToPrimitive, Zero,
};
use postgres_types::{ToSql, Type};
use risingwave_pb::data::ArrayType;
use serde::{Deserialize, Serialize, Serializer};
use to_text::ToText;

use crate::array::ArrayResult;
use crate::types::to_binary::ToBinary;
use crate::types::{to_text, DataType, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(Box<I256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Int256Ref<'a>(pub &'a I256);

impl Display for Int256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

macro_rules! impl_common_for_num256 {
    ($scalar:ident, $scalar_ref:ident < $gen:tt > , $inner:ty, $array_type:ident) => {
        impl Scalar for $scalar {
            type ScalarRefType<$gen> = $scalar_ref<$gen>;

            fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
                $scalar_ref(self.0.as_ref())
            }
        }

        impl<$gen> ScalarRef<$gen> for $scalar_ref<$gen> {
            type ScalarType = $scalar;

            fn to_owned_scalar(&self) -> Self::ScalarType {
                $scalar((*self.0).into())
            }

            fn hash_scalar<H: Hasher>(&self, state: &mut H) {
                use std::hash::Hash as _;
                self.0.hash(state)
            }
        }

        impl Serialize for $scalar_ref<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl<'de> Deserialize<'de> for $scalar {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                <$inner>::deserialize(deserializer).map(Into::into)
            }
        }

        impl FromStr for $scalar {
            type Err = ParseIntError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                <$inner>::from_str(s).map(Into::into)
            }
        }

        impl $scalar {
            #[inline]
            pub fn into_inner(self) -> $inner {
                *self.0
            }

            #[inline]
            pub const fn size() -> usize {
                mem::size_of::<$inner>()
            }

            #[inline]
            pub fn array_type() -> ArrayType {
                ArrayType::$array_type
            }

            #[inline]
            pub fn from_ne_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_ne_bytes(bytes)))
            }

            #[inline]
            pub fn from_le_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_le_bytes(bytes)))
            }

            #[inline]
            pub fn from_be_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_be_bytes(bytes)))
            }

            pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
                let mut buf = [0u8; mem::size_of::<$inner>()];
                input.read_exact(&mut buf)?;
                Ok(Self::from_be_bytes(buf))
            }
        }

        impl From<$inner> for $scalar {
            fn from(value: $inner) -> Self {
                Self(Box::new(value))
            }
        }

        impl $scalar_ref<'_> {
            #[inline]
            pub fn to_le_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_le_bytes()
            }

            #[inline]
            pub fn to_be_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_be_bytes()
            }

            #[inline]
            pub fn to_ne_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_ne_bytes()
            }

            pub fn to_protobuf<T: std::io::Write>(self, output: &mut T) -> ArrayResult<usize> {
                output.write(&self.to_be_bytes()).map_err(Into::into)
            }
        }

        impl ToText for $scalar_ref<'_> {
            fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }

            fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
                self.write(f)
            }
        }

        impl ToBinary for $scalar_ref<'_> {
            fn to_binary_with_type(&self, _ty: &DataType) -> crate::error::Result<Option<Bytes>> {
                let mut output = bytes::BytesMut::new();
                self.0
                    .to_be_bytes()
                    .as_ref()
                    .to_sql(&Type::ANY, &mut output)
                    .unwrap();
                Ok(Some(output.freeze()))
            }
        }
    };
}

// only for int256
impl_common_for_num256!(Int256, Int256Ref<'a>, I256, Int256);

impl FromPrimitive for Int256 {
    fn from_i64(n: i64) -> Option<Self> {
        Some(I256::from(n).into())
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(I256::from(n).into())
    }

    // Special handling for 128 bits, the default action of `FromPrimitive` is to convert `n` to
    // 64 bits before conversion, which will result in loss of precision.
    fn from_i128(n: i128) -> Option<Self> {
        Some(I256::from(n).into())
    }

    fn from_u128(n: u128) -> Option<Self> {
        Some(I256::from(n).into())
    }
}

impl ToPrimitive for Int256 {
    fn to_i64(&self) -> Option<i64> {
        (*self.0 <= i256::from(i64::MAX)).then_some(self.0.as_i64())
    }

    fn to_u64(&self) -> Option<u64> {
        (*self.0 <= i256::from(u64::MAX)).then_some(self.0.as_u64())
    }

    // Special handling for 128 bits, the default action of `ToPrimitive` is to convert self to
    // 64 bits before conversion, which will result in loss of precision.
    fn to_i128(&self) -> Option<i128> {
        (*self.0 <= i256::from(i128::MAX)).then_some(self.0.as_i128())
    }

    fn to_u128(&self) -> Option<u128> {
        (*self.0 <= i256::from(u128::MAX)).then_some(self.0.as_u128())
    }
}

macro_rules! impl_from_type {
    ($source:ty, $call:path, $target:ty) => {
        impl core::convert::From<$source> for $target {
            #[inline]
            fn from(t: $source) -> Self {
                $call(t).unwrap()
            }
        }
    };
}

impl_from_type!(isize, FromPrimitive::from_isize, Int256);
impl_from_type!(i8, FromPrimitive::from_i8, Int256);
impl_from_type!(i16, FromPrimitive::from_i16, Int256);
impl_from_type!(i32, FromPrimitive::from_i32, Int256);
impl_from_type!(i64, FromPrimitive::from_i64, Int256);
impl_from_type!(i128, FromPrimitive::from_i128, Int256);
impl_from_type!(usize, FromPrimitive::from_usize, Int256);
impl_from_type!(u8, FromPrimitive::from_u8, Int256);
impl_from_type!(u16, FromPrimitive::from_u16, Int256);
impl_from_type!(u32, FromPrimitive::from_u32, Int256);
impl_from_type!(u64, FromPrimitive::from_u64, Int256);
impl_from_type!(u128, FromPrimitive::from_u128, Int256);

impl From<Int256Ref<'_>> for Int256 {
    fn from(value: Int256Ref<'_>) -> Self {
        Self(Box::new(*value.0))
    }
}

macro_rules! impl_checked_op {
    ($trait:ty, $func:ident, $op:tt, $proxied_trait:tt, $proxied_func:ident, $scalar:ident, $scalar_ref:ident < $gen:tt >) => {
        impl $proxied_trait<Self> for $scalar {
            type Output = Self;

            fn $proxied_func(self, rhs: Self) -> Self::Output {
                $scalar::from(self.0.as_ref() $op rhs.0.as_ref())
            }
        }

        impl<$gen> $proxied_trait<Self> for $scalar_ref<$gen> {
            type Output = $scalar;

            fn $proxied_func(self, rhs: Self) -> Self::Output {
                Int256::from(self.0 $op rhs.0)
            }
        }

        impl $trait for $scalar {
            fn $func(&self, other: &Self) -> Option<Self> {
                self.0.$func(*other.0).map(Into::into)
            }
        }
    };
}

impl_checked_op!(CheckedAdd, checked_add, +, Add, add, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedSub, checked_sub, -, Sub, sub, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedMul, checked_mul, *, Mul, mul, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedDiv, checked_div, /, Div, div, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedRem, checked_rem, %, Rem, rem, Int256, Int256Ref<'a>);

impl Neg for Int256 {
    type Output = Int256;

    fn neg(self) -> Self::Output {
        Int256::from(self.0.neg())
    }
}

impl CheckedNeg for Int256 {
    fn checked_neg(&self) -> Option<Self> {
        self.0.checked_neg().map(Into::into)
    }
}

impl Zero for Int256 {
    fn zero() -> Self {
        Int256::from(i256::new(0))
    }

    fn is_zero(&self) -> bool {
        !self.0.is_negative() && !self.0.is_positive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! check_convert {
        ($to_type:ident, [$($number:expr),+]) => {
            paste::item! {
                $(assert_eq!(Int256::from($number).[<to_ $to_type>]().unwrap(), $number as $to_type);)+
            }
        };
    }

    #[test]
    #[allow(clippy::unnecessary_cast)]
    fn basic_test() {
        check_convert!(u8, [0, 1, u8::MAX]);
        check_convert!(i8, [0, -1, 1, i8::MAX]);

        check_convert!(u16, [0, 1, u16::MAX]);
        check_convert!(i16, [0, -1, 1, i16::MAX]);

        check_convert!(u32, [0, 1, u32::MAX]);
        check_convert!(i32, [0, -1, 1, i32::MAX]);

        check_convert!(u64, [0, 1, u64::MAX]);
        check_convert!(i64, [0, -1, 1, i64::MAX]);

        check_convert!(u128, [0, 1, u128::MAX]);
        check_convert!(i128, [0, -1, 1, i128::MAX]);
    }

    #[test]
    fn more_than_i128() {
        let i = Int256::from(i256::from(i128::MAX) + i256::from(i128::MAX));
        assert_eq!(i.to_u128(), Some(u128::MAX - 1));
    }

    #[test]
    fn test_op() {
        assert_eq!(
            Int256::from(i256::from(i128::MAX) + i256::from(i128::MAX)),
            Int256::from(i128::MAX) + Int256::from(i128::MAX),
        );
    }
}
