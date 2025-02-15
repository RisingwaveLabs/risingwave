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

use std::fmt::Debug;
use std::io::{Cursor, Write};
use std::mem::size_of;

use anyhow::Context;
use byteorder::{BigEndian, ReadBytesExt};
use risingwave_common_estimate_size::{EstimateSize, ZeroHeapSize};
use risingwave_pb::common::buffer::CompressionType;
use risingwave_pb::common::Buffer;
use risingwave_pb::data::{ArrayType, PbArray};

use super::{Array, ArrayBuilder, ArrayImpl, ArrayResult};
use crate::bitmap::{Bitmap, BitmapBuilder};
use crate::for_all_native_types;
use crate::types::*;

/// Physical type of array items which have fixed size.
pub trait PrimitiveArrayItemType
where
    for<'a> Self: Sized
        + Default
        + PartialOrd
        + ZeroHeapSize
        + Scalar<ScalarRefType<'a> = Self>
        + ScalarRef<'a, ScalarType = Self>,
{
    /// The data type.
    const DATA_TYPE: DataType;
    // array methods
    /// A helper to convert a primitive array to `ArrayImpl`.
    fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl;
    /// A helper to convert `ArrayImpl` to self.
    fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>>;
    /// A helper to convert `ArrayImpl` to self.
    fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>>;
    /// Returns array type of the primitive array
    fn array_type() -> ArrayType;

    // item methods
    fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize>;
    fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Self>;
}

macro_rules! impl_array_methods {
    ($scalar_type:ty, $array_type_pb:ident, $array_impl_variant:ident) => {
        const DATA_TYPE: DataType = DataType::$array_impl_variant;

        fn erase_array_type(arr: PrimitiveArray<Self>) -> ArrayImpl {
            ArrayImpl::$array_impl_variant(arr)
        }

        fn try_into_array(arr: ArrayImpl) -> Option<PrimitiveArray<Self>> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        fn try_into_array_ref(arr: &ArrayImpl) -> Option<&PrimitiveArray<Self>> {
            match arr {
                ArrayImpl::$array_impl_variant(inner) => Some(inner),
                _ => None,
            }
        }

        fn array_type() -> ArrayType {
            ArrayType::$array_type_pb
        }
    };
}

macro_rules! impl_primitive_for_native_types {
    ($({ $naive_type:ty, $scalar_type:ident, $read_fn:ident } ),*) => {
        $(
            impl PrimitiveArrayItemType for $naive_type {
                impl_array_methods!($naive_type, $scalar_type, $scalar_type);

                fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
                    NativeType::to_protobuf(self, output)
                }
                fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Self> {
                    let v = cur
                        .$read_fn::<BigEndian>()
                        .context("failed to read value from buffer")?;
                    Ok(v.into())
                }
            }
        )*
    }
}

for_all_native_types! { impl_primitive_for_native_types }

/// These types have `to_protobuf` and implement `Hash`.
macro_rules! impl_primitive_for_others {
    ($({ $scalar_type:ty, $array_type_pb:ident, $array_impl_variant:ident } ),*) => {
        $(
            impl PrimitiveArrayItemType for $scalar_type {
                impl_array_methods!($scalar_type, $array_type_pb, $array_impl_variant);

                fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
                    <$scalar_type>::to_protobuf(self, output)
                }
                fn from_protobuf(cur: &mut Cursor<&[u8]>) -> ArrayResult<Self> {
                    <$scalar_type>::from_protobuf(cur)
                }
            }
        )*
    }
}

impl_primitive_for_others! {
    { Decimal, Decimal, Decimal },
    { Interval, Interval, Interval },
    { Date, Date, Date },
    { Time, Time, Time },
    { Timestamp, Timestamp, Timestamp },
    { TimestampNanosecond, TimestampNanosecond, TimestampNanosecond },
    { Timestamptz, Timestamptz, Timestamptz }
}

/// `PrimitiveArray` is a collection of primitive types, such as `i32`, `f32`.
#[derive(Debug, Clone, PartialEq, Eq, EstimateSize)]
pub struct PrimitiveArray<T: PrimitiveArrayItemType> {
    bitmap: Bitmap,
    data: Box<[T]>,
}

impl<T: PrimitiveArrayItemType> FromIterator<Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = <Self as Array>::Builder::new(iter.size_hint().0);
        for i in iter {
            builder.append(i);
        }
        builder.finish()
    }
}

impl<'a, T: PrimitiveArrayItemType> FromIterator<&'a Option<T>> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = &'a Option<T>>>(iter: I) -> Self {
        iter.into_iter().cloned().collect()
    }
}

impl<T: PrimitiveArrayItemType> FromIterator<T> for PrimitiveArray<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data: Box<[T]> = iter.into_iter().collect();
        PrimitiveArray {
            bitmap: Bitmap::ones(data.len()),
            data,
        }
    }
}

impl FromIterator<Option<f32>> for PrimitiveArray<F32> {
    fn from_iter<I: IntoIterator<Item = Option<f32>>>(iter: I) -> Self {
        iter.into_iter().map(|o| o.map(F32::from)).collect()
    }
}

impl FromIterator<Option<f64>> for PrimitiveArray<F64> {
    fn from_iter<I: IntoIterator<Item = Option<f64>>>(iter: I) -> Self {
        iter.into_iter().map(|o| o.map(F64::from)).collect()
    }
}

impl FromIterator<f32> for PrimitiveArray<F32> {
    fn from_iter<I: IntoIterator<Item = f32>>(iter: I) -> Self {
        iter.into_iter().map(F32::from).collect()
    }
}

impl FromIterator<f64> for PrimitiveArray<F64> {
    fn from_iter<I: IntoIterator<Item = f64>>(iter: I) -> Self {
        iter.into_iter().map(F64::from).collect()
    }
}

impl<T: PrimitiveArrayItemType> PrimitiveArray<T> {
    /// Build a [`PrimitiveArray`] from iterator and bitmap.
    ///
    /// NOTE: The length of `bitmap` must be equal to the length of `iter`.
    pub fn from_iter_bitmap(iter: impl IntoIterator<Item = T>, bitmap: Bitmap) -> Self {
        let data: Box<[T]> = iter.into_iter().collect();
        assert_eq!(data.len(), bitmap.len());
        PrimitiveArray { bitmap, data }
    }

    /// Returns a slice containing the entire array.
    pub fn as_slice(&self) -> &[T] {
        &self.data
    }

    /// Returns a mutable slice containing the entire array.
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.data
    }
}

impl<T: PrimitiveArrayItemType> Array for PrimitiveArray<T> {
    type Builder = PrimitiveArrayBuilder<T>;
    type OwnedItem = T;
    type RefItem<'a> = T;

    unsafe fn raw_value_at_unchecked(&self, idx: usize) -> Self::RefItem<'_> {
        *self.data.get_unchecked(idx)
    }

    fn raw_iter(&self) -> impl ExactSizeIterator<Item = Self::RefItem<'_>> {
        self.data.iter().cloned()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn to_protobuf(&self) -> PbArray {
        let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<T>());

        for v in self.iter() {
            v.map(|node| node.to_protobuf(&mut output_buffer));
        }

        let buffer = Buffer {
            compression: CompressionType::None as i32,
            body: output_buffer,
        };
        let null_bitmap = self.null_bitmap().to_protobuf();
        PbArray {
            null_bitmap: Some(null_bitmap),
            values: vec![buffer],
            array_type: T::array_type() as i32,
            struct_array_data: None,
            list_array_data: None,
        }
    }

    fn null_bitmap(&self) -> &Bitmap {
        &self.bitmap
    }

    fn into_null_bitmap(self) -> Bitmap {
        self.bitmap
    }

    fn set_bitmap(&mut self, bitmap: Bitmap) {
        self.bitmap = bitmap;
    }

    fn data_type(&self) -> DataType {
        T::DATA_TYPE
    }
}

/// `PrimitiveArrayBuilder` constructs a `PrimitiveArray` from `Option<Primitive>`.
#[derive(Debug, Clone, EstimateSize)]
pub struct PrimitiveArrayBuilder<T: PrimitiveArrayItemType> {
    bitmap: BitmapBuilder,
    data: Vec<T>,
}

impl<T: PrimitiveArrayItemType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    type ArrayType = PrimitiveArray<T>;

    fn new(capacity: usize) -> Self {
        Self {
            bitmap: BitmapBuilder::with_capacity(capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    fn with_type(capacity: usize, ty: DataType) -> Self {
        assert_eq!(ty, T::DATA_TYPE);
        Self::new(capacity)
    }

    fn append_n(&mut self, n: usize, value: Option<T>) {
        match value {
            Some(x) => {
                self.bitmap.append_n(n, true);
                self.data.extend(std::iter::repeat(x).take(n));
            }
            None => {
                self.bitmap.append_n(n, false);
                self.data.extend(std::iter::repeat(T::default()).take(n));
            }
        }
    }

    fn append_array(&mut self, other: &PrimitiveArray<T>) {
        for bit in other.bitmap.iter() {
            self.bitmap.append(bit);
        }
        self.data.extend_from_slice(&other.data);
    }

    fn pop(&mut self) -> Option<()> {
        self.data.pop().map(|_| self.bitmap.pop().unwrap())
    }

    fn len(&self) -> usize {
        self.bitmap.len()
    }

    fn finish(self) -> PrimitiveArray<T> {
        PrimitiveArray {
            bitmap: self.bitmap.finish(),
            data: self.data.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn helper_test_builder<T: PrimitiveArrayItemType>(data: Vec<Option<T>>) -> PrimitiveArray<T> {
        let mut builder = PrimitiveArrayBuilder::<T>::new(data.len());
        for d in data {
            builder.append(d);
        }
        builder.finish()
    }

    #[test]
    fn test_i16_builder() {
        let arr = helper_test_builder::<i16>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int16(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i32_builder() {
        let arr = helper_test_builder::<i32>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_i64_builder() {
        let arr = helper_test_builder::<i64>(
            (0..1000)
                .map(|x| if x % 2 == 0 { None } else { Some(x) })
                .collect(),
        );
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Int64(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f32_builder() {
        let arr = helper_test_builder::<F32>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f32).into())
                    }
                })
                .collect(),
        );
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float32(_)) {
            unreachable!()
        }
    }

    #[test]
    fn test_f64_builder() {
        let arr = helper_test_builder::<F64>(
            (0..1000)
                .map(|x| {
                    if x % 2 == 0 {
                        None
                    } else {
                        Some((x as f64).into())
                    }
                })
                .collect(),
        );
        if !matches!(ArrayImpl::from(arr), ArrayImpl::Float64(_)) {
            unreachable!()
        }
    }
}
