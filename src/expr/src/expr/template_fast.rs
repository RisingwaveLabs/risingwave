//! Generic expressions for fast evaluation.
//!
//! Expressions in this module utilize auto-vectorization (SIMD) to speed up evaluation.
//!
//! It contains:
//! - [`BooleanUnaryExpression`] for boolean operations, like `not`.
//! - [`BooleanBinaryExpression`] for boolean comparisons, like `eq`.
//! - [`UnaryExpression`] for unary operations on [`PrimitiveArray`], like `bitwise_not`.
//! - [`BinaryExpression`] for binary operations on [`PrimitiveArray`], like `bitwise_and`.
//! - [`CompareExpression`] for comparisons on [`PrimitiveArray`], like `eq`.
//!
//! Note that to enable vectorization, operations must be applied to every element in the array,
//! without any branching. So it is only suitable for infallible operations.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use risingwave_common::array::{
    Array, ArrayImpl, ArrayRef, BoolArray, DataChunk, PrimitiveArray, PrimitiveArrayItemType,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum, Scalar};

use super::{BoxedExpression, Expression};

pub struct BooleanUnaryExpression<FA, FV> {
    child: BoxedExpression,
    f_array: FA,
    f_value: FV,
}

impl<FA, FV> fmt::Debug for BooleanUnaryExpression<FA, FV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BooleanUnaryExpression")
            .field("child", &self.child)
            .finish()
    }
}

impl<FA, FV> BooleanUnaryExpression<FA, FV>
where
    FA: Fn(&BoolArray) -> BoolArray + Send + Sync,
    FV: Fn(Option<bool>) -> Option<bool> + Send + Sync,
{
    pub fn new(child: BoxedExpression, f_array: FA, f_value: FV) -> Self {
        BooleanUnaryExpression {
            child,
            f_array,
            f_value,
        }
    }
}

impl<FA, FV> Expression for BooleanUnaryExpression<FA, FV>
where
    FA: Fn(&BoolArray) -> BoolArray + Send + Sync,
    FV: Fn(Option<bool>) -> Option<bool> + Send + Sync,
{
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, data_chunk: &DataChunk) -> crate::Result<ArrayRef> {
        let child = self.child.eval_checked(data_chunk)?;
        let a = child.as_bool();
        let c = (self.f_array)(a);
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, row: &OwnedRow) -> crate::Result<Datum> {
        let datum = self.child.eval_row(row)?;
        let scalar = datum.map(|s| *s.as_bool());
        let output_scalar = (self.f_value)(scalar);
        let output_datum = output_scalar.map(|s| s.to_scalar_value());
        Ok(output_datum)
    }
}

pub struct BooleanBinaryExpression<FA, FV> {
    left: BoxedExpression,
    right: BoxedExpression,
    f_array: FA,
    f_value: FV,
}

impl<FA, FV> fmt::Debug for BooleanBinaryExpression<FA, FV> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BooleanBinaryExpression")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<FA, FV> BooleanBinaryExpression<FA, FV>
where
    FA: Fn(&BoolArray, &BoolArray) -> BoolArray + Send + Sync,
    FV: Fn(Option<bool>, Option<bool>) -> Option<bool> + Send + Sync,
{
    pub fn new(left: BoxedExpression, right: BoxedExpression, f_array: FA, f_value: FV) -> Self {
        BooleanBinaryExpression {
            left,
            right,
            f_array,
            f_value,
        }
    }
}

impl<FA, FV> Expression for BooleanBinaryExpression<FA, FV>
where
    FA: Fn(&BoolArray, &BoolArray) -> BoolArray + Send + Sync,
    FV: Fn(Option<bool>, Option<bool>) -> Option<bool> + Send + Sync,
{
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, data_chunk: &DataChunk) -> crate::Result<ArrayRef> {
        let left = self.left.eval_checked(data_chunk)?;
        let right = self.right.eval_checked(data_chunk)?;
        let a = left.as_bool();
        let b = right.as_bool();
        let c = (self.f_array)(a, b);
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, row: &OwnedRow) -> crate::Result<Datum> {
        let left = self.left.eval_row(row)?.map(|s| *s.as_bool());
        let right = self.right.eval_row(row)?.map(|s| *s.as_bool());
        let output_scalar = (self.f_value)(left, right);
        let output_datum = output_scalar.map(|s| s.to_scalar_value());
        Ok(output_datum)
    }
}

pub struct UnaryExpression<F, A, T> {
    child: BoxedExpression,
    func: F,
    _marker: PhantomData<(A, T)>,
}

impl<F, A, T> fmt::Debug for UnaryExpression<F, A, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnaryExpression")
            .field("child", &self.child)
            .finish()
    }
}

impl<F, A, T> UnaryExpression<F, A, T>
where
    F: Fn(A) -> T + Send + Sync,
    A: PrimitiveArrayItemType,
    T: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
{
    pub fn new(child: BoxedExpression, func: F) -> Self {
        UnaryExpression {
            child,
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, A, T> Expression for UnaryExpression<F, A, T>
where
    F: Fn(A) -> T + Send + Sync,
    A: PrimitiveArrayItemType,
    T: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
{
    fn return_type(&self) -> DataType {
        T::data_type()
    }

    fn eval(&self, data_chunk: &DataChunk) -> crate::Result<ArrayRef> {
        let child = self.child.eval_checked(data_chunk)?;

        let bitmap = match data_chunk.get_visibility_ref() {
            Some(vis) => vis & child.null_bitmap(),
            None => child.null_bitmap().clone(),
        };
        let a: &PrimitiveArray<A> = (&*child).into();
        let c = PrimitiveArray::<T>::from_iter_bitmap(a.raw_iter().map(|a| (self.func)(a)), bitmap);
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, row: &OwnedRow) -> crate::Result<Datum> {
        let datum = self.child.eval_row(row)?;
        let scalar = datum
            .as_ref()
            .map(|s| s.as_scalar_ref_impl().try_into().unwrap());

        let output_scalar = match scalar {
            Some(l) => Some((self.func)(l)),
            _ => None,
        };
        let output_datum = output_scalar.map(|s| s.to_scalar_value());
        Ok(output_datum)
    }
}

pub struct BinaryExpression<F, A, B, T> {
    left: BoxedExpression,
    right: BoxedExpression,
    func: F,
    _marker: PhantomData<(A, B, T)>,
}

impl<F, A, B, T> fmt::Debug for BinaryExpression<F, A, B, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinaryExpression")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<F, A, B, T> BinaryExpression<F, A, B, T>
where
    F: Fn(A, B) -> T + Send + Sync,
    A: PrimitiveArrayItemType,
    B: PrimitiveArrayItemType,
    T: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
    for<'a> &'a PrimitiveArray<B>: From<&'a ArrayImpl>,
{
    pub fn new(left: BoxedExpression, right: BoxedExpression, func: F) -> Self {
        BinaryExpression {
            left,
            right,
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, A, B, T> Expression for BinaryExpression<F, A, B, T>
where
    F: Fn(A, B) -> T + Send + Sync,
    A: PrimitiveArrayItemType,
    B: PrimitiveArrayItemType,
    T: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
    for<'a> &'a PrimitiveArray<B>: From<&'a ArrayImpl>,
{
    fn return_type(&self) -> DataType {
        T::data_type()
    }

    fn eval(&self, data_chunk: &DataChunk) -> crate::Result<ArrayRef> {
        let left = self.left.eval_checked(data_chunk)?;
        let right = self.right.eval_checked(data_chunk)?;
        assert_eq!(left.len(), right.len());

        let mut bitmap = match data_chunk.get_visibility_ref() {
            Some(vis) => vis.clone(),
            None => Bitmap::ones(data_chunk.capacity()),
        };
        bitmap &= left.null_bitmap();
        bitmap &= right.null_bitmap();
        let a: &PrimitiveArray<A> = (&*left).into();
        let b: &PrimitiveArray<B> = (&*right).into();
        let c = PrimitiveArray::<T>::from_iter_bitmap(
            a.raw_iter()
                .zip(b.raw_iter())
                .map(|(a, b)| (self.func)(a, b)),
            bitmap,
        );
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, row: &OwnedRow) -> crate::Result<Datum> {
        let datum1 = self.left.eval_row(row)?;
        let datum2 = self.right.eval_row(row)?;
        let scalar1 = datum1
            .as_ref()
            .map(|s| s.as_scalar_ref_impl().try_into().unwrap());
        let scalar2 = datum2
            .as_ref()
            .map(|s| s.as_scalar_ref_impl().try_into().unwrap());

        let output_scalar = match (scalar1, scalar2) {
            (Some(l), Some(r)) => Some((self.func)(l, r)),
            _ => None,
        };
        let output_datum = output_scalar.map(|s| s.to_scalar_value());
        Ok(output_datum)
    }
}

// Basically the same as `BinaryExpression`, but output the `BoolArray`.
pub struct CompareExpression<F, A, B> {
    left: BoxedExpression,
    right: BoxedExpression,
    func: F,
    _marker: PhantomData<(A, B)>,
}

impl<F, A, B> fmt::Debug for CompareExpression<F, A, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompareExpression")
            .field("left", &self.left)
            .field("right", &self.right)
            .finish()
    }
}

impl<F, A, B> CompareExpression<F, A, B>
where
    F: Fn(A, B) -> bool + Send + Sync,
    A: PrimitiveArrayItemType,
    B: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
    for<'a> &'a PrimitiveArray<B>: From<&'a ArrayImpl>,
{
    pub fn new(left: BoxedExpression, right: BoxedExpression, func: F) -> Self {
        CompareExpression {
            left,
            right,
            func,
            _marker: PhantomData,
        }
    }
}

impl<F, A, B> Expression for CompareExpression<F, A, B>
where
    F: Fn(A, B) -> bool + Send + Sync,
    A: PrimitiveArrayItemType,
    B: PrimitiveArrayItemType,
    for<'a> &'a PrimitiveArray<A>: From<&'a ArrayImpl>,
    for<'a> &'a PrimitiveArray<B>: From<&'a ArrayImpl>,
{
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, data_chunk: &DataChunk) -> crate::Result<ArrayRef> {
        let left = self.left.eval_checked(data_chunk)?;
        let right = self.right.eval_checked(data_chunk)?;
        assert_eq!(left.len(), right.len());

        let mut bitmap = match data_chunk.get_visibility_ref() {
            Some(vis) => vis.clone(),
            None => Bitmap::ones(data_chunk.capacity()),
        };
        bitmap &= left.null_bitmap();
        bitmap &= right.null_bitmap();
        let a: &PrimitiveArray<A> = (&*left).into();
        let b: &PrimitiveArray<B> = (&*right).into();
        let c = BoolArray::from_iter_bitmap(
            a.raw_iter()
                .zip(b.raw_iter())
                .map(|(a, b)| (self.func)(a, b)),
            bitmap,
        );
        Ok(Arc::new(c.into()))
    }

    fn eval_row(&self, row: &OwnedRow) -> crate::Result<Datum> {
        let datum1 = self.left.eval_row(row)?;
        let datum2 = self.right.eval_row(row)?;
        let scalar1 = datum1
            .as_ref()
            .map(|s| s.as_scalar_ref_impl().try_into().unwrap());
        let scalar2 = datum2
            .as_ref()
            .map(|s| s.as_scalar_ref_impl().try_into().unwrap());

        let output_scalar = match (scalar1, scalar2) {
            (Some(l), Some(r)) => Some((self.func)(l, r)),
            _ => None,
        };
        let output_datum = output_scalar.map(|s| s.to_scalar_value());
        Ok(output_datum)
    }
}
