use std::fmt;
use std::sync::Arc;

use itertools::{multizip, Itertools};
use paste::paste;

/// Template macro to generate code for unary/binary/ternary expression.
use crate::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, BytesGuard, BytesWriter, DataChunk, Utf8ArrayBuilder,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{option_as_scalar_ref, DataType, Scalar};

macro_rules! gen_expr_normal {
  ($ty_name:ident,$($arg:ident),*) => {
    paste! {
      pub struct $ty_name<
        $($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem>,
      > {
        // pub the fields in super mod, so that we can construct it directly.
        // FIXME: make private while new function available.
        $(pub(super) [<expr_ $arg:lower>]: BoxedExpression,)*
        pub(super) return_type: DataType,
        pub(super) func: F,
        pub(super) _phantom: std::marker::PhantomData<($($arg, )* OA)>,
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
      > fmt::Debug for $ty_name<$($arg, )* OA, F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          f.debug_struct(stringify!($ty_name))
          .field("func", &std::any::type_name::<F>())
          $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
          .field("return_type", &self.return_type)
          .finish()
        }
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
      >  Expression for $ty_name<$($arg, )* OA, F>
      where
        $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
        for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
      {
        fn return_type(&self) -> DataType {
          self.return_type
        }

        fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
          $(
            let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval(data_chunk)?;
            let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
          )*

          let bitmap = data_chunk.get_visibility_ref();
          let mut output_array = <OA as Array>::Builder::new(data_chunk.capacity())?;
          Ok(Arc::new(match bitmap {
            Some(bitmap) => {
              for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip_eq(bitmap.iter()) {
                if !visible {
                  continue;
                }
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let ret = (self.func)($([<v_ $arg:lower>], )*)?;
                  let output = Some(ret.as_scalar_ref());
                  output_array.append(output)?;
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
            None => {
              for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let ret = (self.func)($([<v_ $arg:lower>], )*)?;
                  let output = Some(ret.as_scalar_ref());
                  output_array.append(output)?;
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
          }))
        }
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
      > $ty_name<$($arg, )* OA, F> {
        // Compile failed due to some GAT lifetime issues so make this field private.
        // Check issues #742.
        fn new(
          $([<expr_ $arg:lower>]: BoxedExpression, )*
          return_type: DataType,
          func: F,
        ) -> Self {
          Self {
            $([<expr_ $arg:lower>], )*
            return_type,
            func,
            _phantom : std::marker::PhantomData,
          }
        }
      }
    }
  }
}

macro_rules! gen_expr_bytes {
  ($ty_name:ident, $($arg:ident),*) => {
    paste! {
      pub struct $ty_name<
        $($arg: Array, )*
        F: for<'a> Fn($($arg::RefItem<'a>, )* BytesWriter) -> Result<BytesGuard>,
      > {
        // pub the fields in super mod, so that we can construct it directly.
        // FIXME: make private while new function available.
        $(pub(super) [<expr_ $arg:lower>]: BoxedExpression,)*
        pub(super) return_type: DataType,
        pub(super) func: F,
        pub(super) _phantom: std::marker::PhantomData<($($arg, )*)>,
      }

      impl<$($arg: Array, )*
        F: for<'a> Fn($($arg::RefItem<'a>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
      > fmt::Debug for $ty_name<$($arg, )* F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          f.debug_struct(stringify!($ty_name))
          .field("func", &std::any::type_name::<F>())
          $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
          .field("return_type", &self.return_type)
          .finish()
        }
      }

      impl<$($arg: Array, )*
        F: for<'a> Fn($($arg::RefItem<'a>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
      > Expression for $ty_name<$($arg, )* F>
      where
        $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
      {
        fn return_type(&self) -> DataType {
          self.return_type
        }

        fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
          $(
            let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval(data_chunk)?;
            let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
          )*

          let bitmap = data_chunk.get_visibility_ref();
          let mut output_array = Utf8ArrayBuilder::new(data_chunk.capacity())?;
          Ok(Arc::new(match bitmap {
            Some(bitmap) => {
              for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip_eq(bitmap.iter()) {
                if !visible {
                  continue;
                }
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let writer = output_array.writer();
                  let guard = (self.func)($([<v_ $arg:lower>], )* writer)?;
                  output_array = guard.into_inner();
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
            None => {
              for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let writer = output_array.writer();
                  let guard = (self.func)($([<v_ $arg:lower>], )* writer)?;
                  output_array = guard.into_inner();
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
          }))
        }
      }

      impl<$($arg: Array, )*
        F: for<'a> Fn($($arg::RefItem<'a>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
      > $ty_name<$($arg, )* F> {
        fn new(
          $([<expr_ $arg:lower>]: BoxedExpression, )*
          return_type: DataType,
          func: F,
        ) -> Self {
          Self {
            $([<expr_ $arg:lower>], )*
            return_type,
            func,
             _phantom: std::marker::PhantomData,
          }
        }
      }
    }
  }
}

#[allow(dead_code, unused_macros)]
macro_rules! gen_expr_nullable {
  ($ty_name:ident,$($arg:ident),*) => {
    paste! {
      pub struct $ty_name<
        $($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($(Option<$arg::RefItem<'a>>, )*) -> Result<Option<OA::OwnedItem>>,
      > {
        // pub the fields in super mod, so that we can construct it directly.
        // FIXME: make private while new function available.
        $(pub(super) [<expr_ $arg:lower>]: BoxedExpression,)*
        pub(super) return_type: DataType,
        pub(super) func: F,
        pub(super) _phantom: std::marker::PhantomData<($($arg, )* OA)>,
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($(Option<$arg::RefItem<'a>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
      > fmt::Debug for $ty_name<$($arg, )* OA, F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
          f.debug_struct(stringify!($ty_name))
          .field("func", &std::any::type_name::<F>())
          $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
          .field("return_type", &self.return_type)
          .finish()
        }
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($(Option<$arg::RefItem<'a>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
      > Expression for $ty_name<$($arg, )* OA, F>
      where
        $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
        for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
      {
        fn return_type(&self) -> DataType {
          self.return_type
        }

        fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
          $(
            let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval(data_chunk)?;
            let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
          )*

          let bitmap = data_chunk.get_visibility_ref();
          let mut output_array = <OA as Array>::Builder::new(data_chunk.capacity())?;
          Ok(Arc::new(match bitmap {
            Some(bitmap) => {
              for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip_eq(bitmap.iter()) {
                if !visible {
                  continue;
                }
                let ret = (self.func)($([<v_$arg:lower>],)*)?;
                output_array.append(option_as_scalar_ref(&ret))?;
              }
              output_array.finish()?.into()
            }
            None => {
              for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                let ret = (self.func)($([<v_$arg:lower>],)*)?;
                output_array.append(option_as_scalar_ref(&ret))?;
              }
              output_array.finish()?.into()
            }
          }))
        }
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($(Option<$arg::RefItem<'a>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
      > $ty_name<$($arg, )* OA, F> {
        // Compile failed due to some GAT lifetime issues so make this field private.
        // Check issues #742.
        fn new(
          $([<expr_ $arg:lower>]: BoxedExpression, )*
          return_type: DataType,
          func: F,
        ) -> Self {
          Self {
            $([<expr_ $arg:lower>], )*
            return_type,
            func,
            _phantom: std::marker::PhantomData,
          }
        }
      }
    }
  }
}

gen_expr_normal!(UnaryExpression, IA1);
gen_expr_normal!(BinaryExpression, IA1, IA2);
gen_expr_normal!(TernaryExpression, IA1, IA2, IA3);

gen_expr_bytes!(UnaryBytesExpression, IA1);
gen_expr_bytes!(BinaryBytesExpression, IA1, IA2);
gen_expr_bytes!(TernaryBytesExpression, IA1, IA2, IA3);

gen_expr_nullable!(UnaryNullableExpression, IA1);
gen_expr_nullable!(BinaryNullableExpression, IA1, IA2);
