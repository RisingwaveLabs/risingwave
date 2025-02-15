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

#![feature(let_chains)]

use std::vec;

use context::{generate_captured_function, CaptureContextAttr, DefineContextAttr};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{Error, ItemFn, Result};

mod context;
mod gen;
mod parse;
mod types;
mod utils;

/// Defining the RisingWave SQL function from a Rust function.
///
/// [Online version of this doc.](https://risingwavelabs.github.io/risingwave/rustdoc/risingwave_expr_macro/attr.function.html)
///
/// # Table of Contents
///
/// - [SQL Function Signature](#sql-function-signature)
///     - [Multiple Function Definitions](#multiple-function-definitions)
///     - [Type Expansion](#type-expansion)
///     - [Automatic Type Inference](#automatic-type-inference)
///     - [Custom Type Inference Function](#custom-type-inference-function)
/// - [Rust Function Signature](#rust-function-signature)
///     - [Nullable Arguments](#nullable-arguments)
///     - [Return Value](#return-value)
///     - [Variadic Function](#variadic-function)
///     - [Optimization](#optimization)
///     - [Functions Returning Strings](#functions-returning-strings)
///     - [Preprocessing Constant Arguments](#preprocessing-constant-arguments)
///     - [Context](#context)
///     - [Async Function](#async-function)
/// - [Table Function](#table-function)
/// - [Registration and Invocation](#registration-and-invocation)
/// - [Appendix: Type Matrix](#appendix-type-matrix)
///
/// The following example demonstrates a simple usage:
///
/// ```ignore
/// #[function("add(int32, int32) -> int32")]
/// fn add(x: i32, y: i32) -> i32 {
///     x + y
/// }
/// ```
///
/// # SQL Function Signature
///
/// Each function must have a signature, specified in the `function("...")` part of the macro
/// invocation. The signature follows this pattern:
///
/// ```text
/// name ( [arg_types],* [...] ) [ -> [setof] return_type ]
/// ```
///
/// Where `name` is the function name in `snake_case`, which must match the function name (in `UPPER_CASE`) defined
/// in `proto/expr.proto`.
///
/// `arg_types` is a comma-separated list of argument types. The allowed data types are listed in
/// in the `name` column of the appendix's [type matrix]. Wildcards or `auto` can also be used, as
/// explained below. If the function is variadic, the last argument can be denoted as `...`.
///
/// When `setof` appears before the return type, this indicates that the function is a set-returning
/// function (table function), meaning it can return multiple values instead of just one. For more
/// details, see the section on table functions.
///
/// If no return type is specified, the function returns `void`. However, the void type is not
/// supported in our type system, so it now returns a null value of type int.
///
/// ## Multiple Function Definitions
///
/// Multiple `#[function]` macros can be applied to a single generic Rust function to define
/// multiple SQL functions of different types. For example:
///
/// ```ignore
/// #[function("add(int16, int16) -> int16")]
/// #[function("add(int32, int32) -> int32")]
/// #[function("add(int64, int64) -> int64")]
/// fn add<T: Add>(x: T, y: T) -> T {
///     x + y
/// }
/// ```
///
/// ## Type Expansion with `*`
///
/// Types can be automatically expanded to multiple types using wildcards. Here are some examples:
///
/// - `*`: expands to all types.
/// - `*int`: expands to int16, int32, int64.
/// - `*float`: expands to float32, float64.
///
/// For instance, `#[function("cast(varchar) -> *int")]` will be expanded to the following three
/// functions:
///
/// ```ignore
/// #[function("cast(varchar) -> int16")]
/// #[function("cast(varchar) -> int32")]
/// #[function("cast(varchar) -> int64")]
/// ```
///
/// Please note the difference between `*` and `any`: `*` will generate a function for each type,
/// whereas `any` will only generate one function with a dynamic data type `Scalar`.
/// This is similar to `impl T` and `dyn T` in Rust. The performance of using `*` would be much better than `any`.
/// But we do not always prefer `*` due to better performance. In some cases, using `any` is more convenient.
/// For example, in array functions, the element type of `ListValue` is `Scalar(Ref)Impl`.
/// It is unnecessary to convert it from/into various `T`.
///
/// ## Automatic Type Inference with `auto`
///
/// Correspondingly, the return type can be denoted as `auto` to be automatically inferred based on
/// the input types. It will be inferred as the _smallest type_ that can accommodate all input types.
///
/// For example, `#[function("add(*int, *int) -> auto")]` will be expanded to:
///
/// ```ignore
/// #[function("add(int16, int16) -> int16")]
/// #[function("add(int16, int32) -> int32")]
/// #[function("add(int16, int64) -> int64")]
/// #[function("add(int32, int16) -> int32")]
/// ...
/// ```
///
/// Especially when there is only one input argument, `auto` will be inferred as the type of that
/// argument. For example, `#[function("neg(*int) -> auto")]` will be expanded to:
///
/// ```ignore
/// #[function("neg(int16) -> int16")]
/// #[function("neg(int32) -> int32")]
/// #[function("neg(int64) -> int64")]
/// ```
///
/// ## Custom Type Inference Function with `type_infer`
///
/// A few functions might have a return type that dynamically changes based on the input argument
/// types, such as `unnest`. This is mainly for composite types like `anyarray`, `struct`, and `anymap`.
///
/// In such cases, the `type_infer` option can be used to specify a function to infer the return
/// type based on the input argument types. Its function signature is
///
/// ```ignore
/// fn(&[DataType]) -> Result<DataType>
/// ```
///
/// For example:
///
/// ```ignore
/// #[function(
///     "unnest(anyarray) -> setof any",
///     type_infer = "|args| Ok(args[0].unnest_list())"
/// )]
/// ```
///
/// This type inference function will be invoked at the frontend (`infer_type_with_sigmap`).
///
/// # Rust Function Signature
///
/// The `#[function]` macro can handle various types of Rust functions.
///
/// Each argument corresponds to the *reference type* in the [type matrix].
///
/// The return value type can be the *reference type* or *owned type* in the [type matrix].
///
/// For instance:
///
/// ```ignore
/// #[function("trim_array(anyarray, int32) -> anyarray")]
/// fn trim_array(array: ListRef<'_>, n: i32) -> ListValue {...}
/// ```
///
/// ## Nullable Arguments
///
/// The functions above will only be called when all arguments are not null.
/// It will return null if any argument is null.
/// If null arguments need to be considered, the `Option` type can be used:
///
/// ```ignore
/// #[function("trim_array(anyarray, int32) -> anyarray")]
/// fn trim_array(array: ListRef<'_>, n: Option<i32>) -> ListValue {...}
/// ```
///
/// This function will be called when `n` is null, but not when `array` is null.
///
/// ## Return `NULL`s and Errors
///
/// Similarly, the return value type can be one of the following:
///
/// - `T`: Indicates that a non-null value is always returned (for non-null inputs), and errors will not occur.
/// - `Option<T>`: Indicates that a null value may be returned, but errors will not occur.
/// - `Result<T>`: Indicates that an error may occur, but a null value will not be returned.
/// - `Result<Option<T>>`: Indicates that a null value may be returned, and an error may also occur.
///
/// ## Optimization
///
/// When all input and output types of the function are *primitive type* (refer to the [type
/// matrix]) and do not contain any Option or Result, the `#[function]` macro will automatically
/// generate SIMD vectorized execution code.
///
/// Therefore, try to avoid returning `Option` and `Result` whenever possible.
///
/// ## Variadic Function
///
/// Variadic functions accept a `impl Row` input to represent tailing arguments.
/// For example:
///
/// ```ignore
/// #[function("concat_ws(varchar, ...) -> varchar")]
/// fn concat_ws(sep: &str, vals: impl Row) -> Option<Box<str>> {
///     let mut string_iter = vals.iter().flatten();
///     // ...
/// }
/// ```
///
/// See `risingwave_common::row::Row` for more details.
///
/// ## Functions Returning Strings
///
/// For functions that return varchar types, you can also use the writer style function signature to
/// avoid memory copying and dynamic memory allocation:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) {
///     writer.write_str(s.trim()).unwrap();
/// }
/// ```
///
/// If errors may be returned, then the return value should be `Result<()>`:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) -> Result<()> {
///     writer.write_str(s.trim()).unwrap();
///     Ok(())
/// }
/// ```
///
/// If null values may be returned, then the return value should be `Option<()>`:
///
/// ```ignore
/// #[function("trim(varchar) -> varchar")]
/// fn trim(s: &str, writer: &mut impl Write) -> Option<()> {
///     if s.is_empty() {
///         None
///     } else {
///         writer.write_str(s.trim()).unwrap();
///         Some(())
///     }
/// }
/// ```
///
/// ## Preprocessing Constant Arguments
///
/// When some input arguments of the function are constants, they can be preprocessed to avoid
/// calculations every time the function is called.
///
/// A classic use case is regular expression matching:
///
/// ```ignore
/// #[function(
///     "regexp_match(varchar, varchar, varchar) -> varchar[]",
///     prebuild = "RegexpContext::from_pattern_flags($1, $2)?"
/// )]
/// fn regexp_match(text: &str, regex: &RegexpContext) -> ListValue {
///     regex.captures(text).collect()
/// }
/// ```
///
/// The `prebuild` argument can be specified, and its value is a Rust expression `Type::method(...)`
/// used to construct a new variable of `Type` from the input arguments of the function.
/// Here `$1`, `$2` represent the second and third arguments of the function (indexed from 0),
/// and their types are `&str`. In the Rust function signature, these positions of parameters will
/// be omitted, replaced by an extra new variable at the end.
///
/// This macro generates two versions of the function. If all the input parameters that `prebuild`
/// depends on are constants, it will precompute them during the build function. Otherwise, it will
/// compute them for each input row during evaluation. This way, we support both constant and variable
/// inputs while optimizing performance for constant inputs.
///
/// ## Context
///
/// If a function needs to obtain type information at runtime, you can add an `&Context` parameter to
/// the function signature. For example:
///
/// ```ignore
/// #[function("foo(int32) -> int64")]
/// fn foo(a: i32, ctx: &Context) -> i64 {
///    assert_eq!(ctx.arg_types[0], DataType::Int32);
///    assert_eq!(ctx.return_type, DataType::Int64);
///    // ...
/// }
/// ```
///
/// ## Async Function
///
/// Functions can be asynchronous.
///
/// ```ignore
/// #[function("pg_sleep(float64)")]
/// async fn pg_sleep(second: F64) {
///     tokio::time::sleep(Duration::from_secs_f64(second.0)).await;
/// }
/// ```
///
/// Asynchronous functions will be evaluated on rows sequentially.
///
/// # Table Function
///
/// A table function is a special kind of function that can return multiple values instead of just
/// one. Its function signature must include the `setof` keyword, and the Rust function should
/// return an iterator of the form `impl Iterator<Item = T>` or its derived types.
///
/// For example:
/// ```ignore
/// #[function("generate_series(int32, int32) -> setof int32")]
/// fn generate_series(start: i32, stop: i32) -> impl Iterator<Item = i32> {
///     start..=stop
/// }
/// ```
///
/// Likewise, the return value `Iterator` can include `Option` or `Result` either internally or
/// externally. For instance:
///
/// - `impl Iterator<Item = Result<T>>`
/// - `Result<impl Iterator<Item = T>>`
/// - `Result<impl Iterator<Item = Result<Option<T>>>>`
///
/// Currently, table function arguments do not support the `Option` type. That is, the function will
/// only be invoked when all arguments are not null.
///
/// # Registration and Invocation
///
/// Every function defined by `#[function]` is automatically registered in the global function
/// table.
///
/// You can build expressions through the following functions:
///
/// ```ignore
/// // scalar functions
/// risingwave_expr::expr::build(...) -> BoxedExpression
/// risingwave_expr::expr::build_from_prost(...) -> BoxedExpression
/// // table functions
/// risingwave_expr::table_function::build(...) -> BoxedTableFunction
/// risingwave_expr::table_function::build_from_prost(...) -> BoxedTableFunction
/// ```
///
/// Or get their metadata through the following functions:
///
/// ```ignore
/// // scalar functions
/// risingwave_expr::sig::func::FUNC_SIG_MAP::get(...)
/// // table functions
/// risingwave_expr::sig::table_function::FUNC_SIG_MAP::get(...)
/// ```
///
/// # Appendix: Type Matrix
///
/// ## Base Types
///
/// | name        | SQL type           | owned type    | reference type     | primitive? |
/// | ----------- | ------------------ | ------------- | ------------------ | ---------- |
/// | boolean     | `boolean`          | `bool`        | `bool`             | yes        |
/// | int2        | `smallint`         | `i16`         | `i16`              | yes        |
/// | int4        | `integer`          | `i32`         | `i32`              | yes        |
/// | int8        | `bigint`           | `i64`         | `i64`              | yes        |
/// | int256      | `rw_int256`        | `Int256`      | `Int256Ref<'_>`    | no         |
/// | float4      | `real`             | `F32`         | `F32`              | yes        |
/// | float8      | `double precision` | `F64`         | `F64`              | yes        |
/// | decimal     | `numeric`          | `Decimal`     | `Decimal`          | yes        |
/// | serial      | `serial`           | `Serial`      | `Serial`           | yes        |
/// | date        | `date`             | `Date`        | `Date`             | yes        |
/// | time        | `time`             | `Time`        | `Time`             | yes        |
/// | timestamp   | `timestamp`        | `Timestamp`   | `Timestamp`        | yes        |
/// | `timestamp_ns`   | `timestamp_ns`        | `TimestampNanosecond`   | `TimestampNanosecond`        | yes        |
/// | timestamptz | `timestamptz`      | `Timestamptz` | `Timestamptz`      | yes        |
/// | interval    | `interval`         | `Interval`    | `Interval`         | yes        |
/// | varchar     | `varchar`          | `Box<str>`    | `&str`             | no         |
/// | bytea       | `bytea`            | `Box<[u8]>`   | `&[u8]`            | no         |
/// | jsonb       | `jsonb`            | `JsonbVal`    | `JsonbRef<'_>`     | no         |
/// | any         | `any`              | `ScalarImpl`  | `ScalarRefImpl<'_>`| no         |
///
/// ## Composite Types
///
/// | name                   | SQL type             | owned type    | reference type     |
/// | ---------------------- | -------------------- | ------------- | ------------------ |
/// | anyarray               | `any[]`              | `ListValue`   | `ListRef<'_>`      |
/// | struct                 | `record`             | `StructValue` | `StructRef<'_>`    |
/// | T[^1][]                | `T[]`                | `ListValue`   | `ListRef<'_>`      |
/// | struct<`name_T`[^1], ..> | `struct<name T, ..>` | `(T, ..)`     | `(&T, ..)`         |
///
/// [^1]: `T` could be any base type
///
/// [type matrix]: #appendix-type-matrix
#[proc_macro_attribute]
pub fn function(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let fn_attr: FunctionAttr = syn::parse(attr)?;
        let user_fn: UserFunctionAttr = syn::parse(item.clone())?;

        let mut tokens: TokenStream2 = item.into();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_function_descriptor(&user_fn, false)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Different from `#[function]`, which implements the `Expression` trait for a rust scalar function,
/// `#[build_function]` is used when you already implemented `Expression` manually.
///
/// The expected input is a "build" function:
/// ```ignore
/// fn(data_type: DataType, children: Vec<BoxedExpression>) -> Result<BoxedExpression>
/// ```
///
/// It generates the function descriptor using the "build" function and
/// registers the description to the `FUNC_SIG_MAP`.
#[proc_macro_attribute]
pub fn build_function(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let fn_attr: FunctionAttr = syn::parse(attr)?;
        let user_fn: UserFunctionAttr = syn::parse(item.clone())?;

        let mut tokens: TokenStream2 = item.into();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_function_descriptor(&user_fn, true)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let fn_attr: FunctionAttr = syn::parse(attr)?;
        let user_fn: AggregateFnOrImpl = syn::parse(item.clone())?;

        let mut tokens: TokenStream2 = item.into();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_aggregate_descriptor(&user_fn, false)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn build_aggregate(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let fn_attr: FunctionAttr = syn::parse(attr)?;
        let user_fn: AggregateFnOrImpl = syn::parse(item.clone())?;

        let mut tokens: TokenStream2 = item.into();
        for attr in fn_attr.expand() {
            tokens.extend(attr.generate_aggregate_descriptor(&user_fn, true)?);
        }
        Ok(tokens)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[derive(Debug, Clone, Default)]
struct FunctionAttr {
    /// Function name
    name: String,
    /// Input argument types
    args: Vec<String>,
    /// Return type
    ret: String,
    /// Whether it is a table function
    is_table_function: bool,
    /// Whether it is an append-only aggregate function
    append_only: bool,
    /// Optional function for batch evaluation.
    batch_fn: Option<String>,
    /// State type for aggregate function.
    /// If not specified, it will be the same as return type.
    state: Option<String>,
    /// Initial state value for aggregate function.
    /// If not specified, it will be NULL.
    init_state: Option<String>,
    /// Prebuild function for arguments.
    /// This could be any Rust expression.
    prebuild: Option<String>,
    /// Type inference function.
    type_infer: Option<String>,
    /// Generic type.
    generic: Option<String>,
    /// Whether the function is volatile.
    volatile: bool,
    /// If true, the function is unavailable on the frontend.
    deprecated: bool,
    /// If true, the function is not implemented on the backend, but its signature is defined.
    rewritten: bool,
}

/// Attributes from function signature `fn(..)`
#[derive(Debug, Clone)]
struct UserFunctionAttr {
    /// Function name
    name: String,
    /// Whether the function is async.
    async_: bool,
    /// Whether contains argument `&Context`.
    context: bool,
    /// Whether contains argument `&mut impl Write`.
    write: bool,
    /// Whether the last argument type is `retract: bool`.
    retract: bool,
    /// Whether each argument type is `Option<T>`.
    args_option: Vec<bool>,
    /// If the first argument type is `&mut T`, then `Some(T)`.
    first_mut_ref_arg: Option<String>,
    /// The return type kind.
    return_type_kind: ReturnTypeKind,
    /// The kind of inner type `T` in `impl Iterator<Item = T>`
    iterator_item_kind: Option<ReturnTypeKind>,
    /// The core return type without `Option` or `Result`.
    core_return_type: String,
    /// The number of generic types.
    generic: usize,
    /// The span of return type.
    return_type_span: proc_macro2::Span,
}

#[derive(Debug, Clone)]
struct AggregateImpl {
    struct_name: String,
    accumulate: UserFunctionAttr,
    retract: Option<UserFunctionAttr>,
    #[allow(dead_code)] // TODO(wrj): add merge to trait
    merge: Option<UserFunctionAttr>,
    finalize: Option<UserFunctionAttr>,
    create_state: Option<UserFunctionAttr>,
    #[allow(dead_code)] // TODO(wrj): support encode
    encode_state: Option<UserFunctionAttr>,
    #[allow(dead_code)] // TODO(wrj): support decode
    decode_state: Option<UserFunctionAttr>,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum AggregateFnOrImpl {
    /// A simple accumulate/retract function.
    Fn(UserFunctionAttr),
    /// A full impl block.
    Impl(AggregateImpl),
}

impl AggregateFnOrImpl {
    fn as_fn(&self) -> &UserFunctionAttr {
        match self {
            AggregateFnOrImpl::Fn(attr) => attr,
            _ => panic!("expect fn"),
        }
    }

    fn accumulate(&self) -> &UserFunctionAttr {
        match self {
            AggregateFnOrImpl::Fn(attr) => attr,
            AggregateFnOrImpl::Impl(impl_) => &impl_.accumulate,
        }
    }

    fn has_retract(&self) -> bool {
        match self {
            AggregateFnOrImpl::Fn(fn_) => fn_.retract,
            AggregateFnOrImpl::Impl(impl_) => impl_.retract.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReturnTypeKind {
    T,
    Option,
    Result,
    ResultOption,
}

impl FunctionAttr {
    /// Return a unique name that can be used as an identifier.
    fn ident_name(&self) -> String {
        format!("{}_{}_{}", self.name, self.args.join("_"), self.ret)
            .replace("[]", "array")
            .replace("...", "variadic")
            .replace(['<', '>', ' ', ','], "_")
            .replace("__", "_")
    }
}

impl UserFunctionAttr {
    /// Returns true if the function is like `fn(T1, T2, .., Tn) -> T`.
    fn is_pure(&self) -> bool {
        !self.async_
            && !self.write
            && !self.context
            && self.args_option.iter().all(|b| !b)
            && self.return_type_kind == ReturnTypeKind::T
    }
}

/// Define the context variables which can be used by risingwave expressions.
#[proc_macro]
pub fn define_context(def: TokenStream) -> TokenStream {
    fn inner(def: TokenStream) -> Result<TokenStream2> {
        let attr: DefineContextAttr = syn::parse(def)?;
        attr.gen()
    }

    match inner(def) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Capture the context from the local context to the function impl.
/// TODO: The macro will be merged to [`#[function(.., capture_context(..))]`](macro@function) later.
///
/// Currently, we should use the macro separately with a simple wrapper.
#[proc_macro_attribute]
pub fn capture_context(attr: TokenStream, item: TokenStream) -> TokenStream {
    fn inner(attr: TokenStream, item: TokenStream) -> Result<TokenStream2> {
        let attr: CaptureContextAttr = syn::parse(attr)?;
        let user_fn: ItemFn = syn::parse(item)?;

        // Generate captured function
        generate_captured_function(attr, user_fn)
    }
    match inner(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
