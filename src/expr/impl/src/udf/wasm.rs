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

use std::borrow::Cow;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{anyhow, bail};
use arrow_udf_wasm::Runtime;
use futures_util::StreamExt;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_expr::sig::UdfOptions;

use super::*;

#[linkme::distributed_slice(UDF_IMPLS)]
static WASM: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, _runtime, _link| language == "wasm",
    create_fn: create_wasm,
    build_fn: build,
};

#[linkme::distributed_slice(UDF_IMPLS)]
static RUST: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, _runtime, _link| language == "rust",
    create_fn: create_rust,
    build_fn: build,
};

fn create_wasm(opts: CreateFunctionOptions<'_>) -> Result<CreateFunctionOutput> {
    let wasm_binary: Cow<'_, [u8]> = if let Some(link) = opts.using_link {
        read_file_from_link(link)?.into()
    } else if let Some(bytes) = opts.using_base64_decoded {
        bytes.into()
    } else {
        bail!("USING must be specified")
    };
    let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
    if runtime.abi_version().0 <= 2 {
        bail!("legacy arrow-udf is no longer supported. please update arrow-udf to 0.3+");
    }
    let identifier_v1 = wasm_identifier_v1(
        opts.name,
        opts.arg_types,
        opts.return_type,
        opts.kind.is_table(),
    );
    let identifier = find_wasm_identifier_v2(&runtime, &identifier_v1)?;
    let compressed_binary = Some(zstd::stream::encode_all(&*wasm_binary, 0)?);
    Ok(CreateFunctionOutput {
        identifier,
        body: None,
        compressed_binary,
    })
}

fn create_rust(opts: CreateFunctionOptions<'_>) -> Result<CreateFunctionOutput> {
    if opts.using_link.is_some() {
        bail!("USING is not supported for rust function");
    }
    let identifier_v1 = wasm_identifier_v1(
        opts.name,
        opts.arg_types,
        opts.return_type,
        opts.kind.is_table(),
    );
    // if the function returns a struct, users need to add `#[function]` macro by themselves.
    // otherwise, we add it automatically. the code should start with `fn ...`.
    let function_macro = if opts.return_type.is_struct() {
        String::new()
    } else {
        format!("#[function(\"{}\")]", identifier_v1)
    };
    let script = format!(
        "use arrow_udf::{{function, types::*}};\n{}\n{}",
        function_macro,
        opts.as_.context("AS must be specified")?
    );
    let body = Some(script.clone());

    let wasm_binary = std::thread::spawn(move || {
        let mut opts = arrow_udf_wasm::build::BuildOpts::default();
        opts.arrow_udf_version = Some("0.5".to_owned());
        opts.script = script;
        // use a fixed tempdir to reuse the build cache
        opts.tempdir = Some(std::env::temp_dir().join("risingwave-rust-udf"));

        arrow_udf_wasm::build::build_with(&opts)
    })
    .join()
    .unwrap()
    .context("failed to build rust function")?;

    let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
    let identifier = find_wasm_identifier_v2(&runtime, &identifier_v1)?;

    let compressed_binary = Some(zstd::stream::encode_all(wasm_binary.as_slice(), 0)?);
    Ok(CreateFunctionOutput {
        identifier,
        body,
        compressed_binary,
    })
}

fn build(opts: UdfOptions<'_>) -> Result<Box<dyn UdfImpl>> {
    let compressed_binary = opts
        .compressed_binary
        .context("compressed binary is required")?;
    let wasm_binary =
        zstd::stream::decode_all(compressed_binary).context("failed to decompress wasm binary")?;
    let runtime = get_or_create_wasm_runtime(&wasm_binary)?;
    Ok(Box::new(WasmFunction {
        runtime,
        identifier: opts.identifier.to_owned(),
    }))
}

#[derive(Debug)]
struct WasmFunction {
    runtime: Arc<Runtime>,
    identifier: String,
}

#[async_trait::async_trait]
impl UdfImpl for WasmFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.runtime.call(&self.identifier, input)
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        self.runtime
            .call_table_function(&self.identifier, input)
            .map(|s| futures_util::stream::iter(s).boxed())
    }

    fn is_legacy(&self) -> bool {
        // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
        self.runtime.abi_version().0 <= 2
    }
}

/// Get or create a wasm runtime.
///
/// Runtimes returned by this function are cached inside for at least 60 seconds.
/// Later calls with the same binary will reuse the same runtime.
fn get_or_create_wasm_runtime(binary: &[u8]) -> Result<Arc<Runtime>> {
    static RUNTIMES: LazyLock<moka::sync::Cache<md5::Digest, Arc<Runtime>>> = LazyLock::new(|| {
        moka::sync::Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build()
    });

    let md5 = md5::compute(binary);
    if let Some(runtime) = RUNTIMES.get(&md5) {
        return Ok(runtime.clone());
    }

    let runtime = Arc::new(arrow_udf_wasm::Runtime::new(binary)?);
    RUNTIMES.insert(md5, runtime.clone());
    Ok(runtime)
}

/// Convert a v0.1 function identifier to v0.2 format.
///
/// In arrow-udf v0.1 format, struct type is inline in the identifier. e.g.
///
/// ```text
/// keyvalue(varchar,varchar)->struct<key:varchar,value:varchar>
/// ```
///
/// However, since arrow-udf v0.2, struct type is no longer inline.
/// The above identifier is divided into a function and a type.
///
/// ```text
/// keyvalue(varchar,varchar)->struct KeyValue
/// KeyValue=key:varchar,value:varchar
/// ```
///
/// For compatibility, we should call `find_wasm_identifier_v2` to
/// convert v0.1 identifiers to v0.2 format before looking up the function.
fn find_wasm_identifier_v2(
    runtime: &arrow_udf_wasm::Runtime,
    inlined_signature: &str,
) -> Result<String> {
    // Inline types in function signature.
    //
    // # Example
    //
    // ```text
    // types = { "KeyValue": "key:varchar,value:varchar" }
    // input = "keyvalue(varchar, varchar) -> struct KeyValue"
    // output = "keyvalue(varchar, varchar) -> struct<key:varchar,value:varchar>"
    // ```
    let inline_types = |s: &str| -> String {
        let mut inlined = s.to_owned();
        // iteratively replace `struct Xxx` with `struct<...>` until no replacement is made.
        loop {
            let replaced = inlined.clone();
            for (k, v) in runtime.types() {
                inlined = inlined.replace(&format!("struct {k}"), &format!("struct<{v}>"));
            }
            if replaced == inlined {
                return inlined;
            }
        }
    };
    // Function signature in arrow-udf is case sensitive.
    // However, SQL identifiers are usually case insensitive and stored in lowercase.
    // So we should convert the signature to lowercase before comparison.
    let identifier = runtime
        .functions()
        .find(|f| inline_types(f).to_lowercase() == inlined_signature)
        .ok_or_else(|| {
            anyhow!(
                "function not found in wasm binary: \"{}\"\nHINT: available functions:\n  {}\navailable types:\n  {}",
                inlined_signature,
                runtime.functions().join("\n  "),
                runtime.types().map(|(k, v)| format!("{k}: {v}")).join("\n  "),
            )
        })?;
    Ok(identifier.into())
}

/// Generate a function identifier in v0.1 format from the function signature.
fn wasm_identifier_v1(
    name: &str,
    args: &[DataType],
    ret: &DataType,
    table_function: bool,
) -> String {
    format!(
        "{}({}){}{}",
        name,
        args.iter().map(datatype_name).join(","),
        if table_function { "->>" } else { "->" },
        datatype_name(ret)
    )
}

/// Convert a data type to string used in identifier.
fn datatype_name(ty: &DataType) -> String {
    match ty {
        DataType::Boolean => "boolean".to_owned(),
        DataType::Int16 => "int16".to_owned(),
        DataType::Int32 => "int32".to_owned(),
        DataType::Int64 => "int64".to_owned(),
        DataType::Float32 => "float32".to_owned(),
        DataType::Float64 => "float64".to_owned(),
        DataType::Date => "date32".to_owned(),
        DataType::Time => "time64".to_owned(),
        DataType::Timestamp => "timestamp".to_owned(),
        DataType::TimestampNanosecond => "timestamp_ns".to_owned(),
        DataType::Timestamptz => "timestamptz".to_owned(),
        DataType::Interval => "interval".to_owned(),
        DataType::Decimal => "decimal".to_owned(),
        DataType::Jsonb => "json".to_owned(),
        DataType::Serial => "serial".to_owned(),
        DataType::Int256 => "int256".to_owned(),
        DataType::Bytea => "binary".to_owned(),
        DataType::Varchar => "string".to_owned(),
        DataType::List(inner) => format!("{}[]", datatype_name(inner)),
        DataType::Struct(s) => format!(
            "struct<{}>",
            s.iter()
                .map(|(name, ty)| format!("{}:{}", name, datatype_name(ty)))
                .join(",")
        ),
        DataType::Map(_m) => todo!("map in wasm udf"),
    }
}
