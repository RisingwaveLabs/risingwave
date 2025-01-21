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
use risingwave_expr::sig::{BuildOptions, UdfKind};

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

fn create_wasm(opts: CreateOptions<'_>) -> Result<CreateFunctionOutput> {
    let wasm_binary: Cow<'_, [u8]> = if let Some(link) = opts.using_link {
        read_file_from_link(link)?.into()
    } else if let Some(bytes) = opts.using_base64_decoded {
        bytes.into()
    } else {
        bail!("USING must be specified")
    };

    let mut runtime = create_wasm_runtime(&wasm_binary)?;
    if runtime.abi_version().0 <= 2 {
        bail!("legacy arrow-udf is no longer supported. please update arrow-udf to 0.3+");
    }

    let arg_type_names = opts.arg_types.iter().map(datatype_name).collect::<Vec<_>>();
    let return_type_name = datatype_name(opts.return_type);
    match opts.kind {
        UdfKind::Scalar | UdfKind::Table => {
            // test if the function exists in the wasm binary
            runtime.add_function(
                opts.name,
                &arg_type_names,
                &return_type_name,
                opts.kind.is_table(),
            )?;
        }
        UdfKind::Aggregate => {
            todo!("wasm udaf");
        }
    }

    let compressed_binary = Some(zstd::stream::encode_all(&*wasm_binary, 0)?);
    Ok(CreateFunctionOutput {
        name_in_runtime: opts.name.to_owned(),
        body: None,
        compressed_binary,
    })
}

fn create_rust(opts: CreateOptions<'_>) -> Result<CreateFunctionOutput> {
    if opts.using_link.is_some() {
        bail!("USING is not supported for rust function");
    }

    let prelude = "use arrow_udf::{{function, types::*}};";
    let export_macro = if opts
        .arg_types
        .iter()
        .chain(std::iter::once(opts.return_type).all(|t| !t.is_struct()))
    {
        let identifier_v1 = wasm_identifier_v1(
            opts.name,
            opts.arg_types,
            opts.return_type,
            opts.kind.is_table(),
        );
        format!("#[function(\"{}\")]", identifier_v1)
    } else {
        String::new()
    };
    let script = [
        prelude,
        &export_macro,
        opts.as_.context("AS must be specified")?,
    ]
    .join("\n");
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

    let mut runtime = create_wasm_runtime(&wasm_binary)?;

    let arg_type_names = opts.arg_types.iter().map(datatype_name).collect::<Vec<_>>();
    let return_type_name = datatype_name(opts.return_type);
    match opts.kind {
        UdfKind::Scalar | UdfKind::Table => {
            // test if the function exists in the wasm binary
            runtime.add_function(
                opts.name,
                &arg_type_names,
                &return_type_name,
                opts.kind.is_table(),
            )?;
        }
        UdfKind::Aggregate => {
            todo!("rust udaf");
        }
    }

    let compressed_binary = Some(zstd::stream::encode_all(wasm_binary.as_slice(), 0)?);
    Ok(CreateFunctionOutput {
        name_in_runtime: opts.name.to_owned(),
        body,
        compressed_binary,
    })
}

fn build(opts: BuildOptions<'_>) -> Result<Box<dyn UdfImpl>> {
    let compressed_binary = opts
        .compressed_binary
        .context("compressed binary is required")?;
    let wasm_binary =
        zstd::stream::decode_all(compressed_binary).context("failed to decompress wasm binary")?;
    let mut runtime = create_wasm_runtime(&wasm_binary)?;

    let arg_type_names = opts.arg_types.iter().map(datatype_name).collect::<Vec<_>>();
    let return_type_name = datatype_name(opts.return_type);
    match opts.kind {
        UdfKind::Scalar | UdfKind::Table => {
            runtime.add_function(
                opts.name_in_runtime,
                &arg_type_names,
                &return_type_name,
                opts.kind.is_table(),
            )?;
        }
        UdfKind::Aggregate => {
            todo!("wasm/rust udaf");
        }
    }

    Ok(Box::new(WasmFunction {
        runtime,
        name: opts.name_in_runtime.to_owned(),
    }))
}

#[derive(Debug)]
struct WasmFunction {
    runtime: Runtime,
    name: String,
}

#[async_trait::async_trait]
impl UdfImpl for WasmFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.runtime.call(&self.name, input)
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        self.runtime
            .call_table_function(&self.name, input)
            .map(|s| futures_util::stream::iter(s).boxed())
    }

    fn is_legacy(&self) -> bool {
        // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
        self.runtime.abi_version().0 <= 2
    }
}

/// Create a WASM runtime.
///
/// Runtimes returned by this function are cached inside for at least 60 seconds.
/// Later calls with the same binary will simply clone the runtime so that inner immutable
/// fields are shared.
fn create_wasm_runtime(binary: &[u8]) -> Result<Runtime> {
    static RUNTIMES: LazyLock<moka::sync::Cache<md5::Digest, Runtime>> = LazyLock::new(|| {
        moka::sync::Cache::builder()
            .time_to_idle(Duration::from_secs(60))
            .build()
    });

    let md5 = md5::compute(binary);
    if let Some(runtime) = RUNTIMES.get(&md5) {
        return Ok(runtime.clone());
    }

    let runtime = Runtime::new(binary)?;
    RUNTIMES.insert(md5, runtime.clone());
    Ok(runtime)
}

/// Generate a function identifier in v0.1 format from the function signature.
/// NOTE(rc): Although we have moved the function signature construction to `arrow-udf`, we
/// still need this to generate the `#[function]` macro call for simple functions.
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
