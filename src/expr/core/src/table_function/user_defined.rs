// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use anyhow::Context;
use arrow_array::RecordBatch;
use arrow_schema::{Fields, Schema, SchemaRef};
use arrow_udf_js::{CallMode as JsCallMode, Runtime as JsRuntime};
#[cfg(feature = "embedded-deno-udf")]
use arrow_udf_js_deno::{CallMode as DenoCallMode, Runtime as DenoRuntime};
#[cfg(feature = "embedded-python-udf")]
use arrow_udf_python::{CallMode as PythonCallMode, Runtime as PythonRuntime};
use cfg_or_panic::cfg_or_panic;
use risingwave_common::array::arrow::{FromArrow, ToArrow, UdfArrowConvert};
use risingwave_common::array::{DataChunk, I32Array};
use risingwave_common::bail;

use super::*;
use crate::expr::expr_udf::UdfImpl;

#[derive(Debug)]
pub struct UserDefinedTableFunction {
    children: Vec<BoxedExpression>,
    arg_schema: SchemaRef,
    return_type: DataType,
    client: UdfImpl,
    identifier: String,
    arrow_convert: UdfArrowConvert,
    #[allow(dead_code)]
    chunk_size: usize,
}

#[async_trait::async_trait]
impl TableFunction for UserDefinedTableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

#[cfg(not(madsim))]
impl UdfImpl {
    #[try_stream(ok = RecordBatch, error = ExprError)]
    async fn call_table_function<'a>(&'a self, identifier: &'a str, input: RecordBatch) {
        match self {
            UdfImpl::External(client) => {
                #[for_await]
                for res in client.call_table_function(identifier, &input).await? {
                    yield res?;
                }
            }
            UdfImpl::JavaScript(runtime) => {
                for res in runtime.call_table_function(identifier, &input, 1024)? {
                    yield res?;
                }
            }
            #[cfg(feature = "embedded-python-udf")]
            UdfImpl::Python(runtime) => {
                for res in runtime.call_table_function(identifier, &input, 1024)? {
                    yield res?;
                }
            }
            #[cfg(feature = "embedded-deno-udf")]
            UdfImpl::Deno(runtime) => {
                let mut iter = runtime.call_table_function(identifier, input, 1024).await?;
                while let Some(res) = iter.next().await {
                    yield res?;
                }
            }
            UdfImpl::Wasm(runtime) => {
                for res in runtime.call_table_function(identifier, &input)? {
                    yield res?;
                }
            }
        }
    }
}

#[cfg(not(madsim))]
impl UserDefinedTableFunction {
    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        // evaluate children expressions
        let mut columns = Vec::with_capacity(self.children.len());
        for c in &self.children {
            let val = c.eval(input).await?;
            columns.push(val);
        }
        let direct_input = DataChunk::new(columns, input.visibility().clone());

        // compact the input chunk and record the row mapping
        let visible_rows = direct_input.visibility().iter_ones().collect::<Vec<_>>();
        // this will drop invisible rows
        let arrow_input = self
            .arrow_convert
            .to_record_batch(self.arg_schema.clone(), &direct_input)?;

        // call UDTF
        #[for_await]
        for res in self
            .client
            .call_table_function(&self.identifier, arrow_input)
        {
            let output = self.arrow_convert.from_record_batch(&res?)?;
            self.check_output(&output)?;

            // we send the compacted input to UDF, so we need to map the row indices back to the
            // original input
            let origin_indices = output
                .column_at(0)
                .as_int32()
                .raw_iter()
                // we have checked all indices are non-negative
                .map(|idx| visible_rows[idx as usize] as i32)
                .collect::<I32Array>();

            let output = DataChunk::new(
                vec![origin_indices.into_ref(), output.column_at(1).clone()],
                output.visibility().clone(),
            );
            yield output;
        }
    }

    /// Check if the output chunk is valid.
    fn check_output(&self, output: &DataChunk) -> Result<()> {
        if output.columns().len() != 2 {
            bail!(
                "UDF returned {} columns, but expected 2",
                output.columns().len()
            );
        }
        if output.column_at(0).data_type() != DataType::Int32 {
            bail!(
                "UDF returned {:?} at column 0, but expected {:?}",
                output.column_at(0).data_type(),
                DataType::Int32,
            );
        }
        if output.column_at(0).as_int32().raw_iter().any(|i| i < 0) {
            bail!("UDF returned negative row index");
        }
        if !output
            .column_at(1)
            .data_type()
            .equals_datatype(&self.return_type)
        {
            bail!(
                "UDF returned {:?} at column 1, but expected {:?}",
                output.column_at(1).data_type(),
                &self.return_type,
            );
        }
        Ok(())
    }
}

#[cfg_or_panic(not(madsim))]
pub fn new_user_defined(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    let Some(udtf) = &prost.udtf else {
        bail!("expect UDTF");
    };

    let identifier = udtf.get_identifier()?;
    let return_type = DataType::from(prost.get_return_type()?);

    #[cfg(not(feature = "embedded-deno-udf"))]
    let runtime = "quickjs";

    #[cfg(feature = "embedded-deno-udf")]
    let runtime = match udtf.runtime.as_deref() {
        Some("deno") => "deno",
        _ => "quickjs",
    };

    let mut arrow_convert = UdfArrowConvert::default();

    let client = match udtf.language.as_str() {
        "wasm" | "rust" => {
            let compressed_wasm_binary = udtf.get_compressed_binary()?;
            let wasm_binary = zstd::stream::decode_all(compressed_wasm_binary.as_slice())
                .context("failed to decompress wasm binary")?;
            let runtime = crate::expr::expr_udf::get_or_create_wasm_runtime(&wasm_binary)?;
            // backward compatibility
            if runtime.abi_version().0 <= 2 {
                arrow_convert = UdfArrowConvert { legacy: true };
            }
            UdfImpl::Wasm(runtime)
        }
        "javascript" if runtime != "deno" => {
            let mut rt = JsRuntime::new()?;
            let body = format!(
                "export function* {}({}) {{ {} }}",
                identifier,
                udtf.arg_names.join(","),
                udtf.get_body()?
            );
            rt.add_function(
                identifier,
                arrow_convert.to_arrow_field("", &return_type)?,
                JsCallMode::CalledOnNullInput,
                &body,
            )?;
            UdfImpl::JavaScript(rt)
        }
        #[cfg(feature = "embedded-deno-udf")]
        "javascript" if runtime == "deno" => {
            let rt = DenoRuntime::new();
            let body = match udtf.get_body() {
                Ok(body) => body.clone(),
                Err(_) => match udtf.get_compressed_binary() {
                    Ok(compressed_binary) => {
                        let binary = zstd::stream::decode_all(compressed_binary.as_slice())
                            .context("failed to decompress binary")?;
                        String::from_utf8(binary).context("failed to decode binary")?
                    }
                    Err(_) => {
                        bail!("UDF body or compressed binary is required for deno UDF");
                    }
                },
            };

            let body = format!(
                "export {} {}({}) {{ {} }}",
                match udtf.function_type.as_deref() {
                    Some("async") => "async function",
                    Some("async_generator") => "async function*",
                    Some("sync") => "function",
                    _ => "function*",
                },
                identifier,
                udtf.arg_names.join(","),
                body
            );

            futures::executor::block_on(rt.add_function(
                identifier,
                arrow_convert.to_arrow_field("", &return_type)?,
                DenoCallMode::CalledOnNullInput,
                &body,
            ))?;
            UdfImpl::Deno(rt)
        }
        #[cfg(feature = "embedded-python-udf")]
        "python" if udtf.body.is_some() => {
            let mut rt = PythonRuntime::builder().sandboxed(true).build()?;
            let body = udtf.get_body()?;
            rt.add_function(
                identifier,
                arrow_convert.to_arrow_field("", &return_type)?,
                PythonCallMode::CalledOnNullInput,
                body,
            )?;
            UdfImpl::Python(rt)
        }
        // connect to UDF service
        _ => {
            let link = udtf.get_link()?;
            let client = crate::expr::expr_udf::get_or_create_flight_client(link)?;
            // backward compatibility
            // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
            if client.protocol_version() == 1 {
                arrow_convert = UdfArrowConvert { legacy: true };
            }
            UdfImpl::External(client)
        }
    };

    let arg_schema = Arc::new(Schema::new(
        udtf.arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", &DataType::from(t)))
            .try_collect::<Fields>()?,
    ));

    Ok(UserDefinedTableFunction {
        children: prost.args.iter().map(expr_build_from_prost).try_collect()?,
        return_type,
        arg_schema,
        client,
        identifier: identifier.clone(),
        arrow_convert,
        chunk_size,
    }
    .boxed())
}
