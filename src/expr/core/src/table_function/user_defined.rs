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
use arrow_schema::{Field, Fields, Schema, SchemaRef};
use arrow_udf_js::{CallMode, Runtime as JsRuntime};
use arrow_udf_wasm::Runtime as WasmRuntime;
use cfg_or_panic::cfg_or_panic;
use futures_util::stream;
use risingwave_common::array::{ArrayError, DataChunk, I32Array};
use risingwave_common::bail;
use risingwave_udf::ArrowFlightUdfClient;
use thiserror_ext::AsReport;

use super::*;

#[derive(Debug)]
pub struct UserDefinedTableFunction {
    children: Vec<BoxedExpression>,
    #[allow(dead_code)]
    arg_schema: SchemaRef,
    return_type: DataType,
    client: UdfImpl,
    identifier: String,
    #[allow(dead_code)]
    chunk_size: usize,
}

#[derive(Debug)]
enum UdfImpl {
    External(Arc<ArrowFlightUdfClient>),
    Wasm(Arc<WasmRuntime>),
    JavaScript(JsRuntime),
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
                for res in client
                    .call_stream(identifier, stream::once(async { input }))
                    .await?
                {
                    yield res?;
                }
            }
            UdfImpl::JavaScript(runtime) => {
                for res in runtime.call_table_function(identifier, &input, 1024)? {
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
        let visible_rows = direct_input.visibility().iter_ones().collect_vec();
        let compacted_input = direct_input.compact_cow();
        let arrow_input = RecordBatch::try_from(compacted_input.as_ref())?;

        // call UDTF
        #[for_await]
        for res in self
            .client
            .call_table_function(&self.identifier, arrow_input)
        {
            let output = DataChunk::try_from(&res?)?;
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

    let arg_schema = Arc::new(Schema::new(
        udtf.arg_types
            .iter()
            .map::<Result<_>, _>(|t| {
                Ok(Field::new(
                    "",
                    DataType::from(t).try_into().map_err(|e: ArrayError| {
                        risingwave_udf::Error::unsupported(e.to_report_string())
                    })?,
                    true,
                ))
            })
            .try_collect::<_, Fields, _>()?,
    ));

    let identifier = udtf.get_identifier()?;
    let return_type = DataType::from(prost.get_return_type()?);

    let client = match udtf.language.as_str() {
        "wasm" | "rust" => {
            let compressed_wasm_binary = udtf.get_compressed_binary()?;
            let wasm_binary = zstd::stream::decode_all(compressed_wasm_binary.as_slice())
                .context("failed to decompress wasm binary")?;
            let runtime = crate::expr::expr_udf::get_or_create_wasm_runtime(&wasm_binary)?;
            UdfImpl::Wasm(runtime)
        }
        "javascript" => {
            let mut rt = JsRuntime::new()?;
            let body = format!(
                "export function* {}({}) {{ {} }}",
                identifier,
                udtf.arg_names.join(","),
                udtf.get_body()?
            );
            rt.add_function(
                identifier,
                arrow_schema::DataType::try_from(&return_type)?,
                CallMode::CalledOnNullInput,
                &body,
            )?;
            UdfImpl::JavaScript(rt)
        }
        // connect to UDF service
        _ => {
            let link = udtf.get_link()?;
            UdfImpl::External(crate::expr::expr_udf::get_or_create_flight_client(link)?)
        }
    };

    Ok(UserDefinedTableFunction {
        children: prost.args.iter().map(expr_build_from_prost).try_collect()?,
        return_type,
        arg_schema,
        client,
        identifier: identifier.clone(),
        chunk_size,
    }
    .boxed())
}
