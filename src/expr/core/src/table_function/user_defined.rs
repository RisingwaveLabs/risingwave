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

use std::sync::Arc;

use anyhow::Context;
use risingwave_common::array::arrow::arrow_schema_udf::{Fields, Schema, SchemaRef};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfFromArrow, UdfToArrow};
use risingwave_common::array::I32Array;
use risingwave_common::bail;
use risingwave_pb::expr::PbUdfProtoVersion;

use super::*;
use crate::sig::{BuildOptions, UdfImpl, UdfKind};

#[derive(Debug)]
pub struct UserDefinedTableFunction {
    children: Vec<BoxedExpression>,
    arg_schema: SchemaRef,
    return_type: DataType,
    runtime: Box<dyn UdfImpl>,
    arrow_convert: UdfArrowConvert,
    #[allow(dead_code)]
    chunk_size: usize,
}

#[async_trait::async_trait]
impl TableFunction for UserDefinedTableFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

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
        for res in self.runtime.call_table_function(&arrow_input).await? {
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

pub fn new_user_defined(prost: &PbTableFunction, chunk_size: usize) -> Result<BoxedTableFunction> {
    let udf = prost.get_udf()?;

    let arg_types = udf.arg_types.iter().map(|t| t.into()).collect::<Vec<_>>();
    let return_type = DataType::from(prost.get_return_type()?);

    let language = udf.language.as_str();
    let runtime = udf.runtime.as_deref();
    let link = udf.link.as_deref();

    let name_in_runtime = if udf.version() < PbUdfProtoVersion::NameInRuntime {
        if language == "rust" || language == "wasm" {
            // The `identifier` value of Rust and WASM UDF before `NameInRuntime`
            // is not used any more. And unfortunately, we don't have the original name
            // in `PbUserDefinedFunctionMetadata`, so we need to extract the name from
            // the old `identifier` value (e.g. `foo()->int32`).
            let old_identifier = udf
                .identifier
                .as_ref()
                .expect("Rust/WASM UDF must have identifier");
            Some(
                old_identifier
                    .split_once("(")
                    .expect("the old identifier must contain `(`")
                    .0,
            )
        } else {
            // `identifier`s of other UDFs already mean `name_in_runtime` before `NameInRuntime`.
            udf.identifier.as_deref()
        }
    } else {
        // after `PbUdfProtoVersion::NameInRuntime`, `identifier` means `name_in_runtime`
        udf.identifier.as_deref()
    }
    .expect("SQL UDF won't get here, other UDFs must have `name_in_runtime`");

    let build_fn = crate::sig::find_udf_impl(language, runtime, link)?.build_fn;
    let runtime = build_fn(BuildOptions {
        kind: UdfKind::Table,
        body: udf.body.as_deref(),
        compressed_binary: udf.compressed_binary.as_deref(),
        link: udf.link.as_deref(),
        name_in_runtime,
        arg_names: &udf.arg_names,
        arg_types: &arg_types,
        return_type: &return_type,
        always_retry_on_network_error: false,
    })
    .context("failed to build UDF runtime")?;

    let arrow_convert = UdfArrowConvert {
        legacy: runtime.is_legacy(),
    };
    let arg_schema = Arc::new(Schema::new(
        arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", t))
            .try_collect::<Fields>()?,
    ));

    Ok(UserDefinedTableFunction {
        children: prost.args.iter().map(expr_build_from_prost).try_collect()?,
        return_type,
        arg_schema,
        runtime,
        arrow_convert,
        chunk_size,
    }
    .boxed())
}
