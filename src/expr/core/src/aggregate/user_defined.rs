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
use risingwave_common::array::arrow::arrow_array_udf::ArrayRef;
use risingwave_common::array::arrow::arrow_schema_udf::{Field, Fields, Schema, SchemaRef};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfFromArrow, UdfToArrow};
use risingwave_common::array::Op;
use risingwave_common::bitmap::Bitmap;
use risingwave_pb::expr::{PbUdfExprVersion, PbUserDefinedFunctionMetadata};

use super::*;
use crate::sig::{BuildOptions, UdfImpl, UdfKind};

#[derive(Debug)]
pub struct UserDefinedAggregateFunction {
    arg_schema: SchemaRef,
    return_type: DataType,
    return_field: Field,
    state_field: Field,
    runtime: Box<dyn UdfImpl>,
}

#[async_trait::async_trait]
impl AggregateFunction for UserDefinedAggregateFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    /// Creates an initial state of the aggregate function.
    fn create_state(&self) -> Result<AggregateState> {
        let state = self.runtime.call_agg_create_state()?;
        Ok(AggregateState::Any(Box::new(State(state))))
    }

    /// Update the state with multiple rows.
    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> Result<()> {
        let state = &mut state.downcast_mut::<State>().0;
        let ops = input
            .visibility()
            .iter_ones()
            .map(|i| Some(matches!(input.ops()[i], Op::Delete | Op::UpdateDelete)))
            .collect();
        // this will drop invisible rows
        let arrow_input = UdfArrowConvert::default()
            .to_record_batch(self.arg_schema.clone(), input.data_chunk())?;
        let new_state = self
            .runtime
            .call_agg_accumulate_or_retract(state, &ops, &arrow_input)?;
        *state = new_state;
        Ok(())
    }

    /// Update the state with a range of rows.
    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> Result<()> {
        // XXX(runji): this may be inefficient
        let vis = input.visibility() & Bitmap::from_range(input.capacity(), range);
        let input = input.clone_with_vis(vis);
        self.update(state, &input).await
    }

    /// Get aggregate result from the state.
    async fn get_result(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        let arrow_output = self.runtime.call_agg_finish(state)?;
        let output = UdfArrowConvert::default().from_array(&self.return_field, &arrow_output)?;
        Ok(output.datum_at(0))
    }

    /// Encode the state into a datum that can be stored in state table.
    fn encode_state(&self, state: &AggregateState) -> Result<Datum> {
        let state = &state.downcast_ref::<State>().0;
        let state = UdfArrowConvert::default().from_array(&self.state_field, state)?;
        Ok(state.datum_at(0))
    }

    /// Decode the state from a datum in state table.
    fn decode_state(&self, datum: Datum) -> Result<AggregateState> {
        let array = {
            let mut builder = DataType::Bytea.create_array_builder(1);
            builder.append(datum);
            builder.finish()
        };
        let state = UdfArrowConvert::default().to_array(self.state_field.data_type(), &array)?;
        Ok(AggregateState::Any(Box::new(State(state))))
    }
}

// In arrow-udf, aggregate state is represented as an `ArrayRef`.
// To avoid unnecessary conversion between `ArrayRef` and `Datum`,
// we store `ArrayRef` directly in our `AggregateState`.
#[derive(Debug)]
struct State(ArrayRef);

impl EstimateSize for State {
    fn estimated_heap_size(&self) -> usize {
        self.0.get_array_memory_size()
    }
}

impl AggStateDyn for State {}

/// Create a new user-defined aggregate function.
pub fn new_user_defined(
    return_type: &DataType,
    udf: &PbUserDefinedFunctionMetadata,
) -> Result<BoxedAggregateFunction> {
    let arg_types = udf.arg_types.iter().map(|t| t.into()).collect::<Vec<_>>();
    let language = udf.language.as_str();
    let runtime = udf.runtime.as_deref();
    let link = udf.link.as_deref();

    // `identifier` field is re-interpreted as `name_in_runtime`.
    if udf.version() < PbUdfExprVersion::NameInRuntime {
        assert_ne!(
            language, "rust",
            "Rust UDAF was not supported yet before this version"
        );
        assert_ne!(
            language, "wasm",
            "WASM UDAF was not supported yet before this version"
        );
    }
    let name_in_runtime = udf
        .identifier
        .as_ref()
        .expect("SQL UDF won't get here, other UDFs must have `name_in_runtime`");

    let build_fn = crate::sig::find_udf_impl(language, runtime, link)?.build_fn;
    let runtime = build_fn(BuildOptions {
        kind: UdfKind::Aggregate,
        body: udf.body.as_deref(),
        compressed_binary: udf.compressed_binary.as_deref(),
        link: udf.link.as_deref(),
        name_in_runtime,
        arg_names: &udf.arg_names,
        arg_types: &arg_types,
        return_type,
        always_retry_on_network_error: false,
    })
    .context("failed to build UDF runtime")?;

    // legacy UDF runtimes do not support aggregate functions,
    // so we can assume that the runtime is not legacy
    let arrow_convert = UdfArrowConvert::default();
    let arg_schema = Arc::new(Schema::new(
        arg_types
            .iter()
            .map(|t| arrow_convert.to_arrow_field("", t))
            .try_collect::<_, Fields, _>()?,
    ));

    Ok(Box::new(UserDefinedAggregateFunction {
        return_field: arrow_convert.to_arrow_field("", return_type)?,
        state_field: Field::new(
            "state",
            risingwave_common::array::arrow::arrow_schema_udf::DataType::Binary,
            true,
        ),
        return_type: return_type.clone(),
        arg_schema,
        runtime,
    }))
}
