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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use await_tree::InstrumentAwait;
use prometheus::{exponential_buckets, Registry};
use risingwave_common::array::arrow::arrow_schema_udf::{Fields, Schema, SchemaRef};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfFromArrow, UdfToArrow};
use risingwave_common::array::{Array, ArrayRef, DataChunk};
use risingwave_common::metrics::*;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr_context::FRAGMENT_ID;
use risingwave_pb::expr::ExprNode;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::sig::{UdfImpl, UdfKind, UdfOptions};
use crate::{bail, ExprError, Result};

#[derive(Debug)]
pub struct UserDefinedFunction {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    arg_schema: SchemaRef,
    runtime: Box<dyn UdfImpl>,
    arrow_convert: UdfArrowConvert,
    span: await_tree::Span,
    metrics: Metrics,
}

#[async_trait::async_trait]
impl Expression for UserDefinedFunction {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        if input.cardinality() == 0 {
            // early return for empty input
            let mut builder = self.return_type.create_array_builder(input.capacity());
            builder.append_n_null(input.capacity());
            return Ok(builder.finish().into_ref());
        }
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval(input).await?;
            columns.push(array);
        }
        let chunk = DataChunk::new(columns, input.visibility().clone());
        self.eval_inner(&chunk).await
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            columns.push(datum);
        }
        let arg_row = OwnedRow::new(columns);
        let chunk = DataChunk::from_rows(std::slice::from_ref(&arg_row), &self.arg_types);
        let output_array = self.eval_inner(&chunk).await?;
        Ok(output_array.to_datum())
    }
}

impl UserDefinedFunction {
    async fn eval_inner(&self, input: &DataChunk) -> Result<ArrayRef> {
        // this will drop invisible rows
        let arrow_input = self
            .arrow_convert
            .to_record_batch(self.arg_schema.clone(), input)?;

        // metrics
        self.metrics
            .input_chunk_rows
            .observe(arrow_input.num_rows() as f64);
        self.metrics
            .input_rows
            .inc_by(arrow_input.num_rows() as u64);
        self.metrics
            .input_bytes
            .inc_by(arrow_input.get_array_memory_size() as u64);
        let timer = self.metrics.latency.start_timer();

        let arrow_output_result = self
            .runtime
            .call(&arrow_input)
            .instrument_await(self.span.clone())
            .await;

        timer.stop_and_record();
        if arrow_output_result.is_ok() {
            &self.metrics.success_count
        } else {
            &self.metrics.failure_count
        }
        .inc();
        // update memory usage
        self.metrics
            .memory_usage_bytes
            .set(self.runtime.memory_usage() as i64);

        let arrow_output = arrow_output_result?;

        if arrow_output.num_rows() != input.cardinality() {
            bail!(
                "UDF returned {} rows, but expected {}",
                arrow_output.num_rows(),
                input.cardinality(),
            );
        }

        let output = self.arrow_convert.from_record_batch(&arrow_output)?;
        let output = output.uncompact(input.visibility().clone());

        let Some(array) = output.columns().first() else {
            bail!("UDF returned no columns");
        };
        if !array.data_type().equals_datatype(&self.return_type) {
            bail!(
                "UDF returned {:?}, but expected {:?}",
                array.data_type(),
                self.return_type,
            );
        }

        // handle optional error column
        if let Some(errors) = output.columns().get(1) {
            if errors.data_type() != DataType::Varchar {
                bail!(
                    "UDF returned errors column with invalid type: {:?}",
                    errors.data_type()
                );
            }
            let errors = errors
                .as_utf8()
                .iter()
                .filter_map(|msg| msg.map(|s| ExprError::Custom(s.into())))
                .collect();
            return Err(crate::ExprError::Multiple(array.clone(), errors));
        }

        Ok(array.clone())
    }
}

impl Build for UserDefinedFunction {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();
        let identifier = udf.get_identifier()?;

        let language = udf.language.as_str();
        let runtime = udf.runtime.as_deref();
        let link = udf.link.as_deref();

        // lookup UDF builder
        let build_fn = crate::sig::find_udf_impl(language, runtime, link)?.build_fn;
        let runtime = build_fn(UdfOptions {
            kind: UdfKind::Scalar,
            body: udf.body.as_deref(),
            compressed_binary: udf.compressed_binary.as_deref(),
            link: udf.link.as_deref(),
            identifier,
            arg_names: &udf.arg_names,
            return_type: &return_type,
            always_retry_on_network_error: udf.always_retry_on_network_error,
            language,
            is_async: udf.is_async,
            is_batched: udf.is_batched,
        })
        .context("failed to build UDF runtime")?;

        let arrow_convert = UdfArrowConvert {
            legacy: runtime.is_legacy(),
        };

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map(|t| arrow_convert.to_arrow_field("", &DataType::from(t)))
                .try_collect::<Fields>()?,
        ));

        let metrics = GLOBAL_METRICS.with_label_values(
            link.unwrap_or(""),
            language,
            identifier,
            // batch query does not have a fragment_id
            &FRAGMENT_ID::try_with(ToOwned::to_owned)
                .unwrap_or(0)
                .to_string(),
        );

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            runtime,
            arrow_convert,
            span: format!("udf_call({})", identifier).into(),
            metrics,
        })
    }
}

/// Monitor metrics for UDF.
#[derive(Debug, Clone)]
struct MetricsVec {
    /// Number of successful UDF calls.
    success_count: LabelGuardedIntCounterVec<4>,
    /// Number of failed UDF calls.
    failure_count: LabelGuardedIntCounterVec<4>,
    /// Total number of retried UDF calls.
    retry_count: LabelGuardedIntCounterVec<4>,
    /// Input chunk rows of UDF calls.
    input_chunk_rows: LabelGuardedHistogramVec<4>,
    /// The latency of UDF calls in seconds.
    latency: LabelGuardedHistogramVec<4>,
    /// Total number of input rows of UDF calls.
    input_rows: LabelGuardedIntCounterVec<4>,
    /// Total number of input bytes of UDF calls.
    input_bytes: LabelGuardedIntCounterVec<4>,
    /// Total memory usage of UDF runtime in bytes.
    memory_usage_bytes: LabelGuardedIntGaugeVec<5>,
}

/// Monitor metrics for UDF.
#[derive(Debug, Clone)]
struct Metrics {
    /// Number of successful UDF calls.
    success_count: LabelGuardedIntCounter<4>,
    /// Number of failed UDF calls.
    failure_count: LabelGuardedIntCounter<4>,
    /// Total number of retried UDF calls.
    #[allow(dead_code)]
    retry_count: LabelGuardedIntCounter<4>,
    /// Input chunk rows of UDF calls.
    input_chunk_rows: LabelGuardedHistogram<4>,
    /// The latency of UDF calls in seconds.
    latency: LabelGuardedHistogram<4>,
    /// Total number of input rows of UDF calls.
    input_rows: LabelGuardedIntCounter<4>,
    /// Total number of input bytes of UDF calls.
    input_bytes: LabelGuardedIntCounter<4>,
    /// Total memory usage of UDF runtime in bytes.
    memory_usage_bytes: LabelGuardedIntGauge<5>,
}

/// Global UDF metrics.
static GLOBAL_METRICS: LazyLock<MetricsVec> =
    LazyLock::new(|| MetricsVec::new(&GLOBAL_METRICS_REGISTRY));

impl MetricsVec {
    fn new(registry: &Registry) -> Self {
        let labels = &["link", "language", "name", "fragment_id"];
        let labels5 = &["link", "language", "name", "fragment_id", "instance_id"];
        let success_count = register_guarded_int_counter_vec_with_registry!(
            "udf_success_count",
            "Total number of successful UDF calls",
            labels,
            registry
        )
        .unwrap();
        let failure_count = register_guarded_int_counter_vec_with_registry!(
            "udf_failure_count",
            "Total number of failed UDF calls",
            labels,
            registry
        )
        .unwrap();
        let retry_count = register_guarded_int_counter_vec_with_registry!(
            "udf_retry_count",
            "Total number of retried UDF calls",
            labels,
            registry
        )
        .unwrap();
        let input_chunk_rows = register_guarded_histogram_vec_with_registry!(
            "udf_input_chunk_rows",
            "Input chunk rows of UDF calls",
            labels,
            exponential_buckets(1.0, 2.0, 10).unwrap(), // 1 to 1024
            registry
        )
        .unwrap();
        let latency = register_guarded_histogram_vec_with_registry!(
            "udf_latency",
            "The latency(s) of UDF calls",
            labels,
            exponential_buckets(0.000001, 2.0, 30).unwrap(), // 1us to 1000s
            registry
        )
        .unwrap();
        let input_rows = register_guarded_int_counter_vec_with_registry!(
            "udf_input_rows",
            "Total number of input rows of UDF calls",
            labels,
            registry
        )
        .unwrap();
        let input_bytes = register_guarded_int_counter_vec_with_registry!(
            "udf_input_bytes",
            "Total number of input bytes of UDF calls",
            labels,
            registry
        )
        .unwrap();
        let memory_usage_bytes = register_guarded_int_gauge_vec_with_registry!(
            "udf_memory_usage",
            "Total memory usage of UDF runtime in bytes",
            labels5,
            registry
        )
        .unwrap();

        MetricsVec {
            success_count,
            failure_count,
            retry_count,
            input_chunk_rows,
            latency,
            input_rows,
            input_bytes,
            memory_usage_bytes,
        }
    }

    fn with_label_values(
        &self,
        link: &str,
        language: &str,
        identifier: &str,
        fragment_id: &str,
    ) -> Metrics {
        // generate an unique id for each instance
        static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);
        let instance_id = NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed).to_string();

        let labels = &[link, language, identifier, fragment_id];
        let labels5 = &[link, language, identifier, fragment_id, &instance_id];

        Metrics {
            success_count: self.success_count.with_guarded_label_values(labels),
            failure_count: self.failure_count.with_guarded_label_values(labels),
            retry_count: self.retry_count.with_guarded_label_values(labels),
            input_chunk_rows: self.input_chunk_rows.with_guarded_label_values(labels),
            latency: self.latency.with_guarded_label_values(labels),
            input_rows: self.input_rows.with_guarded_label_values(labels),
            input_bytes: self.input_bytes.with_guarded_label_values(labels),
            memory_usage_bytes: self.memory_usage_bytes.with_guarded_label_values(labels5),
        }
    }
}
