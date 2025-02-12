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

use arrow_udf_js::{AggregateOptions, FunctionOptions, Runtime};
use futures_util::{FutureExt, StreamExt};
use risingwave_common::array::arrow::arrow_schema_udf::{DataType, Field};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfToArrow};

use super::*;

#[linkme::distributed_slice(UDF_IMPLS)]
static QUICKJS: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, runtime, _link| {
        language == "javascript" && matches!(runtime, None | Some("quickjs"))
    },
    create_fn: |opts| {
        Ok(CreateFunctionOutput {
            identifier: opts.name.to_owned(),
            body: Some(opts.as_.context("AS must be specified")?.to_owned()),
            compressed_binary: None,
        })
    },
    build_fn: |opts| {
        // NOTE: Some function calls such as `add_function()` requires async.
        // However, since the `Runtime` here is not shared, the async block will never block.
        futures::executor::block_on(async {
            let mut runtime = Runtime::new()
                .await
                .context("failed to create QuickJS Runtime")?;
            if opts.is_async.unwrap_or(false) {
                runtime
                    .enable_fetch()
                    .await
                    .context("failed to enable fetch")?;
            }
            if opts.kind.is_aggregate() {
                let options = AggregateOptions {
                    is_async: opts.is_async.unwrap_or(false),
                    ..Default::default()
                };
                runtime
                    .add_aggregate(
                        opts.identifier,
                        Field::new("state", DataType::Binary, true).with_metadata(
                            [("ARROW:extension:name".into(), "arrowudf.json".into())].into(),
                        ),
                        UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
                        opts.body.context("body is required")?,
                        options,
                    )
                    .await
                    .context("failed to add_aggregate")?;
            } else {
                let options = FunctionOptions {
                    is_async: opts.is_async.unwrap_or(false),
                    is_batched: opts.is_batched.unwrap_or(false),
                    ..Default::default()
                };
                let res = runtime
                    .add_function(
                        opts.identifier,
                        UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
                        opts.body.context("body is required")?,
                        options.clone(),
                    )
                    .await;

                if res.is_err() {
                    // COMPATIBILITY: This is for keeping compatible with the legacy syntax that
                    // only function body is provided by users.
                    let body = format!(
                        "export function{} {}({}) {{ {} }}",
                        if opts.kind.is_table() { "*" } else { "" },
                        opts.identifier,
                        opts.arg_names.join(","),
                        opts.body.context("body is required")?,
                    );
                    runtime
                        .add_function(
                            opts.identifier,
                            UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
                            &body,
                            options,
                        )
                        .await
                        .context("failed to add_function")?;
                }
            }
            Ok(Box::new(QuickJsFunction {
                runtime,
                identifier: opts.identifier.to_owned(),
            }) as Box<dyn UdfImpl>)
        })
    },
};

#[derive(Debug)]
struct QuickJsFunction {
    runtime: Runtime,
    identifier: String,
}

#[async_trait::async_trait]
impl UdfImpl for QuickJsFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        // TODO(eric): if not batched, call JS function row by row. Otherwise, one row failure will fail the entire chunk.
        self.runtime.call(&self.identifier, input).await
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        let iter = self
            .runtime
            .call_table_function(&self.identifier, input, 1024)?;
        Ok(Box::pin(iter))
    }

    async fn call_agg_create_state(&self) -> Result<ArrayRef> {
        self.runtime.create_state(&self.identifier).await
    }

    async fn call_agg_accumulate_or_retract(
        &self,
        state: &ArrayRef,
        ops: &BooleanArray,
        input: &RecordBatch,
    ) -> Result<ArrayRef> {
        self.runtime
            .accumulate_or_retract(&self.identifier, state, ops, input)
            .await
    }

    async fn call_agg_finish(&self, state: &ArrayRef) -> Result<ArrayRef> {
        self.runtime.finish(&self.identifier, state).await
    }

    fn memory_usage(&self) -> usize {
        futures::executor::block_on(async {
            // As `runtime` is not shared, this await will never block.
            self.runtime.inner().memory_usage().await.malloc_size as usize
        })
    }
}
