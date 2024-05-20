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

use arrow_udf_python::{CallMode, Runtime};
use futures_util::StreamExt;
use risingwave_common::array::arrow::{ToArrow, UdfArrowConvert};

use super::*;

#[linkme::distributed_slice(UDF_IMPLS)]
static PYTHON: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, _runtime, link| language == "python" && link.is_none(),
    create_fn: |opts| {
        Ok(CreateFunctionOutput {
            identifier: opts.name.to_string(),
            body: Some(opts.as_.context("AS must be specified")?.to_string()),
            compressed_binary: None,
        })
    },
    build_fn: |opts| {
        let mut runtime = Runtime::builder().sandboxed(true).build()?;
        runtime.add_function(
            opts.identifier,
            UdfArrowConvert::default().to_arrow_field("", opts.return_type)?,
            CallMode::CalledOnNullInput,
            opts.body.context("body is required")?,
        )?;
        Ok(Box::new(PythonFunction {
            runtime,
            identifier: opts.identifier.to_string(),
        }))
    },
};

#[derive(Debug)]
struct PythonFunction {
    runtime: Runtime,
    identifier: String,
}

#[async_trait::async_trait]
impl UdfImpl for PythonFunction {
    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        self.runtime.call(&self.identifier, input)
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        self.runtime
            .call_table_function(&self.identifier, input, 1024)
            .map(|s| futures_util::stream::iter(s).boxed())
    }
}
