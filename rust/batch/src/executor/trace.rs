// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use tracing::event;
use tracing_futures::Instrument;

use crate::executor::{BoxedExecutor, Executor};

pub(super) struct TraceExecutor {
    child: BoxedExecutor,
    /// Description of input executor
    input_desc: String,
}

impl TraceExecutor {
    pub fn new(child: BoxedExecutor, input_desc: String) -> Self {
        Self { child, input_desc }
    }
}

#[async_trait::async_trait]
impl Executor for TraceExecutor {
    async fn open(&mut self) -> Result<()> {
        let input_desc = self.input_desc.as_str();
        let span_name = format!("{input_desc}_open");
        self.child
            .open()
            .instrument(tracing::trace_span!(
                "open",
                otel.name = span_name.as_str(),
                open = input_desc,
            ))
            .await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let input_desc = self.input_desc.as_str();
        let span_name = format!("{input_desc}_next");
        let input_chunk = self
            .child
            .next()
            .instrument(tracing::trace_span!(
                "next",
                otel.name = span_name.as_str(),
                next = input_desc,
            ))
            .await;
        match input_chunk {
            Ok(chunk) => {
                match &chunk {
                    Some(chunk) => {
                        event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \n{:#?}", chunk);
                    }
                    None => {
                        event!(tracing::Level::TRACE, prev = %input_desc, msg = "chunk", "input = \nNone");
                    }
                }
                Ok(chunk)
            }
            Err(e) => Err(e),
        }
    }

    async fn close(&mut self) -> Result<()> {
        let input_desc = self.input_desc.as_str();
        let span_name = format!("{input_desc}_close");
        self.child
            .close()
            .instrument(tracing::trace_span!(
                "close",
                otel.name = span_name.as_str(),
                close = input_desc,
            ))
            .await?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        "TraceExecutor"
    }
}
