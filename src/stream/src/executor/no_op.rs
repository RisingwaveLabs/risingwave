// Copyright 2023 RisingWave Labs
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

use risingwave_common::catalog::Schema;

use super::{
    ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo, PkIndicesRef,
};

/// No-op executor directly forwards the input stream. Currently used to break the multiple edges in
/// the fragment graph.
pub struct NoOpExecutor {
    _ctx: ActorContextRef,
    info: ExecutorInfo,
    input: BoxedExecutor,
}

impl NoOpExecutor {
    pub fn new(ctx: ActorContextRef, info: ExecutorInfo, input: BoxedExecutor) -> Self {
        Self {
            _ctx: ctx,
            info,
            input,
        }
    }
}

impl Executor for NoOpExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.input.execute()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
