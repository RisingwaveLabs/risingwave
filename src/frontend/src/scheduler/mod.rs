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

//! Fragment and schedule batch queries.

use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use risingwave_batch::task::BatchTaskContext;
use risingwave_common::array::DataChunk;

use crate::error::Result;
use crate::session::SessionImpl;

mod distributed;
pub use distributed::*;
pub mod plan_fragmenter;
pub use plan_fragmenter::BatchPlanFragmenter;
mod snapshot;
pub use snapshot::*;
mod local;
pub use local::*;
mod fast_insert;
pub use fast_insert::*;

use crate::scheduler::task_context::FrontendBatchTaskContext;

mod error;
pub mod streaming_manager;
mod task_context;

pub use self::error::SchedulerError;
pub type SchedulerResult<T> = std::result::Result<T, SchedulerError>;

pub trait DataChunkStream = Stream<Item = Result<DataChunk>>;

/// Context for mpp query execution.
pub struct ExecutionContext {
    session: Arc<SessionImpl>,
    timeout: Option<Duration>,
}

pub type ExecutionContextRef = Arc<ExecutionContext>;

impl ExecutionContext {
    pub fn new(session: Arc<SessionImpl>, timeout: Option<Duration>) -> Self {
        Self { session, timeout }
    }

    pub fn session(&self) -> &SessionImpl {
        &self.session
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn to_batch_task_context(&self) -> Arc<dyn BatchTaskContext> {
        FrontendBatchTaskContext::create(self.session.clone())
    }
}
