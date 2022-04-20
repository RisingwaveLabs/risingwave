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

use std::collections::VecDeque;

use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::Keyspace;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::executor::*;

#[macro_export]

/// `row_nonnull` builds a `Row` with concrete values.
/// TODO: add macro row!, which requires a new trait `ToScalarValue`.
macro_rules! row_nonnull {
    [$( $value:expr ),*] => {
        {
            use risingwave_common::types::Scalar;
            use risingwave_common::array::Row;
            Row(vec![$(Some($value.to_scalar_value()), )*])
        }
    };
}

pub struct MockSource {
    schema: Schema,
    pk_indices: PkIndices,
    epoch: u64,
    msgs: VecDeque<Message>,
}

impl std::fmt::Debug for MockSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockSource")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl MockSource {
    pub fn new(schema: Schema, pk_indices: PkIndices) -> Self {
        Self {
            schema,
            pk_indices,
            epoch: 0,
            msgs: VecDeque::default(),
        }
    }

    #[allow(dead_code)]
    pub fn with_messages(schema: Schema, pk_indices: PkIndices, msgs: Vec<Message>) -> Self {
        Self {
            schema,
            pk_indices,
            epoch: 0,
            msgs: msgs.into(),
        }
    }

    pub fn push_chunks(&mut self, chunks: impl Iterator<Item = StreamChunk>) {
        self.msgs.extend(chunks.map(Message::Chunk));
    }

    pub fn push_barrier(&mut self, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_mutation(Mutation::Stop(HashSet::default()));
        }
        self.msgs.push_back(Message::Barrier(barrier));
    }
}

#[async_trait]
impl ExecutorV1 for MockSource {
    async fn next(&mut self) -> Result<Message> {
        self.epoch += 1;
        match self.msgs.pop_front() {
            Some(msg) => Ok(msg),
            None => Ok(Message::Barrier(
                Barrier::new_test_barrier(self.epoch)
                    .with_mutation(Mutation::Stop(HashSet::default())),
            )),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "MockSource"
    }

    fn logical_operator_info(&self) -> &str {
        self.identity()
    }
}

/// This source takes message from users asynchronously
pub struct MockAsyncSource {
    schema: Schema,
    pk_indices: PkIndices,
    epoch: u64,
    rx: UnboundedReceiver<Message>,
}

impl std::fmt::Debug for MockAsyncSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockAsyncSource")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl MockAsyncSource {
    #[allow(dead_code)]
    pub fn new(schema: Schema, rx: UnboundedReceiver<Message>) -> Self {
        Self {
            schema,
            pk_indices: vec![],
            rx,
            epoch: 0,
        }
    }

    pub fn with_pk_indices(
        schema: Schema,
        rx: UnboundedReceiver<Message>,
        pk_indices: Vec<usize>,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            rx,
            epoch: 0,
        }
    }

    pub fn push_chunks(
        tx: &mut UnboundedSender<Message>,
        chunks: impl IntoIterator<Item = StreamChunk>,
    ) {
        for chunk in chunks {
            tx.send(Message::Chunk(chunk)).expect("Receiver closed");
        }
    }

    pub fn push_barrier(tx: &mut UnboundedSender<Message>, epoch: u64, stop: bool) {
        let mut barrier = Barrier::new_test_barrier(epoch);
        if stop {
            barrier = barrier.with_mutation(Mutation::Stop(HashSet::default()))
        }
        tx.send(Message::Barrier(barrier)).expect("Receiver closed");
    }
}

#[async_trait]
impl ExecutorV1 for MockAsyncSource {
    async fn next(&mut self) -> Result<Message> {
        self.epoch += 1;
        match self.rx.recv().await {
            Some(msg) => Ok(msg),
            None => Ok(Message::Barrier(
                Barrier::new_test_barrier(self.epoch)
                    .with_mutation(Mutation::Stop(HashSet::default())),
            )),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &'static str {
        "MockAsyncSource"
    }

    fn logical_operator_info(&self) -> &str {
        self.identity()
    }
}

pub fn create_in_memory_keyspace() -> Keyspace<MemoryStateStore> {
    Keyspace::executor_root(MemoryStateStore::new(), 0x2333)
}
