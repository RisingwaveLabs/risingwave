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

use std::collections::HashSet;
use std::env;
use std::fs::{create_dir_all, OpenOptions};
use std::io::BufWriter;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::LazyLock;

use bincode::{Decode, Encode};
use bytes::Bytes;
use parking_lot::Mutex;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch};
use risingwave_pb::meta::SubscribeResponse;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{
    unbounded_channel as channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
use tokio::task_local;

use crate::write::{TraceWriter, TraceWriterImpl};
use crate::{
    ConcurrentIdGenerator, Operation, OperationResult, Record, RecordId, RecordIdGenerator,
    TracedInitOptions, TracedNewLocalOptions, TracedReadOptions, TracedSealCurrentEpochOptions,
    TracedSubResp, TracedTryWaitEpochOptions, UniqueIdGenerator,
};

// Global collector instance used for trace collection
static GLOBAL_COLLECTOR: LazyLock<GlobalCollector> = LazyLock::new(GlobalCollector::new);

// Global record ID generator for generating unique record IDs
static GLOBAL_RECORD_ID: LazyLock<RecordIdGenerator> =
    LazyLock::new(|| UniqueIdGenerator::new(AtomicU64::new(0)));

// Flag indicating whether trace should be used
static SHOULD_USE_TRACE: LazyLock<bool> = LazyLock::new(set_should_use_trace);

// Concurrent record ID generator for generating unique record IDs in concurrent environments
pub static CONCURRENT_ID: LazyLock<ConcurrentIdGenerator> =
    LazyLock::new(|| UniqueIdGenerator::new(AtomicU64::new(0)));

// A tokio runtime for hummock tracing
static TRACE_RT: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

pub const USE_TRACE: &str = "USE_HM_TRACE"; // Environment variable name for enabling trace
const LOG_PATH: &str = "HM_TRACE_PATH"; // Environment variable name for specifying trace log path
const DEFAULT_PATH: &str = ".trace/hummock.ht"; // Default trace log path
const WRITER_BUFFER_SIZE: usize = 1024; // Buffer size for trace writer

/// Returns whether trace should be used based on the environment variable
pub fn should_use_trace() -> bool {
    *SHOULD_USE_TRACE
}

/// Sets the value of the `SHOULD_USE_TRACE` flag based on the `USE_TRACE` environment variable
fn set_should_use_trace() -> bool {
    match std::env::var(USE_TRACE) {
        Ok(v) => v.parse().unwrap_or(false),
        Err(_) => false,
    }
}

/// Initialize the `GLOBAL_COLLECTOR` with configured log file
pub fn init_collector() {
    TRACE_RT.spawn(async move {
        let path = env::var(LOG_PATH).unwrap_or_else(|_| DEFAULT_PATH.to_owned());
        let path = Path::new(&path);
        tracing::info!("Hummock Tracing log path {}", path.to_string_lossy());

        if let Some(parent) = path.parent() {
            if !parent.exists() {
                create_dir_all(parent).unwrap();
            }
        }
        let f = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .expect("failed to open log file");
        let writer = BufWriter::with_capacity(WRITER_BUFFER_SIZE, f);
        let writer = TraceWriterImpl::try_new_bincode(writer).unwrap();
        GlobalCollector::run(writer);
    });
}

/// `GlobalCollector` collects traced hummock operations.
/// It starts a collector thread and writer thread.
struct GlobalCollector {
    tx: Sender<RecordMsg>,
    rx: Mutex<Option<Receiver<RecordMsg>>>,
}

impl GlobalCollector {
    fn new() -> Self {
        let (tx, rx) = channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }

    fn run(mut writer: impl TraceWriter + Send + 'static) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_blocking(move || {
            let mut rx = GLOBAL_COLLECTOR.rx.lock().take().unwrap();
            while let Some(Some(r)) = rx.blocking_recv() {
                writer.write(r).expect("failed to write hummock trace");
            }
            writer.flush().expect("failed to flush hummock trace");
        })
    }

    fn finish(&self) {
        self.tx.send(None).expect("failed to finish worker");
    }

    fn tx(&self) -> Sender<RecordMsg> {
        self.tx.clone()
    }
}

impl Drop for GlobalCollector {
    fn drop(&mut self) {
        // only send when channel is not closed
        if !self.tx.is_closed() {
            self.finish();
        }
    }
}

/// `TraceSpan` traces hummock operations. It marks the beginning of an operation and
/// the end when the span is dropped. So, please make sure the span live long enough.
/// Underscore binding like `let _ = span` will drop the span immediately.
#[must_use = "TraceSpan Lifetime is important"]
#[derive(Clone)]
pub struct TraceSpan {
    tx: Sender<RecordMsg>,
    id: RecordId,
    storage_type: StorageType,
}

#[must_use = "TraceSpan Lifetime is important"]
#[derive(Clone)]
pub struct MayTraceSpan(Option<TraceSpan>);

impl From<Option<TraceSpan>> for MayTraceSpan {
    fn from(value: Option<TraceSpan>) -> Self {
        Self(value)
    }
}

impl MayTraceSpan {
    pub fn may_send_result(&self, res: OperationResult) {
        if let Some(span) = &self.0 {
            span.send_result(res)
        }
    }

    pub fn may_send_op(&self, op: Operation) {
        if let Some(span) = &self.0 {
            span.send(op)
        }
    }

    pub fn may_send_iter_next(&self) {
        if let Some(span) = &self.0 {
            span.send(Operation::IterNext(span.id))
        }
    }
}

impl TraceSpan {
    pub fn new(tx: Sender<RecordMsg>, id: RecordId, storage_type: StorageType) -> Self {
        Self {
            tx,
            id,
            storage_type,
        }
    }

    pub fn new_global_op(op: Operation, storage_type: StorageType) -> MayTraceSpan {
        match should_use_trace() {
            true => Some(Self::new_to_global(op, storage_type)).into(),
            false => None.into(),
        }
    }

    pub fn new_epoch_span(storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(Operation::LocalStorageEpoch, storage_type)
    }

    pub fn new_is_dirty_span(storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(Operation::LocalStorageIsDirty, storage_type)
    }

    pub fn new_seal_current_epoch_span(
        epoch: u64,
        opts: TracedSealCurrentEpochOptions,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(Operation::SealCurrentEpoch { epoch, opts }, storage_type)
    }

    pub fn new_try_wait_epoch_span(
        epoch: HummockReadEpoch,
        options: TracedTryWaitEpochOptions,
    ) -> MayTraceSpan {
        Self::new_global_op(
            Operation::TryWaitEpoch(epoch.into(), options),
            StorageType::Global,
        )
    }

    pub fn new_get_span(
        key: Bytes,
        epoch: Option<u64>,
        read_options: TracedReadOptions,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(Operation::get(key, epoch, read_options), storage_type)
    }

    pub fn new_iter_span(
        key_range: (Bound<Bytes>, Bound<Bytes>),
        epoch: Option<u64>,
        read_options: TracedReadOptions,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(
            Operation::Iter {
                key_range: (
                    key_range.0.as_ref().map(|v| v.clone().into()),
                    key_range.1.as_ref().map(|v| v.clone().into()),
                ),
                epoch,
                read_options,
            },
            storage_type,
        )
    }

    pub fn new_insert_span(
        key: Bytes,
        new_val: Bytes,
        old_val: Option<Bytes>,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(
            Operation::Insert {
                key: key.into(),
                new_val: new_val.into(),
                old_val: old_val.map(|b| b.into()),
            },
            storage_type,
        )
    }

    pub fn new_delete_span(key: Bytes, old_val: Bytes, storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(
            Operation::Delete {
                key: key.into(),
                old_val: old_val.into(),
            },
            storage_type,
        )
    }

    pub fn new_sync_span(
        sync_table_epochs: &Vec<(HummockEpoch, HashSet<TableId>)>,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(
            Operation::Sync(
                sync_table_epochs
                    .iter()
                    .map(|(epoch, table_ids)| {
                        (
                            *epoch,
                            table_ids.iter().map(|table_id| table_id.table_id).collect(),
                        )
                    })
                    .collect(),
            ),
            storage_type,
        )
    }

    pub fn new_local_storage_span(
        option: TracedNewLocalOptions,
        storage_type: StorageType,
        local_storage_id: u64,
    ) -> MayTraceSpan {
        Self::new_global_op(
            Operation::NewLocalStorage(option, local_storage_id),
            storage_type,
        )
    }

    pub fn new_drop_storage_span(storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(Operation::DropLocalStorage, storage_type)
    }

    pub fn new_flush_span(storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(Operation::Flush, storage_type)
    }

    pub fn new_try_flush_span(storage_type: StorageType) -> MayTraceSpan {
        Self::new_global_op(Operation::TryFlush, storage_type)
    }

    pub fn new_meta_message_span(resp: SubscribeResponse) -> MayTraceSpan {
        Self::new_global_op(
            Operation::MetaMessage(Box::new(TracedSubResp::from(resp))),
            StorageType::Global,
        )
    }

    pub fn new_local_storage_init_span(
        options: TracedInitOptions,
        storage_type: StorageType,
    ) -> MayTraceSpan {
        Self::new_global_op(Operation::LocalStorageInit(options), storage_type)
    }

    pub fn send(&self, op: Operation) {
        self.tx
            .send(Some(Record::new(*self.storage_type(), self.id(), op)))
            .expect("failed to log record");
    }

    pub fn send_result(&self, res: OperationResult) {
        self.send(Operation::Result(res));
    }

    pub fn finish(&self) {
        self.send(Operation::Finish);
    }

    pub fn id(&self) -> RecordId {
        self.id
    }

    fn storage_type(&self) -> &StorageType {
        &self.storage_type
    }

    /// Create a span and send operation to the `GLOBAL_COLLECTOR`
    pub fn new_to_global(op: Operation, storage_type: StorageType) -> Self {
        let span = TraceSpan::new(GLOBAL_COLLECTOR.tx(), GLOBAL_RECORD_ID.next(), storage_type);
        span.send(op);
        span
    }

    #[cfg(test)]
    pub fn new_with_op(
        tx: Sender<RecordMsg>,
        id: RecordId,
        op: Operation,
        storage_type: StorageType,
    ) -> Self {
        let span = TraceSpan::new(tx, id, storage_type);
        span.send(op);
        span
    }
}

impl Drop for TraceSpan {
    fn drop(&mut self) {
        self.finish();
    }
}

pub type RecordMsg = Option<Record>;
pub type ConcurrentId = u64;
pub type LocalStorageId = u64;

#[derive(Copy, Clone, Debug, Encode, Decode, PartialEq, Hash, Eq)]
pub enum StorageType {
    Global,
    Local(ConcurrentId, LocalStorageId),
}

task_local! {
    // This is why we need to ignore this rule
    // https://github.com/rust-lang/rust-clippy/issues/9224
    #[allow(clippy::declare_interior_mutable_const)]
    pub static LOCAL_ID: ConcurrentId;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::MockTraceWriter;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_new_spans_concurrent() {
        let count = 200;

        let collector = Arc::new(GlobalCollector::new());
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));
        let mut handles = Vec::with_capacity(count);

        for i in 0..count {
            let collector = collector.clone();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let op = Operation::get(
                    Bytes::from(vec![i as u8]),
                    Some(123),
                    TracedReadOptions::for_test(0),
                );
                let _span = TraceSpan::new_with_op(
                    collector.tx(),
                    generator.next(),
                    op,
                    StorageType::Global,
                );
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let mut rx = collector.rx.lock().take().unwrap();
        let mut rx_count = 0;
        rx.close();
        while rx.recv().await.is_some() {
            rx_count += 1;
        }
        assert_eq!(count * 2, rx_count);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_collector_run() {
        let count = 5000;
        let generator = Arc::new(UniqueIdGenerator::new(AtomicU64::new(0)));

        let op = Operation::get(
            Bytes::from(vec![74, 56, 43, 67]),
            Some(256),
            TracedReadOptions::for_test(0),
        );
        let mut mock_writer = MockTraceWriter::new();

        mock_writer
            .expect_write()
            .times(count * 2)
            .returning(|_| Ok(0));
        mock_writer.expect_flush().times(1).returning(|| Ok(()));

        let runner_handle = GlobalCollector::run(mock_writer);

        let mut handles = Vec::with_capacity(count);

        for _ in 0..count {
            let op = op.clone();
            let tx = GLOBAL_COLLECTOR.tx();
            let generator = generator.clone();
            let handle = tokio::spawn(async move {
                let _span =
                    TraceSpan::new_with_op(tx, generator.next(), op, StorageType::Local(0, 0));
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        GLOBAL_COLLECTOR.finish();

        runner_handle.await.unwrap();
    }

    #[ignore]
    #[test]
    fn test_set_use_trace() {
        std::env::remove_var(USE_TRACE);
        assert!(!set_should_use_trace());

        std::env::set_var(USE_TRACE, "true");
        assert!(set_should_use_trace());

        std::env::set_var(USE_TRACE, "false");
        assert!(!set_should_use_trace());

        std::env::set_var(USE_TRACE, "invalid");
        assert!(!set_should_use_trace());
    }

    #[ignore]
    #[test]
    fn test_should_use_trace() {
        std::env::set_var(USE_TRACE, "true");
        assert!(should_use_trace());
        assert!(set_should_use_trace());
    }
}
