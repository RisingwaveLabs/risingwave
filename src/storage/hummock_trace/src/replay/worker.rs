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

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::StreamExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::{GlobalReplay, LocalReplay, ReplayRequest, WorkerId, WorkerResponse};
use crate::{
    LocalStorageId, Operation, OperationResult, Record, RecordId, ReplayItem, Result, StorageType,
    TraceResult, TracedNewLocalOptions,
};

#[async_trait::async_trait]
pub trait ReplayWorkerScheduler {
    // schedule a replaying task for given record
    fn schedule(&mut self, record: Record);
    // send result of an operation for a worker
    fn send_result(&mut self, record: Record);
    // wait an operation finishes
    async fn wait_finish(&mut self, record: Record);
    // gracefully shutdown all workers
    async fn shutdown(self);
}

pub(crate) struct WorkerScheduler<G: GlobalReplay> {
    workers: HashMap<WorkerId, WorkerHandler>,
    replay: Arc<G>,
}

impl<G: GlobalReplay> WorkerScheduler<G> {
    pub(crate) fn new(replay: Arc<G>) -> Self {
        WorkerScheduler {
            workers: HashMap::new(),
            replay,
        }
    }
}

#[async_trait::async_trait]
impl<G: GlobalReplay + 'static> ReplayWorkerScheduler for WorkerScheduler<G> {
    fn schedule(&mut self, record: Record) {
        let worker_id = allocate_worker_id(&record);
        let handler = self
            .workers
            .entry(worker_id)
            .or_insert_with(|| ReplayWorker::spawn(self.replay.clone()));

        handler.replay(Some(record));
    }

    fn send_result(&mut self, record: Record) {
        let worker_id = allocate_worker_id(&record);
        let Record { operation, .. } = record;
        // Check if the worker with the given ID exists in the workers map and the record contains a
        // Result operation.
        if let (Some(handler), Operation::Result(trace_result)) =
            (self.workers.get_mut(&worker_id), operation)
        {
            // If the worker exists and the record contains a Result operation, send the result to
            // the worker.
            handler.send_result(trace_result);
        }
    }

    async fn wait_finish(&mut self, record: Record) {
        let worker_id = allocate_worker_id(&record);

        // Check if the worker with the given ID exists in the workers map.
        if let Some(handler) = self.workers.get_mut(&worker_id) {
            // If the worker exists, wait for it to finish executing.
            let resp = handler.wait(record.record_id).await;

            // If the worker is a one-shot worker or local workers that should be closed, remove it
            // from the workers map and call its finish method.
            if matches!(worker_id, WorkerId::OneShot(_))
                || matches!(resp, Some(WorkerResponse::Shutdown))
            {
                let handler = self.workers.remove(&worker_id).unwrap();
                handler.finish();
            }
        }
    }

    async fn shutdown(self) {
        // Iterate over the workers map, calling the finish and join methods on each worker.
        for (_, handler) in self.workers {
            handler.finish();
            handler.join().await;
        }
    }
}

struct ReplayWorker {}

impl ReplayWorker {
    fn spawn(replay: Arc<impl GlobalReplay + 'static>) -> WorkerHandler {
        let (req_tx, req_rx) = unbounded_channel();
        let (resp_tx, resp_rx) = unbounded_channel();
        let (res_tx, res_rx) = unbounded_channel();

        let join = tokio::spawn(Self::run(req_rx, res_rx, resp_tx, replay));
        WorkerHandler {
            req_tx,
            res_tx,
            record_end_resp_rx: resp_rx,
            join,
            stacked_replay_reqs: HashMap::new(),
        }
    }

    async fn run(
        mut req_rx: UnboundedReceiver<ReplayRequest>,
        mut res_rx: UnboundedReceiver<OperationResult>,
        resp_tx: UnboundedSender<WorkerResponse>,
        replay: Arc<impl GlobalReplay>,
    ) {
        let mut iters_map: HashMap<RecordId, BoxStream<'static, Result<ReplayItem>>> =
            HashMap::new();
        let mut local_storages = LocalStorages::new();
        let mut should_shutdown = false;
        let mut local_storage_opts_map = HashMap::new();

        while let Some(Some(record)) = req_rx.recv().await {
            Self::handle_record(
                record.clone(),
                &replay,
                &mut res_rx,
                &mut iters_map,
                &mut local_storages,
                &mut local_storage_opts_map,
                &mut should_shutdown,
            )
            .await;

            let message = if should_shutdown {
                WorkerResponse::Shutdown
            } else {
                WorkerResponse::Continue
            };

            resp_tx.send(message).expect("Failed to send message");
        }
    }

    async fn handle_record(
        record: Record,
        replay: &Arc<impl GlobalReplay>,
        res_rx: &mut UnboundedReceiver<OperationResult>,
        iters_map: &mut HashMap<RecordId, BoxStream<'static, Result<ReplayItem>>>,
        local_storages: &mut LocalStorages,
        local_storage_opts_map: &mut HashMap<LocalStorageId, TracedNewLocalOptions>,
        should_shutdown: &mut bool,
    ) {
        let Record {
            storage_type,
            record_id,
            operation,
        } = record;

        match operation {
            Operation::Get {
                key,
                epoch,
                read_options,
            } => {
                let actual = match storage_type {
                    StorageType::Global => {
                        // epoch must be Some
                        let epoch = epoch.unwrap();
                        replay.get(key, epoch, read_options).await
                    }
                    StorageType::Local(_, local_storage_id) => {
                        let opts = local_storage_opts_map.get(&local_storage_id).unwrap();
                        assert_eq!(opts.table_id, read_options.table_id);
                        let s = local_storages.get_mut(&storage_type).unwrap();
                        s.get(key, read_options).await
                    }
                };

                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Get(expected) = res {
                    assert_eq!(TraceResult::from(actual), expected, "get result wrong");
                } else {
                    panic!("expect get result, but got {:?}", res);
                }
            }
            Operation::Insert {
                key,
                new_val,
                old_val,
            } => {
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let actual = local_storage.insert(key, new_val, old_val);

                let expected = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Insert(expected) = expected {
                    assert_eq!(TraceResult::from(actual), expected, "get result wrong");
                } else {
                    panic!("expect insert result, but got {:?}", expected);
                }
            }
            Operation::Delete { key, old_val } => {
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let actual = local_storage.delete(key, old_val);

                let expected = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Delete(expected) = expected {
                    assert_eq!(TraceResult::from(actual), expected, "get result wrong");
                } else {
                    panic!("expect delete result, but got {:?}", expected);
                }
            }
            Operation::Iter {
                key_range,
                epoch,
                read_options,
            } => {
                let iter = match storage_type {
                    StorageType::Global => {
                        // Global Storage must have a epoch
                        let epoch = epoch.unwrap();
                        replay.iter(key_range, epoch, read_options).await
                    }
                    StorageType::Local(_, id) => {
                        let opts = local_storage_opts_map.get(&id).unwrap();
                        assert_eq!(opts.table_id, read_options.table_id);
                        let s = local_storages.get_mut(&storage_type).unwrap();
                        s.iter(key_range, read_options).await
                    }
                };
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Iter(expected) = res {
                    if expected.is_ok() {
                        let iter = iter.unwrap().boxed();
                        let id = record_id;
                        iters_map.insert(id, iter);
                    } else {
                        assert!(iter.is_err());
                    }
                } else {
                    panic!("expect iter result, but got {:?}", res);
                }
            }
            Operation::Sync(sync_table_epochs) => {
                assert_eq!(storage_type, StorageType::Global);
                let sync_result = replay.sync(sync_table_epochs).await.unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Sync(expected) = res {
                    assert_eq!(TraceResult::Ok(sync_result), expected, "sync failed");
                } else {
                    panic!("expect sync result, but got {:?}", res);
                }
            }
            Operation::IterNext(id) => {
                let iter = iters_map.get_mut(&id).expect("iter not in worker");
                let actual = iter.next().await;
                let actual = actual.map(|res| res.unwrap());
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::IterNext(expected) = res {
                    assert_eq!(TraceResult::Ok(actual), expected, "iter_next result wrong");
                } else {
                    panic!("expect iter_next result, but got {:?}", res);
                }
            }
            Operation::NewLocalStorage(new_local_opts, id) => {
                assert_ne!(storage_type, StorageType::Global);
                local_storage_opts_map.insert(id, new_local_opts.clone());
                let local_storage = replay.new_local(new_local_opts).await;
                local_storages.insert(storage_type, local_storage);
            }
            Operation::DropLocalStorage => {
                assert_ne!(storage_type, StorageType::Global);
                if let StorageType::Local(_, _) = storage_type {
                    local_storages.remove(&storage_type);
                }
                // All local storages have been dropped, we should shutdown this worker
                // If there are incoming new_local, this ReplayWorker will spawn again
                if local_storages.is_empty() {
                    *should_shutdown = true;
                }
            }
            Operation::MetaMessage(resp) => {
                assert_eq!(storage_type, StorageType::Global);
                let op = resp.0.operation();
                if let Some(info) = resp.0.info {
                    replay
                        .notify_hummock(info, op, resp.0.version)
                        .await
                        .unwrap();
                }
            }
            Operation::LocalStorageInit(options) => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                local_storage.init(options).await.unwrap();
            }
            Operation::TryWaitEpoch(epoch, options) => {
                assert_eq!(storage_type, StorageType::Global);
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::TryWaitEpoch(expected) = res {
                    let actual = replay.try_wait_epoch(epoch.into(), options).await;
                    assert_eq!(TraceResult::from(actual), expected, "try_wait_epoch wrong");
                } else {
                    panic!(
                        "wrong try_wait_epoch result, expect epoch result, but got {:?}",
                        res
                    );
                }
            }
            Operation::SealCurrentEpoch { epoch, opts } => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                local_storage.seal_current_epoch(epoch, opts);
            }
            Operation::LocalStorageEpoch => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::LocalStorageEpoch(expected) = res {
                    let actual = local_storage.epoch();
                    assert_eq!(TraceResult::Ok(actual), expected, "epoch wrong");
                } else {
                    panic!(
                        "wrong local storage epoch result, expect epoch result, but got {:?}",
                        res
                    );
                }
            }
            Operation::LocalStorageIsDirty => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::LocalStorageIsDirty(expected) = res {
                    let actual = local_storage.is_dirty();
                    assert_eq!(
                        TraceResult::Ok(actual),
                        expected,
                        "is_dirty wrong, epoch: {}",
                        local_storage.epoch()
                    );
                } else {
                    panic!(
                        "wrong local storage is_dirty result, expect is_dirty result, but got {:?}",
                        res
                    );
                }
            }
            Operation::Flush => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::Flush(expected) = res {
                    let actual = local_storage.flush().await;
                    assert_eq!(TraceResult::from(actual), expected, "flush wrong");
                } else {
                    panic!("wrong flush result, expect flush result, but got {:?}", res);
                }
            }
            Operation::TryFlush => {
                assert_ne!(storage_type, StorageType::Global);
                let local_storage = local_storages.get_mut(&storage_type).unwrap();
                let res = res_rx.recv().await.expect("recv result failed");
                if let OperationResult::TryFlush(_) = res {
                    let _ = local_storage.try_flush().await;
                    // todo(wcy-fdu): unify try_flush and flush interface, do not return usize.
                    // assert_eq!(TraceResult::from(actual), expected, "try flush wrong");
                } else {
                    panic!(
                        "wrong try flush result, expect flush result, but got {:?}",
                        res
                    );
                }
            }

            Operation::Finish => unreachable!(),
            Operation::Result(_) => unreachable!(),
        }
    }
}

fn allocate_worker_id(record: &Record) -> WorkerId {
    match record.storage_type() {
        StorageType::Local(concurrent_id, id) => WorkerId::Local(*concurrent_id, *id),
        StorageType::Global => WorkerId::OneShot(record.record_id()),
    }
}

struct WorkerHandler {
    req_tx: UnboundedSender<ReplayRequest>,
    res_tx: UnboundedSender<OperationResult>,
    record_end_resp_rx: UnboundedReceiver<WorkerResponse>,
    join: JoinHandle<()>,
    // Used for ops like iter, since a iter may have multiple next
    // Example
    // Iter begin, Iter next, Iter next...., Iter finish
    // So replay requests may be stacked
    stacked_replay_reqs: HashMap<u64, u32>,
}

impl WorkerHandler {
    async fn join(self) {
        self.join.await.expect("failed to stop worker");
    }

    fn finish(&self) {
        self.send_replay_req(None);
    }

    fn replay(&mut self, req: ReplayRequest) {
        if let Some(r) = &req {
            let entry = self.stacked_replay_reqs.entry(r.record_id).or_insert(0);
            *entry += 1;
        }
        self.send_replay_req(req);
    }

    async fn wait(&mut self, record_id: u64) -> Option<WorkerResponse> {
        let mut stacked_replay_reqs = *self.stacked_replay_reqs.get(&record_id).unwrap();
        assert!(
            stacked_replay_reqs > 0,
            "replay count should be 0, but found {}",
            stacked_replay_reqs
        );
        let mut resp = None;

        while stacked_replay_reqs > 0 {
            resp = Some(
                self.record_end_resp_rx
                    .recv()
                    .await
                    .expect("failed to wait worker resp"),
            );
            stacked_replay_reqs -= 1;
        }
        // cleaned this record from replay worker
        self.stacked_replay_reqs.remove(&record_id);
        // impossible to be None
        resp
    }

    fn send_replay_req(&self, req: ReplayRequest) {
        self.req_tx
            .send(req)
            .expect("failed to send replay request");
    }

    fn send_result(&self, result: OperationResult) {
        self.res_tx.send(result).expect("failed to send result");
    }
}

struct LocalStorages {
    storages: HashMap<StorageType, Box<dyn LocalReplay>>,
}

impl LocalStorages {
    fn new() -> Self {
        Self {
            storages: HashMap::new(),
        }
    }

    fn remove(&mut self, storage_type: &StorageType) {
        self.storages.remove(storage_type);
    }

    fn get_mut(&mut self, storage_type: &StorageType) -> Option<&mut Box<dyn LocalReplay>> {
        self.storages.get_mut(storage_type)
    }

    fn insert(&mut self, storage_type: StorageType, local_storage: Box<dyn LocalReplay>) {
        self.storages.insert(storage_type, local_storage);
    }

    fn is_empty(&self) -> bool {
        self.storages.is_empty()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.storages.len()
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Bound;

    use bytes::Bytes;
    use mockall::predicate;

    use super::*;
    use crate::replay::{MockGlobalReplayInterface, MockLocalReplayInterface};
    use crate::{MockReplayIterStream, TracedBytes, TracedReadOptions};

    #[tokio::test]
    async fn test_handle_record() {
        let mut iters_map = HashMap::new();
        let mut local_storage_opts_map = HashMap::new();
        let mut local_storages = LocalStorages::new();
        let (res_tx, mut res_rx) = unbounded_channel();
        let get_table_id = 12;
        let iter_table_id = 14654;
        let read_options = TracedReadOptions::for_test(get_table_id);
        let iter_read_options = TracedReadOptions::for_test(iter_table_id);
        let op = Operation::get(Bytes::from(vec![123]), Some(123), read_options);

        let new_local_opts = TracedNewLocalOptions::for_test(get_table_id);

        let iter_local_opts = TracedNewLocalOptions::for_test(iter_table_id);
        let mut should_exit = false;
        let get_storage_type = StorageType::Local(0, 0);
        let record = Record::new(get_storage_type, 1, op);
        let mut mock_replay = MockGlobalReplayInterface::new();

        mock_replay.expect_new_local().times(1).returning(move |_| {
            let mut mock_local = MockLocalReplayInterface::new();

            mock_local
                .expect_get()
                .with(
                    predicate::eq(TracedBytes::from(vec![123])),
                    predicate::always(),
                )
                .returning(|_, _| Ok(Some(TracedBytes::from(vec![120]))));

            Box::new(mock_local)
        });

        mock_replay.expect_new_local().times(1).returning(move |_| {
            let mut mock_local = MockLocalReplayInterface::new();

            mock_local
                .expect_iter()
                .with(
                    predicate::eq((Bound::Unbounded, Bound::Unbounded)),
                    predicate::always(),
                )
                .returning(move |_, _| {
                    let iter = MockReplayIterStream::new(vec![(
                        TracedBytes::from(vec![1]),
                        TracedBytes::from(vec![0]),
                    )]);
                    Ok(iter.into_stream().boxed())
                });

            Box::new(mock_local)
        });

        let replay = Arc::new(mock_replay);

        ReplayWorker::handle_record(
            Record::new(
                get_storage_type,
                0,
                Operation::NewLocalStorage(new_local_opts, 0),
            ),
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
            &mut local_storage_opts_map,
            &mut should_exit,
        )
        .await;

        res_tx
            .send(OperationResult::Get(TraceResult::Ok(Some(
                TracedBytes::from(vec![120]),
            ))))
            .unwrap();

        ReplayWorker::handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
            &mut local_storage_opts_map,
            &mut should_exit,
        )
        .await;

        assert_eq!(local_storages.len(), 1);
        assert!(iters_map.is_empty());

        let op = Operation::Iter {
            key_range: (Bound::Unbounded, Bound::Unbounded),
            epoch: Some(45),
            read_options: iter_read_options,
        };

        let iter_storage_type = StorageType::Local(0, 1);

        ReplayWorker::handle_record(
            Record::new(
                iter_storage_type,
                2,
                Operation::NewLocalStorage(iter_local_opts, 1),
            ),
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
            &mut local_storage_opts_map,
            &mut should_exit,
        )
        .await;

        let record = Record::new(iter_storage_type, 1, op);
        res_tx
            .send(OperationResult::Iter(TraceResult::Ok(())))
            .unwrap();

        ReplayWorker::handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
            &mut local_storage_opts_map,
            &mut should_exit,
        )
        .await;

        assert_eq!(local_storages.len(), 2);
        assert_eq!(iters_map.len(), 1);

        let op = Operation::IterNext(1);
        let record = Record::new(iter_storage_type, 3, op);
        res_tx
            .send(OperationResult::IterNext(TraceResult::Ok(Some((
                TracedBytes::from(vec![1]),
                TracedBytes::from(vec![0]),
            )))))
            .unwrap();

        ReplayWorker::handle_record(
            record,
            &replay,
            &mut res_rx,
            &mut iters_map,
            &mut local_storages,
            &mut local_storage_opts_map,
            &mut should_exit,
        )
        .await;

        assert_eq!(local_storages.len(), 2);
        assert_eq!(iters_map.len(), 1);
    }

    #[tokio::test]
    async fn test_worker_scheduler() {
        // Create a mock GlobalReplay and a ReplayWorkerScheduler that uses the mock GlobalReplay.
        let mut mock_replay = MockGlobalReplayInterface::default();
        let record_id = 29053;
        let key = TracedBytes::from(vec![1]);
        let epoch = 2596;
        let read_options = TracedReadOptions::for_test(1);

        let res_bytes = TracedBytes::from(vec![58, 54, 35]);

        mock_replay
            .expect_get()
            .with(
                predicate::eq(key.clone()),
                predicate::eq(epoch),
                predicate::eq(read_options.clone()),
            )
            .returning(move |_, _, _| Ok(Some(TracedBytes::from(vec![58, 54, 35]))));

        let mut scheduler = WorkerScheduler::new(Arc::new(mock_replay));
        // Schedule a record for replay.
        let record = Record::new(
            StorageType::Global,
            record_id,
            Operation::get(key.into(), Some(epoch), read_options),
        );
        scheduler.schedule(record);

        let result = Record::new(
            StorageType::Global,
            record_id,
            Operation::Result(OperationResult::Get(TraceResult::Ok(Some(res_bytes)))),
        );

        scheduler.send_result(result);

        let fin = Record::new(StorageType::Global, record_id, Operation::Finish);
        scheduler.wait_finish(fin).await;

        scheduler.shutdown().await;
    }
}
