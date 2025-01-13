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
use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{Future, FutureExt};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{TableKey, TableKeyRange};
use risingwave_hummock_sdk::{HummockEpoch, HummockReadEpoch, SyncResult};
use risingwave_hummock_trace::{
    init_collector, should_use_trace, ConcurrentId, MayTraceSpan, OperationResult, StorageType,
    TraceResult, TraceSpan, TracedBytes, TracedSealCurrentEpochOptions, LOCAL_ID,
};
use thiserror_ext::AsReport;

use crate::error::StorageResult;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockStorage, SstableObjectIdManagerRef};
use crate::store::*;
use crate::store_impl::AsHummock;

#[derive(Clone)]
pub struct TracedStateStore<S, E = ()> {
    inner: S,
    storage_type: StorageType,
    extra: E,
}

impl<S> TracedStateStore<S, Option<u64>> {
    pub fn new_with_snapshot_epoch(inner: S, epoch: Option<u64>) -> Self {
        if should_use_trace() {
            init_collector();
            tracing::info!("Hummock Tracing Enabled");
        }
        Self {
            inner,
            storage_type: StorageType::Global,
            extra: epoch,
        }
    }

    pub fn epoch(&self) -> Option<u64> {
        self.extra
    }

    pub fn new_local(inner: S, options: NewLocalOptions) -> Self {
        let id = get_concurrent_id();
        let local_storage_id = rand::random::<u64>();
        let storage_type: StorageType = StorageType::Local(id, local_storage_id);
        let _span: MayTraceSpan =
            TraceSpan::new_local_storage_span(options.into(), storage_type, local_storage_id);

        Self {
            inner,
            storage_type,
            extra: None,
        }
    }
}

impl<S> TracedStateStore<S> {
    pub fn new(inner: S, storage_type: StorageType) -> Self {
        if should_use_trace() {
            init_collector();
            tracing::info!("Hummock Tracing Enabled");
        }
        Self {
            inner,
            storage_type,
            extra: (),
        }
    }

    pub fn new_global(inner: S) -> Self {
        Self::new(inner, StorageType::Global)
    }
}

impl<S, E> TracedStateStore<S, E> {
    async fn traced_iter<'a, St: StateStoreIter>(
        &'a self,
        iter_stream_future: impl Future<Output = StorageResult<St>> + 'a,
        span: MayTraceSpan,
    ) -> StorageResult<TracedStateStoreIter<St>> {
        let res = iter_stream_future.await;
        if res.is_ok() {
            span.may_send_result(OperationResult::Iter(TraceResult::Ok(())));
        } else {
            span.may_send_result(OperationResult::Iter(TraceResult::Err));
        }
        let traced = TracedStateStoreIter::new(res?, span);
        Ok(traced)
    }

    async fn traced_get_keyed_row(
        &self,
        key: TableKey<Bytes>,
        epoch: Option<u64>,
        read_options: ReadOptions,
        get_future: impl Future<Output = StorageResult<Option<StateStoreKeyedRow>>>,
    ) -> StorageResult<Option<StateStoreKeyedRow>> {
        let span = TraceSpan::new_get_span(
            key.0.clone(),
            epoch,
            read_options.clone().into(),
            self.storage_type,
        );

        let res = get_future.await;

        span.may_send_result(OperationResult::Get(TraceResult::from(
            res.as_ref()
                .map(|o| o.as_ref().map(|(_, v)| TracedBytes::from(v.clone()))),
        )));
        res
    }
}

impl<S: StateStoreGet> StateStoreGet for TracedStateStore<S, Option<u64>> {
    async fn on_key_value<O: Send + 'static>(
        &self,
        key: TableKey<Bytes>,
        read_options: ReadOptions,
        on_key_value_fn: impl KeyValueFn<O>,
    ) -> StorageResult<Option<O>> {
        if let Some((key, value)) = self
            .traced_get_keyed_row(
                key.clone(),
                self.epoch(),
                read_options.clone(),
                self.inner.on_key_value(key, read_options, |key, value| {
                    Ok((key.copy_into(), Bytes::copy_from_slice(value)))
                }),
            )
            .await?
        {
            Ok(Some(on_key_value_fn(key.to_ref(), value.as_ref())?))
        } else {
            Ok(None)
        }
    }
}

impl<S: LocalStateStore> LocalStateStore for TracedStateStore<S, Option<u64>> {
    type FlushedSnapshotReader = TracedStateStore<S::FlushedSnapshotReader, Option<u64>>;

    type Iter<'a> = impl StateStoreIter + 'a;
    type RevIter<'a> = impl StateStoreIter + 'a;

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter<'_>>> + Send + '_ {
        let (l, r) = key_range.clone();
        let bytes_key_range = (l.map(|l| l.0), r.map(|r| r.0));
        let span = TraceSpan::new_iter_span(
            bytes_key_range,
            None,
            read_options.clone().into(),
            self.storage_type,
        );
        self.traced_iter(self.inner.iter(key_range, read_options), span)
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter<'_>>> + Send + '_ {
        let (l, r) = key_range.clone();
        let bytes_key_range = (l.map(|l| l.0), r.map(|r| r.0));
        let span = TraceSpan::new_iter_span(
            bytes_key_range,
            None,
            read_options.clone().into(),
            self.storage_type,
        );
        self.traced_iter(self.inner.rev_iter(key_range, read_options), span)
    }

    fn insert(
        &mut self,
        key: TableKey<Bytes>,
        new_val: Bytes,
        old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        let span = TraceSpan::new_insert_span(
            key.0.clone(),
            new_val.clone(),
            old_val.clone(),
            self.storage_type,
        );
        let res = self.inner.insert(key, new_val, old_val);

        span.may_send_result(OperationResult::Insert(res.as_ref().map(|o| *o).into()));
        res
    }

    fn delete(&mut self, key: TableKey<Bytes>, old_val: Bytes) -> StorageResult<()> {
        let span = TraceSpan::new_delete_span(key.0.clone(), old_val.clone(), self.storage_type);

        let res = self.inner.delete(key, old_val);

        span.may_send_result(OperationResult::Delete(res.as_ref().map(|o| *o).into()));

        res
    }

    async fn flush(&mut self) -> StorageResult<usize> {
        let span = TraceSpan::new_flush_span(self.storage_type);
        let res = self.inner.flush().await;
        span.may_send_result(OperationResult::Flush(
            res.as_ref().map(|o: &usize| *o).into(),
        ));
        res
    }

    fn epoch(&self) -> u64 {
        let span = TraceSpan::new_epoch_span(self.storage_type);
        let res = self.inner.epoch();
        span.may_send_result(OperationResult::LocalStorageEpoch(TraceResult::Ok(res)));
        res
    }

    fn is_dirty(&self) -> bool {
        let span = TraceSpan::new_is_dirty_span(self.storage_type);
        let res = self.inner.is_dirty();
        span.may_send_result(OperationResult::LocalStorageIsDirty(TraceResult::Ok(res)));
        res
    }

    async fn init(&mut self, options: InitOptions) -> StorageResult<()> {
        let _span =
            TraceSpan::new_local_storage_init_span(options.clone().into(), self.storage_type);
        self.inner.init(options).await
    }

    fn seal_current_epoch(&mut self, next_epoch: u64, opts: SealCurrentEpochOptions) {
        let _span = TraceSpan::new_seal_current_epoch_span(
            next_epoch,
            TracedSealCurrentEpochOptions::from(opts.clone()),
            self.storage_type,
        );
        self.inner.seal_current_epoch(next_epoch, opts)
    }

    async fn try_flush(&mut self) -> StorageResult<()> {
        let span = TraceSpan::new_try_flush_span(self.storage_type);
        let res = self.inner.try_flush().await;
        span.may_send_result(OperationResult::TryFlush(res.as_ref().map(|o| *o).into()));
        res
    }

    // TODO: add trace span
    fn update_vnode_bitmap(&mut self, vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        self.inner.update_vnode_bitmap(vnodes)
    }

    fn get_table_watermark(&self, vnode: VirtualNode) -> Option<Bytes> {
        self.inner.get_table_watermark(vnode)
    }

    fn new_flushed_snapshot_reader(&self) -> Self::FlushedSnapshotReader {
        TracedStateStore::new_with_snapshot_epoch(self.inner.new_flushed_snapshot_reader(), None)
    }
}

impl<S: StateStore> StateStore for TracedStateStore<S> {
    type Local = TracedStateStore<S::Local, Option<u64>>;
    type ReadSnapshot = TracedStateStore<S::ReadSnapshot, Option<u64>>;

    async fn try_wait_epoch(
        &self,
        epoch: HummockReadEpoch,
        options: TryWaitEpochOptions,
    ) -> StorageResult<()> {
        let span = TraceSpan::new_try_wait_epoch_span(epoch, options.clone().into());

        let res = self.inner.try_wait_epoch(epoch, options).await;
        span.may_send_result(OperationResult::TryWaitEpoch(
            res.as_ref().map(|o| *o).into(),
        ));
        res
    }

    async fn new_local(&self, options: NewLocalOptions) -> Self::Local {
        TracedStateStore::new_local(self.inner.new_local(options.clone()).await, options)
    }

    async fn new_read_snapshot(
        &self,
        epoch: HummockReadEpoch,
        options: NewReadSnapshotOptions,
    ) -> StorageResult<Self::ReadSnapshot> {
        self.inner
            .new_read_snapshot(epoch, options)
            .await
            .map(|snapshot| {
                TracedStateStore::new_with_snapshot_epoch(snapshot, Some(epoch.get_epoch()))
            })
    }
}

impl<S: StateStoreRead> StateStoreRead for TracedStateStore<S, Option<u64>> {
    type Iter = impl StateStoreReadIter;
    type RevIter = impl StateStoreReadIter;

    fn iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::Iter>> + '_ {
        let (l, r) = key_range.clone();
        let bytes_key_range = (l.map(|l| l.0), r.map(|r| r.0));
        let span = TraceSpan::new_iter_span(
            bytes_key_range,
            self.epoch(),
            read_options.clone().into(),
            self.storage_type,
        );
        self.traced_iter(self.inner.iter(key_range, read_options), span)
    }

    fn rev_iter(
        &self,
        key_range: TableKeyRange,
        read_options: ReadOptions,
    ) -> impl Future<Output = StorageResult<Self::RevIter>> + '_ {
        let (l, r) = key_range.clone();
        let bytes_key_range = (l.map(|l| l.0), r.map(|r| r.0));
        let span = TraceSpan::new_iter_span(
            bytes_key_range,
            self.epoch(),
            read_options.clone().into(),
            self.storage_type,
        );
        self.traced_iter(self.inner.rev_iter(key_range, read_options), span)
    }
}

impl<S: StateStoreReadLog> StateStoreReadLog for TracedStateStore<S> {
    type ChangeLogIter = impl StateStoreReadChangeLogIter;

    fn next_epoch(&self, epoch: u64, options: NextEpochOptions) -> impl StorageFuture<'_, u64> {
        self.inner.next_epoch(epoch, options)
    }

    fn iter_log(
        &self,
        epoch_range: (u64, u64),
        key_range: TableKeyRange,
        options: ReadLogOptions,
    ) -> impl Future<Output = StorageResult<Self::ChangeLogIter>> + Send + '_ {
        self.inner.iter_log(epoch_range, key_range, options)
    }
}

impl TracedStateStore<HummockStorage> {
    pub fn sstable_store(&self) -> SstableStoreRef {
        self.inner.sstable_store()
    }

    pub fn sstable_object_id_manager(&self) -> &SstableObjectIdManagerRef {
        self.inner.sstable_object_id_manager()
    }
}

impl<S: AsHummock> AsHummock for TracedStateStore<S> {
    fn as_hummock(&self) -> Option<&HummockStorage> {
        self.inner.as_hummock()
    }

    fn sync(
        &self,
        sync_table_epochs: Vec<(HummockEpoch, HashSet<TableId>)>,
    ) -> BoxFuture<'_, StorageResult<SyncResult>> {
        async move {
            let span: MayTraceSpan =
                TraceSpan::new_sync_span(&sync_table_epochs, self.storage_type);

            let future = self.inner.sync(sync_table_epochs);

            future
                .map(move |sync_result| {
                    span.may_send_result(OperationResult::Sync(
                        sync_result.as_ref().map(|res| res.sync_size).into(),
                    ));
                    sync_result
                })
                .await
        }
        .boxed()
    }
}

impl<S> TracedStateStore<S> {
    pub fn inner(&self) -> &S {
        &self.inner
    }
}

impl<S, E> Drop for TracedStateStore<S, E> {
    fn drop(&mut self) {
        if let StorageType::Local(_, _) = self.storage_type {
            let _ = TraceSpan::new_drop_storage_span(self.storage_type);
        }
    }
}

pub struct TracedStateStoreIter<S> {
    inner: S,
    span: MayTraceSpan,
}

impl<S> TracedStateStoreIter<S> {
    fn new(inner: S, span: MayTraceSpan) -> Self {
        TracedStateStoreIter { inner, span }
    }
}

impl<S: StateStoreIter> StateStoreIter for TracedStateStoreIter<S> {
    async fn try_next(&mut self) -> StorageResult<Option<StateStoreKeyedRowRef<'_>>> {
        if let Some((key, value)) = self
            .inner
            .try_next()
            .await
            .inspect_err(|e| tracing::error!(error = %e.as_report(), "Failed in next"))?
        {
            self.span.may_send_iter_next();
            self.span
                .may_send_result(OperationResult::IterNext(TraceResult::Ok(Some((
                    TracedBytes::from(key.user_key.table_key.to_vec()),
                    TracedBytes::from(Bytes::copy_from_slice(value)),
                )))));
            Ok(Some((key, value)))
        } else {
            Ok(None)
        }
    }
}

pub fn get_concurrent_id() -> ConcurrentId {
    LOCAL_ID.get()
}
