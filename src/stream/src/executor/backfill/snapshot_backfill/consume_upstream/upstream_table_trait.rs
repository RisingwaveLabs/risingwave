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

use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use futures::TryStreamExt;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::ChangeLogRow;
use risingwave_storage::StateStore;

use crate::executor::prelude::Stream;
use crate::executor::StreamExecutorResult;

pub trait UpstreamTable: Send + Sync + 'static {
    type SnapshotStream: Stream<Item = StreamExecutorResult<OwnedRow>> + Send + 'static;
    type ChangeLogStream: Stream<Item = StreamExecutorResult<ChangeLogRow>> + Send + 'static;

    fn pk_serde(&self) -> OrderedRowSerde;
    fn output_data_types(&self) -> Vec<DataType>;
    fn pk_in_output_indices(&self) -> Vec<usize>;
    fn next_epoch(&self, epoch: u64)
        -> impl Future<Output = StreamExecutorResult<u64>> + Send + '_;
    fn snapshot_stream(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        start_pk: Option<OwnedRow>,
    ) -> impl Future<Output = StreamExecutorResult<Self::SnapshotStream>> + Send + '_;
    fn change_log_stream(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        start_pk: Option<OwnedRow>,
    ) -> impl Future<Output = StreamExecutorResult<Self::ChangeLogStream>> + Send + '_;

    fn check_initial_vnode_bitmap(&self, vnodes: &Bitmap) -> StreamExecutorResult<()>;
    fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>);
}

impl<S: StateStore> UpstreamTable for StorageTable<S> {
    type ChangeLogStream = impl Stream<Item = StreamExecutorResult<ChangeLogRow>>;
    type SnapshotStream = impl Stream<Item = StreamExecutorResult<OwnedRow>>;

    fn pk_serde(&self) -> OrderedRowSerde {
        self.pk_serializer().clone()
    }

    fn output_data_types(&self) -> Vec<DataType> {
        self.schema().data_types()
    }

    fn pk_in_output_indices(&self) -> Vec<usize> {
        self.pk_in_output_indices().expect("should exist")
    }

    async fn next_epoch(&self, epoch: u64) -> StreamExecutorResult<u64> {
        self.next_epoch(epoch).await.map_err(Into::into)
    }

    async fn snapshot_stream(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        start_pk: Option<OwnedRow>,
    ) -> StreamExecutorResult<Self::SnapshotStream> {
        let stream = self
            .batch_iter_vnode(
                HummockReadEpoch::Committed(epoch),
                start_pk.as_ref(),
                vnode,
                PrefetchOptions::prefetch_for_large_range_scan(),
            )
            .await?;
        Ok(stream.map_err(Into::into))
    }

    async fn change_log_stream(
        &self,
        vnode: VirtualNode,
        epoch: u64,
        start_pk: Option<OwnedRow>,
    ) -> StreamExecutorResult<Self::ChangeLogStream> {
        let stream = self
            .batch_iter_vnode_log(
                epoch,
                HummockReadEpoch::Committed(epoch),
                start_pk.as_ref(),
                vnode,
            )
            .await?;
        Ok(stream.map_err(Into::into))
    }

    fn check_initial_vnode_bitmap(&self, vnodes: &Bitmap) -> StreamExecutorResult<()> {
        let expected_vnodes = &**self.vnodes();
        if expected_vnodes != vnodes {
            Err(anyhow!(
                "mismatch initial vnode bitmap: {:?}, expect: {:?}",
                vnodes,
                self.vnodes()
            )
            .into())
        } else {
            Ok(())
        }
    }

    fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) {
        let _ = self.update_vnode_bitmap(new_vnodes);
    }
}
