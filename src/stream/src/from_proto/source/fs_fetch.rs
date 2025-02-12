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

use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_connector::source::filesystem::opendal_source::{
    OpendalAzblob, OpendalGcs, OpendalPosixFs, OpendalS3,
};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::stream_plan::StreamFsFetchNode;
use risingwave_storage::StateStore;

use crate::error::StreamResult;
use crate::executor::source::{
    FsFetchExecutor, IcebergFetchExecutor, SourceStateTableHandler, StreamSourceCore,
};
use crate::executor::{Execute, Executor};
use crate::from_proto::ExecutorBuilder;
use crate::task::ExecutorParams;

pub struct FsFetchExecutorBuilder;

impl ExecutorBuilder for FsFetchExecutorBuilder {
    type Node = StreamFsFetchNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let [upstream]: [_; 1] = params.input.try_into().unwrap();

        let source = node.node_inner.as_ref().unwrap();

        let source_id = TableId::new(source.source_id);
        let source_name = source.source_name.clone();
        let source_info = source.get_info()?;
        let source_options_with_secret =
            WithOptionsSecResolved::new(source.with_properties.clone(), source.secret_refs.clone());
        let properties = ConnectorProperties::extract(source_options_with_secret.clone(), false)?;
        let source_desc_builder = SourceDescBuilder::new(
            source.columns.clone(),
            params.env.source_metrics(),
            source.row_id_index.map(|x| x as _),
            source_options_with_secret,
            source_info.clone(),
            params.env.config().developer.connector_message_buffer_size,
            params.info.pk_indices.clone(),
        );

        let source_column_ids: Vec<_> = source_desc_builder
            .column_catalogs_to_source_column_descs()
            .iter()
            .map(|column| column.column_id)
            .collect();

        let vnodes = Some(Arc::new(
            params
                .vnode_bitmap
                .expect("vnodes not set for fetch executor"),
        ));
        let state_table_handler = SourceStateTableHandler::from_table_catalog_with_vnodes(
            source.state_table.as_ref().unwrap(),
            store.clone(),
            vnodes,
        )
        .await;
        let stream_source_core = StreamSourceCore::new(
            source_id,
            source_name,
            source_column_ids,
            source_desc_builder,
            state_table_handler,
        );

        let exec = match properties {
            risingwave_connector::source::ConnectorProperties::Gcs(_) => {
                FsFetchExecutor::<_, OpendalGcs>::new(
                    params.actor_context.clone(),
                    stream_source_core,
                    upstream,
                    source.rate_limit,
                )
                .boxed()
            }
            risingwave_connector::source::ConnectorProperties::OpendalS3(_) => {
                FsFetchExecutor::<_, OpendalS3>::new(
                    params.actor_context.clone(),
                    stream_source_core,
                    upstream,
                    source.rate_limit,
                )
                .boxed()
            }
            risingwave_connector::source::ConnectorProperties::Iceberg(_) => {
                IcebergFetchExecutor::new(
                    params.actor_context.clone(),
                    stream_source_core,
                    upstream,
                    source.rate_limit,
                )
                .boxed()
            }
            risingwave_connector::source::ConnectorProperties::Azblob(_) => {
                FsFetchExecutor::<_, OpendalAzblob>::new(
                    params.actor_context.clone(),
                    stream_source_core,
                    upstream,
                    source.rate_limit,
                )
                .boxed()
            }
            risingwave_connector::source::ConnectorProperties::PosixFs(_) => {
                FsFetchExecutor::<_, OpendalPosixFs>::new(
                    params.actor_context.clone(),
                    stream_source_core,
                    upstream,
                    source.rate_limit,
                )
                .boxed()
            }
            _ => unreachable!(),
        };
        Ok((params.info, exec).into())
    }
}
