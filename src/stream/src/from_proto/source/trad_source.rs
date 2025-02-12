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

use risingwave_common::catalog::{
    default_key_column_name_version_mapping, KAFKA_TIMESTAMP_COLUMN_NAME,
};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::should_copy_to_format_encode_options;
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt};
use risingwave_pb::data::data_type::TypeName as PbTypeName;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use risingwave_pb::plan_common::{
    AdditionalColumn, AdditionalColumnKey, AdditionalColumnTimestamp,
    AdditionalColumnType as LegacyAdditionalColumnType, ColumnDescVersion, FormatType,
    PbColumnCatalog, PbEncodeType,
};
use risingwave_pb::stream_plan::SourceNode;
use risingwave_storage::panic_store::PanicStateStore;

use super::*;
use crate::executor::source::{
    FsListExecutor, IcebergListExecutor, SourceExecutor, SourceStateTableHandler, StreamSourceCore,
};
use crate::executor::TroublemakerExecutor;

pub struct SourceExecutorBuilder;

pub fn create_source_desc_builder(
    source_type: &str, // "source" or "source backfill"
    source_id: &TableId,
    mut source_columns: Vec<PbColumnCatalog>,
    params: &ExecutorParams,
    source_info: PbStreamSourceInfo,
    row_id_index: Option<u32>,
    with_properties: WithOptionsSecResolved,
) -> SourceDescBuilder {
    {
        // compatible code: introduced in https://github.com/risingwavelabs/risingwave/pull/13707
        // for upsert and (avro | protobuf) overwrite the `_rw_key` column's ColumnDesc.additional_column_type to Key
        if source_info.format() == FormatType::Upsert
            && (source_info.row_encode() == PbEncodeType::Avro
                || source_info.row_encode() == PbEncodeType::Protobuf
                || source_info.row_encode() == PbEncodeType::Json)
        {
            for c in &mut source_columns {
                if let Some(desc) = c.column_desc.as_mut() {
                    let is_bytea = desc
                        .get_column_type()
                        .map(|col_type| col_type.type_name == PbTypeName::Bytea as i32)
                        .unwrap();
                    if desc.name == default_key_column_name_version_mapping(
                        &desc.version()
                    )
                        && is_bytea
                        // the column is from a legacy version (before v1.5.x)
                        && desc.version == ColumnDescVersion::Unspecified as i32
                    {
                        desc.additional_column = Some(AdditionalColumn {
                            column_type: Some(AdditionalColumnType::Key(AdditionalColumnKey {})),
                        });
                    }

                    // the column is from a legacy version (v1.6.x)
                    // introduced in https://github.com/risingwavelabs/risingwave/pull/15226
                    if desc.additional_column_type == LegacyAdditionalColumnType::Key as i32 {
                        desc.additional_column = Some(AdditionalColumn {
                            column_type: Some(AdditionalColumnType::Key(AdditionalColumnKey {})),
                        });
                    }
                }
            }
        }
    }

    {
        // compatible code: handle legacy column `_rw_kafka_timestamp`
        // the column is auto added for all kafka source to empower batch query on source
        // solution: rewrite the column `additional_column` to Timestamp

        let _ = source_columns.iter_mut().map(|c| {
            let _ = c.column_desc.as_mut().map(|desc| {
                let is_timestamp = desc
                    .get_column_type()
                    .map(|col_type| col_type.type_name == PbTypeName::Timestamptz as i32)
                    .unwrap();
                if desc.name == KAFKA_TIMESTAMP_COLUMN_NAME
                    && is_timestamp
                    // the column is from a legacy version
                    && desc.version == ColumnDescVersion::Unspecified as i32
                {
                    desc.additional_column = Some(AdditionalColumn {
                        column_type: Some(AdditionalColumnType::Timestamp(
                            AdditionalColumnTimestamp {},
                        )),
                    });
                }
            });
        });
    }

    telemetry_source_build(source_type, source_id, &source_info, &with_properties);

    SourceDescBuilder::new(
        source_columns.clone(),
        params.env.source_metrics(),
        row_id_index.map(|x| x as _),
        with_properties,
        source_info,
        params.env.config().developer.connector_message_buffer_size,
        // `pk_indices` is used to ensure that a message will be skipped instead of parsed
        // with null pk when the pk column is missing.
        //
        // Currently pk_indices for source is always empty since pk information is not
        // passed via `StreamSource` so null pk may be emitted to downstream.
        //
        // TODO: use the correct information to fill in pk_dicies.
        // We should consdier add back the "pk_column_ids" field removed by #8841 in
        // StreamSource
        params.info.pk_indices.clone(),
    )
}

impl ExecutorBuilder for SourceExecutorBuilder {
    type Node = SourceNode;

    async fn new_boxed_executor(
        params: ExecutorParams,
        node: &Self::Node,
        store: impl StateStore,
    ) -> StreamResult<Executor> {
        let barrier_receiver = params
            .local_barrier_manager
            .subscribe_barrier(params.actor_context.id);
        let system_params = params.env.system_params_manager_ref().get_params();

        if let Some(source) = &node.source_inner {
            let exec = {
                let source_id = TableId::new(source.source_id);
                let source_name = source.source_name.clone();
                let mut source_info = source.get_info()?.clone();

                if source_info.format_encode_options.is_empty() {
                    // compatible code: quick fix for <https://github.com/risingwavelabs/risingwave/issues/14755>,
                    // will move the logic to FragmentManager::init in release 1.7.
                    let connector = get_connector_name(&source.with_properties);
                    source_info.format_encode_options.extend(
                        source.with_properties.iter().filter_map(|(k, v)| {
                            should_copy_to_format_encode_options(k, &connector)
                                .then_some((k.to_owned(), v.to_owned()))
                        }),
                    );
                }

                let with_properties = WithOptionsSecResolved::new(
                    source.with_properties.clone(),
                    source.secret_refs.clone(),
                );

                let source_desc_builder = create_source_desc_builder(
                    "source",
                    &source_id,
                    source.columns.clone(),
                    &params,
                    source_info,
                    source.row_id_index,
                    with_properties,
                );

                let source_column_ids: Vec<_> = source_desc_builder
                    .column_catalogs_to_source_column_descs()
                    .iter()
                    .map(|column| column.column_id)
                    .collect();

                let state_table_handler = SourceStateTableHandler::from_table_catalog(
                    source.state_table.as_ref().unwrap(),
                    store.clone(),
                )
                .await;
                let stream_source_core = StreamSourceCore::new(
                    source_id,
                    source_name,
                    source_column_ids,
                    source_desc_builder,
                    state_table_handler,
                );

                if source.with_properties.is_legacy_fs_connector() {
                    #[expect(deprecated)]
                    crate::executor::source::LegacyFsSourceExecutor::new(
                        params.actor_context.clone(),
                        stream_source_core,
                        params.executor_stats,
                        barrier_receiver,
                        system_params,
                        source.rate_limit,
                    )?
                    .boxed()
                } else if source.with_properties.is_new_fs_connector() {
                    FsListExecutor::new(
                        params.actor_context.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        source.rate_limit,
                    )
                    .boxed()
                } else if source.with_properties.is_iceberg_connector() {
                    IcebergListExecutor::new(
                        params.actor_context.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        source.rate_limit,
                    )
                    .boxed()
                } else {
                    let is_shared = source.info.as_ref().is_some_and(|info| info.is_shared());
                    SourceExecutor::new(
                        params.actor_context.clone(),
                        Some(stream_source_core),
                        params.executor_stats.clone(),
                        barrier_receiver,
                        system_params,
                        source.rate_limit,
                        is_shared && !source.with_properties.is_cdc_connector(),
                    )
                    .boxed()
                }
            };

            if crate::consistency::insane() {
                let mut info = params.info.clone();
                info.identity = format!("{} (troubled)", info.identity);
                Ok((
                    params.info,
                    TroublemakerExecutor::new(
                        (info, exec).into(),
                        params.env.config().developer.chunk_size,
                    ),
                )
                    .into())
            } else {
                Ok((params.info, exec).into())
            }
        } else {
            // If there is no external stream source, then no data should be persisted. We pass a
            // `PanicStateStore` type here for indication.
            let exec = SourceExecutor::<PanicStateStore>::new(
                params.actor_context,
                None,
                params.executor_stats,
                barrier_receiver,
                system_params,
                None,
                false,
            );
            Ok((params.info, exec).into())
        }
    }
}
