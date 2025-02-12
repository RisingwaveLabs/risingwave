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

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use iceberg::spec::TableMetadataRef;
use itertools::Itertools;
use risingwave_common::array::{ArrayImpl, DataChunk, I64Array, Utf8Array};
use risingwave_common::catalog::{
    Field, Schema, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME,
};
use risingwave_common::types::DataType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::source::iceberg::{
    scan_task_to_chunk, IcebergFileScanTask, IcebergProperties, IcebergSplit,
};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::error::BatchError;
use crate::executor::Executor;
use crate::monitor::BatchMetrics;

pub struct IcebergScanExecutor {
    iceberg_config: IcebergProperties,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadataRef,
    file_scan_tasks: Option<IcebergFileScanTask>,
    batch_size: usize,
    schema: Schema,
    identity: String,
    metrics: Option<BatchMetrics>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
}

impl Executor for IcebergScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergScanExecutor {
    pub fn new(
        iceberg_config: IcebergProperties,
        snapshot_id: Option<i64>,
        table_meta: TableMetadataRef,
        file_scan_tasks: IcebergFileScanTask,
        batch_size: usize,
        schema: Schema,
        identity: String,
        metrics: Option<BatchMetrics>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            batch_size,
            schema,
            file_scan_tasks: Some(file_scan_tasks),
            identity,
            metrics,
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self
            .iceberg_config
            .load_table_with_metadata(self.table_meta)
            .await?;
        let data_types = self.schema.data_types();
        let table_name = table.identifier().name().to_owned();

        let data_file_scan_tasks = match Option::take(&mut self.file_scan_tasks) {
            Some(IcebergFileScanTask::Data(data_file_scan_tasks)) => data_file_scan_tasks,
            Some(IcebergFileScanTask::EqualityDelete(equality_delete_file_scan_tasks)) => {
                equality_delete_file_scan_tasks
            }
            Some(IcebergFileScanTask::PositionDelete(position_delete_file_scan_tasks)) => {
                position_delete_file_scan_tasks
            }
            None => {
                bail!("file_scan_tasks must be Some")
            }
        };

        // let mut read_bytes = 0;
        // let _metrics_report_guard = scopeguard::guard(
        //     (read_bytes, table_name, self.metrics.clone()),
        //     |(read_bytes, table_name, metrics)| {
        //         if let Some(metrics) = metrics {
        //             metrics
        //                 .iceberg_scan_metrics()
        //                 .iceberg_read_bytes
        //                 .with_guarded_label_values(&[&table_name])
        //                 .inc_by(read_bytes as _);
        //         }
        //     },
        // );
        for data_file_scan_task in data_file_scan_tasks {
            #[for_await]
            for chunk in scan_task_to_chunk(
                table.clone(),
                data_file_scan_task,
                self.batch_size,
                // self.schema.clone(),
                self.need_seq_num,
                self.need_file_path_and_pos,
            ) {
                yield chunk?;
            }
        }
    }
}

pub struct IcebergScanExecutorBuilder {}

impl BoxedExecutorBuilder for IcebergScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Iceberg source should not have input executor!"
        );
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergScan
        )?;

        // prepare connector source
        let options_with_secret = WithOptionsSecResolved::new(
            source_node.with_properties.clone(),
            source_node.secret_refs.clone(),
        );
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)?;

        let split_list = source_node
            .split
            .iter()
            .map(|split| SplitImpl::restore_from_bytes(split).unwrap())
            .collect_vec();
        assert_eq!(split_list.len(), 1);

        let fields = source_node
            .columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);
        let metrics = source.context().batch_metrics().clone();

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            let need_seq_num = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
            let need_file_path_and_pos = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_FILE_PATH_COLUMN_NAME)
                && matches!(split.task, IcebergFileScanTask::Data(_));

            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties,
                // Some(split.snapshot_id),
                None,
                split.table_meta.clone(),
                split.task,
                source.context().get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
                metrics,
                need_seq_num,
                need_file_path_and_pos,
            )))
        } else {
            unreachable!()
        }
    }
}
