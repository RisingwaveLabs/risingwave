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
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_connector::source::iceberg::{
    extract_bucket_and_file_name, new_s3_operator, read_parquet_file, FileScanBackend,
};
use risingwave_pb::batch_plan::file_scan_node;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::BatchError;
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

#[derive(PartialEq, Debug)]
pub enum FileFormat {
    Parquet,
}

/// S3 file scan executor. Currently only support parquet file format.
pub struct S3FileScanExecutor {
    file_format: FileFormat,
    file_location: Vec<String>,
    s3_region: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_endpoint: String,
    batch_size: usize,
    schema: Schema,
    identity: String,
}

impl Executor for S3FileScanExecutor {
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

impl S3FileScanExecutor {
    pub fn new(
        file_format: FileFormat,
        file_location: Vec<String>,
        s3_region: String,
        s3_access_key: String,
        s3_secret_key: String,
        batch_size: usize,
        schema: Schema,
        identity: String,
        s3_endpoint: String,
    ) -> Self {
        Self {
            file_format,
            file_location,
            s3_region,
            s3_access_key,
            s3_secret_key,
            s3_endpoint,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        assert_eq!(self.file_format, FileFormat::Parquet);
        for file in self.file_location {
            let (bucket, file_name) = extract_bucket_and_file_name(&file, &FileScanBackend::S3)?;
            let op = new_s3_operator(
                self.s3_region.clone(),
                self.s3_access_key.clone(),
                self.s3_secret_key.clone(),
                bucket.clone(),
                self.s3_endpoint.clone(),
            )?;
            let chunk_stream =
                read_parquet_file(op, file_name, None, None, self.batch_size, 0, None).await?;
            #[for_await]
            for stream_chunk in chunk_stream {
                let stream_chunk = stream_chunk?;
                let (data_chunk, _) = stream_chunk.into_parts();
                yield data_chunk;
            }
        }
    }
}

pub struct FileScanExecutorBuilder {}

impl BoxedExecutorBuilder for FileScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        _inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        let file_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::FileScan
        )?;

        Ok(Box::new(S3FileScanExecutor::new(
            match file_scan_node::FileFormat::try_from(file_scan_node.file_format).unwrap() {
                file_scan_node::FileFormat::Parquet => FileFormat::Parquet,
                file_scan_node::FileFormat::Unspecified => unreachable!(),
            },
            file_scan_node.file_location.clone(),
            file_scan_node.s3_region.clone(),
            file_scan_node.s3_access_key.clone(),
            file_scan_node.s3_secret_key.clone(),
            source.context().get_config().developer.chunk_size,
            Schema::from_iter(file_scan_node.columns.iter().map(Field::from)),
            source.plan_node().get_identity().clone(),
            file_scan_node.s3_endpoint.clone(),
        )))
    }
}
