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
use std::future::IntoFuture;
use std::sync::Arc;

use deltalake::parquet::arrow::async_reader::AsyncFileReader;
use futures_async_stream::try_stream;
use risingwave_common::array::arrow::arrow_array_iceberg::RecordBatch;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, StreamChunk};
use risingwave_common::bail;
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_common::util::tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::parser::ConnectorResult;
use crate::source::filesystem::opendal_source::opendal_enumerator::OpendalEnumerator;
use crate::source::filesystem::opendal_source::{OpendalGcs, OpendalPosixFs, OpendalS3};
use crate::source::iceberg::is_parquet_schema_match_source_schema;
use crate::source::reader::desc::SourceDesc;
use crate::source::{ConnectorProperties, SourceColumnDesc};
/// `ParquetParser` is responsible for converting the incoming `record_batch_stream`
/// into a `streamChunk`.
#[derive(Debug)]
pub struct ParquetParser {
    rw_columns: Vec<SourceColumnDesc>,
    file_name: String,
    offset: usize,
}

impl ParquetParser {
    pub fn new(
        rw_columns: Vec<SourceColumnDesc>,
        file_name: String,
        offset: usize,
    ) -> ConnectorResult<Self> {
        Ok(Self {
            rw_columns,
            file_name,
            offset,
        })
    }

    #[try_stream(boxed, ok = StreamChunk, error = crate::error::ConnectorError)]
    pub async fn into_stream(
        mut self,
        record_batch_stream: parquet::arrow::async_reader::ParquetRecordBatchStream<
            tokio_util::compat::Compat<opendal::FuturesAsyncReader>,
        >,
        parquet_source_skip_row_count_metrics: Option<
            risingwave_common::metrics::LabelGuardedMetric<
                prometheus::core::GenericCounter<prometheus::core::AtomicU64>,
                5,
            >,
        >,
    ) {
        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch: RecordBatch = record_batch?;
            // Convert each record batch into a stream chunk according to user defined schema.
            let chunk: StreamChunk = self.convert_record_batch_to_stream_chunk(
                record_batch,
                parquet_source_skip_row_count_metrics.clone(),
            )?;

            yield chunk;
        }
    }

    fn inc_offset(&mut self) {
        self.offset += 1;
    }

    /// The function `convert_record_batch_to_stream_chunk` is designed to transform the given `RecordBatch` into a `StreamChunk`.
    ///
    /// For each column in the source column:
    /// - If the column's schema matches a column in the `RecordBatch` (both the data type and column name are the same),
    ///   the corresponding records are converted into a column of the `StreamChunk`.
    /// - If the column's schema does not match, null values are inserted.
    /// - Hidden columns are handled separately by filling in the appropriate fields to ensure the data chunk maintains the correct format.
    /// - If a column in the Parquet file does not exist in the source schema, it is skipped.
    ///
    /// # Arguments
    ///
    /// * `record_batch` - The `RecordBatch` to be converted into a `StreamChunk`.
    ///
    /// # Returns
    ///
    /// A `StreamChunk` containing the converted data from the `RecordBatch`.
    ///
    /// The hidden columns that must be included here are `_rw_file` and `_rw_offset`.
    /// Depending on whether the user specifies a primary key (pk), there may be an additional hidden column `row_id`.
    /// Therefore, the maximum number of hidden columns is three.
    fn convert_record_batch_to_stream_chunk(
        &mut self,
        record_batch: RecordBatch,
        parquet_source_skip_row_count_metrics: Option<
            risingwave_common::metrics::LabelGuardedMetric<
                prometheus::core::GenericCounter<prometheus::core::AtomicU64>,
                5,
            >,
        >,
    ) -> Result<StreamChunk, crate::error::ConnectorError> {
        const MAX_HIDDEN_COLUMN_NUMS: usize = 3;
        let column_size = self.rw_columns.len();
        let mut chunk_columns = Vec::with_capacity(self.rw_columns.len() + MAX_HIDDEN_COLUMN_NUMS);

        for source_column in self.rw_columns.clone() {
            match source_column.column_type {
                crate::source::SourceColumnType::Normal => {
                    match source_column.is_hidden_addition_col {
                        false => {
                            let rw_data_type: &risingwave_common::types::DataType =
                                &source_column.data_type;
                            let rw_column_name = &source_column.name;

                            if let Some(parquet_column) =
                                record_batch.column_by_name(rw_column_name)
                                && is_parquet_schema_match_source_schema(
                                    parquet_column.data_type(),
                                    rw_data_type,
                                )
                            {
                                let arrow_field = IcebergArrowConvert
                                    .to_arrow_field(rw_column_name, rw_data_type)?;
                                let array_impl = IcebergArrowConvert
                                    .array_from_arrow_array(&arrow_field, parquet_column)?;
                                let column = Arc::new(array_impl);
                                chunk_columns.push(column);
                            } else {
                                // For columns defined in the source schema but not present in the Parquet file, null values are filled in.
                                let mut array_builder =
                                    ArrayBuilderImpl::with_type(column_size, rw_data_type.clone());
                                if let Some(metrics) = parquet_source_skip_row_count_metrics.clone()
                                {
                                    metrics.inc_by(record_batch.num_rows() as u64);
                                }

                                array_builder.append_n_null(record_batch.num_rows());
                                let res = array_builder.finish();
                                let column = Arc::new(res);
                                chunk_columns.push(column);
                            }
                        }
                        // handle hidden columns, for file source, the hidden columns are only `Offset` and `Filename`
                        true => {
                            if let Some(additional_column_type) =
                                &source_column.additional_column.column_type
                            {
                                match additional_column_type{
                                risingwave_pb::plan_common::additional_column::ColumnType::Offset(_) =>{
                                    let mut array_builder =
                                    ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                    for _ in 0..record_batch.num_rows(){
                                        let datum: Datum = Some(ScalarImpl::Utf8((self.offset).to_string().into()));
                                        self.inc_offset();
                                        array_builder.append(datum);
                                    }
                                    let res = array_builder.finish();
                                    let column = Arc::new(res);
                                    chunk_columns.push(column);

                                },
                                risingwave_pb::plan_common::additional_column::ColumnType::Filename(_) => {
                                    let mut array_builder =
                                    ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                                    let datum: Datum =  Some(ScalarImpl::Utf8(self.file_name.clone().into()));
                                    array_builder.append_n(record_batch.num_rows(), datum);
                                    let res = array_builder.finish();
                                    let column = Arc::new(res);
                                    chunk_columns.push(column);
                                },
                                _ => unreachable!()
                            }
                            }
                        }
                    }
                }
                crate::source::SourceColumnType::RowId => {
                    let mut array_builder =
                        ArrayBuilderImpl::with_type(column_size, source_column.data_type.clone());
                    let datum: Datum = None;
                    array_builder.append_n(record_batch.num_rows(), datum);
                    let res = array_builder.finish();
                    let column = Arc::new(res);
                    chunk_columns.push(column);
                }
                // The following fields is only used in CDC source
                crate::source::SourceColumnType::Offset | crate::source::SourceColumnType::Meta => {
                    unreachable!()
                }
            }
        }

        let data_chunk = DataChunk::new(chunk_columns.clone(), record_batch.num_rows());
        Ok(data_chunk.into())
    }
}

/// Retrieves the total number of rows in the specified Parquet file.
///
/// This function constructs an `OpenDAL` operator using the information
/// from the provided `source_desc`. It then accesses the metadata of the
/// Parquet file to determine and return the total row count.
///
/// # Arguments
///
/// * `file_name` - The parquet file name.
/// * `source_desc` - A struct or type containing the necessary information
///                   to construct the `OpenDAL` operator.
///
/// # Returns
///
/// Returns the total number of rows in the Parquet file as a `usize`.
pub async fn get_total_row_nums_for_parquet_file(
    parquet_file_name: &str,
    source_desc: SourceDesc,
) -> ConnectorResult<usize> {
    let total_row_num = match source_desc.source.config {
        ConnectorProperties::Gcs(prop) => {
            let connector: OpendalEnumerator<OpendalGcs> =
                OpendalEnumerator::new_gcs_source(*prop)?;
            let mut reader = connector
                .op
                .reader_with(parquet_file_name)
                .into_future()
                .await?
                .into_futures_async_read(..)
                .await?
                .compat();

            reader
                .get_metadata()
                .await
                .map_err(anyhow::Error::from)?
                .file_metadata()
                .num_rows()
        }
        ConnectorProperties::OpendalS3(prop) => {
            let connector: OpendalEnumerator<OpendalS3> =
                OpendalEnumerator::new_s3_source(prop.s3_properties, prop.assume_role)?;
            let mut reader = connector
                .op
                .reader_with(parquet_file_name)
                .into_future()
                .await?
                .into_futures_async_read(..)
                .await?
                .compat();
            reader
                .get_metadata()
                .await
                .map_err(anyhow::Error::from)?
                .file_metadata()
                .num_rows()
        }

        ConnectorProperties::PosixFs(prop) => {
            let connector: OpendalEnumerator<OpendalPosixFs> =
                OpendalEnumerator::new_posix_fs_source(*prop)?;
            let mut reader = connector
                .op
                .reader_with(parquet_file_name)
                .into_future()
                .await?
                .into_futures_async_read(..)
                .await?
                .compat();
            reader
                .get_metadata()
                .await
                .map_err(anyhow::Error::from)?
                .file_metadata()
                .num_rows()
        }
        other => bail!("Unsupported source: {:?}", other),
    };
    Ok(total_row_num as usize)
}
