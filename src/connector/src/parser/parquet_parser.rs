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

use futures_async_stream::try_stream;
use risingwave_common::array::arrow::arrow_array_iceberg::RecordBatch;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, StreamChunk};
use risingwave_common::types::{Datum, ScalarImpl};

use crate::parser::ConnectorResult;
use crate::source::iceberg::is_parquet_schema_match_source_schema;
use crate::source::SourceColumnDesc;
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
    ) {
        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch: RecordBatch = record_batch?;
            // Convert each record batch into a stream chunk according to user defined schema.
            let chunk: StreamChunk = self.convert_record_batch_to_stream_chunk(record_batch)?;

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
