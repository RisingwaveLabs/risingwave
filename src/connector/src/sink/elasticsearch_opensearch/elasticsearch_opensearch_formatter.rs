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

use anyhow::anyhow;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::util::iter_util::ZipEqDebug;
use serde_json::{Map, Value};

use super::super::encoder::template::TemplateEncoder;
use super::super::encoder::{JsonEncoder, RowEncoder};
use super::super::SinkError;
use crate::sink::Result;

pub struct ElasticSearchOpenSearchFormatter {
    key_encoder: TemplateEncoder,
    value_encoder: JsonEncoder,
    index_column: Option<usize>,
    index: Option<String>,
    routing_column: Option<usize>,
}

pub struct BuildBulkPara {
    pub index: String,
    pub key: String,
    pub value: Option<Map<String, Value>>,
    pub mem_size_b: usize,
    pub routing_column: Option<String>,
}

impl ElasticSearchOpenSearchFormatter {
    pub fn new(
        pk_indices: Vec<usize>,
        schema: &Schema,
        delimiter: Option<String>,
        index_column: Option<usize>,
        index: Option<String>,
        routing_column: Option<usize>,
    ) -> Result<Self> {
        let key_format = if pk_indices.is_empty() {
            let name = &schema
                .fields()
                .get(0)
                .ok_or_else(|| {
                    SinkError::ElasticSearchOpenSearch(anyhow!(
                        "no value find in sink schema, index is 0"
                    ))
                })?
                .name;
            format!("{{{}}}", name)
        } else if pk_indices.len() == 1 {
            let index = *pk_indices.get(0).unwrap();
            let name = &schema
                .fields()
                .get(index)
                .ok_or_else(|| {
                    SinkError::ElasticSearchOpenSearch(anyhow!(
                        "no value find in sink schema, index is {:?}",
                        index
                    ))
                })?
                .name;
            format!("{{{}}}", name)
        } else {
            let delimiter = delimiter
                .as_ref()
                .ok_or_else(|| anyhow!("please set the separator in the with option, when there are multiple primary key values"))?
                .clone();
            let mut names = Vec::with_capacity(pk_indices.len());
            for index in &pk_indices {
                names.push(format!(
                    "{{{}}}",
                    schema
                        .fields()
                        .get(*index)
                        .ok_or_else(|| {
                            SinkError::ElasticSearchOpenSearch(anyhow!(
                                "no value find in sink schema, index is {:?}",
                                index
                            ))
                        })?
                        .name
                ));
            }
            names.join(&delimiter)
        };
        let col_indices = if let Some(index) = index_column {
            let mut col_indices: Vec<usize> = (0..schema.len()).collect();
            col_indices.remove(index);
            Some(col_indices)
        } else {
            None
        };
        let key_encoder = TemplateEncoder::new(schema.clone(), col_indices.clone(), key_format);
        let value_encoder = JsonEncoder::new_with_es(schema.clone(), col_indices.clone());
        Ok(Self {
            key_encoder,
            value_encoder,
            index_column,
            index,
            routing_column,
        })
    }

    pub fn convert_chunk(
        &self,
        chunk: StreamChunk,
        is_append_only: bool,
    ) -> Result<Vec<BuildBulkPara>> {
        let mut update_delete_row: Option<(String, RowRef<'_>)> = None;
        let mut result_vec = Vec::with_capacity(chunk.capacity());
        for (op, rows) in chunk.rows() {
            let index = if let Some(index_column) = self.index_column {
                rows.datum_at(index_column)
                    .ok_or_else(|| {
                        SinkError::ElasticSearchOpenSearch(anyhow!(
                            "no value find in sink schema, index is {:?}",
                            index_column
                        ))
                    })?
                    .into_utf8()
            } else {
                self.index.as_ref().unwrap()
            };
            let routing_column = self
                .routing_column
                .map(|routing_column| {
                    Ok::<String, SinkError>(
                        rows.datum_at(routing_column)
                            .ok_or_else(|| {
                                SinkError::ElasticSearchOpenSearch(anyhow!(
                                    "no value find in sink schema, index is {:?}",
                                    routing_column
                                ))
                            })?
                            .into_utf8()
                            .to_owned(),
                    )
                })
                .transpose()?;
            match op {
                Op::Insert | Op::UpdateInsert => {
                    let key = self.key_encoder.encode(rows)?;
                    let mut modified_col_indices = Vec::new();
                    if let Some((delete_key, delete_row)) = update_delete_row.take()
                        && delete_key == key
                    {
                        delete_row
                            .iter()
                            .enumerate()
                            .zip_eq_debug(rows.iter())
                            .for_each(|((index, delete_column), insert_column)| {
                                if let Some(insert_column) = insert_column
                                    && let Some(delete_column) = delete_column
                                    && insert_column == delete_column
                                {
                                    // do nothing
                                } else {
                                    modified_col_indices.push(index);
                                }
                            });
                    } else {
                        modified_col_indices = (0..self.value_encoder.schema().len()).collect();
                    }
                    let value = self
                        .value_encoder
                        .encode_cols(rows, modified_col_indices.into_iter())?;
                    result_vec.push(BuildBulkPara {
                        index: index.to_owned(),
                        key,
                        value: Some(value),
                        mem_size_b: rows.value_estimate_size(),
                        routing_column,
                    });
                }
                Op::Delete => {
                    if is_append_only {
                        return Err(SinkError::ElasticSearchOpenSearch(anyhow!(
                            "`Delete` operation is not supported in `append_only` mode"
                        )));
                    }
                    let key = self.key_encoder.encode(rows)?;
                    let mem_size_b = std::mem::size_of_val(&key);
                    result_vec.push(BuildBulkPara {
                        index: index.to_owned(),
                        key,
                        value: None,
                        mem_size_b,
                        routing_column,
                    });
                }
                Op::UpdateDelete => {
                    if is_append_only {
                        return Err(SinkError::ElasticSearchOpenSearch(anyhow!(
                            "`UpdateDelete` operation is not supported in `append_only` mode"
                        )));
                    } else {
                        let key = self.key_encoder.encode(rows)?;
                        update_delete_row = Some((key, rows));
                    }
                }
            }
        }
        Ok(result_vec)
    }
}
