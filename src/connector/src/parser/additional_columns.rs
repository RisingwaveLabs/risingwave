// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
use risingwave_common::types::{DataType, StructType};
use risingwave_pb::plan_common::AdditionalColumnType;

use crate::source::{
    GCS_CONNECTOR, KAFKA_CONNECTOR, KINESIS_CONNECTOR, OPENDAL_S3_CONNECTOR, PULSAR_CONNECTOR,
    S3_CONNECTOR,
};

pub type CompatibleAdditionalColumnsFn =
    Box<dyn Fn(ColumnId, &str) -> ColumnCatalog + Send + Sync + 'static>;

pub fn get_connector_compatible_additional_columns(
    connector_name: &str,
) -> Option<Vec<(&'static str, CompatibleAdditionalColumnsFn)>> {
    let compatible_columns = match connector_name {
        KAFKA_CONNECTOR => kafka_compatible_column_vec(),
        PULSAR_CONNECTOR => pulsar_compatible_column_vec(),
        KINESIS_CONNECTOR => kinesis_compatible_column_vec(),
        OPENDAL_S3_CONNECTOR | S3_CONNECTOR | GCS_CONNECTOR => s3_compatible_column_column_vec(),
        _ => return None,
    };
    Some(compatible_columns)
}

fn kafka_compatible_column_vec() -> Vec<(&'static str, CompatibleAdditionalColumnsFn)> {
    vec![
        (
            "key",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Bytea,
                        AdditionalColumnType::Key,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "timestamp",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Timestamptz,
                        AdditionalColumnType::Timestamp,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "partition",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Partition,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "offset",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Offset,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "header", // type: struct<key varchar, value bytea>[]
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::List(get_kafka_header_item_datatype().into()),
                        AdditionalColumnType::Header,
                    ),
                    is_hidden: false,
                }
            }),
        ),
    ]
}

pub fn get_kafka_header_item_datatype() -> DataType {
    let struct_inner = vec![("key", DataType::Varchar), ("value", DataType::Bytea)];
    DataType::Struct(StructType::new(struct_inner))
}

fn pulsar_compatible_column_vec() -> Vec<(&'static str, CompatibleAdditionalColumnsFn)> {
    vec![
        (
            "key",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Bytea,
                        AdditionalColumnType::Key,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "partition",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Partition,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "offset",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Offset,
                    ),
                    is_hidden: false,
                }
            }),
        ),
    ]
}

fn kinesis_compatible_column_vec() -> Vec<(&'static str, CompatibleAdditionalColumnsFn)> {
    vec![
        (
            "key",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Bytea,
                        AdditionalColumnType::Key,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "partition",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Partition,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "offset",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Offset,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "timestamp",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Timestamptz,
                        AdditionalColumnType::Timestamp,
                    ),
                    is_hidden: false,
                }
            }),
        ),
    ]
}

fn s3_compatible_column_column_vec() -> Vec<(&'static str, CompatibleAdditionalColumnsFn)> {
    vec![
        (
            "file",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Filename,
                    ),
                    is_hidden: false,
                }
            }),
        ),
        (
            "offset",
            Box::new(|id: ColumnId, name: &str| -> ColumnCatalog {
                ColumnCatalog {
                    column_desc: ColumnDesc::named_with_additional_column(
                        name,
                        id,
                        DataType::Varchar,
                        AdditionalColumnType::Offset,
                    ),
                    is_hidden: false,
                }
            }),
        ),
    ]
}
