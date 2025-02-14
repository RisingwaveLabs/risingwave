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

use core::fmt::Debug;
use core::num::NonZeroU64;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use clickhouse::insert::Insert;
use clickhouse::{Client as ClickHouseClient, Row as ClickHouseRow};
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Decimal, ScalarRefImpl, Serial};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::Serialize;
use serde_derive::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use thiserror_ext::AsReport;
use tonic::async_trait;
use tracing::warn;
use with_options::WithOptions;

use super::decouple_checkpoint_log_sink::{
    default_commit_checkpoint_interval, DecoupleCheckpointLogSinkerOf,
};
use super::writer::SinkWriter;
use super::{DummySinkCommitCoordinator, SinkWriterMetrics, SinkWriterParam};
use crate::error::ConnectorResult;
use crate::sink::{
    Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

const QUERY_ENGINE: &str =
    "select distinct ?fields from system.tables where database = ? and name = ?";
const QUERY_COLUMN: &str =
    "select distinct ?fields from system.columns where database = ? and table = ? order by ?";
pub const CLICKHOUSE_SINK: &str = "clickhouse";

#[serde_as]
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct ClickHouseCommon {
    #[serde(rename = "clickhouse.url")]
    pub url: String,
    #[serde(rename = "clickhouse.user")]
    pub user: String,
    #[serde(rename = "clickhouse.password")]
    pub password: String,
    #[serde(rename = "clickhouse.database")]
    pub database: String,
    #[serde(rename = "clickhouse.table")]
    pub table: String,
    #[serde(rename = "clickhouse.delete.column")]
    pub delete_column: Option<String>,
    /// Commit every n(>0) checkpoints, default is 10.
    #[serde(default = "default_commit_checkpoint_interval")]
    #[serde_as(as = "DisplayFromStr")]
    pub commit_checkpoint_interval: u64,
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum ClickHouseEngine {
    MergeTree,
    ReplacingMergeTree(Option<String>),
    SummingMergeTree,
    AggregatingMergeTree,
    CollapsingMergeTree(String),
    VersionedCollapsingMergeTree(String),
    GraphiteMergeTree,
    ReplicatedMergeTree,
    ReplicatedReplacingMergeTree(Option<String>),
    ReplicatedSummingMergeTree,
    ReplicatedAggregatingMergeTree,
    ReplicatedCollapsingMergeTree(String),
    ReplicatedVersionedCollapsingMergeTree(String),
    ReplicatedGraphiteMergeTree,
    SharedMergeTree,
    SharedReplacingMergeTree(Option<String>),
    SharedSummingMergeTree,
    SharedAggregatingMergeTree,
    SharedCollapsingMergeTree(String),
    SharedVersionedCollapsingMergeTree(String),
    SharedGraphiteMergeTree,
    Null,
}
impl ClickHouseEngine {
    pub fn is_collapsing_engine(&self) -> bool {
        matches!(
            self,
            ClickHouseEngine::CollapsingMergeTree(_)
                | ClickHouseEngine::VersionedCollapsingMergeTree(_)
                | ClickHouseEngine::ReplicatedCollapsingMergeTree(_)
                | ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedVersionedCollapsingMergeTree(_)
        )
    }

    pub fn is_delete_replacing_engine(&self) -> bool {
        match self {
            ClickHouseEngine::ReplacingMergeTree(delete_col) => delete_col.is_some(),
            ClickHouseEngine::ReplicatedReplacingMergeTree(delete_col) => delete_col.is_some(),
            ClickHouseEngine::SharedReplacingMergeTree(delete_col) => delete_col.is_some(),
            _ => false,
        }
    }

    pub fn get_delete_col(&self) -> Option<String> {
        match self {
            ClickHouseEngine::ReplacingMergeTree(Some(delete_col)) => Some(delete_col.to_string()),
            ClickHouseEngine::ReplicatedReplacingMergeTree(Some(delete_col)) => {
                Some(delete_col.to_string())
            }
            ClickHouseEngine::SharedReplacingMergeTree(Some(delete_col)) => {
                Some(delete_col.to_string())
            }
            _ => None,
        }
    }

    pub fn get_sign_name(&self) -> Option<String> {
        match self {
            ClickHouseEngine::CollapsingMergeTree(sign_name) => Some(sign_name.to_string()),
            ClickHouseEngine::VersionedCollapsingMergeTree(sign_name) => {
                Some(sign_name.to_string())
            }
            ClickHouseEngine::ReplicatedCollapsingMergeTree(sign_name) => {
                Some(sign_name.to_string())
            }
            ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(sign_name) => {
                Some(sign_name.to_string())
            }
            ClickHouseEngine::SharedCollapsingMergeTree(sign_name) => Some(sign_name.to_string()),
            ClickHouseEngine::SharedVersionedCollapsingMergeTree(sign_name) => {
                Some(sign_name.to_string())
            }
            _ => None,
        }
    }

    pub fn is_shared_tree(&self) -> bool {
        matches!(
            self,
            ClickHouseEngine::SharedMergeTree
                | ClickHouseEngine::SharedReplacingMergeTree(_)
                | ClickHouseEngine::SharedSummingMergeTree
                | ClickHouseEngine::SharedAggregatingMergeTree
                | ClickHouseEngine::SharedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedVersionedCollapsingMergeTree(_)
                | ClickHouseEngine::SharedGraphiteMergeTree
        )
    }

    pub fn from_query_engine(
        engine_name: &ClickhouseQueryEngine,
        config: &ClickHouseConfig,
    ) -> Result<Self> {
        match engine_name.engine.as_str() {
            "MergeTree" => Ok(ClickHouseEngine::MergeTree),
            "Null" => Ok(ClickHouseEngine::Null),
            "ReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::ReplacingMergeTree(delete_column))
            }
            "SummingMergeTree" => Ok(ClickHouseEngine::SummingMergeTree),
            "AggregatingMergeTree" => Ok(ClickHouseEngine::AggregatingMergeTree),
            // VersionedCollapsingMergeTree(sign_name,"a")
            "VersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("VersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::VersionedCollapsingMergeTree(sign_name))
            }
            // CollapsingMergeTree(sign_name)
            "CollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("CollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::CollapsingMergeTree(sign_name))
            }
            "GraphiteMergeTree" => Ok(ClickHouseEngine::GraphiteMergeTree),
            "ReplicatedMergeTree" => Ok(ClickHouseEngine::ReplicatedMergeTree),
            "ReplicatedReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::ReplicatedReplacingMergeTree(
                    delete_column,
                ))
            }
            "ReplicatedSummingMergeTree" => Ok(ClickHouseEngine::ReplicatedSummingMergeTree),
            "ReplicatedAggregatingMergeTree" => {
                Ok(ClickHouseEngine::ReplicatedAggregatingMergeTree)
            }
            // ReplicatedVersionedCollapsingMergeTree("a","b",sign_name,"c")
            "ReplicatedVersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("ReplicatedVersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .rev()
                    .nth(1)
                    .ok_or_else(|| SinkError::ClickHouse("must have index 1".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::ReplicatedVersionedCollapsingMergeTree(
                    sign_name,
                ))
            }
            // ReplicatedCollapsingMergeTree("a","b",sign_name)
            "ReplicatedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("ReplicatedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .split(',')
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::ReplicatedCollapsingMergeTree(sign_name))
            }
            "ReplicatedGraphiteMergeTree" => Ok(ClickHouseEngine::ReplicatedGraphiteMergeTree),
            "SharedMergeTree" => Ok(ClickHouseEngine::SharedMergeTree),
            "SharedReplacingMergeTree" => {
                let delete_column = config.common.delete_column.clone();
                Ok(ClickHouseEngine::SharedReplacingMergeTree(delete_column))
            }
            "SharedSummingMergeTree" => Ok(ClickHouseEngine::SharedSummingMergeTree),
            "SharedAggregatingMergeTree" => Ok(ClickHouseEngine::SharedAggregatingMergeTree),
            // SharedVersionedCollapsingMergeTree("a","b",sign_name,"c")
            "SharedVersionedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("SharedVersionedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(',')
                    .rev()
                    .nth(1)
                    .ok_or_else(|| SinkError::ClickHouse("must have index 1".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::SharedVersionedCollapsingMergeTree(
                    sign_name,
                ))
            }
            // SharedCollapsingMergeTree("a","b",sign_name)
            "SharedCollapsingMergeTree" => {
                let sign_name = engine_name
                    .create_table_query
                    .split("SharedCollapsingMergeTree(")
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .split(')')
                    .next()
                    .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                    .split(',')
                    .last()
                    .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                    .trim()
                    .to_owned();
                Ok(ClickHouseEngine::SharedCollapsingMergeTree(sign_name))
            }
            "SharedGraphiteMergeTree" => Ok(ClickHouseEngine::SharedGraphiteMergeTree),
            _ => Err(SinkError::ClickHouse(format!(
                "Cannot find clickhouse engine {:?}",
                engine_name.engine
            ))),
        }
    }
}

impl ClickHouseCommon {
    pub(crate) fn build_client(&self) -> ConnectorResult<ClickHouseClient> {
        let client = ClickHouseClient::default() // hyper(0.14) client inside
            .with_url(&self.url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database);
        Ok(client)
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct ClickHouseConfig {
    #[serde(flatten)]
    pub common: ClickHouseCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

#[derive(Clone, Debug)]
pub struct ClickHouseSink {
    pub config: ClickHouseConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl ClickHouseConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<ClickHouseConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

impl TryFrom<SinkParam> for ClickHouseSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = ClickHouseConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

impl ClickHouseSink {
    /// Check that the column names and types of risingwave and clickhouse are identical
    fn check_column_name_and_type(&self, clickhouse_columns_desc: &[SystemColumn]) -> Result<()> {
        let rw_fields_name = build_fields_name_type_from_schema(&self.schema)?;
        let clickhouse_columns_desc: HashMap<String, SystemColumn> = clickhouse_columns_desc
            .iter()
            .map(|s| (s.name.clone(), s.clone()))
            .collect();

        if rw_fields_name.len().gt(&clickhouse_columns_desc.len()) {
            return Err(SinkError::ClickHouse("The columns of the sink must be equal to or a superset of the target table's columns.".to_owned()));
        }

        for i in rw_fields_name {
            let value = clickhouse_columns_desc.get(&i.0).ok_or_else(|| {
                SinkError::ClickHouse(format!(
                    "Column name don't find in clickhouse, risingwave is {:?} ",
                    i.0
                ))
            })?;

            Self::check_and_correct_column_type(&i.1, value)?;
        }
        Ok(())
    }

    /// Check that the column names and types of risingwave and clickhouse are identical
    fn check_pk_match(&self, clickhouse_columns_desc: &[SystemColumn]) -> Result<()> {
        let mut clickhouse_pks: HashSet<String> = clickhouse_columns_desc
            .iter()
            .filter(|s| s.is_in_primary_key == 1)
            .map(|s| s.name.clone())
            .collect();

        for (_, field) in self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| self.pk_indices.contains(index))
        {
            if !clickhouse_pks.remove(&field.name) {
                return Err(SinkError::ClickHouse(
                    "Clicklhouse and RisingWave pk is not match".to_owned(),
                ));
            }
        }

        if !clickhouse_pks.is_empty() {
            return Err(SinkError::ClickHouse(
                "Clicklhouse and RisingWave pk is not match".to_owned(),
            ));
        }
        Ok(())
    }

    /// Check that the column types of risingwave and clickhouse are identical
    fn check_and_correct_column_type(
        fields_type: &DataType,
        ck_column: &SystemColumn,
    ) -> Result<()> {
        // FIXME: the "contains" based implementation is wrong
        let is_match = match fields_type {
            risingwave_common::types::DataType::Boolean => Ok(ck_column.r#type.contains("Bool")),
            risingwave_common::types::DataType::Int16 => Ok(ck_column.r#type.contains("UInt16")
                | ck_column.r#type.contains("Int16")
                // Allow Int16 to be pushed to Enum16, they share an encoding and value range
                // No special care is taken to ensure values are valid.
                | ck_column.r#type.contains("Enum16")),
            risingwave_common::types::DataType::Int32 => {
                Ok(ck_column.r#type.contains("UInt32") | ck_column.r#type.contains("Int32"))
            }
            risingwave_common::types::DataType::Int64 => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Float32 => Ok(ck_column.r#type.contains("Float32")),
            risingwave_common::types::DataType::Float64 => Ok(ck_column.r#type.contains("Float64")),
            risingwave_common::types::DataType::Decimal => Ok(ck_column.r#type.contains("Decimal")),
            risingwave_common::types::DataType::Date => Ok(ck_column.r#type.contains("Date32")),
            risingwave_common::types::DataType::Varchar => Ok(ck_column.r#type.contains("String")),
            risingwave_common::types::DataType::Time => Err(SinkError::ClickHouse(
                "clickhouse can not support Time".to_owned(),
            )),
            risingwave_common::types::DataType::Timestamp => Err(SinkError::ClickHouse(
                "clickhouse does not have a type corresponding to naive timestamp".to_owned(),
            )),
            risingwave_common::types::DataType::Timestamptz => {
                Ok(ck_column.r#type.contains("DateTime64"))
            }
            risingwave_common::types::DataType::Interval => Err(SinkError::ClickHouse(
                "clickhouse can not support Interval".to_owned(),
            )),
            risingwave_common::types::DataType::Struct(_) => Err(SinkError::ClickHouse(
                "struct needs to be converted into a list".to_owned(),
            )),
            risingwave_common::types::DataType::List(list) => {
                Self::check_and_correct_column_type(list.as_ref(), ck_column)?;
                Ok(ck_column.r#type.contains("Array"))
            }
            risingwave_common::types::DataType::Bytea => Err(SinkError::ClickHouse(
                "clickhouse can not support Bytea".to_owned(),
            )),
            risingwave_common::types::DataType::Jsonb => Err(SinkError::ClickHouse(
                "clickhouse rust can not support Json".to_owned(),
            )),
            risingwave_common::types::DataType::Serial => {
                Ok(ck_column.r#type.contains("UInt64") | ck_column.r#type.contains("Int64"))
            }
            risingwave_common::types::DataType::Int256 => Err(SinkError::ClickHouse(
                "clickhouse can not support Int256".to_owned(),
            )),
            risingwave_common::types::DataType::Map(_) => Err(SinkError::ClickHouse(
                "clickhouse can not support Map".to_owned(),
            )),
        };
        if !is_match? {
            return Err(SinkError::ClickHouse(format!(
                "Column type can not match name is {:?}, risingwave is {:?} and clickhouse is {:?}",
                ck_column.name, fields_type, ck_column.r#type
            )));
        }

        Ok(())
    }
}
impl Sink for ClickHouseSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = DecoupleCheckpointLogSinkerOf<ClickHouseSinkWriter>;

    const SINK_NAME: &'static str = CLICKHOUSE_SINK;

    async fn validate(&self) -> Result<()> {
        // For upsert clickhouse sink, the primary key must be defined.
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "Primary key not defined for upsert clickhouse sink (please define in `primary_key` field)")));
        }

        // check reachability
        let client = self.config.common.build_client()?;

        let (clickhouse_column, clickhouse_engine) =
            query_column_engine_from_ck(client, &self.config).await?;
        if clickhouse_engine.is_shared_tree() {
            risingwave_common::license::Feature::ClickHouseSharedEngine
                .check_available()
                .map_err(|e| anyhow::anyhow!(e))?;
        }

        if !self.is_append_only
            && !clickhouse_engine.is_collapsing_engine()
            && !clickhouse_engine.is_delete_replacing_engine()
        {
            return match clickhouse_engine {
                ClickHouseEngine::ReplicatedReplacingMergeTree(None) | ClickHouseEngine::ReplacingMergeTree(None) | ClickHouseEngine::SharedReplacingMergeTree(None) =>  {
                    Err(SinkError::ClickHouse("To enable upsert with a `ReplacingMergeTree`, you must set a `clickhouse.delete.column` to the UInt8 column in ClickHouse used to signify deletes. See https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replacingmergetree#is_deleted for more information".to_owned()))
                }
                _ => Err(SinkError::ClickHouse("If you want to use upsert, please use either `VersionedCollapsingMergeTree`, `CollapsingMergeTree` or the `ReplacingMergeTree` in ClickHouse".to_owned()))
            };
        }

        self.check_column_name_and_type(&clickhouse_column)?;
        if !self.is_append_only {
            self.check_pk_match(&clickhouse_column)?;
        }

        if self.config.common.commit_checkpoint_interval == 0 {
            return Err(SinkError::Config(anyhow!(
                "`commit_checkpoint_interval` must be greater than 0"
            )));
        }
        Ok(())
    }

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        let writer = ClickHouseSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await?;
        let commit_checkpoint_interval =
    NonZeroU64::new(self.config.common.commit_checkpoint_interval).expect(
        "commit_checkpoint_interval should be greater than 0, and it should be checked in config validation",
    );

        Ok(DecoupleCheckpointLogSinkerOf::new(
            writer,
            SinkWriterMetrics::new(&writer_param),
            commit_checkpoint_interval,
        ))
    }
}
pub struct ClickHouseSinkWriter {
    pub config: ClickHouseConfig,
    #[expect(dead_code)]
    schema: Schema,
    #[expect(dead_code)]
    pk_indices: Vec<usize>,
    client: ClickHouseClient,
    #[expect(dead_code)]
    is_append_only: bool,
    // Save some features of the clickhouse column type
    column_correct_vec: Vec<ClickHouseSchemaFeature>,
    rw_fields_name_after_calibration: Vec<String>,
    clickhouse_engine: ClickHouseEngine,
    inserter: Option<Insert<ClickHouseColumn>>,
}
#[derive(Debug)]
struct ClickHouseSchemaFeature {
    can_null: bool,
    // Time accuracy in clickhouse for rw and ck conversions
    accuracy_time: u8,

    accuracy_decimal: (u8, u8),
}

impl ClickHouseSinkWriter {
    pub async fn new(
        config: ClickHouseConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        let client = config.common.build_client()?;

        let (clickhouse_column, clickhouse_engine) =
            query_column_engine_from_ck(client.clone(), &config).await?;

        let column_correct_vec: Result<Vec<ClickHouseSchemaFeature>> = clickhouse_column
            .iter()
            .map(Self::build_column_correct_vec)
            .collect();
        let mut rw_fields_name_after_calibration = build_fields_name_type_from_schema(&schema)?
            .iter()
            .map(|(a, _)| a.clone())
            .collect_vec();

        if let Some(sign) = clickhouse_engine.get_sign_name() {
            rw_fields_name_after_calibration.push(sign);
        }
        if let Some(delete_col) = clickhouse_engine.get_delete_col() {
            rw_fields_name_after_calibration.push(delete_col);
        }
        Ok(Self {
            config,
            schema,
            pk_indices,
            client,
            is_append_only,
            column_correct_vec: column_correct_vec?,
            rw_fields_name_after_calibration,
            clickhouse_engine,
            inserter: None,
        })
    }

    /// Check if clickhouse's column is 'Nullable', valid bits of `DateTime64`. And save it in
    /// `column_correct_vec`
    fn build_column_correct_vec(ck_column: &SystemColumn) -> Result<ClickHouseSchemaFeature> {
        let can_null = ck_column.r#type.contains("Nullable");
        // `DateTime64` without precision is already displayed as `DateTime(3)` in `system.columns`.
        let accuracy_time = if ck_column.r#type.contains("DateTime64(") {
            ck_column
                .r#type
                .split("DateTime64(")
                .last()
                .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                .split(')')
                .next()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .split(',')
                .next()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .parse::<u8>()
                .map_err(|e| SinkError::ClickHouse(e.to_report_string()))?
        } else {
            0_u8
        };
        let accuracy_decimal = if ck_column.r#type.contains("Decimal(") {
            let decimal_all = ck_column
                .r#type
                .split("Decimal(")
                .last()
                .ok_or_else(|| SinkError::ClickHouse("must have last".to_owned()))?
                .split(')')
                .next()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .split(", ")
                .collect_vec();
            let length = decimal_all
                .first()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .parse::<u8>()
                .map_err(|e| SinkError::ClickHouse(e.to_report_string()))?;

            if length > 38 {
                return Err(SinkError::ClickHouse(
                    "RW don't support Decimal256".to_owned(),
                ));
            }

            let scale = decimal_all
                .last()
                .ok_or_else(|| SinkError::ClickHouse("must have next".to_owned()))?
                .parse::<u8>()
                .map_err(|e| SinkError::ClickHouse(e.to_report_string()))?;
            (length, scale)
        } else {
            (0_u8, 0_u8)
        };
        Ok(ClickHouseSchemaFeature {
            can_null,
            accuracy_time,
            accuracy_decimal,
        })
    }

    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        if self.inserter.is_none() {
            self.inserter = Some(self.client.insert_with_fields_name(
                &self.config.common.table,
                self.rw_fields_name_after_calibration.clone(),
            )?);
        }
        for (op, row) in chunk.rows() {
            let mut clickhouse_filed_vec = vec![];
            for (index, data) in row.iter().enumerate() {
                clickhouse_filed_vec.extend(ClickHouseFieldWithNull::from_scalar_ref(
                    data,
                    &self.column_correct_vec,
                    index,
                )?);
            }
            match op {
                Op::Insert | Op::UpdateInsert => {
                    if self.clickhouse_engine.is_collapsing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(1),
                        ));
                    }
                    if self.clickhouse_engine.is_delete_replacing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(0),
                        ))
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    if !self.clickhouse_engine.is_collapsing_engine()
                        && !self.clickhouse_engine.is_delete_replacing_engine()
                    {
                        return Err(SinkError::ClickHouse(
                            "Clickhouse engine don't support upsert".to_owned(),
                        ));
                    }
                    if self.clickhouse_engine.is_collapsing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(-1),
                        ));
                    }
                    if self.clickhouse_engine.is_delete_replacing_engine() {
                        clickhouse_filed_vec.push(ClickHouseFieldWithNull::WithoutSome(
                            ClickHouseField::Int8(1),
                        ))
                    }
                }
            }
            let clickhouse_column = ClickHouseColumn {
                row: clickhouse_filed_vec,
            };
            self.inserter
                .as_mut()
                .unwrap()
                .write(&clickhouse_column)
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for ClickHouseSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.write(chunk).await
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, is_checkpoint: bool) -> Result<()> {
        if is_checkpoint && let Some(inserter) = self.inserter.take() {
            inserter.end().await?;
        }
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }
}

#[derive(ClickHouseRow, Deserialize, Clone)]
struct SystemColumn {
    name: String,
    r#type: String,
    is_in_primary_key: u8,
}

#[derive(ClickHouseRow, Deserialize)]
struct ClickhouseQueryEngine {
    #[expect(dead_code)]
    name: String,
    engine: String,
    create_table_query: String,
}

async fn query_column_engine_from_ck(
    client: ClickHouseClient,
    config: &ClickHouseConfig,
) -> Result<(Vec<SystemColumn>, ClickHouseEngine)> {
    let query_engine = QUERY_ENGINE;
    let query_column = QUERY_COLUMN;

    let clickhouse_engine = client
        .query(query_engine)
        .bind(config.common.database.clone())
        .bind(config.common.table.clone())
        .fetch_all::<ClickhouseQueryEngine>()
        .await?;
    let mut clickhouse_column = client
        .query(query_column)
        .bind(config.common.database.clone())
        .bind(config.common.table.clone())
        .bind("position")
        .fetch_all::<SystemColumn>()
        .await?;
    if clickhouse_engine.is_empty() || clickhouse_column.is_empty() {
        return Err(SinkError::ClickHouse(format!(
            "table {:?}.{:?} is not find in clickhouse",
            config.common.database, config.common.table
        )));
    }

    let clickhouse_engine =
        ClickHouseEngine::from_query_engine(clickhouse_engine.first().unwrap(), config)?;

    if let Some(sign) = &clickhouse_engine.get_sign_name() {
        clickhouse_column.retain(|a| sign.ne(&a.name))
    }

    if let Some(delete_col) = &clickhouse_engine.get_delete_col() {
        clickhouse_column.retain(|a| delete_col.ne(&a.name))
    }

    Ok((clickhouse_column, clickhouse_engine))
}

/// Serialize this structure to simulate the `struct` call clickhouse interface
#[derive(ClickHouseRow, Debug)]
struct ClickHouseColumn {
    row: Vec<ClickHouseFieldWithNull>,
}

/// Basic data types for use with the clickhouse interface
#[derive(Debug)]
enum ClickHouseField {
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Serial(Serial),
    Float32(f32),
    Float64(f64),
    String(String),
    Bool(bool),
    List(Vec<ClickHouseFieldWithNull>),
    Int8(i8),
    Decimal(ClickHouseDecimal),
}
#[derive(Debug)]
enum ClickHouseDecimal {
    Decimal32(i32),
    Decimal64(i64),
    Decimal128(i128),
}
impl Serialize for ClickHouseDecimal {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseDecimal::Decimal32(v) => serializer.serialize_i32(*v),
            ClickHouseDecimal::Decimal64(v) => serializer.serialize_i64(*v),
            ClickHouseDecimal::Decimal128(v) => serializer.serialize_i128(*v),
        }
    }
}

/// Enum that support clickhouse nullable
#[derive(Debug)]
enum ClickHouseFieldWithNull {
    WithSome(ClickHouseField),
    WithoutSome(ClickHouseField),
    None,
}

impl ClickHouseFieldWithNull {
    pub fn from_scalar_ref(
        data: Option<ScalarRefImpl<'_>>,
        clickhouse_schema_feature_vec: &Vec<ClickHouseSchemaFeature>,
        clickhouse_schema_feature_index: usize,
    ) -> Result<Vec<ClickHouseFieldWithNull>> {
        let clickhouse_schema_feature = clickhouse_schema_feature_vec
            .get(clickhouse_schema_feature_index)
            .ok_or_else(|| SinkError::ClickHouse(format!("No column found from clickhouse table schema, index is {clickhouse_schema_feature_index}")))?;
        if data.is_none() {
            if !clickhouse_schema_feature.can_null {
                return Err(SinkError::ClickHouse(
                    "clickhouse column can not insert null".to_owned(),
                ));
            } else {
                return Ok(vec![ClickHouseFieldWithNull::None]);
            }
        }
        let data = match data.unwrap() {
            ScalarRefImpl::Int16(v) => ClickHouseField::Int16(v),
            ScalarRefImpl::Int32(v) => ClickHouseField::Int32(v),
            ScalarRefImpl::Int64(v) => ClickHouseField::Int64(v),
            ScalarRefImpl::Int256(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse can not support Int256".to_owned(),
                ))
            }
            ScalarRefImpl::Serial(v) => ClickHouseField::Serial(v),
            ScalarRefImpl::Float32(v) => ClickHouseField::Float32(v.into_inner()),
            ScalarRefImpl::Float64(v) => ClickHouseField::Float64(v.into_inner()),
            ScalarRefImpl::Utf8(v) => ClickHouseField::String(v.to_owned()),
            ScalarRefImpl::Bool(v) => ClickHouseField::Bool(v),
            ScalarRefImpl::Decimal(d) => {
                let d = if let Decimal::Normalized(d) = d {
                    let scale =
                        clickhouse_schema_feature.accuracy_decimal.1 as i32 - d.scale() as i32;
                    if scale < 0 {
                        d.mantissa() / 10_i128.pow(scale.unsigned_abs())
                    } else {
                        d.mantissa() * 10_i128.pow(scale as u32)
                    }
                } else if clickhouse_schema_feature.can_null {
                    warn!("Inf, -Inf, Nan in RW decimal is converted into clickhouse null!");
                    return Ok(vec![ClickHouseFieldWithNull::None]);
                } else {
                    warn!("Inf, -Inf, Nan in RW decimal is converted into clickhouse 0!");
                    0_i128
                };
                if clickhouse_schema_feature.accuracy_decimal.0 <= 9 {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal32(d as i32))
                } else if clickhouse_schema_feature.accuracy_decimal.0 <= 18 {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal64(d as i64))
                } else {
                    ClickHouseField::Decimal(ClickHouseDecimal::Decimal128(d))
                }
            }
            ScalarRefImpl::Interval(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse can not support Interval".to_owned(),
                ))
            }
            ScalarRefImpl::Date(v) => {
                let days = v.get_nums_days_unix_epoch();
                ClickHouseField::Int32(days)
            }
            ScalarRefImpl::Time(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse can not support Time".to_owned(),
                ))
            }
            ScalarRefImpl::Timestamp(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse does not have a type corresponding to naive timestamp".to_owned(),
                ))
            }
            ScalarRefImpl::Timestamptz(v) => {
                let micros = v.timestamp_micros();
                let ticks = match clickhouse_schema_feature.accuracy_time <= 6 {
                    true => {
                        micros / 10_i64.pow((6 - clickhouse_schema_feature.accuracy_time).into())
                    }
                    false => micros
                        .checked_mul(
                            10_i64.pow((clickhouse_schema_feature.accuracy_time - 6).into()),
                        )
                        .ok_or_else(|| SinkError::ClickHouse("DateTime64 overflow".to_owned()))?,
                };
                ClickHouseField::Int64(ticks)
            }
            ScalarRefImpl::Jsonb(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse rust interface can not support Json".to_owned(),
                ))
            }
            ScalarRefImpl::Struct(v) => {
                let mut struct_vec = vec![];
                for (index, field) in v.iter_fields_ref().enumerate() {
                    let a = Self::from_scalar_ref(
                        field,
                        clickhouse_schema_feature_vec,
                        clickhouse_schema_feature_index + index,
                    )?;
                    struct_vec.push(ClickHouseFieldWithNull::WithoutSome(ClickHouseField::List(
                        a,
                    )));
                }
                return Ok(struct_vec);
            }
            ScalarRefImpl::List(v) => {
                let mut vec = vec![];
                for i in v.iter() {
                    vec.extend(Self::from_scalar_ref(
                        i,
                        clickhouse_schema_feature_vec,
                        clickhouse_schema_feature_index,
                    )?)
                }
                return Ok(vec![ClickHouseFieldWithNull::WithoutSome(
                    ClickHouseField::List(vec),
                )]);
            }
            ScalarRefImpl::Bytea(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse can not support Bytea".to_owned(),
                ))
            }
            ScalarRefImpl::Map(_) => {
                return Err(SinkError::ClickHouse(
                    "clickhouse can not support Map".to_owned(),
                ))
            }
        };
        let data = if clickhouse_schema_feature.can_null {
            vec![ClickHouseFieldWithNull::WithSome(data)]
        } else {
            vec![ClickHouseFieldWithNull::WithoutSome(data)]
        };
        Ok(data)
    }
}

impl Serialize for ClickHouseField {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseField::Int16(v) => serializer.serialize_i16(*v),
            ClickHouseField::Int32(v) => serializer.serialize_i32(*v),
            ClickHouseField::Int64(v) => serializer.serialize_i64(*v),
            ClickHouseField::Serial(v) => v.serialize(serializer),
            ClickHouseField::Float32(v) => serializer.serialize_f32(*v),
            ClickHouseField::Float64(v) => serializer.serialize_f64(*v),
            ClickHouseField::String(v) => serializer.serialize_str(v),
            ClickHouseField::Bool(v) => serializer.serialize_bool(*v),
            ClickHouseField::List(v) => {
                let mut s = serializer.serialize_seq(Some(v.len()))?;
                for i in v {
                    s.serialize_element(i)?;
                }
                s.end()
            }
            ClickHouseField::Decimal(v) => v.serialize(serializer),
            ClickHouseField::Int8(v) => serializer.serialize_i8(*v),
        }
    }
}
impl Serialize for ClickHouseFieldWithNull {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ClickHouseFieldWithNull::WithSome(v) => serializer.serialize_some(v),
            ClickHouseFieldWithNull::WithoutSome(v) => v.serialize(serializer),
            ClickHouseFieldWithNull::None => serializer.serialize_none(),
        }
    }
}
impl Serialize for ClickHouseColumn {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut s = serializer.serialize_struct("useless", self.row.len())?;
        for data in &self.row {
            s.serialize_field("useless", &data)?
        }
        s.end()
    }
}

/// 'Struct'(clickhouse type name is nested) will be converted into some arrays by clickhouse. So we
/// need to make some conversions
pub fn build_fields_name_type_from_schema(schema: &Schema) -> Result<Vec<(String, DataType)>> {
    let mut vec = vec![];
    for field in schema.fields() {
        if matches!(field.data_type, DataType::Struct(_)) {
            for i in &field.sub_fields() {
                if matches!(i.data_type, DataType::Struct(_)) {
                    return Err(SinkError::ClickHouse(
                        "Only one level of nesting is supported for struct".to_owned(),
                    ));
                } else {
                    vec.push((
                        format!("{}.{}", field.name, i.name),
                        DataType::List(Box::new(i.data_type())),
                    ))
                }
            }
        } else {
            vec.push((field.name.clone(), field.data_type()));
        }
    }
    Ok(vec)
}
