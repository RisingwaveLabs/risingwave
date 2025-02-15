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

// for derived code of `Message`
#![expect(clippy::doc_markdown)]
#![expect(clippy::upper_case_acronyms)]
#![expect(clippy::needless_lifetimes)]
// For tonic::transport::Endpoint::connect
#![expect(clippy::disallowed_methods)]
#![expect(clippy::enum_variant_names)]
#![expect(clippy::module_inception)]
// FIXME: This should be fixed!!! https://github.com/risingwavelabs/risingwave/issues/19906
#![expect(clippy::large_enum_variant)]

use std::collections::HashMap;
use std::str::FromStr;

use plan_common::AdditionalColumn;
pub use prost::Message;
use risingwave_error::tonic::ToTonicStatus;
use thiserror::Error;

use crate::common::WorkerType;
use crate::stream_plan::PbStreamScanType;

#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/catalog.rs")]
pub mod catalog;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/common.rs")]
pub mod common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/compute.rs")]
pub mod compute;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/cloud_service.rs")]
pub mod cloud_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/data.rs")]
pub mod data;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/ddl_service.rs")]
pub mod ddl_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/expr.rs")]
pub mod expr;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/meta.rs")]
pub mod meta;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/plan_common.rs")]
pub mod plan_common;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/batch_plan.rs")]
pub mod batch_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/task_service.rs")]
pub mod task_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/connector_service.rs")]
pub mod connector_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_plan.rs")]
pub mod stream_plan;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/stream_service.rs")]
pub mod stream_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/hummock.rs")]
pub mod hummock;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/compactor.rs")]
pub mod compactor;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/user.rs")]
pub mod user;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/source.rs")]
pub mod source;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/monitor_service.rs")]
pub mod monitor_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/backup_service.rs")]
pub mod backup_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/frontend_service.rs")]
pub mod frontend_service;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/java_binding.rs")]
pub mod java_binding;
#[rustfmt::skip]
#[cfg_attr(madsim, path = "sim/health.rs")]
pub mod health;
#[rustfmt::skip]
#[path = "sim/telemetry.rs"]
pub mod telemetry;

#[rustfmt::skip]
#[path = "sim/secret.rs"]
pub mod secret;
#[rustfmt::skip]
#[path = "connector_service.serde.rs"]
pub mod connector_service_serde;
#[rustfmt::skip]
#[path = "catalog.serde.rs"]
pub mod catalog_serde;
#[rustfmt::skip]
#[path = "common.serde.rs"]
pub mod common_serde;
#[rustfmt::skip]
#[path = "compute.serde.rs"]
pub mod compute_serde;
#[rustfmt::skip]
#[path = "cloud_service.serde.rs"]
pub mod cloud_service_serde;
#[rustfmt::skip]
#[path = "data.serde.rs"]
pub mod data_serde;
#[rustfmt::skip]
#[path = "ddl_service.serde.rs"]
pub mod ddl_service_serde;
#[rustfmt::skip]
#[path = "expr.serde.rs"]
pub mod expr_serde;
#[rustfmt::skip]
#[path = "meta.serde.rs"]
pub mod meta_serde;
#[rustfmt::skip]
#[path = "plan_common.serde.rs"]
pub mod plan_common_serde;
#[rustfmt::skip]
#[path = "batch_plan.serde.rs"]
pub mod batch_plan_serde;
#[rustfmt::skip]
#[path = "task_service.serde.rs"]
pub mod task_service_serde;
#[rustfmt::skip]
#[path = "stream_plan.serde.rs"]
pub mod stream_plan_serde;
#[rustfmt::skip]
#[path = "stream_service.serde.rs"]
pub mod stream_service_serde;
#[rustfmt::skip]
#[path = "hummock.serde.rs"]
pub mod hummock_serde;
#[rustfmt::skip]
#[path = "compactor.serde.rs"]
pub mod compactor_serde;
#[rustfmt::skip]
#[path = "user.serde.rs"]
pub mod user_serde;
#[rustfmt::skip]
#[path = "source.serde.rs"]
pub mod source_serde;
#[rustfmt::skip]
#[path = "monitor_service.serde.rs"]
pub mod monitor_service_serde;
#[rustfmt::skip]
#[path = "backup_service.serde.rs"]
pub mod backup_service_serde;
#[rustfmt::skip]
#[path = "java_binding.serde.rs"]
pub mod java_binding_serde;
#[rustfmt::skip]
#[path = "telemetry.serde.rs"]
pub mod telemetry_serde;

#[rustfmt::skip]
#[path = "secret.serde.rs"]
pub mod secret_serde;

#[derive(Clone, PartialEq, Eq, Debug, Error)]
#[error("field `{0}` not found")]
pub struct PbFieldNotFound(pub &'static str);

impl From<PbFieldNotFound> for tonic::Status {
    fn from(e: PbFieldNotFound) -> Self {
        e.to_status_unnamed(tonic::Code::Internal)
    }
}

impl FromStr for crate::expr::table_function::PbType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}

impl FromStr for crate::expr::agg_call::PbKind {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_str_name(&s.to_uppercase()).ok_or(())
    }
}

impl stream_plan::MaterializeNode {
    pub fn dist_key_indices(&self) -> Vec<u32> {
        self.get_table()
            .unwrap()
            .distribution_key
            .iter()
            .map(|i| *i as u32)
            .collect()
    }

    pub fn column_ids(&self) -> Vec<i32> {
        self.get_table()
            .unwrap()
            .columns
            .iter()
            .map(|c| c.get_column_desc().unwrap().column_id)
            .collect()
    }
}

// Encapsulating the use of parallelism.
impl common::WorkerNode {
    pub fn compute_node_parallelism(&self) -> usize {
        assert_eq!(self.r#type(), WorkerType::ComputeNode);
        self.property
            .as_ref()
            .expect("property should be exist")
            .parallelism as usize
    }

    pub fn parallelism(&self) -> Option<usize> {
        if WorkerType::ComputeNode == self.r#type() {
            Some(self.compute_node_parallelism())
        } else {
            None
        }
    }

    pub fn resource_group(&self) -> Option<String> {
        self.property
            .as_ref()
            .and_then(|p| p.resource_group.clone())
    }
}

impl stream_plan::SourceNode {
    pub fn column_ids(&self) -> Option<Vec<i32>> {
        Some(
            self.source_inner
                .as_ref()?
                .columns
                .iter()
                .map(|c| c.get_column_desc().unwrap().column_id)
                .collect(),
        )
    }
}

impl meta::table_fragments::ActorStatus {
    pub fn worker_id(&self) -> u32 {
        self.location
            .as_ref()
            .expect("actor location should be exist")
            .worker_node_id
    }
}

impl common::WorkerNode {
    pub fn is_streaming_schedulable(&self) -> bool {
        let property = self.property.as_ref();
        property.map_or(false, |p| p.is_streaming)
            && !property.map_or(false, |p| p.is_unschedulable)
    }
}

impl common::ActorLocation {
    pub fn from_worker(worker_node_id: u32) -> Option<Self> {
        Some(Self { worker_node_id })
    }
}

impl stream_plan::StreamNode {
    /// Find the external stream source info inside the stream node, if any.
    ///
    /// Returns `source_id`.
    pub fn find_stream_source(&self) -> Option<u32> {
        if let Some(crate::stream_plan::stream_node::NodeBody::Source(source)) =
            self.node_body.as_ref()
        {
            if let Some(inner) = &source.source_inner {
                return Some(inner.source_id);
            }
        }

        for child in &self.input {
            if let Some(source) = child.find_stream_source() {
                return Some(source);
            }
        }

        None
    }

    /// Find the external stream source info inside the stream node, if any.
    ///
    /// Returns (`source_id`, `upstream_source_fragment_id`).
    ///
    /// Note: we must get upstream fragment id from the merge node, not from the fragment's
    /// `upstream_fragment_ids`. e.g., DynamicFilter may have 2 upstream fragments, but only
    /// one is the upstream source fragment.
    pub fn find_source_backfill(&self) -> Option<(u32, u32)> {
        if let Some(crate::stream_plan::stream_node::NodeBody::SourceBackfill(source)) =
            self.node_body.as_ref()
        {
            if let crate::stream_plan::stream_node::NodeBody::Merge(merge) =
                self.input[0].node_body.as_ref().unwrap()
            {
                // Note: avoid using `merge.upstream_actor_id` to prevent misuse.
                // See comments there for details.
                return Some((source.upstream_source_id, merge.upstream_fragment_id));
            } else {
                unreachable!(
                    "source backfill must have a merge node as its input: {:?}",
                    self
                );
            }
        }

        for child in &self.input {
            if let Some(source) = child.find_source_backfill() {
                return Some(source);
            }
        }

        None
    }
}

impl stream_plan::FragmentTypeFlag {
    /// Fragments that may be affected by `BACKFILL_RATE_LIMIT`.
    pub fn backfill_rate_limit_fragments() -> i32 {
        stream_plan::FragmentTypeFlag::SourceScan as i32
            | stream_plan::FragmentTypeFlag::StreamScan as i32
    }

    /// Fragments that may be affected by `SOURCE_RATE_LIMIT`.
    /// Note: for `FsFetch`, old fragments don't have this flag set, so don't use this to check.
    pub fn source_rate_limit_fragments() -> i32 {
        stream_plan::FragmentTypeFlag::Source as i32 | stream_plan::FragmentTypeFlag::FsFetch as i32
    }

    /// Fragments that may be affected by `BACKFILL_RATE_LIMIT`.
    pub fn sink_rate_limit_fragments() -> i32 {
        stream_plan::FragmentTypeFlag::Sink as i32
    }

    /// Note: this doesn't include `FsFetch` created in old versions.
    pub fn rate_limit_fragments() -> i32 {
        Self::backfill_rate_limit_fragments()
            | Self::source_rate_limit_fragments()
            | Self::sink_rate_limit_fragments()
    }

    pub fn dml_rate_limit_fragments() -> i32 {
        stream_plan::FragmentTypeFlag::Dml as i32
    }
}

impl stream_plan::Dispatcher {
    pub fn as_strategy(&self) -> stream_plan::DispatchStrategy {
        stream_plan::DispatchStrategy {
            r#type: self.r#type,
            dist_key_indices: self.dist_key_indices.clone(),
            output_indices: self.output_indices.clone(),
        }
    }
}

impl meta::table_fragments::Fragment {
    pub fn dispatches(&self) -> HashMap<i32, stream_plan::DispatchStrategy> {
        self.actors[0]
            .dispatcher
            .iter()
            .map(|d| {
                let fragment_id = d.dispatcher_id as _;
                let strategy = d.as_strategy();
                (fragment_id, strategy)
            })
            .collect()
    }
}

impl catalog::StreamSourceInfo {
    /// Refer to [`Self::cdc_source_job`] for details.
    pub fn is_shared(&self) -> bool {
        self.cdc_source_job
    }
}

impl stream_plan::PbStreamScanType {
    pub fn is_reschedulable(&self) -> bool {
        match self {
            // todo: should this be true?
            PbStreamScanType::UpstreamOnly => false,
            PbStreamScanType::ArrangementBackfill => true,
            // todo: true when stable
            PbStreamScanType::SnapshotBackfill => false,
            _ => false,
        }
    }
}

impl catalog::Sink {
    // TODO: remove this placeholder
    // creating table sink does not have an id, so we need a placeholder
    pub const UNIQUE_IDENTITY_FOR_CREATING_TABLE_SINK: &'static str = "PLACE_HOLDER";

    pub fn unique_identity(&self) -> String {
        // TODO: use a more unique name
        format!("{}", self.id)
    }
}

impl std::fmt::Debug for meta::SystemParams {
    /// Directly formatting `SystemParams` can be inaccurate or leak sensitive information.
    ///
    /// Use `SystemParamsReader` instead.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemParams").finish_non_exhaustive()
    }
}

// More compact formats for debugging

impl std::fmt::Debug for data::DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data::DataType {
            precision,
            scale,
            interval_type,
            field_type,
            field_names,
            type_name,
            // currently all data types are nullable
            is_nullable: _,
            unqualified_name,
        } = self;

        let type_name = data::data_type::TypeName::try_from(*type_name)
            .map(|t| t.as_str_name())
            .unwrap_or("Unknown");

        let mut s = f.debug_struct(type_name);
        if let Some(unqualified_name) = unqualified_name {
            s.field("unqualified_name", unqualified_name);
        }
        if self.precision != 0 {
            s.field("precision", precision);
        }
        if self.scale != 0 {
            s.field("scale", scale);
        }
        if self.interval_type != 0 {
            s.field("interval_type", interval_type);
        }
        if !self.field_type.is_empty() {
            s.field("field_type", field_type);
        }
        if !self.field_names.is_empty() {
            s.field("field_names", field_names);
        }
        s.finish()
    }
}

impl std::fmt::Debug for plan_common::column_desc::GeneratedOrDefaultColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GeneratedColumn(arg0) => f.debug_tuple("GeneratedColumn").field(arg0).finish(),
            Self::DefaultColumn(arg0) => f.debug_tuple("DefaultColumn").field(arg0).finish(),
        }
    }
}

impl std::fmt::Debug for plan_common::ColumnDesc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // destruct here to avoid missing new fields in the future.
        let plan_common::ColumnDesc {
            column_type,
            column_id,
            name,
            field_descs,
            type_name,
            description,
            additional_column_type,
            additional_column,
            generated_or_default_column,
            version,
        } = self;

        let mut s = f.debug_struct("ColumnDesc");
        if let Some(column_type) = column_type {
            s.field("column_type", column_type);
        } else {
            s.field("column_type", &"Unknown");
        }
        s.field("column_id", column_id).field("name", name);
        if !self.field_descs.is_empty() {
            s.field("field_descs", field_descs);
        }
        if !self.type_name.is_empty() {
            s.field("type_name", type_name);
        }
        if let Some(description) = description {
            s.field("description", description);
        }
        if self.additional_column_type != 0 {
            s.field("additional_column_type", additional_column_type);
        }
        s.field("version", version);
        if let Some(AdditionalColumn {
            column_type: Some(column_type),
        }) = additional_column
        {
            // AdditionalColumn { None } means a normal column
            s.field("additional_column", &column_type);
        }
        if let Some(generated_or_default_column) = generated_or_default_column {
            s.field("generated_or_default_column", &generated_or_default_column);
        }
        s.finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::data::{data_type, DataType};
    use crate::plan_common::Field;
    use crate::stream_plan::stream_node::NodeBody;

    #[test]
    fn test_getter() {
        let data_type: DataType = DataType {
            is_nullable: true,
            ..Default::default()
        };
        let field = Field {
            data_type: Some(data_type),
            name: "".to_owned(),
        };
        assert!(field.get_data_type().unwrap().is_nullable);
    }

    #[test]
    fn test_enum_getter() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::Double as i32;
        assert_eq!(
            data_type::TypeName::Double,
            data_type.get_type_name().unwrap()
        );
    }

    #[test]
    fn test_enum_unspecified() {
        let mut data_type: DataType = DataType::default();
        data_type.type_name = data_type::TypeName::TypeUnspecified as i32;
        assert!(data_type.get_type_name().is_err());
    }

    #[test]
    fn test_primitive_getter() {
        let data_type: DataType = DataType::default();
        let new_data_type = DataType {
            is_nullable: data_type.get_is_nullable(),
            ..Default::default()
        };
        assert!(!new_data_type.is_nullable);
    }

    #[test]
    fn test_size() {
        use static_assertions::const_assert_eq;
        // box all fields in NodeBody to avoid large_enum_variant
        // see https://github.com/risingwavelabs/risingwave/issues/19910
        const_assert_eq!(std::mem::size_of::<NodeBody>(), 16);
    }
}
