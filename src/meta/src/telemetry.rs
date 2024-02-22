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

use risingwave_common::config::MetaBackend;
use risingwave_common::telemetry::report::{TelemetryInfoFetcher, TelemetryReportCreator};
use risingwave_common::telemetry::{
    current_timestamp, SystemData, TelemetryNodeType, TelemetryReport, TelemetryReportBase,
    TelemetryResult,
};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_pb::common::WorkerType;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::manager::MetadataManager;
use crate::model::ClusterId;

#[derive(Debug, Serialize, Deserialize)]
struct NodeCount {
    meta_count: u64,
    compute_count: u64,
    frontend_count: u64,
    compactor_count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RwVersion {
    version: String,
    git_sha: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MetaTelemetryReport {
    #[serde(flatten)]
    base: TelemetryReportBase,
    node_count: NodeCount,
    streaming_job_count: u64,
    // At this point, it will always be etcd, but we will enable telemetry when using memory.
    meta_backend: MetaBackend,
    rw_version: RwVersion,
}

impl TelemetryReport for MetaTelemetryReport {}

pub struct MetaTelemetryInfoFetcher {
    tracking_id: ClusterId,
}

impl MetaTelemetryInfoFetcher {
    pub fn new(tracking_id: ClusterId) -> Self {
        Self { tracking_id }
    }
}

#[async_trait::async_trait]
impl TelemetryInfoFetcher for MetaTelemetryInfoFetcher {
    async fn fetch_telemetry_info(&self) -> TelemetryResult<Option<String>> {
        Ok(Some(self.tracking_id.clone().into()))
    }
}

#[derive(Clone)]
pub struct MetaReportCreator {
    metadata_manager: MetadataManager,
    meta_backend: MetaBackend,
}

impl MetaReportCreator {
    pub fn new(metadata_manager: MetadataManager, meta_backend: MetaBackend) -> Self {
        Self {
            metadata_manager,
            meta_backend,
        }
    }
}

#[async_trait::async_trait]
impl TelemetryReportCreator for MetaReportCreator {
    #[expect(refining_impl_trait)]
    async fn create_report(
        &self,
        tracking_id: String,
        session_id: String,
        up_time: u64,
    ) -> TelemetryResult<MetaTelemetryReport> {
        let node_map = self
            .metadata_manager
            .count_worker_node()
            .await
            .map_err(|err| err.as_report().to_string())?;

        let streaming_job_count = self
            .metadata_manager
            .count_streaming_job()
            .await
            .map_err(|err| err.as_report().to_string())? as u64;

        Ok(MetaTelemetryReport {
            rw_version: RwVersion {
                version: RW_VERSION.to_string(),
                git_sha: GIT_SHA.to_string(),
            },
            base: TelemetryReportBase {
                tracking_id,
                session_id,
                system_data: SystemData::new(),
                up_time,
                time_stamp: current_timestamp(),
                node_type: TelemetryNodeType::Meta,
            },
            node_count: NodeCount {
                meta_count: *node_map.get(&WorkerType::Meta).unwrap_or(&0),
                compute_count: *node_map.get(&WorkerType::ComputeNode).unwrap_or(&0),
                frontend_count: *node_map.get(&WorkerType::Frontend).unwrap_or(&0),
                compactor_count: *node_map.get(&WorkerType::Compactor).unwrap_or(&0),
            },
            streaming_job_count,
            meta_backend: self.meta_backend,
        })
    }

    fn report_type(&self) -> &str {
        "meta"
    }
}
