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

pub mod manager;
pub mod pb_compatible;
pub mod report;

use std::env;

use risingwave_pb::telemetry::PbTelemetryClusterType;
pub use risingwave_telemetry_event::{
    current_timestamp, post_telemetry_report_pb, report_event_common, TelemetryError,
    TelemetryResult,
};
use serde::{Deserialize, Serialize};
use sysinfo::System;

use crate::util::env_var::env_var_is_true_or;
use crate::util::resource_util::cpu::total_cpu_available;
use crate::util::resource_util::memory::{system_memory_available_bytes, total_memory_used_bytes};
use crate::RW_VERSION;

type Result<T> = core::result::Result<T, TelemetryError>;

pub const TELEMETRY_CLUSTER_TYPE: &str = "RW_TELEMETRY_TYPE";
pub const TELEMETRY_CLUSTER_TYPE_HOSTED: &str = "hosted"; // hosted on RisingWave Cloud
pub const TELEMETRY_CLUSTER_TYPE_KUBERNETES: &str = "kubernetes";
pub const TELEMETRY_CLUSTER_TYPE_SINGLE_NODE: &str = "single-node";
pub const TELEMETRY_CLUSTER_TYPE_DOCKER_COMPOSE: &str = "docker-compose";

pub use risingwave_telemetry_event::get_telemetry_risingwave_cloud_uuid;

pub fn telemetry_cluster_type_from_env_var() -> PbTelemetryClusterType {
    let cluster_type = match env::var(TELEMETRY_CLUSTER_TYPE) {
        Ok(cluster_type) => cluster_type,
        Err(_) => return PbTelemetryClusterType::Unspecified,
    };
    match cluster_type.as_str() {
        TELEMETRY_CLUSTER_TYPE_HOSTED => PbTelemetryClusterType::CloudHosted,
        TELEMETRY_CLUSTER_TYPE_DOCKER_COMPOSE => PbTelemetryClusterType::DockerCompose,
        TELEMETRY_CLUSTER_TYPE_KUBERNETES => PbTelemetryClusterType::Kubernetes,
        TELEMETRY_CLUSTER_TYPE_SINGLE_NODE => PbTelemetryClusterType::SingleNode,
        _ => PbTelemetryClusterType::Unspecified,
    }
}

/// Url of telemetry backend
pub use risingwave_telemetry_event::TELEMETRY_REPORT_URL;
/// Telemetry reporting interval in seconds, 6 hours
pub const TELEMETRY_REPORT_INTERVAL: u64 = 6 * 60 * 60;

/// Environment Variable that is default to be true
const TELEMETRY_ENV_ENABLE: &str = "ENABLE_TELEMETRY";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum TelemetryNodeType {
    Meta,
    Compute,
    Frontend,
    Compactor,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryReportBase {
    /// `tracking_id` is persistent in metastore
    pub tracking_id: String,
    /// `session_id` is reset every time node restarts
    pub session_id: String,
    /// `system_data` is hardware and os info
    pub system_data: SystemData,
    /// `up_time` is how long the node has been running
    pub up_time: u64,
    /// `time_stamp` is when the report is created
    pub time_stamp: u64,
    /// `node_type` is the node that creates the report
    pub node_type: TelemetryNodeType,
    /// `is_test` is whether the report is from a test environment, default to be false
    /// needed in CI for compatible tests with telemetry backend
    pub is_test: bool,
}

pub trait TelemetryReport: Serialize {}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemData {
    memory: Memory,
    os: Os,
    cpu: Cpu,
}

#[derive(Debug, Serialize, Deserialize)]
struct Memory {
    used: usize,
    total: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct Os {
    name: String,
    kernel_version: String,
    version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Cpu {
    // total number of cpu available as a float
    available: f32,
}

impl SystemData {
    pub fn new() -> Self {
        let memory = {
            let total = system_memory_available_bytes();
            let used = total_memory_used_bytes();
            Memory { used, total }
        };

        let os = Os {
            name: System::name().unwrap_or_default(),
            kernel_version: System::kernel_version().unwrap_or_default(),
            version: System::os_version().unwrap_or_default(),
        };

        let cpu = Cpu {
            available: total_cpu_available(),
        };

        SystemData { memory, os, cpu }
    }
}

impl Default for SystemData {
    fn default() -> Self {
        Self::new()
    }
}

/// check whether telemetry is enabled in environment variable
pub fn telemetry_env_enabled() -> bool {
    // default to be true
    env_var_is_true_or(TELEMETRY_ENV_ENABLE, true)
}

pub fn report_scarf_enabled() -> bool {
    telemetry_env_enabled()
        && !matches!(
            telemetry_cluster_type_from_env_var(),
            PbTelemetryClusterType::CloudHosted
        )
}

// impl logic to report to Scarf service, containing RW version and deployment platform
pub async fn report_to_scarf() {
    let request_url = format!(
        "https://risingwave.gateway.scarf.sh/telemetry/{}/{}",
        RW_VERSION,
        System::name().unwrap_or_default()
    );
    // keep trying every 1h until success
    loop {
        let res = reqwest::get(&request_url).await;
        if let Ok(res) = res {
            if res.status().is_success() {
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enable_scarf() {
        std::env::set_var(TELEMETRY_ENV_ENABLE, "true");

        // setting env var to `Hosted` should disable scarf
        std::env::set_var(TELEMETRY_CLUSTER_TYPE, TELEMETRY_CLUSTER_TYPE_HOSTED);
        assert!(!report_scarf_enabled());

        // setting env var to `DockerCompose` should enable scarf
        std::env::set_var(
            TELEMETRY_CLUSTER_TYPE,
            TELEMETRY_CLUSTER_TYPE_DOCKER_COMPOSE,
        );
        assert!(report_scarf_enabled());
    }

    #[test]
    fn test_system_data_new() {
        let system_data = SystemData::new();

        assert!(system_data.memory.used > 0);
        assert!(system_data.memory.total > 0);
        assert!(!system_data.os.name.is_empty());
        assert!(!system_data.os.kernel_version.is_empty());
        assert!(!system_data.os.version.is_empty());
        assert!(system_data.cpu.available > 0.0);
    }

    #[test]
    fn test_env() {
        let key = "ENABLE_TELEMETRY";

        // make assertions more readable...
        fn is_enabled() -> bool {
            telemetry_env_enabled()
        }
        fn is_not_enabled() -> bool {
            !is_enabled()
        }

        std::env::set_var(key, "true");
        assert!(is_enabled());

        std::env::set_var(key, "false");
        assert!(is_not_enabled());

        std::env::set_var(key, "tRue");
        assert!(is_enabled());

        std::env::set_var(key, "2");
        assert!(is_not_enabled());

        std::env::set_var(key, "1");
        assert!(is_enabled());

        std::env::set_var(key, "not_a_bool");
        assert!(is_not_enabled());

        std::env::remove_var(key);
        assert!(is_enabled());
    }
}
