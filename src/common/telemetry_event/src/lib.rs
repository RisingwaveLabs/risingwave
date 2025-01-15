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

/// Move the Telemetry's Event Report functions here
/// Keep the stats report module in the common/ module
mod util;

use std::env;
use std::sync::OnceLock;

use prost::Message;
use risingwave_pb::telemetry::{
    EventMessage as PbEventMessage, PbBatchEventMessage, PbTelemetryDatabaseObject,
    TelemetryEventStage as PbTelemetryEventStage,
};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::UnboundedSender;
pub use util::*;

pub type TelemetryResult<T> = core::result::Result<T, TelemetryError>;

/// Telemetry errors are generally recoverable/ignorable. `String` is good enough.
pub type TelemetryError = String;

pub static TELEMETRY_TRACKING_ID: OnceLock<String> = OnceLock::new();
pub static TELEMETRY_EVENT_REPORT_TX: OnceLock<UnboundedSender<PbEventMessage>> = OnceLock::new();

pub const TELEMETRY_REPORT_URL: &str = "https://telemetry.risingwave.dev/api/v2/report";

// the UUID of the RisingWave Cloud (if the cluster is hosted on RisingWave Cloud)
pub const TELEMETRY_RISINGWAVE_CLOUD_UUID: &str = "RISINGWAVE_CLOUD_UUID";

pub fn get_telemetry_risingwave_cloud_uuid() -> Option<String> {
    env::var(TELEMETRY_RISINGWAVE_CLOUD_UUID).ok()
}

pub async fn do_telemetry_event_report(event_stash: &mut Vec<PbEventMessage>) {
    if event_stash.is_empty() {
        return;
    }

    const TELEMETRY_EVENT_REPORT_TYPE: &str = "events"; // the batch report url
    let url = (TELEMETRY_REPORT_URL.to_owned() + "/" + TELEMETRY_EVENT_REPORT_TYPE).to_owned();
    let batch_message = PbBatchEventMessage {
        events: std::mem::take(event_stash),
    };

    post_telemetry_report_pb(&url, batch_message.encode_to_vec())
        .await
        .unwrap_or_else(|e| tracing::debug!("{}", e));
}

pub const TELEMETRY_EVENT_REPORT_INTERVAL: u64 = 10; // 10 seconds
pub const TELEMETRY_EVENT_REPORT_STASH_SIZE: usize = 100; // 100 events to trigger a report action

pub fn report_event_common(
    event_stage: PbTelemetryEventStage,
    event_name: &str,
    catalog_id: i64,
    connector_name: Option<String>,
    object: Option<PbTelemetryDatabaseObject>,
    attributes: Option<jsonbb::Value>, // any json string
    node: String,
) {
    let event_tracking_id: String;
    if let Some(tracking_id) = TELEMETRY_TRACKING_ID.get() {
        event_tracking_id = tracking_id.to_string();
    } else {
        tracing::info!("Telemetry tracking_id is not set, event reporting disabled");
        return;
    }

    request_to_telemetry_event(
        event_tracking_id,
        event_stage,
        event_name,
        catalog_id,
        connector_name,
        object,
        attributes,
        node,
        false,
    );
}

pub fn request_to_telemetry_event(
    tracking_id: String,
    event_stage: PbTelemetryEventStage,
    event_name: &str,
    catalog_id: i64,
    connector_name: Option<String>,
    object: Option<PbTelemetryDatabaseObject>,
    attributes: Option<jsonbb::Value>, // any json string
    node: String,
    is_test: bool,
) {
    let event = PbEventMessage {
        tracking_id,
        event_time_sec: current_timestamp(),
        event_stage: event_stage as i32,
        event_name: event_name.to_string(),
        connector_name,
        object: object.map(|c| c as i32),
        catalog_id,
        attributes: attributes.map(|a| a.to_string()),
        node,
        is_test,
    };

    if let Some(tx) = TELEMETRY_EVENT_REPORT_TX.get() {
        let _ = tx.send(event).inspect_err(|e| {
            tracing::warn!("Failed to send telemetry event queue: {}", e.as_report())
        });
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_telemetry_report_event() {
        let event_stage = PbTelemetryEventStage::CreateStreamJob;
        let event_name = "test_feature";
        let catalog_id = 1;
        let connector_name = Some("test_connector".to_string());
        let object = Some(PbTelemetryDatabaseObject::Source);
        let attributes = None;
        let node = "test_node".to_string();

        request_to_telemetry_event(
            "7d45669c-08c7-4571-ae3d-d3a3e70a2f7e".to_string(),
            event_stage,
            event_name,
            catalog_id,
            connector_name,
            object,
            attributes,
            node,
            true,
        );

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
