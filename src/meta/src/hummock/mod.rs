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
pub mod compaction;
pub mod compactor_manager;
pub mod error;
mod manager;

pub use manager::*;
use thiserror_ext::AsReport;

mod level_handler;
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
pub mod model;
pub mod test_utils;
mod utils;
use std::time::Duration;

pub use compactor_manager::*;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::backup_restore::BackupManagerRef;
use crate::MetaOpts;

/// Start hummock's asynchronous tasks.
pub fn start_hummock_workers(
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    meta_opts: &MetaOpts,
) -> Vec<(JoinHandle<()>, Sender<()>)> {
    // These critical tasks are put in their own timer loop deliberately, to avoid long-running ones
    // from blocking others.
    let workers = vec![
        start_checkpoint_loop(
            hummock_manager.clone(),
            backup_manager,
            Duration::from_secs(meta_opts.hummock_version_checkpoint_interval_sec),
            meta_opts.min_delta_log_num_for_hummock_version_checkpoint,
        ),
        start_vacuum_metadata_loop(
            hummock_manager.clone(),
            Duration::from_secs(meta_opts.vacuum_interval_sec),
        ),
        start_vacuum_time_travel_metadata_loop(
            hummock_manager.clone(),
            Duration::from_secs(meta_opts.time_travel_vacuum_interval_sec),
        ),
    ];
    workers
}

/// Starts a task to periodically vacuum stale metadata.
pub fn start_vacuum_metadata_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum metadata loop is stopped");
                    return;
                }
            }
            if let Err(err) = hummock_manager.delete_version_deltas().await {
                tracing::warn!(error = %err.as_report(), "Vacuum metadata error");
            }
        }
    });
    (join_handle, shutdown_tx)
}

pub fn start_vacuum_time_travel_metadata_loop(
    hummock_manager: HummockManagerRef,
    interval: Duration,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = &mut shutdown_rx => {
                    tracing::info!("Vacuum time travel metadata loop is stopped");
                    return;
                }
            }
            if let Err(err) = hummock_manager.delete_time_travel_metadata().await {
                tracing::warn!(error = %err.as_report(), "Vacuum time travel metadata error");
            }
        }
    });
    (join_handle, shutdown_tx)
}

pub fn start_checkpoint_loop(
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    interval: Duration,
    min_delta_log_num: u64,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(interval);
        min_trigger_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown checkpoint
                _ = &mut shutdown_rx => {
                    tracing::info!("Hummock version checkpoint is stopped");
                    return;
                }
            }
            if hummock_manager.is_version_checkpoint_paused()
                || hummock_manager.env.opts.compaction_deterministic_test
            {
                continue;
            }
            if let Err(err) = hummock_manager
                .create_version_checkpoint(min_delta_log_num)
                .await
            {
                tracing::warn!(error = %err.as_report(), "Hummock version checkpoint error.");
            } else {
                let backup_manager_2 = backup_manager.clone();
                let hummock_manager_2 = hummock_manager.clone();
                tokio::task::spawn(async move {
                    let _ = hummock_manager_2
                        .try_start_minor_gc(backup_manager_2)
                        .await
                        .inspect_err(|err| {
                            tracing::warn!(error = %err.as_report(), "Hummock minor GC error.");
                        });
                });
            }
        }
    });
    (join_handle, shutdown_tx)
}
