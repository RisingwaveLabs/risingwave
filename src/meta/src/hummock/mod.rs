// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod compaction;
mod compaction_group;
mod compaction_scheduler;
mod compactor_manager;
mod hummock_manager;
#[cfg(test)]
mod hummock_manager_tests;
mod level_handler;
mod metrics_utils;
#[cfg(any(test, feature = "test"))]
pub mod mock_hummock_meta_client;
mod model;
#[cfg(any(test, feature = "test"))]
pub mod test_utils;
mod vacuum;

use std::sync::Arc;
use std::time::Duration;

pub use compaction_scheduler::CompactionScheduler;
pub use compactor_manager::*;
pub use hummock_manager::*;
#[cfg(any(test, feature = "test"))]
pub use mock_hummock_meta_client::MockHummockMetaClient;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
pub use vacuum::*;

use crate::hummock::compaction_group::CompactionGroupId;
use crate::hummock::compaction_scheduler::CompactionSchedulerRef;
use crate::manager::{LocalNotification, NotificationManagerRef};
use crate::storage::MetaStore;

/// Start hummock's asynchronous tasks.
pub async fn start_hummock_workers<S>(
    hummock_manager: HummockManagerRef<S>,
    compactor_manager: CompactorManagerRef,
    vacuum_trigger: Arc<VacuumTrigger<S>>,
    notification_manager: NotificationManagerRef,
    compaction_scheduler: CompactionSchedulerRef<S>,
) -> Vec<(JoinHandle<()>, UnboundedSender<()>)>
where
    S: MetaStore,
{
    vec![
        start_compaction_scheduler(compaction_scheduler),
        start_vacuum_scheduler(vacuum_trigger),
        subscribe_cluster_membership_change(
            hummock_manager,
            compactor_manager,
            notification_manager,
        )
        .await,
    ]
}

/// Starts a task to handle cluster membership change.
pub async fn subscribe_cluster_membership_change<S>(
    hummock_manager: Arc<HummockManager<S>>,
    compactor_manager: Arc<CompactorManager>,
    notification_manager: NotificationManagerRef,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    notification_manager.insert_local_sender(tx).await;
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        loop {
            let worker_node = tokio::select! {
                notification = rx.recv() => {
                    match notification {
                        None => {
                            return;
                        }
                        Some(LocalNotification::WorkerDeletion(worker_node)) => worker_node
                    }
                }
                _ = shutdown_rx.recv() => {
                    return;
                }
            };
            compactor_manager.remove_compactor(worker_node.id);
            let retry_strategy = ExponentialBackoff::from_millis(10)
                .max_delay(Duration::from_secs(60))
                .map(jitter);
            tokio_retry::Retry::spawn(retry_strategy, || async {
                if let Err(err) = hummock_manager.release_contexts(vec![worker_node.id]).await {
                    tracing::warn!("Failed to release_contexts {}. Will retry.", err);
                    return Err(err);
                }
                Ok(())
            })
            .await
            .expect("Should retry until release_contexts succeeds");
        }
    });
    (join_handle, shutdown_tx)
}

/// Starts a task to accept compaction request.
fn start_compaction_scheduler<S>(
    compaction_scheduler: CompactionSchedulerRef<S>,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    // TODO: remove this periodic trigger after #2121
    let request_sender = compaction_scheduler.request_sender();
    tokio::spawn(async move {
        let mut min_interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            min_interval.tick().await;
            if request_sender.send(CompactionGroupId::new(0)).is_err() {
                tracing::info!("Stop periodic compaction trigger");
                return;
            }
        }
    });

    // Start compaction scheduler
    let shutdown_sender = compaction_scheduler.shutdown_sender();
    let join_handle = tokio::spawn(async move {
        compaction_scheduler.start().await;
    });

    (join_handle, shutdown_sender)
}

/// Vacuum is triggered at this rate.
const VACUUM_TRIGGER_INTERVAL: Duration = Duration::from_secs(30);
/// Orphan SST will be deleted after this interval.
const ORPHAN_SST_RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60 * 24);
/// Starts a task to periodically vacuum hummock.
pub fn start_vacuum_scheduler<S>(
    vacuum: Arc<VacuumTrigger<S>>,
) -> (JoinHandle<()>, UnboundedSender<()>)
where
    S: MetaStore,
{
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
    let join_handle = tokio::spawn(async move {
        let mut min_trigger_interval = tokio::time::interval(VACUUM_TRIGGER_INTERVAL);
        loop {
            tokio::select! {
                // Wait for interval
                _ = min_trigger_interval.tick() => {},
                // Shutdown vacuum
                _ = shutdown_rx.recv() => {
                    tracing::info!("Vacuum is shutting down");
                    return;
                }
            }
            if let Err(err) = vacuum.vacuum_version_metadata().await {
                tracing::warn!("Vacuum tracked data error {}", err);
            }
            // vacuum_orphan_data can be invoked less frequently.
            if let Err(err) = vacuum.vacuum_sst_data(ORPHAN_SST_RETENTION_INTERVAL).await {
                tracing::warn!("Vacuum orphan data error {}", err);
            }
        }
    });
    (join_handle, shutdown_tx)
}
