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

use std::collections::HashMap;

use anyhow::anyhow;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_connector::source::SplitImpl;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::PbRecoveryStatus;
use risingwave_pb::stream_plan::StreamActor;
use tokio::sync::oneshot::Sender;

use self::notifier::Notifier;
use crate::barrier::info::{BarrierInfo, InflightDatabaseInfo};
use crate::manager::ActiveStreamingWorkerNodes;
use crate::model::{ActorId, StreamJobFragments};
use crate::{MetaError, MetaResult};

mod checkpoint;
mod command;
mod complete_task;
mod context;
mod info;
mod manager;
mod notifier;
mod progress;
mod rpc;
mod schedule;
mod trace;
mod utils;
mod worker;

pub use self::command::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType,
    ReplaceStreamJobPlan, Reschedule, SnapshotBackfillInfo,
};
pub use self::info::InflightSubscriptionInfo;
pub use self::manager::{BarrierManagerRef, GlobalBarrierManager};
pub use self::schedule::BarrierScheduler;
pub use self::trace::TracedEpoch;

/// The reason why the cluster is recovering.
enum RecoveryReason {
    /// After bootstrap.
    Bootstrap,
    /// After failure.
    Failover(MetaError),
    /// Manually triggered
    Adhoc,
}

/// Status of barrier manager.
enum BarrierManagerStatus {
    /// Barrier manager is starting.
    Starting,
    /// Barrier manager is under recovery.
    Recovering(RecoveryReason),
    /// Barrier manager is running.
    Running,
}

/// Scheduled command with its notifiers.
struct Scheduled {
    database_id: DatabaseId,
    command: Command,
    notifiers: Vec<Notifier>,
    span: tracing::Span,
}

impl From<&BarrierManagerStatus> for PbRecoveryStatus {
    fn from(status: &BarrierManagerStatus) -> Self {
        match status {
            BarrierManagerStatus::Starting => Self::StatusStarting,
            BarrierManagerStatus::Recovering(reason) => match reason {
                RecoveryReason::Bootstrap => Self::StatusStarting,
                RecoveryReason::Failover(_) | RecoveryReason::Adhoc => Self::StatusRecovering,
            },
            BarrierManagerStatus::Running => Self::StatusRunning,
        }
    }
}

pub(crate) enum BarrierManagerRequest {
    GetDdlProgress(Sender<HashMap<u32, DdlProgress>>),
    AdhocRecovery(Sender<()>),
}

#[derive(Debug)]
struct BarrierWorkerRuntimeInfoSnapshot {
    active_streaming_nodes: ActiveStreamingWorkerNodes,
    database_fragment_infos: HashMap<DatabaseId, InflightDatabaseInfo>,
    state_table_committed_epochs: HashMap<TableId, u64>,
    subscription_infos: HashMap<DatabaseId, InflightSubscriptionInfo>,
    stream_actors: HashMap<ActorId, StreamActor>,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, (String, StreamJobFragments)>,
    hummock_version_stats: HummockVersionStats,
}

impl BarrierWorkerRuntimeInfoSnapshot {
    fn validate_database_info(
        database_id: DatabaseId,
        database_info: &InflightDatabaseInfo,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
        stream_actors: &HashMap<ActorId, StreamActor>,
        state_table_committed_epochs: &HashMap<TableId, u64>,
    ) -> MetaResult<()> {
        {
            for fragment in database_info.fragment_infos() {
                for (actor_id, worker_id) in &fragment.actors {
                    if !active_streaming_nodes.current().contains_key(worker_id) {
                        return Err(anyhow!(
                            "worker_id {} of actor {} do not exist",
                            worker_id,
                            actor_id
                        )
                        .into());
                    }
                    if !stream_actors.contains_key(actor_id) {
                        return Err(anyhow!("cannot find StreamActor of actor {}", actor_id).into());
                    }
                }
                for state_table_id in &fragment.state_table_ids {
                    if !state_table_committed_epochs.contains_key(state_table_id) {
                        return Err(anyhow!(
                            "state table {} is not registered to hummock",
                            state_table_id
                        )
                        .into());
                    }
                }
            }
            let mut committed_epochs = database_info.existing_table_ids().map(|table_id| {
                (
                    table_id,
                    *state_table_committed_epochs
                        .get(&table_id)
                        .expect("checked exist"),
                )
            });
            let (first_table, first_epoch) = committed_epochs.next().ok_or_else(|| {
                anyhow!("database {} has no state table after recovery", database_id)
            })?;
            for (table_id, epoch) in committed_epochs {
                if epoch != first_epoch {
                    return Err(anyhow!(
                        "database {} has tables with different table ids. {}:{}, {}:{}",
                        database_id,
                        first_table,
                        first_epoch,
                        table_id,
                        epoch
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    fn validate(&self) -> MetaResult<()> {
        for (database_id, database_info) in &self.database_fragment_infos {
            Self::validate_database_info(
                *database_id,
                database_info,
                &self.active_streaming_nodes,
                &self.stream_actors,
                &self.state_table_committed_epochs,
            )?
        }
        Ok(())
    }
}

#[derive(Debug)]
struct DatabaseRuntimeInfoSnapshot {
    database_fragment_info: InflightDatabaseInfo,
    state_table_committed_epochs: HashMap<TableId, u64>,
    subscription_info: InflightSubscriptionInfo,
    stream_actors: HashMap<ActorId, StreamActor>,
    source_splits: HashMap<ActorId, Vec<SplitImpl>>,
    background_jobs: HashMap<TableId, (String, StreamJobFragments)>,
}

impl DatabaseRuntimeInfoSnapshot {
    fn validate(
        &self,
        database_id: DatabaseId,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<()> {
        BarrierWorkerRuntimeInfoSnapshot::validate_database_info(
            database_id,
            &self.database_fragment_info,
            active_streaming_nodes,
            &self.stream_actors,
            &self.state_table_committed_epochs,
        )
    }
}
