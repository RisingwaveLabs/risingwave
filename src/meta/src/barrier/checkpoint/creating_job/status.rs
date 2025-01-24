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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::mem::take;

use risingwave_common::hash::ActorId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::barrier_complete_response::{
    CreateMviewProgress, PbCreateMviewProgress,
};
use tracing::warn;

use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::{BarrierInfo, BarrierKind, TracedEpoch};
use crate::model::StreamJobActorsToCreate;

#[derive(Debug)]
pub(super) struct CreateMviewLogStoreProgressTracker {
    /// `actor_id` -> `pending_barrier_count`
    ongoing_actors: HashMap<ActorId, usize>,
    finished_actors: HashSet<ActorId>,
}

impl CreateMviewLogStoreProgressTracker {
    fn new(actors: impl Iterator<Item = ActorId>, initial_pending_count: usize) -> Self {
        Self {
            ongoing_actors: HashMap::from_iter(actors.map(|actor| (actor, initial_pending_count))),
            finished_actors: HashSet::new(),
        }
    }

    pub(super) fn gen_ddl_progress(&self) -> String {
        let sum = self.ongoing_actors.values().sum::<usize>() as f64;
        let count = if self.ongoing_actors.is_empty() {
            1
        } else {
            self.ongoing_actors.len()
        } as f64;
        let avg = sum / count;
        format!(
            "finished: {}/{}, avg epoch count {}",
            self.finished_actors.len(),
            self.ongoing_actors.len() + self.finished_actors.len(),
            avg
        )
    }

    fn update(&mut self, progress: impl IntoIterator<Item = &PbCreateMviewProgress>) {
        for progress in progress {
            match self.ongoing_actors.entry(progress.backfill_actor_id) {
                Entry::Occupied(mut entry) => {
                    if progress.done {
                        entry.remove_entry();
                        assert!(
                            self.finished_actors.insert(progress.backfill_actor_id),
                            "non-duplicate"
                        );
                    } else {
                        *entry.get_mut() = progress.pending_barrier_num as _;
                    }
                }
                Entry::Vacant(_) => {
                    if cfg!(debug_assertions) {
                        panic!(
                            "reporting progress on non-inflight actor: {:?} {:?}",
                            progress, self
                        );
                    } else {
                        warn!(?progress, progress_tracker = ?self, "reporting progress on non-inflight actor");
                    }
                }
            }
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.ongoing_actors.is_empty()
    }
}

#[derive(Debug)]
pub(super) enum CreatingStreamingJobStatus {
    /// The creating job is consuming upstream snapshot.
    /// Will transit to `ConsumingLogStore` on `update_progress` when
    /// the snapshot has been fully consumed after `update_progress`.
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_upstream_barriers: Vec<BarrierInfo>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        snapshot_backfill_actors: HashSet<ActorId>,
        backfill_epoch: u64,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
        /// Info of the first barrier: (`actors_to_create`, `mutation`)
        /// Take the mutation out when injecting the first barrier
        initial_barrier_info: Option<(StreamJobActorsToCreate, Mutation)>,
    },
    /// The creating job is consuming log store.
    ///
    /// Will transit to `Finishing` on `on_new_upstream_epoch` when `start_consume_upstream` is `true`.
    ConsumingLogStore {
        log_store_progress_tracker: CreateMviewLogStoreProgressTracker,
    },
    /// All backfill actors have started consuming upstream, and the job
    /// will be finished when all previously injected barriers have been collected
    /// Store the `prev_epoch` that will finish at.
    Finishing(u64),
}

pub(super) struct CreatingJobInjectBarrierInfo {
    pub barrier_info: BarrierInfo,
    pub new_actors: Option<StreamJobActorsToCreate>,
    pub mutation: Option<Mutation>,
}

impl CreatingStreamingJobStatus {
    pub(super) fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<Item = &CreateMviewProgress>,
    ) -> Option<Vec<CreatingJobInjectBarrierInfo>> {
        match self {
            Self::ConsumingSnapshot {
                create_mview_tracker,
                ref version_stats,
                prev_epoch_fake_physical_time,
                pending_upstream_barriers,
                pending_non_checkpoint_barriers,
                ref backfill_epoch,
                initial_barrier_info,
                ref snapshot_backfill_actors,
                ..
            } => {
                create_mview_tracker.update_tracking_jobs(
                    None,
                    create_mview_progress,
                    version_stats,
                );
                if create_mview_tracker.has_pending_finished_jobs() {
                    let (new_actors, mutation) = match initial_barrier_info.take() {
                        Some((new_actors, mutation)) => (Some(new_actors), Some(mutation)),
                        None => (None, None),
                    };
                    assert!(initial_barrier_info.is_none());
                    pending_non_checkpoint_barriers.push(*backfill_epoch);

                    let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                    let barriers_to_inject: Vec<_> = [CreatingJobInjectBarrierInfo {
                        barrier_info: BarrierInfo {
                            curr_epoch: TracedEpoch::new(Epoch(*backfill_epoch)),
                            prev_epoch: TracedEpoch::new(prev_epoch),
                            kind: BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                        },
                        new_actors,
                        mutation,
                    }]
                    .into_iter()
                    .chain(pending_upstream_barriers.drain(..).map(|barrier_info| {
                        CreatingJobInjectBarrierInfo {
                            barrier_info,
                            new_actors: None,
                            mutation: None,
                        }
                    }))
                    .collect();

                    *self = CreatingStreamingJobStatus::ConsumingLogStore {
                        log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                            snapshot_backfill_actors.iter().cloned(),
                            barriers_to_inject.len(),
                        ),
                    };
                    Some(barriers_to_inject)
                } else {
                    None
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                log_store_progress_tracker.update(create_mview_progress);
                None
            }
            CreatingStreamingJobStatus::Finishing(_) => None,
        }
    }

    pub(super) fn on_new_upstream_epoch(
        &mut self,
        barrier_info: &BarrierInfo,
        start_consume_upstream: bool,
    ) -> Option<CreatingJobInjectBarrierInfo> {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_upstream_barriers,
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                initial_barrier_info,
                ..
            } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job that are consuming snapshot"
                );
                pending_upstream_barriers.push(barrier_info.clone());
                Some(CreatingStreamingJobStatus::new_fake_barrier(
                    prev_epoch_fake_physical_time,
                    pending_non_checkpoint_barriers,
                    initial_barrier_info,
                    barrier_info.kind.is_checkpoint(),
                ))
            }
            CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                let prev_epoch = barrier_info.prev_epoch();
                if start_consume_upstream {
                    assert!(barrier_info.kind.is_checkpoint());
                    *self = CreatingStreamingJobStatus::Finishing(prev_epoch);
                }
                Some(CreatingJobInjectBarrierInfo {
                    barrier_info: barrier_info.clone(),
                    new_actors: None,
                    mutation: None,
                })
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job again"
                );
                None
            }
        }
    }

    pub(super) fn new_fake_barrier(
        prev_epoch_fake_physical_time: &mut u64,
        pending_non_checkpoint_barriers: &mut Vec<u64>,
        initial_barrier_info: &mut Option<(StreamJobActorsToCreate, Mutation)>,
        is_checkpoint: bool,
    ) -> CreatingJobInjectBarrierInfo {
        {
            {
                let prev_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                *prev_epoch_fake_physical_time += 1;
                let curr_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                let kind = if is_checkpoint {
                    BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
                } else {
                    BarrierKind::Barrier
                };
                let (new_actors, mutation) =
                    if let Some((new_actors, mutation)) = initial_barrier_info.take() {
                        (Some(new_actors), Some(mutation))
                    } else {
                        Default::default()
                    };
                CreatingJobInjectBarrierInfo {
                    barrier_info: BarrierInfo {
                        prev_epoch,
                        curr_epoch,
                        kind,
                    },
                    new_actors,
                    mutation,
                }
            }
        }
    }

    pub(super) fn is_finishing(&self) -> bool {
        matches!(self, Self::Finishing(_))
    }
}
