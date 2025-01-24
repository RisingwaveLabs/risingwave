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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_plan::PbSubscriptionUpstreamInfo;
use tracing::warn;

use crate::barrier::{BarrierKind, Command, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{ActorId, FragmentId, SubscriptionId};

#[derive(Debug, Clone)]
pub(super) struct BarrierInfo {
    pub prev_epoch: TracedEpoch,
    pub curr_epoch: TracedEpoch,
    pub kind: BarrierKind,
}

impl BarrierInfo {
    pub(super) fn prev_epoch(&self) -> u64 {
        self.prev_epoch.value().0
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CommandFragmentChanges {
    NewFragment(TableId, InflightFragmentInfo),
    Reschedule {
        new_actors: HashMap<ActorId, WorkerId>,
        to_remove: HashSet<ActorId>,
    },
    RemoveFragment,
}

#[derive(Default, Clone, Debug)]
pub struct InflightSubscriptionInfo {
    /// `mv_table_id` => `subscription_id` => retention seconds
    pub mv_depended_subscriptions: HashMap<TableId, HashMap<SubscriptionId, u64>>,
}

#[derive(Clone, Debug)]
pub struct InflightStreamingJobInfo {
    pub job_id: TableId,
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
}

impl InflightStreamingJobInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.fragment_infos.values()
    }
}

#[derive(Clone, Debug)]
pub struct InflightDatabaseInfo {
    jobs: HashMap<TableId, InflightStreamingJobInfo>,
    fragment_location: HashMap<FragmentId, TableId>,
}

impl InflightDatabaseInfo {
    pub fn fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.jobs.values().flat_map(|job| job.fragment_infos())
    }

    pub fn job_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.jobs.keys().cloned()
    }

    pub fn contains_job(&self, job_id: TableId) -> bool {
        self.jobs.contains_key(&job_id)
    }

    pub fn fragment(&self, fragment_id: FragmentId) -> &InflightFragmentInfo {
        let job_id = self.fragment_location[&fragment_id];
        self.jobs
            .get(&job_id)
            .expect("should exist")
            .fragment_infos
            .get(&fragment_id)
            .expect("should exist")
    }
}

impl InflightDatabaseInfo {
    pub fn empty() -> Self {
        Self {
            jobs: Default::default(),
            fragment_location: Default::default(),
        }
    }

    /// Resolve inflight actor info from given nodes and actors that are loaded from meta store. It will be used during recovery to rebuild all streaming actors.
    pub fn new<I: Iterator<Item = (FragmentId, InflightFragmentInfo)>>(
        fragment_infos: impl Iterator<Item = (TableId, I)>,
    ) -> Self {
        let mut fragment_location = HashMap::new();
        let mut jobs = HashMap::new();

        for (job_id, job_fragment_info) in fragment_infos {
            let job_fragment_info: HashMap<_, _> = job_fragment_info.collect();
            assert!(!job_fragment_info.is_empty());
            for fragment_id in job_fragment_info.keys() {
                fragment_location
                    .try_insert(*fragment_id, job_id)
                    .expect("no duplicate");
            }
            jobs.insert(
                job_id,
                InflightStreamingJobInfo {
                    job_id,
                    fragment_infos: job_fragment_info,
                },
            );
        }
        assert!(!jobs.is_empty());
        Self {
            jobs,
            fragment_location,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }

    pub(crate) fn extend(&mut self, job: InflightStreamingJobInfo) {
        self.apply_add(job.fragment_infos.into_iter().map(|(fragment_id, info)| {
            (
                fragment_id,
                CommandFragmentChanges::NewFragment(job.job_id, info),
            )
        }))
    }

    /// Apply some actor changes before issuing a barrier command, if the command contains any new added actors, we should update
    /// the info correspondingly.
    pub(crate) fn pre_apply(
        &mut self,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) {
        self.apply_add(
            fragment_changes
                .iter()
                .map(|(fragment_id, change)| (*fragment_id, change.clone())),
        )
    }

    fn apply_add(
        &mut self,
        fragment_changes: impl Iterator<Item = (FragmentId, CommandFragmentChanges)>,
    ) {
        {
            for (fragment_id, change) in fragment_changes {
                match change {
                    CommandFragmentChanges::NewFragment(job_id, info) => {
                        let fragment_infos =
                            self.jobs
                                .entry(job_id)
                                .or_insert_with(|| InflightStreamingJobInfo {
                                    job_id,
                                    fragment_infos: Default::default(),
                                });
                        fragment_infos
                            .fragment_infos
                            .try_insert(fragment_id, info)
                            .expect("non duplicate");
                        self.fragment_location
                            .try_insert(fragment_id, job_id)
                            .expect("non duplicate");
                    }
                    CommandFragmentChanges::Reschedule { new_actors, .. } => {
                        let job_id = self.fragment_location[&fragment_id];
                        let info = self
                            .jobs
                            .get_mut(&job_id)
                            .expect("should exist")
                            .fragment_infos
                            .get_mut(&fragment_id)
                            .expect("should exist");
                        let actors = &mut info.actors;
                        for (actor_id, node_id) in &new_actors {
                            assert!(actors.insert(*actor_id as _, *node_id as _).is_none());
                        }
                    }
                    CommandFragmentChanges::RemoveFragment => {}
                }
            }
        }
    }
}

impl InflightSubscriptionInfo {
    pub fn pre_apply(&mut self, command: &Command) {
        if let Command::CreateSubscription {
            subscription_id,
            upstream_mv_table_id,
            retention_second,
        } = command
        {
            if let Some(prev_retiontion) = self
                .mv_depended_subscriptions
                .entry(*upstream_mv_table_id)
                .or_default()
                .insert(*subscription_id, *retention_second)
            {
                warn!(subscription_id, ?upstream_mv_table_id, mv_depended_subscriptions = ?self.mv_depended_subscriptions, prev_retiontion, "add an existing subscription id");
            }
        }
    }
}

impl<'a> IntoIterator for &'a InflightSubscriptionInfo {
    type Item = PbSubscriptionUpstreamInfo;

    type IntoIter = impl Iterator<Item = PbSubscriptionUpstreamInfo> + 'a;

    fn into_iter(self) -> Self::IntoIter {
        self.mv_depended_subscriptions
            .iter()
            .flat_map(|(table_id, subscriptions)| {
                subscriptions
                    .keys()
                    .map(|subscriber_id| PbSubscriptionUpstreamInfo {
                        subscriber_id: *subscriber_id,
                        upstream_mv_table_id: table_id.table_id,
                    })
            })
    }
}

impl InflightDatabaseInfo {
    /// Apply some actor changes after the barrier command is collected, if the command contains any actors that are dropped, we should
    /// remove that from the snapshot correspondingly.
    pub(crate) fn post_apply(
        &mut self,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) {
        {
            for (fragment_id, changes) in fragment_changes {
                match changes {
                    CommandFragmentChanges::NewFragment(_, _) => {}
                    CommandFragmentChanges::Reschedule { to_remove, .. } => {
                        let job_id = self.fragment_location[fragment_id];
                        let info = self
                            .jobs
                            .get_mut(&job_id)
                            .expect("should exist")
                            .fragment_infos
                            .get_mut(fragment_id)
                            .expect("should exist");
                        for actor_id in to_remove {
                            assert!(info.actors.remove(&(*actor_id as _)).is_some());
                        }
                    }
                    CommandFragmentChanges::RemoveFragment => {
                        let job_id = self
                            .fragment_location
                            .remove(fragment_id)
                            .expect("should exist");
                        let job = self.jobs.get_mut(&job_id).expect("should exist");
                        job.fragment_infos
                            .remove(fragment_id)
                            .expect("should exist");
                        if job.fragment_infos.is_empty() {
                            self.jobs.remove(&job_id).expect("should exist");
                        }
                    }
                }
            }
        }
    }
}

impl InflightSubscriptionInfo {
    pub fn post_apply(&mut self, command: &Command) {
        if let Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id,
        } = command
        {
            let removed = match self.mv_depended_subscriptions.get_mut(upstream_mv_table_id) {
                Some(subscriptions) => {
                    let removed = subscriptions.remove(subscription_id).is_some();
                    if removed && subscriptions.is_empty() {
                        self.mv_depended_subscriptions.remove(upstream_mv_table_id);
                    }
                    removed
                }
                None => false,
            };
            if !removed {
                warn!(subscription_id, ?upstream_mv_table_id, mv_depended_subscriptions = ?self.mv_depended_subscriptions, "remove a non-existing subscription id");
            }
        }
    }
}

impl InflightFragmentInfo {
    /// Returns actor list to collect in the target worker node.
    pub(crate) fn actor_ids_to_collect(
        infos: impl IntoIterator<Item = &Self>,
    ) -> HashMap<WorkerId, HashSet<ActorId>> {
        let mut ret: HashMap<_, HashSet<_>> = HashMap::new();
        for (actor_id, worker_id) in infos.into_iter().flat_map(|info| info.actors.iter()) {
            assert!(ret
                .entry(*worker_id as WorkerId)
                .or_default()
                .insert(*actor_id as _))
        }
        ret
    }

    pub fn existing_table_ids<'a>(
        infos: impl IntoIterator<Item = &'a Self> + 'a,
    ) -> impl Iterator<Item = TableId> + 'a {
        infos
            .into_iter()
            .flat_map(|info| info.state_table_ids.iter().cloned())
    }

    pub fn contains_worker(infos: impl IntoIterator<Item = &Self>, worker_id: WorkerId) -> bool {
        infos.into_iter().any(|fragment| {
            fragment
                .actors
                .values()
                .any(|location| (*location as WorkerId) == worker_id)
        })
    }
}

impl InflightDatabaseInfo {
    pub fn contains_worker(&self, worker_id: WorkerId) -> bool {
        InflightFragmentInfo::contains_worker(self.fragment_infos(), worker_id)
    }

    pub fn existing_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        InflightFragmentInfo::existing_table_ids(self.fragment_infos())
    }
}
