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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::OsString;
use std::fmt::Write;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use cfg_or_panic::cfg_or_panic;
use clap::Parser;
use itertools::Itertools;
use rand::seq::IteratorRandom;
use rand::{thread_rng, Rng};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::WorkerSlotId;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_hummock_sdk::{CompactionGroupId, HummockSstableId};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::PbFragment;
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::stream_plan::StreamNode;

use self::predicate::BoxedPredicate;
use crate::cluster::Cluster;

/// Predicates used for locating fragments.
pub mod predicate {
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::DispatcherType;

    use super::*;

    trait Predicate = Fn(&PbFragment) -> bool + Send + 'static;
    pub type BoxedPredicate = Box<dyn Predicate>;

    fn root(fragment: &PbFragment) -> &StreamNode {
        fragment.nodes.as_ref().unwrap()
    }

    fn count(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> usize {
        let child = root.input.iter().map(|n| count(n, p)).sum::<usize>();
        child + if p(root) { 1 } else { 0 }
    }

    fn any(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> bool {
        p(root) || root.input.iter().any(|n| any(n, p))
    }

    fn all(root: &StreamNode, p: &impl Fn(&StreamNode) -> bool) -> bool {
        p(root) && root.input.iter().all(|n| all(n, p))
    }

    /// There're exactly `n` operators whose identity contains `s` in the fragment.
    pub fn identity_contains_n(n: usize, s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            count(root(f), &|n| {
                n.identity.to_lowercase().contains(&s.to_lowercase())
            }) == n
        };
        Box::new(p)
    }

    /// There exists operators whose identity contains `s` in the fragment (case insensitive).
    pub fn identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            any(root(f), &|n| {
                n.identity.to_lowercase().contains(&s.to_lowercase())
            })
        };
        Box::new(p)
    }

    /// There does not exist any operator whose identity contains `s` in the fragment.
    pub fn no_identity_contains(s: impl Into<String>) -> BoxedPredicate {
        let s: String = s.into();
        let p = move |f: &PbFragment| {
            all(root(f), &|n| {
                !n.identity.to_lowercase().contains(&s.to_lowercase())
            })
        };
        Box::new(p)
    }

    /// There're `n` upstream fragments of the fragment.
    pub fn upstream_fragment_count(n: usize) -> BoxedPredicate {
        let p = move |f: &PbFragment| f.upstream_fragment_ids.len() == n;
        Box::new(p)
    }

    /// The fragment is able to be rescheduled. Used for locating random fragment.
    pub fn can_reschedule() -> BoxedPredicate {
        let p = |f: &PbFragment| {
            // The rescheduling of no-shuffle downstreams must be derived from the most upstream
            // fragment. So if a fragment has no-shuffle upstreams, it cannot be rescheduled.
            !any(root(f), &|n| {
                let Some(NodeBody::Merge(merge)) = &n.node_body else {
                    return false;
                };
                merge.upstream_dispatcher_type() == DispatcherType::NoShuffle
            })
        };
        Box::new(p)
    }

    /// The fragment with the given id.
    pub fn id(id: u32) -> BoxedPredicate {
        let p = move |f: &PbFragment| f.fragment_id == id;
        Box::new(p)
    }
}

#[derive(Debug)]
pub struct Fragment {
    pub inner: risingwave_pb::meta::table_fragments::Fragment,

    r: Arc<GetClusterInfoResponse>,
}

impl Fragment {
    /// The fragment id.
    pub fn id(&self) -> u32 {
        self.inner.fragment_id
    }

    /// Generate a reschedule plan for the fragment.
    pub fn reschedule(
        &self,
        remove: impl AsRef<[WorkerSlotId]>,
        add: impl AsRef<[WorkerSlotId]>,
    ) -> String {
        let remove = remove.as_ref();
        let add = add.as_ref();

        let mut worker_decreased = HashMap::new();
        for worker_slot in remove {
            let worker_id = worker_slot.worker_id();
            *worker_decreased.entry(worker_id).or_insert(0) += 1;
        }

        let mut worker_increased = HashMap::new();
        for worker_slot in add {
            let worker_id = worker_slot.worker_id();
            *worker_increased.entry(worker_id).or_insert(0) += 1;
        }

        let worker_ids: HashSet<_> = worker_increased
            .keys()
            .chain(worker_decreased.keys())
            .cloned()
            .collect();

        let mut worker_actor_diff = HashMap::new();

        for worker_id in worker_ids {
            let increased = worker_increased.remove(&worker_id).unwrap_or(0);
            let decreased = worker_decreased.remove(&worker_id).unwrap_or(0);
            let diff = increased - decreased;
            if diff != 0 {
                worker_actor_diff.insert(worker_id, diff);
            }
        }

        let mut f = String::new();

        if !worker_actor_diff.is_empty() {
            let worker_diff_str = worker_actor_diff
                .into_iter()
                .map(|(k, v)| format!("{}:{}", k, v))
                .join(", ");

            write!(f, "{}", self.id()).unwrap();
            write!(f, ":[{}]", worker_diff_str).unwrap();
        }

        f
    }

    /// Generate a random reschedule plan for the fragment.
    ///
    /// Consumes `self` as the actor info will be stale after rescheduling.
    pub fn random_reschedule(self) -> String {
        let all_worker_slots = self.all_worker_slots();
        let used_worker_slots = self.used_worker_slots();

        let rng = &mut thread_rng();
        let target_worker_slot_count = match self.inner.distribution_type() {
            FragmentDistributionType::Unspecified => unreachable!(),
            FragmentDistributionType::Single => 1,
            FragmentDistributionType::Hash => rng.gen_range(1..=all_worker_slots.len()),
        };

        let target_worker_slots: HashSet<_> = all_worker_slots
            .into_iter()
            .choose_multiple(rng, target_worker_slot_count)
            .into_iter()
            .collect();

        let remove = used_worker_slots
            .difference(&target_worker_slots)
            .copied()
            .collect_vec();

        let add = target_worker_slots
            .difference(&used_worker_slots)
            .copied()
            .collect_vec();

        self.reschedule(remove, add)
    }

    pub fn all_worker_count(&self) -> HashMap<u32, usize> {
        self.r
            .worker_nodes
            .iter()
            .map(|w| (w.id, w.compute_node_parallelism()))
            .collect()
    }

    pub fn all_worker_slots(&self) -> HashSet<WorkerSlotId> {
        self.all_worker_count()
            .into_iter()
            .flat_map(|(k, v)| (0..v).map(move |idx| WorkerSlotId::new(k, idx as _)))
            .collect()
    }

    pub fn parallelism(&self) -> usize {
        self.inner.actors.len()
    }

    pub fn used_worker_count(&self) -> HashMap<u32, usize> {
        let actor_to_worker: HashMap<_, _> = self
            .r
            .table_fragments
            .iter()
            .flat_map(|tf| {
                tf.actor_status
                    .iter()
                    .map(|(&actor_id, status)| (actor_id, status.worker_id()))
            })
            .collect();

        self.inner
            .actors
            .iter()
            .map(|a| actor_to_worker[&a.actor_id])
            .fold(HashMap::<u32, usize>::new(), |mut acc, num| {
                *acc.entry(num).or_insert(0) += 1;
                acc
            })
    }

    pub fn used_worker_slots(&self) -> HashSet<WorkerSlotId> {
        self.used_worker_count()
            .into_iter()
            .flat_map(|(k, v)| (0..v).map(move |idx| WorkerSlotId::new(k, idx as _)))
            .collect()
    }
}

impl Cluster {
    /// Locate fragments that satisfy all the predicates.
    #[cfg_or_panic(madsim)]
    pub async fn locate_fragments(
        &mut self,
        predicates: impl IntoIterator<Item = BoxedPredicate>,
    ) -> Result<Vec<Fragment>> {
        let predicates = predicates.into_iter().collect_vec();

        let fragments = self
            .ctl
            .spawn(async move {
                let r: Arc<_> = risingwave_ctl::cmd_impl::meta::get_cluster_info(
                    &risingwave_ctl::common::CtlContext::default(),
                )
                .await?
                .into();

                let mut results = vec![];
                for tf in &r.table_fragments {
                    for f in tf.fragments.values() {
                        let selected = predicates.iter().all(|p| p(f));
                        if selected {
                            results.push(Fragment {
                                inner: f.clone(),
                                r: r.clone(),
                            });
                        }
                    }
                }

                Ok::<_, anyhow::Error>(results)
            })
            .await??;

        Ok(fragments)
    }

    /// Locate exactly one fragment that satisfies all the predicates.
    pub async fn locate_one_fragment(
        &mut self,
        predicates: impl IntoIterator<Item = BoxedPredicate>,
    ) -> Result<Fragment> {
        let [fragment]: [_; 1] = self
            .locate_fragments(predicates)
            .await?
            .try_into()
            .map_err(|fs| anyhow!("not exactly one fragment: {fs:#?}"))?;
        Ok(fragment)
    }

    /// Locate a random fragment that is reschedulable.
    pub async fn locate_random_fragment(&mut self) -> Result<Fragment> {
        self.locate_fragments([predicate::can_reschedule()])
            .await?
            .into_iter()
            .choose(&mut thread_rng())
            .ok_or_else(|| anyhow!("no reschedulable fragment"))
    }

    /// Locate some random fragments that are reschedulable.
    pub async fn locate_random_fragments(&mut self) -> Result<Vec<Fragment>> {
        let fragments = self.locate_fragments([predicate::can_reschedule()]).await?;
        let len = thread_rng().gen_range(1..=fragments.len());
        let selected = fragments
            .into_iter()
            .choose_multiple(&mut thread_rng(), len);
        Ok(selected)
    }

    /// Locate a fragment with the given id.
    pub async fn locate_fragment_by_id(&mut self, id: u32) -> Result<Fragment> {
        self.locate_one_fragment([predicate::id(id)]).await
    }

    #[cfg_or_panic(madsim)]
    pub async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        let response = self
            .ctl
            .spawn(async move {
                risingwave_ctl::cmd_impl::meta::get_cluster_info(
                    &risingwave_ctl::common::CtlContext::default(),
                )
                .await
            })
            .await??;
        Ok(response)
    }

    /// `actor_id -> splits`
    pub async fn list_source_splits(&self) -> Result<BTreeMap<u32, String>> {
        let info = self.get_cluster_info().await?;
        let mut res = BTreeMap::new();

        for table in info.table_fragments {
            for (actor_id, splits) in table.actor_splits {
                let splits = splits
                    .splits
                    .iter()
                    .map(|split| SplitImpl::try_from(split).unwrap())
                    .map(|split| split.id())
                    .collect_vec()
                    .join(",");
                res.insert(actor_id, splits);
            }
        }

        Ok(res)
    }

    // update node schedulability
    #[cfg_or_panic(madsim)]
    async fn update_worker_node_schedulability(
        &self,
        worker_ids: Vec<u32>,
        target: Schedulability,
    ) -> Result<()> {
        let worker_ids = worker_ids
            .into_iter()
            .map(|id| id.to_string())
            .collect_vec();

        let _ = self
            .ctl
            .spawn(async move {
                risingwave_ctl::cmd_impl::scale::update_schedulability(
                    &risingwave_ctl::common::CtlContext::default(),
                    worker_ids,
                    target,
                )
                .await
            })
            .await?;
        Ok(())
    }

    pub async fn cordon_worker(&self, id: u32) -> Result<()> {
        self.update_worker_node_schedulability(vec![id], Schedulability::Unschedulable)
            .await
    }

    pub async fn uncordon_worker(&self, id: u32) -> Result<()> {
        self.update_worker_node_schedulability(vec![id], Schedulability::Schedulable)
            .await
    }

    /// Reschedule with the given `plan`. Check the document of
    /// [`risingwave_ctl::cmd_impl::meta::reschedule`] for more details.
    pub async fn reschedule(&mut self, plan: impl Into<String>) -> Result<()> {
        self.reschedule_helper(plan, false).await
    }

    /// Same as reschedule, but resolve the no-shuffle upstream
    pub async fn reschedule_resolve_no_shuffle(&mut self, plan: impl Into<String>) -> Result<()> {
        self.reschedule_helper(plan, true).await
    }

    #[cfg_or_panic(madsim)]
    async fn reschedule_helper(
        &mut self,
        plan: impl Into<String>,
        resolve_no_shuffle_upstream: bool,
    ) -> Result<()> {
        let plan = plan.into();

        let revision = self
            .ctl
            .spawn(async move {
                let r = risingwave_ctl::cmd_impl::meta::get_cluster_info(
                    &risingwave_ctl::common::CtlContext::default(),
                )
                .await?;

                Ok::<_, anyhow::Error>(r.revision)
            })
            .await??;

        self.ctl
            .spawn(async move {
                let revision = format!("{}", revision);
                let mut v = vec![
                    "meta",
                    "reschedule",
                    "--plan",
                    plan.as_ref(),
                    "--revision",
                    &revision,
                ];

                if resolve_no_shuffle_upstream {
                    v.push("--resolve-no-shuffle");
                }

                start_ctl(v).await
            })
            .await??;

        Ok(())
    }

    /// Pause all data sources in the cluster.
    #[cfg_or_panic(madsim)]
    pub async fn pause(&mut self) -> Result<()> {
        self.ctl.spawn(start_ctl(["meta", "pause"])).await??;
        Ok(())
    }

    /// Resume all data sources in the cluster.
    #[cfg_or_panic(madsim)]
    pub async fn resume(&mut self) -> Result<()> {
        self.ctl.spawn(start_ctl(["meta", "resume"])).await??;
        Ok(())
    }

    /// Throttle a Mv in the cluster
    #[cfg_or_panic(madsim)]
    pub async fn throttle_mv(&mut self, table_id: TableId, rate_limit: Option<u32>) -> Result<()> {
        self.ctl
            .spawn(async move {
                let mut command: Vec<String> = vec![
                    "throttle".into(),
                    "mv".into(),
                    table_id.table_id.to_string(),
                ];
                if let Some(rate_limit) = rate_limit {
                    command.push(rate_limit.to_string());
                }
                start_ctl(command).await
            })
            .await??;
        Ok(())
    }

    #[cfg_or_panic(madsim)]
    pub async fn split_compaction_group(
        &mut self,
        compaction_group_id: CompactionGroupId,
        table_id: HummockSstableId,
    ) -> Result<()> {
        self.ctl
            .spawn(async move {
                let mut command: Vec<String> = vec![
                    "hummock".into(),
                    "split-compaction-group".into(),
                    "--compaction-group-id".into(),
                    compaction_group_id.to_string(),
                    "--table-ids".into(),
                    table_id.to_string(),
                ];
                start_ctl(command).await
            })
            .await??;
        Ok(())
    }

    #[cfg_or_panic(madsim)]
    pub async fn trigger_manual_compaction(
        &mut self,
        compaction_group_id: CompactionGroupId,
        level_id: u32,
    ) -> Result<()> {
        self.ctl
            .spawn(async move {
                let mut command: Vec<String> = vec![
                    "hummock".into(),
                    "trigger-manual-compaction".into(),
                    "--compaction-group-id".into(),
                    compaction_group_id.to_string(),
                    "--level".into(),
                    level_id.to_string(),
                ];
                start_ctl(command).await
            })
            .await??;
        Ok(())
    }
}

#[cfg_attr(not(madsim), allow(dead_code))]
async fn start_ctl<S, I>(args: I) -> Result<()>
where
    S: Into<OsString>,
    I: IntoIterator<Item = S>,
{
    let args = std::iter::once("ctl".into()).chain(args.into_iter().map(|s| s.into()));
    let opts = risingwave_ctl::CliOpts::parse_from(args);
    let context = risingwave_ctl::common::CtlContext::default();
    risingwave_ctl::start_fallible(opts, &context).await
}
