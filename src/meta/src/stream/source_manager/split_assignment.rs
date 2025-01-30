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

use itertools::Itertools;

use super::*;

impl SourceManager {
    /// Migrates splits from previous actors to the new actors for a rescheduled fragment.
    ///
    /// Very occasionally split removal may happen during scaling, in which case we need to
    /// use the old splits for reallocation instead of the latest splits (which may be missing),
    /// so that we can resolve the split removal in the next command.
    pub async fn migrate_splits_for_source_actors(
        &self,
        fragment_id: FragmentId,
        prev_actor_ids: &[ActorId],
        curr_actor_ids: &[ActorId],
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        let core = self.core.lock().await;

        let prev_splits = prev_actor_ids
            .iter()
            .flat_map(|actor_id| core.actor_splits.get(actor_id).unwrap())
            .map(|split| (split.id(), split.clone()))
            .collect();

        let empty_actor_splits = curr_actor_ids
            .iter()
            .map(|actor_id| (*actor_id, vec![]))
            .collect();

        let diff = reassign_splits(
            fragment_id,
            empty_actor_splits,
            &prev_splits,
            // pre-allocate splits is the first time getting splits and it does not have scale-in scene
            SplitDiffOptions::default(),
        )
        .unwrap_or_default();

        Ok(diff)
    }

    /// Migrates splits from previous actors to the new actors for a rescheduled fragment.
    pub fn migrate_splits_for_backfill_actors(
        &self,
        fragment_id: FragmentId,
        upstream_source_fragment_id: FragmentId,
        curr_actor_ids: &[ActorId],
        fragment_actor_splits: &HashMap<FragmentId, HashMap<ActorId, Vec<SplitImpl>>>,
        no_shuffle_upstream_actor_map: &HashMap<ActorId, HashMap<FragmentId, ActorId>>,
    ) -> MetaResult<HashMap<ActorId, Vec<SplitImpl>>> {
        // align splits for backfill fragments with its upstream source fragment
        let actors = no_shuffle_upstream_actor_map
            .iter()
            .filter(|(id, _)| curr_actor_ids.contains(id))
            .map(|(id, upstream_fragment_actors)| {
                (
                    *id,
                    *upstream_fragment_actors
                        .get(&upstream_source_fragment_id)
                        .unwrap(),
                )
            });
        let upstream_assignment = fragment_actor_splits
            .get(&upstream_source_fragment_id)
            .unwrap();
        tracing::info!(
            fragment_id,
            upstream_source_fragment_id,
            ?upstream_assignment,
            "migrate_splits_for_backfill_actors"
        );
        Ok(align_splits(
            actors,
            upstream_assignment,
            fragment_id,
            upstream_source_fragment_id,
        )?)
    }

    /// Allocates splits to actors for a newly created source executor.
    pub async fn allocate_splits(&self, job_id: &TableId) -> MetaResult<SplitAssignment> {
        let core = self.core.lock().await;
        let table_fragments = core
            .metadata_manager
            .get_job_fragments_by_id(job_id)
            .await?;

        let source_fragments = table_fragments.stream_source_fragments();

        let mut assigned = HashMap::new();

        'loop_source: for (source_id, fragments) in source_fragments {
            let handle = core
                .managed_sources
                .get(&source_id)
                .with_context(|| format!("could not find source {}", source_id))?;

            if handle.splits.lock().await.splits.is_none() {
                handle.force_tick().await?;
            }

            for fragment_id in fragments {
                let empty_actor_splits: HashMap<u32, Vec<SplitImpl>> = table_fragments
                    .fragments
                    .get(&fragment_id)
                    .unwrap()
                    .actors
                    .iter()
                    .map(|actor| (actor.actor_id, vec![]))
                    .collect();
                let actor_hashset: HashSet<u32> = empty_actor_splits.keys().cloned().collect();
                let splits = handle.discovered_splits(source_id, &actor_hashset).await?;
                if splits.is_empty() {
                    tracing::warn!("no splits detected for source {}", source_id);
                    continue 'loop_source;
                }

                if let Some(diff) = reassign_splits(
                    fragment_id,
                    empty_actor_splits,
                    &splits,
                    SplitDiffOptions::default(),
                ) {
                    assigned.insert(fragment_id, diff);
                }
            }
        }

        Ok(assigned)
    }

    /// Allocates splits to actors for replace source job.
    pub async fn allocate_splits_for_replace_source(
        &self,
        job_id: &TableId,
        merge_updates: &HashMap<FragmentId, Vec<MergeUpdate>>,
    ) -> MetaResult<SplitAssignment> {
        tracing::debug!(?merge_updates, "allocate_splits_for_replace_source");
        if merge_updates.is_empty() {
            // no existing downstream. We can just re-allocate splits arbitrarily.
            return self.allocate_splits(job_id).await;
        }

        let core = self.core.lock().await;
        let table_fragments = core
            .metadata_manager
            .get_job_fragments_by_id(job_id)
            .await?;

        let source_fragments = table_fragments.stream_source_fragments();
        assert_eq!(
            source_fragments.len(),
            1,
            "replace source job should only have one source"
        );
        let (_source_id, fragments) = source_fragments.into_iter().next().unwrap();
        assert_eq!(
            fragments.len(),
            1,
            "replace source job should only have one fragment"
        );
        let fragment_id = fragments.into_iter().next().unwrap();

        debug_assert!(
            merge_updates.values().flatten().next().is_some()
                && merge_updates.values().flatten().all(|merge_update| {
                    merge_update.new_upstream_fragment_id == Some(fragment_id)
                })
                && merge_updates
                    .values()
                    .flatten()
                    .map(|merge_update| merge_update.upstream_fragment_id)
                    .all_equal(),
            "merge update should only replace one fragment: {:?}",
            merge_updates
        );
        let prev_fragment_id = merge_updates
            .values()
            .flatten()
            .next()
            .expect("non-empty")
            .upstream_fragment_id;
        // Here we align the new source executor to backfill executors
        //
        // old_source => new_source            backfill_1
        // actor_x1   => actor_y1 -----┬------>actor_a1
        // actor_x2   => actor_y2 -----┼-┬---->actor_a2
        //                             │ │
        //                             │ │     backfill_2
        //                             └─┼---->actor_b1
        //                               └---->actor_b2
        //
        // Note: we can choose any backfill actor to align here.
        // We use `HashMap` to dedup.
        let aligned_actors: HashMap<ActorId, ActorId> = merge_updates
            .values()
            .flatten()
            .map(|merge_update| {
                assert_eq!(merge_update.added_upstream_actor_id.len(), 1);
                // Note: removed_upstream_actor_id is not set for replace job, so we can't use it.
                assert_eq!(merge_update.removed_upstream_actor_id.len(), 0);
                (
                    merge_update.added_upstream_actor_id[0],
                    merge_update.actor_id,
                )
            })
            .collect();
        let assignment = align_splits(
            aligned_actors.into_iter(),
            &core.actor_splits,
            fragment_id,
            prev_fragment_id,
        )?;
        Ok(HashMap::from([(fragment_id, assignment)]))
    }

    /// Allocates splits to actors for a newly created `SourceBackfill` executor.
    ///
    /// Unlike [`Self::allocate_splits`], which creates a new assignment,
    /// this method aligns the splits for backfill fragments with its upstream source fragment ([`align_splits`]).
    pub async fn allocate_splits_for_backfill(
        &self,
        table_id: &TableId,
        // dispatchers from SourceExecutor to SourceBackfillExecutor
        dispatchers: &HashMap<FragmentId, HashMap<ActorId, Vec<Dispatcher>>>,
    ) -> MetaResult<SplitAssignment> {
        let core = self.core.lock().await;
        let table_fragments = core
            .metadata_manager
            .get_job_fragments_by_id(table_id)
            .await?;

        let source_backfill_fragments = table_fragments.source_backfill_fragments()?;

        let mut assigned = HashMap::new();

        for (_source_id, fragments) in source_backfill_fragments {
            for (fragment_id, upstream_source_fragment_id) in fragments {
                let fragment_dispatchers = dispatchers.get(&upstream_source_fragment_id);
                let upstream_actors = core
                    .metadata_manager
                    .get_running_actors_of_fragment(upstream_source_fragment_id)
                    .await?;
                let mut backfill_actors = vec![];
                for upstream_actor in upstream_actors {
                    if let Some(dispatchers) = fragment_dispatchers
                        .and_then(|dispatchers| dispatchers.get(&upstream_actor))
                    {
                        let err = || {
                            anyhow::anyhow!(
                            "source backfill fragment's upstream fragment should have one dispatcher, fragment_id: {fragment_id}, upstream_fragment_id: {upstream_source_fragment_id}, upstream_actor: {upstream_actor}, dispatchers: {dispatchers:?}",
                            fragment_id = fragment_id,
                            upstream_source_fragment_id = upstream_source_fragment_id,
                            upstream_actor = upstream_actor,
                            dispatchers = dispatchers
                        )
                        };
                        if dispatchers.len() != 1 || dispatchers[0].downstream_actor_id.len() != 1 {
                            return Err(err().into());
                        }

                        backfill_actors
                            .push((dispatchers[0].downstream_actor_id[0], upstream_actor));
                    }
                }
                assigned.insert(
                    fragment_id,
                    align_splits(
                        backfill_actors,
                        &core.actor_splits,
                        fragment_id,
                        upstream_source_fragment_id,
                    )?,
                );
            }
        }

        Ok(assigned)
    }
}

impl SourceManagerCore {
    /// Checks whether the external source metadata has changed,
    /// and re-assigns splits if there's a diff.
    ///
    /// `self.actor_splits` will not be updated. It will be updated by `Self::apply_source_change`,
    /// after the mutation barrier has been collected.
    pub async fn reassign_splits(&self) -> MetaResult<HashMap<DatabaseId, SplitAssignment>> {
        let mut split_assignment: SplitAssignment = HashMap::new();

        'loop_source: for (source_id, handle) in &self.managed_sources {
            let source_fragment_ids = match self.source_fragments.get(source_id) {
                Some(fragment_ids) if !fragment_ids.is_empty() => fragment_ids,
                _ => {
                    continue;
                }
            };
            let backfill_fragment_ids = self.backfill_fragments.get(source_id);

            'loop_fragment: for &fragment_id in source_fragment_ids {
                let actors = match self
                    .metadata_manager
                    .get_running_actors_of_fragment(fragment_id)
                    .await
                {
                    Ok(actors) => {
                        if actors.is_empty() {
                            tracing::warn!("No actors found for fragment {}", fragment_id);
                            continue 'loop_fragment;
                        }
                        actors
                    }
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "Failed to get the actor of the fragment, maybe the fragment doesn't exist anymore");
                        continue 'loop_fragment;
                    }
                };

                let discovered_splits = handle.discovered_splits(*source_id, &actors).await?;
                if discovered_splits.is_empty() {
                    // The discover loop for this source is not ready yet; we'll wait for the next run
                    continue 'loop_source;
                }

                let prev_actor_splits: HashMap<_, _> = actors
                    .into_iter()
                    .map(|actor_id| {
                        (
                            actor_id,
                            self.actor_splits
                                .get(&actor_id)
                                .cloned()
                                .unwrap_or_default(),
                        )
                    })
                    .collect();

                if let Some(new_assignment) = reassign_splits(
                    fragment_id,
                    prev_actor_splits,
                    &discovered_splits,
                    SplitDiffOptions {
                        enable_scale_in: handle.enable_drop_split,
                        enable_adaptive: handle.enable_adaptive_splits,
                    },
                ) {
                    split_assignment.insert(fragment_id, new_assignment);
                }
            }

            if let Some(backfill_fragment_ids) = backfill_fragment_ids {
                // align splits for backfill fragments with its upstream source fragment
                for (fragment_id, upstream_fragment_id) in backfill_fragment_ids {
                    let Some(upstream_assignment) = split_assignment.get(upstream_fragment_id)
                    else {
                        // upstream fragment unchanged, do not update backfill fragment too
                        continue;
                    };
                    let actors = match self
                        .metadata_manager
                        .get_running_actors_for_source_backfill(*fragment_id, *upstream_fragment_id)
                        .await
                    {
                        Ok(actors) => {
                            if actors.is_empty() {
                                tracing::warn!("No actors found for fragment {}", fragment_id);
                                continue;
                            }
                            actors
                        }
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(),"Failed to get the actor of the fragment, maybe the fragment doesn't exist anymore");
                            continue;
                        }
                    };
                    split_assignment.insert(
                        *fragment_id,
                        align_splits(
                            actors,
                            upstream_assignment,
                            *fragment_id,
                            *upstream_fragment_id,
                        )?,
                    );
                }
            }
        }

        self.metadata_manager
            .split_fragment_map_by_database(split_assignment)
            .await
    }
}

/// Reassigns splits if there are new splits or dropped splits,
/// i.e., `actor_splits` and `discovered_splits` differ, or actors are rescheduled.
///
/// The existing splits will remain unmoved in their currently assigned actor.
///
/// If an actor has an upstream actor, it should be a backfill executor,
/// and its splits should be aligned with the upstream actor. **`reassign_splits` should not be used in this case.
/// Use [`align_splits`] instead.**
///
/// - `fragment_id`: just for logging
///
/// ## Different connectors' behavior of split change
///
/// ### Kafka and Pulsar
/// They only support increasing the number of splits via adding new empty splits.
/// Old data is not moved.
///
/// ### Kinesis
/// It supports *pairwise* shard split and merge.
///
/// In both cases, old data remain in the old shard(s) and the old shard is still available.
/// New data are routed to the new shard(s).
/// After the retention period has expired, the old shard will become `EXPIRED` and isn't
/// listed any more. In other words, the total number of shards will first increase and then decrease.
///
/// See also:
/// - [Kinesis resharding doc](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-after-resharding.html#kinesis-using-sdk-java-resharding-data-routing)
/// - An example of how the shards can be like: <https://stackoverflow.com/questions/72272034/list-shard-show-more-shards-than-provisioned>
fn reassign_splits<T>(
    fragment_id: FragmentId,
    actor_splits: HashMap<ActorId, Vec<T>>,
    discovered_splits: &BTreeMap<SplitId, T>,
    opts: SplitDiffOptions,
) -> Option<HashMap<ActorId, Vec<T>>>
where
    T: SplitMetaData + Clone,
{
    // if no actors, return
    if actor_splits.is_empty() {
        return None;
    }

    let prev_split_ids: HashSet<_> = actor_splits
        .values()
        .flat_map(|splits| splits.iter().map(SplitMetaData::id))
        .collect();

    tracing::trace!(fragment_id, prev_split_ids = ?prev_split_ids, "previous splits");
    tracing::trace!(fragment_id, prev_split_ids = ?discovered_splits.keys(), "discovered splits");

    let discovered_split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

    let dropped_splits: HashSet<_> = prev_split_ids
        .difference(&discovered_split_ids)
        .cloned()
        .collect();

    if !dropped_splits.is_empty() {
        if opts.enable_scale_in {
            tracing::info!(fragment_id, dropped_spltis = ?dropped_splits, "new dropped splits");
        } else {
            tracing::warn!(fragment_id, dropped_spltis = ?dropped_splits, "split dropping happened, but it is not allowed");
        }
    }

    let new_discovered_splits: BTreeSet<_> = discovered_split_ids
        .into_iter()
        .filter(|split_id| !prev_split_ids.contains(split_id))
        .collect();

    if opts.enable_scale_in || opts.enable_adaptive {
        // if we support scale in, no more splits are discovered, and no splits are dropped, return
        // we need to check if discovered_split_ids is empty, because if it is empty, we need to
        // handle the case of scale in to zero (like deleting all objects from s3)
        if dropped_splits.is_empty()
            && new_discovered_splits.is_empty()
            && !discovered_splits.is_empty()
        {
            return None;
        }
    } else {
        // if we do not support scale in, and no more splits are discovered, return
        if new_discovered_splits.is_empty() && !discovered_splits.is_empty() {
            return None;
        }
    }

    tracing::info!(fragment_id, new_discovered_splits = ?new_discovered_splits, "new discovered splits");

    let mut heap = BinaryHeap::with_capacity(actor_splits.len());

    for (actor_id, mut splits) in actor_splits {
        if opts.enable_scale_in || opts.enable_adaptive {
            splits.retain(|split| !dropped_splits.contains(&split.id()));
        }

        heap.push(ActorSplitsAssignment { actor_id, splits })
    }

    for split_id in new_discovered_splits {
        // ActorSplitsAssignment's Ord is reversed, so this is min heap, i.e.,
        // we get the assignment with the least splits here.

        // Note: If multiple actors have the same number of splits, it will be randomly picked.
        // When the number of source actors is larger than the number of splits,
        // It's possible that the assignment is uneven.
        // e.g., https://github.com/risingwavelabs/risingwave/issues/14324#issuecomment-1875033158
        // TODO: We should make the assignment rack-aware to make sure it's even.
        let mut peek_ref = heap.peek_mut().unwrap();
        peek_ref
            .splits
            .push(discovered_splits.get(&split_id).cloned().unwrap());
    }

    Some(
        heap.into_iter()
            .map(|ActorSplitsAssignment { actor_id, splits }| (actor_id, splits))
            .collect(),
    )
}

/// Assign splits to a new set of actors, according to existing assignment.
///
/// illustration:
/// ```text
/// upstream                               new
/// actor x1 [split 1, split2]      ->     actor y1 [split 1, split2]
/// actor x2 [split 3]              ->     actor y2 [split 3]
/// ...
/// ```
fn align_splits(
    // (actor_id, upstream_actor_id)
    aligned_actors: impl IntoIterator<Item = (ActorId, ActorId)>,
    existing_assignment: &HashMap<ActorId, Vec<SplitImpl>>,
    fragment_id: FragmentId,
    upstream_source_fragment_id: FragmentId,
) -> anyhow::Result<HashMap<ActorId, Vec<SplitImpl>>> {
    aligned_actors
        .into_iter()
        .map(|(actor_id, upstream_actor_id)| {
            let Some(splits) = existing_assignment.get(&upstream_actor_id) else {
                return Err(anyhow::anyhow!("upstream assignment not found, fragment_id: {fragment_id}, upstream_fragment_id: {upstream_source_fragment_id}, actor_id: {actor_id}, upstream_assignment: {existing_assignment:?}, upstream_actor_id: {upstream_actor_id:?}"));
            };
            Ok((
                actor_id,
                splits.clone(),
            ))
        })
        .collect()
}

/// Note: the `PartialEq` and `Ord` impl just compares the number of splits.
#[derive(Debug)]
struct ActorSplitsAssignment<T: SplitMetaData> {
    actor_id: ActorId,
    splits: Vec<T>,
}

impl<T: SplitMetaData + Clone> Eq for ActorSplitsAssignment<T> {}

impl<T: SplitMetaData + Clone> PartialEq<Self> for ActorSplitsAssignment<T> {
    fn eq(&self, other: &Self) -> bool {
        self.splits.len() == other.splits.len()
    }
}

impl<T: SplitMetaData + Clone> PartialOrd<Self> for ActorSplitsAssignment<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: SplitMetaData + Clone> Ord for ActorSplitsAssignment<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Note: this is reversed order, to make BinaryHeap a min heap.
        other.splits.len().cmp(&self.splits.len())
    }
}

#[derive(Debug)]
pub struct SplitDiffOptions {
    pub enable_scale_in: bool,

    /// For most connectors, this should be false. When enabled, RisingWave will not track any progress.
    pub enable_adaptive: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for SplitDiffOptions {
    fn default() -> Self {
        SplitDiffOptions {
            enable_scale_in: false,
            enable_adaptive: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use risingwave_common::types::JsonbVal;
    use risingwave_connector::error::ConnectorResult;
    use risingwave_connector::source::{SplitId, SplitMetaData};
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::model::{ActorId, FragmentId};

    #[derive(Debug, Copy, Clone, Serialize, Deserialize)]
    struct TestSplit {
        id: u32,
    }

    impl SplitMetaData for TestSplit {
        fn id(&self) -> SplitId {
            format!("{}", self.id).into()
        }

        fn encode_to_json(&self) -> JsonbVal {
            serde_json::to_value(*self).unwrap().into()
        }

        fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
            serde_json::from_value(value.take()).map_err(Into::into)
        }

        fn update_offset(&mut self, _last_read_offset: String) -> ConnectorResult<()> {
            Ok(())
        }
    }

    fn check_all_splits(
        discovered_splits: &BTreeMap<SplitId, TestSplit>,
        diff: &HashMap<ActorId, Vec<TestSplit>>,
    ) {
        let mut split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

        for splits in diff.values() {
            for split in splits {
                assert!(split_ids.remove(&split.id()))
            }
        }

        assert!(split_ids.is_empty());
    }

    #[test]
    fn test_drop_splits() {
        let mut actor_splits: HashMap<ActorId, _> = HashMap::new();
        actor_splits.insert(0, vec![TestSplit { id: 0 }, TestSplit { id: 1 }]);
        actor_splits.insert(1, vec![TestSplit { id: 2 }, TestSplit { id: 3 }]);
        actor_splits.insert(2, vec![TestSplit { id: 4 }, TestSplit { id: 5 }]);

        let mut prev_split_to_actor = HashMap::new();
        for (actor_id, splits) in &actor_splits {
            for split in splits {
                prev_split_to_actor.insert(split.id(), *actor_id);
            }
        }

        let discovered_splits: BTreeMap<SplitId, TestSplit> = (1..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let opts = SplitDiffOptions {
            enable_scale_in: true,
            enable_adaptive: false,
        };

        let prev_split_ids: HashSet<_> = actor_splits
            .values()
            .flat_map(|splits| splits.iter().map(|split| split.id()))
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            opts,
        )
        .unwrap();
        check_all_splits(&discovered_splits, &diff);

        let mut after_split_to_actor = HashMap::new();
        for (actor_id, splits) in &diff {
            for split in splits {
                after_split_to_actor.insert(split.id(), *actor_id);
            }
        }

        let discovered_split_ids: HashSet<_> = discovered_splits.keys().cloned().collect();

        let retained_split_ids: HashSet<_> =
            prev_split_ids.intersection(&discovered_split_ids).collect();

        for retained_split_id in retained_split_ids {
            assert_eq!(
                prev_split_to_actor.get(retained_split_id),
                after_split_to_actor.get(retained_split_id)
            )
        }
    }

    #[test]
    fn test_drop_splits_to_empty() {
        let mut actor_splits: HashMap<ActorId, _> = HashMap::new();
        actor_splits.insert(0, vec![TestSplit { id: 0 }]);

        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();

        let opts = SplitDiffOptions {
            enable_scale_in: true,
            enable_adaptive: false,
        };

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            opts,
        )
        .unwrap();

        assert!(!diff.is_empty())
    }

    #[test]
    fn test_reassign_splits() {
        let actor_splits = HashMap::new();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        assert!(reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default()
        )
        .is_none());

        let actor_splits = (0..3).map(|i| (i, vec![])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = BTreeMap::new();
        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            assert!(splits.is_empty())
        }

        let actor_splits = (0..3).map(|i| (i, vec![])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..3)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            assert_eq!(splits.len(), 1);
        }

        check_all_splits(&discovered_splits, &diff);

        let actor_splits = (0..3).map(|i| (i, vec![TestSplit { id: i }])).collect();
        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 3);
        for splits in diff.values() {
            let len = splits.len();
            assert!(len == 1 || len == 2);
        }

        check_all_splits(&discovered_splits, &diff);

        let mut actor_splits: HashMap<ActorId, Vec<TestSplit>> =
            (0..3).map(|i| (i, vec![TestSplit { id: i }])).collect();
        actor_splits.insert(3, vec![]);
        actor_splits.insert(4, vec![]);

        let discovered_splits: BTreeMap<SplitId, TestSplit> = (0..5)
            .map(|i| {
                let split = TestSplit { id: i };
                (split.id(), split)
            })
            .collect();

        let diff = reassign_splits(
            FragmentId::default(),
            actor_splits,
            &discovered_splits,
            Default::default(),
        )
        .unwrap();
        assert_eq!(diff.len(), 5);
        for splits in diff.values() {
            assert_eq!(splits.len(), 1);
        }

        check_all_splits(&discovered_splits, &diff);
    }
}
