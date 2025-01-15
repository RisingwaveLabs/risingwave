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

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::poll_fn;
use std::ops::Range;
use std::sync::{Arc, LazyLock};
use std::task::{ready, Poll};
use std::time::{Duration, Instant};

use foyer::{HybridCacheEntry, RangeBoundsExt};
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntGauge, Registry,
};
use risingwave_common::config::RefillTarget;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use thiserror_ext::AsReport;
use tokio::sync::{oneshot, Semaphore};
use tokio::task::JoinHandle;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{
    Block, HummockError, HummockResult, Sstable, SstableBlockIndex, SstableStoreRef, TableHolder,
};
use crate::monitor::StoreLocalStatistic;
use crate::opts::StorageOpts;

pub static GLOBAL_CACHE_REFILL_METRICS: LazyLock<CacheRefillMetrics> =
    LazyLock::new(|| CacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct CacheRefillMetrics {
    pub refill_duration: HistogramVec,
    pub refill_total: GenericCounterVec<AtomicU64>,
    pub refill_bytes: GenericCounterVec<AtomicU64>,

    pub data_refill_success_duration: Histogram,
    pub meta_refill_success_duration: Histogram,

    pub data_refill_filtered_total: GenericCounter<AtomicU64>,
    pub data_refill_attempts_total: GenericCounter<AtomicU64>,
    pub data_refill_started_total: GenericCounter<AtomicU64>,
    pub meta_refill_attempts_total: GenericCounter<AtomicU64>,

    pub data_refill_parent_meta_lookup_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_parent_meta_lookup_miss_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_miss_total: GenericCounter<AtomicU64>,

    pub data_refill_block_unfiltered_total: GenericCounter<AtomicU64>,
    pub data_refill_block_success_total: GenericCounter<AtomicU64>,

    pub data_refill_ideal_bytes: GenericCounter<AtomicU64>,
    pub data_refill_success_bytes: GenericCounter<AtomicU64>,

    pub refill_queue_total: IntGauge,
}

impl CacheRefillMetrics {
    pub fn new(registry: &Registry) -> Self {
        let refill_duration = register_histogram_vec_with_registry!(
            "refill_duration",
            "refill duration",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_total = register_int_counter_vec_with_registry!(
            "refill_total",
            "refill total",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_bytes = register_int_counter_vec_with_registry!(
            "refill_bytes",
            "refill bytes",
            &["type", "op"],
            registry,
        )
        .unwrap();

        let data_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();
        let meta_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["meta", "success"])
            .unwrap();

        let data_refill_filtered_total = refill_total
            .get_metric_with_label_values(&["data", "filtered"])
            .unwrap();
        let data_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["data", "attempts"])
            .unwrap();
        let data_refill_started_total = refill_total
            .get_metric_with_label_values(&["data", "started"])
            .unwrap();
        let meta_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["meta", "attempts"])
            .unwrap();

        let data_refill_parent_meta_lookup_hit_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "hit"])
            .unwrap();
        let data_refill_parent_meta_lookup_miss_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "miss"])
            .unwrap();
        let data_refill_unit_inheritance_hit_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "hit"])
            .unwrap();
        let data_refill_unit_inheritance_miss_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "miss"])
            .unwrap();

        let data_refill_block_unfiltered_total = refill_total
            .get_metric_with_label_values(&["block", "unfiltered"])
            .unwrap();
        let data_refill_block_success_total = refill_total
            .get_metric_with_label_values(&["block", "success"])
            .unwrap();

        let data_refill_ideal_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "ideal"])
            .unwrap();
        let data_refill_success_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();

        let refill_queue_total = register_int_gauge_with_registry!(
            "refill_queue_total",
            "refill queue total",
            registry,
        )
        .unwrap();

        Self {
            refill_duration,
            refill_total,
            refill_bytes,

            data_refill_success_duration,
            meta_refill_success_duration,
            data_refill_filtered_total,
            data_refill_attempts_total,
            data_refill_started_total,
            meta_refill_attempts_total,

            data_refill_parent_meta_lookup_hit_total,
            data_refill_parent_meta_lookup_miss_total,
            data_refill_unit_inheritance_hit_total,
            data_refill_unit_inheritance_miss_total,

            data_refill_block_unfiltered_total,
            data_refill_block_success_total,

            data_refill_ideal_bytes,
            data_refill_success_bytes,

            refill_queue_total,
        }
    }
}

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Cache refill timeout.
    pub timeout: Duration,

    /// Data file cache refill levels.
    pub data_refill_levels: HashSet<u32>,

    /// Data block refill target.
    pub data_refill_target: RefillTarget,

    /// Data file cache refill concurrency.
    pub concurrency: usize,

    /// Data file cache refill unit (blocks).
    pub unit: usize,

    /// Data file cache refill unit threshold.
    ///
    /// Only units whose admit rate > threshold will be refilled.
    pub threshold: f64,
}

impl CacheRefillConfig {
    pub fn from_storage_opts(options: &StorageOpts) -> Self {
        Self {
            timeout: Duration::from_millis(options.cache_refill_timeout_ms),
            data_refill_levels: options
                .cache_refill_data_refill_levels
                .iter()
                .copied()
                .collect(),
            data_refill_target: options.cache_refill_data_refill_target,
            concurrency: options.cache_refill_concurrency,
            unit: options.cache_refill_unit,
            threshold: options.cache_refill_threshold,
        }
    }
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

pub(crate) type SpawnRefillTask = Arc<
    // first current version, second new version
    dyn Fn(Vec<SstDeltaInfo>, CacheRefillContext, PinnedVersion, PinnedVersion) -> JoinHandle<()>
        + Send
        + Sync
        + 'static,
>;

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    context: CacheRefillContext,

    spawn_refill_task: SpawnRefillTask,
}

impl CacheRefiller {
    pub(crate) fn new(
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        Self {
            queue: VecDeque::new(),
            context: CacheRefillContext {
                config,
                concurrency,
                sstable_store,
            },
            spawn_refill_task,
        }
    }

    pub(crate) fn default_spawn_refill_task() -> SpawnRefillTask {
        Arc::new(|deltas, context, _, _| {
            let task = CacheRefillTask { deltas, context };
            tokio::spawn(task.run())
        })
    }

    pub(crate) fn start_cache_refill(
        &mut self,
        deltas: Vec<SstDeltaInfo>,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
    ) {
        let handle = (self.spawn_refill_task)(
            deltas,
            self.context.clone(),
            pinned_version.clone(),
            new_pinned_version.clone(),
        );
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let item = Item { handle, event };
        self.queue.push_back(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
    }

    pub(crate) fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }
}

impl CacheRefiller {
    pub(crate) fn next_event(&mut self) -> impl Future<Output = CacheRefillerEvent> + '_ {
        poll_fn(|cx| {
            if let Some(item) = self.queue.front_mut() {
                ready!(item.handle.poll_unpin(cx)).unwrap();
                let item = self.queue.pop_front().unwrap();
                GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.sub(1);
                return Poll::Ready(item.event);
            }
            Poll::Pending
        })
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: PinnedVersion,
    pub new_pinned_version: PinnedVersion,
}

#[derive(Clone)]
pub(crate) struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
}

struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "meta cache refill error");
                            return;
                        }
                    };
                    Self::data_cache_refill(&context, delta, holders).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let tasks = delta
            .insert_sst_infos
            .iter()
            .map(|info| async {
                let mut stats = StoreLocalStatistic::default();
                GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();

                let now = Instant::now();
                let res = context.sstable_store.sstable(info, &mut stats).await;
                stats.discard();
                GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_success_duration
                    .observe(now.elapsed().as_secs_f64());
                res
            })
            .collect_vec();
        let holders = try_join_all(tasks).await?;
        Ok(holders)
    }

    /// Get sstable inheritance info in unit level.
    fn get_units_to_refill_by_inheritance(
        context: &CacheRefillContext,
        ssts: &[TableHolder],
        parent_ssts: impl IntoIterator<Item = HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>>,
    ) -> HashSet<SstableUnit> {
        let mut res = HashSet::default();

        let Some(filter) = context.sstable_store.data_recent_filter() else {
            return res;
        };

        let units = {
            let unit = context.config.unit;
            ssts.iter()
                .flat_map(|sst| {
                    let units = Unit::units(sst, unit);
                    (0..units).map(|uidx| Unit::new(sst, unit, uidx))
                })
                .collect_vec()
        };

        if cfg!(debug_assertions) {
            // assert units in asc order
            units.iter().tuple_windows().for_each(|(a, b)| {
                debug_assert_ne!(
                    KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
                    std::cmp::Ordering::Greater
                )
            });
        }

        for psst in parent_ssts {
            for pblk in 0..psst.block_count() {
                let pleft = &psst.meta.block_metas[pblk].smallest_key;
                let pright = if pblk + 1 == psst.block_count() {
                    // `largest_key` can be included or excluded, both are treated as included here
                    &psst.meta.largest_key
                } else {
                    &psst.meta.block_metas[pblk + 1].smallest_key
                };

                // partition point: unit.right < pblk.left
                let uleft = units.partition_point(|unit| {
                    KeyComparator::compare_encoded_full_key(unit.largest_key(), pleft)
                        == std::cmp::Ordering::Less
                });
                // partition point: unit.left <= pblk.right
                let uright = units.partition_point(|unit| {
                    KeyComparator::compare_encoded_full_key(unit.smallest_key(), pright)
                        != std::cmp::Ordering::Greater
                });

                // overlapping: uleft..uright
                for u in units.iter().take(uright).skip(uleft) {
                    let unit = SstableUnit {
                        sst_obj_id: u.sst.id,
                        blks: u.blks.clone(),
                    };
                    if res.contains(&unit) {
                        continue;
                    }
                    if filter.contains(&(psst.id, pblk)) {
                        res.insert(unit);
                    }
                }
            }
        }

        let hit = res.len();
        let miss = units.len() - res.len();
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_hit_total
            .inc_by(hit as u64);
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_miss_total
            .inc_by(miss as u64);

        res
    }

    async fn data_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        // return if data file cache is disabled
        let Some(filter) = context.sstable_store.data_recent_filter() else {
            return;
        };

        // return if no data to refill
        if delta.insert_sst_infos.is_empty() || delta.delete_sst_object_ids.is_empty() {
            return;
        }

        // return if recent filter miss
        if !context
            .config
            .data_refill_levels
            .contains(&delta.insert_sst_level)
            || !delta
                .delete_sst_object_ids
                .iter()
                .any(|&id| filter.contains(&(id, usize::MAX)))
        {
            GLOBAL_CACHE_REFILL_METRICS.data_refill_filtered_total.inc();
            return;
        }

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_block_unfiltered_total
            .inc_by(
                holders
                    .iter()
                    .map(|sst| sst.block_count() as u64)
                    .sum::<u64>(),
            );

        if delta.insert_sst_level == 0 {
            Self::data_file_cache_refill_l0_impl(context, delta, holders).await;
        } else {
            Self::data_file_cache_refill_impl(context, delta, holders).await;
        }
    }

    async fn data_file_cache_refill_l0_impl(
        context: &CacheRefillContext,
        _delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let unit = context.config.unit;

        let mut futures = vec![];

        for sst in &holders {
            for blk_start in (0..sst.block_count()).step_by(unit) {
                let blk_end = std::cmp::min(sst.block_count(), blk_start + unit);
                let unit = SstableUnit {
                    sst_obj_id: sst.id,
                    blks: blk_start..blk_end,
                };
                let future = match context.config.data_refill_target {
                    RefillTarget::Memory => {
                        async move { Self::data_cache_refill_unit(context, sst, unit).await }
                            .left_future()
                    }
                    RefillTarget::Disk => {
                        async move { Self::data_file_cache_refill_unit(context, sst, unit).await }
                            .right_future()
                    }
                };
                futures.push(future);
            }
        }
        join_all(futures).await;
    }

    async fn data_file_cache_refill_impl(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let sstable_store = context.sstable_store.clone();
        let futures = delta.delete_sst_object_ids.iter().map(|sst_obj_id| {
            let store = &sstable_store;
            async move {
                let res = store.sstable_cached(*sst_obj_id).await;
                match res {
                    Ok(Some(_)) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_hit_total
                        .inc(),
                    Ok(None) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_miss_total
                        .inc(),
                    _ => {}
                }
                res
            }
        });
        let parent_ssts = match try_join_all(futures).await {
            Ok(parent_ssts) => parent_ssts.into_iter().flatten(),
            Err(e) => {
                return tracing::error!(error = %e.as_report(), "get old meta from cache error")
            }
        };
        let units = Self::get_units_to_refill_by_inheritance(context, &holders, parent_ssts);

        let ssts: HashMap<HummockSstableObjectId, TableHolder> =
            holders.into_iter().map(|meta| (meta.id, meta)).collect();
        let futures = units.into_iter().map(|unit| {
            let ssts = &ssts;
            async move {
                let sst = ssts.get(&unit.sst_obj_id).unwrap();
                let future = match context.config.data_refill_target {
                    RefillTarget::Memory => {
                        async move { Self::data_cache_refill_unit(context, sst, unit).await }
                            .left_future()
                    }
                    RefillTarget::Disk => {
                        async move { Self::data_file_cache_refill_unit(context, sst, unit).await }
                            .right_future()
                    }
                };
                if let Err(e) = future.await {
                    tracing::error!(error = %e.as_report(), "data refill error");
                }
            }
        });
        join_all(futures).await;
    }

    async fn data_file_cache_refill_unit(
        context: &CacheRefillContext,
        sst: &Sstable,
        unit: SstableUnit,
    ) -> HummockResult<()> {
        let sstable_store = &context.sstable_store;
        let threshold = context.config.threshold;

        // update filter for sst id only
        if let Some(filter) = sstable_store.data_recent_filter() {
            filter.insert((sst.id, usize::MAX));
        }

        let blocks = unit.blks.size().unwrap();

        let mut tasks = vec![];
        let mut contexts = Vec::with_capacity(blocks);
        let mut admits = 0;

        let (range_first, _) = sst.calculate_block_info(unit.blks.start);
        let (range_last, _) = sst.calculate_block_info(unit.blks.end - 1);
        let range = range_first.start..range_last.end;

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_ideal_bytes
            .inc_by(range.size().unwrap() as u64);

        for blk in unit.blks {
            let (range, uncompressed_capacity) = sst.calculate_block_info(blk);
            let key = SstableBlockIndex {
                sst_id: sst.id,
                block_idx: blk as u64,
            };

            let mut writer = sstable_store.block_cache().storage_writer(key);

            if writer.pick() {
                admits += 1;
            }

            contexts.push((writer, range, uncompressed_capacity))
        }

        if admits as f64 / contexts.len() as f64 >= threshold {
            let task = async move {
                GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

                let permit = context.concurrency.acquire().await.unwrap();

                GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

                let timer = GLOBAL_CACHE_REFILL_METRICS
                    .data_refill_success_duration
                    .start_timer();

                let data = sstable_store
                    .store()
                    .read(&sstable_store.get_sst_data_path(sst.id), range.clone())
                    .await?;
                let mut futures = vec![];
                for (w, r, uc) in contexts {
                    let offset = r.start - range.start;
                    let len = r.end - r.start;
                    let bytes = data.slice(offset..offset + len);
                    let future = async move {
                        let value = Box::new(Block::decode(bytes, uc)?);
                        // The entry should always be `Some(..)`, use if here for compatible.
                        if let Some(_entry) = w.force().insert(value) {
                            GLOBAL_CACHE_REFILL_METRICS
                                .data_refill_success_bytes
                                .inc_by(len as u64);
                            GLOBAL_CACHE_REFILL_METRICS
                                .data_refill_block_success_total
                                .inc();
                        }
                        Ok::<_, HummockError>(())
                    };
                    futures.push(future);
                }
                try_join_all(futures)
                    .await
                    .map_err(HummockError::file_cache)?;

                drop(permit);
                drop(timer);

                Ok::<_, HummockError>(())
            };
            tasks.push(task);
        }

        try_join_all(tasks).await?;

        Ok(())
    }

    async fn data_cache_refill_unit(
        context: &CacheRefillContext,
        sst: &Sstable,
        unit: SstableUnit,
    ) -> HummockResult<()> {
        let sstable_store = context.sstable_store.clone();
        let sst = sst.clone();

        // update filter for sst id only
        if let Some(filter) = sstable_store.data_recent_filter() {
            filter.insert((sst.id, usize::MAX));
        }

        let blocks = unit.blks.size().unwrap();

        let mut contexts = Vec::with_capacity(blocks);
        let mut fetches = Vec::with_capacity(blocks);

        let (range_first, _) = sst.calculate_block_info(unit.blks.start);
        let (range_last, _) = sst.calculate_block_info(unit.blks.end - 1);
        let range = range_first.start..range_last.end;

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_ideal_bytes
            .inc_by(range.size().unwrap() as u64);

        for blk in unit.blks {
            let (range, uncompressed_capacity) = sst.calculate_block_info(blk);
            let key = SstableBlockIndex {
                sst_id: sst.id,
                block_idx: blk as u64,
            };

            let (tx, rx) = oneshot::channel();

            contexts.push((tx, range, uncompressed_capacity));
            let block_cache = sstable_store.block_cache().clone();
            fetches.push(
                block_cache.fetch(key, || async move { rx.await.map_err(anyhow::Error::from) }),
            );
        }

        let concurrency = context.concurrency.clone();
        let task = async move {
            GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

            let permit = concurrency.acquire().await.unwrap();

            GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

            let timer = GLOBAL_CACHE_REFILL_METRICS
                .data_refill_success_duration
                .start_timer();

            let data = sstable_store
                .store()
                .read(&sstable_store.get_sst_data_path(sst.id), range.clone())
                .await?;
            let mut futures = vec![];
            for (tx, r, uc) in contexts {
                let offset = r.start - range.start;
                let len = r.end - r.start;
                let bytes = data.slice(offset..offset + len);
                let future = async move {
                    let value = Box::new(Block::decode(bytes, uc)?);
                    if tx.send(value).is_ok() {
                        GLOBAL_CACHE_REFILL_METRICS
                            .data_refill_success_bytes
                            .inc_by(len as u64);
                        GLOBAL_CACHE_REFILL_METRICS
                            .data_refill_block_success_total
                            .inc();
                    }

                    Ok::<_, HummockError>(())
                };
                futures.push(future);
            }
            try_join_all(futures)
                .await
                .map_err(HummockError::file_cache)?;

            drop(permit);
            drop(timer);

            Ok::<_, HummockError>(())
        };
        tokio::spawn(task);

        if let Err(e) = try_join_all(fetches).await {
            tracing::error!(error = %e.as_report(), "data refill fetches error");
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct SstableBlock {
    pub sst_obj_id: HummockSstableObjectId,
    pub blk_idx: usize,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SstableUnit {
    pub sst_obj_id: HummockSstableObjectId,
    pub blks: Range<usize>,
}

impl Ord for SstableUnit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.sst_obj_id.cmp(&other.sst_obj_id) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.blks.start.cmp(&other.blks.start) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.blks.end.cmp(&other.blks.end)
    }
}

impl PartialOrd for SstableUnit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct Unit<'a> {
    sst: &'a Sstable,
    blks: Range<usize>,
}

impl<'a> Unit<'a> {
    fn new(sst: &'a Sstable, unit: usize, uidx: usize) -> Self {
        let blks = unit * uidx..std::cmp::min(unit * (uidx + 1), sst.block_count());
        Self { sst, blks }
    }

    fn smallest_key(&self) -> &Vec<u8> {
        &self.sst.meta.block_metas[self.blks.start].smallest_key
    }

    // `largest_key` can be included or excluded, both are treated as included here
    fn largest_key(&self) -> &Vec<u8> {
        if self.blks.end == self.sst.block_count() {
            &self.sst.meta.largest_key
        } else {
            &self.sst.meta.block_metas[self.blks.end].smallest_key
        }
    }

    fn units(sst: &Sstable, unit: usize) -> usize {
        sst.block_count() / unit + if sst.block_count() % unit == 0 { 0 } else { 1 }
    }
}
