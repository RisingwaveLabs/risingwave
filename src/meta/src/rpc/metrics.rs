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
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use prometheus::core::{AtomicF64, GenericGaugeVec};
use prometheus::{
    exponential_buckets, histogram_opts, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_vec_with_registry, register_int_gauge_with_registry, Histogram,
    HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};
use risingwave_connector::source::monitor::EnumeratorMetrics as SourceEnumeratorMetrics;
use risingwave_meta_model::WorkerId;
use risingwave_object_store::object::object_metrics::{
    ObjectStoreMetrics, GLOBAL_OBJECT_STORE_METRICS,
};
use risingwave_pb::common::WorkerType;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::controller::catalog::CatalogControllerRef;
use crate::controller::cluster::ClusterControllerRef;
use crate::controller::utils::PartialFragmentStateTables;
use crate::hummock::HummockManagerRef;
use crate::manager::MetadataManager;
use crate::rpc::ElectionClientRef;

#[derive(Clone)]
pub struct MetaMetrics {
    // ********************************** Meta ************************************
    /// The number of workers in the cluster.
    pub worker_num: IntGaugeVec,
    /// The roles of all meta nodes in the cluster.
    pub meta_type: IntGaugeVec,

    // ********************************** gRPC ************************************
    /// gRPC latency of meta services
    pub grpc_latency: HistogramVec,

    // ********************************** Barrier ************************************
    /// The duration from barrier injection to commit
    /// It is the sum of inflight-latency, sync-latency and wait-commit-latency
    pub barrier_latency: Histogram,
    /// The duration from barrier complete to commit
    pub barrier_wait_commit_latency: Histogram,
    /// Latency between each barrier send
    pub barrier_send_latency: Histogram,
    /// The number of all barriers. It is the sum of barriers that are in-flight or completed but
    /// waiting for other barriers
    pub all_barrier_nums: IntGaugeVec,
    /// The number of in-flight barriers
    pub in_flight_barrier_nums: IntGaugeVec,
    /// The timestamp (UNIX epoch seconds) of the last committed barrier's epoch time.
    pub last_committed_barrier_time: IntGauge,

    // ********************************** Snapshot Backfill ***************************
    /// The barrier latency in second of `table_id` and snapshto backfill `barrier_type`
    pub snapshot_backfill_barrier_latency: LabelGuardedHistogramVec<2>, // (table_id, barrier_type)
    /// The latency of commit epoch of `table_id`
    pub snapshot_backfill_wait_commit_latency: LabelGuardedHistogramVec<1>, // (table_id, )
    /// The lags between the upstream epoch and the downstream epoch.
    pub snapshot_backfill_lag: LabelGuardedIntGaugeVec<1>, // (table_id, )
    /// The number of inflight barriers of `table_id`
    pub snapshot_backfill_inflight_barrier_num: LabelGuardedIntGaugeVec<1>, // (table_id, _)

    // ********************************** Recovery ************************************
    pub recovery_failure_cnt: IntCounter,
    pub recovery_latency: Histogram,

    // ********************************** Hummock ************************************
    /// Max committed epoch
    pub max_committed_epoch: IntGauge,
    /// Min committed epoch
    pub min_committed_epoch: IntGauge,
    /// The number of SSTs in each level
    pub level_sst_num: IntGaugeVec,
    /// The number of SSTs to be merged to next level in each level
    pub level_compact_cnt: IntGaugeVec,
    /// The number of compact tasks
    pub compact_frequency: IntCounterVec,
    /// Size of each level
    pub level_file_size: IntGaugeVec,
    /// Hummock version size
    pub version_size: IntGauge,
    /// The version Id of current version.
    pub current_version_id: IntGauge,
    /// The version id of checkpoint version.
    pub checkpoint_version_id: IntGauge,
    /// The smallest version id that is being pinned by worker nodes.
    pub min_pinned_version_id: IntGauge,
    /// The smallest version id that is being guarded by meta node safe points.
    pub min_safepoint_version_id: IntGauge,
    /// Compaction groups that is in write stop state.
    pub write_stop_compaction_groups: IntGaugeVec,
    /// The number of attempts to trigger full GC.
    pub full_gc_trigger_count: IntGauge,
    /// The number of candidate object to delete after scanning object store.
    pub full_gc_candidate_object_count: Histogram,
    /// The number of object to delete after filtering by meta node.
    pub full_gc_selected_object_count: Histogram,
    /// Hummock version stats
    pub version_stats: IntGaugeVec,
    /// Hummock version stats
    pub materialized_view_stats: IntGaugeVec,
    /// Total number of objects that is no longer referenced by versions.
    pub stale_object_count: IntGauge,
    /// Total size of objects that is no longer referenced by versions.
    pub stale_object_size: IntGauge,
    /// Total number of objects that is still referenced by non-current versions.
    pub old_version_object_count: IntGauge,
    /// Total size of objects that is still referenced by non-current versions.
    pub old_version_object_size: IntGauge,
    /// Total number of objects that is referenced by time travel.
    pub time_travel_object_count: IntGauge,
    /// Total number of objects that is referenced by current version.
    pub current_version_object_count: IntGauge,
    /// Total size of objects that is referenced by current version.
    pub current_version_object_size: IntGauge,
    /// Total number of objects that includes dangling objects.
    pub total_object_count: IntGauge,
    /// Total size of objects that includes dangling objects.
    pub total_object_size: IntGauge,
    /// The number of hummock version delta log.
    pub delta_log_count: IntGauge,
    /// latency of version checkpoint
    pub version_checkpoint_latency: Histogram,
    /// Latency for hummock manager to acquire lock
    pub hummock_manager_lock_time: HistogramVec,
    /// Latency for hummock manager to really process a request after acquire the lock
    pub hummock_manager_real_process_time: HistogramVec,
    /// The number of compactions from one level to another level that have been skipped
    pub compact_skip_frequency: IntCounterVec,
    /// Bytes of lsm tree needed to reach balance
    pub compact_pending_bytes: IntGaugeVec,
    /// Per level compression ratio
    pub compact_level_compression_ratio: GenericGaugeVec<AtomicF64>,
    /// Per level number of running compaction task
    pub level_compact_task_cnt: IntGaugeVec,
    pub time_after_last_observation: Arc<AtomicU64>,
    pub l0_compact_level_count: HistogramVec,
    pub compact_task_size: HistogramVec,
    pub compact_task_file_count: HistogramVec,
    pub compact_task_batch_count: HistogramVec,
    pub split_compaction_group_count: IntCounterVec,
    pub state_table_count: IntGaugeVec,
    pub branched_sst_count: IntGaugeVec,
    pub compact_task_trivial_move_sst_count: HistogramVec,

    pub compaction_event_consumed_latency: Histogram,
    pub compaction_event_loop_iteration_latency: Histogram,

    // ********************************** Object Store ************************************
    // Object store related metrics (for backup/restore and version checkpoint)
    pub object_store_metric: Arc<ObjectStoreMetrics>,

    // ********************************** Source ************************************
    /// supervisor for which source is still up.
    pub source_is_up: LabelGuardedIntGaugeVec<2>,
    pub source_enumerator_metrics: Arc<SourceEnumeratorMetrics>,

    // ********************************** Fragment ************************************
    /// A dummpy gauge metrics with its label to be the mapping from actor id to fragment id
    pub actor_info: IntGaugeVec,
    /// A dummpy gauge metrics with its label to be the mapping from table id to actor id
    pub table_info: IntGaugeVec,
    /// A dummy gauge metrics with its label to be the mapping from actor id to sink id
    pub sink_info: IntGaugeVec,

    /// Write throughput of commit epoch for each stable
    pub table_write_throughput: IntCounterVec,

    /// The number of compaction groups that have been triggered to move
    pub merge_compaction_group_count: IntCounterVec,

    // ********************************** Auto Schema Change ************************************
    pub auto_schema_change_failure_cnt: LabelGuardedIntCounterVec<2>,
    pub auto_schema_change_success_cnt: LabelGuardedIntCounterVec<2>,
    pub auto_schema_change_latency: LabelGuardedHistogramVec<2>,

    pub time_travel_version_replay_latency: Histogram,

    pub compaction_group_count: IntGauge,
    pub compaction_group_size: IntGaugeVec,
    pub compaction_group_file_count: IntGaugeVec,
    pub compaction_group_throughput: IntGaugeVec,
}

pub static GLOBAL_META_METRICS: LazyLock<MetaMetrics> =
    LazyLock::new(|| MetaMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl MetaMetrics {
    fn new(registry: &Registry) -> Self {
        let opts = histogram_opts!(
            "meta_grpc_duration_seconds",
            "gRPC latency of meta services",
            exponential_buckets(0.0001, 2.0, 20).unwrap() // max 52s
        );
        let grpc_latency =
            register_histogram_vec_with_registry!(opts, &["path"], registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_duration_seconds",
            "barrier latency",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let barrier_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_wait_commit_duration_seconds",
            "barrier_wait_commit_latency",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let barrier_wait_commit_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "meta_barrier_send_duration_seconds",
            "barrier send latency",
            exponential_buckets(0.1, 1.5, 19).unwrap() // max 148s
        );
        let barrier_send_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let all_barrier_nums = register_int_gauge_vec_with_registry!(
            "all_barrier_nums",
            "num of of all_barrier",
            &["database_id"],
            registry
        )
        .unwrap();
        let in_flight_barrier_nums = register_int_gauge_vec_with_registry!(
            "in_flight_barrier_nums",
            "num of of in_flight_barrier",
            &["database_id"],
            registry
        )
        .unwrap();
        let last_committed_barrier_time = register_int_gauge_with_registry!(
            "last_committed_barrier_time",
            "The timestamp (UNIX epoch seconds) of the last committed barrier's epoch time.",
            registry
        )
        .unwrap();

        // snapshot backfill metrics
        let opts = histogram_opts!(
            "meta_snapshot_backfill_barrier_duration_seconds",
            "snapshot backfill barrier latency",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let snapshot_backfill_barrier_latency = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "barrier_type"],
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "meta_snapshot_backfill_barrier_wait_commit_duration_seconds",
            "snapshot backfill barrier_wait_commit_latency",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let snapshot_backfill_wait_commit_latency =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let snapshot_backfill_lag = register_guarded_int_gauge_vec_with_registry!(
            "meta_snapshot_backfill_upstream_lag",
            "snapshot backfill upstream_lag",
            &["table_id"],
            registry
        )
        .unwrap();
        let snapshot_backfill_inflight_barrier_num = register_guarded_int_gauge_vec_with_registry!(
            "meta_snapshot_backfill_inflight_barrier_num",
            "snapshot backfill inflight_barrier_num",
            &["table_id"],
            registry
        )
        .unwrap();

        let max_committed_epoch = register_int_gauge_with_registry!(
            "storage_max_committed_epoch",
            "max committed epoch",
            registry
        )
        .unwrap();

        let min_committed_epoch = register_int_gauge_with_registry!(
            "storage_min_committed_epoch",
            "min committed epoch",
            registry
        )
        .unwrap();

        let level_sst_num = register_int_gauge_vec_with_registry!(
            "storage_level_sst_num",
            "num of SSTs in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let level_compact_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_cnt",
            "num of SSTs to be merged to next level in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let compact_frequency = register_int_counter_vec_with_registry!(
            "storage_level_compact_frequency",
            "The number of compactions from one level to another level that have completed or failed.",
            &["compactor", "group", "task_type", "result"],
            registry
        )
        .unwrap();
        let compact_skip_frequency = register_int_counter_vec_with_registry!(
            "storage_skip_compact_frequency",
            "The number of compactions from one level to another level that have been skipped.",
            &["level", "type"],
            registry
        )
        .unwrap();

        let version_size =
            register_int_gauge_with_registry!("storage_version_size", "version size", registry)
                .unwrap();

        let current_version_id = register_int_gauge_with_registry!(
            "storage_current_version_id",
            "current version id",
            registry
        )
        .unwrap();

        let checkpoint_version_id = register_int_gauge_with_registry!(
            "storage_checkpoint_version_id",
            "checkpoint version id",
            registry
        )
        .unwrap();

        let min_pinned_version_id = register_int_gauge_with_registry!(
            "storage_min_pinned_version_id",
            "min pinned version id",
            registry
        )
        .unwrap();

        let write_stop_compaction_groups = register_int_gauge_vec_with_registry!(
            "storage_write_stop_compaction_groups",
            "compaction groups of write stop state",
            &["compaction_group_id"],
            registry
        )
        .unwrap();

        let full_gc_trigger_count = register_int_gauge_with_registry!(
            "storage_full_gc_trigger_count",
            "the number of attempts to trigger full GC",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_full_gc_candidate_object_count",
            "the number of candidate object to delete after scanning object store",
            exponential_buckets(1.0, 10.0, 6).unwrap()
        );
        let full_gc_candidate_object_count =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "storage_full_gc_selected_object_count",
            "the number of object to delete after filtering by meta node",
            exponential_buckets(1.0, 10.0, 6).unwrap()
        );
        let full_gc_selected_object_count =
            register_histogram_with_registry!(opts, registry).unwrap();

        let min_safepoint_version_id = register_int_gauge_with_registry!(
            "storage_min_safepoint_version_id",
            "min safepoint version id",
            registry
        )
        .unwrap();

        let level_file_size = register_int_gauge_vec_with_registry!(
            "storage_level_total_file_size",
            "KBs total file bytes in each level",
            &["level_index"],
            registry
        )
        .unwrap();

        let version_stats = register_int_gauge_vec_with_registry!(
            "storage_version_stats",
            "per table stats in current hummock version",
            &["table_id", "metric"],
            registry
        )
        .unwrap();

        let materialized_view_stats = register_int_gauge_vec_with_registry!(
            "storage_materialized_view_stats",
            "per materialized view stats in current hummock version",
            &["table_id", "metric"],
            registry
        )
        .unwrap();

        let stale_object_count = register_int_gauge_with_registry!(
            "storage_stale_object_count",
            "total number of objects that is no longer referenced by versions.",
            registry
        )
        .unwrap();

        let stale_object_size = register_int_gauge_with_registry!(
            "storage_stale_object_size",
            "total size of objects that is no longer referenced by versions.",
            registry
        )
        .unwrap();

        let old_version_object_count = register_int_gauge_with_registry!(
            "storage_old_version_object_count",
            "total number of objects that is still referenced by non-current versions",
            registry
        )
        .unwrap();

        let old_version_object_size = register_int_gauge_with_registry!(
            "storage_old_version_object_size",
            "total size of objects that is still referenced by non-current versions",
            registry
        )
        .unwrap();

        let current_version_object_count = register_int_gauge_with_registry!(
            "storage_current_version_object_count",
            "total number of objects that is referenced by current version",
            registry
        )
        .unwrap();

        let current_version_object_size = register_int_gauge_with_registry!(
            "storage_current_version_object_size",
            "total size of objects that is referenced by current version",
            registry
        )
        .unwrap();

        let total_object_count = register_int_gauge_with_registry!(
            "storage_total_object_count",
            "Total number of objects that includes dangling objects. Note that the metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.",
            registry
        ).unwrap();

        let total_object_size = register_int_gauge_with_registry!(
            "storage_total_object_size",
            "Total size of objects that includes dangling objects. Note that the metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.",
            registry
        ).unwrap();

        let time_travel_object_count = register_int_gauge_with_registry!(
            "storage_time_travel_object_count",
            "total number of objects that is referenced by time travel.",
            registry
        )
        .unwrap();

        let delta_log_count = register_int_gauge_with_registry!(
            "storage_delta_log_count",
            "total number of hummock version delta log",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_version_checkpoint_latency",
            "hummock version checkpoint latency",
            exponential_buckets(0.1, 1.5, 20).unwrap()
        );
        let version_checkpoint_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let hummock_manager_lock_time = register_histogram_vec_with_registry!(
            "hummock_manager_lock_time",
            "latency for hummock manager to acquire the rwlock",
            &["lock_name", "lock_type"],
            registry
        )
        .unwrap();

        let hummock_manager_real_process_time = register_histogram_vec_with_registry!(
            "meta_hummock_manager_real_process_time",
            "latency for hummock manager to really process the request",
            &["method"],
            registry
        )
        .unwrap();

        let worker_num = register_int_gauge_vec_with_registry!(
            "worker_num",
            "number of nodes in the cluster",
            &["worker_type"],
            registry,
        )
        .unwrap();

        let meta_type = register_int_gauge_vec_with_registry!(
            "meta_num",
            "role of meta nodes in the cluster",
            &["worker_addr", "role"],
            registry,
        )
        .unwrap();

        let compact_pending_bytes = register_int_gauge_vec_with_registry!(
            "storage_compact_pending_bytes",
            "bytes of lsm tree needed to reach balance",
            &["group"],
            registry
        )
        .unwrap();

        let compact_level_compression_ratio = register_gauge_vec_with_registry!(
            "storage_compact_level_compression_ratio",
            "compression ratio of each level of the lsm tree",
            &["group", "level", "algorithm"],
            registry
        )
        .unwrap();

        let level_compact_task_cnt = register_int_gauge_vec_with_registry!(
            "storage_level_compact_task_cnt",
            "num of compact_task organized by group and level",
            &["task"],
            registry
        )
        .unwrap();
        let object_store_metric = Arc::new(GLOBAL_OBJECT_STORE_METRICS.clone());

        let recovery_failure_cnt = register_int_counter_with_registry!(
            "recovery_failure_cnt",
            "Number of failed recovery attempts",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "recovery_latency",
            "Latency of the recovery process",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let recovery_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let auto_schema_change_failure_cnt = register_guarded_int_counter_vec_with_registry!(
            "auto_schema_change_failure_cnt",
            "Number of failed auto schema change",
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let auto_schema_change_success_cnt = register_guarded_int_counter_vec_with_registry!(
            "auto_schema_change_success_cnt",
            "Number of success auto schema change",
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "auto_schema_change_latency",
            "Latency of the auto schema change process",
            exponential_buckets(0.1, 1.5, 20).unwrap() // max 221s
        );
        let auto_schema_change_latency = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "table_name"],
            registry
        )
        .unwrap();

        let source_is_up = register_guarded_int_gauge_vec_with_registry!(
            "source_status_is_up",
            "source is up or not",
            &["source_id", "source_name"],
            registry
        )
        .unwrap();
        let source_enumerator_metrics = Arc::new(SourceEnumeratorMetrics::default());

        let actor_info = register_int_gauge_vec_with_registry!(
            "actor_info",
            "Mapping from actor id to (fragment id, compute node)",
            &["actor_id", "fragment_id", "compute_node"],
            registry
        )
        .unwrap();

        let table_info = register_int_gauge_vec_with_registry!(
            "table_info",
            "Mapping from table id to (actor id, table name)",
            &[
                "materialized_view_id",
                "table_id",
                "fragment_id",
                "table_name",
                "table_type",
                "compaction_group_id"
            ],
            registry
        )
        .unwrap();

        let sink_info = register_int_gauge_vec_with_registry!(
            "sink_info",
            "Mapping from actor id to (actor id, sink name)",
            &["actor_id", "sink_id", "sink_name",],
            registry
        )
        .unwrap();

        let l0_compact_level_count = register_histogram_vec_with_registry!(
            "storage_l0_compact_level_count",
            "level_count of l0 compact task",
            &["group", "type"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compact_task_size",
            "Total size of compact that have been issued to state store",
            exponential_buckets(1048576.0, 2.0, 16).unwrap()
        );

        let compact_task_size =
            register_histogram_vec_with_registry!(opts, &["group", "type"], registry).unwrap();

        let compact_task_file_count = register_histogram_vec_with_registry!(
            "storage_compact_task_file_count",
            "file count of compact task",
            &["group", "type"],
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "storage_compact_task_batch_count",
            "count of compact task batch",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let compact_task_batch_count =
            register_histogram_vec_with_registry!(opts, &["type"], registry).unwrap();

        let table_write_throughput = register_int_counter_vec_with_registry!(
            "storage_commit_write_throughput",
            "The number of compactions from one level to another level that have been skipped.",
            &["table_id"],
            registry
        )
        .unwrap();

        let split_compaction_group_count = register_int_counter_vec_with_registry!(
            "storage_split_compaction_group_count",
            "Count of trigger split compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let state_table_count = register_int_gauge_vec_with_registry!(
            "storage_state_table_count",
            "Count of stable table per compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let branched_sst_count = register_int_gauge_vec_with_registry!(
            "storage_branched_sst_count",
            "Count of branched sst per compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compaction_event_consumed_latency",
            "The latency(ms) of each event being consumed",
            exponential_buckets(1.0, 1.5, 30).unwrap() // max 191s
        );
        let compaction_event_consumed_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "storage_compaction_event_loop_iteration_latency",
            "The latency(ms) of each iteration of the compaction event loop",
            exponential_buckets(1.0, 1.5, 30).unwrap() // max 191s
        );
        let compaction_event_loop_iteration_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let merge_compaction_group_count = register_int_counter_vec_with_registry!(
            "storage_merge_compaction_group_count",
            "Count of trigger merge compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_time_travel_version_replay_latency",
            "The latency(ms) of replaying a hummock version for time travel",
            exponential_buckets(0.01, 10.0, 6).unwrap()
        );
        let time_travel_version_replay_latency =
            register_histogram_with_registry!(opts, registry).unwrap();

        let compaction_group_count = register_int_gauge_with_registry!(
            "storage_compaction_group_count",
            "The number of compaction groups",
            registry,
        )
        .unwrap();

        let compaction_group_size = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_size",
            "The size of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let compaction_group_file_count = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_file_count",
            "The file count of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let compaction_group_throughput = register_int_gauge_vec_with_registry!(
            "storage_compaction_group_throughput",
            "The throughput of compaction group",
            &["group"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "storage_compact_task_trivial_move_sst_count",
            "sst count of compact trivial-move task",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let compact_task_trivial_move_sst_count =
            register_histogram_vec_with_registry!(opts, &["group"], registry).unwrap();

        Self {
            grpc_latency,
            barrier_latency,
            barrier_wait_commit_latency,
            barrier_send_latency,
            all_barrier_nums,
            in_flight_barrier_nums,
            last_committed_barrier_time,
            snapshot_backfill_barrier_latency,
            snapshot_backfill_wait_commit_latency,
            snapshot_backfill_lag,
            snapshot_backfill_inflight_barrier_num,
            recovery_failure_cnt,
            recovery_latency,

            max_committed_epoch,
            min_committed_epoch,
            level_sst_num,
            level_compact_cnt,
            compact_frequency,
            compact_skip_frequency,
            level_file_size,
            version_size,
            version_stats,
            materialized_view_stats,
            stale_object_count,
            stale_object_size,
            old_version_object_count,
            old_version_object_size,
            time_travel_object_count,
            current_version_object_count,
            current_version_object_size,
            total_object_count,
            total_object_size,
            delta_log_count,
            version_checkpoint_latency,
            current_version_id,
            checkpoint_version_id,
            min_pinned_version_id,
            min_safepoint_version_id,
            write_stop_compaction_groups,
            full_gc_trigger_count,
            full_gc_candidate_object_count,
            full_gc_selected_object_count,
            hummock_manager_lock_time,
            hummock_manager_real_process_time,
            time_after_last_observation: Arc::new(AtomicU64::new(0)),
            worker_num,
            meta_type,
            compact_pending_bytes,
            compact_level_compression_ratio,
            level_compact_task_cnt,
            object_store_metric,
            source_is_up,
            source_enumerator_metrics,
            actor_info,
            table_info,
            sink_info,
            l0_compact_level_count,
            compact_task_size,
            compact_task_file_count,
            compact_task_batch_count,
            compact_task_trivial_move_sst_count,
            table_write_throughput,
            split_compaction_group_count,
            state_table_count,
            branched_sst_count,
            compaction_event_consumed_latency,
            compaction_event_loop_iteration_latency,
            auto_schema_change_failure_cnt,
            auto_schema_change_success_cnt,
            auto_schema_change_latency,
            merge_compaction_group_count,
            time_travel_version_replay_latency,
            compaction_group_count,
            compaction_group_size,
            compaction_group_file_count,
            compaction_group_throughput,
        }
    }

    #[cfg(test)]
    pub fn for_test(registry: &Registry) -> Self {
        Self::new(registry)
    }
}
impl Default for MetaMetrics {
    fn default() -> Self {
        GLOBAL_META_METRICS.clone()
    }
}

pub fn start_worker_info_monitor(
    metadata_manager: MetadataManager,
    election_client: ElectionClientRef,
    interval: Duration,
    meta_metrics: Arc<MetaMetrics>,
) -> (JoinHandle<()>, Sender<()>) {
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut monitor_interval = tokio::time::interval(interval);
        monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = monitor_interval.tick() => {},
                // Shutdown monitor
                _ = &mut shutdown_rx => {
                    tracing::info!("Worker number monitor is stopped");
                    return;
                }
            }

            let node_map = match metadata_manager.count_worker_node().await {
                Ok(node_map) => node_map,
                Err(err) => {
                    tracing::warn!(error = %err.as_report(), "fail to count worker node");
                    continue;
                }
            };

            // Reset metrics to clean the stale labels e.g. invalid lease ids
            meta_metrics.worker_num.reset();
            meta_metrics.meta_type.reset();

            for (worker_type, worker_num) in node_map {
                meta_metrics
                    .worker_num
                    .with_label_values(&[(worker_type.as_str_name())])
                    .set(worker_num as i64);
            }
            if let Ok(meta_members) = election_client.get_members().await {
                meta_metrics
                    .worker_num
                    .with_label_values(&[WorkerType::Meta.as_str_name()])
                    .set(meta_members.len() as i64);
                meta_members.into_iter().for_each(|m| {
                    let role = if m.is_leader { "leader" } else { "follower" };
                    meta_metrics
                        .meta_type
                        .with_label_values(&[&m.id, role])
                        .set(1);
                });
            }
        }
    });

    (join_handle, shutdown_tx)
}

pub async fn refresh_fragment_info_metrics(
    catalog_controller: &CatalogControllerRef,
    cluster_controller: &ClusterControllerRef,
    hummock_manager: &HummockManagerRef,
    meta_metrics: Arc<MetaMetrics>,
) {
    let worker_nodes = match cluster_controller
        .list_workers(Some(WorkerType::ComputeNode.into()), None)
        .await
    {
        Ok(worker_nodes) => worker_nodes,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to list worker node");
            return;
        }
    };
    let actor_locations = match catalog_controller.list_actor_locations().await {
        Ok(actor_locations) => actor_locations,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to get actor locations");
            return;
        }
    };
    let sink_actor_mapping = match catalog_controller.list_sink_actor_mapping().await {
        Ok(sink_actor_mapping) => sink_actor_mapping,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to get sink actor mapping");
            return;
        }
    };
    let fragment_state_tables = match catalog_controller.list_fragment_state_tables().await {
        Ok(fragment_state_tables) => fragment_state_tables,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to get fragment state tables");
            return;
        }
    };
    let table_name_and_type_mapping = match catalog_controller.get_table_name_type_mapping().await {
        Ok(mapping) => mapping,
        Err(err) => {
            tracing::warn!(error=%err.as_report(), "fail to get table name mapping");
            return;
        }
    };

    let worker_addr_mapping: HashMap<WorkerId, String> = worker_nodes
        .into_iter()
        .map(|worker_node| {
            let addr = match worker_node.host {
                Some(host) => format!("{}:{}", host.host, host.port),
                None => "".to_owned(),
            };
            (worker_node.id as WorkerId, addr)
        })
        .collect();
    let table_compaction_group_id_mapping = hummock_manager
        .get_table_compaction_group_id_mapping()
        .await;

    // Start fresh with a reset to clear all outdated labels. This is safe since we always
    // report full info on each interval.
    meta_metrics.actor_info.reset();
    meta_metrics.table_info.reset();
    meta_metrics.sink_info.reset();
    for actor_location in actor_locations {
        let actor_id_str = actor_location.actor_id.to_string();
        let fragment_id_str = actor_location.fragment_id.to_string();
        // Report a dummy gauge metrics with (fragment id, actor id, node
        // address) as its label
        if let Some(address) = worker_addr_mapping.get(&actor_location.worker_id) {
            meta_metrics
                .actor_info
                .with_label_values(&[&actor_id_str, &fragment_id_str, address])
                .set(1);
        }
    }
    for (sink_id, (sink_name, actor_ids)) in sink_actor_mapping {
        let sink_id_str = sink_id.to_string();
        for actor_id in actor_ids {
            let actor_id_str = actor_id.to_string();
            meta_metrics
                .sink_info
                .with_label_values(&[&actor_id_str, &sink_id_str, &sink_name])
                .set(1);
        }
    }
    for PartialFragmentStateTables {
        fragment_id,
        job_id,
        state_table_ids,
    } in fragment_state_tables
    {
        let fragment_id_str = fragment_id.to_string();
        let job_id_str = job_id.to_string();
        for table_id in state_table_ids.into_inner() {
            let table_id_str = table_id.to_string();
            let (table_name, table_type) = table_name_and_type_mapping
                .get(&table_id)
                .cloned()
                .unwrap_or_else(|| ("unknown".to_owned(), "unknown".to_owned()));
            let compaction_group_id = table_compaction_group_id_mapping
                .get(&(table_id as u32))
                .map(|cg_id| cg_id.to_string())
                .unwrap_or_else(|| "unknown".to_owned());
            meta_metrics
                .table_info
                .with_label_values(&[
                    &job_id_str,
                    &table_id_str,
                    &fragment_id_str,
                    &table_name,
                    &table_type,
                    &compaction_group_id,
                ])
                .set(1);
        }
    }
}

pub fn start_fragment_info_monitor(
    metadata_manager: MetadataManager,
    hummock_manager: HummockManagerRef,
    meta_metrics: Arc<MetaMetrics>,
) -> (JoinHandle<()>, Sender<()>) {
    const COLLECT_INTERVAL_SECONDS: u64 = 60;

    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    let join_handle = tokio::spawn(async move {
        let mut monitor_interval =
            tokio::time::interval(Duration::from_secs(COLLECT_INTERVAL_SECONDS));
        monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // Wait for interval
                _ = monitor_interval.tick() => {},
                // Shutdown monitor
                _ = &mut shutdown_rx => {
                    tracing::info!("Fragment info monitor is stopped");
                    return;
                }
            }

            refresh_fragment_info_metrics(
                &metadata_manager.catalog_controller,
                &metadata_manager.cluster_controller,
                &hummock_manager,
                meta_metrics.clone(),
            )
            .await;
        }
    });

    (join_handle, shutdown_tx)
}
