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

use std::sync::OnceLock;

use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, IntCounter,
    IntGauge, Registry,
};
use risingwave_common::catalog::TableId;
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedGauge, LabelGuardedGaugeVec, LabelGuardedHistogramVec, LabelGuardedIntCounter,
    LabelGuardedIntCounterVec, LabelGuardedIntGauge, LabelGuardedIntGaugeVec, MetricVecRelabelExt,
    RelabeledGuardedHistogramVec, RelabeledGuardedIntCounterVec, RelabeledGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_gauge_vec_with_registry, register_guarded_histogram_vec_with_registry,
    register_guarded_int_counter_vec_with_registry, register_guarded_int_gauge_vec_with_registry,
};
use risingwave_connector::sink::catalog::SinkId;

use crate::common::log_store_impl::kv_log_store::{
    REWIND_BACKOFF_FACTOR, REWIND_BASE_DELAY, REWIND_MAX_DELAY,
};
use crate::executor::prelude::ActorId;
use crate::task::FragmentId;

#[derive(Clone)]
pub struct StreamingMetrics {
    pub level: MetricLevel,

    // Executor metrics (disabled by default)
    pub executor_row_count: RelabeledGuardedIntCounterVec<3>,

    // Streaming actor metrics from tokio (disabled by default)
    actor_execution_time: LabelGuardedGaugeVec<1>,
    actor_scheduled_duration: LabelGuardedGaugeVec<1>,
    actor_scheduled_cnt: LabelGuardedIntGaugeVec<1>,
    actor_fast_poll_duration: LabelGuardedGaugeVec<1>,
    actor_fast_poll_cnt: LabelGuardedIntGaugeVec<1>,
    actor_slow_poll_duration: LabelGuardedGaugeVec<1>,
    actor_slow_poll_cnt: LabelGuardedIntGaugeVec<1>,
    actor_poll_duration: LabelGuardedGaugeVec<1>,
    actor_poll_cnt: LabelGuardedIntGaugeVec<1>,
    actor_idle_duration: LabelGuardedGaugeVec<1>,
    actor_idle_cnt: LabelGuardedIntGaugeVec<1>,

    // Streaming actor
    pub actor_count: LabelGuardedIntGaugeVec<1>,
    pub actor_in_record_cnt: RelabeledGuardedIntCounterVec<3>,
    pub actor_out_record_cnt: RelabeledGuardedIntCounterVec<2>,
    pub actor_current_epoch: RelabeledGuardedIntGaugeVec<2>,

    // Source
    pub source_output_row_count: LabelGuardedIntCounterVec<4>,
    pub source_split_change_count: LabelGuardedIntCounterVec<4>,
    pub source_backfill_row_count: LabelGuardedIntCounterVec<4>,

    // Sink
    sink_input_row_count: LabelGuardedIntCounterVec<3>,
    sink_input_bytes: LabelGuardedIntCounterVec<3>,
    sink_chunk_buffer_size: LabelGuardedIntGaugeVec<3>,

    // Exchange (see also `compute::ExchangeServiceMetrics`)
    pub exchange_frag_recv_size: LabelGuardedIntCounterVec<2>,

    // Streaming Merge (We break out this metric from `barrier_align_duration` because
    // the alignment happens on different levels)
    pub merge_barrier_align_duration: RelabeledGuardedHistogramVec<2>,

    // Backpressure
    pub actor_output_buffer_blocking_duration_ns: RelabeledGuardedIntCounterVec<3>,
    actor_input_buffer_blocking_duration_ns: LabelGuardedIntCounterVec<3>,

    // Streaming Join
    pub join_lookup_miss_count: LabelGuardedIntCounterVec<4>,
    pub join_lookup_total_count: LabelGuardedIntCounterVec<4>,
    pub join_insert_cache_miss_count: LabelGuardedIntCounterVec<4>,
    pub join_actor_input_waiting_duration_ns: LabelGuardedIntCounterVec<2>,
    pub join_match_duration_ns: LabelGuardedIntCounterVec<3>,
    pub join_cached_entry_count: LabelGuardedIntGaugeVec<3>,
    pub join_matched_join_keys: RelabeledGuardedHistogramVec<3>,

    // Streaming Join, Streaming Dynamic Filter and Streaming Union
    pub barrier_align_duration: RelabeledGuardedIntCounterVec<4>,

    // Streaming Aggregation
    agg_lookup_miss_count: LabelGuardedIntCounterVec<3>,
    agg_total_lookup_count: LabelGuardedIntCounterVec<3>,
    agg_cached_entry_count: LabelGuardedIntGaugeVec<3>,
    agg_chunk_lookup_miss_count: LabelGuardedIntCounterVec<3>,
    agg_chunk_total_lookup_count: LabelGuardedIntCounterVec<3>,
    agg_dirty_groups_count: LabelGuardedIntGaugeVec<3>,
    agg_dirty_groups_heap_size: LabelGuardedIntGaugeVec<3>,
    agg_distinct_cache_miss_count: LabelGuardedIntCounterVec<3>,
    agg_distinct_total_cache_count: LabelGuardedIntCounterVec<3>,
    agg_distinct_cached_entry_count: LabelGuardedIntGaugeVec<3>,
    agg_state_cache_lookup_count: LabelGuardedIntCounterVec<3>,
    agg_state_cache_miss_count: LabelGuardedIntCounterVec<3>,

    // Streaming TopN
    group_top_n_cache_miss_count: LabelGuardedIntCounterVec<3>,
    group_top_n_total_query_cache_count: LabelGuardedIntCounterVec<3>,
    group_top_n_cached_entry_count: LabelGuardedIntGaugeVec<3>,
    // TODO(rc): why not just use the above three?
    group_top_n_appendonly_cache_miss_count: LabelGuardedIntCounterVec<3>,
    group_top_n_appendonly_total_query_cache_count: LabelGuardedIntCounterVec<3>,
    group_top_n_appendonly_cached_entry_count: LabelGuardedIntGaugeVec<3>,

    // Lookup executor
    lookup_cache_miss_count: LabelGuardedIntCounterVec<3>,
    lookup_total_query_cache_count: LabelGuardedIntCounterVec<3>,
    lookup_cached_entry_count: LabelGuardedIntGaugeVec<3>,

    // temporal join
    temporal_join_cache_miss_count: LabelGuardedIntCounterVec<3>,
    temporal_join_total_query_cache_count: LabelGuardedIntCounterVec<3>,
    temporal_join_cached_entry_count: LabelGuardedIntGaugeVec<3>,

    // Backfill
    backfill_snapshot_read_row_count: LabelGuardedIntCounterVec<2>,
    backfill_upstream_output_row_count: LabelGuardedIntCounterVec<2>,

    // CDC Backfill
    cdc_backfill_snapshot_read_row_count: LabelGuardedIntCounterVec<2>,
    cdc_backfill_upstream_output_row_count: LabelGuardedIntCounterVec<2>,

    // Snapshot Backfill
    pub(crate) snapshot_backfill_consume_row_count: LabelGuardedIntCounterVec<3>,

    // Over Window
    over_window_cached_entry_count: LabelGuardedIntGaugeVec<3>,
    over_window_cache_lookup_count: LabelGuardedIntCounterVec<3>,
    over_window_cache_miss_count: LabelGuardedIntCounterVec<3>,
    over_window_range_cache_entry_count: LabelGuardedIntGaugeVec<3>,
    over_window_range_cache_lookup_count: LabelGuardedIntCounterVec<3>,
    over_window_range_cache_left_miss_count: LabelGuardedIntCounterVec<3>,
    over_window_range_cache_right_miss_count: LabelGuardedIntCounterVec<3>,
    over_window_accessed_entry_count: LabelGuardedIntCounterVec<3>,
    over_window_compute_count: LabelGuardedIntCounterVec<3>,
    over_window_same_output_count: LabelGuardedIntCounterVec<3>,

    /// The duration from receipt of barrier to all actors collection.
    /// And the max of all node `barrier_inflight_latency` is the latency for a barrier
    /// to flow through the graph.
    pub barrier_inflight_latency: Histogram,
    /// The duration of sync to storage.
    pub barrier_sync_latency: Histogram,
    pub barrier_batch_size: Histogram,
    /// The progress made by the earliest in-flight barriers in the local barrier manager.
    pub barrier_manager_progress: IntCounter,

    pub kv_log_store_storage_write_count: LabelGuardedIntCounterVec<4>,
    pub kv_log_store_storage_write_size: LabelGuardedIntCounterVec<4>,
    pub kv_log_store_rewind_count: LabelGuardedIntCounterVec<4>,
    pub kv_log_store_rewind_delay: LabelGuardedHistogramVec<4>,
    pub kv_log_store_storage_read_count: LabelGuardedIntCounterVec<5>,
    pub kv_log_store_storage_read_size: LabelGuardedIntCounterVec<5>,
    pub kv_log_store_buffer_unconsumed_item_count: LabelGuardedIntGaugeVec<4>,
    pub kv_log_store_buffer_unconsumed_row_count: LabelGuardedIntGaugeVec<4>,
    pub kv_log_store_buffer_unconsumed_epoch_count: LabelGuardedIntGaugeVec<4>,
    pub kv_log_store_buffer_unconsumed_min_epoch: LabelGuardedIntGaugeVec<4>,

    // Memory management
    pub lru_runtime_loop_count: IntCounter,
    pub lru_latest_sequence: IntGauge,
    pub lru_watermark_sequence: IntGauge,
    pub lru_eviction_policy: IntGauge,
    pub jemalloc_allocated_bytes: IntGauge,
    pub jemalloc_active_bytes: IntGauge,
    pub jemalloc_resident_bytes: IntGauge,
    pub jemalloc_metadata_bytes: IntGauge,
    pub jvm_allocated_bytes: IntGauge,
    pub jvm_active_bytes: IntGauge,
    pub stream_memory_usage: RelabeledGuardedIntGaugeVec<3>,

    // Materialized view
    materialize_cache_hit_count: RelabeledGuardedIntCounterVec<3>,
    materialize_cache_total_count: RelabeledGuardedIntCounterVec<3>,
    materialize_input_row_count: RelabeledGuardedIntCounterVec<3>,
    pub materialize_current_epoch: RelabeledGuardedIntGaugeVec<3>,
}

pub static GLOBAL_STREAMING_METRICS: OnceLock<StreamingMetrics> = OnceLock::new();

pub fn global_streaming_metrics(metric_level: MetricLevel) -> StreamingMetrics {
    GLOBAL_STREAMING_METRICS
        .get_or_init(|| StreamingMetrics::new(&GLOBAL_METRICS_REGISTRY, metric_level))
        .clone()
}

impl StreamingMetrics {
    fn new(registry: &Registry, level: MetricLevel) -> Self {
        let executor_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_executor_row_count",
            "Total number of rows that have been output from each executor",
            &["actor_id", "fragment_id", "executor_identity"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let source_output_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts",
            "Total number of rows that have been output from source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let source_split_change_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_split_change_event_count",
            "Total number of split change events that have been operated by source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let source_backfill_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_source_backfill_rows_counts",
            "Total number of rows that have been backfilled for source",
            &["source_id", "source_name", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let sink_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_sink_input_row_count",
            "Total number of rows streamed into sink executors",
            &["sink_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let sink_input_bytes = register_guarded_int_counter_vec_with_registry!(
            "stream_sink_input_bytes",
            "Total size of chunks streamed into sink executors",
            &["sink_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let materialize_input_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_mview_input_row_count",
            "Total number of rows streamed into materialize executors",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_current_epoch = register_guarded_int_gauge_vec_with_registry!(
            "stream_mview_current_epoch",
            "The current epoch of the materialized executor",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let sink_chunk_buffer_size = register_guarded_int_gauge_vec_with_registry!(
            "stream_sink_chunk_buffer_size",
            "Total size of chunks buffered in a barrier",
            &["sink_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let actor_execution_time = register_guarded_gauge_vec_with_registry!(
            "stream_actor_actor_execution_time",
            "Total execution time (s) of an actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_output_buffer_blocking_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "stream_actor_output_buffer_blocking_duration_ns",
                "Total blocking duration (ns) of output buffer",
                &["actor_id", "fragment_id", "downstream_fragment_id"],
                registry
            )
            .unwrap()
            // mask the first label `actor_id` if the level is less verbose than `Debug`
            .relabel_debug_1(level);

        let actor_input_buffer_blocking_duration_ns =
            register_guarded_int_counter_vec_with_registry!(
                "stream_actor_input_buffer_blocking_duration_ns",
                "Total blocking duration (ns) of input buffer",
                &["actor_id", "fragment_id", "upstream_fragment_id"],
                registry
            )
            .unwrap();

        let exchange_frag_recv_size = register_guarded_int_counter_vec_with_registry!(
            "stream_exchange_frag_recv_size",
            "Total size of messages that have been received from upstream Fragment",
            &["up_fragment_id", "down_fragment_id"],
            registry
        )
        .unwrap();

        let actor_fast_poll_duration = register_guarded_gauge_vec_with_registry!(
            "stream_actor_fast_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_fast_poll_cnt = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_fast_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_slow_poll_duration = register_guarded_gauge_vec_with_registry!(
            "stream_actor_slow_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_slow_poll_cnt = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_slow_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_poll_duration = register_guarded_gauge_vec_with_registry!(
            "stream_actor_poll_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_poll_cnt = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_poll_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_scheduled_duration = register_guarded_gauge_vec_with_registry!(
            "stream_actor_scheduled_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_scheduled_cnt = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_scheduled_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_idle_duration = register_guarded_gauge_vec_with_registry!(
            "stream_actor_idle_duration",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_idle_cnt = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_idle_cnt",
            "tokio's metrics",
            &["actor_id"],
            registry
        )
        .unwrap();

        let actor_in_record_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_in_record_cnt",
            "Total number of rows actor received",
            &["actor_id", "fragment_id", "upstream_fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_out_record_cnt = register_guarded_int_counter_vec_with_registry!(
            "stream_actor_out_record_cnt",
            "Total number of rows actor sent",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_current_epoch = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_current_epoch",
            "Current epoch of actor",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let actor_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_actor_count",
            "Total number of actors (parallelism)",
            &["fragment_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "stream_merge_barrier_align_duration",
            "Duration of merge align barrier",
            exponential_buckets(0.0001, 2.0, 21).unwrap() // max 104s
        );
        let merge_barrier_align_duration = register_guarded_histogram_vec_with_registry!(
            opts,
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_lookup_miss_count",
            "Join executor lookup miss duration",
            &["side", "join_table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let join_lookup_total_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_lookup_total_count",
            "Join executor lookup total operation",
            &["side", "join_table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let join_insert_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_join_insert_cache_miss_count",
            "Join executor cache miss when insert operation",
            &["side", "join_table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let join_actor_input_waiting_duration_ns = register_guarded_int_counter_vec_with_registry!(
            "stream_join_actor_input_waiting_duration_ns",
            "Total waiting duration (ns) of input buffer of join actor",
            &["actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let join_match_duration_ns = register_guarded_int_counter_vec_with_registry!(
            "stream_join_match_duration_ns",
            "Matching duration for each side",
            &["actor_id", "fragment_id", "side"],
            registry
        )
        .unwrap();

        let barrier_align_duration = register_guarded_int_counter_vec_with_registry!(
            "stream_barrier_align_duration_ns",
            "Duration of join align barrier",
            &["actor_id", "fragment_id", "wait_side", "executor"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let join_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_join_cached_entry_count",
            "Number of cached entries in streaming join operators",
            &["actor_id", "fragment_id", "side"],
            registry
        )
        .unwrap();

        let join_matched_join_keys_opts = histogram_opts!(
            "stream_join_matched_join_keys",
            "The number of keys matched in the opposite side",
            exponential_buckets(16.0, 2.0, 28).unwrap() // max 2^31
        );

        let join_matched_join_keys = register_guarded_histogram_vec_with_registry!(
            join_matched_join_keys_opts,
            &["actor_id", "fragment_id", "table_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let agg_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_lookup_miss_count",
            "Aggregation executor lookup miss duration",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_total_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_lookup_total_count",
            "Aggregation executor lookup total operation",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_distinct_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_distinct_cache_miss_count",
            "Aggregation executor dinsinct miss duration",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_distinct_total_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_distinct_total_cache_count",
            "Aggregation executor distinct total operation",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_distinct_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_distinct_cached_entry_count",
            "Total entry counts in distinct aggregation executor cache",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_dirty_groups_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_dirty_groups_count",
            "Total dirty group counts in aggregation executor",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_dirty_groups_heap_size = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_dirty_groups_heap_size",
            "Total dirty group heap size in aggregation executor",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_state_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_state_cache_lookup_count",
            "Aggregation executor state cache lookup count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_state_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_state_cache_miss_count",
            "Aggregation executor state cache miss count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let group_top_n_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_group_top_n_cache_miss_count",
            "Group top n executor cache miss count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let group_top_n_total_query_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_group_top_n_total_query_cache_count",
            "Group top n executor query cache total count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let group_top_n_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_group_top_n_cached_entry_count",
            "Total entry counts in group top n executor cache",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let group_top_n_appendonly_cache_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_group_top_n_appendonly_cache_miss_count",
                "Group top n appendonly executor cache miss count",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let group_top_n_appendonly_total_query_cache_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_group_top_n_appendonly_total_query_cache_count",
                "Group top n appendonly executor total cache count",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let group_top_n_appendonly_cached_entry_count =
            register_guarded_int_gauge_vec_with_registry!(
                "stream_group_top_n_appendonly_cached_entry_count",
                "Total entry counts in group top n appendonly executor cache",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let lookup_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_lookup_cache_miss_count",
            "Lookup executor cache miss count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let lookup_total_query_cache_count = register_guarded_int_counter_vec_with_registry!(
            "stream_lookup_total_query_cache_count",
            "Lookup executor query cache total count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let lookup_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_lookup_cached_entry_count",
            "Total entry counts in lookup executor cache",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let temporal_join_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_temporal_join_cache_miss_count",
            "Temporal join executor cache miss count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let temporal_join_total_query_cache_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_temporal_join_total_query_cache_count",
                "Temporal join executor query cache total count",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let temporal_join_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_temporal_join_cached_entry_count",
            "Total entry count in temporal join executor cache",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_agg_cached_entry_count",
            "Number of cached keys in streaming aggregation operators",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_chunk_lookup_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_chunk_lookup_miss_count",
            "Aggregation executor chunk-level lookup miss duration",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let agg_chunk_total_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_agg_chunk_lookup_total_count",
            "Aggregation executor chunk-level lookup total operation",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let backfill_snapshot_read_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_backfill_snapshot_read_row_count",
            "Total number of rows that have been read from the backfill snapshot",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let backfill_upstream_output_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_backfill_upstream_output_row_count",
            "Total number of rows that have been output from the backfill upstream",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let cdc_backfill_snapshot_read_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_cdc_backfill_snapshot_read_row_count",
            "Total number of rows that have been read from the cdc_backfill snapshot",
            &["table_id", "actor_id"],
            registry
        )
        .unwrap();

        let cdc_backfill_upstream_output_row_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_cdc_backfill_upstream_output_row_count",
                "Total number of rows that have been output from the cdc_backfill upstream",
                &["table_id", "actor_id"],
                registry
            )
            .unwrap();

        let snapshot_backfill_consume_row_count = register_guarded_int_counter_vec_with_registry!(
            "stream_snapshot_backfill_consume_snapshot_row_count",
            "Total number of rows that have been output from snapshot backfill",
            &["table_id", "actor_id", "stage"],
            registry
        )
        .unwrap();

        let over_window_cached_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_over_window_cached_entry_count",
            "Total entry (partition) count in over window executor cache",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_cache_lookup_count",
            "Over window executor cache lookup count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_cache_miss_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_cache_miss_count",
            "Over window executor cache miss count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_range_cache_entry_count = register_guarded_int_gauge_vec_with_registry!(
            "stream_over_window_range_cache_entry_count",
            "Over window partition range cache entry count",
            &["table_id", "actor_id", "fragment_id"],
            registry,
        )
        .unwrap();

        let over_window_range_cache_lookup_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_range_cache_lookup_count",
            "Over window partition range cache lookup count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_range_cache_left_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_over_window_range_cache_left_miss_count",
                "Over window partition range cache left miss count",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let over_window_range_cache_right_miss_count =
            register_guarded_int_counter_vec_with_registry!(
                "stream_over_window_range_cache_right_miss_count",
                "Over window partition range cache right miss count",
                &["table_id", "actor_id", "fragment_id"],
                registry
            )
            .unwrap();

        let over_window_accessed_entry_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_accessed_entry_count",
            "Over window accessed entry count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_compute_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_compute_count",
            "Over window compute count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let over_window_same_output_count = register_guarded_int_counter_vec_with_registry!(
            "stream_over_window_same_output_count",
            "Over window same output count",
            &["table_id", "actor_id", "fragment_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "stream_barrier_inflight_duration_seconds",
            "barrier_inflight_latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43s
        );
        let barrier_inflight_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "stream_barrier_sync_storage_duration_seconds",
            "barrier_sync_latency",
            exponential_buckets(0.1, 1.5, 16).unwrap() // max 43
        );
        let barrier_sync_latency = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "stream_barrier_batch_size",
            "barrier_batch_size",
            exponential_buckets(1.0, 2.0, 8).unwrap()
        );
        let barrier_batch_size = register_histogram_with_registry!(opts, registry).unwrap();

        let barrier_manager_progress = register_int_counter_with_registry!(
            "stream_barrier_manager_progress",
            "The number of actors that have processed the earliest in-flight barriers",
            registry
        )
        .unwrap();

        let kv_log_store_storage_write_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_write_count",
            "Write row count throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let kv_log_store_storage_write_size = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_write_size",
            "Write size throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let kv_log_store_storage_read_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_read_count",
            "Write row count throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name", "read_type"],
            registry
        )
        .unwrap();

        let kv_log_store_storage_read_size = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_storage_read_size",
            "Write size throughput of kv log store",
            &["actor_id", "connector", "sink_id", "sink_name", "read_type"],
            registry
        )
        .unwrap();

        let kv_log_store_rewind_count = register_guarded_int_counter_vec_with_registry!(
            "kv_log_store_rewind_count",
            "Kv log store rewind rate",
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let kv_log_store_rewind_delay_opts = {
            assert_eq!(2, REWIND_BACKOFF_FACTOR);
            let bucket_count = (REWIND_MAX_DELAY.as_secs_f64().log2()
                - REWIND_BASE_DELAY.as_secs_f64().log2())
            .ceil() as usize;
            let buckets = exponential_buckets(
                REWIND_BASE_DELAY.as_secs_f64(),
                REWIND_BACKOFF_FACTOR as _,
                bucket_count,
            )
            .unwrap();
            histogram_opts!(
                "kv_log_store_rewind_delay",
                "Kv log store rewind delay",
                buckets,
            )
        };

        let kv_log_store_rewind_delay = register_guarded_histogram_vec_with_registry!(
            kv_log_store_rewind_delay_opts,
            &["actor_id", "connector", "sink_id", "sink_name"],
            registry
        )
        .unwrap();

        let kv_log_store_buffer_unconsumed_item_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_item_count",
                "Number of Unconsumed Item in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap();

        let kv_log_store_buffer_unconsumed_row_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_row_count",
                "Number of Unconsumed Row in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap();

        let kv_log_store_buffer_unconsumed_epoch_count =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_epoch_count",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap();

        let kv_log_store_buffer_unconsumed_min_epoch =
            register_guarded_int_gauge_vec_with_registry!(
                "kv_log_store_buffer_unconsumed_min_epoch",
                "Number of Unconsumed Epoch in buffer",
                &["actor_id", "connector", "sink_id", "sink_name"],
                registry
            )
            .unwrap();

        let lru_runtime_loop_count = register_int_counter_with_registry!(
            "lru_runtime_loop_count",
            "The counts of the eviction loop in LRU manager per second",
            registry
        )
        .unwrap();

        let lru_latest_sequence = register_int_gauge_with_registry!(
            "lru_latest_sequence",
            "Current LRU global sequence",
            registry,
        )
        .unwrap();

        let lru_watermark_sequence = register_int_gauge_with_registry!(
            "lru_watermark_sequence",
            "Current LRU watermark sequence",
            registry,
        )
        .unwrap();

        let lru_eviction_policy = register_int_gauge_with_registry!(
            "lru_eviction_policy",
            "Current LRU eviction policy",
            registry,
        )
        .unwrap();

        let jemalloc_allocated_bytes = register_int_gauge_with_registry!(
            "jemalloc_allocated_bytes",
            "The allocated memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_active_bytes = register_int_gauge_with_registry!(
            "jemalloc_active_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_resident_bytes = register_int_gauge_with_registry!(
            "jemalloc_resident_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jemalloc_metadata_bytes = register_int_gauge_with_registry!(
            "jemalloc_metadata_bytes",
            "The active memory jemalloc, got from jemalloc_ctl",
            registry
        )
        .unwrap();

        let jvm_allocated_bytes = register_int_gauge_with_registry!(
            "jvm_allocated_bytes",
            "The allocated jvm memory",
            registry
        )
        .unwrap();

        let jvm_active_bytes = register_int_gauge_with_registry!(
            "jvm_active_bytes",
            "The active jvm memory",
            registry
        )
        .unwrap();

        let materialize_cache_hit_count = register_guarded_int_counter_vec_with_registry!(
            "stream_materialize_cache_hit_count",
            "Materialize executor cache hit count",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let materialize_cache_total_count = register_guarded_int_counter_vec_with_registry!(
            "stream_materialize_cache_total_count",
            "Materialize executor cache total operation",
            &["actor_id", "table_id", "fragment_id"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        let stream_memory_usage = register_guarded_int_gauge_vec_with_registry!(
            "stream_memory_usage",
            "Memory usage for stream executors",
            &["actor_id", "table_id", "desc"],
            registry
        )
        .unwrap()
        .relabel_debug_1(level);

        Self {
            level,
            executor_row_count,
            actor_execution_time,
            actor_scheduled_duration,
            actor_scheduled_cnt,
            actor_fast_poll_duration,
            actor_fast_poll_cnt,
            actor_slow_poll_duration,
            actor_slow_poll_cnt,
            actor_poll_duration,
            actor_poll_cnt,
            actor_idle_duration,
            actor_idle_cnt,
            actor_count,
            actor_in_record_cnt,
            actor_out_record_cnt,
            actor_current_epoch,
            source_output_row_count,
            source_split_change_count,
            source_backfill_row_count,
            sink_input_row_count,
            sink_input_bytes,
            sink_chunk_buffer_size,
            exchange_frag_recv_size,
            merge_barrier_align_duration,
            actor_output_buffer_blocking_duration_ns,
            actor_input_buffer_blocking_duration_ns,
            join_lookup_miss_count,
            join_lookup_total_count,
            join_insert_cache_miss_count,
            join_actor_input_waiting_duration_ns,
            join_match_duration_ns,
            join_cached_entry_count,
            join_matched_join_keys,
            barrier_align_duration,
            agg_lookup_miss_count,
            agg_total_lookup_count,
            agg_cached_entry_count,
            agg_chunk_lookup_miss_count,
            agg_chunk_total_lookup_count,
            agg_dirty_groups_count,
            agg_dirty_groups_heap_size,
            agg_distinct_cache_miss_count,
            agg_distinct_total_cache_count,
            agg_distinct_cached_entry_count,
            agg_state_cache_lookup_count,
            agg_state_cache_miss_count,
            group_top_n_cache_miss_count,
            group_top_n_total_query_cache_count,
            group_top_n_cached_entry_count,
            group_top_n_appendonly_cache_miss_count,
            group_top_n_appendonly_total_query_cache_count,
            group_top_n_appendonly_cached_entry_count,
            lookup_cache_miss_count,
            lookup_total_query_cache_count,
            lookup_cached_entry_count,
            temporal_join_cache_miss_count,
            temporal_join_total_query_cache_count,
            temporal_join_cached_entry_count,
            backfill_snapshot_read_row_count,
            backfill_upstream_output_row_count,
            cdc_backfill_snapshot_read_row_count,
            cdc_backfill_upstream_output_row_count,
            snapshot_backfill_consume_row_count,
            over_window_cached_entry_count,
            over_window_cache_lookup_count,
            over_window_cache_miss_count,
            over_window_range_cache_entry_count,
            over_window_range_cache_lookup_count,
            over_window_range_cache_left_miss_count,
            over_window_range_cache_right_miss_count,
            over_window_accessed_entry_count,
            over_window_compute_count,
            over_window_same_output_count,
            barrier_inflight_latency,
            barrier_sync_latency,
            barrier_batch_size,
            barrier_manager_progress,
            kv_log_store_storage_write_count,
            kv_log_store_storage_write_size,
            kv_log_store_rewind_count,
            kv_log_store_rewind_delay,
            kv_log_store_storage_read_count,
            kv_log_store_storage_read_size,
            kv_log_store_buffer_unconsumed_item_count,
            kv_log_store_buffer_unconsumed_row_count,
            kv_log_store_buffer_unconsumed_epoch_count,
            kv_log_store_buffer_unconsumed_min_epoch,
            lru_runtime_loop_count,
            lru_latest_sequence,
            lru_watermark_sequence,
            lru_eviction_policy,
            jemalloc_allocated_bytes,
            jemalloc_active_bytes,
            jemalloc_resident_bytes,
            jemalloc_metadata_bytes,
            jvm_allocated_bytes,
            jvm_active_bytes,
            stream_memory_usage,
            materialize_cache_hit_count,
            materialize_cache_total_count,
            materialize_input_row_count,
            materialize_current_epoch,
        }
    }

    /// Create a new `StreamingMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        global_streaming_metrics(MetricLevel::Disabled)
    }

    pub fn new_actor_metrics(&self, actor_id: ActorId) -> ActorMetrics {
        let label_list: &[&str; 1] = &[&actor_id.to_string()];
        let actor_execution_time = self
            .actor_execution_time
            .with_guarded_label_values(label_list);
        let actor_scheduled_duration = self
            .actor_scheduled_duration
            .with_guarded_label_values(label_list);
        let actor_scheduled_cnt = self
            .actor_scheduled_cnt
            .with_guarded_label_values(label_list);
        let actor_fast_poll_duration = self
            .actor_fast_poll_duration
            .with_guarded_label_values(label_list);
        let actor_fast_poll_cnt = self
            .actor_fast_poll_cnt
            .with_guarded_label_values(label_list);
        let actor_slow_poll_duration = self
            .actor_slow_poll_duration
            .with_guarded_label_values(label_list);
        let actor_slow_poll_cnt = self
            .actor_slow_poll_cnt
            .with_guarded_label_values(label_list);
        let actor_poll_duration = self
            .actor_poll_duration
            .with_guarded_label_values(label_list);
        let actor_poll_cnt = self.actor_poll_cnt.with_guarded_label_values(label_list);
        let actor_idle_duration = self
            .actor_idle_duration
            .with_guarded_label_values(label_list);
        let actor_idle_cnt = self.actor_idle_cnt.with_guarded_label_values(label_list);
        ActorMetrics {
            actor_execution_time,
            actor_scheduled_duration,
            actor_scheduled_cnt,
            actor_fast_poll_duration,
            actor_fast_poll_cnt,
            actor_slow_poll_duration,
            actor_slow_poll_cnt,
            actor_poll_duration,
            actor_poll_cnt,
            actor_idle_duration,
            actor_idle_cnt,
        }
    }

    pub(crate) fn new_actor_input_metrics(
        &self,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
    ) -> ActorInputMetrics {
        let actor_id_str = actor_id.to_string();
        let fragment_id_str = fragment_id.to_string();
        let upstream_fragment_id_str = upstream_fragment_id.to_string();
        ActorInputMetrics {
            actor_in_record_cnt: self.actor_in_record_cnt.with_guarded_label_values(&[
                &actor_id_str,
                &fragment_id_str,
                &upstream_fragment_id_str,
            ]),
            actor_input_buffer_blocking_duration_ns: self
                .actor_input_buffer_blocking_duration_ns
                .with_guarded_label_values(&[
                    &actor_id_str,
                    &fragment_id_str,
                    &upstream_fragment_id_str,
                ]),
        }
    }

    pub fn new_sink_exec_metrics(
        &self,
        id: SinkId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> SinkExecutorMetrics {
        let label_list: &[&str; 3] = &[
            &id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];
        SinkExecutorMetrics {
            sink_input_row_count: self
                .sink_input_row_count
                .with_guarded_label_values(label_list),
            sink_input_bytes: self.sink_input_bytes.with_guarded_label_values(label_list),
            sink_chunk_buffer_size: self
                .sink_chunk_buffer_size
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_group_top_n_metrics(
        &self,
        table_id: u32,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> GroupTopNMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];

        GroupTopNMetrics {
            group_top_n_cache_miss_count: self
                .group_top_n_cache_miss_count
                .with_guarded_label_values(label_list),
            group_top_n_total_query_cache_count: self
                .group_top_n_total_query_cache_count
                .with_guarded_label_values(label_list),
            group_top_n_cached_entry_count: self
                .group_top_n_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_append_only_group_top_n_metrics(
        &self,
        table_id: u32,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> GroupTopNMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];

        GroupTopNMetrics {
            group_top_n_cache_miss_count: self
                .group_top_n_appendonly_cache_miss_count
                .with_guarded_label_values(label_list),
            group_top_n_total_query_cache_count: self
                .group_top_n_appendonly_total_query_cache_count
                .with_guarded_label_values(label_list),
            group_top_n_cached_entry_count: self
                .group_top_n_appendonly_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_lookup_executor_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> LookupExecutorMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];

        LookupExecutorMetrics {
            lookup_cache_miss_count: self
                .lookup_cache_miss_count
                .with_guarded_label_values(label_list),
            lookup_total_query_cache_count: self
                .lookup_total_query_cache_count
                .with_guarded_label_values(label_list),
            lookup_cached_entry_count: self
                .lookup_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_hash_agg_metrics(
        &self,
        table_id: u32,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> HashAggMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];
        HashAggMetrics {
            agg_lookup_miss_count: self
                .agg_lookup_miss_count
                .with_guarded_label_values(label_list),
            agg_total_lookup_count: self
                .agg_total_lookup_count
                .with_guarded_label_values(label_list),
            agg_cached_entry_count: self
                .agg_cached_entry_count
                .with_guarded_label_values(label_list),
            agg_chunk_lookup_miss_count: self
                .agg_chunk_lookup_miss_count
                .with_guarded_label_values(label_list),
            agg_chunk_total_lookup_count: self
                .agg_chunk_total_lookup_count
                .with_guarded_label_values(label_list),
            agg_dirty_groups_count: self
                .agg_dirty_groups_count
                .with_guarded_label_values(label_list),
            agg_dirty_groups_heap_size: self
                .agg_dirty_groups_heap_size
                .with_guarded_label_values(label_list),
            agg_state_cache_lookup_count: self
                .agg_state_cache_lookup_count
                .with_guarded_label_values(label_list),
            agg_state_cache_miss_count: self
                .agg_state_cache_miss_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_agg_distinct_dedup_metrics(
        &self,
        table_id: u32,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> AggDistinctDedupMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];
        AggDistinctDedupMetrics {
            agg_distinct_cache_miss_count: self
                .agg_distinct_cache_miss_count
                .with_guarded_label_values(label_list),
            agg_distinct_total_cache_count: self
                .agg_distinct_total_cache_count
                .with_guarded_label_values(label_list),
            agg_distinct_cached_entry_count: self
                .agg_distinct_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_temporal_join_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> TemporalJoinMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];
        TemporalJoinMetrics {
            temporal_join_cache_miss_count: self
                .temporal_join_cache_miss_count
                .with_guarded_label_values(label_list),
            temporal_join_total_query_cache_count: self
                .temporal_join_total_query_cache_count
                .with_guarded_label_values(label_list),
            temporal_join_cached_entry_count: self
                .temporal_join_cached_entry_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_backfill_metrics(&self, table_id: u32, actor_id: ActorId) -> BackfillMetrics {
        let label_list: &[&str; 2] = &[&table_id.to_string(), &actor_id.to_string()];
        BackfillMetrics {
            backfill_snapshot_read_row_count: self
                .backfill_snapshot_read_row_count
                .with_guarded_label_values(label_list),
            backfill_upstream_output_row_count: self
                .backfill_upstream_output_row_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_cdc_backfill_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
    ) -> CdcBackfillMetrics {
        let label_list: &[&str; 2] = &[&table_id.to_string(), &actor_id.to_string()];
        CdcBackfillMetrics {
            cdc_backfill_snapshot_read_row_count: self
                .cdc_backfill_snapshot_read_row_count
                .with_guarded_label_values(label_list),
            cdc_backfill_upstream_output_row_count: self
                .cdc_backfill_upstream_output_row_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_over_window_metrics(
        &self,
        table_id: u32,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> OverWindowMetrics {
        let label_list: &[&str; 3] = &[
            &table_id.to_string(),
            &actor_id.to_string(),
            &fragment_id.to_string(),
        ];
        OverWindowMetrics {
            over_window_cached_entry_count: self
                .over_window_cached_entry_count
                .with_guarded_label_values(label_list),
            over_window_cache_lookup_count: self
                .over_window_cache_lookup_count
                .with_guarded_label_values(label_list),
            over_window_cache_miss_count: self
                .over_window_cache_miss_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_entry_count: self
                .over_window_range_cache_entry_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_lookup_count: self
                .over_window_range_cache_lookup_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_left_miss_count: self
                .over_window_range_cache_left_miss_count
                .with_guarded_label_values(label_list),
            over_window_range_cache_right_miss_count: self
                .over_window_range_cache_right_miss_count
                .with_guarded_label_values(label_list),
            over_window_accessed_entry_count: self
                .over_window_accessed_entry_count
                .with_guarded_label_values(label_list),
            over_window_compute_count: self
                .over_window_compute_count
                .with_guarded_label_values(label_list),
            over_window_same_output_count: self
                .over_window_same_output_count
                .with_guarded_label_values(label_list),
        }
    }

    pub fn new_materialize_metrics(
        &self,
        table_id: TableId,
        actor_id: ActorId,
        fragment_id: FragmentId,
    ) -> MaterializeMetrics {
        let label_list: &[&str; 3] = &[
            &actor_id.to_string(),
            &table_id.to_string(),
            &fragment_id.to_string(),
        ];
        MaterializeMetrics {
            materialize_cache_hit_count: self
                .materialize_cache_hit_count
                .with_guarded_label_values(label_list),
            materialize_cache_total_count: self
                .materialize_cache_total_count
                .with_guarded_label_values(label_list),
            materialize_input_row_count: self
                .materialize_input_row_count
                .with_guarded_label_values(label_list),
            materialize_current_epoch: self
                .materialize_current_epoch
                .with_guarded_label_values(label_list),
        }
    }
}

pub(crate) struct ActorInputMetrics {
    pub(crate) actor_in_record_cnt: LabelGuardedIntCounter<3>,
    pub(crate) actor_input_buffer_blocking_duration_ns: LabelGuardedIntCounter<3>,
}

/// Tokio metrics for actors
pub struct ActorMetrics {
    pub actor_execution_time: LabelGuardedGauge<1>,
    pub actor_scheduled_duration: LabelGuardedGauge<1>,
    pub actor_scheduled_cnt: LabelGuardedIntGauge<1>,
    pub actor_fast_poll_duration: LabelGuardedGauge<1>,
    pub actor_fast_poll_cnt: LabelGuardedIntGauge<1>,
    pub actor_slow_poll_duration: LabelGuardedGauge<1>,
    pub actor_slow_poll_cnt: LabelGuardedIntGauge<1>,
    pub actor_poll_duration: LabelGuardedGauge<1>,
    pub actor_poll_cnt: LabelGuardedIntGauge<1>,
    pub actor_idle_duration: LabelGuardedGauge<1>,
    pub actor_idle_cnt: LabelGuardedIntGauge<1>,
}

pub struct SinkExecutorMetrics {
    pub sink_input_row_count: LabelGuardedIntCounter<3>,
    pub sink_input_bytes: LabelGuardedIntCounter<3>,
    pub sink_chunk_buffer_size: LabelGuardedIntGauge<3>,
}

pub struct MaterializeMetrics {
    pub materialize_cache_hit_count: LabelGuardedIntCounter<3>,
    pub materialize_cache_total_count: LabelGuardedIntCounter<3>,
    pub materialize_input_row_count: LabelGuardedIntCounter<3>,
    pub materialize_current_epoch: LabelGuardedIntGauge<3>,
}

pub struct GroupTopNMetrics {
    pub group_top_n_cache_miss_count: LabelGuardedIntCounter<3>,
    pub group_top_n_total_query_cache_count: LabelGuardedIntCounter<3>,
    pub group_top_n_cached_entry_count: LabelGuardedIntGauge<3>,
}

pub struct LookupExecutorMetrics {
    pub lookup_cache_miss_count: LabelGuardedIntCounter<3>,
    pub lookup_total_query_cache_count: LabelGuardedIntCounter<3>,
    pub lookup_cached_entry_count: LabelGuardedIntGauge<3>,
}

pub struct HashAggMetrics {
    pub agg_lookup_miss_count: LabelGuardedIntCounter<3>,
    pub agg_total_lookup_count: LabelGuardedIntCounter<3>,
    pub agg_cached_entry_count: LabelGuardedIntGauge<3>,
    pub agg_chunk_lookup_miss_count: LabelGuardedIntCounter<3>,
    pub agg_chunk_total_lookup_count: LabelGuardedIntCounter<3>,
    pub agg_dirty_groups_count: LabelGuardedIntGauge<3>,
    pub agg_dirty_groups_heap_size: LabelGuardedIntGauge<3>,
    pub agg_state_cache_lookup_count: LabelGuardedIntCounter<3>,
    pub agg_state_cache_miss_count: LabelGuardedIntCounter<3>,
}

pub struct AggDistinctDedupMetrics {
    pub agg_distinct_cache_miss_count: LabelGuardedIntCounter<3>,
    pub agg_distinct_total_cache_count: LabelGuardedIntCounter<3>,
    pub agg_distinct_cached_entry_count: LabelGuardedIntGauge<3>,
}

pub struct TemporalJoinMetrics {
    pub temporal_join_cache_miss_count: LabelGuardedIntCounter<3>,
    pub temporal_join_total_query_cache_count: LabelGuardedIntCounter<3>,
    pub temporal_join_cached_entry_count: LabelGuardedIntGauge<3>,
}

pub struct BackfillMetrics {
    pub backfill_snapshot_read_row_count: LabelGuardedIntCounter<2>,
    pub backfill_upstream_output_row_count: LabelGuardedIntCounter<2>,
}

pub struct CdcBackfillMetrics {
    pub cdc_backfill_snapshot_read_row_count: LabelGuardedIntCounter<2>,
    pub cdc_backfill_upstream_output_row_count: LabelGuardedIntCounter<2>,
}

pub struct OverWindowMetrics {
    pub over_window_cached_entry_count: LabelGuardedIntGauge<3>,
    pub over_window_cache_lookup_count: LabelGuardedIntCounter<3>,
    pub over_window_cache_miss_count: LabelGuardedIntCounter<3>,
    pub over_window_range_cache_entry_count: LabelGuardedIntGauge<3>,
    pub over_window_range_cache_lookup_count: LabelGuardedIntCounter<3>,
    pub over_window_range_cache_left_miss_count: LabelGuardedIntCounter<3>,
    pub over_window_range_cache_right_miss_count: LabelGuardedIntCounter<3>,
    pub over_window_accessed_entry_count: LabelGuardedIntCounter<3>,
    pub over_window_compute_count: LabelGuardedIntCounter<3>,
    pub over_window_same_output_count: LabelGuardedIntCounter<3>,
}
