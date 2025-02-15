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

//! This module defines the structure of the configuration file `risingwave.toml`.
//!
//! [`RwConfig`] corresponds to the whole config file and each other config struct corresponds to a
//! section in `risingwave.toml`.

use std::collections::BTreeMap;
use std::fs;
use std::num::NonZeroUsize;

use anyhow::Context;
use clap::ValueEnum;
use educe::Educe;
use foyer::{Compression, LfuConfig, LruConfig, RecoverMode, RuntimeOptions, S3FifoConfig};
use risingwave_common_proc_macro::ConfigDoc;
pub use risingwave_common_proc_macro::OverrideConfig;
use risingwave_pb::meta::SystemParams;
use serde::{Deserialize, Serialize, Serializer};
use serde_default::DefaultFromSerde;
use serde_json::Value;

use crate::for_all_params;

/// Use the maximum value for HTTP/2 connection window size to avoid deadlock among multiplexed
/// streams on the same connection.
pub const MAX_CONNECTION_WINDOW_SIZE: u32 = (1 << 31) - 1;
/// Use a large value for HTTP/2 stream window size to improve the performance of remote exchange,
/// as we don't rely on this for back-pressure.
pub const STREAM_WINDOW_SIZE: u32 = 32 * 1024 * 1024; // 32 MB

/// Unrecognized fields in a config section. Generic over the config section type to provide better
/// error messages.
///
/// The current implementation will log warnings if there are unrecognized fields.
#[derive(Educe)]
#[educe(Clone, Default)]
pub struct Unrecognized<T: 'static> {
    inner: BTreeMap<String, Value>,
    _marker: std::marker::PhantomData<&'static T>,
}

impl<T> std::fmt::Debug for Unrecognized<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Unrecognized<T> {
    /// Returns all unrecognized fields as a map.
    pub fn into_inner(self) -> BTreeMap<String, Value> {
        self.inner
    }
}

impl<'de, T> Deserialize<'de> for Unrecognized<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let inner = BTreeMap::deserialize(deserializer)?;
        if !inner.is_empty() {
            tracing::warn!(
                "unrecognized fields in `{}`: {:?}",
                std::any::type_name::<T>(),
                inner.keys()
            );
        }
        Ok(Unrecognized {
            inner,
            _marker: std::marker::PhantomData,
        })
    }
}

impl<T> Serialize for Unrecognized<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.inner.serialize(serializer)
    }
}

pub fn load_config(path: &str, cli_override: impl OverrideConfig) -> RwConfig
where
{
    let mut config = if path.is_empty() {
        tracing::warn!("risingwave.toml not found, using default config.");
        RwConfig::default()
    } else {
        let config_str = fs::read_to_string(path)
            .with_context(|| format!("failed to open config file at `{path}`"))
            .unwrap();
        toml::from_str(config_str.as_str())
            .context("failed to parse config file")
            .unwrap()
    };
    cli_override.r#override(&mut config);
    config
}

pub trait OverrideConfig {
    fn r#override(&self, config: &mut RwConfig);
}

impl<T: OverrideConfig> OverrideConfig for &T {
    fn r#override(&self, config: &mut RwConfig) {
        T::r#override(self, config)
    }
}

/// For non-user-facing components where the CLI arguments do not override the config file.
#[derive(Clone, Copy)]
pub struct NoOverride;

impl OverrideConfig for NoOverride {
    fn r#override(&self, _config: &mut RwConfig) {}
}

/// [`RwConfig`] corresponds to the whole config file `risingwave.toml`. Each field corresponds to a
/// section.
#[derive(Educe, Clone, Serialize, Deserialize, Default, ConfigDoc)]
#[educe(Debug)]
pub struct RwConfig {
    #[serde(default)]
    #[config_doc(nested)]
    pub server: ServerConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta: MetaConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub batch: BatchConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub streaming: StreamingConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub storage: StorageConfig,

    #[serde(default)]
    #[educe(Debug(ignore))]
    #[config_doc(nested)]
    pub system: SystemConfig,

    #[serde(flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

serde_with::with_prefix!(meta_prefix "meta_");
serde_with::with_prefix!(streaming_prefix "stream_");
serde_with::with_prefix!(batch_prefix "batch_");

#[derive(Copy, Clone, Debug, Default, ValueEnum, Serialize, Deserialize)]
pub enum MetaBackend {
    #[default]
    Mem,
    Sql, // any database url
    Sqlite,
    Postgres,
    Mysql,
}

/// The section `[meta]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaConfig {
    /// Objects within `min_sst_retention_time_sec` won't be deleted by hummock full GC, even they
    /// are dangling.
    #[serde(default = "default::meta::min_sst_retention_time_sec")]
    pub min_sst_retention_time_sec: u64,

    /// Interval of automatic hummock full GC.
    #[serde(default = "default::meta::full_gc_interval_sec")]
    pub full_gc_interval_sec: u64,

    /// Max number of object per full GC job can fetch.
    #[serde(default = "default::meta::full_gc_object_limit")]
    pub full_gc_object_limit: u64,

    /// Duration in seconds to retain garbage collection history data.
    #[serde(default = "default::meta::gc_history_retention_time_sec")]
    pub gc_history_retention_time_sec: u64,

    /// Max number of inflight time travel query.
    #[serde(default = "default::meta::max_inflight_time_travel_query")]
    pub max_inflight_time_travel_query: u64,

    /// Schedule compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_compaction_interval_sec")]
    pub periodic_compaction_interval_sec: u64,

    /// Interval of invoking a vacuum job, to remove stale metadata from meta store and objects
    /// from object store.
    #[serde(default = "default::meta::vacuum_interval_sec")]
    pub vacuum_interval_sec: u64,

    /// The spin interval inside a vacuum job. It avoids the vacuum job monopolizing resources of
    /// meta node.
    #[serde(default = "default::meta::vacuum_spin_interval_ms")]
    pub vacuum_spin_interval_ms: u64,

    /// Interval of hummock version checkpoint.
    #[serde(default = "default::meta::hummock_version_checkpoint_interval_sec")]
    pub hummock_version_checkpoint_interval_sec: u64,

    /// If enabled, `SSTable` object file and version delta will be retained.
    ///
    /// `SSTable` object file need to be deleted via full GC.
    ///
    /// version delta need to be manually deleted.
    #[serde(default = "default::meta::enable_hummock_data_archive")]
    pub enable_hummock_data_archive: bool,

    /// The interval at which a Hummock version snapshot is taken for time travel.
    ///
    /// Larger value indicates less storage overhead but worse query performance.
    #[serde(default = "default::meta::hummock_time_travel_snapshot_interval")]
    pub hummock_time_travel_snapshot_interval: u64,

    /// The minimum delta log number a new checkpoint should compact, otherwise the checkpoint
    /// attempt is rejected.
    #[serde(default = "default::meta::min_delta_log_num_for_hummock_version_checkpoint")]
    pub min_delta_log_num_for_hummock_version_checkpoint: u64,

    /// Maximum allowed heartbeat interval in seconds.
    #[serde(default = "default::meta::max_heartbeat_interval_sec")]
    pub max_heartbeat_interval_secs: u32,

    /// Whether to enable fail-on-recovery. Should only be used in e2e tests.
    #[serde(default)]
    pub disable_recovery: bool,

    /// Whether to disable adaptive-scaling feature.
    #[serde(default)]
    pub disable_automatic_parallelism_control: bool,

    /// The number of streaming jobs per scaling operation.
    #[serde(default = "default::meta::parallelism_control_batch_size")]
    pub parallelism_control_batch_size: usize,

    /// The period of parallelism control trigger.
    #[serde(default = "default::meta::parallelism_control_trigger_period_sec")]
    pub parallelism_control_trigger_period_sec: u64,

    /// The first delay of parallelism control.
    #[serde(default = "default::meta::parallelism_control_trigger_first_delay_sec")]
    pub parallelism_control_trigger_first_delay_sec: u64,

    #[serde(default = "default::meta::meta_leader_lease_secs")]
    pub meta_leader_lease_secs: u64,

    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// It is mainly useful for playgrounds.
    #[serde(default)]
    pub dangerous_max_idle_secs: Option<u64>,

    /// The default global parallelism for all streaming jobs, if user doesn't specify the
    /// parallelism, this value will be used. `FULL` means use all available parallelism units,
    /// otherwise it's a number.
    #[serde(default = "default::meta::default_parallelism")]
    pub default_parallelism: DefaultParallelism,

    /// Whether to enable deterministic compaction scheduling, which
    /// will disable all auto scheduling of compaction tasks.
    /// Should only be used in e2e tests.
    #[serde(default)]
    pub enable_compaction_deterministic: bool,

    /// Enable sanity check when SSTs are committed.
    #[serde(default)]
    pub enable_committed_sst_sanity_check: bool,

    #[serde(default = "default::meta::node_num_monitor_interval_sec")]
    pub node_num_monitor_interval_sec: u64,

    #[serde(default = "default::meta::backend")]
    pub backend: MetaBackend,

    /// Schedule `space_reclaim` compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_space_reclaim_compaction_interval_sec")]
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// Schedule `ttl_reclaim` compaction for all compaction groups with this interval.
    #[serde(default = "default::meta::periodic_ttl_reclaim_compaction_interval_sec")]
    pub periodic_ttl_reclaim_compaction_interval_sec: u64,

    #[serde(default = "default::meta::periodic_tombstone_reclaim_compaction_interval_sec")]
    pub periodic_tombstone_reclaim_compaction_interval_sec: u64,

    #[serde(default = "default::meta::move_table_size_limit")]
    #[deprecated]
    pub move_table_size_limit: u64,

    #[serde(default = "default::meta::split_group_size_limit")]
    #[deprecated]
    pub split_group_size_limit: u64,

    #[serde(default = "default::meta::cut_table_size_limit")]
    #[deprecated]
    pub cut_table_size_limit: u64,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// Whether config object storage bucket lifecycle to purge stale data.
    #[serde(default)]
    pub do_not_config_object_storage_lifecycle: bool,

    /// Count of partition in split group. Meta will assign this value to every new group when it splits from default-group by automatically.
    /// Each partition contains aligned data of `vnode_count / partition_vnode_count` consecutive virtual-nodes of one state table.
    #[serde(default = "default::meta::partition_vnode_count")]
    pub partition_vnode_count: u32,

    /// The threshold of write throughput to trigger a group split.
    #[serde(
        default = "default::meta::table_high_write_throughput_threshold",
        alias = "table_write_throughput_threshold"
    )]
    pub table_high_write_throughput_threshold: u64,

    #[serde(
        default = "default::meta::table_low_write_throughput_threshold",
        alias = "min_table_split_write_throughput"
    )]
    /// The threshold of write throughput to trigger a group merge.
    pub table_low_write_throughput_threshold: u64,

    // If the compaction task does not report heartbeat beyond the
    // `compaction_task_max_heartbeat_interval_secs` interval, we will cancel the task
    #[serde(default = "default::meta::compaction_task_max_heartbeat_interval_secs")]
    pub compaction_task_max_heartbeat_interval_secs: u64,

    // If the compaction task does not change in progress beyond the
    // `compaction_task_max_heartbeat_interval_secs` interval, we will cancel the task
    #[serde(default = "default::meta::compaction_task_max_progress_interval_secs")]
    pub compaction_task_max_progress_interval_secs: u64,

    #[serde(default)]
    #[config_doc(nested)]
    pub compaction_config: CompactionConfig,

    /// Count of partitions of tables in default group and materialized view group.
    /// The meta node will decide according to some strategy whether to cut the boundaries of the file according to the vnode alignment.
    /// Each partition contains aligned data of `vnode_count / hybrid_partition_vnode_count` consecutive virtual-nodes of one state table.
    /// Set it zero to disable this feature.
    #[serde(default = "default::meta::hybrid_partition_vnode_count")]
    pub hybrid_partition_vnode_count: u32,

    #[serde(default = "default::meta::event_log_enabled")]
    pub event_log_enabled: bool,
    /// Keeps the latest N events per channel.
    #[serde(default = "default::meta::event_log_channel_max_size")]
    pub event_log_channel_max_size: u32,

    #[serde(default, with = "meta_prefix")]
    #[config_doc(omitted)]
    pub developer: MetaDeveloperConfig,
    /// Whether compactor should rewrite row to remove dropped column.
    #[serde(default = "default::meta::enable_dropped_column_reclaim")]
    pub enable_dropped_column_reclaim: bool,

    /// Whether to split the compaction group when the size of the group exceeds the `compaction_group_config.max_estimated_group_size() * split_group_size_ratio`.
    #[serde(default = "default::meta::split_group_size_ratio")]
    pub split_group_size_ratio: f64,

    // During group scheduling, the configured `*_throughput_ratio` is used to determine if the sample exceeds the threshold.
    // Use `table_stat_throuput_window_seconds_for_*` to check if the split and merge conditions are met.
    /// To split the compaction group when the high throughput statistics of the group exceeds the threshold.
    #[serde(default = "default::meta::table_stat_high_write_throughput_ratio_for_split")]
    pub table_stat_high_write_throughput_ratio_for_split: f64,

    /// To merge the compaction group when the low throughput statistics of the group exceeds the threshold.
    #[serde(default = "default::meta::table_stat_low_write_throughput_ratio_for_merge")]
    pub table_stat_low_write_throughput_ratio_for_merge: f64,

    // Hummock also control the size of samples to be judged during group scheduling by `table_stat_sample_size_for_split` and `table_stat_sample_size_for_merge`.
    // Will use max(table_stat_throuput_window_seconds_for_split /ckpt, table_stat_throuput_window_seconds_for_merge/ckpt) as the global sample size.
    // For example, if `table_stat_throuput_window_seconds_for_merge` = 240 and `table_stat_throuput_window_seconds_for_split` = 60, and `ckpt_sec = 1`,
    //  global sample size will be max(240/1, 60/1), then only the last 60 samples will be considered for split, and so on.
    /// The window seconds of table throughput statistic history for split compaction group.
    #[serde(default = "default::meta::table_stat_throuput_window_seconds_for_split")]
    pub table_stat_throuput_window_seconds_for_split: usize,

    /// The window seconds of table throughput statistic history for merge compaction group.
    #[serde(default = "default::meta::table_stat_throuput_window_seconds_for_merge")]
    pub table_stat_throuput_window_seconds_for_merge: usize,

    /// The threshold of table size in one compact task to decide whether to partition one table into `hybrid_partition_vnode_count` parts, which belongs to default group and materialized view group.
    /// Set it max value of 64-bit number to disable this feature.
    #[serde(default = "default::meta::compact_task_table_size_partition_threshold_low")]
    pub compact_task_table_size_partition_threshold_low: u64,

    /// The threshold of table size in one compact task to decide whether to partition one table into `partition_vnode_count` parts, which belongs to default group and materialized view group.
    /// Set it max value of 64-bit number to disable this feature.
    #[serde(default = "default::meta::compact_task_table_size_partition_threshold_high")]
    pub compact_task_table_size_partition_threshold_high: u64,

    #[serde(
        default = "default::meta::periodic_scheduling_compaction_group_split_interval_sec",
        alias = "periodic_split_compact_group_interval_sec"
    )]
    pub periodic_scheduling_compaction_group_split_interval_sec: u64,

    #[serde(default = "default::meta::periodic_scheduling_compaction_group_merge_interval_sec")]
    pub periodic_scheduling_compaction_group_merge_interval_sec: u64,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta_store_config: MetaStoreConfig,
}

#[derive(Copy, Clone, Debug, Default)]
pub enum DefaultParallelism {
    #[default]
    Full,
    Default(NonZeroUsize),
}

impl Serialize for DefaultParallelism {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Debug, Serialize, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        match self {
            DefaultParallelism::Full => Parallelism::Str("Full".to_owned()).serialize(serializer),
            DefaultParallelism::Default(val) => {
                Parallelism::Int(val.get() as _).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for DefaultParallelism {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(untagged)]
        enum Parallelism {
            Str(String),
            Int(usize),
        }
        let p = Parallelism::deserialize(deserializer)?;
        match p {
            Parallelism::Str(s) => {
                if s.trim().eq_ignore_ascii_case("full") {
                    Ok(DefaultParallelism::Full)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "invalid default parallelism: {}",
                        s
                    )))
                }
            }
            Parallelism::Int(i) => Ok(DefaultParallelism::Default(
                // Note: we won't check whether this exceeds the maximum parallelism (i.e., vnode count)
                // here because it requires extra context. The check will be done when scheduling jobs.
                NonZeroUsize::new(i).ok_or_else(|| {
                    serde::de::Error::custom("default parallelism should not be 0")
                })?,
            )),
        }
    }
}

/// The subsections `[meta.developer]`.
///
/// It is put at [`MetaConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaDeveloperConfig {
    /// The number of traces to be cached in-memory by the tracing collector
    /// embedded in the meta node.
    #[serde(default = "default::developer::meta_cached_traces_num")]
    pub cached_traces_num: u32,

    /// The maximum memory usage in bytes for the tracing collector embedded
    /// in the meta node.
    #[serde(default = "default::developer::meta_cached_traces_memory_limit_bytes")]
    pub cached_traces_memory_limit_bytes: usize,

    /// Compaction picker config
    #[serde(default = "default::developer::enable_trivial_move")]
    pub enable_trivial_move: bool,
    #[serde(default = "default::developer::enable_check_task_level_overlap")]
    pub enable_check_task_level_overlap: bool,
    #[serde(default = "default::developer::max_trivial_move_task_count_per_loop")]
    pub max_trivial_move_task_count_per_loop: usize,

    #[serde(default = "default::developer::max_get_task_probe_times")]
    pub max_get_task_probe_times: usize,

    /// Max number of actor allowed per parallelism (default = 100).
    /// CREATE MV/Table will be noticed when the number of actors exceeds this limit.
    #[serde(default = "default::developer::actor_cnt_per_worker_parallelism_soft_limit")]
    pub actor_cnt_per_worker_parallelism_soft_limit: usize,

    /// Max number of actor allowed per parallelism (default = 400).
    /// CREATE MV/Table will be rejected when the number of actors exceeds this limit.
    #[serde(default = "default::developer::actor_cnt_per_worker_parallelism_hard_limit")]
    pub actor_cnt_per_worker_parallelism_hard_limit: usize,

    /// Max number of SSTs fetched from meta store per SELECT, during time travel Hummock version replay.
    #[serde(default = "default::developer::hummock_time_travel_sst_info_fetch_batch_size")]
    pub hummock_time_travel_sst_info_fetch_batch_size: usize,

    /// Max number of SSTs inserted into meta store per INSERT, during time travel metadata writing.
    #[serde(default = "default::developer::hummock_time_travel_sst_info_insert_batch_size")]
    pub hummock_time_travel_sst_info_insert_batch_size: usize,

    #[serde(default = "default::developer::time_travel_vacuum_interval_sec")]
    pub time_travel_vacuum_interval_sec: u64,

    /// Max number of epoch-to-version inserted into meta store per INSERT, during time travel metadata writing.
    #[serde(default = "default::developer::hummock_time_travel_epoch_version_insert_batch_size")]
    pub hummock_time_travel_epoch_version_insert_batch_size: usize,

    #[serde(default = "default::developer::hummock_gc_history_insert_batch_size")]
    pub hummock_gc_history_insert_batch_size: usize,

    #[serde(default = "default::developer::hummock_time_travel_filter_out_objects_batch_size")]
    pub hummock_time_travel_filter_out_objects_batch_size: usize,
}

/// The section `[server]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct ServerConfig {
    /// The interval for periodic heartbeat from worker to the meta service.
    #[serde(default = "default::server::heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u32,

    /// The default number of the connections when connecting to a gRPC server.
    ///
    /// For the connections used in streaming or batch exchange, please refer to the entries in
    /// `[stream.developer]` and `[batch.developer]` sections. This value will be used if they
    /// are not specified.
    #[serde(default = "default::server::connection_pool_size")]
    // Intentionally made private to avoid abuse. Check the related methods on `RwConfig`.
    connection_pool_size: u16,

    /// Used for control the metrics level, similar to log level.
    #[serde(default = "default::server::metrics_level")]
    pub metrics_level: MetricLevel,

    #[serde(default = "default::server::telemetry_enabled")]
    pub telemetry_enabled: bool,

    /// Enable heap profile dump when memory usage is high.
    #[serde(default)]
    pub heap_profiling: HeapProfilingConfig,

    // Number of max pending reset stream for grpc server.
    #[serde(default = "default::server::grpc_max_reset_stream_size")]
    pub grpc_max_reset_stream: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// The section `[batch]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchConfig {
    /// The thread number of the batch task runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub worker_threads_num: Option<usize>,

    #[serde(default, with = "batch_prefix")]
    #[config_doc(omitted)]
    pub developer: BatchDeveloperConfig,

    /// This is the max number of queries per sql session.
    #[serde(default)]
    pub distributed_query_limit: Option<u64>,

    /// This is the max number of batch queries per frontend node.
    #[serde(default)]
    pub max_batch_queries_per_frontend_node: Option<u64>,

    #[serde(default = "default::batch::enable_barrier_read")]
    pub enable_barrier_read: bool,

    /// Timeout for a batch query in seconds.
    #[serde(default = "default::batch::statement_timeout_in_sec")]
    pub statement_timeout_in_sec: u32,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    #[serde(default = "default::batch::frontend_compute_runtime_worker_threads")]
    /// frontend compute runtime worker threads
    pub frontend_compute_runtime_worker_threads: usize,

    /// This is the secs used to mask a worker unavailable temporarily.
    #[serde(default = "default::batch::mask_worker_temporary_secs")]
    pub mask_worker_temporary_secs: usize,

    /// Keywords on which SQL option redaction is based in the query log.
    /// A SQL option with a name containing any of these keywords will be redacted.
    #[serde(default = "default::batch::redact_sql_option_keywords")]
    pub redact_sql_option_keywords: Vec<String>,

    /// Enable the spill out to disk feature for batch queries.
    #[serde(default = "default::batch::enable_spill")]
    pub enable_spill: bool,
}

/// The section `[streaming]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingConfig {
    /// The maximum number of barriers in-flight in the compute nodes.
    #[serde(default = "default::streaming::in_flight_barrier_nums")]
    pub in_flight_barrier_nums: usize,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    #[serde(default)]
    pub actor_runtime_worker_threads_num: Option<usize>,

    /// Enable async stack tracing through `await-tree` for risectl.
    #[serde(default = "default::streaming::async_stack_trace")]
    pub async_stack_trace: AsyncStackTraceOption,

    #[serde(default, with = "streaming_prefix")]
    #[config_doc(omitted)]
    pub developer: StreamingDeveloperConfig,

    /// Max unique user stream errors per actor
    #[serde(default = "default::streaming::unique_user_stream_errors")]
    pub unique_user_stream_errors: usize,

    /// Control the strictness of stream consistency.
    #[serde(default = "default::streaming::unsafe_enable_strict_consistency")]
    pub unsafe_enable_strict_consistency: bool,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

pub use risingwave_common_metrics::MetricLevel;

/// the section `[storage.cache]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheConfig {
    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `meta_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the block cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub block_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub block_cache_eviction: CacheEvictionConfig,

    /// Configure the capacity of the block cache in MB explicitly.
    /// The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `shared_buffer_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// Configure the number of shards in the meta cache explicitly.
    /// If not set, the shard number will be determined automatically based on cache capacity.
    #[serde(default)]
    pub meta_cache_shard_num: Option<usize>,

    #[serde(default)]
    #[config_doc(omitted)]
    pub meta_cache_eviction: CacheEvictionConfig,
}

/// the section `[storage.cache.eviction]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "algorithm")]
pub enum CacheEvictionConfig {
    Lru {
        high_priority_ratio_in_percent: Option<usize>,
    },
    Lfu {
        window_capacity_ratio_in_percent: Option<usize>,
        protected_capacity_ratio_in_percent: Option<usize>,
        cmsketch_eps: Option<f64>,
        cmsketch_confidence: Option<f64>,
    },
    S3Fifo {
        small_queue_capacity_ratio_in_percent: Option<usize>,
        ghost_queue_capacity_ratio_in_percent: Option<usize>,
        small_to_main_freq_threshold: Option<u8>,
    },
}

impl Default for CacheEvictionConfig {
    fn default() -> Self {
        Self::Lru {
            high_priority_ratio_in_percent: None,
        }
    }
}

/// The section `[storage]` in `risingwave.toml`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StorageConfig {
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    #[serde(default = "default::storage::share_buffers_sync_parallelism")]
    pub share_buffers_sync_parallelism: u32,

    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    #[serde(default = "default::storage::share_buffer_compaction_worker_threads_number")]
    pub share_buffer_compaction_worker_threads_number: u32,

    /// Configure the maximum shared buffer size in MB explicitly. Writes attempting to exceed the capacity
    /// will stall until there is enough space. The overridden value will only be effective if:
    /// 1. `block_cache_capacity_mb` and `meta_cache_capacity_mb` are also configured explicitly.
    /// 2. `block_cache_capacity_mb` + `meta_cache_capacity_mb` + `meta_cache_capacity_mb` doesn't exceed 0.3 * non-reserved memory.
    #[serde(default)]
    pub shared_buffer_capacity_mb: Option<usize>,

    /// The shared buffer will start flushing data to object when the ratio of memory usage to the
    /// shared buffer capacity exceed such ratio.
    #[serde(default = "default::storage::shared_buffer_flush_ratio")]
    pub shared_buffer_flush_ratio: f32,

    /// The minimum total flush size of shared buffer spill. When a shared buffer spilled is trigger,
    /// the total flush size across multiple epochs should be at least higher than this size.
    #[serde(default = "default::storage::shared_buffer_min_batch_flush_size_mb")]
    pub shared_buffer_min_batch_flush_size_mb: usize,

    /// The threshold for the number of immutable memtables to merge to a new imm.
    #[serde(default = "default::storage::imm_merge_threshold")]
    #[deprecated]
    pub imm_merge_threshold: usize,

    /// Whether to enable write conflict detection
    #[serde(default = "default::storage::write_conflict_detection_enabled")]
    pub write_conflict_detection_enabled: bool,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache: CacheConfig,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_capacity_mb` instead.
    #[serde(default)]
    pub block_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.meta_cache_capacity_mb` instead.
    #[serde(default)]
    pub meta_cache_capacity_mb: Option<usize>,

    /// DEPRECATED: This config will be deprecated in the future version, use `storage.cache.block_cache_eviction.high_priority_ratio_in_percent` with `storage.cache.block_cache_eviction.algorithm = "Lru"` instead.
    #[serde(default)]
    pub high_priority_ratio_in_percent: Option<usize>,

    /// max memory usage for large query
    #[serde(default)]
    pub prefetch_buffer_capacity_mb: Option<usize>,

    #[serde(default = "default::storage::max_cached_recent_versions_number")]
    pub max_cached_recent_versions_number: usize,

    /// max prefetch block number
    #[serde(default = "default::storage::max_prefetch_block_number")]
    pub max_prefetch_block_number: usize,

    #[serde(default = "default::storage::disable_remote_compactor")]
    pub disable_remote_compactor: bool,

    /// Number of tasks shared buffer can upload in parallel.
    #[serde(default = "default::storage::share_buffer_upload_concurrency")]
    pub share_buffer_upload_concurrency: usize,

    #[serde(default)]
    pub compactor_memory_limit_mb: Option<usize>,

    /// Compactor calculates the maximum number of tasks that can be executed on the node based on
    /// `worker_num` and `compactor_max_task_multiplier`.
    /// `max_pull_task_count` = `worker_num` * `compactor_max_task_multiplier`
    #[serde(default = "default::storage::compactor_max_task_multiplier")]
    pub compactor_max_task_multiplier: f32,

    /// The percentage of memory available when compactor is deployed separately.
    /// `non_reserved_memory_bytes` = `system_memory_available_bytes` * `compactor_memory_available_proportion`
    #[serde(default = "default::storage::compactor_memory_available_proportion")]
    pub compactor_memory_available_proportion: f64,

    /// Number of SST ids fetched from meta per RPC
    #[serde(default = "default::storage::sstable_id_remote_fetch_number")]
    pub sstable_id_remote_fetch_number: u32,

    #[serde(default = "default::storage::min_sstable_size_mb")]
    pub min_sstable_size_mb: u32,

    #[serde(default)]
    #[config_doc(nested)]
    pub data_file_cache: FileCacheConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub meta_file_cache: FileCacheConfig,

    #[serde(default)]
    #[config_doc(nested)]
    pub cache_refill: CacheRefillConfig,

    /// Whether to enable streaming upload for sstable.
    #[serde(default = "default::storage::min_sst_size_for_streaming_upload")]
    pub min_sst_size_for_streaming_upload: u64,

    #[serde(default = "default::storage::max_concurrent_compaction_task_number")]
    pub max_concurrent_compaction_task_number: u64,

    #[serde(default = "default::storage::max_preload_wait_time_mill")]
    pub max_preload_wait_time_mill: u64,

    #[serde(default = "default::storage::max_version_pinning_duration_sec")]
    pub max_version_pinning_duration_sec: u64,

    #[serde(default = "default::storage::compactor_max_sst_key_count")]
    pub compactor_max_sst_key_count: u64,
    // DEPRECATED: This config will be deprecated in the future version, use `storage.compactor_iter_max_io_retry_times` instead.
    #[serde(default = "default::storage::compact_iter_recreate_timeout_ms")]
    pub compact_iter_recreate_timeout_ms: u64,
    #[serde(default = "default::storage::compactor_max_sst_size")]
    pub compactor_max_sst_size: u64,
    #[serde(default = "default::storage::enable_fast_compaction")]
    pub enable_fast_compaction: bool,
    #[serde(default = "default::storage::check_compaction_result")]
    pub check_compaction_result: bool,
    #[serde(default = "default::storage::max_preload_io_retry_times")]
    pub max_preload_io_retry_times: usize,
    #[serde(default = "default::storage::compactor_fast_max_compact_delete_ratio")]
    pub compactor_fast_max_compact_delete_ratio: u32,
    #[serde(default = "default::storage::compactor_fast_max_compact_task_size")]
    pub compactor_fast_max_compact_task_size: u64,
    #[serde(default = "default::storage::compactor_iter_max_io_retry_times")]
    pub compactor_iter_max_io_retry_times: usize,

    /// Deprecated: The window size of table info statistic history.
    #[serde(default = "default::storage::table_info_statistic_history_times")]
    #[deprecated]
    pub table_info_statistic_history_times: usize,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,

    /// The spill threshold for mem table.
    #[serde(default = "default::storage::mem_table_spill_threshold")]
    pub mem_table_spill_threshold: usize,

    /// The concurrent uploading number of `SSTables` of builder
    #[serde(default = "default::storage::compactor_concurrent_uploading_sst_count")]
    pub compactor_concurrent_uploading_sst_count: Option<usize>,

    #[serde(default = "default::storage::compactor_max_overlap_sst_count")]
    pub compactor_max_overlap_sst_count: usize,

    /// The maximum number of meta files that can be preloaded.
    /// If the number of meta files exceeds this value, the compactor will try to compute parallelism only through `SstableInfo`, no longer preloading `SstableMeta`.
    /// This is to prevent the compactor from consuming too much memory, but it may cause the compactor to be less efficient.
    #[serde(default = "default::storage::compactor_max_preload_meta_file_count")]
    pub compactor_max_preload_meta_file_count: usize,

    /// Object storage configuration
    /// 1. General configuration
    /// 2. Some special configuration of Backend
    /// 3. Retry and timeout configuration
    #[serde(default)]
    pub object_store: ObjectStoreConfig,

    #[serde(default = "default::storage::time_travel_version_cache_capacity")]
    pub time_travel_version_cache_capacity: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CacheRefillConfig {
    /// `SSTable` levels to refill.
    #[serde(default = "default::cache_refill::data_refill_levels")]
    pub data_refill_levels: Vec<u32>,

    /// Cache refill maximum timeout to apply version delta.
    #[serde(default = "default::cache_refill::timeout_ms")]
    pub timeout_ms: u64,

    /// Inflight data cache refill tasks.
    #[serde(default = "default::cache_refill::concurrency")]
    pub concurrency: usize,

    /// Block count that a data cache refill request fetches.
    #[serde(default = "default::cache_refill::unit")]
    pub unit: usize,

    /// Data cache refill unit admission ratio.
    ///
    /// Only unit whose blocks are admitted above the ratio will be refilled.
    #[serde(default = "default::cache_refill::threshold")]
    pub threshold: f64,

    /// Recent filter layer count.
    #[serde(default = "default::cache_refill::recent_filter_layers")]
    pub recent_filter_layers: usize,

    /// Recent filter layer rotate interval.
    #[serde(default = "default::cache_refill::recent_filter_rotate_interval_ms")]
    pub recent_filter_rotate_interval_ms: usize,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

/// The subsection `[storage.data_file_cache]` and `[storage.meta_file_cache]` in `risingwave.toml`.
///
/// It's put at [`StorageConfig::data_file_cache`] and  [`StorageConfig::meta_file_cache`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct FileCacheConfig {
    #[serde(default = "default::file_cache::dir")]
    pub dir: String,

    #[serde(default = "default::file_cache::capacity_mb")]
    pub capacity_mb: usize,

    #[serde(default = "default::file_cache::file_capacity_mb")]
    pub file_capacity_mb: usize,

    #[serde(default = "default::file_cache::flushers")]
    pub flushers: usize,

    #[serde(default = "default::file_cache::reclaimers")]
    pub reclaimers: usize,

    #[serde(default = "default::file_cache::recover_concurrency")]
    pub recover_concurrency: usize,

    #[serde(default = "default::file_cache::insert_rate_limit_mb")]
    pub insert_rate_limit_mb: usize,

    #[serde(default = "default::file_cache::indexer_shards")]
    pub indexer_shards: usize,

    #[serde(default = "default::file_cache::compression")]
    pub compression: Compression,

    #[serde(default = "default::file_cache::flush_buffer_threshold_mb")]
    pub flush_buffer_threshold_mb: Option<usize>,

    /// Recover mode.
    ///
    /// Options:
    ///
    /// - "None": Do not recover disk cache.
    /// - "Quiet": Recover disk cache and skip errors.
    /// - "Strict": Recover disk cache and panic on errors.
    ///
    /// More details, see [`RecoverMode::None`], [`RecoverMode::Quiet`] and [`RecoverMode::Strict`],
    #[serde(default = "default::file_cache::recover_mode")]
    pub recover_mode: RecoverMode,

    #[serde(default = "default::file_cache::runtime_config")]
    pub runtime_config: RuntimeOptions,

    #[serde(default, flatten)]
    #[config_doc(omitted)]
    pub unrecognized: Unrecognized<Self>,
}

#[derive(Debug, Default, Clone, Copy, ValueEnum, Serialize, Deserialize)]
pub enum AsyncStackTraceOption {
    /// Disabled.
    Off,
    /// Enabled with basic instruments.
    On,
    /// Enabled with extra verbose instruments in release build.
    /// Behaves the same as `on` in debug build due to performance concern.
    #[default]
    #[clap(alias = "verbose")]
    ReleaseVerbose,
}

impl AsyncStackTraceOption {
    pub fn is_verbose(self) -> Option<bool> {
        match self {
            Self::Off => None,
            Self::On => Some(false),
            Self::ReleaseVerbose => Some(!cfg!(debug_assertions)),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, ValueEnum)]
pub enum CompactorMode {
    #[default]
    #[clap(alias = "dedicated")]
    Dedicated,

    #[clap(alias = "shared")]
    Shared,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct HeapProfilingConfig {
    /// Enable to auto dump heap profile when memory usage is high
    #[serde(default = "default::heap_profiling::enable_auto")]
    pub enable_auto: bool,

    /// The proportion (number between 0 and 1) of memory usage to trigger heap profile dump
    #[serde(default = "default::heap_profiling::threshold_auto")]
    pub threshold_auto: f32,

    /// The directory to dump heap profile. If empty, the prefix in `MALLOC_CONF` will be used
    #[serde(default = "default::heap_profiling::dir")]
    pub dir: String,
}

/// The subsections `[streaming.developer]`.
///
/// It is put at [`StreamingConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct StreamingDeveloperConfig {
    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    #[serde(default = "default::developer::stream_enable_executor_row_count")]
    pub enable_executor_row_count: bool,

    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// Limit number of the cached entries in an extreme aggregation call.
    #[serde(default = "default::developer::unsafe_stream_extreme_cache_size")]
    pub unsafe_extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    #[serde(default = "default::developer::stream_chunk_size")]
    pub chunk_size: usize,

    /// The initial permits that a channel holds, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_exchange_initial_permits")]
    pub exchange_initial_permits: usize,

    /// The permits that are batched to add back, for reducing the backward `AddPermits` messages
    /// in remote exchange.
    #[serde(default = "default::developer::stream_exchange_batched_permits")]
    pub exchange_batched_permits: usize,

    /// The maximum number of concurrent barriers in an exchange channel.
    #[serde(default = "default::developer::stream_exchange_concurrent_barriers")]
    pub exchange_concurrent_barriers: usize,

    /// The concurrency for dispatching messages to different downstream jobs.
    ///
    /// - `1` means no concurrency, i.e., dispatch messages to downstream jobs one by one.
    /// - `0` means unlimited concurrency.
    #[serde(default = "default::developer::stream_exchange_concurrent_dispatchers")]
    pub exchange_concurrent_dispatchers: usize,

    /// The initial permits for a dml channel, i.e., the maximum row count can be buffered in
    /// the channel.
    #[serde(default = "default::developer::stream_dml_channel_initial_permits")]
    pub dml_channel_initial_permits: usize,

    /// The max heap size of dirty groups of `HashAggExecutor`.
    #[serde(default = "default::developer::stream_hash_agg_max_dirty_groups_heap_size")]
    pub hash_agg_max_dirty_groups_heap_size: usize,

    #[serde(default = "default::developer::memory_controller_threshold_aggressive")]
    pub memory_controller_threshold_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_threshold_graceful")]
    pub memory_controller_threshold_graceful: f64,

    #[serde(default = "default::developer::memory_controller_threshold_stable")]
    pub memory_controller_threshold_stable: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_aggressive")]
    pub memory_controller_eviction_factor_aggressive: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_graceful")]
    pub memory_controller_eviction_factor_graceful: f64,

    #[serde(default = "default::developer::memory_controller_eviction_factor_stable")]
    pub memory_controller_eviction_factor_stable: f64,

    #[serde(default = "default::developer::memory_controller_update_interval_ms")]
    pub memory_controller_update_interval_ms: usize,

    #[serde(default = "default::developer::memory_controller_sequence_tls_step")]
    pub memory_controller_sequence_tls_step: u64,

    #[serde(default = "default::developer::memory_controller_sequence_tls_lag")]
    pub memory_controller_sequence_tls_lag: u64,

    #[serde(default = "default::developer::stream_enable_arrangement_backfill")]
    /// Enable arrangement backfill
    /// If false, the arrangement backfill will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_arrangement_backfill` (default true)
    pub enable_arrangement_backfill: bool,

    #[serde(default = "default::developer::stream_high_join_amplification_threshold")]
    /// If number of hash join matches exceeds this threshold number,
    /// it will be logged.
    pub high_join_amplification_threshold: usize,

    /// Actor tokio metrics is enabled if `enable_actor_tokio_metrics` is set or metrics level >= Debug.
    #[serde(default = "default::developer::enable_actor_tokio_metrics")]
    pub enable_actor_tokio_metrics: bool,

    /// The number of the connections for streaming remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::stream_exchange_connection_pool_size")]
    pub exchange_connection_pool_size: Option<u16>,

    /// A flag to allow disabling the auto schema change handling
    #[serde(default = "default::developer::stream_enable_auto_schema_change")]
    pub enable_auto_schema_change: bool,

    #[serde(default = "default::developer::enable_shared_source")]
    /// Enable shared source
    /// If false, the shared source will be disabled,
    /// even if session variable set.
    /// If true, it's decided by session variable `streaming_use_shared_source` (default true)
    pub enable_shared_source: bool,

    #[serde(default = "default::developer::switch_jdbc_pg_to_native")]
    /// When true, all jdbc sinks with connector='jdbc' and jdbc.url="jdbc:postgresql://..."
    /// will be switched from jdbc postgresql sinks to rust native (connector='postgres') sinks.
    pub switch_jdbc_pg_to_native: bool,

    /// The maximum number of consecutive barriers allowed in a message when sent between actors.
    #[serde(default = "default::developer::stream_max_barrier_batch_size")]
    pub max_barrier_batch_size: u32,

    /// Configure the system-wide cache row cardinality of hash join.
    /// For example, if this is set to 1000, it means we can have at most 1000 rows in cache.
    #[serde(default = "default::developer::streaming_hash_join_entry_state_max_rows")]
    pub hash_join_entry_state_max_rows: usize,
}

/// The subsections `[batch.developer]`.
///
/// It is put at [`BatchConfig::developer`].
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct BatchDeveloperConfig {
    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    #[serde(default = "default::developer::connector_message_buffer_size")]
    pub connector_message_buffer_size: usize,

    /// The size of the channel used for output to exchange/shuffle.
    #[serde(default = "default::developer::batch_output_channel_size")]
    pub output_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    #[serde(default = "default::developer::batch_chunk_size")]
    pub chunk_size: usize,

    /// The number of the connections for batch remote exchange between two nodes.
    /// If not specified, the value of `server.connection_pool_size` will be used.
    #[serde(default = "default::developer::batch_exchange_connection_pool_size")]
    exchange_connection_pool_size: Option<u16>,
}

macro_rules! define_system_config {
    ($({ $field:ident, $type:ty, $default:expr, $is_mutable:expr, $doc:literal, $($rest:tt)* },)*) => {
        paste::paste!(
            /// The section `[system]` in `risingwave.toml`. All these fields are used to initialize the system
            /// parameters persisted in Meta store. Most fields are for testing purpose only and should not be
            /// documented.
            #[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
            pub struct SystemConfig {
                $(
                    #[doc = $doc]
                    #[serde(default = "default::system::" $field "_opt")]
                    pub $field: Option<$type>,
                )*
            }
        );
    };
}

for_all_params!(define_system_config);

/// The subsections `[storage.object_store]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::set_atomic_write_dir",
        alias = "object_store_set_atomic_write_dir"
    )]
    pub set_atomic_write_dir: bool,

    /// Retry and timeout configuration
    /// Description retry strategy driven by exponential back-off
    /// Exposes the timeout and retries of each Object store interface. Therefore, the total timeout for each interface is determined based on the interface's timeout/retry configuration and the exponential back-off policy.
    #[serde(default)]
    pub retry: ObjectStoreRetryConfig,

    /// Some special configuration of S3 Backend
    #[serde(default)]
    pub s3: S3ObjectStoreConfig,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default = "default::object_store_config::opendal_upload_concurrency")]
    pub opendal_upload_concurrency: usize,

    // TODO: the following field will be deprecated after opendal is stabilized
    #[serde(default)]
    pub opendal_writer_abort_on_err: bool,

    #[serde(default = "default::object_store_config::upload_part_size")]
    pub upload_part_size: usize,
}

impl ObjectStoreConfig {
    pub fn set_atomic_write_dir(&mut self) {
        self.set_atomic_write_dir = true;
    }
}

/// The subsections `[storage.object_store.s3]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreConfig {
    // alias is for backward compatibility
    #[serde(
        default = "default::object_store_config::s3::keepalive_ms",
        alias = "object_store_keepalive_ms"
    )]
    pub keepalive_ms: Option<u64>,
    #[serde(
        default = "default::object_store_config::s3::recv_buffer_size",
        alias = "object_store_recv_buffer_size"
    )]
    pub recv_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::send_buffer_size",
        alias = "object_store_send_buffer_size"
    )]
    pub send_buffer_size: Option<usize>,
    #[serde(
        default = "default::object_store_config::s3::nodelay",
        alias = "object_store_nodelay"
    )]
    pub nodelay: Option<bool>,
    /// For backwards compatibility, users should use `S3ObjectStoreDeveloperConfig` instead.
    #[serde(default = "default::object_store_config::s3::developer::retry_unknown_service_error")]
    pub retry_unknown_service_error: bool,
    #[serde(default = "default::object_store_config::s3::identity_resolution_timeout_s")]
    pub identity_resolution_timeout_s: u64,
    #[serde(default)]
    pub developer: S3ObjectStoreDeveloperConfig,
}

/// The subsections `[storage.object_store.s3.developer]`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct S3ObjectStoreDeveloperConfig {
    /// Whether to retry s3 sdk error from which no error metadata is provided.
    #[serde(
        default = "default::object_store_config::s3::developer::retry_unknown_service_error",
        alias = "object_store_retry_unknown_service_error"
    )]
    pub retry_unknown_service_error: bool,
    /// An array of error codes that should be retried.
    /// e.g. `["SlowDown", "TooManyRequests"]`
    #[serde(
        default = "default::object_store_config::s3::developer::retryable_service_error_codes",
        alias = "object_store_retryable_service_error_codes"
    )]
    pub retryable_service_error_codes: Vec<String>,

    // TODO: deprecate this config when we are completely deprecate aws sdk.
    #[serde(default = "default::object_store_config::s3::developer::use_opendal")]
    pub use_opendal: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde)]
pub struct ObjectStoreRetryConfig {
    // A retry strategy driven by exponential back-off.
    // The retry strategy is used for all object store operations.
    /// Given a base duration for retry strategy in milliseconds.
    #[serde(default = "default::object_store_config::object_store_req_backoff_interval_ms")]
    pub req_backoff_interval_ms: u64,

    /// The max delay interval for the retry strategy. No retry delay will be longer than this `Duration`.
    #[serde(default = "default::object_store_config::object_store_req_backoff_max_delay_ms")]
    pub req_backoff_max_delay_ms: u64,

    /// A multiplicative factor that will be applied to the exponential back-off retry delay.
    #[serde(default = "default::object_store_config::object_store_req_backoff_factor")]
    pub req_backoff_factor: u64,

    /// Maximum timeout for `upload` operation
    #[serde(default = "default::object_store_config::object_store_upload_attempt_timeout_ms")]
    pub upload_attempt_timeout_ms: u64,

    /// Total counts of `upload` operation retries
    #[serde(default = "default::object_store_config::object_store_upload_retry_attempts")]
    pub upload_retry_attempts: usize,

    /// Maximum timeout for `streaming_upload_init` and `streaming_upload`
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_attempt_timeout_ms"
    )]
    pub streaming_upload_attempt_timeout_ms: u64,

    /// Total counts of `streaming_upload` operation retries
    #[serde(
        default = "default::object_store_config::object_store_streaming_upload_retry_attempts"
    )]
    pub streaming_upload_retry_attempts: usize,

    /// Maximum timeout for `read` operation
    #[serde(default = "default::object_store_config::object_store_read_attempt_timeout_ms")]
    pub read_attempt_timeout_ms: u64,

    /// Total counts of `read` operation retries
    #[serde(default = "default::object_store_config::object_store_read_retry_attempts")]
    pub read_retry_attempts: usize,

    /// Maximum timeout for `streaming_read_init` and `streaming_read` operation
    #[serde(
        default = "default::object_store_config::object_store_streaming_read_attempt_timeout_ms"
    )]
    pub streaming_read_attempt_timeout_ms: u64,

    /// Total counts of `streaming_read operation` retries
    #[serde(default = "default::object_store_config::object_store_streaming_read_retry_attempts")]
    pub streaming_read_retry_attempts: usize,

    /// Maximum timeout for `metadata` operation
    #[serde(default = "default::object_store_config::object_store_metadata_attempt_timeout_ms")]
    pub metadata_attempt_timeout_ms: u64,

    /// Total counts of `metadata` operation retries
    #[serde(default = "default::object_store_config::object_store_metadata_retry_attempts")]
    pub metadata_retry_attempts: usize,

    /// Maximum timeout for `delete` operation
    #[serde(default = "default::object_store_config::object_store_delete_attempt_timeout_ms")]
    pub delete_attempt_timeout_ms: u64,

    /// Total counts of `delete` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_retry_attempts")]
    pub delete_retry_attempts: usize,

    /// Maximum timeout for `delete_object` operation
    #[serde(
        default = "default::object_store_config::object_store_delete_objects_attempt_timeout_ms"
    )]
    pub delete_objects_attempt_timeout_ms: u64,

    /// Total counts of `delete_object` operation retries
    #[serde(default = "default::object_store_config::object_store_delete_objects_retry_attempts")]
    pub delete_objects_retry_attempts: usize,

    /// Maximum timeout for `list` operation
    #[serde(default = "default::object_store_config::object_store_list_attempt_timeout_ms")]
    pub list_attempt_timeout_ms: u64,

    /// Total counts of `list` operation retries
    #[serde(default = "default::object_store_config::object_store_list_retry_attempts")]
    pub list_retry_attempts: usize,
}

impl SystemConfig {
    #![allow(deprecated)]
    pub fn into_init_system_params(self) -> SystemParams {
        macro_rules! fields {
            ($({ $field:ident, $($rest:tt)* },)*) => {
                SystemParams {
                    $($field: self.$field.map(Into::into),)*
                    ..Default::default() // deprecated fields
                }
            };
        }

        let mut system_params = for_all_params!(fields);

        // Initialize backup_storage_url and backup_storage_directory if not set.
        if let Some(state_store) = &system_params.state_store
            && let Some(data_directory) = &system_params.data_directory
        {
            if system_params.backup_storage_url.is_none() {
                if let Some(hummock_state_store) = state_store.strip_prefix("hummock+") {
                    system_params.backup_storage_url = Some(hummock_state_store.to_owned());
                } else {
                    system_params.backup_storage_url = Some("memory".to_owned());
                }
                tracing::info!("initialize backup_storage_url based on state_store");
            }
            if system_params.backup_storage_directory.is_none() {
                system_params.backup_storage_directory = Some(format!("{data_directory}/backup"));
                tracing::info!("initialize backup_storage_directory based on data_directory");
            }
        }
        system_params
    }
}

impl RwConfig {
    pub const fn default_connection_pool_size(&self) -> u16 {
        self.server.connection_pool_size
    }

    /// Returns [`StreamingDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn streaming_exchange_connection_pool_size(&self) -> u16 {
        self.streaming
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }

    /// Returns [`BatchDeveloperConfig::exchange_connection_pool_size`] if set,
    /// otherwise [`ServerConfig::connection_pool_size`].
    pub fn batch_exchange_connection_pool_size(&self) -> u16 {
        self.batch
            .developer
            .exchange_connection_pool_size
            .unwrap_or_else(|| self.default_connection_pool_size())
    }
}

pub mod default {
    pub mod meta {
        use crate::config::{DefaultParallelism, MetaBackend};

        pub fn min_sst_retention_time_sec() -> u64 {
            3600 * 6
        }

        pub fn gc_history_retention_time_sec() -> u64 {
            3600 * 6
        }

        pub fn full_gc_interval_sec() -> u64 {
            3600
        }

        pub fn full_gc_object_limit() -> u64 {
            100_000
        }

        pub fn max_inflight_time_travel_query() -> u64 {
            1000
        }

        pub fn periodic_compaction_interval_sec() -> u64 {
            60
        }

        pub fn vacuum_interval_sec() -> u64 {
            30
        }

        pub fn vacuum_spin_interval_ms() -> u64 {
            100
        }

        pub fn hummock_version_checkpoint_interval_sec() -> u64 {
            30
        }

        pub fn enable_hummock_data_archive() -> bool {
            false
        }

        pub fn hummock_time_travel_snapshot_interval() -> u64 {
            100
        }

        pub fn min_delta_log_num_for_hummock_version_checkpoint() -> u64 {
            10
        }

        pub fn max_heartbeat_interval_sec() -> u32 {
            60
        }

        pub fn meta_leader_lease_secs() -> u64 {
            30
        }

        pub fn default_parallelism() -> DefaultParallelism {
            DefaultParallelism::Full
        }

        pub fn node_num_monitor_interval_sec() -> u64 {
            10
        }

        pub fn backend() -> MetaBackend {
            MetaBackend::Mem
        }

        pub fn periodic_space_reclaim_compaction_interval_sec() -> u64 {
            3600 // 60min
        }

        pub fn periodic_ttl_reclaim_compaction_interval_sec() -> u64 {
            1800 // 30mi
        }

        pub fn periodic_scheduling_compaction_group_split_interval_sec() -> u64 {
            10 // 10s
        }

        pub fn periodic_tombstone_reclaim_compaction_interval_sec() -> u64 {
            600
        }

        // limit the size of state table to trigger split by high throughput
        pub fn move_table_size_limit() -> u64 {
            10 * 1024 * 1024 * 1024 // 10GB
        }

        // limit the size of group to trigger split by group_size and avoid too many small groups
        pub fn split_group_size_limit() -> u64 {
            64 * 1024 * 1024 * 1024 // 64GB
        }

        pub fn partition_vnode_count() -> u32 {
            16
        }

        pub fn table_high_write_throughput_threshold() -> u64 {
            16 * 1024 * 1024 // 16MB
        }

        pub fn table_low_write_throughput_threshold() -> u64 {
            4 * 1024 * 1024 // 4MB
        }

        pub fn compaction_task_max_heartbeat_interval_secs() -> u64 {
            30 // 30s
        }

        pub fn compaction_task_max_progress_interval_secs() -> u64 {
            60 * 10 // 10min
        }

        pub fn cut_table_size_limit() -> u64 {
            1024 * 1024 * 1024 // 1GB
        }

        pub fn hybrid_partition_vnode_count() -> u32 {
            4
        }

        pub fn compact_task_table_size_partition_threshold_low() -> u64 {
            128 * 1024 * 1024 // 128MB
        }

        pub fn compact_task_table_size_partition_threshold_high() -> u64 {
            512 * 1024 * 1024 // 512MB
        }

        pub fn event_log_enabled() -> bool {
            true
        }

        pub fn event_log_channel_max_size() -> u32 {
            10
        }

        pub fn parallelism_control_batch_size() -> usize {
            10
        }

        pub fn parallelism_control_trigger_period_sec() -> u64 {
            10
        }

        pub fn parallelism_control_trigger_first_delay_sec() -> u64 {
            30
        }

        pub fn enable_dropped_column_reclaim() -> bool {
            false
        }

        pub fn split_group_size_ratio() -> f64 {
            0.9
        }

        pub fn table_stat_high_write_throughput_ratio_for_split() -> f64 {
            0.5
        }

        pub fn table_stat_low_write_throughput_ratio_for_merge() -> f64 {
            0.7
        }

        pub fn table_stat_throuput_window_seconds_for_split() -> usize {
            60
        }

        pub fn table_stat_throuput_window_seconds_for_merge() -> usize {
            240
        }

        pub fn periodic_scheduling_compaction_group_merge_interval_sec() -> u64 {
            60 * 10 // 10min
        }
    }

    pub mod server {
        use crate::config::MetricLevel;

        pub fn heartbeat_interval_ms() -> u32 {
            1000
        }

        pub fn connection_pool_size() -> u16 {
            16
        }

        pub fn metrics_level() -> MetricLevel {
            MetricLevel::Info
        }

        pub fn telemetry_enabled() -> bool {
            true
        }

        pub fn grpc_max_reset_stream_size() -> u32 {
            200
        }
    }

    pub mod storage {
        pub fn share_buffers_sync_parallelism() -> u32 {
            1
        }

        pub fn share_buffer_compaction_worker_threads_number() -> u32 {
            4
        }

        pub fn shared_buffer_capacity_mb() -> usize {
            1024
        }

        pub fn shared_buffer_flush_ratio() -> f32 {
            0.8
        }

        pub fn shared_buffer_min_batch_flush_size_mb() -> usize {
            800
        }

        pub fn imm_merge_threshold() -> usize {
            0 // disable
        }

        pub fn write_conflict_detection_enabled() -> bool {
            cfg!(debug_assertions)
        }

        pub fn max_cached_recent_versions_number() -> usize {
            60
        }

        pub fn block_cache_capacity_mb() -> usize {
            512
        }

        pub fn high_priority_ratio_in_percent() -> usize {
            70
        }

        pub fn window_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn protected_capacity_ratio_in_percent() -> usize {
            80
        }

        pub fn cmsketch_eps() -> f64 {
            0.002
        }

        pub fn cmsketch_confidence() -> f64 {
            0.95
        }

        pub fn small_queue_capacity_ratio_in_percent() -> usize {
            10
        }

        pub fn ghost_queue_capacity_ratio_in_percent() -> usize {
            1000
        }

        pub fn small_to_main_freq_threshold() -> u8 {
            1
        }

        pub fn meta_cache_capacity_mb() -> usize {
            128
        }

        pub fn disable_remote_compactor() -> bool {
            false
        }

        pub fn share_buffer_upload_concurrency() -> usize {
            8
        }

        pub fn compactor_memory_limit_mb() -> usize {
            512
        }

        pub fn compactor_max_task_multiplier() -> f32 {
            3.0000
        }

        pub fn compactor_memory_available_proportion() -> f64 {
            0.8
        }

        pub fn sstable_id_remote_fetch_number() -> u32 {
            10
        }

        pub fn min_sstable_size_mb() -> u32 {
            32
        }

        pub fn min_sst_size_for_streaming_upload() -> u64 {
            // 32MB
            32 * 1024 * 1024
        }

        pub fn max_concurrent_compaction_task_number() -> u64 {
            16
        }

        pub fn max_preload_wait_time_mill() -> u64 {
            0
        }

        pub fn max_version_pinning_duration_sec() -> u64 {
            3 * 3600
        }

        pub fn compactor_max_sst_key_count() -> u64 {
            2 * 1024 * 1024 // 200w
        }

        pub fn compact_iter_recreate_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn compactor_iter_max_io_retry_times() -> usize {
            8
        }

        pub fn compactor_max_sst_size() -> u64 {
            512 * 1024 * 1024 // 512m
        }

        pub fn enable_fast_compaction() -> bool {
            true
        }

        pub fn check_compaction_result() -> bool {
            false
        }

        pub fn max_preload_io_retry_times() -> usize {
            3
        }

        pub fn mem_table_spill_threshold() -> usize {
            4 << 20
        }

        pub fn compactor_fast_max_compact_delete_ratio() -> u32 {
            40
        }

        pub fn compactor_fast_max_compact_task_size() -> u64 {
            2 * 1024 * 1024 * 1024 // 2g
        }

        pub fn max_prefetch_block_number() -> usize {
            16
        }

        pub fn compactor_concurrent_uploading_sst_count() -> Option<usize> {
            None
        }

        pub fn compactor_max_overlap_sst_count() -> usize {
            64
        }

        pub fn compactor_max_preload_meta_file_count() -> usize {
            32
        }

        // deprecated
        pub fn table_info_statistic_history_times() -> usize {
            240
        }

        pub fn block_file_cache_flush_buffer_threshold_mb() -> usize {
            256
        }

        pub fn meta_file_cache_flush_buffer_threshold_mb() -> usize {
            64
        }

        pub fn time_travel_version_cache_capacity() -> u64 {
            2
        }
    }

    pub mod streaming {
        use crate::config::AsyncStackTraceOption;

        pub fn in_flight_barrier_nums() -> usize {
            // quick fix
            // TODO: remove this limitation from code
            10000
        }

        pub fn async_stack_trace() -> AsyncStackTraceOption {
            AsyncStackTraceOption::default()
        }

        pub fn unique_user_stream_errors() -> usize {
            10
        }

        pub fn unsafe_enable_strict_consistency() -> bool {
            true
        }
    }

    pub mod file_cache {
        use foyer::{Compression, RecoverMode, RuntimeOptions, TokioRuntimeOptions};

        pub fn dir() -> String {
            "".to_owned()
        }

        pub fn capacity_mb() -> usize {
            1024
        }

        pub fn file_capacity_mb() -> usize {
            64
        }

        pub fn flushers() -> usize {
            4
        }

        pub fn reclaimers() -> usize {
            4
        }

        pub fn recover_concurrency() -> usize {
            8
        }

        pub fn insert_rate_limit_mb() -> usize {
            0
        }

        pub fn indexer_shards() -> usize {
            64
        }

        pub fn compression() -> Compression {
            Compression::None
        }

        pub fn flush_buffer_threshold_mb() -> Option<usize> {
            None
        }

        pub fn recover_mode() -> RecoverMode {
            RecoverMode::None
        }

        pub fn runtime_config() -> RuntimeOptions {
            RuntimeOptions::Unified(TokioRuntimeOptions::default())
        }
    }

    pub mod cache_refill {
        pub fn data_refill_levels() -> Vec<u32> {
            vec![]
        }

        pub fn timeout_ms() -> u64 {
            6000
        }

        pub fn concurrency() -> usize {
            10
        }

        pub fn unit() -> usize {
            64
        }

        pub fn threshold() -> f64 {
            0.5
        }

        pub fn recent_filter_layers() -> usize {
            6
        }

        pub fn recent_filter_rotate_interval_ms() -> usize {
            10000
        }
    }

    pub mod heap_profiling {
        pub fn enable_auto() -> bool {
            true
        }

        pub fn threshold_auto() -> f32 {
            0.9
        }

        pub fn dir() -> String {
            "./".to_owned()
        }
    }

    pub mod developer {
        pub fn meta_cached_traces_num() -> u32 {
            256
        }

        pub fn meta_cached_traces_memory_limit_bytes() -> usize {
            1 << 27 // 128 MiB
        }

        pub fn batch_output_channel_size() -> usize {
            64
        }

        pub fn batch_chunk_size() -> usize {
            1024
        }

        /// Default to unset to be compatible with the behavior before this config is introduced,
        /// that is, follow the value of `server.connection_pool_size`.
        pub fn batch_exchange_connection_pool_size() -> Option<u16> {
            None
        }

        pub fn stream_enable_executor_row_count() -> bool {
            false
        }

        pub fn connector_message_buffer_size() -> usize {
            16
        }

        pub fn unsafe_stream_extreme_cache_size() -> usize {
            10
        }

        pub fn stream_chunk_size() -> usize {
            256
        }

        pub fn stream_exchange_initial_permits() -> usize {
            2048
        }

        pub fn stream_exchange_batched_permits() -> usize {
            256
        }

        pub fn stream_exchange_concurrent_barriers() -> usize {
            1
        }

        pub fn stream_exchange_concurrent_dispatchers() -> usize {
            0
        }

        pub fn stream_dml_channel_initial_permits() -> usize {
            32768
        }

        pub fn stream_max_barrier_batch_size() -> u32 {
            1024
        }

        pub fn stream_hash_agg_max_dirty_groups_heap_size() -> usize {
            64 << 20 // 64MB
        }

        pub fn enable_trivial_move() -> bool {
            true
        }

        pub fn enable_check_task_level_overlap() -> bool {
            false
        }

        pub fn max_trivial_move_task_count_per_loop() -> usize {
            256
        }

        pub fn max_get_task_probe_times() -> usize {
            5
        }

        pub fn actor_cnt_per_worker_parallelism_soft_limit() -> usize {
            100
        }

        pub fn actor_cnt_per_worker_parallelism_hard_limit() -> usize {
            400
        }

        pub fn hummock_time_travel_sst_info_fetch_batch_size() -> usize {
            10_000
        }

        pub fn hummock_time_travel_sst_info_insert_batch_size() -> usize {
            100
        }

        pub fn time_travel_vacuum_interval_sec() -> u64 {
            30
        }
        pub fn hummock_time_travel_epoch_version_insert_batch_size() -> usize {
            1000
        }

        pub fn hummock_gc_history_insert_batch_size() -> usize {
            1000
        }

        pub fn hummock_time_travel_filter_out_objects_batch_size() -> usize {
            1000
        }

        pub fn memory_controller_threshold_aggressive() -> f64 {
            0.9
        }

        pub fn memory_controller_threshold_graceful() -> f64 {
            0.81
        }

        pub fn memory_controller_threshold_stable() -> f64 {
            0.72
        }

        pub fn memory_controller_eviction_factor_aggressive() -> f64 {
            2.0
        }

        pub fn memory_controller_eviction_factor_graceful() -> f64 {
            1.5
        }

        pub fn memory_controller_eviction_factor_stable() -> f64 {
            1.0
        }

        pub fn memory_controller_update_interval_ms() -> usize {
            100
        }

        pub fn memory_controller_sequence_tls_step() -> u64 {
            128
        }

        pub fn memory_controller_sequence_tls_lag() -> u64 {
            32
        }

        pub fn stream_enable_arrangement_backfill() -> bool {
            true
        }

        pub fn enable_shared_source() -> bool {
            true
        }

        pub fn stream_high_join_amplification_threshold() -> usize {
            2048
        }

        /// Default to 1 to be compatible with the behavior before this config is introduced.
        pub fn stream_exchange_connection_pool_size() -> Option<u16> {
            Some(1)
        }

        pub fn enable_actor_tokio_metrics() -> bool {
            false
        }

        pub fn stream_enable_auto_schema_change() -> bool {
            true
        }

        pub fn switch_jdbc_pg_to_native() -> bool {
            false
        }

        pub fn streaming_hash_join_entry_state_max_rows() -> usize {
            // NOTE(kwannoel): This is just an arbitrary number.
            30000
        }
    }

    pub use crate::system_param::default as system;

    pub mod batch {
        pub fn enable_barrier_read() -> bool {
            false
        }

        pub fn enable_spill() -> bool {
            true
        }

        pub fn statement_timeout_in_sec() -> u32 {
            // 1 hour
            60 * 60
        }

        pub fn frontend_compute_runtime_worker_threads() -> usize {
            4
        }

        pub fn mask_worker_temporary_secs() -> usize {
            30
        }

        pub fn redact_sql_option_keywords() -> Vec<String> {
            [
                "credential",
                "key",
                "password",
                "private",
                "secret",
                "token",
            ]
            .into_iter()
            .map(str::to_string)
            .collect()
        }
    }

    pub mod compaction_config {
        const MB: u64 = 1024 * 1024;
        const GB: u64 = 1024 * 1024 * 1024;
        const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * GB; // 2GB
        const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * MB; // 128MB
        const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * MB; // 512MB

        // decrease this configure when the generation of checkpoint barrier is not frequent.
        const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 12;
        const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 32 * MB;
        // 32MB
        const DEFAULT_MAX_SUB_COMPACTION: u32 = 4;
        const DEFAULT_LEVEL_MULTIPLIER: u64 = 5;
        const DEFAULT_MAX_SPACE_RECLAIM_BYTES: u64 = 512 * MB; // 512MB;
        const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER: u64 = 300;
        const DEFAULT_MAX_COMPACTION_FILE_COUNT: u64 = 100;
        const DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 3;
        const DEFAULT_MIN_OVERLAPPING_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 12;
        const DEFAULT_TOMBSTONE_RATIO_PERCENT: u32 = 40;
        const DEFAULT_EMERGENCY_PICKER: bool = true;
        const DEFAULT_MAX_LEVEL: u32 = 6;
        const DEFAULT_MAX_L0_COMPACT_LEVEL_COUNT: u32 = 42;
        const DEFAULT_SST_ALLOWED_TRIVIAL_MOVE_MIN_SIZE: u64 = 4 * MB;
        const DEFAULT_SST_ALLOWED_TRIVIAL_MOVE_MAX_COUNT: u32 = 64;
        const DEFAULT_EMERGENCY_LEVEL0_SST_FILE_COUNT: u32 = 2000; // > 50G / 32M = 1600
        const DEFAULT_EMERGENCY_LEVEL0_SUB_LEVEL_PARTITION: u32 = 256;
        const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_MAX_SST_COUNT: u32 = 10000; // 10000 * 32M = 320G
        const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_MAX_SIZE: u64 = 300 * 1024 * MB; // 300GB

        use crate::catalog::hummock::CompactionFilterFlag;

        pub fn max_bytes_for_level_base() -> u64 {
            DEFAULT_MAX_BYTES_FOR_LEVEL_BASE
        }

        pub fn max_bytes_for_level_multiplier() -> u64 {
            DEFAULT_LEVEL_MULTIPLIER
        }

        pub fn max_compaction_bytes() -> u64 {
            DEFAULT_MAX_COMPACTION_BYTES
        }

        pub fn sub_level_max_compaction_bytes() -> u64 {
            DEFAULT_MIN_COMPACTION_BYTES
        }

        pub fn level0_tier_compact_file_number() -> u64 {
            DEFAULT_TIER_COMPACT_TRIGGER_NUMBER
        }

        pub fn target_file_size_base() -> u64 {
            DEFAULT_TARGET_FILE_SIZE_BASE
        }

        pub fn compaction_filter_mask() -> u32 {
            (CompactionFilterFlag::STATE_CLEAN | CompactionFilterFlag::TTL).into()
        }

        pub fn max_sub_compaction() -> u32 {
            DEFAULT_MAX_SUB_COMPACTION
        }

        pub fn level0_stop_write_threshold_sub_level_number() -> u64 {
            DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER
        }

        pub fn level0_sub_level_compact_level_count() -> u32 {
            DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT
        }

        pub fn level0_overlapping_sub_level_compact_level_count() -> u32 {
            DEFAULT_MIN_OVERLAPPING_SUB_LEVEL_COMPACT_LEVEL_COUNT
        }

        pub fn max_space_reclaim_bytes() -> u64 {
            DEFAULT_MAX_SPACE_RECLAIM_BYTES
        }

        pub fn level0_max_compact_file_number() -> u64 {
            DEFAULT_MAX_COMPACTION_FILE_COUNT
        }

        pub fn tombstone_reclaim_ratio() -> u32 {
            DEFAULT_TOMBSTONE_RATIO_PERCENT
        }

        pub fn enable_emergency_picker() -> bool {
            DEFAULT_EMERGENCY_PICKER
        }

        pub fn max_level() -> u32 {
            DEFAULT_MAX_LEVEL
        }

        pub fn max_l0_compact_level_count() -> u32 {
            DEFAULT_MAX_L0_COMPACT_LEVEL_COUNT
        }

        pub fn sst_allowed_trivial_move_min_size() -> u64 {
            DEFAULT_SST_ALLOWED_TRIVIAL_MOVE_MIN_SIZE
        }

        pub fn disable_auto_group_scheduling() -> bool {
            false
        }

        pub fn max_overlapping_level_size() -> u64 {
            256 * MB
        }

        pub fn sst_allowed_trivial_move_max_count() -> u32 {
            DEFAULT_SST_ALLOWED_TRIVIAL_MOVE_MAX_COUNT
        }

        pub fn emergency_level0_sst_file_count() -> u32 {
            DEFAULT_EMERGENCY_LEVEL0_SST_FILE_COUNT
        }

        pub fn emergency_level0_sub_level_partition() -> u32 {
            DEFAULT_EMERGENCY_LEVEL0_SUB_LEVEL_PARTITION
        }

        pub fn level0_stop_write_threshold_max_sst_count() -> u32 {
            DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_MAX_SST_COUNT
        }

        pub fn level0_stop_write_threshold_max_size() -> u64 {
            DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_MAX_SIZE
        }
    }

    pub mod object_store_config {
        const DEFAULT_REQ_BACKOFF_INTERVAL_MS: u64 = 1000; // 1s
        const DEFAULT_REQ_BACKOFF_MAX_DELAY_MS: u64 = 10 * 1000; // 10s
        const DEFAULT_REQ_MAX_RETRY_ATTEMPTS: usize = 3;

        pub fn set_atomic_write_dir() -> bool {
            false
        }

        pub fn object_store_req_backoff_interval_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_INTERVAL_MS
        }

        pub fn object_store_req_backoff_max_delay_ms() -> u64 {
            DEFAULT_REQ_BACKOFF_MAX_DELAY_MS // 10s
        }

        pub fn object_store_req_backoff_factor() -> u64 {
            2
        }

        pub fn object_store_upload_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // init + upload_part + finish
        pub fn object_store_streaming_upload_attempt_timeout_ms() -> u64 {
            5 * 1000 // 5s
        }

        pub fn object_store_streaming_upload_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on block_size
        pub fn object_store_read_attempt_timeout_ms() -> u64 {
            8 * 1000 // 8s
        }

        pub fn object_store_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_streaming_read_attempt_timeout_ms() -> u64 {
            3 * 1000 // 3s
        }

        pub fn object_store_streaming_read_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_metadata_attempt_timeout_ms() -> u64 {
            60 * 1000 // 1min
        }

        pub fn object_store_metadata_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_delete_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        // tips: depend on batch size
        pub fn object_store_delete_objects_attempt_timeout_ms() -> u64 {
            5 * 1000
        }

        pub fn object_store_delete_objects_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn object_store_list_attempt_timeout_ms() -> u64 {
            10 * 60 * 1000
        }

        pub fn object_store_list_retry_attempts() -> usize {
            DEFAULT_REQ_MAX_RETRY_ATTEMPTS
        }

        pub fn opendal_upload_concurrency() -> usize {
            256
        }

        pub fn upload_part_size() -> usize {
            // 16m
            16 * 1024 * 1024
        }

        pub mod s3 {
            const DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S: u64 = 5;

            const DEFAULT_KEEPALIVE_MS: u64 = 600 * 1000; // 10min

            pub fn keepalive_ms() -> Option<u64> {
                Some(DEFAULT_KEEPALIVE_MS) // 10min
            }

            pub fn recv_buffer_size() -> Option<usize> {
                Some(1 << 21) // 2m
            }

            pub fn send_buffer_size() -> Option<usize> {
                None
            }

            pub fn nodelay() -> Option<bool> {
                Some(true)
            }

            pub fn identity_resolution_timeout_s() -> u64 {
                DEFAULT_IDENTITY_RESOLUTION_TIMEOUT_S
            }

            pub mod developer {
                pub fn retry_unknown_service_error() -> bool {
                    false
                }

                pub fn retryable_service_error_codes() -> Vec<String> {
                    vec!["SlowDown".into(), "TooManyRequests".into()]
                }

                pub fn use_opendal() -> bool {
                    true
                }
            }
        }
    }

    pub mod meta_store_config {
        const DEFAULT_MAX_CONNECTIONS: u32 = 10;
        const DEFAULT_MIN_CONNECTIONS: u32 = 1;
        const DEFAULT_CONNECTION_TIMEOUT_SEC: u64 = 10;
        const DEFAULT_IDLE_TIMEOUT_SEC: u64 = 30;
        const DEFAULT_ACQUIRE_TIMEOUT_SEC: u64 = 30;

        pub fn max_connections() -> u32 {
            DEFAULT_MAX_CONNECTIONS
        }

        pub fn min_connections() -> u32 {
            DEFAULT_MIN_CONNECTIONS
        }

        pub fn connection_timeout_sec() -> u64 {
            DEFAULT_CONNECTION_TIMEOUT_SEC
        }

        pub fn idle_timeout_sec() -> u64 {
            DEFAULT_IDLE_TIMEOUT_SEC
        }

        pub fn acquire_timeout_sec() -> u64 {
            DEFAULT_ACQUIRE_TIMEOUT_SEC
        }
    }
}

#[derive(Debug, Clone)]
pub enum EvictionConfig {
    Lru(LruConfig),
    Lfu(LfuConfig),
    S3Fifo(S3FifoConfig),
}

impl EvictionConfig {
    pub fn for_test() -> Self {
        Self::Lru(LruConfig {
            high_priority_pool_ratio: 0.0,
        })
    }
}

impl From<EvictionConfig> for foyer::EvictionConfig {
    fn from(value: EvictionConfig) -> Self {
        match value {
            EvictionConfig::Lru(lru) => foyer::EvictionConfig::Lru(lru),
            EvictionConfig::Lfu(lfu) => foyer::EvictionConfig::Lfu(lfu),
            EvictionConfig::S3Fifo(s3fifo) => foyer::EvictionConfig::S3Fifo(s3fifo),
        }
    }
}

pub struct StorageMemoryConfig {
    pub block_cache_capacity_mb: usize,
    pub block_cache_shard_num: usize,
    pub meta_cache_capacity_mb: usize,
    pub meta_cache_shard_num: usize,
    pub shared_buffer_capacity_mb: usize,
    pub compactor_memory_limit_mb: usize,
    pub prefetch_buffer_capacity_mb: usize,
    pub block_cache_eviction_config: EvictionConfig,
    pub meta_cache_eviction_config: EvictionConfig,
    pub block_file_cache_flush_buffer_threshold_mb: usize,
    pub meta_file_cache_flush_buffer_threshold_mb: usize,
}

pub const MAX_META_CACHE_SHARD_BITS: usize = 4;
pub const MIN_BUFFER_SIZE_PER_SHARD: usize = 256;
pub const MAX_BLOCK_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.

pub fn extract_storage_memory_config(s: &RwConfig) -> StorageMemoryConfig {
    let block_cache_capacity_mb = s.storage.cache.block_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::block_cache_capacity_mb()),
    );
    let meta_cache_capacity_mb = s.storage.cache.meta_cache_capacity_mb.unwrap_or(
        // adapt to old version
        s.storage
            .block_cache_capacity_mb
            .unwrap_or(default::storage::meta_cache_capacity_mb()),
    );
    let shared_buffer_capacity_mb = s
        .storage
        .shared_buffer_capacity_mb
        .unwrap_or(default::storage::shared_buffer_capacity_mb());
    let meta_cache_shard_num = s.storage.cache.meta_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        shard_bits
    });
    let block_cache_shard_num = s.storage.cache.block_cache_shard_num.unwrap_or_else(|| {
        let mut shard_bits = MAX_BLOCK_CACHE_SHARD_BITS;
        while (block_cache_capacity_mb >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0
        {
            shard_bits -= 1;
        }
        shard_bits
    });
    let compactor_memory_limit_mb = s
        .storage
        .compactor_memory_limit_mb
        .unwrap_or(default::storage::compactor_memory_limit_mb());

    let get_eviction_config = |c: &CacheEvictionConfig| {
        match c {
            CacheEvictionConfig::Lru {
                high_priority_ratio_in_percent,
            } => EvictionConfig::Lru(LruConfig {
                high_priority_pool_ratio: high_priority_ratio_in_percent.unwrap_or(
                    // adapt to old version
                    s.storage
                        .high_priority_ratio_in_percent
                        .unwrap_or(default::storage::high_priority_ratio_in_percent()),
                ) as f64
                    / 100.0,
            }),
            CacheEvictionConfig::Lfu {
                window_capacity_ratio_in_percent,
                protected_capacity_ratio_in_percent,
                cmsketch_eps,
                cmsketch_confidence,
            } => EvictionConfig::Lfu(LfuConfig {
                window_capacity_ratio: window_capacity_ratio_in_percent
                    .unwrap_or(default::storage::window_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                protected_capacity_ratio: protected_capacity_ratio_in_percent
                    .unwrap_or(default::storage::protected_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                cmsketch_eps: cmsketch_eps.unwrap_or(default::storage::cmsketch_eps()),
                cmsketch_confidence: cmsketch_confidence
                    .unwrap_or(default::storage::cmsketch_confidence()),
            }),
            CacheEvictionConfig::S3Fifo {
                small_queue_capacity_ratio_in_percent,
                ghost_queue_capacity_ratio_in_percent,
                small_to_main_freq_threshold,
            } => EvictionConfig::S3Fifo(S3FifoConfig {
                small_queue_capacity_ratio: small_queue_capacity_ratio_in_percent
                    .unwrap_or(default::storage::small_queue_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                ghost_queue_capacity_ratio: ghost_queue_capacity_ratio_in_percent
                    .unwrap_or(default::storage::ghost_queue_capacity_ratio_in_percent())
                    as f64
                    / 100.0,
                small_to_main_freq_threshold: small_to_main_freq_threshold
                    .unwrap_or(default::storage::small_to_main_freq_threshold()),
            }),
        }
    };

    let block_cache_eviction_config = get_eviction_config(&s.storage.cache.block_cache_eviction);
    let meta_cache_eviction_config = get_eviction_config(&s.storage.cache.meta_cache_eviction);

    let prefetch_buffer_capacity_mb =
        s.storage
            .shared_buffer_capacity_mb
            .unwrap_or(match &block_cache_eviction_config {
                EvictionConfig::Lru(lru) => {
                    ((1.0 - lru.high_priority_pool_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::Lfu(lfu) => {
                    ((1.0 - lfu.protected_capacity_ratio) * block_cache_capacity_mb as f64) as usize
                }
                EvictionConfig::S3Fifo(s3fifo) => {
                    (s3fifo.small_queue_capacity_ratio * block_cache_capacity_mb as f64) as usize
                }
            });

    let block_file_cache_flush_buffer_threshold_mb = s
        .storage
        .data_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::block_file_cache_flush_buffer_threshold_mb());
    let meta_file_cache_flush_buffer_threshold_mb = s
        .storage
        .meta_file_cache
        .flush_buffer_threshold_mb
        .unwrap_or(default::storage::block_file_cache_flush_buffer_threshold_mb());

    StorageMemoryConfig {
        block_cache_capacity_mb,
        block_cache_shard_num,
        meta_cache_capacity_mb,
        meta_cache_shard_num,
        shared_buffer_capacity_mb,
        compactor_memory_limit_mb,
        prefetch_buffer_capacity_mb,
        block_cache_eviction_config,
        meta_cache_eviction_config,
        block_file_cache_flush_buffer_threshold_mb,
        meta_file_cache_flush_buffer_threshold_mb,
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CompactionConfig {
    #[serde(default = "default::compaction_config::max_bytes_for_level_base")]
    pub max_bytes_for_level_base: u64,
    #[serde(default = "default::compaction_config::max_bytes_for_level_multiplier")]
    pub max_bytes_for_level_multiplier: u64,
    #[serde(default = "default::compaction_config::max_compaction_bytes")]
    pub max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::sub_level_max_compaction_bytes")]
    pub sub_level_max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::level0_tier_compact_file_number")]
    pub level0_tier_compact_file_number: u64,
    #[serde(default = "default::compaction_config::target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default::compaction_config::compaction_filter_mask")]
    pub compaction_filter_mask: u32,
    #[serde(default = "default::compaction_config::max_sub_compaction")]
    pub max_sub_compaction: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_sub_level_number")]
    pub level0_stop_write_threshold_sub_level_number: u64,
    #[serde(default = "default::compaction_config::level0_sub_level_compact_level_count")]
    pub level0_sub_level_compact_level_count: u32,
    #[serde(
        default = "default::compaction_config::level0_overlapping_sub_level_compact_level_count"
    )]
    pub level0_overlapping_sub_level_compact_level_count: u32,
    #[serde(default = "default::compaction_config::max_space_reclaim_bytes")]
    pub max_space_reclaim_bytes: u64,
    #[serde(default = "default::compaction_config::level0_max_compact_file_number")]
    pub level0_max_compact_file_number: u64,
    #[serde(default = "default::compaction_config::tombstone_reclaim_ratio")]
    pub tombstone_reclaim_ratio: u32,
    #[serde(default = "default::compaction_config::enable_emergency_picker")]
    pub enable_emergency_picker: bool,
    #[serde(default = "default::compaction_config::max_level")]
    pub max_level: u32,
    #[serde(default = "default::compaction_config::sst_allowed_trivial_move_min_size")]
    pub sst_allowed_trivial_move_min_size: u64,
    #[serde(default = "default::compaction_config::sst_allowed_trivial_move_max_count")]
    pub sst_allowed_trivial_move_max_count: u32,
    #[serde(default = "default::compaction_config::max_l0_compact_level_count")]
    pub max_l0_compact_level_count: u32,
    #[serde(default = "default::compaction_config::disable_auto_group_scheduling")]
    pub disable_auto_group_scheduling: bool,
    #[serde(default = "default::compaction_config::max_overlapping_level_size")]
    pub max_overlapping_level_size: u64,
    #[serde(default = "default::compaction_config::emergency_level0_sst_file_count")]
    pub emergency_level0_sst_file_count: u32,
    #[serde(default = "default::compaction_config::emergency_level0_sub_level_partition")]
    pub emergency_level0_sub_level_partition: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_max_sst_count")]
    pub level0_stop_write_threshold_max_sst_count: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_max_size")]
    pub level0_stop_write_threshold_max_size: u64,
}

/// Note: only applies to meta store backends other than `SQLite`.
#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct MetaStoreConfig {
    /// Maximum number of connections for the meta store connection pool.
    #[serde(default = "default::meta_store_config::max_connections")]
    pub max_connections: u32,
    /// Minimum number of connections for the meta store connection pool.
    #[serde(default = "default::meta_store_config::min_connections")]
    pub min_connections: u32,
    /// Connection timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::connection_timeout_sec")]
    pub connection_timeout_sec: u64,
    /// Idle timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::idle_timeout_sec")]
    pub idle_timeout_sec: u64,
    /// Acquire timeout in seconds for a meta store connection.
    #[serde(default = "default::meta_store_config::acquire_timeout_sec")]
    pub acquire_timeout_sec: u64,
}

#[cfg(test)]
mod tests {
    use risingwave_license::LicenseKey;

    use super::*;

    fn default_config_for_docs() -> RwConfig {
        let mut config = RwConfig::default();
        // Set `license_key` to empty in the docs to avoid any confusion.
        config.system.license_key = Some(LicenseKey::empty());
        config
    }

    /// This test ensures that `config/example.toml` is up-to-date with the default values specified
    /// in this file. Developer should run `./risedev generate-example-config` to update it if this
    /// test fails.
    #[test]
    fn test_example_up_to_date() {
        const HEADER: &str = "# This file is generated by ./risedev generate-example-config
# Check detailed comments in src/common/src/config.rs";

        let actual = expect_test::expect_file!["../../config/example.toml"];
        let default = toml::to_string(&default_config_for_docs()).expect("failed to serialize");

        let expected = format!("{HEADER}\n\n{default}");
        actual.assert_eq(&expected);

        let expected = rw_config_to_markdown();
        let actual = expect_test::expect_file!["../../config/docs.md"];
        actual.assert_eq(&expected);
    }

    #[derive(Debug)]
    struct ConfigItemDoc {
        desc: String,
        default: String,
    }

    fn rw_config_to_markdown() -> String {
        let mut config_rustdocs = BTreeMap::<String, Vec<(String, String)>>::new();
        RwConfig::config_docs("".to_owned(), &mut config_rustdocs);

        // Section -> Config Name -> ConfigItemDoc
        let mut configs: BTreeMap<String, BTreeMap<String, ConfigItemDoc>> = config_rustdocs
            .into_iter()
            .map(|(k, v)| {
                let docs: BTreeMap<String, ConfigItemDoc> = v
                    .into_iter()
                    .map(|(name, desc)| {
                        (
                            name,
                            ConfigItemDoc {
                                desc,
                                default: "".to_owned(), // unset
                            },
                        )
                    })
                    .collect();
                (k, docs)
            })
            .collect();

        let toml_doc: BTreeMap<String, toml::Value> =
            toml::from_str(&toml::to_string(&default_config_for_docs()).unwrap()).unwrap();
        toml_doc.into_iter().for_each(|(name, value)| {
            set_default_values("".to_owned(), name, value, &mut configs);
        });

        let mut markdown = "# RisingWave System Configurations\n\n".to_owned()
            + "This page is automatically generated by `./risedev generate-example-config`\n";
        for (section, configs) in configs {
            if configs.is_empty() {
                continue;
            }
            markdown.push_str(&format!("\n## {}\n\n", section));
            markdown.push_str("| Config | Description | Default |\n");
            markdown.push_str("|--------|-------------|---------|\n");
            for (config, doc) in configs {
                markdown.push_str(&format!(
                    "| {} | {} | {} |\n",
                    config, doc.desc, doc.default
                ));
            }
        }
        markdown
    }

    fn set_default_values(
        section: String,
        name: String,
        value: toml::Value,
        configs: &mut BTreeMap<String, BTreeMap<String, ConfigItemDoc>>,
    ) {
        // Set the default value if it's a config name-value pair, otherwise it's a sub-section (Table) that should be recursively processed.
        if let toml::Value::Table(table) = value {
            let section_configs: BTreeMap<String, toml::Value> =
                table.clone().into_iter().collect();
            let sub_section = if section.is_empty() {
                name
            } else {
                format!("{}.{}", section, name)
            };
            section_configs
                .into_iter()
                .for_each(|(k, v)| set_default_values(sub_section.clone(), k, v, configs))
        } else if let Some(t) = configs.get_mut(&section) {
            if let Some(item_doc) = t.get_mut(&name) {
                item_doc.default = format!("{}", value);
            }
        }
    }

    #[test]
    fn test_object_store_configs_backward_compatibility() {
        // Define configs with the old name and make sure it still works
        {
            let config: RwConfig = toml::from_str(
                r#"
            [storage.object_store]
            object_store_set_atomic_write_dir = true

            [storage.object_store.s3]
            object_store_keepalive_ms = 1
            object_store_send_buffer_size = 1
            object_store_recv_buffer_size = 1
            object_store_nodelay = false

            [storage.object_store.s3.developer]
            object_store_retry_unknown_service_error = true
            object_store_retryable_service_error_codes = ['dummy']


            "#,
            )
            .unwrap();

            assert!(config.storage.object_store.set_atomic_write_dir);
            assert_eq!(config.storage.object_store.s3.keepalive_ms, Some(1));
            assert_eq!(config.storage.object_store.s3.send_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.recv_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.nodelay, Some(false));
            assert!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retry_unknown_service_error
            );
            assert_eq!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retryable_service_error_codes,
                vec!["dummy".to_owned()]
            );
        }

        // Define configs with the new name and make sure it works
        {
            let config: RwConfig = toml::from_str(
                r#"
            [storage.object_store]
            set_atomic_write_dir = true

            [storage.object_store.s3]
            keepalive_ms = 1
            send_buffer_size = 1
            recv_buffer_size = 1
            nodelay = false

            [storage.object_store.s3.developer]
            retry_unknown_service_error = true
            retryable_service_error_codes = ['dummy']


            "#,
            )
            .unwrap();

            assert!(config.storage.object_store.set_atomic_write_dir);
            assert_eq!(config.storage.object_store.s3.keepalive_ms, Some(1));
            assert_eq!(config.storage.object_store.s3.send_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.recv_buffer_size, Some(1));
            assert_eq!(config.storage.object_store.s3.nodelay, Some(false));
            assert!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retry_unknown_service_error
            );
            assert_eq!(
                config
                    .storage
                    .object_store
                    .s3
                    .developer
                    .retryable_service_error_codes,
                vec!["dummy".to_owned()]
            );
        }
    }

    #[test]
    fn test_meta_configs_backward_compatibility() {
        // Test periodic_space_reclaim_compaction_interval_sec
        {
            let config: RwConfig = toml::from_str(
                r#"
            [meta]
            periodic_split_compact_group_interval_sec = 1
            table_write_throughput_threshold = 10
            min_table_split_write_throughput = 5
            "#,
            )
            .unwrap();

            assert_eq!(
                config
                    .meta
                    .periodic_scheduling_compaction_group_split_interval_sec,
                1
            );
            assert_eq!(config.meta.table_high_write_throughput_threshold, 10);
            assert_eq!(config.meta.table_low_write_throughput_threshold, 5);
        }
    }
}
