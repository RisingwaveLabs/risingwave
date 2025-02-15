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

mod non_zero64;
mod over_window;
mod query_mode;
mod search_path;
pub mod sink_decouple;
mod transaction_isolation_level;
mod visibility_mode;

use chrono_tz::Tz;
pub use over_window::OverWindowCachePolicy;
pub use query_mode::QueryMode;
use risingwave_common_proc_macro::{ConfigDoc, SessionConfig};
pub use search_path::{SearchPath, USER_NAME_WILD_CARD};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use thiserror::Error;

use self::non_zero64::ConfigNonZeroU64;
use crate::hash::VirtualNode;
use crate::session_config::sink_decouple::SinkDecouple;
use crate::session_config::transaction_isolation_level::IsolationLevel;
pub use crate::session_config::visibility_mode::VisibilityMode;
use crate::{PG_VERSION, SERVER_ENCODING, SERVER_VERSION_NUM, STANDARD_CONFORMING_STRINGS};

pub const SESSION_CONFIG_LIST_SEP: &str = ", ";

#[derive(Error, Debug)]
pub enum SessionConfigError {
    #[error("Invalid value `{value}` for `{entry}`")]
    InvalidValue {
        entry: &'static str,
        value: String,
        source: anyhow::Error,
    },

    #[error("Unrecognized config entry `{0}`")]
    UnrecognizedEntry(String),
}

type SessionConfigResult<T> = std::result::Result<T, SessionConfigError>;

// NOTE(kwannoel): We declare it separately as a constant,
// otherwise seems like it can't infer the type of -1 when written inline.
const DISABLE_BACKFILL_RATE_LIMIT: i32 = -1;
const DISABLE_SOURCE_RATE_LIMIT: i32 = -1;
const DISABLE_DML_RATE_LIMIT: i32 = -1;
const DISABLE_SINK_RATE_LIMIT: i32 = -1;

/// Default to bypass cluster limits iff in debug mode.
const BYPASS_CLUSTER_LIMITS: bool = cfg!(debug_assertions);

#[serde_as]
/// This is the Session Config of RisingWave.
#[derive(Clone, Debug, Deserialize, Serialize, SessionConfig, ConfigDoc, PartialEq)]
pub struct SessionConfig {
    /// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
    /// until the entire dataflow is refreshed. In other words, every related table & MV will
    /// be able to see the write.
    #[parameter(default = false, alias = "rw_implicit_flush")]
    implicit_flush: bool,

    /// If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in
    /// MV creation.
    #[parameter(default = false)]
    create_compaction_group_for_mv: bool,

    /// A temporary config variable to force query running in either local or distributed mode.
    /// The default value is auto which means let the system decide to run batch queries in local
    /// or distributed mode automatically.
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = QueryMode::default())]
    query_mode: QueryMode,

    /// Sets the number of digits displayed for floating-point values.
    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#:~:text=for%20more%20information.-,extra_float_digits,-(integer)>
    #[parameter(default = 1)]
    extra_float_digits: i32,

    /// Sets the application name to be reported in statistics and logs.
    /// See <https://www.postgresql.org/docs/14/runtime-config-logging.html#:~:text=What%20to%20Log-,application_name,-(string)>
    #[parameter(default = "", flags = "REPORT")]
    application_name: String,

    /// It is typically set by an application upon connection to the server.
    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-DATESTYLE>
    #[parameter(default = "", rename = "datestyle")]
    date_style: String,

    /// Force the use of lookup join instead of hash join when possible for local batch execution.
    #[parameter(default = true, alias = "rw_batch_enable_lookup_join")]
    batch_enable_lookup_join: bool,

    /// Enable usage of sortAgg instead of hash agg when order property is satisfied in batch
    /// execution
    #[parameter(default = true, alias = "rw_batch_enable_sort_agg")]
    batch_enable_sort_agg: bool,

    /// Enable distributed DML, so an insert, delete, and update statement can be executed in a distributed way (e.g. running in multiple compute nodes).
    /// No atomicity guarantee in this mode. Its goal is to gain the best ingestion performance for initial batch ingestion where users always can drop their table when failure happens.
    #[parameter(default = false, rename = "batch_enable_distributed_dml")]
    batch_enable_distributed_dml: bool,

    /// Evaluate expression in strict mode for batch queries.
    /// If set to false, an expression failure will not cause an error but leave a null value
    /// on the result set.
    #[parameter(default = true)]
    batch_expr_strict_mode: bool,

    /// The max gap allowed to transform small range scan into multi point lookup.
    #[parameter(default = 8)]
    max_split_range_gap: i32,

    /// Sets the order in which schemas are searched when an object (table, data type, function, etc.)
    /// is referenced by a simple name with no schema specified.
    /// See <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = SearchPath::default())]
    search_path: SearchPath,

    /// If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = VisibilityMode::default())]
    visibility_mode: VisibilityMode,

    /// See <https://www.postgresql.org/docs/current/transaction-iso.html>
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = IsolationLevel::default())]
    transaction_isolation: IsolationLevel,

    /// Select as of specific epoch.
    /// Sets the historical epoch for querying data. If 0, querying latest data.
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = ConfigNonZeroU64::default())]
    query_epoch: ConfigNonZeroU64,

    /// Session timezone. Defaults to UTC.
    #[parameter(default = "UTC", check_hook = check_timezone)]
    timezone: String,

    /// The execution parallelism for streaming queries, including tables, materialized views, indexes,
    /// and sinks. Defaults to 0, which means they will be scheduled adaptively based on the cluster size.
    ///
    /// If a non-zero value is set, streaming queries will be scheduled to use a fixed number of parallelism.
    /// Note that the value will be bounded at `STREAMING_MAX_PARALLELISM`.
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = ConfigNonZeroU64::default())]
    streaming_parallelism: ConfigNonZeroU64,

    /// Enable delta join for streaming queries. Defaults to false.
    #[parameter(default = false, alias = "rw_streaming_enable_delta_join")]
    streaming_enable_delta_join: bool,

    /// Enable bushy join for streaming queries. Defaults to true.
    #[parameter(default = true, alias = "rw_streaming_enable_bushy_join")]
    streaming_enable_bushy_join: bool,

    /// Enable arrangement backfill for streaming queries. Defaults to true.
    /// When set to true, the parallelism of the upstream fragment will be
    /// decoupled from the parallelism of the downstream scan fragment.
    /// Or more generally, the parallelism of the upstream table / index / mv
    /// will be decoupled from the parallelism of the downstream table / index / mv / sink.
    #[parameter(default = true)]
    streaming_use_arrangement_backfill: bool,

    #[parameter(default = false)]
    streaming_use_snapshot_backfill: bool,

    /// Allow `jsonb` in stream key
    #[parameter(default = false, alias = "rw_streaming_allow_jsonb_in_stream_key")]
    streaming_allow_jsonb_in_stream_key: bool,

    /// Enable join ordering for streaming and batch queries. Defaults to true.
    #[parameter(default = true, alias = "rw_enable_join_ordering")]
    enable_join_ordering: bool,

    /// Enable two phase agg optimization. Defaults to true.
    /// Setting this to true will always set `FORCE_TWO_PHASE_AGG` to false.
    #[parameter(default = true, flags = "SETTER", alias = "rw_enable_two_phase_agg")]
    enable_two_phase_agg: bool,

    /// Force two phase agg optimization whenever there's a choice between
    /// optimizations. Defaults to false.
    /// Setting this to true will always set `ENABLE_TWO_PHASE_AGG` to false.
    #[parameter(default = false, flags = "SETTER", alias = "rw_force_two_phase_agg")]
    force_two_phase_agg: bool,

    /// Enable sharing of common sub-plans.
    /// This means that DAG structured query plans can be constructed,
    #[parameter(default = true, alias = "rw_enable_share_plan")]
    /// rather than only tree structured query plans.
    enable_share_plan: bool,

    /// Enable split distinct agg
    #[parameter(default = false, alias = "rw_force_split_distinct_agg")]
    force_split_distinct_agg: bool,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE>
    #[parameter(default = "", rename = "intervalstyle")]
    interval_style: String,

    /// If `BATCH_PARALLELISM` is non-zero, batch queries will use this parallelism.
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = ConfigNonZeroU64::default())]
    batch_parallelism: ConfigNonZeroU64,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[parameter(default = PG_VERSION)]
    server_version: String,

    /// The version of PostgreSQL that Risingwave claims to be.
    #[parameter(default = SERVER_VERSION_NUM)]
    server_version_num: i32,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-MIN-MESSAGES>
    #[parameter(default = "notice")]
    client_min_messages: String,

    /// see <https://www.postgresql.org/docs/15/runtime-config-client.html#GUC-CLIENT-ENCODING>
    #[parameter(default = SERVER_ENCODING, check_hook = check_client_encoding)]
    client_encoding: String,

    /// Enable decoupling sink and internal streaming graph or not
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = SinkDecouple::default())]
    sink_decouple: SinkDecouple,

    /// See <https://www.postgresql.org/docs/current/runtime-config-compatible.html#RUNTIME-CONFIG-COMPATIBLE-VERSION>
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = false)]
    synchronize_seqscans: bool,

    /// Abort query statement that takes more than the specified amount of time in sec. If
    /// `log_min_error_statement` is set to ERROR or lower, the statement that timed out will also be
    /// logged. If this value is specified without units, it is taken as milliseconds. A value of
    /// zero (the default) disables the timeout.
    #[parameter(default = 0u32)]
    statement_timeout: u32,

    /// Terminate any session that has been idle (that is, waiting for a client query) within an open transaction for longer than the specified amount of time in milliseconds.
    #[parameter(default = 60000u32)]
    idle_in_transaction_session_timeout: u32,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-LOCK-TIMEOUT>
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = 0)]
    lock_timeout: i32,

    /// For limiting the startup time of a shareable CDC streaming source when the source is being created. Unit: seconds.
    #[parameter(default = 30)]
    cdc_source_wait_streaming_start_timeout: i32,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-ROW-SECURITY>.
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = true)]
    row_security: bool,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
    #[parameter(default = STANDARD_CONFORMING_STRINGS)]
    standard_conforming_strings: String,

    /// Set streaming rate limit (rows per second) for each parallelism for mv / source / sink backfilling
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the snapshot read / source read.
    #[parameter(default = DISABLE_BACKFILL_RATE_LIMIT)]
    backfill_rate_limit: i32,

    /// Set streaming rate limit (rows per second) for each parallelism for mv / source backfilling, source reads.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the snapshot read / source read.
    #[parameter(default = DISABLE_SOURCE_RATE_LIMIT)]
    source_rate_limit: i32,

    /// Set streaming rate limit (rows per second) for each parallelism for table DML.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the DML.
    #[parameter(default = DISABLE_DML_RATE_LIMIT)]
    dml_rate_limit: i32,

    /// Set sink rate limit (rows per second) for each parallelism for external sink.
    /// If set to -1, disable rate limit.
    /// If set to 0, this pauses the sink.
    #[parameter(default = DISABLE_SINK_RATE_LIMIT)]
    sink_rate_limit: i32,

    /// Cache policy for partition cache in streaming over window.
    /// Can be "full", "recent", "`recent_first_n`" or "`recent_last_n`".
    #[serde_as(as = "DisplayFromStr")]
    #[parameter(default = OverWindowCachePolicy::default(), alias = "rw_streaming_over_window_cache_policy")]
    streaming_over_window_cache_policy: OverWindowCachePolicy,

    /// Run DDL statements in background
    #[parameter(default = false)]
    background_ddl: bool,

    /// Enable shared source. Currently only for Kafka.
    ///
    /// When enabled, `CREATE SOURCE` will create a source streaming job, and `CREATE MATERIALIZED VIEWS` from the source
    /// will forward the data from the same source streaming job, and also backfill prior data from the external source.
    #[parameter(default = true)]
    streaming_use_shared_source: bool,

    /// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time.
    #[parameter(default = SERVER_ENCODING)]
    server_encoding: String,

    #[parameter(default = "hex", check_hook = check_bytea_output)]
    bytea_output: String,

    /// Bypass checks on cluster limits
    ///
    /// When enabled, `CREATE MATERIALIZED VIEW` will not fail if the cluster limit is hit.
    #[parameter(default = BYPASS_CLUSTER_LIMITS)]
    bypass_cluster_limits: bool,

    /// The maximum number of parallelism a streaming query can use. Defaults to 256.
    ///
    /// Compared to `STREAMING_PARALLELISM`, which configures the initial parallelism, this configures
    /// the maximum parallelism a streaming query can use in the future, if the cluster size changes or
    /// users manually change the parallelism with `ALTER .. SET PARALLELISM`.
    ///
    /// It's not always a good idea to set this to a very large number, as it may cause performance
    /// degradation when performing range scans on the table or the materialized view.
    // a.k.a. vnode count
    #[parameter(default = VirtualNode::COUNT_FOR_COMPAT, check_hook = check_streaming_max_parallelism)]
    streaming_max_parallelism: usize,

    /// Used to provide the connection information for the iceberg engine.
    /// Format: iceberg_engine_connection = schema_name.connection_name.
    #[parameter(default = "", check_hook = check_iceberg_engine_connection)]
    iceberg_engine_connection: String,

    /// Whether the streaming join should be unaligned or not.
    #[parameter(default = false)]
    streaming_unaligned_join: bool,
}

fn check_iceberg_engine_connection(val: &str) -> Result<(), String> {
    if val.is_empty() {
        return Ok(());
    }

    let parts: Vec<&str> = val.split('.').collect();
    if parts.len() != 2 {
        return Err("Invalid iceberg engine connection format, Should be set to this format: schema_name.connection_name.".to_owned());
    }

    Ok(())
}

fn check_timezone(val: &str) -> Result<(), String> {
    // Check if the provided string is a valid timezone.
    Tz::from_str_insensitive(val).map_err(|_e| "Not a valid timezone")?;
    Ok(())
}

fn check_client_encoding(val: &str) -> Result<(), String> {
    // https://github.com/postgres/postgres/blob/REL_15_3/src/common/encnames.c#L525
    let clean = val.replace(|c: char| !c.is_ascii_alphanumeric(), "");
    if !clean.eq_ignore_ascii_case("UTF8") {
        Err("Only support 'UTF8' for CLIENT_ENCODING".to_owned())
    } else {
        Ok(())
    }
}

fn check_bytea_output(val: &str) -> Result<(), String> {
    if val == "hex" {
        Ok(())
    } else {
        Err("Only support 'hex' for BYTEA_OUTPUT".to_owned())
    }
}

/// Check if the provided value is a valid max parallelism.
fn check_streaming_max_parallelism(val: &usize) -> Result<(), String> {
    match val {
        // TODO(var-vnode): this is to prevent confusion with singletons, after we distinguish
        // them better, we may allow 1 as the max parallelism (though not much point).
        0 | 1 => Err("STREAMING_MAX_PARALLELISM must be greater than 1".to_owned()),
        2..=VirtualNode::MAX_COUNT => Ok(()),
        _ => Err(format!(
            "STREAMING_MAX_PARALLELISM must be less than or equal to {}",
            VirtualNode::MAX_COUNT
        )),
    }
}

impl SessionConfig {
    pub fn set_force_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<bool> {
        let set_val = self.set_force_two_phase_agg_inner(val, reporter)?;
        if self.force_two_phase_agg {
            self.set_enable_two_phase_agg(true, reporter)
        } else {
            Ok(set_val)
        }
    }

    pub fn set_enable_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<bool> {
        let set_val = self.set_enable_two_phase_agg_inner(val, reporter)?;
        if !self.force_two_phase_agg {
            self.set_force_two_phase_agg(false, reporter)
        } else {
            Ok(set_val)
        }
    }
}

pub struct VariableInfo {
    pub name: String,
    pub setting: String,
    pub description: String,
}

/// Report status or notice to caller.
pub trait ConfigReporter {
    fn report_status(&mut self, key: &str, new_val: String);
}

// Report nothing.
impl ConfigReporter for () {
    fn report_status(&mut self, _key: &str, _new_val: String) {}
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(SessionConfig)]
    struct TestConfig {
        #[parameter(default = 1, flags = "NO_ALTER_SYS", alias = "test_param_alias" | "alias_param_test")]
        test_param: i32,
    }

    #[test]
    fn test_session_config_alias() {
        let mut config = TestConfig::default();
        config.set("test_param", "2".to_owned(), &mut ()).unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "2");
        config
            .set("alias_param_test", "3".to_owned(), &mut ())
            .unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "3");
        assert!(TestConfig::check_no_alter_sys("test_param").unwrap());
    }
}
