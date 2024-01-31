// Copyright 2024 RisingWave Labs
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
use risingwave_common_proc_macro::SessionConfig;
pub use search_path::{SearchPath, USER_NAME_WILD_CARD};
use thiserror::Error;

use self::non_zero64::ConfigNonZeroU64;
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

/// This is the Session Config of RisingWave.
#[derive(SessionConfig)]
pub struct ConfigMap {
    /// If `RW_IMPLICIT_FLUSH` is on, then every INSERT/UPDATE/DELETE statement will block
    /// until the entire dataflow is refreshed. In other words, every related table & MV will
    /// be able to see the write.
    #[parameter(default = false, rename = "rw_implicit_flush")]
    implicit_flush: bool,

    /// If `CREATE_COMPACTION_GROUP_FOR_MV` is on, dedicated compaction groups will be created in
    /// MV creation.
    #[parameter(default = false)]
    create_compaction_group_for_mv: bool,

    /// A temporary config variable to force query running in either local or distributed mode.
    /// The default value is auto which means let the system decide to run batch queries in local
    /// or distributed mode automatically.
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
    #[parameter(default = true, rename = "rw_batch_enable_lookup_join")]
    batch_enable_lookup_join: bool,

    /// Enable usage of sortAgg instead of hash agg when order property is satisfied in batch
    /// execution
    #[parameter(default = true, rename = "rw_batch_enable_sort_agg")]
    batch_enable_sort_agg: bool,

    /// Enable distributed DML, so an insert, delete, and update statement can be executed in a distributed way (e.g. running in multiple compute nodes).
    /// No atomicity guarantee in this mode. Its goal is to gain the best ingestion performance for initial batch ingestion where users always can drop their table when failure happens.
    #[parameter(default = false, rename = "batch_enable_distributed_dml")]
    batch_enable_distributed_dml: bool,

    /// The max gap allowed to transform small range scan into multi point lookup.
    #[parameter(default = 8)]
    max_split_range_gap: i32,

    /// Sets the order in which schemas are searched when an object (table, data type, function, etc.)
    /// is referenced by a simple name with no schema specified.
    /// See <https://www.postgresql.org/docs/14/runtime-config-client.html#GUC-SEARCH-PATH>
    #[parameter(default = SearchPath::default())]
    search_path: SearchPath,

    /// If `VISIBILITY_MODE` is all, we will support querying data without checkpoint.
    #[parameter(default = VisibilityMode::default())]
    visibility_mode: VisibilityMode,

    /// See <https://www.postgresql.org/docs/current/transaction-iso.html>
    #[parameter(default = IsolationLevel::default())]
    transaction_isolation: IsolationLevel,

    /// Select as of specific epoch.
    /// Sets the historical epoch for querying data. If 0, querying latest data.
    #[parameter(default = ConfigNonZeroU64::default())]
    query_epoch: ConfigNonZeroU64,

    /// Session timezone. Defaults to UTC.
    #[parameter(default = "UTC", check_hook = check_timezone)]
    timezone: String,

    /// If `STREAMING_PARALLELISM` is non-zero, CREATE MATERIALIZED VIEW/TABLE/INDEX will use it as
    /// streaming parallelism.
    #[parameter(default = ConfigNonZeroU64::default())]
    streaming_parallelism: ConfigNonZeroU64,

    /// Enable delta join for streaming queries. Defaults to false.
    #[parameter(default = false, rename = "rw_streaming_enable_delta_join")]
    streaming_enable_delta_join: bool,

    /// Enable bushy join for streaming queries. Defaults to true.
    #[parameter(default = true, rename = "rw_streaming_enable_bushy_join")]
    streaming_enable_bushy_join: bool,

    /// Enable arrangement backfill for streaming queries. Defaults to false.
    #[parameter(default = true)]
    streaming_enable_arrangement_backfill: bool,

    /// Allow `jsonb` in stream key
    #[parameter(default = false, rename = "rw_streaming_allow_jsonb_in_stream_key")]
    streaming_allow_jsonb_in_stream_key: bool,

    /// Enable join ordering for streaming and batch queries. Defaults to true.
    #[parameter(default = true, rename = "rw_enable_join_ordering")]
    enable_join_ordering: bool,

    /// Enable two phase agg optimization. Defaults to true.
    /// Setting this to true will always set `FORCE_TWO_PHASE_AGG` to false.
    #[parameter(default = true, flags = "SETTER", rename = "rw_enable_two_phase_agg")]
    enable_two_phase_agg: bool,

    /// Force two phase agg optimization whenever there's a choice between
    /// optimizations. Defaults to false.
    /// Setting this to true will always set `ENABLE_TWO_PHASE_AGG` to false.
    #[parameter(default = false, flags = "SETTER", rename = "rw_force_two_phase_agg")]
    force_two_phase_agg: bool,

    /// Enable sharing of common sub-plans.
    /// This means that DAG structured query plans can be constructed,
    #[parameter(default = true, rename = "rw_enable_share_plan")]
    /// rather than only tree structured query plans.
    enable_share_plan: bool,

    /// Enable split distinct agg
    #[parameter(default = false, rename = "rw_force_split_distinct_agg")]
    force_split_distinct_agg: bool,

    /// See <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-INTERVALSTYLE>
    #[parameter(default = "", rename = "intervalstyle")]
    interval_style: String,

    /// If `BATCH_PARALLELISM` is non-zero, batch queries will use this parallelism.
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
    #[parameter(default = SinkDecouple::default())]
    sink_decouple: SinkDecouple,

    /// See <https://www.postgresql.org/docs/current/runtime-config-compatible.html#RUNTIME-CONFIG-COMPATIBLE-VERSION>
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = false)]
    synchronize_seqscans: bool,

    /// Abort query statement that takes more than the specified amount of time in sec. If
    /// log_min_error_statement is set to ERROR or lower, the statement that timed out will also be
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

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-ROW-SECURITY>.
    /// Unused in RisingWave, support for compatibility.
    #[parameter(default = true)]
    row_security: bool,

    /// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
    #[parameter(default = STANDARD_CONFORMING_STRINGS)]
    standard_conforming_strings: String,

    /// Set streaming rate limit (rows per second) for each parallelism for mv backfilling
    #[parameter(default = ConfigNonZeroU64::default())]
    streaming_rate_limit: ConfigNonZeroU64,

    /// Cache policy for partition cache in streaming over window.
    /// Can be "full", "recent", "recent_first_n" or "recent_last_n".
    #[parameter(default = OverWindowCachePolicy::default(), rename = "rw_streaming_over_window_cache_policy")]
    streaming_over_window_cache_policy: OverWindowCachePolicy,

    /// Run DDL statements in background
    #[parameter(default = false)]
    background_ddl: bool,

    /// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time.
    #[parameter(default = SERVER_ENCODING)]
    server_encoding: String,

    #[parameter(default = "hex", check_hook = check_bytea_output)]
    bytea_output: String,
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
        Err("Only support 'UTF8' for CLIENT_ENCODING".to_string())
    } else {
        Ok(())
    }
}

fn check_bytea_output(val: &str) -> Result<(), String> {
    if val == "hex" {
        Ok(())
    } else {
        Err("Only support 'hex' for BYTEA_OUTPUT".to_string())
    }
}

impl ConfigMap {
    pub fn set_force_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<()> {
        self.set_force_two_phase_agg_inner(val, reporter)?;
        if self.force_two_phase_agg {
            self.set_enable_two_phase_agg(true, reporter)
        } else {
            Ok(())
        }
    }

    pub fn set_enable_two_phase_agg(
        &mut self,
        val: bool,
        reporter: &mut impl ConfigReporter,
    ) -> SessionConfigResult<()> {
        self.set_enable_two_phase_agg_inner(val, reporter)?;
        if !self.force_two_phase_agg {
            self.set_force_two_phase_agg(false, reporter)
        } else {
            Ok(())
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
        #[parameter(default = 1, alias = "test_param_alias" | "alias_param_test")]
        test_param: i32,
    }

    #[test]
    fn test_session_config_alias() {
        let mut config = TestConfig::default();
        config.set("test_param", "2".to_string(), &mut ()).unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "2");
        config
            .set("alias_param_test", "3".to_string(), &mut ())
            .unwrap();
        assert_eq!(config.get("test_param_alias").unwrap(), "3");
    }
}
