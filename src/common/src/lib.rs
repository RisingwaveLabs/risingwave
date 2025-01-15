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

#![expect(
    refining_impl_trait,
    reason = "Some of the Row::iter() implementations returns ExactSizeIterator. Is this reasonable?"
)]
#![feature(extract_if)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]
#![feature(test)]
#![feature(trusted_len)]
#![feature(allocator_api)]
#![feature(coroutines)]
#![feature(map_try_insert)]
#![feature(error_generic_member_access)]
#![feature(let_chains)]
#![feature(portable_simd)]
#![feature(array_chunks)]
#![feature(inline_const_pat)]
#![allow(incomplete_features)]
#![feature(iterator_try_collect)]
#![feature(iter_order_by)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(impl_trait_in_assoc_type)]
#![feature(negative_impls)]
#![feature(register_tool)]
#![feature(btree_cursors)]
#![register_tool(rw)]

#[cfg_attr(not(test), allow(unused_extern_crates))]
extern crate self as risingwave_common;

// Re-export all macros from `risingwave_error` crate for code compatibility,
// since they were previously defined and exported from `risingwave_common`.
#[macro_use]
extern crate risingwave_error;
pub use risingwave_error::common::{
    bail_no_function, bail_not_implemented, no_function, not_implemented,
};
pub use risingwave_error::macros::*;

#[macro_use]
pub mod jemalloc;
#[macro_use]
pub mod error;
#[macro_use]
pub mod array;
#[macro_use]
pub mod util;
pub mod acl;
pub mod bitmap;
pub mod cache;
pub mod cast;
pub mod catalog;
pub mod config;
pub mod constants;
pub mod field_generator;
pub mod hash;
pub mod log;
pub mod memory;
pub use risingwave_common_metrics::{
    monitor, register_guarded_gauge_vec_with_registry,
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry, register_guarded_uint_gauge_vec_with_registry,
};
pub use {
    risingwave_common_metrics as metrics, risingwave_common_secret as secret,
    risingwave_license as license,
};
pub mod lru;
pub mod opts;
pub mod range;
pub mod row;
pub mod sequence;
pub mod session_config;
pub mod system_param;
pub mod telemetry;
pub mod test_utils;
pub mod transaction;
pub mod types;
pub mod vnode_mapping;

pub mod test_prelude {
    pub use super::array::{DataChunkTestExt, StreamChunkTestExt};
    pub use super::catalog::test_utils::ColumnDescTestExt;
}

pub const RW_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Placeholder for unknown git sha.
pub const UNKNOWN_GIT_SHA: &str = "unknown";

// The single source of truth of the pg parameters, Used in SessionConfig and current_cluster_version.
// The version of PostgreSQL that Risingwave claims to be.
pub const PG_VERSION: &str = "13.14.0";
/// The version of PostgreSQL that Risingwave claims to be.
pub const SERVER_VERSION_NUM: i32 = 130014;
/// Shows the server-side character set encoding. At present, this parameter can be shown but not set, because the encoding is determined at database creation time. It is also the default value of `client_encoding`.
pub const SERVER_ENCODING: &str = "UTF8";
/// see <https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-STANDARD-CONFORMING-STRINGS>
pub const STANDARD_CONFORMING_STRINGS: &str = "on";

#[macro_export]
macro_rules! git_sha {
    ($env:literal) => {
        match option_env!($env) {
            Some(v) if !v.is_empty() => v,
            _ => $crate::UNKNOWN_GIT_SHA,
        }
    };
}

// FIXME: We expand `unwrap_or` since it's unavailable in const context now.
// `const_option_ext` was broken by https://github.com/rust-lang/rust/pull/110393
// Tracking issue: https://github.com/rust-lang/rust/issues/91930
pub const GIT_SHA: &str = git_sha!("GIT_SHA");

pub fn current_cluster_version() -> String {
    format!(
        "PostgreSQL {}-RisingWave-{} ({})",
        PG_VERSION, RW_VERSION, GIT_SHA
    )
}

/// Panics if `debug_assertions` is set, otherwise logs a warning.
///
/// Note: unlike `panic` which returns `!`, this macro returns `()`,
/// which cannot be used like `result.unwrap_or_else(|| panic_if_debug!(...))`.
#[macro_export]
macro_rules! panic_if_debug {
    ($($arg:tt)*) => {
        if cfg!(debug_assertions) {
            panic!($($arg)*)
        } else {
            tracing::warn!($($arg)*)
        }
    };
}
