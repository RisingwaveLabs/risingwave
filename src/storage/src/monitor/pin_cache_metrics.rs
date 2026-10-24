// Copyright 2026 RisingWave Labs
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

use std::sync::LazyLock;

use prometheus::{
    IntCounter, IntCounterVec, IntGauge, Registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

pub(crate) static GLOBAL_PIN_CACHE_METRICS: LazyLock<PinCacheMetrics> =
    LazyLock::new(|| PinCacheMetrics::new(&GLOBAL_METRICS_REGISTRY));

/// Metrics for Hummock-local Pin Cache refill, lifecycle and physical reclamation.
pub(crate) struct PinCacheMetrics {
    pub io_failures: IntCounterVec,
    pub refill_outcomes: IntCounterVec,
    pub capacity_bytes: IntGauge,
    pub accounted_bytes: IntGauge,
    pub published_objects: IntGauge,
    pub published_bytes: IntGauge,
    pub recovery_ready: IntGauge,
    pub recovery_failures: IntCounter,
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "GC scheduling is not wired yet")
    )]
    pub gc_failures: IntCounter,
}

impl PinCacheMetrics {
    fn new(registry: &Registry) -> Self {
        let refill_outcomes = register_int_counter_vec_with_registry!(
            "pin_cache_refill_total",
            "Pin whole-object refill attempt outcomes",
            &["result"],
            registry
        )
        .unwrap();
        for result in [
            "published",
            "capacity_rejected",
            "obsolete",
            "error",
            "panic",
        ] {
            let _ = refill_outcomes.with_label_values(&[result]);
        }
        let io_failures = register_int_counter_vec_with_registry!(
            "pin_cache_io_failure_total",
            "Pin Cache refill I/O failures by phase",
            &["phase"],
            registry
        )
        .unwrap();
        let capacity_bytes = register_int_gauge_vec_with_registry!(
            "pin_cache_capacity_bytes",
            "Pin Cache capacity accounting by state",
            &["state"],
            registry
        )
        .unwrap();
        let published_objects = register_int_gauge_with_registry!(
            "pin_cache_published_objects",
            "SST objects currently routed to the local Pin Cache",
            registry
        )
        .unwrap();
        let published_bytes = register_int_gauge_with_registry!(
            "pin_cache_published_bytes",
            "Bytes in files currently routed to the local Pin Cache",
            registry
        )
        .unwrap();
        let recovery_ready = register_int_gauge_with_registry!(
            "pin_cache_recovery_ready",
            "Whether the local Pin Cache inventory scan completed successfully",
            registry
        )
        .unwrap();
        let recovery_failures = register_int_counter_with_registry!(
            "pin_cache_recovery_failure_total",
            "Pin Cache inventory scan failures",
            registry
        )
        .unwrap();
        let gc_failures = register_int_counter_with_registry!(
            "pin_cache_gc_failure_total",
            "Failed Pin Cache reclaim batches",
            registry
        )
        .unwrap();

        Self {
            io_failures,
            refill_outcomes,
            accounted_bytes: capacity_bytes.with_label_values(&["accounted"]),
            capacity_bytes: capacity_bytes.with_label_values(&["capacity"]),
            published_objects,
            published_bytes,
            recovery_ready,
            recovery_failures,
            gc_failures,
        }
    }
}
