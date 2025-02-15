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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use risingwave_common::sequence::AtomicSequence;
use risingwave_stream::executor::monitor::StreamingMetrics;

use super::controller::LruWatermarkController;

pub struct MemoryManagerConfig {
    /// [`MemoryManager`] will try to control the jemalloc-reported memory usage
    /// to be lower than this
    pub target_memory: usize,

    pub threshold_aggressive: f64,
    pub threshold_graceful: f64,
    pub threshold_stable: f64,

    pub eviction_factor_stable: f64,
    pub eviction_factor_graceful: f64,
    pub eviction_factor_aggressive: f64,

    pub metrics: Arc<StreamingMetrics>,
}

/// Compute node uses [`MemoryManager`] to limit the memory usage.
pub struct MemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_sequence: Arc<AtomicSequence>,

    metrics: Arc<StreamingMetrics>,

    controller: Mutex<LruWatermarkController>,
}

impl MemoryManager {
    // Arbitrarily set a minimal barrier interval in case it is too small,
    // especially when it's 0.
    const MIN_INTERVAL: Duration = Duration::from_millis(10);

    pub fn new(config: MemoryManagerConfig) -> Arc<Self> {
        let controller = Mutex::new(LruWatermarkController::new(&config));
        tracing::info!("LRU watermark controller: {:?}", &controller);

        Arc::new(Self {
            watermark_sequence: Arc::new(0.into()),
            metrics: config.metrics,
            controller,
        })
    }

    pub fn get_watermark_sequence(&self) -> Arc<AtomicU64> {
        self.watermark_sequence.clone()
    }

    pub async fn run(self: Arc<Self>, interval: Duration) {
        // Loop interval of running control policy
        let interval = std::cmp::max(interval, Self::MIN_INTERVAL);
        tracing::info!("start running MemoryManager with interval {interval:?}",);

        // Keep same interval with the barrier interval
        let mut tick_interval = tokio::time::interval(interval);

        loop {
            tick_interval.tick().await;

            let new_watermark_sequence = self.controller.lock().unwrap().tick();

            self.watermark_sequence
                .store(new_watermark_sequence, Ordering::Relaxed);

            self.metrics.lru_runtime_loop_count.inc();
        }
    }
}
