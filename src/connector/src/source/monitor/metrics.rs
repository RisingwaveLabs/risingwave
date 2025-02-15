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

use std::sync::{Arc, LazyLock};

use prometheus::{exponential_buckets, histogram_opts, Registry};
use risingwave_common::metrics::{
    LabelGuardedHistogramVec, LabelGuardedIntCounterVec, LabelGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};

use crate::source::kafka::stats::RdKafkaStats;

#[derive(Debug, Clone)]
pub struct EnumeratorMetrics {
    pub high_watermark: LabelGuardedIntGaugeVec<2>,
}

pub static GLOBAL_ENUMERATOR_METRICS: LazyLock<EnumeratorMetrics> =
    LazyLock::new(|| EnumeratorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl EnumeratorMetrics {
    fn new(registry: &Registry) -> Self {
        let high_watermark = register_guarded_int_gauge_vec_with_registry!(
            "source_kafka_high_watermark",
            "High watermark for a exec per partition",
            &["source_id", "partition"],
            registry,
        )
        .unwrap();
        EnumeratorMetrics { high_watermark }
    }

    pub fn unused() -> Self {
        Default::default()
    }
}

impl Default for EnumeratorMetrics {
    fn default() -> Self {
        GLOBAL_ENUMERATOR_METRICS.clone()
    }
}

#[derive(Debug, Clone)]
pub struct SourceMetrics {
    pub partition_input_count: LabelGuardedIntCounterVec<5>,

    // **Note**: for normal messages, the metric is the message's payload size.
    // For messages from load generator, the metric is the size of stream chunk.
    pub partition_input_bytes: LabelGuardedIntCounterVec<5>,
    /// Report latest message id
    pub latest_message_id: LabelGuardedIntGaugeVec<3>,
    pub rdkafka_native_metric: Arc<RdKafkaStats>,

    pub direct_cdc_event_lag_latency: LabelGuardedHistogramVec<1>,

    pub parquet_source_skip_row_count: LabelGuardedIntCounterVec<5>,
}

pub static GLOBAL_SOURCE_METRICS: LazyLock<SourceMetrics> =
    LazyLock::new(|| SourceMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl SourceMetrics {
    fn new(registry: &Registry) -> Self {
        let partition_input_count = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_count",
            "Total number of rows that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap();
        let partition_input_bytes = register_guarded_int_counter_vec_with_registry!(
            "source_partition_input_bytes",
            "Total bytes that have been input from specific partition",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap();
        let latest_message_id = register_guarded_int_gauge_vec_with_registry!(
            "source_latest_message_id",
            "Latest message id for a exec per partition",
            &["source_id", "actor_id", "partition"],
            registry,
        )
        .unwrap();

        let opts = histogram_opts!(
            "source_cdc_event_lag_duration_milliseconds",
            "source_cdc_lag_latency",
            exponential_buckets(1.0, 2.0, 21).unwrap(), // max 1048s
        );

        let parquet_source_skip_row_count = register_guarded_int_counter_vec_with_registry!(
            "parquet_source_skip_row_count",
            "Total number of rows that have been set to null in parquet source",
            &[
                "actor_id",
                "source_id",
                "partition",
                "source_name",
                "fragment_id"
            ],
            registry
        )
        .unwrap();

        let direct_cdc_event_lag_latency =
            register_guarded_histogram_vec_with_registry!(opts, &["table_name"], registry).unwrap();

        let rdkafka_native_metric = Arc::new(RdKafkaStats::new(registry.clone()));
        SourceMetrics {
            partition_input_count,
            partition_input_bytes,
            latest_message_id,
            rdkafka_native_metric,
            direct_cdc_event_lag_latency,
            parquet_source_skip_row_count,
        }
    }
}

impl Default for SourceMetrics {
    fn default() -> Self {
        GLOBAL_SOURCE_METRICS.clone()
    }
}
