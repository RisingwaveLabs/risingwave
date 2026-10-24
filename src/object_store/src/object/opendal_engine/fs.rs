// Copyright 2023 RisingWave Labs
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

use std::sync::{Arc, OnceLock};

use opendal::Operator;
use opendal::layers::{LoggingLayer, PrometheusLayer};
use opendal::services::Fs;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

use super::{MediaType, OpendalObjectStore, new_operator};
use crate::object::ObjectResult;
use crate::object::object_metrics::ObjectStoreMetrics;
use crate::object::opendal_engine::ATOMIC_WRITE_DIR;

fn prometheus_layer() -> &'static PrometheusLayer {
    static LAYER: OnceLock<PrometheusLayer> = OnceLock::new();
    LAYER.get_or_init(|| {
        PrometheusLayer::builder()
            .register(&GLOBAL_METRICS_REGISTRY)
            .expect("OpenDAL Prometheus metrics must be registered once")
    })
}

impl OpendalObjectStore {
    /// create opendal fs engine.
    pub fn new_fs_engine(
        root: String,
        config: Arc<ObjectStoreConfig>,
        metrics: Arc<ObjectStoreMetrics>,
    ) -> ObjectResult<Self> {
        // Create fs backend builder.
        let mut builder = Fs::default().root(&root);
        if config.set_atomic_write_dir {
            let atomic_write_dir = format!("{}/{}", root, ATOMIC_WRITE_DIR);
            builder = builder.atomic_write_dir(&atomic_write_dir);
        }

        let op = new_operator(
            &config,
            Operator::new(builder)?
                .layer(prometheus_layer().clone())
                .layer(LoggingLayer::default()),
        );

        Ok(Self {
            op,
            media_type: MediaType::Fs,
            config,
            metrics,
        })
    }
}
