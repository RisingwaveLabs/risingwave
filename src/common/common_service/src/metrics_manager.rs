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

use std::ops::Deref;
use std::sync::OnceLock;

use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Router};
use prometheus::{Encoder, Registry, TextEncoder};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use thiserror_ext::AsReport;
use tokio::net::TcpListener;
use tower_http::add_extension::AddExtensionLayer;
use tracing::{error, info, warn};

pub struct MetricsManager {}

impl MetricsManager {
    pub fn boot_metrics_service(listen_addr: String) {
        static METRICS_SERVICE_LISTEN_ADDR: OnceLock<String> = OnceLock::new();
        let new_listen_addr = listen_addr.clone();
        let current_listen_addr =
            METRICS_SERVICE_LISTEN_ADDR.get_or_init(|| {
                let listen_addr_clone = listen_addr.clone();
                #[cfg(not(madsim))] // no need in simulation test
                tokio::spawn(async move {
                    info!(
                        "Prometheus listener for Prometheus is set up on http://{}",
                        listen_addr
                    );

                    let service = Router::new().fallback(Self::metrics_service).layer(
                        AddExtensionLayer::new(GLOBAL_METRICS_REGISTRY.deref().clone()),
                    );

                    let serve_future =
                        axum::serve(TcpListener::bind(&listen_addr).await.unwrap(), service);
                    if let Err(err) = serve_future.await {
                        error!(error = %err.as_report(), "metrics service exited with error");
                    }
                });
                listen_addr_clone
            });
        if new_listen_addr != *current_listen_addr {
            warn!(
                "unable to listen port {} for metrics service. Currently listening on {}",
                new_listen_addr, current_listen_addr
            );
        }
    }

    #[expect(clippy::unused_async, reason = "required by service_fn")]
    async fn metrics_service(Extension(registry): Extension<Registry>) -> impl IntoResponse {
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        let mf = registry.gather();
        encoder.encode(&mf, &mut buffer).unwrap();

        Response::builder()
            .header(axum::http::header::CONTENT_TYPE, encoder.format_type())
            .body(Body::from(buffer))
            .unwrap()
    }
}
