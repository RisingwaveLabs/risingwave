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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Weak};
use std::time::Duration;

use anyhow::bail;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_udf_flight::Client;
use futures_util::{StreamExt, TryStreamExt};
use ginepro::{LoadBalancedChannel, ResolutionStrategy};
use risingwave_common::array::arrow::arrow_schema_udf::{self, Fields};
use risingwave_common::array::arrow::{UdfArrowConvert, UdfToArrow};
use risingwave_common::util::addr::HostAddr;
use thiserror_ext::AsReport;
use tokio::runtime::Runtime;

use super::*;

#[linkme::distributed_slice(UDF_IMPLS)]
static EXTERNAL: UdfImplDescriptor = UdfImplDescriptor {
    match_fn: |language, _runtime, link| {
        link.is_some() && matches!(language, "python" | "java" | "")
    },
    create_fn: |opts| {
        let link = opts.using_link.context("USING LINK must be specified")?;
        let name_in_runtime = opts.as_.context("AS must be specified")?.to_owned();

        // check UDF server
        let client = get_or_create_flight_client(link)?;
        let convert = UdfArrowConvert {
            legacy: client.protocol_version() == 1,
        };
        // A helper function to create a unnamed field from data type.
        let to_field = |data_type| convert.to_arrow_field("", data_type);
        let args = arrow_schema_udf::Schema::new(
            opts.arg_types
                .iter()
                .map(to_field)
                .try_collect::<Fields>()?,
        );
        let returns = arrow_schema_udf::Schema::new(if opts.kind.is_table() {
            vec![
                arrow_schema_udf::Field::new("row", arrow_schema_udf::DataType::Int32, true),
                to_field(opts.return_type)?,
            ]
        } else {
            vec![to_field(opts.return_type)?]
        });
        let function = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(client.get(&name_in_runtime))
        })
        .context("failed to check UDF signature")?;
        if !data_types_match(&function.args, &args) {
            bail!(
                "argument type mismatch, expect: {:?}, actual: {:?}",
                args,
                function.args,
            );
        }
        if !data_types_match(&function.returns, &returns) {
            bail!(
                "return type mismatch, expect: {:?}, actual: {:?}",
                returns,
                function.returns,
            );
        }
        Ok(CreateFunctionOutput {
            name_in_runtime,
            body: None,
            compressed_binary: None,
        })
    },
    build_fn: |opts| {
        let link = opts.link.context("link is required")?;
        let client = get_or_create_flight_client(link)?;
        Ok(Box::new(ExternalFunction {
            remote_name: opts.name_in_runtime.to_owned(),
            client,
            disable_retry_count: AtomicU8::new(INITIAL_RETRY_COUNT),
            always_retry_on_network_error: opts.always_retry_on_network_error,
        }))
    },
};

#[derive(Debug)]
struct ExternalFunction {
    remote_name: String,
    client: Arc<Client>,
    /// Number of remaining successful calls until retry is enabled.
    /// This parameter is designed to prevent continuous retry on every call, which would increase delay.
    /// Logic:
    /// It resets to `INITIAL_RETRY_COUNT` after a single failure and then decrements with each call, enabling retry when it reaches zero.
    /// If non-zero, we will not retry on connection errors to prevent blocking the stream.
    /// On each connection error, the count will be reset to `INITIAL_RETRY_COUNT`.
    /// On each successful call, the count will be decreased by 1.
    /// Link:
    /// See <https://github.com/risingwavelabs/risingwave/issues/13791>.
    disable_retry_count: AtomicU8,
    /// Always retry. Overrides `disable_retry_count`.
    always_retry_on_network_error: bool,
}

const INITIAL_RETRY_COUNT: u8 = 16;

#[async_trait::async_trait]
impl UdfImpl for ExternalFunction {
    fn is_legacy(&self) -> bool {
        // see <https://github.com/risingwavelabs/risingwave/pull/16619> for details
        self.client.protocol_version() == 1
    }

    async fn call(&self, input: &RecordBatch) -> Result<RecordBatch> {
        let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
        let result = if self.always_retry_on_network_error {
            self.call_with_always_retry_on_network_error(input).await
        } else {
            let result = if disable_retry_count != 0 {
                self.client.call(&self.remote_name, input).await
            } else {
                self.call_with_retry(input).await
            };
            let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
            let connection_error = matches!(&result, Err(e) if is_connection_error(e));
            if connection_error && disable_retry_count != INITIAL_RETRY_COUNT {
                // reset count on connection error
                self.disable_retry_count
                    .store(INITIAL_RETRY_COUNT, Ordering::Relaxed);
            } else if !connection_error && disable_retry_count != 0 {
                // decrease count on success, ignore if exchange failed
                _ = self.disable_retry_count.compare_exchange(
                    disable_retry_count,
                    disable_retry_count - 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                );
            }
            result
        };
        result.map_err(|e| e.into())
    }

    async fn call_table_function<'a>(
        &'a self,
        input: &'a RecordBatch,
    ) -> Result<BoxStream<'a, Result<RecordBatch>>> {
        let stream = self
            .client
            .call_table_function(&self.remote_name, input)
            .await?;
        Ok(stream.map_err(|e| e.into()).boxed())
    }
}

/// Get or create a client for the given UDF service.
///
/// There is a global cache for clients, so that we can reuse the same client for the same service.
fn get_or_create_flight_client(link: &str) -> Result<Arc<Client>> {
    static CLIENTS: LazyLock<std::sync::Mutex<HashMap<String, Weak<Client>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("rw-udf")
                .enable_all()
                .build()
                .expect("failed to build udf runtime")
        });
        // create new client
        let client = Arc::new(tokio::task::block_in_place(|| {
            RUNTIME.block_on(async {
                let channel = connect_tonic(link).await?;
                let client =
                    FlightServiceClient::new(channel).max_decoding_message_size(usize::MAX);
                Ok(Client::new(client).await?) as Result<_>
            })
        })?);
        clients.insert(link.to_owned(), Arc::downgrade(&client));
        Ok(client)
    }
}

/// Connect to a UDF service and return a tonic `Channel`.
async fn connect_tonic(mut addr: &str) -> Result<tonic::transport::Channel> {
    // Interval between two successive probes of the UDF DNS.
    const DNS_PROBE_INTERVAL_SECS: u64 = 5;
    // Timeout duration for performing an eager DNS resolution.
    const EAGER_DNS_RESOLVE_TIMEOUT_SECS: u64 = 5;
    const REQUEST_TIMEOUT_SECS: u64 = 5;
    const CONNECT_TIMEOUT_SECS: u64 = 5;

    if let Some(s) = addr.strip_prefix("http://") {
        addr = s;
    }
    if let Some(s) = addr.strip_prefix("https://") {
        addr = s;
    }
    let host_addr = addr.parse::<HostAddr>()?;
    let channel = LoadBalancedChannel::builder((host_addr.host.clone(), host_addr.port))
        .dns_probe_interval(std::time::Duration::from_secs(DNS_PROBE_INTERVAL_SECS))
        .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
        .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
        .resolution_strategy(ResolutionStrategy::Eager {
            timeout: tokio::time::Duration::from_secs(EAGER_DNS_RESOLVE_TIMEOUT_SECS),
        })
        .channel()
        .await
        .with_context(|| format!("failed to create LoadBalancedChannel, address: {host_addr}"))?;
    Ok(channel.into())
}

impl ExternalFunction {
    /// Call a function, retry up to 5 times / 3s if connection is broken.
    async fn call_with_retry(
        &self,
        input: &RecordBatch,
    ) -> Result<RecordBatch, arrow_udf_flight::Error> {
        let mut backoff = Duration::from_millis(100);
        for i in 0..5 {
            match self.client.call(&self.remote_name, input).await {
                Err(err) if is_connection_error(&err) && i != 4 => {
                    tracing::error!(?backoff, error = %err.as_report(), "UDF connection error. retry...");
                }
                ret => return ret,
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        unreachable!()
    }

    /// Always retry on connection error
    async fn call_with_always_retry_on_network_error(
        &self,
        input: &RecordBatch,
    ) -> Result<RecordBatch, arrow_udf_flight::Error> {
        let mut backoff = Duration::from_millis(100);
        loop {
            match self.client.call(&self.remote_name, input).await {
                Err(err) if is_tonic_error(&err) => {
                    tracing::error!(?backoff, error = %err.as_report(), "UDF tonic error. retry...");
                }
                ret => {
                    if ret.is_err() {
                        tracing::error!(error = %ret.as_ref().unwrap_err().as_report(), "UDF error. exiting...");
                    }
                    return ret;
                }
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
    }
}

/// Returns true if the arrow flight error is caused by a connection error.
fn is_connection_error(err: &arrow_udf_flight::Error) -> bool {
    match err {
        // Connection refused
        arrow_udf_flight::Error::Tonic(status) if status.code() == tonic::Code::Unavailable => true,
        _ => false,
    }
}

fn is_tonic_error(err: &arrow_udf_flight::Error) -> bool {
    matches!(
        err,
        arrow_udf_flight::Error::Tonic(_)
            | arrow_udf_flight::Error::Flight(arrow_flight::error::FlightError::Tonic(_))
    )
}

/// Check if two list of data types match, ignoring field names.
fn data_types_match(a: &arrow_schema_udf::Schema, b: &arrow_schema_udf::Schema) -> bool {
    if a.fields().len() != b.fields().len() {
        return false;
    }
    #[allow(clippy::disallowed_methods)]
    a.fields()
        .iter()
        .zip(b.fields())
        .all(|(a, b)| a.data_type().equals_datatype(b.data_type()))
}
