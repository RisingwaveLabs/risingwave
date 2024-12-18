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

use std::borrow::Cow;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

use either::Either;
use fastrace_opentelemetry::OpenTelemetryReporter;
use opentelemetry::trace::{SpanKind, TracerProvider};
use opentelemetry::InstrumentationLibrary;
use opentelemetry_sdk::Resource;
use risingwave_common::metrics::MetricsLayer;
use risingwave_common::util::deployment::Deployment;
use risingwave_common::util::env_var::env_var_is_true;
use risingwave_common::util::query_log::*;
use risingwave_common::util::tracing::layer::set_toggle_otel_layer_fn;
use thiserror_ext::AsReport;
use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::time::OffsetTime;
use tracing_subscriber::fmt::FormatFields;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, reload, EnvFilter};

pub struct LoggerSettings {
    /// The name of the service. Used to identify the service in distributed tracing.
    name: String,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
    /// Output to `stderr` instead of `stdout`.
    stderr: bool,
    /// Whether to include thread name in the log.
    with_thread_name: bool,
    /// Override target settings.
    targets: Vec<(String, tracing::metadata::LevelFilter)>,
    /// Override the default level.
    default_level: Option<tracing::metadata::LevelFilter>,
    /// The endpoint of the tracing collector in OTLP gRPC protocol.
    tracing_endpoint: Option<String>,
}

impl Default for LoggerSettings {
    fn default() -> Self {
        Self::new("risingwave")
    }
}

impl LoggerSettings {
    /// Create a new logger settings from the given command-line options.
    ///
    /// If env var `RW_TRACING_ENDPOINT` is not set, the meta address will be used
    /// as the default tracing endpoint, which means that the embedded tracing
    /// collector will be used.
    pub fn from_opts<O: risingwave_common::opts::Opts>(opts: &O) -> Self {
        let mut settings = Self::new(O::name());
        if settings.tracing_endpoint.is_none() // no explicit endpoint
            && let Some(addr) = opts.meta_addr().exactly_one()
        // meta address is valid
        {
            // Use embedded collector in the meta service.
            // TODO: when there's multiple meta nodes for high availability, we may send
            // to a wrong node here.
            settings.tracing_endpoint = Some(addr.to_string());
        }
        settings
    }

    /// Create a new logger settings with the given service name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enable_tokio_console: false,
            colorful: console::colors_enabled_stderr() && console::colors_enabled(),
            stderr: false,
            with_thread_name: false,
            targets: vec![],
            default_level: None,
            tracing_endpoint: std::env::var("RW_TRACING_ENDPOINT").ok(),
        }
    }

    /// Enable tokio console output.
    pub fn tokio_console(mut self, enabled: bool) -> Self {
        self.enable_tokio_console = enabled;
        self
    }

    /// Output to `stderr` instead of `stdout`.
    pub fn stderr(mut self, enabled: bool) -> Self {
        self.stderr = enabled;
        self
    }

    /// Whether to include thread name in the log.
    pub fn with_thread_name(mut self, enabled: bool) -> Self {
        self.with_thread_name = enabled;
        self
    }

    /// Overrides the default target settings.
    pub fn with_target(
        mut self,
        target: impl Into<String>,
        level: impl Into<tracing::metadata::LevelFilter>,
    ) -> Self {
        self.targets.push((target.into(), level.into()));
        self
    }

    /// Overrides the default level.
    pub fn with_default(mut self, level: impl Into<tracing::metadata::LevelFilter>) -> Self {
        self.default_level = Some(level.into());
        self
    }

    /// Overrides the tracing endpoint.
    pub fn with_tracing_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.tracing_endpoint = Some(endpoint.into());
        self
    }
}

/// Create a filter that disables all events or spans.
fn disabled_filter() -> filter::Targets {
    filter::Targets::new()
}

/// Init logger for RisingWave binaries.
///
/// ## Environment variables to configure logger dynamically
///
/// ### `RUST_LOG`
///
/// Overrides default level and tracing targets of the fmt layer (formatting and
/// logging to `stdout` or `stderr`).
///
/// Note that only verbosity levels below or equal to `DEBUG` are effective in
/// release builds.
///
/// e.g.,
/// ```bash
/// RUST_LOG="info,risingwave_stream=debug,events=debug"
/// ```
///
/// ### `RW_QUERY_LOG_PATH`
///
/// Configures the path to generate query log.
///
/// If it is set,
/// - Dump logs of all SQLs, i.e., tracing target [`PGWIRE_QUERY_LOG`] to
///   `RW_QUERY_LOG_PATH/query.log`.
/// - Dump slow queries, i.e., tracing target [`PGWIRE_SLOW_QUERY_LOG`] to
///   `RW_QUERY_LOG_PATH/slow_query.log`.
///
/// Note:
/// To enable query log in the fmt layer (slow query is included by default), set
/// ```bash
/// RUST_LOG="pgwire_query_log=info"
/// ```
///
/// `RW_QUERY_LOG_TRUNCATE_LEN` configures the max length of the SQLs logged in the query log,
/// to avoid the log file growing too large. The default value is 1024 in production.
///
/// ### `ENABLE_PRETTY_LOG`
///
/// If it is set to `true`, enable pretty log output, which contains line numbers and prints spans in multiple lines.
/// This can be helpful for development and debugging.
///
/// Hint: Also turn off other uninteresting logs to make the most of the pretty log.
/// e.g.,
/// ```bash
/// RUST_LOG="risingwave_storage::hummock::event_handler=off,batch_execute=off,risingwave_batch::task=off" ENABLE_PRETTY_LOG=true risedev d
/// ```
pub fn init_risingwave_logger(settings: LoggerSettings) {
    let deployment = Deployment::current();

    // Default timer for logging with local time offset.
    let default_timer = OffsetTime::local_rfc_3339().unwrap_or_else(|e| {
        println!(
            "failed to get local time offset, falling back to UTC: {}",
            e.as_report()
        );
        OffsetTime::new(
            time::UtcOffset::UTC,
            time::format_description::well_known::Rfc3339,
        )
    });

    // Default filter for logging to stdout and tracing.
    let default_filter = {
        let mut filter = filter::Targets::new();

        // Configure levels for some RisingWave crates.
        // Other RisingWave crates like `stream` and `storage` will follow the default level.
        filter = filter
            .with_target("auto_schema_change", Level::INFO)
            .with_target("risingwave_sqlparser", Level::INFO)
            .with_target("risingwave_connector_node", Level::INFO)
            .with_target("pgwire", Level::INFO)
            .with_target(PGWIRE_QUERY_LOG, Level::OFF)
            // debug-purposed events are disabled unless `RUST_LOG` overrides
            .with_target("events", Level::OFF);

        // Configure levels for external crates.
        filter = filter
            .with_target("foyer", Level::INFO)
            .with_target("aws", Level::INFO)
            .with_target("aws_config", Level::WARN)
            .with_target("aws_endpoint", Level::WARN)
            .with_target("aws_credential_types::cache::lazy_caching", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("tonic", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("console_subscriber", Level::WARN)
            .with_target("reqwest", Level::WARN)
            .with_target("sled", Level::INFO)
            .with_target("cranelift", Level::INFO)
            .with_target("wasmtime", Level::INFO)
            .with_target("sqlx", Level::WARN)
            .with_target("opendal", Level::INFO)
            .with_target("reqsign", Level::INFO);

        // For all other crates, apply default level depending on the deployment and `debug_assertions` flag.
        let default_level = match deployment {
            Deployment::Ci => Level::INFO,
            _ => {
                if cfg!(debug_assertions) {
                    Level::DEBUG
                } else {
                    Level::INFO
                }
            }
        };
        filter = filter.with_default(default_level);

        // Overrides from settings.
        filter = filter.with_targets(settings.targets);
        if let Some(default_level) = settings.default_level {
            filter = filter.with_default(default_level);
        }

        // Overrides from env var.
        if let Ok(rust_log) = std::env::var(EnvFilter::DEFAULT_ENV)
            && !rust_log.is_empty()
        {
            let rust_log_targets: Targets = rust_log.parse().expect("failed to parse `RUST_LOG`");
            if let Some(default_level) = rust_log_targets.default_level() {
                filter = filter.with_default(default_level);
            }
            filter = filter.with_targets(rust_log_targets)
        };

        filter
    };

    let mut layers = vec![];

    // fmt layer (formatting and logging to `stdout` or `stderr`)
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .with_thread_names(settings.with_thread_name)
            .with_timer(default_timer.clone())
            .with_ansi(settings.colorful)
            .with_writer(move || {
                if settings.stderr {
                    Either::Left(std::io::stderr())
                } else {
                    Either::Right(std::io::stdout())
                }
            });

        let fmt_layer = match deployment {
            // Deployment::Ci => fmt_layer
            //     .compact()
            //     .with_filter(FilterFn::new(|metadata| metadata.is_event())) // filter-out all span-related info
            //     .boxed(),
            Deployment::Cloud => fmt_layer
                .json()
                .map_event_format(|e| e.with_current_span(false)) // avoid duplication as there's a span list field
                .boxed(),
            Deployment::Ci | Deployment::Other => {
                if env_var_is_true("ENABLE_PRETTY_LOG") {
                    fmt_layer.pretty().boxed()
                } else {
                    fmt_layer.boxed()
                }
            }
        };

        layers.push(
            fmt_layer
                .with_filter(default_filter.clone().with_target("rw_tracing", Level::OFF)) // filter-out tracing-only events
                .boxed(),
        );
    };

    // If `RW_QUERY_LOG_PATH` env var is set to a directory, turn on query log files.
    let query_log_path = std::env::var("RW_QUERY_LOG_PATH");
    if let Ok(query_log_path) = query_log_path {
        let query_log_path = PathBuf::from(query_log_path);
        std::fs::create_dir_all(query_log_path.clone()).unwrap_or_else(|e| {
            panic!(
                "failed to create directory '{}' for query log: {}",
                query_log_path.display(),
                e.as_report(),
            )
        });

        /// Newtype wrapper for `DefaultFields`.
        ///
        /// `fmt::Layer` will share the same `FormattedFields` extension for spans across
        /// different layers, as long as the type of `N: FormatFields` is the same. This
        /// will cause several problems:
        ///
        /// - `with_ansi(false)` does not take effect and it will follow the settings of
        ///   the primary fmt layer installed above.
        /// - `Span::record` will update the same `FormattedFields` multiple times,
        ///   leading to duplicated fields.
        ///
        /// As a workaround, we use a newtype wrapper here to get a different type id.
        /// The const generic parameter `SLOW` is further used to distinguish between the
        /// query log and the slow query log.
        #[derive(Default)]
        struct FmtFields<const SLOW: bool>(DefaultFields);

        impl<'writer, const SLOW: bool> FormatFields<'writer> for FmtFields<SLOW> {
            fn format_fields<R: tracing_subscriber::field::RecordFields>(
                &self,
                writer: tracing_subscriber::fmt::format::Writer<'writer>,
                fields: R,
            ) -> std::fmt::Result {
                self.0.format_fields(writer, fields)
            }
        }

        for (file_name, target, is_slow) in [
            ("query.log", PGWIRE_QUERY_LOG, false),
            ("slow_query.log", PGWIRE_SLOW_QUERY_LOG, true),
        ] {
            let path = query_log_path.join(file_name);

            let file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .unwrap_or_else(|e| {
                    panic!("failed to create `{}`: {}", path.display(), e.as_report(),)
                });

            let layer = tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_level(false)
                .with_file(false)
                .with_target(false)
                .with_timer(default_timer.clone())
                .with_thread_names(true)
                .with_thread_ids(true)
                .with_writer(file);

            let layer = match is_slow {
                true => layer.fmt_fields(FmtFields::<true>::default()).boxed(),
                false => layer.fmt_fields(FmtFields::<false>::default()).boxed(),
            };

            let layer = layer.with_filter(
                filter::Targets::new()
                    // Root span must be enabled to provide common info like the SQL query.
                    .with_target(PGWIRE_ROOT_SPAN_TARGET, Level::INFO)
                    .with_target(target, Level::INFO),
            );

            layers.push(layer.boxed());
        }
    }

    if settings.enable_tokio_console {
        let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .build();
        let console_layer = console_layer.with_filter(
            filter::Targets::new()
                .with_target("tokio", Level::TRACE)
                .with_target("runtime", Level::TRACE),
        );
        layers.push(console_layer.boxed());
        std::thread::spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    println!("serving console subscriber");
                    server.serve().await.unwrap();
                });
        });
    };

    // Tracing layer
    #[cfg(not(madsim))]
    if let Some(endpoint) = settings.tracing_endpoint {
        println!("opentelemetry tracing will be exported to `{endpoint}` if enabled");

        use opentelemetry::KeyValue;
        use opentelemetry_otlp::WithExportConfig;
        use opentelemetry_sdk as sdk;
        use opentelemetry_semantic_conventions::resource;

        let id = format!(
            "{}-{}",
            hostname::get()
                .ok()
                .and_then(|o| o.into_string().ok())
                .unwrap_or_default(),
            std::process::id()
        );

        let (otel_tracer, exporter) = {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("rw-otel")
                .worker_threads(2)
                .build()
                .unwrap();
            let runtime = Box::leak(Box::new(runtime));

            // Installing the exporter requires a tokio runtime.
            let _entered = runtime.enter();

            // TODO(bugen): better service name
            // https://github.com/jaegertracing/jaeger-ui/issues/336
            let service_name = format!("{}-{}", settings.name, id);
            let otel_tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(&endpoint),
                )
                .with_trace_config(
                    sdk::trace::Config::default().with_resource(sdk::Resource::new([
                        KeyValue::new(resource::SERVICE_NAME, service_name.clone()),
                        KeyValue::new(resource::SERVICE_INSTANCE_ID, id.clone()),
                        KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                        KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
                    ])),
                )
                .install_batch(sdk::runtime::Tokio)
                .unwrap()
                .tracer(service_name);

            let exporter = opentelemetry_otlp::new_exporter()
                .tonic()
                .with_endpoint(&endpoint)
                .with_protocol(opentelemetry_otlp::Protocol::Grpc)
                .with_timeout(Duration::from_secs(
                    opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
                ))
                .build_span_exporter()
                .unwrap();

            (otel_tracer, exporter)
        };

        // Disable by filtering out all events or spans by default.
        //
        // It'll be enabled with `toggle_otel_layer` based on the system parameter `enable_tracing` later.
        let (reload_filter, reload_handle) = reload::Layer::new(disabled_filter());

        set_toggle_otel_layer_fn(move |enabled: bool| {
            let result = reload_handle.modify(|f| {
                *f = if enabled {
                    default_filter.clone()
                } else {
                    disabled_filter()
                }
            });

            match result {
                Ok(_) => tracing::info!(
                    "opentelemetry tracing {}",
                    if enabled { "enabled" } else { "disabled" },
                ),

                Err(error) => tracing::error!(
                    error = %error.as_report(),
                    "failed to {} opentelemetry tracing",
                    if enabled { "enable" } else { "disable" },
                ),
            }
        });

        let layer = tracing_opentelemetry::layer()
            .with_tracer(otel_tracer)
            .with_filter(reload_filter);

        layers.push(layer.boxed());

        // The reporter is used by fastrace in foyer for dynamically tail-based tracing.
        //
        // Code here only setup the OpenTelemetry reporter. To enable/disable the function, please use risectl.
        //
        // e.g.
        //
        // ```bash
        // risectl hummock tiered-cache-tracing -h
        // ```
        let reporter = OpenTelemetryReporter::new(
            exporter,
            SpanKind::Server,
            Cow::Owned(Resource::new([KeyValue::new(
                resource::SERVICE_NAME,
                format!("fastrace-{id}"),
            )])),
            InstrumentationLibrary::builder("opentelemetry-instrumentation-foyer").build(),
        );
        fastrace::set_reporter(reporter, fastrace::collector::Config::default());
        tracing::info!("opentelemetry exporter for fastrace is set at {endpoint}");
    }

    // Metrics layer
    {
        let filter = filter::Targets::new().with_target("aws_smithy_client::retry", Level::DEBUG);

        layers.push(Box::new(MetricsLayer::new().with_filter(filter)));
    }
    tracing_subscriber::registry().with(layers).init();
    // TODO: add file-appender tracing subscriber in the future
}
