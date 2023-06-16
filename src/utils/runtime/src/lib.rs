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

//! Configures the RisingWave binary, including logging, locks, panic handler, etc.

#![feature(panic_update_hook)]

use std::env;
use std::path::PathBuf;
use std::time::Duration;

use futures::Future;
use opentelemetry_otlp::WithExportConfig;
use risingwave_common::metrics::MetricsLayer;
use tracing::level_filters::LevelFilter as Level;
use tracing_subscriber::filter::{Directive, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{filter, EnvFilter};

// ============================================================================
// BEGIN SECTION: frequently used log configurations for debugging
// ============================================================================

/// Dump logs of all SQLs, i.e., tracing target `pgwire_query_log` to `.risingwave/log/query.log`.
///
/// Other ways to enable query log:
/// - Sets `RW_QUERY_LOG_PATH` to a directory.
/// - Changing the level of `pgwire` to `TRACE` in `configure_risingwave_targets_fmt` can also turn
///   on the logs, but without a dedicated file.
const ENABLE_QUERY_LOG_FILE: bool = false;
/// Use an [excessively pretty, human-readable formatter](tracing_subscriber::fmt::format::Pretty).
/// Includes line numbers for each log.
const ENABLE_PRETTY_LOG: bool = false;

const PGWIRE_QUERY_LOG: &str = "pgwire_query_log";

const SLOW_QUERY_LOG: &str = "risingwave_frontend_slow_query_log";

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets_fmt(targets: filter::Targets) -> filter::Targets {
    targets
        // enable trace for most modules
        .with_target("risingwave_stream", Level::DEBUG)
        .with_target("risingwave_batch", Level::INFO)
        .with_target("risingwave_storage", Level::DEBUG)
        .with_target("risingwave_sqlparser", Level::INFO)
        .with_target("risingwave_source", Level::INFO)
        .with_target("risingwave_connector", Level::INFO)
        .with_target("risingwave_frontend", Level::INFO)
        .with_target("risingwave_meta", Level::INFO)
        .with_target("risingwave_tracing", Level::INFO)
        .with_target("risingwave_compute", Level::INFO)
        .with_target("risingwave_compactor", Level::INFO)
        .with_target("risingwave_hummock_sdk", Level::INFO)
        .with_target("pgwire", Level::ERROR)
        // disable events that are too verbose
        // if you want to enable any of them, find the target name and set it to `TRACE`
        // .with_target("events::stream::mview::scan", Level::TRACE)
        .with_target("events", Level::ERROR)
}

// ===========================================================================
// END SECTION
// ===========================================================================

pub struct LoggerSettings {
    /// The name of the service.
    name: String,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
    /// Override default target settings.
    targets: Vec<(String, tracing::metadata::LevelFilter)>,
}

impl Default for LoggerSettings {
    fn default() -> Self {
        Self::new("risingwave")
    }
}

impl LoggerSettings {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            enable_tokio_console: false,
            colorful: console::colors_enabled_stderr() && console::colors_enabled(),
            targets: vec![],
        }
    }

    pub fn enable_tokio_console(mut self, enable: bool) -> Self {
        self.enable_tokio_console = enable;
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
}

/// Set panic hook to abort the process if we're not catching unwind, without losing the information
/// of stack trace and await-tree.
pub fn set_panic_hook() {
    std::panic::update_hook(|default_hook, info| {
        default_hook(info);

        if let Some(context) = await_tree::current_tree() {
            println!("\n\n*** await tree context of current task ***\n");
            println!("{}\n", context);
        }

        if !risingwave_common::util::panic::is_catching_unwind() {
            std::process::abort();
        }
    });
}

/// Init logger for RisingWave binaries.
///
/// Currently, the following env variables will be read:
///
/// * `RUST_LOG`: overrides default level and tracing targets. e.g.,
///   `RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info"`
/// * `RW_QUERY_LOG_PATH`: the path to generate query log. If set, [`ENABLE_QUERY_LOG_FILE`] is
///   turned on.
pub fn init_risingwave_logger(settings: LoggerSettings, registry: prometheus::Registry) {
    // Default filter for logging to stdout and tracing.
    let filter = {
        let filter = filter::Targets::new()
            .with_target("aws_sdk_ec2", Level::INFO)
            .with_target("aws_sdk_s3", Level::INFO)
            .with_target("aws_config", Level::WARN)
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("aws_endpoint", Level::WARN)
            .with_target("aws_credential_types::cache::lazy_caching", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("tonic", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("console_subscriber", Level::WARN)
            .with_target("reqwest", Level::WARN)
            .with_target("sled", Level::INFO);

        let filter = configure_risingwave_targets_fmt(filter);

        // For all other crates
        let filter = filter.with_default(if cfg!(debug_assertions) {
            Level::DEBUG
        } else {
            Level::INFO
        });

        let filter = settings
            .targets
            .into_iter()
            .fold(filter, |filter, (target, level)| {
                filter.with_target(target, level)
            });

        move |additional_targets: Vec<(&str, Level)>| {
            to_env_filter(filter.clone().with_targets(additional_targets))
        }
    };

    let mut layers = vec![];

    // fmt layer (formatting and logging to stdout)
    {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(settings.colorful);
        let fmt_layer = if ENABLE_PRETTY_LOG {
            fmt_layer.pretty().boxed()
        } else {
            fmt_layer.boxed()
        };

        layers.push(
            fmt_layer
                .with_filter(filter(vec![
                    ("rw_tracing", Level::OFF), // filter out tracing-only events
                ]))
                .boxed(),
        );
    };

    let default_query_log_path = "./".to_string();

    let query_log_path = std::env::var("RW_QUERY_LOG_PATH");
    if query_log_path.is_ok() || ENABLE_QUERY_LOG_FILE {
        let query_log_path = query_log_path.unwrap_or(default_query_log_path.clone());
        let query_log_path = PathBuf::from(query_log_path);
        std::fs::create_dir_all(query_log_path.clone()).unwrap_or_else(|e| {
            panic!(
                "failed to create directory '{}' for query log: {e}",
                query_log_path.display()
            )
        });
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(query_log_path.join("query.log"))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to create '{}/query.log': {e}",
                    query_log_path.display()
                )
            });
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_writer(std::sync::Mutex::new(file))
            .with_filter(filter::Targets::new().with_target(PGWIRE_QUERY_LOG, Level::TRACE));
        layers.push(layer.boxed());
    }

    // slow query log is always enabled
    {
        let slow_query_log_path = std::env::var("RW_QUERY_LOG_PATH");
        let slow_query_log_path = slow_query_log_path.unwrap_or(default_query_log_path);
        let slow_query_log_path = PathBuf::from(slow_query_log_path);

        std::fs::create_dir_all(slow_query_log_path.clone()).unwrap_or_else(|e| {
            panic!(
                "failed to create directory '{}' for slow query log: {e}",
                slow_query_log_path.display()
            )
        });
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(slow_query_log_path.join("slow_query.log"))
            .unwrap_or_else(|e| {
                panic!(
                    "failed to create '{}/slow_query.log': {e}",
                    slow_query_log_path.display()
                )
            });
        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_thread_names(true)
            .with_thread_ids(true)
            .with_writer(std::sync::Mutex::new(file))
            .with_filter(filter::Targets::new().with_target(SLOW_QUERY_LOG, Level::TRACE));
        layers.push(layer.boxed());
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
    if let Ok(endpoint) = std::env::var("RW_TRACING_ENDPOINT") {
        println!("tracing enabled, exported to `{endpoint}`");

        use opentelemetry::{sdk, KeyValue};
        use opentelemetry_semantic_conventions::resource;

        let id = format!(
            "{}-{}",
            hostname::get()
                .ok()
                .and_then(|o| o.into_string().ok())
                .unwrap_or_default(),
            std::process::id()
        );

        let otel_tracer = {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("risingwave-otel")
                .worker_threads(2)
                .build()
                .unwrap();
            let runtime = Box::leak(Box::new(runtime));

            // Installing the exporter requires a tokio runtime.
            let _entered = runtime.enter();

            opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint(endpoint),
                )
                .with_trace_config(sdk::trace::config().with_resource(sdk::Resource::new([
                    KeyValue::new(
                        resource::SERVICE_NAME,
                        // TODO(bugen): better service name
                        // https://github.com/jaegertracing/jaeger-ui/issues/336
                        format!("{}-{}", settings.name, id),
                    ),
                    KeyValue::new(resource::SERVICE_INSTANCE_ID, id),
                    KeyValue::new(resource::SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
                    KeyValue::new(resource::PROCESS_PID, std::process::id().to_string()),
                ])))
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap()
        };

        let layer = tracing_opentelemetry::layer()
            .with_tracer(otel_tracer)
            .with_filter(filter(vec![]));

        layers.push(layer.boxed());
    }

    // Metrics layer
    {
        let filter = filter::Targets::new().with_target("aws_smithy_client::retry", Level::DEBUG);

        layers.push(Box::new(MetricsLayer::new(registry).with_filter(filter)));
    }

    tracing_subscriber::registry().with(layers).init();

    // TODO: add file-appender tracing subscriber in the future
}

/// Returns a `EnvFilter` that
/// 1. inherits given `filter`'s target-LevelFilter pairs and default-LevelFilter.
/// 2. parses `RUST_LOG` environment variable and adds these filters.
///
/// Filters from step 1 will be overwritten by filters from step 2 that matches.
fn to_env_filter(filter: Targets) -> EnvFilter {
    let mut env_filter = EnvFilter::new("");
    for (target, level) in filter.iter() {
        let directive = format!("{}={}", target, level).parse().unwrap();
        env_filter = env_filter.add_directive(directive);
    }
    if let Some(g) = filter.default_level() {
        env_filter = env_filter.add_directive(g.into());
    }
    if let Ok(rust_log) = env::var(EnvFilter::DEFAULT_ENV) {
        if rust_log.is_empty() {
            return env_filter;
        }
        let directives = rust_log
            .split(',')
            .map(|s: &str| s.parse::<Directive>().expect("failed to parse RUST_LOG"));
        for directive in directives {
            env_filter = env_filter.add_directive(directive);
        }
    }
    env_filter
}

/// Enable parking lot's deadlock detection.
pub fn enable_parking_lot_deadlock_detection() {
    tracing::info!("parking lot deadlock detection enabled");
    // TODO: deadlock detection as a feature instead of always enabling
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(3));
        let deadlocks = parking_lot::deadlock::check_deadlock();
        if deadlocks.is_empty() {
            continue;
        }

        println!("{} deadlocks detected", deadlocks.len());
        for (i, threads) in deadlocks.iter().enumerate() {
            println!("Deadlock #{}", i);
            for t in threads {
                println!("Thread Id {:#?}", t.thread_id());
                println!("{:#?}", t.backtrace());
            }
        }
    });
}

fn spawn_prof_thread(profile_path: String) -> std::thread::JoinHandle<()> {
    tracing::info!("writing prof data to directory {}", profile_path);
    let profile_path = PathBuf::from(profile_path);

    std::fs::create_dir_all(&profile_path).unwrap();

    std::thread::spawn(move || {
        let mut cnt = 0;
        loop {
            let guard = pprof::ProfilerGuardBuilder::default()
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .unwrap();
            std::thread::sleep(Duration::from_secs(60));
            match guard.report().build() {
                Ok(report) => {
                    let profile_svg = profile_path.join(format!("{}.svg", cnt));
                    let file = std::fs::File::create(&profile_svg).unwrap();
                    report.flamegraph(file).unwrap();
                    tracing::info!("produced {:?}", profile_svg);
                }
                Err(err) => {
                    tracing::warn!("failed to generate flamegraph: {}", err);
                }
            }
            cnt += 1;
        }
    })
}

/// Start RisingWave components with configs from environment variable.
///
/// Currently, the following env variables will be read:
///
/// * `RW_WORKER_THREADS`: number of tokio worker threads. If not set, it will use tokio's default
///   config (equivalent to CPU cores). Note: This will not effect the dedicated runtimes for each
///   service which are controlled by their own configurations, like streaming actors, compactions,
///   etc.
/// * `RW_DEADLOCK_DETECTION`: whether to enable deadlock detection. If not set, will enable in
///   debug mode, and disable in release mode.
/// * `RW_PROFILE_PATH`: the path to generate flamegraph. If set, then profiling is automatically
///   enabled.
pub fn main_okk<F>(f: F) -> F::Output
where
    F: Future + Send + 'static,
{
    set_panic_hook();

    let mut builder = tokio::runtime::Builder::new_multi_thread();

    if let Ok(worker_threads) = std::env::var("RW_WORKER_THREADS") {
        let worker_threads = worker_threads.parse().unwrap();
        tracing::info!("setting tokio worker threads to {}", worker_threads);
        builder.worker_threads(worker_threads);
    }

    if let Ok(enable_deadlock_detection) = std::env::var("RW_DEADLOCK_DETECTION") {
        let enable_deadlock_detection = enable_deadlock_detection
            .parse()
            .expect("Failed to parse RW_DEADLOCK_DETECTION");
        if enable_deadlock_detection {
            enable_parking_lot_deadlock_detection();
        }
    } else {
        // In case the env variable is not set
        if cfg!(debug_assertions) {
            enable_parking_lot_deadlock_detection();
        }
    }

    if let Ok(profile_path) = std::env::var("RW_PROFILE_PATH") {
        spawn_prof_thread(profile_path);
    }

    builder
        .thread_name("risingwave-main")
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}
