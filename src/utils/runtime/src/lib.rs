// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configures the RisingWave binary, including logging, locks, panic handler, etc.

#![feature(let_chains)]

mod trace_runtime;

use std::path::PathBuf;
use std::time::Duration;

use futures::Future;
use tracing::Level;
use tracing_subscriber::filter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::prelude::*;

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets_jaeger(targets: filter::Targets) -> filter::Targets {
    targets
        // enable trace for most modules
        .with_target("risingwave_stream", Level::TRACE)
        .with_target("risingwave_batch", Level::TRACE)
        .with_target("risingwave_storage", Level::TRACE)
        .with_target("risingwave_sqlparser", Level::INFO)
        // disable events that are too verbose
        // if you want to enable any of them, find the target name and set it to `TRACE`
        // .with_target("events::stream::mview::scan", Level::TRACE)
        .with_target("events", Level::ERROR)
}

/// Configure log targets for all `RisingWave` crates. When new crates are added and TRACE level
/// logs are needed, add them here.
fn configure_risingwave_targets_fmt(targets: filter::Targets) -> filter::Targets {
    targets
        // enable trace for most modules
        .with_target("risingwave_stream", Level::DEBUG)
        .with_target("risingwave_batch", Level::DEBUG)
        .with_target("risingwave_storage", Level::DEBUG)
        .with_target("risingwave_sqlparser", Level::INFO)
        .with_target("risingwave_source", Level::INFO)
        .with_target("risingwave_connector", Level::INFO)
        .with_target("risingwave_frontend", Level::INFO)
        .with_target("risingwave_meta", Level::INFO)
        .with_target("pgwire", Level::ERROR)
        // disable events that are too verbose
        // if you want to enable any of them, find the target name and set it to `TRACE`
        // .with_target("events::stream::mview::scan", Level::TRACE)
        .with_target("events", Level::ERROR)

    // if env_var_is_true("RW_CI") {
    //     targets.with_target("events::meta::server_heartbeat", Level::TRACE)
    // } else {
    //     targets
    // }
}

pub struct LoggerSettings {
    /// Enable Jaeger tracing.
    enable_jaeger_tracing: bool,
    /// Enable tokio console output.
    enable_tokio_console: bool,
    /// Enable colorful output in console.
    colorful: bool,
}

impl LoggerSettings {
    pub fn new_default() -> Self {
        Self::new(false, false)
    }

    pub fn new(enable_jaeger_tracing: bool, enable_tokio_console: bool) -> Self {
        Self {
            enable_jaeger_tracing,
            enable_tokio_console,
            colorful: console::colors_enabled_stderr(),
        }
    }
}

/// Set panic hook to abort the process (without losing debug info and stack trace).
pub fn set_panic_abort() {
    use std::panic;

    let default_hook = panic::take_hook();

    panic::set_hook(Box::new(move |info| {
        default_hook(info);
        std::process::abort();
    }));
}

/// Init logger for RisingWave binaries.
pub fn init_risingwave_logger(settings: LoggerSettings) {
    use isahc::config::Configurable;

    let fmt_layer = {
        // Configure log output to stdout
        let fmt_layer = tracing_subscriber::fmt::layer()
            .compact()
            .with_ansi(settings.colorful);

        let filter = filter::Targets::new()
            // Only enable WARN and ERROR for 3rd-party crates
            .with_target("aws_endpoint", Level::WARN)
            .with_target("hyper", Level::WARN)
            .with_target("h2", Level::WARN)
            .with_target("tower", Level::WARN)
            .with_target("isahc", Level::WARN)
            .with_target("console_subscriber", Level::WARN);

        // Configure RisingWave's own crates to log at TRACE level, uncomment the following line if
        // needed.

        let filter = configure_risingwave_targets_fmt(filter);

        // Enable DEBUG level for all other crates
        // TODO: remove this in release mode
        let filter = filter.with_default(Level::DEBUG);

        fmt_layer.with_filter(filter)
    };

    let opentelemetry_layer = if settings.enable_jaeger_tracing {
        // With Jaeger tracing enabled, we should configure opentelemetry endpoints.

        opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            // TODO: use UDP tracing in production environment
            .with_collector_endpoint("http://127.0.0.1:14268/api/traces")
            // TODO: change service name to compute-{port}
            .with_service_name("compute")
            // disable proxy
            .with_http_client(isahc::HttpClient::builder().proxy(None).build().unwrap())
            .install_batch(trace_runtime::RwTokio)
            .unwrap();

        let opentelemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Configure RisingWave's own crates to log at TRACE level, and ignore all third-party
        // crates
        let filter = filter::Targets::new();
        let filter = configure_risingwave_targets_jaeger(filter);

        Some(opentelemetry_layer.with_filter(filter))
    } else {
        None
    };

    let tokio_console_layer = if settings.enable_tokio_console {
        let (console_layer, server) = console_subscriber::ConsoleLayer::builder()
            .with_default_env()
            .build();
        let console_layer = console_layer.with_filter(
            filter::Targets::new()
                .with_target("tokio", Level::TRACE)
                .with_target("runtime", Level::TRACE),
        );
        Some((console_layer, server))
    } else {
        None
    };

    match (opentelemetry_layer, tokio_console_layer) {
        (Some(_), Some(_)) => {
            // tracing_subscriber::registry()
            //     .with(fmt_layer)
            //     .with(opentelemetry_layer)
            //     .with(tokio_console_layer)
            //     .init();
            // Strange compiler bug is preventing us from enabling both of them...
            panic!("cannot enable opentelemetry layer and tokio console layer at the same time");
        }
        (Some(opentelemetry_layer), None) => {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(opentelemetry_layer)
                .init();
        }
        (None, Some((tokio_console_layer, server))) => {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(tokio_console_layer)
                .init();
            std::thread::spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        tracing::info!("serving console subscriber");
                        server.serve().await.unwrap();
                    });
            });
        }
        (None, None) => {
            tracing_subscriber::registry().with(fmt_layer).init();
        }
    }

    // TODO: add file-appender tracing subscriber in the future
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
///   config (equivalent to CPU cores).
/// * `RW_DEADLOCK_DETECTION`: whether to enable deadlock detection. If not set, will enable in
///   debug mode, and disable in release mode.
/// * `RW_PROFILE_PATH`: the path to generate flamegraph. If set, then profiling is automatically
///   enabled.
pub fn main_okk<F>(f: F) -> F::Output
where
    F: Future + Send + 'static,
{
    set_panic_abort();

    let mut builder = tokio::runtime::Builder::new_multi_thread();

    if let Ok(worker_threads) = std::env::var("RW_WORKER_THREADS") {
        let worker_threads = worker_threads.parse().unwrap();
        tracing::info!("setting tokio worker threads to {}", worker_threads);
        builder.worker_threads(worker_threads);
    }

    if let Ok(enable_deadlock_detection) = std::env::var("RW_DEADLOCK_DETECTION") {
        let enable_deadlock_detection = enable_deadlock_detection.parse().unwrap();
        if enable_deadlock_detection {
            enable_parking_lot_deadlock_detection();
        }
    } else {
        // In case the env variable is not set
        if cfg!(debug_assertion) {
            enable_parking_lot_deadlock_detection();
        }
    }

    if let Ok(profile_path) = std::env::var("RW_PROFILE_PATH") {
        spawn_prof_thread(profile_path);
    }

    builder.enable_all().build().unwrap().block_on(f)
}
