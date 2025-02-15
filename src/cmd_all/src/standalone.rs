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

use std::env;
use std::fmt::Write;
use std::future::Future;
use std::path::Path;

use clap::Parser;
use risingwave_common::config::MetaBackend;
use risingwave_common::util::env_var::env_var_is_true;
use risingwave_common::util::meta_addr::MetaAddressStrategy;
use risingwave_common::util::runtime::BackgroundShutdownRuntime;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use risingwave_compactor::CompactorOpts;
use risingwave_compute::ComputeNodeOpts;
use risingwave_frontend::FrontendOpts;
use risingwave_meta_node::MetaNodeOpts;
use shell_words::split;

use crate::common::osstrs;

#[derive(Eq, PartialOrd, PartialEq, Debug, Clone, Parser)]
#[command(
    version,
    about = "The Standalone mode allows users to start multiple services in one process, it exposes node-level options for each service",
    hide = true
)]
pub struct StandaloneOpts {
    /// Compute node options
    /// If missing, compute node won't start
    #[clap(short, long, env = "RW_STANDALONE_COMPUTE_OPTS")]
    compute_opts: Option<String>,

    #[clap(short, long, env = "RW_STANDALONE_META_OPTS")]
    /// Meta node options
    /// If missing, meta node won't start
    meta_opts: Option<String>,

    #[clap(short, long, env = "RW_STANDALONE_FRONTEND_OPTS")]
    /// Frontend node options
    /// If missing, frontend node won't start
    frontend_opts: Option<String>,

    #[clap(long, env = "RW_STANDALONE_COMPACTOR_OPTS")]
    /// Compactor node options
    /// If missing compactor node won't start
    compactor_opts: Option<String>,

    #[clap(long, env = "RW_STANDALONE_PROMETHEUS_LISTENER_ADDR")]
    /// Prometheus listener address
    /// If present, it will override prometheus listener address for
    /// Frontend, Compute and Compactor nodes
    prometheus_listener_addr: Option<String>,

    #[clap(long, env = "RW_STANDALONE_CONFIG_PATH")]
    /// Path to the config file
    /// If present, it will override config path for
    /// Frontend, Compute and Compactor nodes
    config_path: Option<String>,
}

#[derive(Debug)]
pub struct ParsedStandaloneOpts {
    pub meta_opts: Option<MetaNodeOpts>,
    pub compute_opts: Option<ComputeNodeOpts>,
    pub frontend_opts: Option<FrontendOpts>,
    pub compactor_opts: Option<CompactorOpts>,
}

impl risingwave_common::opts::Opts for ParsedStandaloneOpts {
    fn name() -> &'static str {
        "standalone"
    }

    fn meta_addr(&self) -> MetaAddressStrategy {
        if let Some(opts) = self.meta_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.compute_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.frontend_opts.as_ref() {
            opts.meta_addr()
        } else if let Some(opts) = self.compactor_opts.as_ref() {
            opts.meta_addr()
        } else {
            unreachable!("at least one service should be specified as checked during parsing")
        }
    }
}

pub fn parse_standalone_opt_args(opts: &StandaloneOpts) -> ParsedStandaloneOpts {
    let meta_opts = opts.meta_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "meta-node".into());
        s
    });
    let mut meta_opts = meta_opts.map(|o| MetaNodeOpts::parse_from(osstrs(o)));

    let compute_opts = opts.compute_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compute-node".into());
        s
    });
    let mut compute_opts = compute_opts.map(|o| ComputeNodeOpts::parse_from(osstrs(o)));

    let frontend_opts = opts.frontend_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "frontend-node".into());
        s
    });
    let mut frontend_opts = frontend_opts.map(|o| FrontendOpts::parse_from(osstrs(o)));

    let compactor_opts = opts.compactor_opts.as_ref().map(|s| {
        let mut s = split(s).unwrap();
        s.insert(0, "compactor-node".into());
        s
    });
    let mut compactor_opts = compactor_opts.map(|o| CompactorOpts::parse_from(osstrs(o)));

    if let Some(config_path) = opts.config_path.as_ref() {
        if let Some(meta_opts) = meta_opts.as_mut() {
            meta_opts.config_path.clone_from(config_path);
        }
        if let Some(compute_opts) = compute_opts.as_mut() {
            compute_opts.config_path.clone_from(config_path);
        }
        if let Some(frontend_opts) = frontend_opts.as_mut() {
            frontend_opts.config_path.clone_from(config_path);
        }
        if let Some(compactor_opts) = compactor_opts.as_mut() {
            compactor_opts.config_path.clone_from(config_path);
        }
    }
    if let Some(prometheus_listener_addr) = opts.prometheus_listener_addr.as_ref() {
        if let Some(compute_opts) = compute_opts.as_mut() {
            compute_opts
                .prometheus_listener_addr
                .clone_from(prometheus_listener_addr);
        }
        if let Some(frontend_opts) = frontend_opts.as_mut() {
            frontend_opts
                .prometheus_listener_addr
                .clone_from(prometheus_listener_addr);
        }
        if let Some(compactor_opts) = compactor_opts.as_mut() {
            compactor_opts
                .prometheus_listener_addr
                .clone_from(prometheus_listener_addr);
        }
        if let Some(meta_opts) = meta_opts.as_mut() {
            meta_opts.prometheus_listener_addr = Some(prometheus_listener_addr.clone());
        }
    }

    if meta_opts.is_none()
        && compute_opts.is_none()
        && frontend_opts.is_none()
        && compactor_opts.is_none()
    {
        panic!("No service is specified to start.");
    }

    ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }
}

/// A service under standalone mode.
struct Service {
    name: &'static str,
    runtime: BackgroundShutdownRuntime,
    main_task: tokio::task::JoinHandle<()>,
    shutdown: CancellationToken,
}

impl Service {
    /// Spawn a new tokio runtime and start a service in it.
    ///
    /// By using a separate runtime, we get better isolation between services. For example,
    ///
    /// - The logs in the main runtime of each service can be distinguished by the thread name.
    /// - Each service can be shutdown cleanly by shutting down its runtime.
    fn spawn<F, Fut>(name: &'static str, f: F) -> Self
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name(format!("rw-standalone-{name}"))
            .enable_all()
            .build()
            .unwrap();
        let shutdown = CancellationToken::new();
        let main_task = runtime.spawn(f(shutdown.clone()));

        Self {
            name,
            runtime: runtime.into(),
            main_task,
            shutdown,
        }
    }

    /// Shutdown the service and the runtime gracefully.
    ///
    /// As long as the main task of the service is resolved after signaling `shutdown`,
    /// the service is considered stopped and the runtime will be shutdown. This follows
    /// the same convention as described in `risingwave_rt::main_okk`.
    async fn shutdown(self) {
        tracing::info!("stopping {} service...", self.name);

        self.shutdown.cancel();
        let _ = self.main_task.await;
        drop(self.runtime); // shutdown in background

        tracing::info!("{} service stopped", self.name);
    }
}

/// For `standalone` mode, we can configure and start multiple services in one process.
/// `standalone` mode is meant to be used by our cloud service and docker,
/// where we can configure and start multiple services in one process.
///
/// Services are started in the order of `meta`, `compute`, `frontend`, then `compactor`.
/// When the `shutdown` token is signaled, all services will be stopped gracefully in the
/// reverse order.
pub async fn standalone(
    ParsedStandaloneOpts {
        meta_opts,
        compute_opts,
        frontend_opts,
        compactor_opts,
    }: ParsedStandaloneOpts,
    shutdown: CancellationToken,
) {
    tracing::info!("launching Risingwave in standalone mode");

    let (meta, is_in_memory) = if let Some(opts) = meta_opts.clone() {
        let is_in_memory = matches!(opts.backend, Some(MetaBackend::Mem));
        tracing::info!("starting meta-node thread with cli args: {:?}", opts);
        let service = Service::spawn("meta", |shutdown| {
            risingwave_meta_node::start(opts, shutdown)
        });

        // wait for the service to be ready
        let mut tries = 0;
        while !risingwave_meta_node::is_server_started() {
            if tries % 50 == 0 {
                tracing::info!("waiting for meta service to be ready...");
            }
            if service.main_task.is_finished() {
                tracing::error!("meta service failed to start, exiting...");
                return;
            }
            tries += 1;
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        (Some(service), is_in_memory)
    } else {
        (None, false)
    };

    let compute = if let Some(opts) = compute_opts {
        tracing::info!("starting compute-node thread with cli args: {:?}", opts);
        let service = Service::spawn("compute", |shutdown| {
            risingwave_compute::start(opts, shutdown)
        });
        Some(service)
    } else {
        None
    };

    let frontend = if let Some(opts) = frontend_opts.clone() {
        tracing::info!("starting frontend-node thread with cli args: {:?}", opts);
        let service = Service::spawn("frontend", |shutdown| {
            risingwave_frontend::start(opts, shutdown)
        });
        Some(service)
    } else {
        None
    };

    let compactor = if let Some(opts) = compactor_opts {
        tracing::info!("starting compactor-node thread with cli args: {:?}", opts);
        let service = Service::spawn("compactor", |shutdown| {
            risingwave_compactor::start(opts, shutdown)
        });
        Some(service)
    } else {
        None
    };

    // wait for log messages to be flushed
    tokio::time::sleep(std::time::Duration::from_millis(5000)).await;

    eprintln!("----------------------------------------");
    eprintln!("| RisingWave standalone mode is ready. |");
    eprintln!("----------------------------------------");
    if is_in_memory {
        eprintln!(
            "{}",
            console::style(
                "WARNING: You are using RisingWave's in-memory mode.
It SHOULD NEVER be used in benchmarks and production environment!!!"
            )
            .red()
            .bold()
        );
    }

    // This is a specialization of `generate_risedev_env` in `src/risedevtool/src/risedev_env.rs`.
    let mut risedev_env = String::new();

    if let Some(opts) = &frontend_opts {
        let host = opts.listen_addr.split(':').next().unwrap_or("localhost");
        let port = opts.listen_addr.split(':').last().unwrap_or("4566");
        let database = "dev";
        let user = "root";

        writeln!(
            risedev_env,
            r#"RISEDEV_RW_FRONTEND_LISTEN_ADDRESS="{host}""#
        )
        .unwrap();
        writeln!(risedev_env, r#"RISEDEV_RW_FRONTEND_PORT="{port}""#).unwrap();

        eprintln!();
        eprintln!("Connect to the RisingWave instance via psql:");
        eprintln!(
            "{}",
            console::style(format!(
                "  psql -h {host} -p {port} -d {database} -U {user}"
            ))
            .blue()
        );
    }

    if let Some(opts) = &meta_opts {
        let meta_addr = &opts.listen_addr;
        writeln!(risedev_env, r#"RW_META_ADDR="http://{meta_addr}""#).unwrap();
    }

    // Create the environment file when launched by RiseDev.
    if env_var_is_true("RISEDEV") {
        let env_path = Path::new(
            &env::var("PREFIX_CONFIG").expect("env var `PREFIX_CONFIG` must be set by RiseDev"),
        )
        .join("risedev-env");
        std::fs::write(env_path, risedev_env).unwrap();
    }

    let meta_stopped = meta
        .as_ref()
        .map(|m| m.shutdown.clone())
        // If there's no meta service, use a dummy token which will never resolve.
        .unwrap_or_else(CancellationToken::new)
        .cancelled_owned();

    // Wait for shutdown signals.
    tokio::select! {
        // Meta service stopped itself, typically due to leadership loss of idleness.
        // Directly exit in this case.
        _ = meta_stopped => {
            tracing::info!("meta service is stopped, terminating...");
        }

        // Shutdown requested by the user.
        _ = shutdown.cancelled() => {
            for service in [compactor, frontend, compute, meta].into_iter().flatten() {
                service.shutdown().await;
            }
            tracing::info!("all services stopped, bye");
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[allow(clippy::assertions_on_constants)]
    #[test]
    fn test_parse_opt_args() {
        // Test parsing into standalone-level opts.
        let raw_opts = "
--compute-opts=--listen-addr 127.0.0.1:8000 --total-memory-bytes 34359738368 --parallelism 10 --temp-secret-file-dir ./compute/secrets/
--meta-opts=--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\" --listen-addr 127.0.0.1:8001 --temp-secret-file-dir ./meta/secrets/
--frontend-opts=--config-path=src/config/original.toml --temp-secret-file-dir ./frontend/secrets/ --frontend-total-memory-bytes=34359738368
--prometheus-listener-addr=127.0.0.1:1234
--config-path=src/config/test.toml
";
        let actual = StandaloneOpts::parse_from(raw_opts.lines());
        let opts = StandaloneOpts {
            compute_opts: Some("--listen-addr 127.0.0.1:8000 --total-memory-bytes 34359738368 --parallelism 10 --temp-secret-file-dir ./compute/secrets/".into()),
            meta_opts: Some("--advertise-addr 127.0.0.1:9999 --data-directory \"some path with spaces\" --listen-addr 127.0.0.1:8001 --temp-secret-file-dir ./meta/secrets/".into()),
            frontend_opts: Some("--config-path=src/config/original.toml --temp-secret-file-dir ./frontend/secrets/ --frontend-total-memory-bytes=34359738368".into() ),
            compactor_opts: None,
            prometheus_listener_addr: Some("127.0.0.1:1234".into()),
            config_path: Some("src/config/test.toml".into()),
        };
        assert_eq!(actual, opts);

        // Test parsing into node-level opts.
        let actual = parse_standalone_opt_args(&opts);

        if let Some(compute_opts) = &actual.compute_opts {
            assert_eq!(compute_opts.listen_addr, "127.0.0.1:8000");
            assert_eq!(compute_opts.total_memory_bytes, 34359738368);
            assert_eq!(compute_opts.parallelism, 10);
            assert_eq!(compute_opts.temp_secret_file_dir, "./compute/secrets/");
            assert_eq!(compute_opts.prometheus_listener_addr, "127.0.0.1:1234");
            assert_eq!(compute_opts.config_path, "src/config/test.toml");
        } else {
            assert!(false);
        }
        if let Some(meta_opts) = &actual.meta_opts {
            assert_eq!(meta_opts.listen_addr, "127.0.0.1:8001");
            assert_eq!(meta_opts.advertise_addr, "127.0.0.1:9999");
            assert_eq!(
                meta_opts.data_directory,
                Some("some path with spaces".to_string())
            );
            assert_eq!(meta_opts.temp_secret_file_dir, "./meta/secrets/");
            assert_eq!(
                meta_opts.prometheus_listener_addr,
                Some("127.0.0.1:1234".to_string())
            );
            assert_eq!(meta_opts.config_path, "src/config/test.toml");
        } else {
            assert!(false);
        }

        if let Some(frontend_opts) = &actual.frontend_opts {
            assert_eq!(frontend_opts.config_path, "src/config/test.toml");
            assert_eq!(frontend_opts.temp_secret_file_dir, "./frontend/secrets/");
            assert_eq!(frontend_opts.frontend_total_memory_bytes, 34359738368);
            assert_eq!(frontend_opts.prometheus_listener_addr, "127.0.0.1:1234");
        } else {
            assert!(false);
        }

        assert!(actual.compactor_opts.is_none());
    }
}
