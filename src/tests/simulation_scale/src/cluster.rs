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

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Parser;
use futures::future::BoxFuture;
use madsim::rand::thread_rng;
use madsim::runtime::{Handle, NodeHandle};
use rand::seq::SliceRandom;

use crate::RisingWave;

#[derive(Debug, Parser)]
pub struct Configuration {
    /// The number of frontend nodes.
    #[clap(long, default_value = "1")]
    frontend_nodes: usize,

    /// The number of compute nodes.
    #[clap(long, default_value = "3")]
    compute_nodes: usize,

    /// The number of compactor nodes.
    #[clap(long, default_value = "1")]
    compactor_nodes: usize,

    /// The number of CPU cores for each compute node.
    ///
    /// This determines worker_node_parallelism.
    #[clap(long, default_value = "2")]
    compute_node_cores: usize,
}

impl Default for Configuration {
    fn default() -> Self {
        Self::parse_from::<_, &str>([])
    }
}

pub struct Cluster {
    frontends: Vec<IpAddr>,

    _handle: Handle,
    pub(crate) client: NodeHandle,
    pub(crate) ctl: NodeHandle,
}

impl Cluster {
    async fn start_inner(conf: Configuration) -> Result<Self> {
        let handle = madsim::runtime::Handle::current();
        println!("seed = {}", handle.seed());
        println!("{:?}", conf);

        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let meta = "192.168.1.1".parse().unwrap();
        std::env::set_var("RW_META_ADDR", format!("https://{meta}:5690/"));

        // meta node
        handle
            .create_node()
            .name("meta")
            .ip(meta)
            .init(|| async {
                let opts = risingwave_meta::MetaNodeOpts::parse_from([
                    "meta-node",
                    "--listen-addr",
                    "0.0.0.0:5690",
                    "--backend",
                    "mem",
                ]);
                risingwave_meta::start(opts).await
            })
            .build();
        // wait for the service to be ready
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        // frontend node
        let mut frontends = vec![];
        for i in 1..=conf.frontend_nodes {
            let frontend_ip = format!("192.168.2.{i}").parse().unwrap();
            frontends.push(frontend_ip);
            handle
                .create_node()
                .name(format!("frontend-{i}"))
                .ip([192, 168, 2, i as u8].into())
                .init(move || async move {
                    let opts = risingwave_frontend::FrontendOpts::parse_from([
                        "frontend-node",
                        "--host",
                        "0.0.0.0:4566",
                        "--client-address",
                        &format!("{frontend_ip}:4566"),
                        "--meta-addr",
                        &format!("{meta}:5690"),
                    ]);
                    risingwave_frontend::start(opts).await
                })
                .build();
        }

        // compute node
        for i in 1..=conf.compute_nodes {
            handle
                .create_node()
                .name(format!("compute-{i}"))
                .ip([192, 168, 3, i as u8].into())
                .cores(conf.compute_node_cores)
                .init(move || async move {
                    let opts = risingwave_compute::ComputeNodeOpts::parse_from([
                        "compute-node",
                        "--host",
                        "0.0.0.0:5688",
                        "--client-address",
                        &format!("192.168.3.{i}:5688"),
                        "--meta-address",
                        &format!("{meta}:5690"),
                        "--state-store",
                        "hummock+memory-shared",
                    ]);
                    risingwave_compute::start(opts).await
                })
                .build();
        }

        // compactor node
        for i in 1..=conf.compactor_nodes {
            handle
                .create_node()
                .name(format!("compactor-{i}"))
                .ip([192, 168, 4, i as u8].into())
                .init(move || async move {
                    let opts = risingwave_compactor::CompactorOpts::parse_from([
                        "compactor-node",
                        "--host",
                        "0.0.0.0:6660",
                        "--client-address",
                        &format!("192.168.4.{i}:6660"),
                        "--meta-address",
                        "192.168.1.1:5690",
                        "--state-store",
                        "hummock+memory-shared",
                    ]);
                    risingwave_compactor::start(opts).await
                })
                .build();
        }

        // wait for the service to be ready
        tokio::time::sleep(Duration::from_secs(10)).await;

        // client
        let client = handle
            .create_node()
            .name("client")
            .ip([192, 168, 100, 1].into())
            .build();

        // risectl
        let ctl = handle
            .create_node()
            .name("ctl")
            .ip([192, 168, 101, 1].into())
            .build();

        Ok(Self {
            frontends,
            _handle: handle,
            client,
            ctl,
        })
    }

    pub fn start(conf: Configuration) -> BoxFuture<'static, Result<Self>> {
        Box::pin(Self::start_inner(conf))
    }

    async fn run_inner(&mut self, sql: String) -> Result<String> {
        let frontend = self
            .frontends
            .choose(&mut thread_rng())
            .unwrap()
            .to_string();

        let result = self
            .client
            .spawn(async move {
                // TODO: reuse session
                let mut session = RisingWave::connect(frontend, "dev".to_string()).await;
                let result = session.run(&sql).await?;
                session.close().await;
                Ok::<_, anyhow::Error>(result)
            })
            .await??;

        Ok(result)
    }

    pub fn run(&mut self, sql: &str) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.run_inner(sql.to_string()))
    }

    async fn wait_until_inner(
        &mut self,
        sql: String,
        mut p: impl FnMut(&str) -> bool + Send + 'static,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        let fut = async move {
            let mut interval = madsim::time::interval(interval);
            loop {
                interval.tick().await;
                let result = self.run(&sql).await?;
                if p(&result) {
                    return Ok::<_, anyhow::Error>(result);
                }
            }
        };

        match madsim::time::timeout(timeout, fut).await {
            Ok(r) => Ok(r?),
            Err(_) => bail!("wait_until timeout"),
        }
    }

    pub fn wait_until(
        &mut self,
        sql: &str,
        p: impl FnMut(&str) -> bool + Send + 'static,
        interval: Duration,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.wait_until_inner(sql.to_string(), p, interval, timeout))
    }

    async fn wait_until_non_empty_inner(
        &mut self,
        sql: String,
        interval: Duration,
        timeout: Duration,
    ) -> Result<String> {
        self.wait_until_inner(sql, |r| !r.trim().is_empty(), interval, timeout)
            .await
    }

    pub fn wait_until_non_empty(
        &mut self,
        sql: &str,
        interval: Duration,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(self.wait_until_non_empty_inner(sql.to_string(), interval, timeout))
    }
}
