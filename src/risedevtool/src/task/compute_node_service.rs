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
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::Result;

use super::{risingwave_cmd, ExecuteContext, Task};
use crate::util::{get_program_args, get_program_env_cmd, get_program_name};
use crate::{add_meta_node, add_tempo_endpoint, ComputeNodeConfig};

pub struct ComputeNodeService {
    config: ComputeNodeConfig,
}

impl ComputeNodeService {
    pub fn new(config: ComputeNodeConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Apply command args according to config
    pub fn apply_command_args(cmd: &mut Command, config: &ComputeNodeConfig) -> Result<()> {
        cmd.arg("--listen-addr")
            .arg(format!("{}:{}", config.listen_address, config.port))
            .arg("--prometheus-listener-addr")
            .arg(format!(
                "{}:{}",
                config.listen_address, config.exporter_port
            ))
            .arg("--advertise-addr")
            .arg(format!("{}:{}", config.address, config.port))
            .arg("--async-stack-trace")
            .arg(&config.async_stack_trace)
            .arg("--parallelism")
            .arg(config.parallelism.to_string())
            .arg("--total-memory-bytes")
            .arg(config.total_memory_bytes.to_string())
            .arg("--role")
            .arg(&config.role)
            .arg("--resource-group")
            .arg(&config.resource_group);

        let provide_meta_node = config.provide_meta_node.as_ref().unwrap();
        add_meta_node(provide_meta_node, cmd)?;

        let provide_tempo = config.provide_tempo.as_ref().unwrap();
        add_tempo_endpoint(provide_tempo, cmd)?;

        Ok(())
    }
}

impl Task for ComputeNodeService {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);
        ctx.pb.set_message("starting...");

        let mut cmd = risingwave_cmd("compute-node")?;

        cmd.env(
            "TOKIO_CONSOLE_BIND",
            format!("127.0.0.1:{}", self.config.port + 1000),
        );

        if crate::util::is_env_set("RISEDEV_ENABLE_PROFILE") {
            cmd.env(
                "RW_PROFILE_PATH",
                Path::new(&env::var("PREFIX_LOG")?).join(format!("profile-{}", self.id())),
            );
        }

        if crate::util::is_env_set("RISEDEV_ENABLE_HEAP_PROFILE") {
            // See https://linux.die.net/man/3/jemalloc for the descriptions of profiling options
            let conf = "prof:true,lg_prof_interval:34,lg_prof_sample:19,prof_prefix:compute-node";
            cmd.env("_RJEM_MALLOC_CONF", conf); // prefixed for macos
            cmd.env("MALLOC_CONF", conf); // unprefixed for linux
        }

        Self::apply_command_args(&mut cmd, &self.config)?;
        if self.config.enable_tiered_cache {
            let prefix_data = env::var("PREFIX_DATA")?;
            cmd.arg("--data-file-cache-dir").arg(
                PathBuf::from(&prefix_data)
                    .join("foyer")
                    .join(self.config.port.to_string())
                    .join("data"),
            );
            cmd.arg("--meta-file-cache-dir").arg(
                PathBuf::from(&prefix_data)
                    .join("foyer")
                    .join(self.config.port.to_string())
                    .join("meta"),
            );
        }

        if !self.config.user_managed {
            ctx.run_command(ctx.tmux_run(cmd)?)?;
            ctx.pb.set_message("started");
        } else {
            ctx.pb.set_message("user managed");
            writeln!(
                &mut ctx.log,
                "Please use the following parameters to start the compute node:\n{}\n{} {}\n\n",
                get_program_env_cmd(&cmd),
                get_program_name(&cmd),
                get_program_args(&cmd)
            )?;
        }

        Ok(())
    }

    fn id(&self) -> String {
        self.config.id.clone()
    }
}
