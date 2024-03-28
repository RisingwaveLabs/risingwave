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

use std::env;
use std::path::Path;
use std::process::Command;

use anyhow::{bail, Context, Result};
use console::style;

use crate::{ExecuteContext, Task};

pub struct ConfigureTmuxTask;

pub const RISEDEV_NAME: &str = "risedev";

pub fn new_tmux_command() -> Command {
    let mut cmd = Command::new("tmux");
    cmd.arg("-L").arg(RISEDEV_NAME); // `-L` specifies a dedicated tmux server
    cmd
}

impl ConfigureTmuxTask {
    pub fn new() -> Result<Self> {
        Ok(Self)
    }
}

impl Task for ConfigureTmuxTask {
    fn execute(&mut self, ctx: &mut ExecuteContext<impl std::io::Write>) -> anyhow::Result<()> {
        ctx.service(self);

        ctx.pb.set_message("starting...");

        let prefix_path = env::var("PREFIX")?;
        let prefix_bin = env::var("PREFIX_BIN")?;

        let mut cmd = new_tmux_command();
        cmd.arg("-V");
        ctx.run_command(cmd).with_context(|| {
            format!(
                "Failed to execute {} command. Did you install tmux?",
                style("tmux").blue().bold()
            )
        })?;

        let mut cmd = new_tmux_command();
        cmd.arg("list-sessions");
        if ctx.run_command(cmd).is_ok() {
            bail!(
                "A previous cluster is already running. Please kill it first with {}.",
                style("./risedev k").blue().bold()
            );
        }

        ctx.pb.set_message("creating new session...");

        let mut cmd = new_tmux_command();
        cmd.arg("new-session") // this will automatically create the `risedev` tmux server
            .arg("-d")
            .arg("-s")
            .arg(RISEDEV_NAME)
            .arg("-c")
            .arg(Path::new(&prefix_path))
            .arg(Path::new(&prefix_bin).join("welcome.sh"));

        ctx.run_command(cmd)?;

        ctx.complete_spin();

        ctx.pb.set_message(format!("session {}", RISEDEV_NAME));

        Ok(())
    }

    fn id(&self) -> String {
        "tmux".into()
    }
}
