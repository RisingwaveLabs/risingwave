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

#![cfg_attr(coverage, feature(no_coverage))]

use risingwave_common::enable_jemalloc_on_unix;

enable_jemalloc_on_unix!();

#[cfg_attr(coverage, no_coverage)]
#[tokio::main]
async fn main() {
    use clap::Parser;

    let opts = risingwave_meta_probe::MetaProbeOpts::parse();

    risingwave_rt::init_risingwave_logger(risingwave_rt::LoggerSettings::new());

    let return_code: i32 = match risingwave_meta_probe::ok(opts).await {
        true => 0,
        false => 1,
    };
    std::process::exit(return_code);
}
