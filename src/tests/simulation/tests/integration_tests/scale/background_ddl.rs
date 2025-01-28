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

use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

#[tokio::test]
async fn test_background_arrangement_backfill_offline_scaling() -> Result<()> {
    let config = Configuration::for_background_ddl();

    let cores_per_node = config.compute_node_cores;
    let node_count = config.compute_nodes;

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session
        .run("SET STREAMING_USE_ARRANGEMENT_BACKFILL = true;")
        .await?;
    session.run("create table t (v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 1000);")
        .await?;

    session.run("SET BACKGROUND_DDL=true;").await?;
    session.run("SET BACKFILL_RATE_LIMIT=1;").await?;

    session
        .run("create materialized view m as select * from t;")
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), cores_per_node * node_count);

    sleep(Duration::from_secs(10)).await;

    cluster.simple_kill_nodes(["compute-2", "compute-3"]).await;

    sleep(Duration::from_secs(100)).await;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), cores_per_node);

    sleep(Duration::from_secs(2000)).await;

    // job is finished
    session.run("show jobs;").await?.assert_result_eq("");

    Ok(())
}
