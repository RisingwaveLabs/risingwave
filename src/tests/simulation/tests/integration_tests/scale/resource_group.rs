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
use std::time::Duration;

use anyhow::Result;
use risingwave_common::config::default;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use tokio::time::sleep;

#[tokio::test]
async fn test_resource_group() -> Result<()> {
    let mut config = Configuration::for_arrangement_backfill();

    config.compute_nodes = 3;
    config.compute_node_cores = 2;
    config.compute_resource_groups = HashMap::from([
        (1, DEFAULT_RESOURCE_GROUP.to_owned()),
        (2, "test".to_owned()),
        (3, "test".to_owned()),
    ]);

    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    cluster.simple_kill_nodes(["compute-2", "compute-3"]).await;

    let compute_node_timeout = default::meta::max_heartbeat_interval_sec() as u64;
    let meta_parallelism_ctrl_period = default::meta::parallelism_control_trigger_period_sec();

    sleep(Duration::from_secs(compute_node_timeout * 2)).await;

    session.run("create table t(v int)").await?;
    session
        .run("create materialized view m as select * from t")
        .await?;

    assert!(session
        .run("alter table t set resource_group to 'test'")
        .await
        .is_err());
    assert!(session
        .run("alter materialized view m set resource_group to 'test'")
        .await
        .is_err());

    cluster.simple_restart_nodes(["compute-2"]).await;

    sleep(Duration::from_secs(meta_parallelism_ctrl_period * 2)).await;

    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(union_fragment.inner.actors.len(), 2);
    assert_eq!(mat_fragment.inner.actors.len(), 2);

    let _ = session
        .run("alter materialized view m set resource_group to 'test'")
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(mat_fragment.inner.actors.len(), 2);

    cluster.simple_restart_nodes(["compute-3"]).await;

    sleep(Duration::from_secs(meta_parallelism_ctrl_period * 2)).await;

    let union_fragment = cluster
        .locate_one_fragment([identity_contains("union")])
        .await?;

    let mat_fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("union"),
        ])
        .await?;

    assert_eq!(union_fragment.inner.actors.len(), 2);
    assert_eq!(mat_fragment.inner.actors.len(), 4);

    Ok(())
}
