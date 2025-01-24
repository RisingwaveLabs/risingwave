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

use anyhow::Result;
use itertools::Itertools;
use maplit::{convert_args, hashmap};
use risingwave_common::hash::WorkerSlotId;
use risingwave_pb::meta::table_fragments::Fragment;
use risingwave_pb::stream_plan::DispatcherType;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};

const CREATE_SOURCE: &str = r#"
CREATE SOURCE s(v1 int, v2 varchar) WITH (
    connector='kafka',
    properties.bootstrap.server='192.168.11.1:29092',
    topic='shared_source'
) FORMAT PLAIN ENCODE JSON;"#;

/// Returns `Vec<(backfill_actor_id, source_actor_id)>`
fn source_backfill_upstream(
    source_backfill_fragment: &Fragment,
    source_fragment: &Fragment,
) -> Vec<(u32, u32)> {
    let mut no_shuffle_downstream_to_upstream = HashMap::new();
    for source_actor in &source_fragment.actors {
        for dispatcher in &source_actor.dispatcher {
            if dispatcher.r#type == DispatcherType::NoShuffle as i32 {
                assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                let downstream_actor_id = dispatcher.downstream_actor_id[0];
                no_shuffle_downstream_to_upstream
                    .insert(downstream_actor_id, source_actor.actor_id);
            }
        }
    }

    let (_, source_fragment_id) = source_backfill_fragment
        .get_nodes()
        .unwrap()
        .find_source_backfill()
        .unwrap();
    assert_eq!(source_fragment.fragment_id, source_fragment_id);

    source_backfill_fragment
        .actors
        .iter()
        .map(|backfill_actor| {
            (
                backfill_actor.actor_id,
                no_shuffle_downstream_to_upstream[&backfill_actor.actor_id],
            )
        })
        .collect_vec()
}

async fn validate_splits_aligned(cluster: &mut Cluster) -> Result<()> {
    let source_backfill_fragment = cluster
        .locate_one_fragment([identity_contains("StreamSourceScan")])
        .await?;
    let source_fragment = cluster
        .locate_one_fragment([
            identity_contains("Source"),
            no_identity_contains("StreamSourceScan"),
        ])
        .await?;
    // The result of scaling is non-deterministic.
    // So we just print the result here, instead of asserting with a fixed value.
    let actor_upstream =
        source_backfill_upstream(&source_backfill_fragment.inner, &source_fragment.inner);
    tracing::info!(
        "{}",
        actor_upstream
            .iter()
            .format_with("\n", |(actor_id, upstream), f| f(&format_args!(
                "{} <- {:?}",
                actor_id, upstream
            )))
    );
    let actor_splits = cluster.list_source_splits().await?;
    tracing::info!("{:#?}", actor_splits);
    for (actor, upstream) in actor_upstream {
        assert_eq!(
            actor_splits.get(&actor).unwrap(),
            actor_splits.get(&upstream).unwrap()
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_shared_source() -> Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .with_env_filter("risingwave_stream::executor::source::source_backfill_executor=DEBUG,integration_tests=DEBUG")
        .init();

    let mut cluster = Cluster::start(Configuration::for_scale_shared_source()).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "shared_source" => 4,
    )));
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;

    session.run(CREATE_SOURCE).await?;
    session
        .run("create materialized view mv as select count(*) from s group by v1;")
        .await?;
    let source_fragment = cluster
        .locate_one_fragment([
            identity_contains("Source"),
            no_identity_contains("StreamSourceScan"),
        ])
        .await?;
    let source_workers = source_fragment.all_worker_count().into_keys().collect_vec();
    let source_backfill_fragment = cluster
        .locate_one_fragment([identity_contains("StreamSourceScan")])
        .await?;
    let source_backfill_workers = source_backfill_fragment
        .all_worker_count()
        .into_keys()
        .collect_vec();
    let hash_agg_fragment = cluster
        .locate_one_fragment([identity_contains("hashagg")])
        .await?;
    let hash_agg_workers = hash_agg_fragment
        .all_worker_count()
        .into_keys()
        .collect_vec();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW} 6 256
        3 8 HASH {SOURCE_SCAN} 6 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED ADAPTIVE 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // SourceBackfill cannot be scaled because of NoShuffle.
    assert!(
        &cluster
            .reschedule(
                source_backfill_fragment
                    .reschedule([WorkerSlotId::new(source_backfill_workers[0], 0)], []),
            )
            .await.unwrap_err().to_string().contains("rescheduling NoShuffle downstream fragment (maybe Chain fragment) is forbidden, please use NoShuffle upstream fragment (like Materialized fragment) to scale"),
    );

    // hash agg can be scaled independently
    cluster
        .reschedule(hash_agg_fragment.reschedule([WorkerSlotId::new(hash_agg_workers[0], 0)], []))
        .await
        .unwrap();
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 6 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);

    // source is the NoShuffle upstream. It can be scaled, and the downstream SourceBackfill will be scaled together.
    cluster
        .reschedule(source_fragment.reschedule(
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
            ],
            [],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 3 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 3 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED CUSTOM 256
        8 CREATED CUSTOM 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // resolve_no_shuffle for backfill fragment is OK, which will scale the upstream together.
    cluster
        .reschedule_resolve_no_shuffle(source_backfill_fragment.reschedule(
            [],
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
                WorkerSlotId::new(source_workers[2], 1),
            ],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 7 256
        2 8 HASH {MVIEW} 5 256
        3 8 HASH {SOURCE_SCAN} 7 256"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED CUSTOM 256
        8 CREATED CUSTOM 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);
    Ok(())
}

#[tokio::test]
async fn test_issue_19563() -> Result<()> {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_max_level(tracing::Level::ERROR)
        .with_env_filter("risingwave_stream::executor::source::source_backfill_executor=DEBUG,integration_tests=DEBUG")
        .init();

    let mut cluster = Cluster::start(Configuration::for_scale_shared_source()).await?;
    cluster.create_kafka_topics(convert_args!(hashmap!(
        "shared_source" => 4,
    )));
    let mut session = cluster.start_session();

    session.run("set rw_implicit_flush = true;").await?;

    session
        .run(
            r#"
CREATE SOURCE s(v1 timestamp with time zone) WITH (
    connector='kafka',
    properties.bootstrap.server='192.168.11.1:29092',
    topic='shared_source'
) FORMAT PLAIN ENCODE JSON;"#,
        )
        .await?;
    session
        .run("create materialized view mv1 as select v1 from s where v1 between now() and now() + interval '1 day' * 365 * 2000;")
        .await?;
    let source_fragment = cluster
        .locate_one_fragment([
            identity_contains("Source"),
            no_identity_contains("StreamSourceScan"),
        ])
        .await?;
    let source_workers = source_fragment.all_worker_count().into_keys().collect_vec();
    let source_backfill_dynamic_filter_fragment = cluster
        .locate_one_fragment([
            identity_contains("StreamSourceScan"),
            identity_contains("StreamDynamicFilter"),
        ])
        .await?;
    let source_backfill_workers = source_backfill_dynamic_filter_fragment
        .all_worker_count()
        .into_keys()
        .collect_vec();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 6 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 6 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED ADAPTIVE 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // SourceBackfill/DynamicFilter cannot be scaled because of NoShuffle.
    assert!(
        &cluster
            .reschedule(
                source_backfill_dynamic_filter_fragment
                    .reschedule([WorkerSlotId::new(source_backfill_workers[0], 0)], []),
            )
            .await.unwrap_err().to_string().contains("rescheduling NoShuffle downstream fragment (maybe Chain fragment) is forbidden, please use NoShuffle upstream fragment (like Materialized fragment) to scale"),
    );

    // source is the NoShuffle upstream. It can be scaled, and the downstream SourceBackfill/DynamicFilter will be scaled together.
    cluster
        .reschedule(source_fragment.reschedule(
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
            ],
            [],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 3 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 3 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED CUSTOM 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);

    // resolve_no_shuffle for backfill fragment is OK, which will scale the upstream together.
    cluster
        .reschedule_resolve_no_shuffle(source_backfill_dynamic_filter_fragment.reschedule(
            [],
            [
                WorkerSlotId::new(source_workers[0], 0),
                WorkerSlotId::new(source_workers[0], 1),
                WorkerSlotId::new(source_workers[2], 0),
                WorkerSlotId::new(source_workers[2], 1),
            ],
        ))
        .await
        .unwrap();
    validate_splits_aligned(&mut cluster).await?;
    expect_test::expect![[r#"
        1 6 HASH {SOURCE} 7 256
        2 8 HASH {MVIEW,SOURCE_SCAN} 7 256
        3 8 SINGLE {NOW} 1 1
        4 8 SINGLE {NOW} 1 1"#]]
    .assert_eq(&cluster.run("select fragment_id, table_id, distribution_type, flags, parallelism, max_parallelism from rw_fragments;").await?);
    expect_test::expect![[r#"
        6 CREATED CUSTOM 256
        8 CREATED ADAPTIVE 256"#]]
    .assert_eq(&cluster.run("select * from rw_table_fragments;").await?);
    Ok(())
}
