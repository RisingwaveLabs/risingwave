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

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::fmt::Write;
use std::sync::Arc;

use itertools::Itertools;
use prometheus_http_query::response::Data::Vector;
use risingwave_common::types::Timestamptz;
use risingwave_common::util::StackTraceResponseExt;
use risingwave_hummock_sdk::level::Level;
use risingwave_meta_model::table::TableType;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::event_log::Event;
use risingwave_pb::meta::EventLog;
use risingwave_pb::monitor_service::StackTraceResponse;
use risingwave_rpc_client::ComputeClientPool;
use risingwave_sqlparser::ast::{CompatibleFormatEncode, Statement, Value};
use risingwave_sqlparser::parser::Parser;
use serde_json::json;
use thiserror_ext::AsReport;

use crate::hummock::HummockManagerRef;
use crate::manager::event_log::EventLogManagerRef;
use crate::manager::MetadataManager;
use crate::MetaResult;

pub type DiagnoseCommandRef = Arc<DiagnoseCommand>;

pub struct DiagnoseCommand {
    metadata_manager: MetadataManager,
    hummock_manger: HummockManagerRef,
    event_log_manager: EventLogManagerRef,
    prometheus_client: Option<prometheus_http_query::Client>,
    prometheus_selector: String,
}

impl DiagnoseCommand {
    pub fn new(
        metadata_manager: MetadataManager,
        hummock_manger: HummockManagerRef,
        event_log_manager: EventLogManagerRef,
        prometheus_client: Option<prometheus_http_query::Client>,
        prometheus_selector: String,
    ) -> Self {
        Self {
            metadata_manager,
            hummock_manger,
            event_log_manager,
            prometheus_client,
            prometheus_selector,
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    pub async fn report(&self) -> String {
        let mut report = String::new();
        let _ = writeln!(
            report,
            "report created at: {}\nversion: {}",
            chrono::DateTime::<chrono::offset::Utc>::from(std::time::SystemTime::now()),
            risingwave_common::current_cluster_version(),
        );
        let _ = writeln!(report);
        self.write_catalog(&mut report).await;
        let _ = writeln!(report);
        self.write_worker_nodes(&mut report).await;
        let _ = writeln!(report);
        self.write_streaming_prometheus(&mut report).await;
        let _ = writeln!(report);
        self.write_storage(&mut report).await;
        let _ = writeln!(report);
        self.write_await_tree(&mut report).await;
        let _ = writeln!(report);
        self.write_event_logs(&mut report);
        report
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_catalog(&self, s: &mut String) {
        self.write_catalog_inner(s).await;
        let _ = self.write_table_definition(s).await.inspect_err(|e| {
            tracing::warn!(
                error = e.to_report_string(),
                "failed to display table definition"
            )
        });
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_catalog_inner(&self, s: &mut String) {
        let guard = self
            .metadata_manager
            .catalog_controller
            .get_inner_read_guard()
            .await;
        let stat = match guard.stats().await {
            Ok(stat) => stat,
            Err(err) => {
                tracing::warn!(error=?err.as_report(), "failed to get catalog stats");
                return;
            }
        };
        let _ = writeln!(s, "number of fragment: {}", stat.streaming_job_num);
        let _ = writeln!(s, "number of actor: {}", stat.actor_num);
        let _ = writeln!(s, "number of source: {}", stat.source_num);
        let _ = writeln!(s, "number of table: {}", stat.table_num);
        let _ = writeln!(s, "number of materialized view: {}", stat.mview_num);
        let _ = writeln!(s, "number of sink: {}", stat.sink_num);
        let _ = writeln!(s, "number of index: {}", stat.index_num);
        let _ = writeln!(s, "number of function: {}", stat.function_num);
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_worker_nodes(&self, s: &mut String) {
        let Ok(worker_actor_count) = self.metadata_manager.worker_actor_count().await else {
            tracing::warn!("failed to get worker actor count");
            return;
        };

        use comfy_table::{Row, Table};
        let Ok(worker_nodes) = self.metadata_manager.list_worker_node(None, None).await else {
            tracing::warn!("failed to get worker nodes");
            return;
        };
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("id".into());
            row.add_cell("host".into());
            row.add_cell("type".into());
            row.add_cell("state".into());
            row.add_cell("parallelism".into());
            row.add_cell("is_streaming".into());
            row.add_cell("is_serving".into());
            row.add_cell("rw_version".into());
            row.add_cell("total_memory_bytes".into());
            row.add_cell("total_cpu_cores".into());
            row.add_cell("started_at".into());
            row.add_cell("actor_count".into());
            row
        });
        for worker_node in worker_nodes {
            let mut row = Row::new();
            row.add_cell(worker_node.id.into());
            try_add_cell(
                &mut row,
                worker_node
                    .host
                    .as_ref()
                    .map(|h| format!("{}:{}", h.host, h.port)),
            );
            try_add_cell(
                &mut row,
                worker_node.get_type().ok().map(|t| t.as_str_name()),
            );
            try_add_cell(
                &mut row,
                worker_node.get_state().ok().map(|s| s.as_str_name()),
            );
            // FIXME:
            // row.add_cell(worker_node.parallelism().into());
            try_add_cell(
                &mut row,
                worker_node.property.as_ref().map(|p| p.is_streaming),
            );
            try_add_cell(
                &mut row,
                worker_node.property.as_ref().map(|p| p.is_serving),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.rw_version.clone()),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.total_memory_bytes),
            );
            try_add_cell(
                &mut row,
                worker_node.resource.as_ref().map(|r| r.total_cpu_cores),
            );
            try_add_cell(
                &mut row,
                worker_node
                    .started_at
                    .and_then(|ts| Timestamptz::from_secs(ts as _).map(|t| t.to_string())),
            );
            let actor_count = {
                if let Ok(t) = worker_node.get_type()
                    && t != WorkerType::ComputeNode
                {
                    None
                } else {
                    match worker_actor_count.get(&(worker_node.id as _)) {
                        None => Some(0),
                        Some(c) => Some(*c),
                    }
                }
            };
            try_add_cell(&mut row, actor_count);
            table.add_row(row);
        }
        let _ = writeln!(s, "{table}");
    }

    #[cfg_attr(coverage, coverage(off))]
    fn write_event_logs(&self, s: &mut String) {
        let event_logs = self
            .event_log_manager
            .list_event_logs()
            .into_iter()
            .sorted_by(|a, b| {
                a.timestamp
                    .unwrap_or(0)
                    .cmp(&b.timestamp.unwrap_or(0))
                    .reverse()
            })
            .collect_vec();

        let _ = writeln!(s, "latest barrier completions");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::BarrierComplete(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(10),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest barrier collection failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::CollectBarrierFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest barrier injection failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::InjectBarrierFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest worker node panics");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::WorkerNodePanic(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(10),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest create stream job failures");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::CreateStreamJobFail(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );

        let _ = writeln!(s);
        let _ = writeln!(s, "latest dirty stream job clear-ups");
        Self::write_event_logs_impl(
            s,
            event_logs.iter(),
            |e| {
                let Event::DirtyStreamJobClear(info) = e else {
                    return None;
                };
                Some(json!(info).to_string())
            },
            Some(3),
        );
    }

    #[cfg_attr(coverage, coverage(off))]
    fn write_event_logs_impl<'a, F>(
        s: &mut String,
        event_logs: impl Iterator<Item = &'a EventLog>,
        get_event_info: F,
        limit: Option<usize>,
    ) where
        F: Fn(&Event) -> Option<String>,
    {
        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("created_at".into());
            row.add_cell("info".into());
            row
        });
        let mut row_count = 0;
        for event_log in event_logs {
            let Some(ref inner) = event_log.event else {
                continue;
            };
            if let Some(limit) = limit
                && row_count >= limit
            {
                break;
            }
            let mut row = Row::new();
            let ts = event_log
                .timestamp
                .and_then(|ts| Timestamptz::from_millis(ts as _).map(|ts| ts.to_string()));
            try_add_cell(&mut row, ts);
            if let Some(info) = get_event_info(inner) {
                row.add_cell(info.into());
                row_count += 1;
            } else {
                continue;
            }
            table.add_row(row);
        }
        let _ = writeln!(s, "{table}");
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_storage(&self, s: &mut String) {
        let mut sst_num = 0;
        let mut sst_total_file_size = 0;
        let back_pressured_compaction_groups = self
            .hummock_manger
            .write_limits()
            .await
            .into_iter()
            .filter_map(|(k, v)| {
                if v.table_ids.is_empty() {
                    None
                } else {
                    Some(k)
                }
            })
            .join(",");
        if !back_pressured_compaction_groups.is_empty() {
            let _ = writeln!(
                s,
                "back pressured compaction groups: {back_pressured_compaction_groups}"
            );
        }

        #[derive(PartialEq, Eq)]
        struct SstableSort {
            compaction_group_id: u64,
            sst_id: u64,
            delete_ratio: u64,
        }
        impl PartialOrd for SstableSort {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for SstableSort {
            fn cmp(&self, other: &Self) -> Ordering {
                self.delete_ratio.cmp(&other.delete_ratio)
            }
        }
        fn top_k_sstables(
            top_k: usize,
            heap: &mut BinaryHeap<Reverse<SstableSort>>,
            e: SstableSort,
        ) {
            if heap.len() < top_k {
                heap.push(Reverse(e));
            } else if let Some(mut p) = heap.peek_mut() {
                if e.delete_ratio > p.0.delete_ratio {
                    *p = Reverse(e);
                }
            }
        }

        let top_k = 10;
        let mut top_tombstone_delete_sst = BinaryHeap::with_capacity(top_k);
        let compaction_group_num = self
            .hummock_manger
            .on_current_version(|version| {
                for compaction_group in version.levels.values() {
                    let mut visit_level = |level: &Level| {
                        sst_num += level.table_infos.len();
                        sst_total_file_size +=
                            level.table_infos.iter().map(|t| t.sst_size).sum::<u64>();
                        for sst in &level.table_infos {
                            if sst.total_key_count == 0 {
                                continue;
                            }
                            let tombstone_delete_ratio =
                                sst.stale_key_count * 10000 / sst.total_key_count;
                            let e = SstableSort {
                                compaction_group_id: compaction_group.group_id,
                                sst_id: sst.sst_id,
                                delete_ratio: tombstone_delete_ratio,
                            };
                            top_k_sstables(top_k, &mut top_tombstone_delete_sst, e);
                        }
                    };
                    let l0 = &compaction_group.l0;
                    // FIXME: why chaining levels iter leads to segmentation fault?
                    for level in &l0.sub_levels {
                        visit_level(level);
                    }
                    for level in &compaction_group.levels {
                        visit_level(level);
                    }
                }
                version.levels.len()
            })
            .await;

        let _ = writeln!(s, "number of SSTables: {sst_num}");
        let _ = writeln!(s, "total size of SSTables (byte): {sst_total_file_size}");
        let _ = writeln!(s, "number of compaction groups: {compaction_group_num}");
        use comfy_table::{Row, Table};
        fn format_table(heap: BinaryHeap<Reverse<SstableSort>>) -> Table {
            let mut table = Table::new();
            table.set_header({
                let mut row = Row::new();
                row.add_cell("compaction group id".into());
                row.add_cell("sstable id".into());
                row.add_cell("delete ratio".into());
                row
            });
            for sst in &heap.into_sorted_vec() {
                let mut row = Row::new();
                row.add_cell(sst.0.compaction_group_id.into());
                row.add_cell(sst.0.sst_id.into());
                row.add_cell(format!("{:.2}%", sst.0.delete_ratio as f64 / 100.0).into());
                table.add_row(row);
            }
            table
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "top tombstone delete ratio");
        let _ = writeln!(s, "{}", format_table(top_tombstone_delete_sst));
        let _ = writeln!(s);

        let _ = writeln!(s);
        self.write_storage_prometheus(s).await;
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_streaming_prometheus(&self, s: &mut String) {
        let _ = writeln!(s, "top sources by throughput (rows/s)");
        let query = format!(
            "topk(3, sum(rate(stream_source_output_rows_counts{{{}}}[10m]))by (source_name))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["source_name"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top materialized views by throughput (rows/s)");
        let query = format!(
            "topk(3, sum(rate(stream_mview_input_row_count{{{}}}[10m]))by (table_id))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top join executor by matched rows");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(stream_join_matched_join_keys_bucket{{{}}}[10m])) by (le, fragment_id, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id", "fragment_id"])
            .await;
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_storage_prometheus(&self, s: &mut String) {
        let _ = writeln!(s, "top Hummock Get by duration (second)");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(state_store_get_duration_bucket{{{}}}[10m])) by (le, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top Hummock Iter Init by duration (second)");
        let query = format!(
            "topk(3, histogram_quantile(0.9, sum(rate(state_store_iter_init_duration_bucket{{{}}}[10m])) by (le, table_id)))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "top table commit flush by size (byte)");
        let query = format!(
            "topk(3, sum(rate(storage_commit_write_throughput{{{}}}[10m])) by (table_id))",
            self.prometheus_selector
        );
        self.write_instant_vector_impl(s, &query, vec!["table_id"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store read throughput (bytes/s)");
        let query = format!(
            "sum(rate(object_store_read_bytes{{{}}}[10m])) by (job, instance)",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store write throughput (bytes/s)");
        let query = format!(
            "sum(rate(object_store_write_bytes{{{}}}[10m])) by (job, instance)",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store operation rate");
        let query = format!(
            "sum(rate(object_store_operation_latency_count{{{}}}[10m])) by (le, type, job, instance)",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\", type!~\"streaming_upload_write_bytes|streaming_read_read_bytes|streaming_read\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["type", "job", "instance"])
            .await;

        let _ = writeln!(s);
        let _ = writeln!(s, "object store operation duration (second)");
        let query = format!(
            "histogram_quantile(0.9, sum(rate(object_store_operation_latency_bucket{{{}}}[10m])) by (le, type, job, instance))",
            merge_prometheus_selector([&self.prometheus_selector, "job=~\"compute|compactor\", type!~\"streaming_upload_write_bytes|streaming_read\""])
        );
        self.write_instant_vector_impl(s, &query, vec!["type", "job", "instance"])
            .await;
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_instant_vector_impl<'a>(&self, s: &mut String, query: &str, labels: Vec<&str>) {
        let Some(ref client) = self.prometheus_client else {
            return;
        };
        if let Ok(Vector(instant_vec)) = client
            .query(query)
            .get()
            .await
            .map(|result| result.into_inner().0)
        {
            for i in instant_vec {
                let l = labels
                    .iter()
                    .map(|label| {
                        format!(
                            "{}={}",
                            *label,
                            i.metric()
                                .get(*label)
                                .map(|s| s.as_str())
                                .unwrap_or_default()
                        )
                    })
                    .join(",");
                let _ = writeln!(s, "{}: {:.3}", l, i.sample().value());
            }
        }
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn write_await_tree(&self, s: &mut String) {
        // Most lines of code are copied from dashboard::handlers::dump_await_tree_all, because the latter cannot be called directly from here.
        let Ok(worker_nodes) = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::ComputeNode), None)
            .await
        else {
            tracing::warn!("failed to get worker nodes");
            return;
        };

        let mut all = StackTraceResponse::default();

        let compute_clients = ComputeClientPool::adhoc();
        for worker_node in &worker_nodes {
            if let Ok(client) = compute_clients.get(worker_node).await
                && let Ok(result) = client.stack_trace().await
            {
                all.merge_other(result);
            }
        }

        write!(s, "{}", all.output()).unwrap();
    }

    async fn write_table_definition(&self, s: &mut String) -> MetaResult<()> {
        let sources = self
            .metadata_manager
            .catalog_controller
            .list_sources()
            .await?
            .into_iter()
            .map(|s| (s.id, (s.name, s.schema_id, s.definition)))
            .collect::<BTreeMap<_, _>>();
        let tables = self
            .metadata_manager
            .catalog_controller
            .list_tables_by_type(TableType::Table)
            .await?
            .into_iter()
            .map(|t| (t.id, (t.name, t.schema_id, t.definition)))
            .collect::<BTreeMap<_, _>>();
        let mvs = self
            .metadata_manager
            .catalog_controller
            .list_tables_by_type(TableType::MaterializedView)
            .await?
            .into_iter()
            .map(|t| (t.id, (t.name, t.schema_id, t.definition)))
            .collect::<BTreeMap<_, _>>();
        let indexes = self
            .metadata_manager
            .catalog_controller
            .list_tables_by_type(TableType::Index)
            .await?
            .into_iter()
            .map(|t| (t.id, (t.name, t.schema_id, t.definition)))
            .collect::<BTreeMap<_, _>>();
        let sinks = self
            .metadata_manager
            .catalog_controller
            .list_sinks()
            .await?
            .into_iter()
            .map(|s| (s.id, (s.name, s.schema_id, s.definition)))
            .collect::<BTreeMap<_, _>>();
        let catalogs = [
            ("SOURCE", sources),
            ("TABLE", tables),
            ("MATERIALIZED VIEW", mvs),
            ("INDEX", indexes),
            ("SINK", sinks),
        ];
        let mut obj_id_to_name = HashMap::new();
        for (title, items) in catalogs {
            use comfy_table::{Row, Table};
            let mut table = Table::new();
            table.set_header({
                let mut row = Row::new();
                row.add_cell("id".into());
                row.add_cell("name".into());
                row.add_cell("schema_id".into());
                row.add_cell("definition".into());
                row
            });
            for (id, (name, schema_id, definition)) in items {
                obj_id_to_name.insert(id, name.clone());
                let mut row = Row::new();
                let may_redact =
                    redact_all_sql_options(&definition).unwrap_or_else(|| "[REDACTED]".into());
                row.add_cell(id.into());
                row.add_cell(name.into());
                row.add_cell(schema_id.into());
                row.add_cell(may_redact.into());
                table.add_row(row);
            }
            let _ = writeln!(s);
            let _ = writeln!(s, "{title}");
            let _ = writeln!(s, "{table}");
        }

        let actors = self
            .metadata_manager
            .catalog_controller
            .list_actor_info()
            .await?
            .into_iter()
            .map(|(actor_id, fragment_id, job_id, schema_id, obj_type)| {
                (
                    actor_id,
                    (
                        fragment_id,
                        job_id,
                        schema_id,
                        obj_type,
                        obj_id_to_name
                            .get(&(job_id as _))
                            .cloned()
                            .unwrap_or_default(),
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>();

        use comfy_table::{Row, Table};
        let mut table = Table::new();
        table.set_header({
            let mut row = Row::new();
            row.add_cell("id".into());
            row.add_cell("fragment_id".into());
            row.add_cell("job_id".into());
            row.add_cell("schema_id".into());
            row.add_cell("type".into());
            row.add_cell("name".into());
            row
        });
        for (actor_id, (fragment_id, job_id, schema_id, ddl_type, name)) in actors {
            let mut row = Row::new();
            row.add_cell(actor_id.into());
            row.add_cell(fragment_id.into());
            row.add_cell(job_id.into());
            row.add_cell(schema_id.into());
            row.add_cell(ddl_type.as_str().into());
            row.add_cell(name.into());
            table.add_row(row);
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "ACTOR");
        let _ = writeln!(s, "{table}");
        Ok(())
    }
}

#[cfg_attr(coverage, coverage(off))]
fn try_add_cell<T: Into<comfy_table::Cell>>(row: &mut comfy_table::Row, t: Option<T>) {
    match t {
        Some(t) => {
            row.add_cell(t.into());
        }
        None => {
            row.add_cell("".into());
        }
    }
}

#[cfg_attr(coverage, coverage(off))]
fn merge_prometheus_selector<'a>(selectors: impl IntoIterator<Item = &'a str>) -> String {
    selectors.into_iter().filter(|s| !s.is_empty()).join(",")
}

fn redact_all_sql_options(sql: &str) -> Option<String> {
    let Ok(mut statements) = Parser::parse_sql(sql) else {
        return None;
    };
    let mut redacted = String::new();
    for statement in &mut statements {
        let options = match statement {
            Statement::CreateTable {
                with_options,
                format_encode,
                ..
            } => {
                let format_encode = match format_encode {
                    Some(CompatibleFormatEncode::V2(cs)) => Some(&mut cs.row_options),
                    _ => None,
                };
                (Some(with_options), format_encode)
            }
            Statement::CreateSource { stmt } => {
                let format_encode = match &mut stmt.format_encode {
                    CompatibleFormatEncode::V2(cs) => Some(&mut cs.row_options),
                    _ => None,
                };
                (Some(&mut stmt.with_properties.0), format_encode)
            }
            Statement::CreateSink { stmt } => {
                let format_encode = match &mut stmt.sink_schema {
                    Some(cs) => Some(&mut cs.row_options),
                    _ => None,
                };
                (Some(&mut stmt.with_properties.0), format_encode)
            }
            _ => (None, None),
        };
        if let Some(options) = options.0 {
            for option in options {
                option.value = Value::SingleQuotedString("[REDACTED]".into()).into();
            }
        }
        if let Some(options) = options.1 {
            for option in options {
                option.value = Value::SingleQuotedString("[REDACTED]".into()).into();
            }
        }
        writeln!(&mut redacted, "{statement}").unwrap();
    }
    Some(redacted)
}
