import logging
import os
import sys

p = os.path.dirname(__file__)
sys.path.append(p)

from jsonmerge import merge

from common import *

source_uid = os.environ.get(SOURCE_UID, "risedev-prometheus")
dashboard_uid = os.environ.get(DASHBOARD_UID, "Ecy3uV1nz")
dashboard_version = int(os.environ.get(DASHBOARD_VERSION, "0"))
datasource = {"type": "prometheus", "uid": f"{source_uid}"}

datasource_const = "datasource"
if dynamic_source_enabled:
    datasource = {"type": "prometheus", "uid": "${datasource}"}

panels = Panels(datasource)
logging.basicConfig(level=logging.WARN)


def section_actor_info(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Actor/Table Id Info",
            [
                panels.table_info(
                    "Actor Info",
                    "Information about actors",
                    [
                        panels.table_target(
                            f"group({metric('actor_info')}) by (actor_id, fragment_id, compute_node)"
                        )
                    ],
                    ["actor_id", "fragment_id", "compute_node"],
                ),
                panels.table_info(
                    "State Table Info",
                    "Information about state tables. Column `materialized_view_id` is the id of the materialized view that this state table belongs to.",
                    [
                        panels.table_target(
                            f"group({metric('table_info')}) by (table_id, table_name, table_type, materialized_view_id, fragment_id, compaction_group_id)"
                        )
                    ],
                    [
                        "table_id",
                        "table_name",
                        "table_type",
                        "materialized_view_id",
                        "fragment_id",
                        "compaction_group_id",
                    ],
                ),
                panels.table_info(
                    "Actor Count (Group By Compute Node)",
                    "Actor count per compute node",
                    [
                        panels.table_target(
                            f"count({metric('actor_info')}) by (compute_node)"
                        )
                    ],
                    [
                        "table_id",
                        "table_name",
                        "table_type",
                        "materialized_view_id",
                        "fragment_id",
                        "compaction_group_id",
                    ],
                    dict.fromkeys(["Time"], True),
                ),
            ],
        )
    ]


def section_cluster_node(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Cluster Node",
            [
                panels.timeseries_count(
                    "Node Count",
                    "The number of each type of RisingWave components alive.",
                    [
                        panels.target(
                            f"sum({metric('worker_num')}) by (worker_type)",
                            "{{worker_type}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_memory(
                    "Node Memory",
                    "The memory usage of each RisingWave component.",
                    [
                        panels.target(
                            f"avg({metric('process_resident_memory_bytes')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_percentage(
                    "Node Memory relative",
                    "Memory usage relative to k8s resource limit of container. Only works in K8s environment",
                    [
                        panels.target(
                            '(avg by(namespace, pod) (container_memory_working_set_bytes{namespace=~"$namespace",pod=~"$pod",container=~"$component"})) / (  sum by(namespace, pod) (kube_pod_container_resource_limits{namespace=~"$namespace", pod=~"$pod", container="$component", resource="memory", unit="byte"}))',
                            "avg memory usage @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_cpu(
                    "Node CPU",
                    "The CPU usage of each RisingWave component.",
                    [
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "cpu usage (total) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('process_cpu_seconds_total')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / avg({metric('process_cpu_core_num')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                            "cpu usage (avg per core) - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_cpu(
                    "Node CPU relative",
                    "CPU usage relative to k8s resource limit of container. Only works in K8s environment",
                    [
                        panels.target(
                            '(sum(rate(container_cpu_usage_seconds_total{namespace=~"$namespace",container=~"$component",pod=~"$pod"}[$__rate_interval])) by (namespace, pod)) / (sum(kube_pod_container_resource_limits{namespace=~"$namespace",pod=~"$pod",container=~"$component", resource="cpu"}) by (namespace, pod))',
                            "cpu usage @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Meta Cluster",
                    "RW cluster can configure multiple meta nodes to achieve high availability. One is the leader and the "
                    "rest are the followers.",
                    [
                        panels.target(
                            f"sum({metric('meta_num')}) by (worker_addr,role)",
                            "{{worker_addr}} @ {{role}}",
                        )
                    ],
                    ["last"],
                ),
            ],
        )
    ]


def section_recovery_node(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Recovery",
            [
                panels.timeseries_ops(
                    "Recovery Successful Rate",
                    "The rate of successful recovery attempts",
                    [
                        panels.target(
                            f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Failed recovery attempts",
                    "Total number of failed reocovery attempts",
                    [
                        panels.target(
                            f"sum({metric('recovery_failure_cnt')}) by ({NODE_LABEL})",
                            "{{%s}}" % NODE_LABEL,
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Recovery latency",
                    "Time spent in a successful recovery attempt",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('recovery_latency_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                                f"recovery latency p{legend}"
                                + " - {{%s}}" % NODE_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by (le) (rate({metric('recovery_latency_sum')}[$__rate_interval])) / sum by (le) (rate({metric('recovery_latency_count')}[$__rate_interval])) > 0",
                            "recovery latency avg",
                        ),
                    ],
                    ["last"],
                ),
            ],
        )
    ]


def section_compaction(outer_panels):
    successful_compaction_fiiler = "result='SUCCESS'"
    failed_compaction_filter = "result!='SUCCESS'"
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Compaction",
            [
                panels.timeseries_count(
                    "SSTable Count",
                    "The number of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_sst_num')}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "SSTable Size(KB)",
                    "The size(KB) of SSTables at each level",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}, level_index)",
                            "L{{level_index}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Commit Flush Bytes by Table",
                    "The  of bytes that have been written by commit epoch per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_commit_write_throughput')}[$__rate_interval])) by (table_id)",
                            "write - {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Failure Count",
                    "The number of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency', failed_compaction_filter)}) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Success Count",
                    "The number of compactions from one level to another level that have completed or failed",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_frequency', successful_compaction_fiiler)}) by (compactor, group, task_type, result)",
                            "{{task_type}} - {{result}} - group-{{group}} @ {{compactor}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Skip Count",
                    "The number of compactions from one level to another level that have been skipped.",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_skip_compact_frequency')}[$__rate_interval])) by (level, type)",
                            "{{level}}-{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Task L0 Select Level Count",
                    "Avg l0 select_level_count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_l0_compact_level_count_count')}[$__rate_interval])) > 0",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compaction Task File Count",
                    "Avg file count of the compact task, and categorize it according to different cg, levels and task types",
                    [
                        panels.target(
                            f"sum by(le, group, type)(irate({metric('storage_compact_task_file_count_sum')}[$__rate_interval]))  / sum by(le, group, type)(irate({metric('storage_compact_task_file_count_count')}[$__rate_interval])) > 0",
                            "avg cg{{group}}@{{type}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Task Size Distribution",
                    "The distribution of the compact task size triggered, including p90 and max. and categorize it according to different cg, levels and task types.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('storage_compact_task_size_bucket')}[$__rate_interval])) by (le, group, type))",
                                f"p{legend}" + " - cg{{group}}@{{type}}",
                            ),
                            [50, 90, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compactor Running Task Count",
                    "The number of compactions from one level to another level that are running.",
                    [
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_num')}) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "compactor_task_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('storage_compact_task_pending_parallelism')}) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "compactor_task_pending_parallelism - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Compaction Duration",
                    "compact-task: The total time have been spent on compaction.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_task_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-task p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [50, 90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compact_sst_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"compact-key-range p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_get_table_id_total_time_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"get-table-id p{legend}"
                                + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('compactor_remote_read_time_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}))",
                                f"remote-io p{legend}" + " - {{%s}}" % COMPONENT_LABEL,
                            ),
                            [90, "max"],
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(rate({metric('compute_refill_cache_duration_bucket')}[$__rate_interval])) by (le))",
                            "compute_apply_version_duration_p99",
                        ),
                        panels.target(
                            f"sum by(le)(rate({metric('compactor_compact_task_duration_sum')}[$__rate_interval])) / sum by(le)(rate({metric('compactor_compact_task_duration_count')}[$__rate_interval])) > 0",
                            "compact-task avg",
                        ),
                        panels.target(
                            f"sum by(le)(rate({metric('state_store_compact_sst_duration_sum')}[$__rate_interval])) / sum by(le)(rate({metric('state_store_compact_sst_duration_count')}[$__rate_interval])) > 0",
                            "compact-key-range avg",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Compaction Throughput",
                    "KBs read from next level during history compactions to next level",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by({COMPONENT_LABEL}) + sum(rate("
                            f"{metric('storage_level_compact_read_curr')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "read - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_write_build_l0_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_fast_compact_bytes')}[$__rate_interval]))by ({COMPONENT_LABEL})",
                            "fast compact - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Compaction Write Bytes(GiB)",
                    "The number of bytes that have been written by compaction."
                    "Flush refers to the process of compacting Memtables to SSTables at Level 0."
                    "Write refers to the process of compacting SSTables at one level to another level.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) by ({COMPONENT_LABEL})",
                            "write - {{%s}}" % COMPONENT_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('compactor_write_build_l0_bytes')}) by ({COMPONENT_LABEL})",
                            "flush - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Compaction Write Amplification",
                    "Write amplification is the amount of bytes written to the remote storage by compaction for each "
                    "one byte of flushed SSTable data. Write amplification is by definition higher than 1.0 because "
                    "we write each piece of data to L0, and then write it again to an SSTable, and then compaction "
                    "may read this piece of data and write it to a new SSTable, that's another write.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_compact_write')}) / sum({metric('compactor_write_build_l0_bytes')}) > 0",
                            "write amplification",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting SSTable Count",
                    "The number of SSTables that is being compacted at each level",
                    [
                        panels.target(
                            f"{metric('storage_level_compact_cnt')}", "L{{level_index}}"
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Compacting Task Count",
                    "num of compact_task",
                    [
                        panels.target(
                            f"{metric('storage_level_compact_task_cnt')}", "{{task}}"
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "KBs Read/Write by Level",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_read_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                        panels.target(
                            f"sum(rate({metric('storage_level_compact_write')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Count of SSTs Read/Write by level",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_write_sstn')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} write to next level",
                        ),
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_next')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from next level",
                        ),
                        panels.target(
                            f"sum(irate({metric('storage_level_compact_read_sstn_curr')}[$__rate_interval])) by (le, group, level_index)",
                            "cg{{group}}-L{{level_index}} read from current level",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Bloom Filter Size",
                    "For observing bloom_filter size, sstable file size, sstable block size etc.",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_bloom_filter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "bloom_filter_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_bloom_filter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "bloom_filter_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable File Size",
                    "For observing sstable file size",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_file_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_file_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_file_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_file_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Block Size",
                    "For observing sstable block size",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(rate({metric('compactor_sstable_block_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_block_size_p50 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(rate({metric('compactor_sstable_block_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "sstable_block_size_p90 - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Hummock Sstable Avg Key And Value Count",
                    "For observing avg key and value count",
                    [
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_key_size_count')}[$__rate_interval])) > 0",
                            "avg_key_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('compactor_sstable_avg_value_size_count')}[$__rate_interval]))",
                            "avg_value_size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Hummock Remote Read Duration",
                    "Total time of operations which read from remote storage when enable prefetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_remote_read_time_per_task_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"remote-io p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [90, "max"],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Compactor Iter keys",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('compactor_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type)",
                            "iter keys flow - {{type}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Lsm Compact Pending Bytes",
                    "bytes of Lsm tree needed to reach balance",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_pending_bytes')}) by ({NODE_LABEL}, group)",
                            "compact pending bytes - {{group}} @ {{%s}} " % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Lsm Level Compression Ratio",
                    "compression ratio of each level of the lsm tree",
                    [
                        panels.target(
                            f"sum({metric('storage_compact_level_compression_ratio')}) by ({NODE_LABEL}, group, level, algorithm)",
                            "lsm compression ratio - cg{{group}} @ L{{level}} - {{algorithm}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Compaction Group Count",
                    "The number of compaction groups",
                    [
                        panels.target(
                            f"{metric('storage_compaction_group_count')}",
                            "compaction group count",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Compaction Group Size",
                    "The size of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_size')}) by (group)",
                            "compaction group size - cg{{group}}",
                        ),
                    ],
                ),

                panels.timeseries_count(
                    "Compaction Group File Count",
                    "The file count of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_file_count')}) by (group)",
                            "compaction group file count - cg{{group}}",
                        ),
                    ],
                ),

                panels.timeseries_bytes(
                    "Compaction Group Throughput",
                    "The throughput of compaction group",
                    [
                        panels.target(
                            f"sum({metric('storage_compaction_group_throughput')}) by (group)",
                            "compaction group throughput - cg{{group}}",
                        ),
                    ],
                ),

                 panels.timeseries_count(
                    "Compaction Group Schedule",
                    "The times of move_state_table occurs",
                    [
                        panels.target(
                            f"sum({table_metric('storage_split_compaction_group_count')}) by (group)",
                            "split compaction group - left cg{{group}}",
                        ),
                        panels.target(
                            f"sum({table_metric('storage_merge_compaction_group_count')}) by (group)",
                            "merge compaction group - left cg{{group}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_object_storage(outer_panels):
    panels = outer_panels.sub_panel()
    operation_rate_blacklist = "type!~'streaming_read'"
    operation_duration_blacklist = "type!~'streaming_read'"
    put_op_types = ["upload", "streaming_upload"]
    post_op_types = [
        "streaming_upload_init",
        "streaming_upload_finish",
        "delete_objects",
    ]
    list_op_types = ["list"]
    delete_op_types = ["delete"]
    read_op_types = ["read", "streaming_read_init", "metadata"]
    write_op_filter = f"type=~'({'|'.join(put_op_types + post_op_types + list_op_types + delete_op_types)})'"
    read_op_filter = f"type=~'({'|'.join(read_op_types)})'"
    return [
        outer_panels.row_collapsed(
            "Object Storage",
            [
                panels.timeseries_bytes_per_sec(
                    "Operation Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_read_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "read - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_write_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "write - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Operation Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_latency_bucket', operation_duration_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                "{{type}}"
                                + f" p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('object_store_operation_latency_sum', operation_duration_blacklist)}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('object_store_operation_latency_count')}[$__rate_interval])) > 0",
                            "{{type}} avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', operation_rate_blacklist)}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', write_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-write - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('object_store_operation_latency_count', read_op_filter)}[$__rate_interval])) by (le, media_type, {COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{media_type}}-read - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Operation Size",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('object_store_operation_bytes_bucket')}[$__rate_interval])) by (le, type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "{{type}}"
                            + f" p{legend}"
                            + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        [50, 99, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Operation Failure Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Operation Retry Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('object_store_request_retry_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                            "{{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Realtime)",
                    "There are two types of operations: 1. GET, SELECT, and DELETE, they cost 0.0004 USD per 1000 "
                    "requests. 2. PUT, COPY, POST, LIST, they cost 0.005 USD per 1000 requests."
                    "Reading from S3 across different regions impose extra cost. This metric assumes 0.01 USD per 1GB "
                    "data transfer. Please checkout AWS's pricing model for more accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('object_store_read_bytes')}) * 0.01 / 1000 / 1000 / 1000",
                            "(Cross Region) Data Transfer Cost",
                            True,
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', read_op_filter)}) * 0.0004 / 1000",
                            "GET, SELECT, and all other Requests Cost",
                        ),
                        panels.target(
                            f"sum({metric('object_store_operation_latency_count', write_op_filter)}) * 0.005 / 1000",
                            "PUT, COPY, POST, LIST Requests Cost",
                        ),
                    ],
                ),
                panels.timeseries_dollar(
                    "Estimated S3 Cost (Monthly)",
                    "This metric uses the total size of data in S3 at this second to derive the cost of storing data "
                    "for a whole month. The price is 0.023 USD per GB. Please checkout AWS's pricing model for more "
                    "accurate calculation.",
                    [
                        panels.target(
                            f"sum({metric('storage_level_total_file_size')}) by ({NODE_LABEL}) * 0.023 / 1000 / 1000",
                            "Monthly Storage Cost",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming",
            [
                panels.timeseries_count(
                    "Barrier Number",
                    "The number of barriers that have been ingested but not completely processed. This metric reflects the "
                    "current level of congestion within the system.",
                    [
                        panels.target(f"{metric('all_barrier_nums')}", "all_barrier {{database_id}}"),
                        panels.target(
                            f"{metric('in_flight_barrier_nums')}", "in_flight_barrier {{database_id}}"
                        ),
                        panels.target(
                            f"{metric('meta_snapshot_backfill_inflight_barrier_num')}",
                            "snapshot_backfill_in_flight_barrier {{table_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Latency",
                    "The time that the data between two consecutive barriers gets fully processed, i.e. the computation "
                    "results are made durable into materialized views or sink to external systems. This metric shows to users "
                    "the freshness of materialized views.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_latency_avg",
                        ),
                    ]
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id, barrier_type))",
                            f"snapshot_backfill_barrier_latency_p{legend} table_id[{{{{table_id}}}}] {{{{barrier_type}}}}",
                        ),
                        [50, 90, 99, 999, "max"],
                    ),
                ),
                panels.timeseries(
                    "Barrier pending time (secs)",
                    "The duration from the last committed barrier's epoch time to the current time. This metric reflects the "
                    "data freshness of the system. During this time, no new data has been committed.",
                    [
                        panels.target(
                            f"timestamp({metric('last_committed_barrier_time')}) - {metric('last_committed_barrier_time')}",
                            "barrier_pending_time",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_output_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Throughput(rows/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(rows).",
                    [
                        panels.target(
                            f"rate({metric('source_partition_input_count')}[$__rate_interval])",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s)",
                    "The figure shows the number of bytes read by each source per second.",
                    [
                        panels.target(
                            f"(sum by (source_id, source_name, fragment_id)(rate({metric('source_partition_input_bytes')}[$__rate_interval])))/(1000*1000)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        )
                    ],
                ),
                panels.timeseries_bytesps(
                    "Source Throughput(MB/s) Per Partition",
                    "Each query is executed in parallel with a user-defined parallelism. This figure shows the throughput of "
                    "each parallelism. The throughput of all the parallelism added up is equal to Source Throughput(MB/s).",
                    [
                        panels.target(
                            f"(rate({metric('source_partition_input_bytes')}[$__rate_interval]))/(1000*1000)",
                            "actor={{actor_id}} source={{source_id}} partition={{partition}} fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_rowsps(
                    "Source Backfill Throughput(rows/s)",
                    "The figure shows the number of rows read by each source per second.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_source_backfill_rows_counts')}[$__rate_interval])) by (source_id, source_name, fragment_id)",
                            "{{source_id}} {{source_name}} (fragment {{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Upstream Status",
                    "Monitor each source upstream, 0 means the upstream is not normal, 1 means the source is ready.",
                    [
                        panels.target(
                            f"{metric('source_status_is_up')}",
                            "source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_ops(
                    "Source Split Change Events frequency(events/s)",
                    "Source Split Change Events frequency by source_id and actor_id",
                    [
                        panels.target(
                            f"rate({metric('stream_source_split_change_event_count')}[$__rate_interval])",
                            "source={{source_name}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Kafka Consumer Lag Size",
                    "Kafka Consumer Lag Size by source_id, partition and actor_id",
                    [
                        panels.target(
                            f"clamp_min({metric('source_kafka_high_watermark')} - on(source_id, partition) group_right() {metric('source_latest_message_id')}, 0)",
                            "source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
                # TODO: These 2 metrics should be deprecated because they are unaware of Log Store
                # Let's remove them when all sinks are migrated to Log Store
                panels.timeseries_rowsps(
                    "Sink Executor Throughput (rows/s)",
                    "The number of rows streamed into the SinkExecutor per second. For sinks with 'sink_decouple = true', please refer to the 'Sink Metrics' section",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)",
                            "sink {{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('stream_sink_input_row_count')}[$__rate_interval])) by (sink_id, actor_id) * on(actor_id) group_left(sink_name) {metric('sink_info')}",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Sink Executor Throughput (MB/s)",
                    "The figure shows the number of bytes written SinkExecutor per second. For sinks with 'sink_decouple = true', please refer to the 'Sink Metrics' section",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_sink_input_bytes')}[$__rate_interval])) by (sink_id) * on(sink_id) group_left(sink_name) group({metric('sink_info')}) by (sink_id, sink_name)) / (1000*1000)",
                            "sink {{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"(sum(rate({metric('stream_sink_input_bytes')}[$__rate_interval])) by (sink_id, actor_id) * on(actor_id) group_left(sink_name) {metric('sink_info')}) / (1000*1000)",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Materialized View Throughput (rows/s)",
                    "The figure shows the number of rows written into each materialized view per second.",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_mview_input_row_count')}[$__rate_interval])) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "mview {{table_id}} {{table_name}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_mview_input_row_count')}[$__rate_interval]) * on(fragment_id, table_id) group_left(table_name) {metric('table_info')}",
                            "mview {{table_id}} {{table_name}} - actor {{actor_id}} fragment_id {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Snapshot-Read Throughput (rows/s)",
                    "Rows/sec that we read from the backfill snapshot by materialized view",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_snapshot_read_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_snapshot_backfill_consume_snapshot_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} {{stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Backfill Upstream Throughput (rows/s)",
                    "Total number of rows that have been output from the backfill upstream",
                    [
                        panels.target(
                            f"""
                                sum by (table_id) (
                                  rate({metric('stream_backfill_upstream_output_row_count', node_filter_enabled=False, table_id_filter_enabled=True)}[$__rate_interval])
                                )
                                * on(table_id) group_left(table_name) (
                                  group({metric('table_info', node_filter_enabled=False)}) by (table_name, table_id)
                                )
                            """,
                            "table_name={{table_name}} table_id={{table_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Send Latency",
                    "The duration between the time point when the scheduled barrier needs to be sent and the time point when "
                    "the barrier gets actually sent to all the compute nodes. Developers can thus detect any internal "
                    "congestion.",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_send_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_send_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_send_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_send_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_send_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier In-Flight Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_inflight_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_inflight_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"max(sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_inflight_duration_seconds_count')}[$__rate_interval]))) > 0",
                            "barrier_inflight_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Sync Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('stream_barrier_sync_storage_duration_seconds_bucket')}[$__rate_interval])) by (le, {NODE_LABEL}))",
                            f"barrier_sync_latency_p{legend}"
                            + " - {{%s}}" % NODE_LABEL,
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_sum')}[$__rate_interval]))  / sum by(le, {NODE_LABEL})(rate({metric('stream_barrier_sync_storage_duration_seconds_count')}[$__rate_interval])) > 0",
                            "barrier_sync_latency_avg - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Barrier Wait Commit Latency",
                    "",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le))",
                            f"barrier_wait_commit_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('meta_barrier_wait_commit_duration_seconds_sum')}[$__rate_interval]) / rate({metric('meta_barrier_wait_commit_duration_seconds_count')}[$__rate_interval]) > 0",
                            "barrier_wait_commit_avg",
                        ),
                    ]
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_barrier_wait_commit_duration_seconds_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_barrier_wait_commit_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('meta_snapshot_backfill_upstream_wait_progress_latency_bucket')}[$__rate_interval])) by (le, table_id))",
                            f"snapshot_backfill_upstream_wait_progress_latency_p{legend} table_id[{{{{table_id}}}}]",
                        ),
                        [50, 90, 99, 999, "max"],
                    ),
                ),
                panels.timeseries_ops(
                    "Earliest In-Flight Barrier Progress",
                    "The number of actors that have processed the earliest in-flight barriers per second. "
                    "This metric helps users to detect potential congestion or stuck in the system.",
                    [
                        panels.target(
                            f"rate({metric('stream_barrier_manager_progress')}[$__rate_interval])",
                            "{{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Current Epoch of Materialize Views",
                    "The current epoch that the Materialize Executors are processing. If an MV's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled `current_epoch` makes sense.
                            f"min({metric('stream_mview_current_epoch')} != 0) by (table_id) * on(table_id) group_left(table_name) group({metric('table_info')}) by (table_id, table_name)",
                            "{{table_id}} {{table_name}}",
                            ),
                    ]
                ),
                panels.timeseries_latency(
                    "Snapshot Backfill Lag",
                    "",
                    [
                        panels.target(
                            f"{metric('meta_snapshot_backfill_upstream_lag')} / (2^16) / 1000",
                            "lag @ {{table_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming_cdc(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming CDC",
            [
                panels.timeseries_rowsps(
                    "CDC Backfill Snapshot Read Throughput(rows)",
                    "Total number of rows that have been read from the cdc backfill snapshot",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_snapshot_read_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "CDC Backfill Upstream Throughput(rows)",
                    "Total number of rows that have been output from the cdc backfill upstream",
                    [
                        panels.target(
                            f"rate({table_metric('stream_cdc_backfill_upstream_output_row_count')}[$__rate_interval])",
                            "table_id={{table_id}} actor={{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "CDC Consume Lag Latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('source_cdc_event_lag_duration_milliseconds_bucket')}[$__rate_interval])) by (le, table_name))",
                                f"lag p{legend}" + " - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "CDC Source Errors",
                    "",
                    [
                        panels.target(
                            f"sum({metric('cdc_source_error')}) by (connector_name, source_id, error_msg)",
                            "{{connector_name}}: {{error_msg}} ({{source_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Failure Count",
                    "Total number of failed auto schema change attempts of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_failure_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Auto Schema Change Success Count",
                    "Total number of succeeded auto schema change of CDC Table",
                    [
                        panels.target(
                            f"sum({metric('auto_schema_change_success_cnt')}) by (table_id, table_name)",
                            "{{table_id}} - {{table_name}}",
                        )
                    ],
                    ["last"],
                ),
                panels.timeseries_latency(
                    "Auto Schema Change Latency (sec)",
                    "Latency of Auto Schema Change Process",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('auto_schema_change_latency_bucket')}[$__rate_interval])) by (le, table_id, table_name))",
                                f"lag p{legend}" + "{{table_id}} - {{table_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_streaming_actors(outer_panels: Panels):
    # The actor_id can be masked due to metrics level settings.
    # We use this filter to suppress the actor-level panels if applicable.
    actor_level_filter = "actor_id!=''"
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors",
            [
                panels.timeseries_percentage(
                    "Actor Output Blocking Time Ratio (Backpressure)",
                    "We first record the total blocking duration(ns) of output buffer of each actor. It shows how "
                    "much time it takes an actor to process a message, i.e. a barrier, a watermark or rows of data, "
                    "on average. Then we divide this duration by 1 second and show it as a percentage.",
                    [
                        # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                        # Thus to calculate the average, we need to manually divide the actor count.
                        #
                        # Note: actor_count is equal to the number of dispatchers for a given downstream fragment,
                        # this holds true as long as we don't support multiple edges between two fragments.
                        panels.target(
                            f"sum(rate({metric('stream_actor_output_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, downstream_fragment_id) \
                            / ignoring (downstream_fragment_id) group_left sum({metric('stream_actor_count')}) by (fragment_id) \
                            / 1000000000",
                            "fragment {{fragment_id}}->{{downstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_actor_input_buffer_blocking_duration_ns')}[$__rate_interval])) by (fragment_id, upstream_fragment_id) / 1000000000",
                            "fragment {{fragment_id}}<-{{upstream_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Actor Input Throughput (rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_in_record_cnt')}[$__rate_interval])) by (fragment_id, upstream_fragment_id)",
                            "fragment total {{fragment_id}}<-{{upstream_fragment_id}}",
                        ),
                        panels.target(
                            f"rate({metric('stream_actor_in_record_cnt', actor_level_filter)}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Actor Output Throughput (rows/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_actor_out_record_cnt')}[$__rate_interval])) by (fragment_id)",
                            "fragment total {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_actor_out_record_cnt', actor_level_filter)}[$__rate_interval])",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage",
                    "The operator-level memory usage statistics collected by each LRU cache",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')}) by (table_id, desc)",
                            "table total {{table_id}}: {{desc}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_memory_usage', actor_level_filter)}",
                            "actor {{actor_id}} table {{table_id}}: {{desc}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Executor Cache Memory Usage of Materialized Views",
                    "Memory usage aggregated by materialized views",
                    [
                        panels.target(
                            f"sum({metric('stream_memory_usage')} * on(table_id) group_left(materialized_view_id) {metric('table_info')}) by (materialized_view_id)",
                            "materialized view {{materialized_view_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Temporal Join Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (fragment_id)",
                            "temporal join cache miss, table_id {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])",
                            "temporal join cache miss, table_id {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Materialize Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache hit count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "total cached count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_hit_count', actor_level_filter)}[$__rate_interval])",
                            "cache hit count - actor {{actor_id}} table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_materialize_cache_total_count', actor_level_filter)}[$__rate_interval])",
                            "total cached count - actor {{actor_id}} table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Over Window Executor Cache",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache lookup count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "cache miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_lookup_count')}[$__rate_interval])",
                            "cache lookup count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({table_metric('stream_over_window_cache_miss_count')}[$__rate_interval])",
                            "cache miss count - table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache lookup count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_left_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache left miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_range_cache_right_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "partition range cache right miss count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Over Window Executor State Computation",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_accessed_entry_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "accessed entry count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_compute_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "compute count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({table_metric('stream_over_window_same_output_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "same output count - table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({metric('stream_join_lookup_miss_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id) ) / (sum(rate({metric('stream_join_lookup_total_count')}[$__rate_interval])) by (side, join_table_id, degree_table_id, fragment_id)) >= 0",
                            "Join executor cache miss ratio - - {{side}} side, join_table_id {{join_table_id}} degree_table_id {{degree_table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_state_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_state_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Agg state cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_agg_distinct_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_agg_distinct_total_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Distinct agg cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_group_top_n_appendonly_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_group_top_n_appendonly_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream group top n appendonly cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_lookup_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_lookup_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream lookup cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_temporal_join_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_temporal_join_total_query_cache_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Stream temporal join cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"1 - (sum(rate({metric('stream_materialize_cache_hit_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_materialize_cache_total_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Materialize executor cache miss ratio - table {{table_id}} fragment {{fragment_id}}"
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_cache_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window cache miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_left_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache left miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                        panels.target(
                            f"(sum(rate({metric('stream_over_window_range_cache_right_miss_count')}[$__rate_interval])) by (table_id, fragment_id) ) / (sum(rate({metric('stream_over_window_range_cache_lookup_count')}[$__rate_interval])) by (table_id, fragment_id)) >= 0",
                            "Over window partition range cache right miss ratio - table {{table_id}} fragment {{fragment_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Executor Barrier Align Per Second",
                    "",
                    [
                        # The metrics might be pre-aggregated locally on each compute node when `actor_id` is masked due to metrics level settings.
                        # Thus to calculate the average, we need to manually divide the actor count.
                        panels.target(
                            f"sum(rate({metric('stream_barrier_align_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id, wait_side, executor) \
                            / ignoring (wait_side, executor) group_left sum({metric('stream_actor_count')}) by (fragment_id)",
                            "fragment avg {{fragment_id}} {{wait_side}} {{executor}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_barrier_align_duration_ns', actor_level_filter)}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} fragment {{fragment_id}} {{wait_side}} {{executor}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency(
                    "Merger Barrier Align",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_merge_barrier_align_duration_bucket')}[$__rate_interval])) by (le, fragment_id, {COMPONENT_LABEL}))",
                                f"p{legend} - fragment {{{{fragment_id}}}} - {{{{{COMPONENT_LABEL}}}}}",
                            ),
                            [90, 99, 999, "max"],
                        ),
                        panels.target(
                            f"sum by(le, fragment_id, job)(rate({metric('stream_merge_barrier_align_duration_sum')}[$__rate_interval])) / sum by(le,fragment_id,{COMPONENT_LABEL}) (rate({metric('stream_merge_barrier_align_duration_count')}[$__rate_interval])) > 0",
                            "avg - fragment {{fragment_id}} - {{%s}}" % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Input Blocking Time Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id)",
                            "fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_actor_input_waiting_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Join Actor Match Duration Per Second",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000) by (fragment_id,side)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_join_match_duration_ns')}[$__rate_interval]) / 1000000000",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Cached Keys",
                    "Multiple rows with distinct primary keys may have the same join key. This metric counts the "
                    "number of join keys in the executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_join_cached_entry_count')}) by (fragment_id, side)",
                            "fragment {{fragment_id}} {{side}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_join_cached_entry_count')}",
                            "actor {{actor_id}} {{side}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Join Executor Matched Rows",
                    "The number of matched rows on the opposite side",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target_hidden(
                                f"histogram_quantile({quantile}, sum(rate({metric('stream_join_matched_join_keys_bucket')}[$__rate_interval])) by (le, fragment_id, table_id, {COMPONENT_LABEL}))",
                                f"p{legend} - fragment {{{{fragment_id}}}} table_id {{{{table_id}}}} - {{{{{COMPONENT_LABEL}}}}}",
                            ),
                            [90, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, fragment_id, table_id) (rate({metric('stream_join_matched_join_keys_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, fragment_id, table_id) (rate({table_metric('stream_join_matched_join_keys_count')}[$__rate_interval])) >= 0",
                            "avg - fragment {{fragment_id}} table_id {{table_id}} - {{%s}}"
                            % COMPONENT_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_actor_ops(
                    "Aggregation Executor Cache Statistics For Each StreamChunk",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level cache miss - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])) by (table_id, fragment_id)",
                            "chunk-level total lookups - table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_miss_count')}[$__rate_interval])",
                            "chunk-level cache miss  - table {{table_id}} actor {{actor_id}}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_agg_chunk_lookup_total_count')}[$__rate_interval])",
                            "chunk-level total lookups  - table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Cached Keys",
                    "The number of keys cached in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_agg_distinct_cached_entry_count')}) by (table_id, fragment_id)",
                            "stream agg distinct cached keys count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_cached_entry_count')}",
                            "stream agg cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_distinct_cached_entry_count')}",
                            "stream agg distinct cached keys count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Aggregation Dirty Groups Count",
                    "The number of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_count')}) by (table_id, fragment_id)",
                            "stream agg dirty groups count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_count')}",
                            "stream agg dirty groups count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Aggregation Dirty Groups Heap Size",
                    "The total heap size of dirty (unflushed) groups in each hash aggregation executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_agg_dirty_groups_heap_size')}) by (table_id, fragment_id)",
                            "stream agg dirty groups heap size | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_agg_dirty_groups_heap_size')}",
                            "stream agg dirty groups heap size | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "TopN Cached Keys",
                    "The number of keys cached in each top_n executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_group_top_n_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_group_top_n_appendonly_cached_entry_count')}) by (table_id, fragment_id)",
                            "group top_n appendonly cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_cached_entry_count')}",
                            "group top_n cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_group_top_n_appendonly_cached_entry_count')}",
                            "group top_n appendonly cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Temporal Join Cache Keys",
                    "The number of keys cached in temporal join executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_temporal_join_cached_entry_count')}) by (table_id, fragment_id)",
                            "Temporal Join cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_temporal_join_cached_entry_count')}",
                            "Temporal Join cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Lookup Cached Keys",
                    "The number of keys cached in lookup executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_lookup_cached_entry_count')}) by (table_id, fragment_id)",
                            "lookup cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_lookup_cached_entry_count')}",
                            "lookup cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Over Window Cached Keys",
                    "The number of keys cached in over window executor's executor cache.",
                    [
                        panels.target(
                            f"sum({metric('stream_over_window_cached_entry_count')}) by (table_id, fragment_id)",
                            "over window cached count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"{metric('stream_over_window_cached_entry_count')}",
                            "over window cached count | table {{table_id}} actor {{actor_id}}",
                        ),
                        panels.target(
                            f"sum({metric('stream_over_window_range_cache_entry_count')}) by (table_id, fragment_id)",
                            "over window partition range cache entry count | table {{table_id}} fragment {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_rowsps(
                    "Executor Throughput",
                    "When enabled, this metric shows the input throughput of each executor.",
                    [
                        panels.target(
                            f"sum(rate({metric('stream_executor_row_count')}[$__rate_interval])) by (executor_identity, fragment_id)",
                            "{{executor_identity}} fragment total {{fragment_id}}",
                        ),
                        panels.target_hidden(
                            f"rate({metric('stream_executor_row_count', actor_level_filter)}[$__rate_interval])",
                            "{{executor_identity}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Current Epoch of Actors",
                    "The current epoch that the actors are processing. If an actor's epoch is far behind the others, "
                    "it's very likely to be the performance bottleneck",
                    [
                        panels.target(
                            # Here we use `min` but actually no much difference. Any of the sampled epochs makes sense.
                            f"min({metric('stream_actor_current_epoch')} != 0) by (fragment_id)",
                            "fragment {{fragment_id}}",
                            ),
                    ]
                ),
            ],
        )
    ]


def section_streaming_actors_tokio(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Actors (Tokio)",
            [
                panels.timeseries_actor_latency(
                    "Actor Execution Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_actor_execution_time')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Fast Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Fast Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_fast_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_fast_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Slow Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Slow Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_slow_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_slow_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Poll Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Poll Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_poll_duration')}[$__rate_interval]) / rate({metric('stream_actor_poll_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Idle Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Idle Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_idle_duration')}[$__rate_interval]) / rate({metric('stream_actor_idle_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Total Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_ops_small(
                    "Tokio: Actor Scheduled Count",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_actor_latency_small(
                    "Tokio: Actor Scheduled Avg Time",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_actor_scheduled_duration')}[$__rate_interval]) / rate({metric('stream_actor_scheduled_cnt')}[$__rate_interval]) > 0",
                            "{{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_streaming_exchange(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Streaming Exchange",
            [
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Send Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_send_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Fragment-level Remote Exchange Recv Throughput",
                    "",
                    [
                        panels.target(
                            f"rate({metric('stream_exchange_frag_recv_size')}[$__rate_interval])",
                            "{{up_fragment_id}}->{{down_fragment_id}}",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_streaming_errors(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "User Streaming Errors",
            [
                panels.timeseries_count(
                    "Compute Errors by Type",
                    "Errors that happened during computation. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"sum({metric('user_compute_error')}) by (error_type, executor_name, fragment_id)",
                            "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Source Errors by Type",
                    "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"sum({metric('user_source_error')}) by (error_type, source_id, source_name, fragment_id)",
                            "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Parquet Source Skip Count",
                    "Errors that happened during source data ingestion. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"{metric('parquet_source_skip_row_count')}",
                            "source_id={{source_id}} @ source_name =  {{source_name}}  fragment_id={{fragment_id}}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Sink Errors by Type",
                    "Errors that happened during data sink out. Check the logs for detailed error message.",
                    [
                        panels.target(
                            f"sum({metric('user_sink_error')}) by (error_type, sink_id, sink_name, fragment_id)",
                            "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_batch(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Batch Metrics",
            [
                panels.timeseries_row(
                    "Exchange Recv Row Number",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_exchange_recv_row_number')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Mpp Task Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_task_num')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "Batch Mem Usage",
                    "All memory usage of batch executors in bytes",
                    [
                        panels.target(
                            f"{metric('compute_batch_total_mem')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('frontend_batch_total_mem')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Batch Heartbeat Worker Number",
                    "",
                    [
                        panels.target(
                            f"{metric('batch_heartbeat_worker_num')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Row SeqScan Next Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('batch_row_seq_scan_next_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"row_seq_scan next p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('batch_row_seq_scan_next_duration_count')}[$__rate_interval])) > 0",
                            "row_seq_scan next avg - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Batch Spill Throughput",
                    "Disk throughputs of spilling-out in the bacth query engine",
                    [
                        panels.target(
                            f"sum(rate({metric('batch_spill_read_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "read - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('batch_spill_write_bytes')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "write - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_frontend(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Frontend",
            [
                panels.timeseries_count(
                    "Active Sessions",
                    "Number of active sessions",
                    [
                        panels.target(
                            f"{metric('frontend_active_sessions')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Local Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('frontend_query_counter_local_execution')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_query_per_sec(
                    "Query Per Second (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"rate({metric('distributed_completed_query_counter')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "The Number of Running Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_running_query_num')}",
                            "The number of running query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Rejected queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_rejected_query_counter')}",
                            "The number of rejected query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "The Number of Completed Queries (Distributed Query Mode)",
                    "",
                    [
                        panels.target(
                            f"{metric('distributed_completed_query_counter')}",
                            "The number of completed query in distributed execution mode",
                        ),
                    ],
                    ["last"],
                ),
                panels.timeseries_count(
                    "Subsription Cursor Nums",
                    "The number of valid and invalid subscription cursor",
                    [
                        panels.target(
                            f"{metric('subsription_cursor_nums')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('invalid_subsription_cursor_nums')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Subscription Cursor Error Count",
                    "The subscription error num of cursor",
                    [
                        panels.target(
                            f"{metric('subscription_cursor_error_count')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Query Duration(ms)",
                    "The amount of time a query exists inside the cursor",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_query_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Declare Duration(ms)",
                    "Subscription cursor duration of declare",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_declare_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Fetch Duration(ms)",
                    "Subscription cursor duration of fetch",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_fetch_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Subscription Cursor Last Fetch Duration(ms)",
                    "Since the last fetch, the time up to now",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('subscription_cursor_last_fetch_duration_bucket')}[$__rate_interval])) by (le, subscription_name))",
                                f"p{legend} - {{{{subscription_name}}}}",
                            ),
                            [50, 99, "max"],
                        )
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency (Distributed Query Mode)",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('distributed_query_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Query Latency (Local Query Mode)",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('frontend_latency_local_execution_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_hummock_read(outer_panels):
    panels = outer_panels.sub_panel()
    meta_miss_filter = "type='meta_miss'"
    meta_total_filter = "type='meta_total'"
    data_miss_filter = "type='data_miss'"
    data_total_filter = "type='data_total'"
    file_cache_get_filter = "op='get'"

    return [
        outer_panels.row_collapsed(
            "Hummock (Read)",
            [
                panels.timeseries_ops(
                    "Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_sst_store_block_request_counts')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, table_id, type)",
                            "{{table_id}} @ {{type}} - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, type)",
                            "total_meta_miss_count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Cache Size",
                    "Hummock has three parts of memory usage: 1. Meta Cache 2. Block Cache"
                    "This metric shows the real memory usage of each of these three caches.",
                    [
                        panels.target(
                            f"avg({metric('state_store_meta_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "meta cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_block_cache_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "data cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"avg({metric('state_store_prefetch_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "prefetch cache - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Cache Miss Ratio",
                    "",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', meta_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) >= 0",
                            "meta cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"(sum(rate({table_metric('state_store_sst_store_block_request_counts', data_miss_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) / (sum(rate({table_metric('state_store_sst_store_block_request_counts', data_total_filter)}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)) >= 0",
                            "block cache miss ratio - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Block Cache Efficiency",
                    "Histogram of the estimated hit ratio of a block while in the block cache.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"clamp_max(histogram_quantile({quantile}, sum(rate({metric('block_efficiency_histogram_bucket')}[$__rate_interval])) by (le,{COMPONENT_LABEL},{NODE_LABEL})), 1)",
                                f"block cache efficienfy - p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [10, 25, 50, 75, 90, 100],
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Iter keys flow",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_scan_key_counts')}[$__rate_interval])) by ({NODE_LABEL}, type, table_id)",
                            "iter keys flow - {{table_id}} @ {{type}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Merged SSTs",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_merge_sstable_counts_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, table_id, type))",
                                f"# merged ssts p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{type}}"
                                % COMPONENT_LABEL,
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_iter_merge_sstable_counts_count')}[$__rate_interval])) > 0",
                            "# merged ssts avg  - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Read Duration - Get",
                    "Histogram of the latency of Get operations that have been issued to the state store.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_get_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Read Duration - Iter",
                    "Histogram of the time spent on iterator initialization."
                    "Histogram of the time spent on iterator scanning.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_init_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"create_iter_time p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_init_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, iter_type) (rate({metric('state_store_iter_init_duration_count')}[$__rate_interval])) > 0",
                            "create_iter_time avg - {{iter_type}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_scan_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"pure_scan_time p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_iter_scan_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, iter_type) (rate({metric('state_store_iter_scan_duration_count')}[$__rate_interval])) > 0",
                            "pure_scan_time avg - {{iter_type}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Bloom Filter Ops",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter false positive count  - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter positive count - {{table_id}} - {{type}}",
                        ),
                        panels.target(
                            f"sum(irate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)",
                            "bloom filter check count- {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter Positive Rate",
                    "Positive / Total",
                    [
                        panels.target(
                            f"(sum(rate({table_metric('state_store_read_req_bloom_filter_positive_counts')}[$__rate_interval])) by (table_id,type)) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type)) >= 0",
                            "bloom filter positive rate - {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Bloom Filter False-Positive Rate",
                    "False-Positive / Total",
                    [
                        panels.target(
                            f"(((sum(rate({table_metric('state_store_read_req_positive_but_non_exist_counts')}[$__rate_interval])) by (table_id,type))) / (sum(rate({table_metric('state_store_read_req_check_bloom_filter_counts')}[$__rate_interval])) by (table_id,type))) >= 0",
                            "read req bloom filter false positive rate - {{table_id}} - {{type}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Slow Fetch Meta Unhits",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_iter_slow_fetch_meta_cache_unhits')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Read Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "get - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_get_shared_buffer_hit_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "shared_buffer hit - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({table_metric('state_store_iter_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id, iter_type)",
                            "{{iter_type}} - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Get",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_key_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({table_metric('state_store_get_value_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"p{legend} - {{{{table_id}}}} {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Read Item Size - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Materialized View Read Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_iter_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id) + sum((histogram_quantile({quantile}, sum(rate({metric("state_store_get_key_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) + histogram_quantile({quantile}, sum(rate({metric("state_store_get_value_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id)',
                                f"read p{legend} - materialized view {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Read Item Count - Iter",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_item_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id, iter_type))",
                                f"p{legend} - {{{{iter_type}}}} {{{{table_id}}}} @ {{{{{COMPONENT_LABEL}}}}} @ {{{{{NODE_LABEL}}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"{metric('state_store_iter_in_progress_counts')}",
                            "Existing {{iter_type}} count @ {{table_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_iter_log_op_type_counts')}[$__rate_interval])) by (table_id, op_type)",
                            "iter_log op count @ {{table_id}} {{op_type}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Read Throughput - Get",
                    "The size of a single key-value pair when reading by operation Get."
                    "Operation Get gets a single key-value pair with respect to a caller-specified key. If the key does not "
                    "exist in the storage, the size of key is counted into this metric and the size of value is 0.",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_get_key_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL}) + sum(rate({metric('state_store_get_value_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Read Throughput - Iter",
                    "The size of all the key-value paris when reading by operation Iter."
                    "Operation Iter scans a range of key-value pairs.",
                    [
                        panels.target(
                            f"sum(rate({metric('state_store_iter_size_sum')}[$__rate_interval])) by({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Fetch Meta Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_iter_fetch_meta_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"fetch_meta_duration p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id) (rate({table_metric('state_store_iter_fetch_meta_duration_count')}[$__rate_interval])) > 0",
                            "fetch_meta_duration avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Fetch Meta Unhits",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_iter_fetch_meta_cache_unhits')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Safe Version Fetch Count",
                    "",
                    [
                        panels.target(
                            f"{metric('state_store_safe_version_hit')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('state_store_safe_version_miss')}",
                            "",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_hummock_write(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Hummock (Write)",
            [
                panels.timeseries_bytes(
                    "Uploader Memory Size",
                    "This metric shows the real memory usage of uploader.",
                    [
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading memory - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_uploading_task_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading task size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploader imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) - "
                            f"sum({metric('state_store_uploader_uploading_task_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "unflushed imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('uploading_memory_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL}) - "
                            f"sum({metric('state_store_uploader_imm_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "orphan imm size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_old_value_size')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "old value size - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Build and Sync Sstable Duration",
                    "Histogram of time spent on compacting shared buffer to remote storage.",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " Sync duration - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_duration_count')}[$__rate_interval])) > 0",
                            "avg Sync duration - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_uploader_upload_task_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " upload task duration - {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Materialized View Write Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f'sum(histogram_quantile({quantile}, sum(rate({metric("state_store_write_batch_size_bucket")}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)) * on(table_id) group_left(materialized_view_id) (group({metric("table_info")}) by (materialized_view_id, table_id))) by (materialized_view_id, table_name)',
                                f"write p{legend} - materialized view {{{{materialized_view_id}}}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Uploader - Tasks Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_merge_imm_task_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "merge imm tasks - {{table_id}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('state_store_spill_task_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploader spill tasks - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_uploading_task_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "uploading task count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('state_store_uploader_syncing_epoch_count')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "syncing epoch count - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Uploader - Task Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_merge_imm_memory_sz')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "Merging tasks memory size - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_spill_task_size')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},uploader_stage)",
                            "Uploading tasks size - {{uploader_stage}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Write Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write batch - {{table_id}} @ {{%s}} @ {{%s}} "
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('state_store_sync_duration_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "l0 - {{%s}} @ {{%s}} " % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({table_metric('state_store_write_batch_duration_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id))",
                                f"write to shared_buffer p{legend}"
                                + " - {{table_id}} @ {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}, table_id)(rate({table_metric('state_store_write_batch_duration_count')}[$__rate_interval])) > 0",
                            "write to shared_buffer avg - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_write_shared_buffer_sync_time_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"write to object_store p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_write_shared_buffer_sync_time_sum')}[$__rate_interval]))  / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL})(rate({metric('state_store_write_shared_buffer_sync_time_count')}[$__rate_interval])) > 0",
                            "write to object_store - {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Write Item Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_write_batch_tuple_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "write_batch_kv_pair_count - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Write Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({table_metric('state_store_write_batch_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id) / sum(rate({table_metric('state_store_write_batch_size_count')}[$__rate_interval]))by({COMPONENT_LABEL},{NODE_LABEL},table_id) > 0",
                            "shared_buffer - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('compactor_shared_buffer_to_sstable_size_sum')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL}) / sum(rate({metric('compactor_shared_buffer_to_sstable_size_count')}[$__rate_interval]))by({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                            "sync - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Write Batch Size",
                    "This metric shows the statistics of mem_table size on flush. By default only max (p100) is shown.",
                    [
                        panels.target(
                            f"histogram_quantile(1.0, sum(rate({metric('state_store_write_batch_size_bucket')}[$__rate_interval])) by (le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "pmax - {{table_id}} @ {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_sum')}[$__rate_interval])) / sum by(le, table_id, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_write_batch_size_count')}[$__rate_interval])) > 0",
                            "avg - {{table_id}} {{%s}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Mem Table Spill Count",
                    "",
                    [
                        panels.target(
                            f"sum(irate({table_metric('state_store_mem_table_spill_counts')}[$__rate_interval])) by ({COMPONENT_LABEL},{NODE_LABEL},table_id)",
                            "mem table spill table id - {{table_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Checkpoint Sync Size",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_sync_size_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_sum')}[$__rate_interval])) / sum by(le, {COMPONENT_LABEL}, {NODE_LABEL}) (rate({metric('state_store_sync_size_count')}[$__rate_interval])) > 0",
                            "avg - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Event handler pending event number",
                    "",
                    [
                        panels.target(
                            f"sum({metric('state_store_event_handler_pending_event')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Event handle latency",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_event_handler_latency_bucket')}[$__rate_interval])) by (le, event_type, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " {{event_type}} {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('state_store_uploader_wait_poll_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"p{legend}"
                                + " finished_task_wait_poll {{%s}} @ {{%s}}"
                                % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]


def section_hummock_tiered_cache(outer_panels):
    panels = outer_panels.sub_panel()
    file_cache_hit_filter = 'op="lookup",extra="hit"'
    file_cache_miss_filter = 'op="lookup",extra="miss"'
    refill_ops_filter = 'type=~"meta|data",op!~"filtered|ignored"'
    inheritance_parent_lookup_filter = 'type="parent_meta"'
    inheritance_parent_lookup_hit_filter = 'type="parent_meta",op="hit"'
    inheritance_parent_lookup_miss_filter = 'type="parent_meta",op="miss"'
    unit_inheritance_filter = 'type="unit_inheritance"'
    unit_inheritance_hit_filter = 'type="unit_inheritance",op="hit"'
    unit_inheritance_miss_filter = 'type="unit_inheritance",op="miss"'
    block_refill_filter = 'type="block"'
    block_refill_success_filter = 'type="block",op="success"'
    block_refill_unfiltered_filter = 'type="block",op="unfiltered"'
    cache_hit_filter = 'op="hit"'
    cache_miss_filter = 'op="miss"'
    return [
        outer_panels.row_collapsed(
            "Hummock Tiered Cache",
            [
                # hybrid
                panels.timeseries_ops(
                    "Hybrid Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_hybrid_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - hybrid - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Hybrid Cache Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_hybrid_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - hybrid - {{op}} @ {{%s}}" % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Hybrid Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_hybrid_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_hybrid_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_hybrid_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - hybrid - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                # memory
                panels.timeseries_ops(
                    "Memory Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_memory_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - memory - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Memory Cache Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('foyer_memory_usage')}) by (name, {NODE_LABEL})",
                            "{{name}} - memory - size @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Memory Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_memory_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_memory_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_memory_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - memory - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                # storage
                panels.timeseries_ops(
                    "Storage Cache Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - storage - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Storage Cache Inner Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_inner_op_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - storage - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Storage Cache Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - storage - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Storage Cache Inner Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_inner_op_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - storage - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Storage Cache Hit Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) / (sum(rate({metric('foyer_storage_op_total', cache_hit_filter)}[$__rate_interval])) by (name, {NODE_LABEL}) + sum(rate({metric('foyer_storage_op_total', cache_miss_filter)}[$__rate_interval])) by (name, {NODE_LABEL}))",
                            "{{name}} - storage - hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Storage Region Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('foyer_storage_region')}) by (name, type, {NODE_LABEL}) * on(name, {NODE_LABEL}) group_left() avg({metric('foyer_storage_region_size_bytes')}) by (name, type, {NODE_LABEL})",
                            "{{name}} - {{type}} region - size @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                # disk
                panels.timeseries_ops(
                    "Disk Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_disk_io_total')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Disk Op Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('foyer_storage_disk_io_duration_bucket')}[$__rate_interval])) by (le, name, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Disk Op Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('foyer_storage_disk_io_bytes')}[$__rate_interval])) by (name, op, {NODE_LABEL})",
                            "{{name}} - disk - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                # refill
                panels.timeseries_ops(
                    "Refill Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_duration_count')}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('refill_total', refill_ops_filter)}[$__rate_interval])) by (type, op, {NODE_LABEL})",
                            "{{type}} file cache refill - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytes_per_sec(
                    "Data Refill Throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_bytes')}[$__rate_interval])) by (foyer, op, {NODE_LABEL})",
                            "{{type}} file cache - {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Refill Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('refill_duration_bucket')}[$__rate_interval])) by (le, foyer, op, {NODE_LABEL}))",
                                f"p{legend}"
                                + " - {{foyer}} cache refill - {{op}} @ {{%s}}"
                                % NODE_LABEL,
                            ),
                            [50, 90, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Refill Queue Length",
                    "",
                    [
                        panels.target(
                            f"sum(refill_queue_total) by ({NODE_LABEL})",
                            "refill queue length @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Inheritance - Parent Meta Lookup Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', inheritance_parent_lookup_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "parent meta lookup {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Inheritance - Parent Meta Lookup Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', inheritance_parent_lookup_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) / (sum(rate({metric('refill_total', inheritance_parent_lookup_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) + sum(rate({metric('refill_total', inheritance_parent_lookup_miss_filter)}[$__rate_interval])) by ({NODE_LABEL})) >= 0",
                            "parent meta lookup hit ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Inheritance - Unit inheritance Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', unit_inheritance_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "unit inheritance {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Inheritance - Unit inheritance Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', unit_inheritance_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) / (sum(rate({metric('refill_total', unit_inheritance_hit_filter)}[$__rate_interval])) by ({NODE_LABEL}) + sum(rate({metric('refill_total', unit_inheritance_miss_filter)}[$__rate_interval])) by ({NODE_LABEL})) >= 0",
                            "unit inheritance ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Block Refill Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', block_refill_filter)}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "block refill {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Block Refill Ratio",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('refill_total', block_refill_success_filter)}[$__rate_interval])) by ({NODE_LABEL}) / sum(rate({metric('refill_total', block_refill_unfiltered_filter)}[$__rate_interval])) by ({NODE_LABEL}) >= 0",
                            "block refill ratio @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Recent Filter Size",
                    "Item numbers of the recent filter.",
                    [
                        panels.target(
                            f"sum({metric('recent_filter_items')}) by ({NODE_LABEL})",
                            "items @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Recent Filter Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('recent_filter_ops')}[$__rate_interval])) by (op, {NODE_LABEL})",
                            "recent filter {{op}} @ {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
            ],
        )
    ]


def section_hummock_manager(outer_panels):
    panels = outer_panels.sub_panel()
    total_key_size_filter = "metric='total_key_size'"
    total_value_size_filter = "metric='total_value_size'"
    total_key_count_filter = "metric='total_key_count'"
    mv_total_size_filter = "metric='materialized_view_total_size'"
    return [
        outer_panels.row_collapsed(
            "Hummock Manager",
            [
                panels.timeseries_latency(
                    "Lock Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('hummock_manager_lock_time_bucket')}[$__rate_interval])) by (le, method, lock_name, lock_type))",
                                f"Lock Time p{legend}"
                                + " - {{method}} @ {{lock_type}} @ {{lock_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Real Process Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('meta_hummock_manager_real_process_time_bucket')}[$__rate_interval])) by (le, method))",
                                f"Real Process Time p{legend}" + " - {{method}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Version Size",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_version_size')}", "version size"
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Version Id",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_current_version_id')}",
                            "current version id",
                        ),
                        panels.target(
                            f"{metric('storage_checkpoint_version_id')}",
                            "checkpoint version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_pinned_version_id')}",
                            "min pinned version id",
                        ),
                        panels.target(
                            f"{metric('storage_min_safepoint_version_id')}",
                            "min safepoint version id",
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('storage_max_committed_epoch')}",
                            "max committed epoch",
                        ),
                        panels.target(
                            f"{metric('storage_min_committed_epoch')}",
                            "min committed epoch",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Table Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                        panels.target(
                            f"{table_metric('storage_version_stats', total_value_size_filter)}/1024",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_kilobytes(
                    "Materialized View Size",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_materialized_view_stats', mv_total_size_filter)}/1024",
                            "{{metric}}, mv id - {{table_id}} ",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Table KV Count",
                    "",
                    [
                        panels.target(
                            f"{table_metric('storage_version_stats', total_key_count_filter)}",
                            "table{{table_id}} {{metric}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Object Total Number",
                    """
Objects are classified into 3 groups:
- not referenced by versions: these object are being deleted from object store.
- referenced by non-current versions: these objects are stale (not in the latest version), but those old versions may still be in use (e.g. long-running pinning). Thus those objects cannot be deleted at the moment.
- referenced by current version: these objects are in the latest version.

Additionally, a metric on all objects (including dangling ones) is updated with low-frequency. The metric is updated right before full GC. So subsequent full GC may reduce the actual value significantly, without updating the metric.
                    """,
                    [
                        panels.target(
                            f"{metric('storage_stale_object_count')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_count')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_count')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_time_travel_object_count')}",
                            "referenced by time travel",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_count')}",
                            "all objects (including dangling ones)",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Object Total Size",
                    "Refer to `Object Total Number` panel for classification of objects.",
                    [
                        panels.target(
                            f"{metric('storage_stale_object_size')}",
                            "not referenced by versions",
                        ),
                        panels.target(
                            f"{metric('storage_old_version_object_size')}",
                            "referenced by non-current versions",
                        ),
                        panels.target(
                            f"{metric('storage_current_version_object_size')}",
                            "referenced by current version",
                        ),
                        panels.target(
                            f"{metric('storage_total_object_size')}",
                            "all objects, including dangling ones",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Delta Log Total Number",
                    "total number of hummock version delta log",
                    [
                        panels.target(
                            f"{metric('storage_delta_log_count')}",
                            "delta log total number",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Version Checkpoint Latency",
                    "hummock version checkpoint latency",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('storage_version_checkpoint_latency_bucket')}[$__rate_interval])) by (le))",
                            f"version_checkpoint_latency_p{legend}",
                        ),
                        [50, 90, 99, 999, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('storage_version_checkpoint_latency_sum')}[$__rate_interval]) / rate({metric('storage_version_checkpoint_latency_count')}[$__rate_interval]) > 0",
                            "version_checkpoint_latency_avg",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Write Stop Compaction Groups",
                    "When certain per compaction group threshold is exceeded (e.g. number of level 0 sub-level in LSMtree), write op to that compaction group is stopped temporarily. Check log for detail reason of write stop.",
                    [
                        panels.target(
                            f"{metric('storage_write_stop_compaction_groups')}",
                            "compaction_group_{{compaction_group_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Full GC Trigger Count",
                    "total number of attempts to trigger full GC",
                    [
                        panels.target(
                            f"{metric('storage_full_gc_trigger_count')}",
                            "full_gc_trigger_count",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Compaction Event Loop Time",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('storage_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"meta iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_consumed_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor consumed latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(irate({metric('compactor_compaction_event_loop_iteration_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                                f"compactor iteration latency p{legend}"
                                + " - {{%s}} @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "State Table Count",
                    "The number of state_tables in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_state_table_count')}[$__rate_interval])) by (group)",
                            "state table cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Branched SST Count",
                    "The number of branched_sst in each CG",
                    [
                        panels.target(
                            f"sum(irate({table_metric('storage_branched_sst_count')}[$__rate_interval])) by (group)",
                            "branched sst cg{{group}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Time Travel Replay Latency",
                    "The latency of replaying a hummock version for time travel",
                    quantile(
                        lambda quantile, legend: panels.target(
                            f"histogram_quantile({quantile}, sum(rate({metric('storage_time_travel_version_replay_latency_bucket')}[$__rate_interval])) by (le))",
                            f"time_travel_version_replay_latency_p{legend}",
                        ),
                        [50, 90, 99, "max"],
                    )
                    + [
                        panels.target(
                            f"rate({metric('storage_time_travel_version_replay_latency_sum')}[$__rate_interval]) / rate({metric('storage_time_travel_version_replay_latency_count')}[$__rate_interval]) > 0",
                            "time_travel_version_replay_avg",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Time Travel Replay Ops",
                    "The frequency of replaying a hummock version for time travel",
                    [
                        panels.target(
                            f"sum(rate({metric('storage_time_travel_version_replay_latency_count')}[$__rate_interval]))",
                            "time_travel_version_replay_ops",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_backup_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Backup Manager",
            [
                panels.timeseries_count(
                    "Job Count",
                    "Total backup job count since the Meta node starts",
                    [
                        panels.target(
                            f"{metric('backup_job_count')}",
                            "job count",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Job Process Time",
                    "Latency of backup jobs since the Meta node starts",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('backup_job_latency_bucket')}[$__rate_interval])) by (le, state))",
                                f"Job Process Time p{legend}" + " - {{state}}",
                            ),
                            [50, 99, "max"],
                        ),
                    ],
                ),
            ],
        )
    ]


def grpc_metrics_target(panels, name, filter):
    return panels.timeseries_latency_small(
        f"{name} latency",
        "",
        [
            panels.target(
                f"histogram_quantile(0.5, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p50",
            ),
            panels.target(
                f"histogram_quantile(0.9, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p90",
            ),
            panels.target(
                f"histogram_quantile(0.99, sum(irate({metric('meta_grpc_duration_seconds_bucket', filter)}[$__rate_interval])) by (le))",
                f"{name}_p99",
            ),
            panels.target(
                f"sum(irate({metric('meta_grpc_duration_seconds_sum', filter)}[$__rate_interval])) / sum(irate({metric('meta_grpc_duration_seconds_count', filter)}[$__rate_interval])) > 0",
                f"{name}_avg",
            ),
        ],
    )


def section_grpc_meta_catalog_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Catalog Service",
            [
                grpc_metrics_target(
                    panels, "Create", "path='/meta.CatalogService/Create'"
                ),
                grpc_metrics_target(panels, "Drop", "path='/meta.CatalogService/Drop'"),
                grpc_metrics_target(
                    panels, "GetCatalog", "path='/meta.CatalogService/GetCatalog'"
                ),
            ],
        )
    ]


def section_grpc_meta_cluster_service(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Cluster Service",
            [
                grpc_metrics_target(
                    panels, "AddWorkerNode", "path='/meta.ClusterService/AddWorkerNode'"
                ),
                grpc_metrics_target(
                    panels, "ListAllNodes", "path='/meta.ClusterService/ListAllNodes'"
                ),
            ],
        ),
    ]


def section_grpc_meta_stream_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "gRPC Meta: Stream Manager",
            [
                grpc_metrics_target(
                    panels,
                    "CreateMaterializedView",
                    "path='/meta.StreamManagerService/CreateMaterializedView'",
                ),
                grpc_metrics_target(
                    panels,
                    "DropMaterializedView",
                    "path='/meta.StreamManagerService/DropMaterializedView'",
                ),
                grpc_metrics_target(
                    panels, "Flush", "path='/meta.StreamManagerService/Flush'"
                ),
            ],
        ),
    ]

def section_kafka_metrics(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Kafka Metrics",
            [
                panels.timeseries_count(
                    "Kafka high watermark and source latest message",
                    "Kafka high watermark by source and partition and source latest message by partition, source and actor",
                    [
                        panels.target(
                            f"{metric('source_kafka_high_watermark')}",
                            "high watermark: source={{source_id}} partition={{partition}}",
                        ),
                        panels.target(
                            f"{metric('source_latest_message_id')}",
                            "latest msg: source={{source_id}} partition={{partition}} actor_id={{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message Count in Producer Queue",
                    "Current number of messages in producer queues",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Message Size in Producer Queue",
                    "Current total size of messages in producer queues",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_msg_size')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message Produced Count",
                    "Total number of messages transmitted (produced) to Kafka brokers",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_tx_msgs')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Message Received Count",
                    "Total number of messages consumed, not including ignored messages (due to offset, etc), from Kafka brokers.",
                    [
                        panels.target(
                            f"{metric('rdkafka_top_rx_msgs')}",
                            "id {{ id }}, client_id {{ client_id }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Message Count Pending to Transmit (per broker)",
                    "Number of messages awaiting transmission to broker",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_outbuf_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Inflight Message Count (per broker)",
                    "Number of messages in-flight to broker awaiting response",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_waitresp_msg_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Error Count When Transmitting (per broker)",
                    "Total number of transmission errors",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_tx_errs')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Error Count When Receiving (per broker)",
                    "Total number of receive errors",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_rx_errs')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Timeout Request Count (per broker)",
                    "Total number of requests timed out",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_req_timeouts')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, state {{ state }}",
                        )
                    ],
                ),
                panels.timeseries_latency_ms(
                    "RTT (per broker)",
                    "Broker latency / round-trip time in milli seconds",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_avg')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p75')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p90')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_p99_99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_rtt_out_of_range')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Throttle Time (per broker)",
                    "Broker throttling time in milliseconds",
                    [
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_avg')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p75')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p90')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_p99_99')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_broker_throttle_out_of_range')}/1000",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}",
                        ),
                    ],
                ),
                panels.timeseries_latency_ms(
                    "Topic Metadata_age Age",
                    "Age of metadata from broker for this topic (milliseconds)",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_metadata_age')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}",
                        )
                    ],
                ),
                panels.timeseries_bytes(
                    "Topic Batch Size",
                    "Batch sizes in bytes",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_avg')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p75')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p90')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p99')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_p99_99')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.target(
                            f"{metric('rdkafka_topic_batchsize_out_of_range')}",
                            "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                        ),
                        panels.timeseries_count(
                            "Topic Batch Messages",
                            "Batch message counts",
                            [
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_avg')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p75')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p90')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p99')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_p99_99')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                                panels.target(
                                    f"{metric('rdkafka_topic_batchcnt_out_of_range')}",
                                    "id {{ id }}, client_id {{ client_id}}, broker {{ broker }}, topic {{ topic }}",
                                ),
                            ],
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message to be Transmitted",
                    "Number of messages ready to be produced in transmit queue",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_xmit_msgq_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Message in pre fetch queue",
                    "Number of pre-fetched messages in fetch queue",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_fetchq_cnt')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Next offset to fetch",
                    "Next offset to fetch",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_next_offset')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        )
                    ],
                ),
                panels.timeseries_count(
                    "Committed Offset",
                    "Last committed offset",
                    [
                        panels.target(
                            f"{metric('rdkafka_topic_partition_committed_offset')}",
                            "id {{ id }}, client_id {{ client_id}}, topic {{ topic }}, partition {{ partition }}",
                        )
                    ],
                ),
            ],
        )
    ]


def section_iceberg_metrics(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Iceberg Metrics",
            [
                panels.timeseries_ops(
                    "Write Qps Of Iceberg Writer",
                    "iceberg write qps",
                    [
                        panels.target(
                            f"sum(rate({metric('iceberg_write_qps')}[$__rate_interval])) by (actor_id, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Write Latency Of Iceberg Writer",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('iceberg_write_latency_bucket')}[$__rate_interval])) by (le, sink_id, sink_name))",
                                f"p{legend}" + " @ {{sink_id}} {{sink_name}}",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, job, instance, sink_id, sink_name)(rate({metric('iceberg_write_latency_sum')}[$__rate_interval])) / sum by(le, type, job, instance, sink_id, sink_name) (rate({metric('iceberg_write_latency_count')}[$__rate_interval])) > 0",
                            "avg @ {{sink_id}} {{sink_name}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg rolling unfushed data file",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_rolling_unflushed_data_file')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg position delete cache num",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_position_delete_cache_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Iceberg partition num",
                    "",
                    [
                        panels.target(
                            f"{metric('iceberg_partition_num')}",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Iceberg Write Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('iceberg_write_bytes')}) by (sink_name)",
                            "write @ {{sink_name}}",
                        ),
                        panels.target(
                            f"sum({metric('iceberg_write_bytes')})",
                            "total write",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Iceberg Read Size",
                    "",
                    [
                        panels.target(
                            f"sum({metric('iceberg_read_bytes')}) by (table_name)",
                            "read @ {{table_name}}",
                        ),
                        panels.target(
                            f"sum({metric('iceberg_read_bytes')})",
                            "total read",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_memory_manager(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Memory manager",
            [
                panels.timeseries_count(
                    "LRU manager loop count per sec",
                    "",
                    [
                        panels.target(
                            f"rate({metric('lru_runtime_loop_count')}[$__rate_interval])",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager eviction policy",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_eviction_policy')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries(
                    "LRU manager sequence",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_latest_sequence')}",
                            "",
                        ),
                        panels.target(
                            f"{metric('lru_watermark_sequence')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The allocated memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The resident memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_resident_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The metadata memory of jemalloc",
                    "",
                    [
                        panels.target(
                            f"{metric('jemalloc_metadata_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The allocated memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_allocated_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_memory(
                    "The active memory of jvm",
                    "",
                    [
                        panels.target(
                            f"{metric('jvm_active_bytes')}",
                            "",
                        ),
                    ],
                ),
                panels.timeseries_ms(
                    "LRU manager diff between current watermark and evicted watermark time (ms) for actors",
                    "",
                    [
                        panels.target(
                            f"{metric('lru_current_watermark_time_ms')} - on() group_right() {metric('lru_evicted_watermark_time_ms')}",
                            "table {{table_id}} actor {{actor_id}} desc: {{desc}}",
                        ),
                    ],
                ),
            ],
        ),
    ]


def section_sink_metrics(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "Sink Metrics",
            [
                panels.timeseries_rowsps(
                    "Remote Sink (Java) Throughput",
                    "The rows sent by remote sink to the Java connector process",
                    [
                        panels.target(
                            f"sum(rate({metric('connector_sink_rows_received')}[$__rate_interval])) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Commit Duration",
                    "",
                    [
                        *quantile(
                            lambda quantile, legend: panels.target(
                                f"histogram_quantile({quantile}, sum(rate({metric('sink_commit_duration_bucket')}[$__rate_interval])) by (le, connector, sink_id, sink_name))",
                                f"p{legend}"
                                + " @ {{sink_id}} {{sink_name}} ({{connector}})",
                            ),
                            [50, 99, "max"],
                        ),
                        panels.target(
                            f"sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}, sink_id, sink_name)(rate({metric('sink_commit_duration_sum')}[$__rate_interval])) / sum by(le, type, {COMPONENT_LABEL}, {NODE_LABEL}, sink_id, sink_name) (rate({metric('sink_commit_duration_count')}[$__rate_interval])) > 0",
                            "avg @ {{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_epoch(
                    "Log Store Read/Write Epoch",
                    "",
                    [
                        panels.target(
                            f"{metric('log_store_latest_write_epoch')}",
                            "latest write epoch @ {{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('log_store_latest_read_epoch')}",
                            "latest read epoch @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_min_epoch')}",
                            "Kv log store unconsumed min epoch @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Lag",
                    "",
                    [
                        panels.target(
                            f"(max({metric('log_store_latest_write_epoch')}) by (sink_id, actor_id, sink_name)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (sink_id, actor_id, sink_name)) / (2^16) / 1000",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_percentage(
                    "Log Store Backpressure Ratio",
                    "",
                    [
                        panels.target(
                            f"avg(rate({metric('log_store_reader_wait_new_future_duration_ns')}[$__rate_interval])) by (connector, sink_id, actor_id, sink_name) / 1000000000",
                            "Backpressure @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Log Store Consume Persistent Log Lag",
                    "",
                    [
                        panels.target(
                            f"clamp_min((max({metric('log_store_first_write_epoch')}) by (sink_id, actor_id, sink_name)"
                            + f"- max({metric('log_store_latest_read_epoch')}) by (sink_id, actor_id, sink_name)) / (2^16) / 1000, 0)",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by (connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Executor Log Store Consume Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_rows')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, actor_id, sink_name)",
                            "{{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_bytes')}[$__rate_interval])) by (connector, sink_id, sink_name) / (1000*1000)",
                            "{{sink_id}} {{sink_name}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "Executor Log Store Consume Throughput(MB/s)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_read_bytes')}[$__rate_interval])) by ({NODE_LABEL}, connector, sink_id, actor_id, sink_name) / (1000*1000)",
                            "{{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}} @ {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_rowsps(
                    "Log Store Write Throughput(rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by (sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}}",
                        ),
                        panels.target_hidden(
                            f"sum(rate({metric('log_store_write_rows')}[$__rate_interval])) by ({NODE_LABEL}, sink_id, actor_id, sink_name)",
                            "{{sink_id}} {{sink_name}} @ actor {{actor_id}} {{%s}}"
                            % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Read Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Read Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_read_size')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Write Storage Row Ops",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Kv Log Store Write Storage Size",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_storage_write_size')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Kv Log Store Buffer State",
                    "",
                    [
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_item_count')}",
                            "Unconsumed item count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_row_count')}",
                            "Unconsumed row count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                        panels.target(
                            f"{metric('kv_log_store_buffer_unconsumed_epoch_count')}",
                            "Unconsumed epoch count @ {{sink_id}} {{sink_name}} ({{connector}}) actor {{actor_id}}",
                        ),
                    ],
                ),
                panels.timeseries_ops(
                    "Kv Log Store Rewind Rate",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('kv_log_store_rewind_count')}[$__rate_interval])) by (actor_id, connector, sink_id, sink_name)",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "Rewind delay (second)",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(1.0, sum(rate({metric('kv_log_store_rewind_delay_bucket')}[$__rate_interval])) by (le, actor_id, connector, sink_id, sink_name))",
                            "{{sink_id}} {{sink_name}} actor {{actor_id}} ({{connector}})",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "Chunk Buffer Size",
                    "Total size of chunks buffered in a barrier",
                    [
                        panels.target(
                            f"sum({metric('stream_sink_chunk_buffer_size')}) by (sink_id, actor_id, sink_name) * on(actor_id) group_left(sink_name) {metric('sink_info')}",
                            "sink {{sink_id}} {{sink_name}} - actor {{actor_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]


def section_network_connection(outer_panels):
    panels = outer_panels.sub_panel()
    s3_filter = 'connection_type="S3"'
    grpc_filter = 'connection_type=~"grpc.*"'
    return [
        outer_panels.row_collapsed(
            "Network connection",
            [
                panels.timeseries_bytesps(
                    "Network throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "S3 throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} read @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} write @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "gRPC throughput",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type) / (1024*1024)",
                            "{{%s}} {{connection_type}} write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_read_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total read @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_write_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "{{%s}} total write @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "IO error rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_io_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} S3 {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} grpc {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(rate({metric('connection_io_err_rate')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, op_type, error_kind)",
                            "{{%s}} total {{op_type}} err[{{error_kind}}] @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Existing connection count",
                    "",
                    [
                        panels.target(
                            f"sum({metric('connection_count', filter=s3_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum({metric('connection_count', filter=grpc_filter)}) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_create_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "Create new connection err rate",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=s3_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "{{%s}} S3 @ {{%s}}" % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                        panels.target(
                            f"sum(irate({metric('connection_err_rate', filter=grpc_filter)}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}, connection_type)",
                            "{{%s}} {{connection_type}} @ {{%s}}"
                            % (COMPONENT_LABEL, NODE_LABEL),
                        ),
                    ],
                ),
            ],
        )
    ]


def section_udf(outer_panels):
    panels = outer_panels.sub_panel()
    return [
        outer_panels.row_collapsed(
            "User Defined Function",
            [
                panels.timeseries_count(
                    "UDF Calls Count",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_success_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_success_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_failure_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_failure_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_retry_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_retry_count - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_success_count')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_success_count - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_failure_count')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_failure_count - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_retry_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_retry_count - {{%s}}" % NODE_LABEL,
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "UDF Input Chunk Rows",
                    "",
                    [
                        panels.target(
                            f"sum(irate({metric('udf_input_chunk_rows_sum')}[$__rate_interval])) by (link, name, fragment_id) / sum(irate({metric('udf_input_chunk_rows_count')}[$__rate_interval])) by (link, name, fragment_id) > 0",
                            "udf_input_chunk_rows_avg - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_latency(
                    "UDF Latency",
                    "",
                    [
                        panels.target(
                            f"histogram_quantile(0.50, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p50 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.90, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p90 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, {COMPONENT_LABEL}, {NODE_LABEL}))",
                            "udf_latency_p99 - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(irate({metric('udf_latency_sum')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / sum(irate({metric('udf_latency_count')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) > 0",
                            "udf_latency_avg - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"histogram_quantile(0.99, sum(irate({metric('udf_latency_bucket')}[$__rate_interval])) by (le, link, name, fragment_id))",
                            "udf_latency_p99_by_name - {{link}} {{name}} {{fragment_id}}",
                        ),
                        panels.target(
                            f"sum(irate({metric('udf_latency_sum')}[$__rate_interval])) by (link, name, fragment_id) / sum(irate({metric('udf_latency_count')}[$__rate_interval])) by (link, name, fragment_id) > 0",
                            "udf_latency_avg_by_name - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_count(
                    "UDF Throughput (rows)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_input_rows')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_throughput_rows - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_input_rows')}[$__rate_interval])) by (link, name, fragment_id)",
                            "udf_throughput_rows - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytesps(
                    "UDF Throughput (bytes)",
                    "",
                    [
                        panels.target(
                            f"sum(rate({metric('udf_input_bytes')}[$__rate_interval])) by ({COMPONENT_LABEL}, {NODE_LABEL}) / (1024*1024)",
                            "udf_throughput_bytes - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum(rate({metric('udf_input_bytes')}[$__rate_interval])) by (link, name, fragment_id) / (1024*1024)",
                            "udf_throughput_bytes - {{link}} {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
                panels.timeseries_bytes(
                    "UDF Memory Usage (bytes)",
                    "Currently only embedded JS UDF supports this. Others will always show 0.",
                    [
                        panels.target(
                            f"sum({metric('udf_memory_usage')}) by ({COMPONENT_LABEL}, {NODE_LABEL})",
                            "udf_memory_usage - {{%s}}" % NODE_LABEL,
                        ),
                        panels.target(
                            f"sum({metric('udf_memory_usage')}) by (name, fragment_id)",
                            "udf_memory_usage - {{name}} {{fragment_id}}",
                        ),
                    ],
                ),
            ],
        )
    ]

def section_alert_overview(panels):
    return [
        panels.row("Alert Overview"),
        panels.timeseries_count(
            "Alerts",
            """Alerts in the system group by type:
            - Too Many Barriers: there are too many uncommitted barriers generated. This means the streaming graph is stuck or under heavy load. Check 'Barrier Latency' panel.
            - Recovery Triggered: cluster recovery is triggered. Check 'Errors by Type' / 'Node Count' panels.
            - Lagging Version: the checkpointed or pinned version id is lagging behind the current version id. Check 'Hummock Manager' section in dev dashboard.
            - Lagging Compaction: there are too many ssts in L0. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard, and take care of the type of 'Commit Flush Bytes' and 'Compaction Throughput', whether the throughput is too low.
            - Lagging Vacuum: there are too many stale files waiting to be cleaned. This can be caused by compactor failure or lag of compactor resource. Check 'Compaction' section in dev dashboard.
            - Abnormal Meta Cache Memory: the meta cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Block Cache Memory: the block cache memory usage is too large, exceeding the expected 10 percent.
            - Abnormal Uploading Memory Usage: uploading memory is more than 70 percent of the expected, and is about to spill.
            - Write Stall: Compaction cannot keep up. Stall foreground write, Check 'Compaction' section in dev dashboard.
            - Abnormal Version Size: the size of the version is too large, exceeding the expected 300MB. Check 'Hummock Manager' section in dev dashboard.
            - Abnormal Delta Log Number: the number of delta logs is too large, exceeding the expected 5000. Check 'Hummock Manager' and `Compaction` section in dev dashboard and take care of the type of 'Compaction Success Count', whether the number of trivial-move tasks spiking.
            - Abnormal Pending Event Number: the number of pending events is too large, exceeding the expected 10000000. Check 'Hummock Write' section in dev dashboard and take care of the 'Event handle latency', whether the time consumed exceeds the barrier latency.
            - Abnormal Object Storage Failure: the number of object storage failures is too large, exceeding the expected 50. Check 'Object Storage' section in dev dashboard and take care of the 'Object Storage Failure Rate', whether the rate is too high.
            """,
            [
                panels.target(
                    f"{metric('all_barrier_nums')} >= bool 200",
                    "Too Many Barriers {{database_id}}",
                ),
                panels.target(
                    f"sum(rate({metric('recovery_latency_count')}[$__rate_interval])) > bool 0 + sum({metric('recovery_failure_cnt')}) > bool 0",
                    "Recovery Triggered",
                ),
                panels.target(
                    f"(({metric('storage_current_version_id')} - {metric('storage_checkpoint_version_id')}) >= bool 100) + "
                    + f"(({metric('storage_current_version_id')} - {metric('storage_min_pinned_version_id')}) >= bool 100)",
                    "Lagging Version",
                ),
                panels.target(
                    f"sum(label_replace({metric('storage_level_total_file_size')}, 'L0', 'L0', 'level_index', '.*_L0') unless "
                    + f"{metric('storage_level_total_file_size')}) by (L0) >= bool 52428800",
                    "Lagging Compaction",
                ),
                panels.target(
                    f"{metric('storage_stale_object_count')} >= bool 200",
                    "Lagging Vacuum",
                ),
                panels.target(
                    f"{metric('state_store_meta_cache_usage_ratio')} >= bool 1.1",
                    "Abnormal Meta Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_block_cache_usage_ratio')} >= bool 1.1",
                    "Abnormal Block Cache Memory",
                ),
                panels.target(
                    f"{metric('state_store_uploading_memory_usage_ratio')} >= bool 0.7",
                    "Abnormal Uploading Memory Usage",
                ),
                panels.target(
                    f"{metric('storage_write_stop_compaction_groups')} > bool 0",
                    "Write Stall",
                ),
                panels.target(
                    f"{metric('storage_version_size')} >= bool 314572800",
                    "Abnormal Version Size",
                ),
                panels.target(
                    f"{metric('storage_delta_log_count')} >= bool 5000",
                    "Abnormal Delta Log Number",
                ),
                panels.target(
                    f"{metric('state_store_event_handler_pending_event')} >= bool 10000000",
                    "Abnormal Pending Event Number",
                ),
                panels.target(
                    f"{metric('object_store_failure_count')} >= bool 50",
                    "Abnormal Object Storage Failure",
                ),
            ],
            ["last"],
        ),
        panels.timeseries_count(
            "Errors",
            "Errors in the system group by type",
            [
                panels.target(
                    f"sum({metric('user_compute_error')}) by (error_type, executor_name, fragment_id)",
                    "{{error_type}} @ {{executor_name}} (fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"sum({metric('user_source_error')}) by (error_type, source_id, source_name, fragment_id)",
                    "{{error_type}} @ {{source_name}} (source_id={{source_id}} fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"sum({metric('user_sink_error')}) by (error_type, sink_id, sink_name, fragment_id)",
                    "{{error_type}} @ {{sink_name}} (sink_id={{sink_id}} fragment_id={{fragment_id}})",
                ),
                panels.target(
                    f"{metric('source_status_is_up')} == 0",
                    "source error: source_id={{source_id}}, source_name={{source_name}} @ {{%s}}"
                    % NODE_LABEL,
                ),
                panels.target(
                    f"sum(rate({metric('object_store_failure_count')}[$__rate_interval])) by ({NODE_LABEL}, {COMPONENT_LABEL}, type)",
                    "remote storage error {{type}}: {{%s}} @ {{%s}}"
                    % (COMPONENT_LABEL, NODE_LABEL),
                ),
            ],
        ),
    ]

templating_list = []
if dynamic_source_enabled:
    templating_list.append(
        {
            "hide": 0,
            "includeAll": False,
            "multi": False,
            "name": f"{datasource_const}",
            "options": [],
            "query": "prometheus",
            "queryValue": "",
            "refresh": 2,
            "skipUrlSync": False,
            "type": "datasource",
        }
    )

if namespace_filter_enabled:
    namespace_json = {
        "definition": 'label_values(up{risingwave_name=~".+"}, namespace)',
        "description": "Kubernetes namespace.",
        "hide": 0,
        "includeAll": False,
        "label": "Namespace",
        "multi": False,
        "name": "namespace",
        "options": [],
        "query": {
            "query": 'label_values(up{risingwave_name=~".+"}, namespace)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 0,
        "type": "query",
    }

    name_json = {
        "current": {"selected": False, "text": "risingwave", "value": "risingwave"},
        "definition": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
        "hide": 0,
        "includeAll": False,
        "label": "RisingWave",
        "multi": False,
        "name": "instance",
        "options": [],
        "query": {
            "query": 'label_values(up{namespace="$namespace", risingwave_name=~".+"}, risingwave_name)',
            "refId": "StandardVariableQuery",
        },
        "refresh": 2,
        "regex": "",
        "skipUrlSync": False,
        "sort": 6,
        "type": "query",
    }
    if dynamic_source_enabled:
        namespace_json = merge(namespace_json, {"datasource": datasource})
        name_json = merge(name_json, {"datasource": datasource})

    templating_list.append(namespace_json)
    templating_list.append(name_json)

node_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
    "description": "Reporting instance of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{NODE_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{NODE_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {NODE_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

job_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
    "description": "Reporting job of the metric",
    "hide": 0,
    "includeAll": True,
    "label": f"{COMPONENT_VARIABLE_LABEL}",
    "multi": True,
    "name": f"{COMPONENT_VARIABLE}",
    "options": [],
    "query": {
        "query": f"label_values({metric('process_cpu_seconds_total', node_filter_enabled=False)}, {COMPONENT_LABEL})",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

table_id_json = {
    "current": {"selected": False, "text": "All", "value": "__all"},
    "definition": f"label_values({metric('table_info', node_filter_enabled=False)}, table_id)",
    "description": "Reporting table id of the metric",
    "hide": 0,
    "includeAll": True,
    "label": "Table",
    "multi": True,
    "name": "table",
    "options": [],
    "query": {
        "query": f"label_values({metric('table_info', node_filter_enabled=False)}, table_id)",
        "refId": "StandardVariableQuery",
    },
    "refresh": 2,
    "regex": "",
    "skipUrlSync": False,
    "sort": 6,
    "type": "query",
}

if dynamic_source_enabled:
    node_json = merge(node_json, {"datasource": datasource})
    job_json = merge(job_json, {"datasource": datasource})
    table_id_json = merge(table_id_json, {"datasource": datasource})

templating_list.append(node_json)
templating_list.append(job_json)
templating_list.append(table_id_json)

templating = Templating(templating_list)

dashboard = Dashboard(
    title="risingwave_dev_dashboard",
    description="RisingWave Dev Dashboard",
    tags=["risingwave"],
    timezone="browser",
    editable=True,
    uid=dashboard_uid,
    time=Time(start="now-30m", end="now"),
    sharedCrosshair=True,
    templating=templating,
    version=dashboard_version,
    refresh="",
    panels=[
        *section_actor_info(panels),
        *section_cluster_node(panels),
        *section_recovery_node(panels),
        *section_streaming(panels),
        *section_streaming_cdc(panels),
        *section_streaming_actors(panels),
        *section_streaming_actors_tokio(panels),
        *section_streaming_exchange(panels),
        *section_streaming_errors(panels),
        *section_batch(panels),
        *section_hummock_read(panels),
        *section_hummock_write(panels),
        *section_compaction(panels),
        *section_object_storage(panels),
        *section_hummock_tiered_cache(panels),
        *section_hummock_manager(panels),
        *section_backup_manager(panels),
        *section_grpc_meta_catalog_service(panels),
        *section_grpc_meta_cluster_service(panels),
        *section_grpc_meta_stream_manager(panels),
        *section_frontend(panels),
        *section_memory_manager(panels),
        *section_sink_metrics(panels),
        *section_kafka_metrics(panels),
        *section_network_connection(panels),
        *section_iceberg_metrics(panels),
        *section_udf(panels),
        *section_alert_overview(panels),
    ],
).auto_panel_ids()
