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

use itertools::Itertools;
use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;
use risingwave_pb::common::WorkerType;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// `rw_worker_nodes` contains all information about the compute nodes in the cluster.
#[derive(Fields)]
struct RwWorkerNode {
    #[primary_key]
    id: i32,
    host: Option<String>,
    port: Option<String>,
    r#type: String,
    state: String,
    parallelism: Option<i32>,
    is_streaming: Option<bool>,
    is_serving: Option<bool>,
    is_unschedulable: Option<bool>,
    internal_rpc_host_addr: Option<String>,
    rw_version: Option<String>,
    system_total_memory_bytes: Option<i64>,
    system_total_cpu_cores: Option<i64>,
    started_at: Option<Timestamptz>,
    resource_group: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_worker_nodes")]
async fn read_rw_worker_nodes_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwWorkerNode>> {
    let workers = reader.meta_client.list_all_nodes().await?;

    Ok(workers
        .into_iter()
        .sorted_by_key(|w| w.id)
        .map(|worker| {
            let host = worker.host.as_ref();
            let property = worker.property.as_ref();
            let resource = worker.resource.as_ref();
            let is_compute = worker.get_type().unwrap() == WorkerType::ComputeNode;
            RwWorkerNode {
                id: worker.id as i32,
                host: host.map(|h| h.host.clone()),
                port: host.map(|h| h.port.to_string()),
                r#type: worker.get_type().unwrap().as_str_name().into(),
                state: worker.get_state().unwrap().as_str_name().into(),
                parallelism: worker.parallelism().map(|parallelism| parallelism as _),
                is_streaming: if is_compute {
                    property.map(|p| p.is_streaming)
                } else {
                    None
                },
                is_serving: if is_compute {
                    property.map(|p| p.is_serving)
                } else {
                    None
                },
                is_unschedulable: if is_compute {
                    property.map(|p| p.is_unschedulable)
                } else {
                    None
                },
                internal_rpc_host_addr: property.map(|p| p.internal_rpc_host_addr.clone()),
                rw_version: resource.map(|r| r.rw_version.to_owned()),
                system_total_memory_bytes: resource.map(|r| r.total_memory_bytes as _),
                system_total_cpu_cores: resource.map(|r| r.total_cpu_cores as _),
                started_at: worker
                    .started_at
                    .map(|ts| Timestamptz::from_secs(ts as i64).unwrap()),
                resource_group: if is_compute {
                    property.and_then(|p| p.resource_group.clone())
                } else {
                    None
                },
            }
        })
        .collect())
}
