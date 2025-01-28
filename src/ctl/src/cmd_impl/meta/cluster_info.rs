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

use std::collections::{BTreeMap, BTreeSet, HashMap};

use comfy_table::{Attribute, Cell, Row, Table};
use itertools::Itertools;
use risingwave_common::util::addr::HostAddr;
use risingwave_connector::source::{SplitImpl, SplitMetaData};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::meta::GetClusterInfoResponse;
use risingwave_pb::source::ConnectorSplits;
use risingwave_pb::stream_plan::FragmentTypeFlag;

use crate::CtlContext;

pub async fn get_cluster_info(context: &CtlContext) -> anyhow::Result<GetClusterInfoResponse> {
    let meta_client = context.meta_client().await?;
    let response = meta_client.get_cluster_info().await?;
    Ok(response)
}

pub async fn source_split_info(context: &CtlContext, ignore_id: bool) -> anyhow::Result<()> {
    let GetClusterInfoResponse {
        worker_nodes: _,
        source_infos: _,
        table_fragments,
        mut actor_splits,
        revision: _,
    } = get_cluster_info(context).await?;

    let mut actor_splits_map: BTreeMap<u32, (usize, String)> = BTreeMap::new();

    // build actor_splits_map
    for table_fragment in &table_fragments {
        if table_fragment.actor_splits.is_empty() {
            continue;
        }

        for fragment in table_fragment.fragments.values() {
            let fragment_type_mask = fragment.fragment_type_mask;
            if fragment_type_mask & FragmentTypeFlag::Source as u32 == 0
                && fragment_type_mask & FragmentTypeFlag::SourceScan as u32 == 0
            {
                // no source or source backfill
                continue;
            }
            if fragment_type_mask & FragmentTypeFlag::Dml as u32 != 0 {
                // skip dummy source for dml fragment
                continue;
            }

            for actor in &fragment.actors {
                if let Some(ConnectorSplits { splits }) = actor_splits.remove(&actor.actor_id) {
                    let num_splits = splits.len();
                    let splits = splits
                        .iter()
                        .map(|split| SplitImpl::try_from(split).unwrap())
                        .map(|split| split.id())
                        .collect_vec()
                        .join(",");
                    actor_splits_map.insert(actor.actor_id, (num_splits, splits));
                }
            }
        }
    }

    // print in the second iteration. Otherwise we don't have upstream splits info
    for table_fragment in &table_fragments {
        if table_fragment.actor_splits.is_empty() {
            continue;
        }
        if ignore_id {
            println!("Table");
        } else {
            println!("Table #{}", table_fragment.table_id);
        }
        for fragment in table_fragment.fragments.values() {
            let fragment_type_mask = fragment.fragment_type_mask;
            if fragment_type_mask & FragmentTypeFlag::Source as u32 == 0
                && fragment_type_mask & FragmentTypeFlag::SourceScan as u32 == 0
            {
                // no source or source backfill
                continue;
            }
            if fragment_type_mask & FragmentTypeFlag::Dml as u32 != 0 {
                // skip dummy source for dml fragment
                continue;
            }

            println!(
                "\tFragment{} ({})",
                if ignore_id {
                    "".to_owned()
                } else {
                    format!(" #{}", fragment.fragment_id)
                },
                if fragment_type_mask == FragmentTypeFlag::Source as u32 {
                    "Source"
                } else {
                    "SourceScan"
                }
            );
            for actor in &fragment.actors {
                if let Some((split_count, splits)) = actor_splits_map.get(&actor.actor_id) {
                    println!(
                        "\t\tActor{} ({} splits): [{}]",
                        if ignore_id {
                            "".to_owned()
                        } else {
                            format!(" #{:<3}", actor.actor_id,)
                        },
                        split_count,
                        splits,
                    );
                } else {
                    println!(
                        "\t\tError: Actor #{:<3} (not found in actor_splits)",
                        actor.actor_id,
                    )
                }
            }
        }
    }

    Ok(())
}

pub async fn cluster_info(context: &CtlContext) -> anyhow::Result<()> {
    let GetClusterInfoResponse {
        worker_nodes,
        table_fragments,
        actor_splits: _,
        source_infos: _,
        revision,
    } = get_cluster_info(context).await?;

    // Fragment ID -> [Worker ID -> [Actor ID]]
    let mut fragments = BTreeMap::new();
    // Fragment ID -> Table Fragments' State
    let mut fragment_states = HashMap::new();

    for table_fragment in &table_fragments {
        for (&id, fragment) in &table_fragment.fragments {
            for actor in &fragment.actors {
                let worker_id = table_fragment
                    .actor_status
                    .get(&actor.actor_id)
                    .unwrap()
                    .worker_id();

                fragments
                    .entry(id)
                    .or_insert_with(BTreeMap::new)
                    .entry(worker_id)
                    .or_insert(BTreeSet::new())
                    .insert(actor.actor_id);
            }
            fragment_states.insert(id, table_fragment.state());
        }
    }

    let mut table = Table::new();

    let cross_out_if_creating = |cell: Cell, fid: u32| -> Cell {
        match fragment_states[&fid] {
            State::Unspecified => unreachable!(),
            State::Creating => cell.add_attribute(Attribute::CrossedOut),
            State::Created | State::Initial => cell,
        }
    };

    // Compute Node, Frag 1, Frag 2, ..., Frag N
    table.set_header({
        let mut row = Row::new();
        row.add_cell("Compute Node".into());
        for &fid in fragments.keys() {
            let cell = Cell::new(format!("Frag {fid}"));
            let cell = cross_out_if_creating(cell, fid);
            row.add_cell(cell);
        }
        row
    });

    let mut last_worker_id = None;
    for worker in worker_nodes {
        // Compute Node,  Actor 1, Actor 11, -, ..., Actor N
        let mut row = Row::new();
        row.add_cell(if last_worker_id == Some(worker.id) {
            "".into()
        } else {
            last_worker_id = Some(worker.id);
            let cordoned = if worker.get_property().map_or(true, |p| p.is_unschedulable) {
                " (cordoned)"
            } else {
                ""
            };
            Cell::new(format!(
                "{}@{}{}",
                worker.id,
                HostAddr::from(worker.get_host().unwrap()),
                cordoned,
            ))
            .add_attribute(Attribute::Bold)
        });
        for (&fragment_id, worker_actors) in &fragments {
            let cell = if let Some(actors) = worker_actors.get(&worker.id) {
                actors
                    .iter()
                    .map(|actor| format!("{}", actor))
                    .join(",")
                    .into()
            } else {
                "-".into()
            };
            let cell = cross_out_if_creating(cell, fragment_id);
            row.add_cell(cell);
        }
        table.add_row(row);
    }

    println!("{table}");
    println!("Revision: {}", revision);

    Ok(())
}
