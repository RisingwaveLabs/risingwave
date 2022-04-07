// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::meta::table_fragments::fragment::FragmentType;
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::stream_plan::StreamActor;
use tokio::sync::RwLock;

use crate::cluster::WorkerId;
use crate::model::{ActorId, MetadataModel, TableFragments, Transactional};
use crate::storage::{MetaStore, Transaction};

struct FragmentManagerCore {
    table_fragments: HashMap<TableId, TableFragments>,
}

/// `FragmentManager` stores definition and status of fragment as well as the actors inside.
pub struct FragmentManager<S> {
    meta_store: Arc<S>,

    core: RwLock<FragmentManagerCore>,
}

pub struct ActorInfos {
    /// node_id => actor_ids
    pub actor_maps: HashMap<WorkerId, Vec<ActorId>>,

    /// all reachable source actors
    pub source_actor_maps: HashMap<WorkerId, Vec<ActorId>>,
}

pub type FragmentManagerRef<S> = Arc<FragmentManager<S>>;

impl<S> FragmentManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>) -> Result<Self> {
        let table_fragments = try_match_expand!(
            TableFragments::list(&*meta_store).await,
            Ok,
            "TableFragments::list fail"
        )?;

        let table_fragments = table_fragments
            .into_iter()
            .map(|tf| (tf.table_id(), tf))
            .collect();

        Ok(Self {
            meta_store,
            core: RwLock::new(FragmentManagerCore { table_fragments }),
        })
    }

    pub async fn add_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(_) => Err(RwError::from(InternalError(format!(
                "table_fragment already exist: id={}",
                table_fragment.table_id()
            )))),
            Entry::Vacant(v) => {
                table_fragment.insert(&*self.meta_store).await?;
                v.insert(table_fragment);
                Ok(())
            }
        }
    }

    pub async fn list_table_fragments(&self) -> Result<Vec<TableFragments>> {
        let map = &self.core.read().await.table_fragments;

        Ok(map.values().cloned().collect())
    }

    pub async fn update_table_fragments(&self, table_fragment: TableFragments) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(table_fragment.table_id()) {
            Entry::Occupied(mut entry) => {
                table_fragment.insert(&*self.meta_store).await?;
                entry.insert(table_fragment);

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_fragment.table_id()
            )))),
        }
    }

    /// Add table fragments with downstream actor ids in sink actors.
    pub async fn add_table_fragments_downstream(
        &self,
        table_id: &TableId,
        extra_downstream_actors: &HashMap<ActorId, Vec<ActorId>>,
    ) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        match map.entry(*table_id) {
            Entry::Occupied(mut entry) => {
                let table_fragment = entry.get_mut();
                for fragment in table_fragment.fragments.values_mut() {
                    for actor in &mut fragment.actors {
                        if let Some(downstream_actors) =
                            extra_downstream_actors.get(&actor.actor_id)
                        {
                            actor.dispatcher[0]
                                .downstream_actor_id
                                .extend(downstream_actors.iter().cloned());
                        }
                    }
                }
                table_fragment.insert(&*self.meta_store).await?;

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    /// Drop table fragments info and remove downstream actor infos in fragments from its dependent
    /// tables.
    pub async fn drop_table_fragments(&self, table_id: &TableId) -> Result<()> {
        let map = &mut self.core.write().await.table_fragments;

        if let Some(table_fragments) = map.get(table_id) {
            let mut transaction = Transaction::default();
            table_fragments.delete_in_transaction(&mut transaction)?;

            let dependent_table_ids = table_fragments.dependent_table_ids();
            let chain_actor_ids = table_fragments.chain_actor_ids();
            let mut dependent_tables = Vec::with_capacity(dependent_table_ids.len());
            for dependent_table_id in dependent_table_ids {
                let mut dependent_table = map
                    .get(&dependent_table_id)
                    .ok_or_else(|| {
                        RwError::from(InternalError(format!(
                            "table_fragment not exist: id={}",
                            dependent_table_id
                        )))
                    })?
                    .clone();
                for fragment in dependent_table.fragments.values_mut() {
                    if fragment.fragment_type == FragmentType::Sink as i32 {
                        for actor in &mut fragment.actors {
                            actor.dispatcher[0]
                                .downstream_actor_id
                                .retain(|x| !chain_actor_ids.contains(x));
                        }
                    }
                }
                dependent_table.upsert_in_transaction(&mut transaction)?;
                dependent_tables.push(dependent_table);
            }

            self.meta_store.txn(transaction).await?;
            map.remove(table_id);
            for dependent_table in dependent_tables {
                map.insert(dependent_table.table_id(), dependent_table);
            }

            Ok(())
        } else {
            Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            ))))
        }
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`]
    pub async fn load_all_actors(&self, with_creating_table: Option<TableId>) -> ActorInfos {
        let mut actor_maps = HashMap::new();
        let mut source_actor_ids = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            let include_inactive = with_creating_table.contains(&fragments.table_id());
            let check_state = |s: ActorState| {
                s == ActorState::Running || include_inactive && s == ActorState::Inactive
            };

            for (node_id, actors_status) in fragments.node_actors_status() {
                for actor_status in actors_status {
                    if check_state(actor_status.1) {
                        actor_maps
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_status.0);
                    }
                }
            }

            let source_actors = fragments.node_source_actors_status();
            for (&node_id, actors_status) in &source_actors {
                for actor_status in actors_status {
                    if check_state(actor_status.1) {
                        source_actor_ids
                            .entry(node_id)
                            .or_insert_with(Vec::new)
                            .push(actor_status.0);
                    }
                }
            }
        }

        ActorInfos {
            actor_maps,
            source_actor_maps: source_actor_ids,
        }
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> HashMap<WorkerId, Vec<StreamActor>> {
        let mut actor_maps = HashMap::new();

        let map = &self.core.read().await.table_fragments;
        for fragments in map.values() {
            for (node_id, actor_ids) in fragments.node_actors(include_inactive) {
                let node_actor_ids = actor_maps.entry(node_id).or_insert_with(Vec::new);
                node_actor_ids.extend(actor_ids);
            }
        }

        actor_maps
    }

    pub async fn all_chain_actor_ids(&self) -> HashSet<ActorId> {
        let map = &self.core.read().await.table_fragments;

        map.values()
            .flat_map(|table_fragment| table_fragment.chain_actor_ids())
            .collect::<HashSet<_>>()
    }

    pub async fn table_node_actors(
        &self,
        table_id: &TableId,
    ) -> Result<BTreeMap<WorkerId, Vec<ActorId>>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.node_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    pub async fn get_table_sink_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        let map = &self.core.read().await.table_fragments;
        match map.get(table_id) {
            Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
            None => Err(RwError::from(InternalError(format!(
                "table_fragment not exist: id={}",
                table_id
            )))),
        }
    }

    // TODO(bugen): remove this.
    pub fn blocking_table_node_actors(
        &self,
        table_id: &TableId,
    ) -> Result<BTreeMap<WorkerId, Vec<ActorId>>> {
        tokio::task::block_in_place(|| {
            let map = &self.core.blocking_read().table_fragments;
            match map.get(table_id) {
                Some(table_fragment) => Ok(table_fragment.node_actor_ids()),
                None => Err(RwError::from(InternalError(format!(
                    "table_fragment not exist: id={}",
                    table_id
                )))),
            }
        })
    }

    // TODO(bugen): remove this.
    pub fn blocking_get_table_sink_actor_ids(&self, table_id: &TableId) -> Result<Vec<ActorId>> {
        tokio::task::block_in_place(|| {
            let map = &self.core.blocking_read().table_fragments;
            match map.get(table_id) {
                Some(table_fragment) => Ok(table_fragment.sink_actor_ids()),
                None => Err(RwError::from(InternalError(format!(
                    "table_fragment not exist: id={}",
                    table_id
                )))),
            }
        })
    }
}
