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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use rand::seq::SliceRandom;
use risingwave_common::bail;
use risingwave_common::catalog::OBJECT_ID_PLACEHOLDER;
use risingwave_common::hash::{WorkerSlotId, WorkerSlotMapping};
use risingwave_common::vnode_mapping::vnode_placement::place_vnode;
use risingwave_pb::common::{WorkerNode, WorkerType};

use crate::error::{BatchError, Result};

pub(crate) type FragmentId = u32;

/// `WorkerNodeManager` manages live worker nodes and table vnode mapping information.
pub struct WorkerNodeManager {
    inner: RwLock<WorkerNodeManagerInner>,
    /// Temporarily make worker invisible from serving cluster.
    worker_node_mask: Arc<RwLock<HashSet<u32>>>,
}

struct WorkerNodeManagerInner {
    worker_nodes: Vec<WorkerNode>,
    /// fragment vnode mapping info for streaming
    streaming_fragment_vnode_mapping: HashMap<FragmentId, WorkerSlotMapping>,
    /// fragment vnode mapping info for serving
    serving_fragment_vnode_mapping: HashMap<FragmentId, WorkerSlotMapping>,
}

pub type WorkerNodeManagerRef = Arc<WorkerNodeManager>;

impl Default for WorkerNodeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerNodeManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(WorkerNodeManagerInner {
                worker_nodes: Default::default(),
                streaming_fragment_vnode_mapping: Default::default(),
                serving_fragment_vnode_mapping: Default::default(),
            }),
            worker_node_mask: Arc::new(Default::default()),
        }
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let inner = RwLock::new(WorkerNodeManagerInner {
            worker_nodes,
            streaming_fragment_vnode_mapping: HashMap::new(),
            serving_fragment_vnode_mapping: HashMap::new(),
        });
        Self {
            inner,
            worker_node_mask: Arc::new(Default::default()),
        }
    }

    pub fn list_worker_nodes(&self) -> Vec<WorkerNode> {
        self.inner
            .read()
            .unwrap()
            .worker_nodes
            .iter()
            .filter(|w| w.r#type() == WorkerType::ComputeNode)
            .cloned()
            .collect()
    }

    fn list_serving_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_worker_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
            .collect()
    }

    fn list_streaming_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_worker_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_streaming))
            .collect()
    }

    pub fn add_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        match write_guard
            .worker_nodes
            .iter_mut()
            .find(|w| w.id == node.id)
        {
            None => {
                // insert
                write_guard.worker_nodes.push(node);
            }
            Some(w) => {
                // update
                *w = node;
            }
        }
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        write_guard.worker_nodes.retain(|x| x.id != node.id);
    }

    pub fn refresh(
        &self,
        nodes: Vec<WorkerNode>,
        streaming_mapping: HashMap<FragmentId, WorkerSlotMapping>,
        serving_mapping: HashMap<FragmentId, WorkerSlotMapping>,
    ) {
        let mut write_guard = self.inner.write().unwrap();
        tracing::debug!("Refresh worker nodes {:?}.", nodes);
        tracing::debug!(
            "Refresh streaming vnode mapping for fragments {:?}.",
            streaming_mapping.keys()
        );
        tracing::debug!(
            "Refresh serving vnode mapping for fragments {:?}.",
            serving_mapping.keys()
        );
        write_guard.worker_nodes = nodes;
        write_guard.streaming_fragment_vnode_mapping = streaming_mapping;
        write_guard.serving_fragment_vnode_mapping = serving_mapping;
    }

    /// If worker slot ids is empty, the scheduler may fail to schedule any task and stuck at
    /// schedule next stage. If we do not return error in this case, needs more complex control
    /// logic above. Report in this function makes the schedule root fail reason more clear.
    pub fn get_workers_by_worker_slot_ids(
        &self,
        worker_slot_ids: &[WorkerSlotId],
    ) -> Result<Vec<WorkerNode>> {
        if worker_slot_ids.is_empty() {
            return Err(BatchError::EmptyWorkerNodes);
        }

        let guard = self.inner.read().unwrap();

        let worker_index: HashMap<_, _> = guard.worker_nodes.iter().map(|w| (w.id, w)).collect();

        let mut workers = Vec::with_capacity(worker_slot_ids.len());

        for worker_slot_id in worker_slot_ids {
            match worker_index.get(&worker_slot_id.worker_id()) {
                Some(worker) => workers.push((*worker).clone()),
                None => bail!(
                    "No worker node found for worker slot id: {}",
                    worker_slot_id
                ),
            }
        }

        Ok(workers)
    }

    pub fn get_streaming_fragment_mapping(
        &self,
        fragment_id: &FragmentId,
    ) -> Result<WorkerSlotMapping> {
        self.inner
            .read()
            .unwrap()
            .streaming_fragment_vnode_mapping
            .get(fragment_id)
            .cloned()
            .ok_or_else(|| BatchError::StreamingVnodeMappingNotFound(*fragment_id))
    }

    pub fn insert_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: WorkerSlotMapping,
    ) {
        if self
            .inner
            .write()
            .unwrap()
            .streaming_fragment_vnode_mapping
            .try_insert(fragment_id, vnode_mapping)
            .is_err()
        {
            tracing::info!("Previous batch vnode mapping not found for fragment {fragment_id}, maybe offline scaling with background ddl");
        }
    }

    pub fn update_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: WorkerSlotMapping,
    ) {
        let mut guard = self.inner.write().unwrap();
        if guard
            .streaming_fragment_vnode_mapping
            .insert(fragment_id, vnode_mapping)
            .is_none()
        {
            tracing::info!("Previous vnode mapping not found for fragment {fragment_id}, maybe offline scaling with background ddl");
        }
    }

    pub fn remove_streaming_fragment_mapping(&self, fragment_id: &FragmentId) {
        let mut guard = self.inner.write().unwrap();

        let res = guard.streaming_fragment_vnode_mapping.remove(fragment_id);
        match &res {
            Some(_) => {}
            None if OBJECT_ID_PLACEHOLDER == *fragment_id => {
                // Do nothing for placeholder fragment.
            }
            None => {
                tracing::warn!(fragment_id, "Streaming vnode mapping not found");
            }
        };
    }

    /// Returns fragment's vnode mapping for serving.
    fn serving_fragment_mapping(&self, fragment_id: FragmentId) -> Result<WorkerSlotMapping> {
        self.inner
            .read()
            .unwrap()
            .get_serving_fragment_mapping(fragment_id)
            .ok_or_else(|| BatchError::ServingVnodeMappingNotFound(fragment_id))
    }

    pub fn set_serving_fragment_mapping(&self, mappings: HashMap<FragmentId, WorkerSlotMapping>) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Set serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        guard.serving_fragment_vnode_mapping = mappings;
    }

    pub fn upsert_serving_fragment_mapping(
        &self,
        mappings: HashMap<FragmentId, WorkerSlotMapping>,
    ) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Upsert serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        for (fragment_id, mapping) in mappings {
            guard
                .serving_fragment_vnode_mapping
                .insert(fragment_id, mapping);
        }
    }

    pub fn remove_serving_fragment_mapping(&self, fragment_ids: &[FragmentId]) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Delete serving vnode mapping for fragments {:?}",
            fragment_ids
        );
        for fragment_id in fragment_ids {
            guard.serving_fragment_vnode_mapping.remove(fragment_id);
        }
    }

    fn worker_node_mask(&self) -> RwLockReadGuard<'_, HashSet<u32>> {
        self.worker_node_mask.read().unwrap()
    }

    pub fn mask_worker_node(&self, worker_node_id: u32, duration: Duration) {
        tracing::info!(
            "Mask worker node {} for {:?} temporarily",
            worker_node_id,
            duration
        );
        let mut worker_node_mask = self.worker_node_mask.write().unwrap();
        if worker_node_mask.contains(&worker_node_id) {
            return;
        }
        worker_node_mask.insert(worker_node_id);
        let worker_node_mask_ref = self.worker_node_mask.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            worker_node_mask_ref
                .write()
                .unwrap()
                .remove(&worker_node_id);
        });
    }
}

impl WorkerNodeManagerInner {
    fn get_serving_fragment_mapping(&self, fragment_id: FragmentId) -> Option<WorkerSlotMapping> {
        self.serving_fragment_vnode_mapping
            .get(&fragment_id)
            .cloned()
    }
}

/// Selects workers for query according to `enable_barrier_read`
#[derive(Clone)]
pub struct WorkerNodeSelector {
    pub manager: WorkerNodeManagerRef,
    enable_barrier_read: bool,
}

impl WorkerNodeSelector {
    pub fn new(manager: WorkerNodeManagerRef, enable_barrier_read: bool) -> Self {
        Self {
            manager,
            enable_barrier_read,
        }
    }

    pub fn worker_node_count(&self) -> usize {
        if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes().len()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
                .len()
        }
    }

    pub fn schedule_unit_count(&self) -> usize {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
        };
        worker_nodes
            .iter()
            .map(|node| node.compute_node_parallelism())
            .sum()
    }

    pub fn fragment_mapping(&self, fragment_id: FragmentId) -> Result<WorkerSlotMapping> {
        if self.enable_barrier_read {
            self.manager.get_streaming_fragment_mapping(&fragment_id)
        } else {
            let mapping = (self.manager.serving_fragment_mapping(fragment_id)).or_else(|_| {
                tracing::warn!(
                    fragment_id,
                    "Serving fragment mapping not found, fall back to streaming one."
                );
                self.manager.get_streaming_fragment_mapping(&fragment_id)
            })?;

            // Filter out unavailable workers.
            if self.manager.worker_node_mask().is_empty() {
                Ok(mapping)
            } else {
                let workers = self.apply_worker_node_mask(self.manager.list_serving_worker_nodes());
                // If it's a singleton, set max_parallelism=1 for place_vnode.
                let max_parallelism = mapping.to_single().map(|_| 1);
                let masked_mapping =
                    place_vnode(Some(&mapping), &workers, max_parallelism, mapping.len())
                        .ok_or_else(|| BatchError::EmptyWorkerNodes)?;
                Ok(masked_mapping)
            }
        }
    }

    pub fn next_random_worker(&self) -> Result<WorkerNode> {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
        };
        worker_nodes
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| BatchError::EmptyWorkerNodes)
            .map(|w| (*w).clone())
    }

    fn apply_worker_node_mask(&self, origin: Vec<WorkerNode>) -> Vec<WorkerNode> {
        let mask = self.manager.worker_node_mask();
        if origin.iter().all(|w| mask.contains(&w.id)) {
            return origin;
        }
        origin
            .into_iter()
            .filter(|w| !mask.contains(&w.id))
            .collect()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node;
    use risingwave_pb::common::worker_node::Property;

    #[test]
    fn test_worker_node_manager() {
        use super::*;

        let manager = WorkerNodeManager::mock(vec![]);
        assert_eq!(manager.list_serving_worker_nodes().len(), 0);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(manager.list_worker_nodes(), vec![]);

        let worker_nodes = vec![
            WorkerNode {
                id: 1,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1234").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    is_unschedulable: false,
                    is_serving: true,
                    is_streaming: true,
                    ..Default::default()
                }),
                transactional_id: Some(1),
                ..Default::default()
            },
            WorkerNode {
                id: 2,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1235").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                property: Some(Property {
                    is_unschedulable: false,
                    is_serving: true,
                    is_streaming: false,
                    ..Default::default()
                }),
                transactional_id: Some(2),
                ..Default::default()
            },
        ];
        worker_nodes
            .iter()
            .for_each(|w| manager.add_worker_node(w.clone()));
        assert_eq!(manager.list_serving_worker_nodes().len(), 2);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 1);
        assert_eq!(manager.list_worker_nodes(), worker_nodes);

        manager.remove_worker_node(worker_nodes[0].clone());
        assert_eq!(manager.list_serving_worker_nodes().len(), 1);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(
            manager.list_worker_nodes(),
            worker_nodes.as_slice()[1..].to_vec()
        );
    }
}
