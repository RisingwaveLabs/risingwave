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
use std::sync::Arc;

use risingwave_common::catalog::{ColumnId, TableId};
use risingwave_connector::source::reader::desc::SourceDescBuilder;
use risingwave_connector::source::{SplitId, SplitImpl, SplitMetaData};
use risingwave_storage::StateStore;
use tokio::sync::RwLock;

use super::SourceStateTableHandler;

/// [`StreamSourceCore`] stores the necessary information for the source executor to execute on the
/// external connector.
pub struct StreamSourceCore<S: StateStore> {
    pub(crate) source_id: TableId,
    pub(crate) source_name: String,

    pub(crate) column_ids: Vec<ColumnId>,

    /// `source_desc_builder` will be taken (`mem::take`) on execution. A `SourceDesc` (currently
    /// named `SourceDescV2`) will be constructed and used for execution.
    pub(crate) source_desc_builder: Option<SourceDescBuilder>,

    /// Split info for stream source. A source executor might read data from several splits of
    /// external connector.
    pub(crate) latest_split_info: Arc<RwLock<HashMap<SplitId, SplitImpl>>>,

    /// Stores information of the splits.
    pub(crate) split_state_store: SourceStateTableHandler<S>,

    /// Contains the latests offsets for the splits that are updated *in the current epoch*.
    /// It is cleared after each barrier.
    ///
    /// Source messages will only write the cache.
    /// It is read on split change and rebuild stream reader on error.
    pub(crate) updated_splits_in_epoch: HashMap<SplitId, SplitImpl>,
}

impl<S> StreamSourceCore<S>
where
    S: StateStore,
{
    pub fn new(
        source_id: TableId,
        source_name: String,
        column_ids: Vec<ColumnId>,
        source_desc_builder: SourceDescBuilder,
        split_state_store: SourceStateTableHandler<S>,
    ) -> Self {
        Self {
            source_id,
            source_name,
            column_ids,
            source_desc_builder: Some(source_desc_builder),
            latest_split_info: Arc::new(RwLock::new(HashMap::new())),
            split_state_store,
            updated_splits_in_epoch: HashMap::new(),
        }
    }

    pub async fn init_split_state(&mut self, splits: Vec<SplitImpl>) {
        let mut latest_split_info_guard = self.latest_split_info.write().await;
        for split in splits {
            latest_split_info_guard.insert(split.id(), split);
        }
    }
}
