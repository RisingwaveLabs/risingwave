use std::sync::Arc;

use async_trait::async_trait;
use risingwave_pb::hummock::{CompactTask, HummockSnapshot, HummockVersion, SstableInfo};
use risingwave_storage::hummock::hummock_meta_client::HummockMetaClient;
use risingwave_storage::hummock::{
    HummockContextId, HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId,
};

use crate::hummock::HummockManager;
use crate::storage::SledMetaStore;

pub(crate) struct MockHummockMetaClient {
    hummock_manager: Arc<HummockManager<SledMetaStore>>,
    context_id: HummockContextId,
}

impl MockHummockMetaClient {
    pub fn new(
        hummock_manager: Arc<HummockManager<SledMetaStore>>,
        context_id: HummockContextId,
    ) -> MockHummockMetaClient {
        MockHummockMetaClient {
            hummock_manager,
            context_id,
        }
    }
}

#[async_trait]
impl HummockMetaClient for MockHummockMetaClient {
    async fn pin_version(&self) -> HummockResult<(HummockVersionId, HummockVersion)> {
        self.hummock_manager
            .pin_version(self.context_id)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn unpin_version(&self, pinned_version_id: HummockVersionId) -> HummockResult<()> {
        self.hummock_manager
            .unpin_version(self.context_id, pinned_version_id)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn pin_snapshot(&self) -> HummockResult<HummockEpoch> {
        self.hummock_manager
            .pin_snapshot(self.context_id)
            .await
            .map(|e| e.epoch)
            .map_err(HummockError::meta_error)
    }

    async fn unpin_snapshot(&self, pinned_epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .unpin_snapshot(
                self.context_id,
                HummockSnapshot {
                    epoch: pinned_epoch,
                },
            )
            .await
            .map_err(HummockError::meta_error)
    }

    async fn get_new_table_id(&self) -> HummockResult<HummockSSTableId> {
        self.hummock_manager
            .get_new_table_id()
            .await
            .map_err(HummockError::meta_error)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> HummockResult<()> {
        self.hummock_manager
            .add_tables(self.context_id, sstables, epoch)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn get_compaction_task(&self) -> HummockResult<Option<CompactTask>> {
        self.hummock_manager
            .get_compact_task(self.context_id)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        task_result: bool,
    ) -> HummockResult<()> {
        self.hummock_manager
            .report_compact_task(self.context_id, compact_task, task_result)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .commit_epoch(self.context_id, epoch)
            .await
            .map_err(HummockError::meta_error)
    }

    async fn abort_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        self.hummock_manager
            .abort_epoch(self.context_id, epoch)
            .await
            .map_err(HummockError::meta_error)
    }
}
