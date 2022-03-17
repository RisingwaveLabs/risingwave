use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::Result;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::{Compactor, SubCompactContext};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::iterator::{BoxedHummockIterator, MergeIterator};
use crate::hummock::key_range::KeyRange;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

#[derive(Debug)]
pub struct SyncItem {
    /// Epoch to sync. None means syncing all epochs.
    pub(super) epoch: Option<u64>,
    /// Notifier to notify on sync finishes
    pub(super) notifier: Option<tokio::sync::oneshot::Sender<HummockResult<()>>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum SharedBufferUploaderItem {
    Batch(SharedBufferBatch),
    Sync(SyncItem),
    Reset(u64),
}

pub struct SharedBufferUploader {
    /// Batches to upload grouped by epoch
    batches_to_upload: BTreeMap<u64, Vec<SharedBufferBatch>>,
    local_version_manager: Arc<LocalVersionManager>,
    options: Arc<StorageConfig>,

    /// Statistics.
    // TODO: should be separated `HummockStats` instead of `StateStoreMetrics`.
    stats: Arc<StateStoreMetrics>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_store: SstableStoreRef,

    /// For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to true
    /// in `StorageConfig`
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
    ) -> Self {
        Self {
            batches_to_upload: BTreeMap::new(),
            options: options.clone(),
            local_version_manager,

            stats,
            hummock_meta_client,
            sstable_store,
            write_conflict_detector: if options.write_conflict_detection_enabled {
                Some(Arc::new(ConflictDetector::new()))
            } else {
                None
            },
            rx,
        }
    }

    /// Upload buffer batches to S3.
    async fn sync(&mut self, epoch: u64) -> HummockResult<()> {
        if let Some(detector) = &self.write_conflict_detector {
            detector.archive_epoch(epoch);
        }

        let buffers = match self.batches_to_upload.remove(&epoch) {
            Some(m) => m,
            None => return Ok(()),
        };

        // Compact buffers into SSTs
        let merge_iters = {
            let iters = buffers
                .into_iter()
                .map(|m| Box::new(m.iter()) as BoxedHummockIterator);
            MergeIterator::new(iters)
        };
        let sub_compact_context = SubCompactContext {
            options: self.options.clone(),
            local_version_manager: self.local_version_manager.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            sstable_store: self.sstable_store.clone(),
            stats: self.stats.clone(),
            is_share_buffer_compact: true,
        };
        let mut tables = Vec::new();
        Compactor::sub_compact(
            sub_compact_context,
            KeyRange::inf(),
            merge_iters,
            &mut tables,
            false,
            u64::MAX,
        )
        .await?;

        if tables.is_empty() {
            return Ok(());
        }

        // Add all tables at once.
        let version = self
            .hummock_meta_client
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|sst| SstableInfo {
                        id: sst.id,
                        key_range: Some(risingwave_pb::hummock::KeyRange {
                            left: sst.meta.get_smallest_key().to_vec(),
                            right: sst.meta.get_largest_key().to_vec(),
                            inf: false,
                        }),
                    })
                    .collect(),
            )
            .await?;

        // Ensure the added data is available locally
        self.local_version_manager.try_set_version(version);

        Ok(())
    }

    async fn handle(&mut self, item: SharedBufferUploaderItem) -> Result<()> {
        match item {
            SharedBufferUploaderItem::Batch(m) => {
                if let Some(detector) = &self.write_conflict_detector {
                    detector.check_conflict_and_track_write_batch(&m.inner, m.epoch);
                }

                self.batches_to_upload
                    .entry(m.epoch())
                    .or_insert(Vec::new())
                    .push(m);
                Ok(())
            }
            SharedBufferUploaderItem::Sync(sync_item) => {
                let res = match sync_item.epoch {
                    Some(e) => {
                        // Sync a specific epoch
                        self.sync(e).await
                    }
                    None => {
                        // Sync all epochs
                        let epochs = self.batches_to_upload.keys().copied().collect_vec();
                        let mut res = Ok(());
                        for e in epochs {
                            res = self.sync(e).await;
                            if res.is_err() {
                                break;
                            }
                        }
                        res
                    }
                };

                if let Some(tx) = sync_item.notifier {
                    tx.send(res).map_err(|_| {
                        HummockError::shared_buffer_error(
                            "Failed to notify shared buffer sync because of send drop",
                        )
                    })?;
                }
                Ok(())
            }
            SharedBufferUploaderItem::Reset(epoch) => {
                self.batches_to_upload.remove(&epoch);
                Ok(())
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(m) = self.rx.recv().await {
            if let Err(e) = self.handle(m).await {
                return Err(e);
            }
        }
        Ok(())
    }
}
