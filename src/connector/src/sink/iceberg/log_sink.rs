use std::time::Instant;

use async_trait::async_trait;

use crate::sink::log_store::{LogStoreReadItem, TruncateOffset};
use crate::sink::writer::SinkWriter;
use crate::sink::{LogSinker, Result, SinkLogReader, SinkMetrics};

pub struct IcebergLogSinkerOf<W> {
    writer: W,
    sink_metrics: SinkMetrics,
    commit_checkpoint_interval: u64,
}

impl<W> IcebergLogSinkerOf<W> {
    /// Create a log sinker with a commit checkpoint interval. The sinker should be used with a
    /// decouple log reader `KvLogStoreReader`.
    pub fn new(writer: W, sink_metrics: SinkMetrics, commit_checkpoint_interval: u64) -> Self {
        IcebergLogSinkerOf {
            writer,
            sink_metrics,
            commit_checkpoint_interval,
        }
    }
}

#[async_trait]
impl<W: SinkWriter<CommitMetadata = ()>> LogSinker for IcebergLogSinkerOf<W> {
    async fn consume_log_and_sink(self, log_reader: &mut impl SinkLogReader) -> Result<()> {
        let mut sink_writer = self.writer;
        let sink_metrics = self.sink_metrics;
        #[derive(Debug)]
        enum LogConsumerState {
            /// Mark that the log consumer is not initialized yet
            Uninitialized,

            /// Mark that a new epoch has begun.
            EpochBegun { curr_epoch: u64 },

            /// Mark that the consumer has just received a barrier
            BarrierReceived { prev_epoch: u64 },
        }

        let mut state = LogConsumerState::Uninitialized;

        let mut current_checkpoint: u64 = 0;
        let commit_checkpoint_interval = self.commit_checkpoint_interval;

        loop {
            let (epoch, item): (u64, LogStoreReadItem) = log_reader.next_item().await?;
            if let LogStoreReadItem::UpdateVnodeBitmap(_) = &item {
                match &state {
                    LogConsumerState::BarrierReceived { .. } => {}
                    _ => unreachable!(
                        "update vnode bitmap can be accepted only right after \
                    barrier, but current state is {:?}",
                        state
                    ),
                }
            }
            // begin_epoch when not previously began
            state = match state {
                LogConsumerState::Uninitialized => {
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::EpochBegun { curr_epoch } => {
                    assert!(
                        epoch >= curr_epoch,
                        "new epoch {} should not be below the current epoch {}",
                        epoch,
                        curr_epoch
                    );
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
                LogConsumerState::BarrierReceived { prev_epoch } => {
                    assert!(
                        epoch > prev_epoch,
                        "new epoch {} should be greater than prev epoch {}",
                        epoch,
                        prev_epoch
                    );
                    sink_writer.begin_epoch(epoch).await?;
                    LogConsumerState::EpochBegun { curr_epoch: epoch }
                }
            };
            match item {
                LogStoreReadItem::StreamChunk { chunk, .. } => {
                    if let Err(e) = sink_writer.write_batch(chunk).await {
                        sink_writer.abort().await?;
                        return Err(e);
                    }
                }
                LogStoreReadItem::Barrier { is_checkpoint } => {
                    let prev_epoch = match state {
                        LogConsumerState::EpochBegun { curr_epoch } => curr_epoch,
                        _ => unreachable!("epoch must have begun before handling barrier"),
                    };
                    if is_checkpoint {
                        current_checkpoint += 1;
                        if commit_checkpoint_interval <= 1
                            || current_checkpoint >= commit_checkpoint_interval
                        {
                            let start_time = Instant::now();
                            sink_writer.barrier(true).await?;
                            sink_metrics
                                .sink_commit_duration_metrics
                                .observe(start_time.elapsed().as_millis() as f64);
                            log_reader
                                .truncate(TruncateOffset::Barrier { epoch })
                                .await?;
                            current_checkpoint = 0;
                        } else {
                            sink_writer.barrier(false).await?;
                        }
                    } else {
                        sink_writer.barrier(false).await?;
                    }
                    state = LogConsumerState::BarrierReceived { prev_epoch }
                }
                LogStoreReadItem::UpdateVnodeBitmap(vnode_bitmap) => {
                    sink_writer.update_vnode_bitmap(vnode_bitmap).await?;
                }
            }
        }
    }
}
