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

use std::pin::pin;

use anyhow::Context;
use either::Either;
use futures::future::select;
use futures::{future, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::table::batch_table::BatchTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Barrier, BoxedMessageStream, Executor, Message, MessageStream};

/// Join side of Lookup Executor's stream
pub(crate) struct StreamJoinSide {
    /// Indices of the join key columns
    pub key_indices: Vec<usize>,

    /// The primary key indices of this side, used for state store
    pub pk_indices: Vec<usize>,

    /// The date type of each columns to join on
    pub col_types: Vec<DataType>,
}

impl std::fmt::Debug for StreamJoinSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrangeJoinSide")
            .field("key_indices", &self.key_indices)
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .finish()
    }
}

/// Join side of Arrange Executor's stream
pub(crate) struct ArrangeJoinSide<S: StateStore> {
    /// The primary key indices of this side, used for state store
    pub pk_indices: Vec<usize>,

    /// The datatype of columns in arrangement
    pub col_types: Vec<DataType>,

    /// The column descriptors of columns in arrangement
    pub col_descs: Vec<ColumnDesc>,

    /// Order rules of the arrangement (only join key is needed, pk should not be included, used
    /// for lookup)
    pub order_rules: Vec<ColumnOrder>,

    /// Key indices for the join
    ///
    /// The key indices of the arrange side won't be used for the lookup process, but we still
    /// record it here in case anyone would use it in the future.
    pub key_indices: Vec<usize>,

    /// Whether to join with the arrangement of the current epoch
    pub use_current_epoch: bool,

    pub batch_table: BatchTable<S>,
}

/// Message from the `arrange_join_stream`.
#[derive(Debug)]
pub enum ArrangeMessage {
    /// Arrangement sides' update in this epoch. There will be only one arrange batch message
    /// within epoch. Once the executor receives an arrange batch message, it can start doing
    /// joins.
    ArrangeReady(Vec<StreamChunk>, Barrier),

    /// There's a message from stream side.
    Stream(StreamChunk),

    /// Barrier (once every epoch).
    Barrier(Barrier),
}

pub type BarrierAlignedMessage = Either<Message, Message>;

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn poll_until_barrier(stream: impl MessageStream, expected_barrier: Barrier) {
    #[for_await]
    for item in stream {
        match item? {
            Message::Watermark(_) => {
                // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
            }
            c @ Message::Chunk(_) => yield c,
            Message::Barrier(b) => {
                if b.epoch != expected_barrier.epoch {
                    return Err(StreamExecutorError::align_barrier(expected_barrier, b));
                } else {
                    yield Message::Barrier(b);
                    break;
                }
            }
        }
    }
}

/// A biased barrier aligner which prefers message from the right side. Barrier message will be
/// available for both left and right side, instead of being combined.
#[try_stream(ok = BarrierAlignedMessage, error = StreamExecutorError)]
pub async fn align_barrier(mut left: BoxedMessageStream, mut right: BoxedMessageStream) {
    enum SideStatus {
        LeftBarrier,
        RightBarrier,
    }

    'outer: loop {
        let (side_status, side_barrier) = 'inner: loop {
            // Prefer right
            let select_result = match select(right.next(), left.next()).await {
                future::Either::Left(x) => future::Either::Right(x),
                future::Either::Right(x) => future::Either::Left(x),
            };
            match select_result {
                future::Either::Left((None, _)) => {
                    // left stream end, passthrough right chunks
                    while let Some(msg) = right.next().await {
                        match msg? {
                            w @ Message::Watermark(_) => yield Either::Left(w),
                            c @ Message::Chunk(_) => yield Either::Left(c),
                            Message::Barrier(_) => {
                                bail!("right barrier received while left stream end");
                            }
                        }
                    }
                    break 'outer;
                }
                future::Either::Right((None, _)) => {
                    // right stream end, passthrough left chunks
                    while let Some(msg) = left.next().await {
                        match msg? {
                            w @ Message::Watermark(_) => yield Either::Right(w),
                            c @ Message::Chunk(_) => yield Either::Right(c),
                            Message::Barrier(_) => {
                                bail!("left barrier received while right stream end");
                            }
                        }
                    }
                    break 'outer;
                }
                future::Either::Left((Some(msg), _)) => match msg? {
                    w @ Message::Watermark(_) => yield Either::Left(w),
                    c @ Message::Chunk(_) => yield Either::Left(c),
                    Message::Barrier(b) => {
                        yield Either::Left(Message::Barrier(b.clone()));
                        break 'inner (SideStatus::LeftBarrier, b);
                    }
                },
                future::Either::Right((Some(msg), _)) => match msg? {
                    w @ Message::Watermark(_) => yield Either::Right(w),
                    c @ Message::Chunk(_) => yield Either::Right(c),
                    Message::Barrier(b) => {
                        yield Either::Right(Message::Barrier(b.clone()));
                        break 'inner (SideStatus::RightBarrier, b);
                    }
                },
            }
        };

        match side_status {
            SideStatus::LeftBarrier => {
                #[for_await]
                for item in poll_until_barrier(right.by_ref(), side_barrier) {
                    yield Either::Right(item?);
                }
            }
            SideStatus::RightBarrier => {
                #[for_await]
                for item in poll_until_barrier(left.by_ref(), side_barrier) {
                    yield Either::Left(item?);
                }
            }
        }
    }
}

/// Join the stream with the previous stable snapshot of the arrangement.
///
/// For example, the executor will receive the following message sequence from
/// `stream_lookup_arrange_prev_epoch`:
///
/// * `[Msg`] Barrier (prev = `[1`], current = `[2`])
/// * `[Msg`] Stream (key = a)
/// * `[Do`] lookup `a` in arrangement of epoch `[1`] (prev epoch)
/// * `[Msg`] Arrangement (batch)
/// * `[Msg`] Stream (key = b)
/// * `[Do`] lookup `b` in arrangement of epoch `[1`] (prev epoch)
/// * `[Do`] update cache with epoch `[2`]
/// * Barrier (prev = `[2`], current = `[3`])
/// * `[Msg`] Arrangement (batch)
#[try_stream(ok = ArrangeMessage, error = StreamExecutorError)]
pub async fn stream_lookup_arrange_prev_epoch(stream: Executor, arrangement: Executor) {
    let mut input = pin!(align_barrier(stream.execute(), arrangement.execute()));
    let mut arrange_buf = vec![];
    let mut stream_side_end = false;

    loop {
        let mut arrange_barrier = None;

        while let Some(item) = input.next().await {
            match item? {
                Either::Left(Message::Chunk(msg)) => {
                    // As prev epoch is already available, we can directly forward messages from the
                    // stream side.
                    yield ArrangeMessage::Stream(msg);
                }
                Either::Right(Message::Chunk(chunk)) => {
                    // For message from the arrangement side, put it in a buf
                    arrange_buf.push(chunk);
                }
                Either::Left(Message::Barrier(barrier)) => {
                    yield ArrangeMessage::Barrier(barrier);
                    stream_side_end = true;
                }
                Either::Right(Message::Barrier(barrier)) => {
                    if stream_side_end {
                        yield ArrangeMessage::ArrangeReady(
                            std::mem::take(&mut arrange_buf),
                            barrier,
                        );
                        stream_side_end = false;
                    } else {
                        arrange_barrier = Some(barrier);
                        break;
                    }
                }
                Either::Left(Message::Watermark(_)) => {
                    // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                }
                Either::Right(Message::Watermark(_)) => {
                    // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                }
            }
        }

        loop {
            match input
                .next()
                .await
                .context("unexpected close of barrier aligner")??
            {
                Either::Left(Message::Watermark(_)) => {
                    // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                }
                Either::Left(Message::Chunk(msg)) => yield ArrangeMessage::Stream(msg),
                Either::Left(Message::Barrier(b)) => {
                    yield ArrangeMessage::Barrier(b);
                    break;
                }
                Either::Right(_) => unreachable!(),
            }
        }

        yield ArrangeMessage::ArrangeReady(
            std::mem::take(&mut arrange_buf),
            arrange_barrier.take().unwrap(),
        );
    }
}

/// Join the stream with the current state of the arrangement.
///
/// For example, the executor will receive the following message sequence from
/// `stream_lookup_arrange_this_epoch`:
///
/// * `[Msg`] Barrier (prev = `[1`], current = `[2`])
/// * `[Msg`] Arrangement (batch)
/// * `[Do`] update cache with epoch `[2`]
/// * `[Msg`] Stream (key = a)
/// * `[Do`] lookup `a` in arrangement of epoch `[2`] (current epoch)
/// * Barrier (prev = `[2`], current = `[3`])
#[try_stream(ok = ArrangeMessage, error = StreamExecutorError)]
pub async fn stream_lookup_arrange_this_epoch(stream: Executor, arrangement: Executor) {
    let mut input = pin!(align_barrier(stream.execute(), arrangement.execute()));
    let mut stream_buf = vec![];
    let mut arrange_buf = vec![];

    enum Status {
        ArrangeReady,
        StreamReady(Barrier),
    }

    loop {
        let status = 'inner: loop {
            match input
                .next()
                .await
                .context("unexpected close of barrier aligner")??
            {
                Either::Left(Message::Chunk(msg)) => {
                    // Should wait until arrangement from this epoch is available.
                    stream_buf.push(msg);
                }
                Either::Right(Message::Chunk(chunk)) => {
                    // For message from the arrangement side, put it in buf.
                    arrange_buf.push(chunk);
                }
                Either::Left(Message::Barrier(barrier)) => {
                    break 'inner Status::StreamReady(barrier);
                }
                Either::Right(Message::Barrier(barrier)) => {
                    yield ArrangeMessage::ArrangeReady(std::mem::take(&mut arrange_buf), barrier);
                    for msg in std::mem::take(&mut stream_buf) {
                        yield ArrangeMessage::Stream(msg);
                    }
                    break 'inner Status::ArrangeReady;
                }
                Either::Left(Message::Watermark(_)) => {
                    // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                }
                Either::Right(Message::Watermark(_)) => {
                    // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                }
            }
        };
        match status {
            // Arrangement is ready, but still stream message in this epoch -- we directly forward
            // message from the stream side.
            Status::ArrangeReady => loop {
                match input
                    .next()
                    .await
                    .context("unexpected close of barrier aligner")??
                {
                    Either::Left(Message::Chunk(msg)) => yield ArrangeMessage::Stream(msg),
                    Either::Left(Message::Barrier(b)) => {
                        yield ArrangeMessage::Barrier(b);
                        break;
                    }
                    Either::Left(Message::Watermark(_)) => {
                        // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                    }
                    Either::Right(Message::Watermark(_)) => {
                        // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                    }
                    Either::Right(_) => unreachable!(),
                }
            },
            // Stream is done in this epoch, but arrangement is not ready -- we wait for the
            // arrangement ready and pipe out all buffered stream messages.
            Status::StreamReady(stream_barrier) => loop {
                match input
                    .next()
                    .await
                    .context("unexpected close of barrier aligner")??
                {
                    Either::Left(_) => unreachable!(),
                    Either::Right(Message::Chunk(chunk)) => {
                        arrange_buf.push(chunk);
                    }
                    Either::Right(Message::Barrier(barrier)) => {
                        yield ArrangeMessage::ArrangeReady(
                            std::mem::take(&mut arrange_buf),
                            barrier,
                        );
                        for msg in std::mem::take(&mut stream_buf) {
                            yield ArrangeMessage::Stream(msg);
                        }
                        yield ArrangeMessage::Barrier(stream_barrier);
                        break;
                    }
                    Either::Right(Message::Watermark(_)) => {
                        // TODO: https://github.com/risingwavelabs/risingwave/issues/6042
                    }
                }
            },
        }
    }
}

impl<S: StateStore> std::fmt::Debug for ArrangeJoinSide<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrangeJoinSide")
            .field("pk_indices", &self.pk_indices)
            .field("col_types", &self.col_types)
            .field("col_descs", &self.col_descs)
            .field("order_rules", &self.order_rules)
            .field("use_current_epoch", &self.use_current_epoch)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;

    use crate::executor::lookup::sides::stream_lookup_arrange_this_epoch;
    use crate::executor::test_utils::MockSource;
    use crate::executor::StreamExecutorResult;

    #[tokio::test]
    async fn test_stream_lookup_arrange_this_epoch() -> StreamExecutorResult<()> {
        let chunk_l1 = StreamChunk::from_pretty(
            "  I I
             + 1 1",
        );

        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64), // join key
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx_l, source_l) = MockSource::channel();
        let source_l = source_l
            .stop_on_finish(false)
            .into_executor(schema.clone(), vec![1]);
        let (tx_r, source_r) = MockSource::channel();
        let source_r = source_r
            .stop_on_finish(false)
            .into_executor(schema, vec![1]);

        let mut stream = stream_lookup_arrange_this_epoch(source_l, source_r).boxed();

        // Simulate recovery test
        drop(tx_r);

        tx_l.push_barrier(test_epoch(1), false);

        tx_l.push_chunk(chunk_l1);

        // It should throw an error instead of panic.
        stream.next().await.unwrap().unwrap_err();

        Ok(())
    }
}
