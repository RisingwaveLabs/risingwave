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

//! Channel implementation for permit-based back-pressure.

use std::sync::Arc;

use risingwave_pb::task_service::permits;
use tokio::sync::{mpsc, AcquireError, Semaphore, SemaphorePermit};

use crate::executor::DispatcherMessageBatch as Message;

/// Message with its required permits.
///
/// We store the `permits` in the struct instead of implying it from the `message` so that the
/// permit number is totally determined by the sender and the downstream only needs to give the
/// `permits` back verbatim, in case the version of the upstream and the downstream are different.
pub struct MessageWithPermits {
    pub message: Message,
    pub permits: Option<permits::Value>,
}

/// Create a channel for the exchange service.
pub fn channel(
    initial_permits: usize,
    batched_permits: usize,
    concurrent_barriers: usize,
) -> (Sender, Receiver) {
    // Use an unbounded channel since we manage the permits manually.
    let (tx, rx) = mpsc::unbounded_channel();

    let records = Semaphore::new(initial_permits);
    let barriers = Semaphore::new(concurrent_barriers);
    let permits = Arc::new(Permits { records, barriers });

    let max_chunk_permits: usize = initial_permits - batched_permits;

    (
        Sender {
            tx,
            permits: permits.clone(),
            max_chunk_permits,
        },
        Receiver { rx, permits },
    )
}

/// The configuration for tests.
pub mod for_test {
    pub const INITIAL_PERMITS: usize = (u32::MAX / 2) as _;
    pub const BATCHED_PERMITS: usize = 1;
    pub const CONCURRENT_BARRIERS: usize = (u32::MAX / 2) as _;
}

pub fn channel_for_test() -> (Sender, Receiver) {
    use for_test::*;

    channel(INITIAL_PERMITS, BATCHED_PERMITS, CONCURRENT_BARRIERS)
}

/// Semaphore-based permits to control the back-pressure.
///
/// The number of messages in the exchange channel is limited by these semaphores.
pub struct Permits {
    /// The permits for records in chunks.
    records: Semaphore,
    /// The permits for barriers.
    barriers: Semaphore,
}

impl Permits {
    /// Add permits back to the semaphores.
    pub fn add_permits(&self, permits: permits::Value) {
        match permits {
            permits::Value::Record(p) => self.records.add_permits(p as usize),
            permits::Value::Barrier(p) => self.barriers.add_permits(p as usize),
        }
    }

    /// Acquire permits from the semaphores.
    ///
    /// This function is cancellation-safe except for the fairness of waking.
    async fn acquire_permits(&self, permits: &permits::Value) -> Result<(), AcquireError> {
        match permits {
            permits::Value::Record(p) => self.records.acquire_many(*p as _),
            permits::Value::Barrier(p) => self.barriers.acquire_many(*p as _),
        }
        .await
        .map(SemaphorePermit::forget)
    }

    /// Close the semaphores so that all pending `acquire` will fail immediately.
    fn close(&self) {
        self.records.close();
        self.barriers.close();
    }
}

/// The sender of the exchange service with permit-based back-pressure.
pub struct Sender {
    tx: mpsc::UnboundedSender<MessageWithPermits>,
    permits: Arc<Permits>,

    /// The maximum permits required by a chunk. If there're too many rows in a chunk, we only
    /// acquire these permits. `BATCHED_PERMITS` is subtracted to avoid deadlock with
    /// batching.
    max_chunk_permits: usize,
}

impl Sender {
    /// Send a message, waiting until there are enough permits.
    ///
    /// Returns error if the receive half of the channel is closed, including the message passed.
    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        // The semaphores should never be closed.
        let permits = match &message {
            Message::Chunk(c) => {
                let card = c.cardinality().clamp(1, self.max_chunk_permits);
                if card == self.max_chunk_permits {
                    tracing::warn!(cardinality = c.cardinality(), "large chunk in exchange")
                }
                Some(permits::Value::Record(card as _))
            }
            Message::BarrierBatch(_) => Some(permits::Value::Barrier(1)),
            Message::Watermark(_) => None,
        };

        if let Some(permits) = &permits {
            if self.permits.acquire_permits(permits).await.is_err() {
                return Err(mpsc::error::SendError(message));
            }
        }

        self.tx
            .send(MessageWithPermits { message, permits })
            .map_err(|e| mpsc::error::SendError(e.0.message))
    }
}

/// The receiver of the exchange service with permit-based back-pressure.
pub struct Receiver {
    rx: mpsc::UnboundedReceiver<MessageWithPermits>,
    permits: Arc<Permits>,
}

impl Receiver {
    /// Receive the next message for this receiver, with the permits of this message added back.
    /// Used for local exchange.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv(&mut self) -> Option<Message> {
        let MessageWithPermits { message, permits } = self.recv_raw().await?;

        if let Some(permits) = permits {
            self.permits.add_permits(permits);
        }

        Some(message)
    }

    /// Try to receive the next message for this receiver, with the permits of this message added
    /// back.
    ///
    /// Returns error if the channel is currently empty.
    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        let MessageWithPermits { message, permits } = self.rx.try_recv()?;

        if let Some(permits) = permits {
            self.permits.add_permits(permits);
        }

        Ok(message)
    }

    /// Receive the next message and its permits for this receiver, **without** adding the permits
    /// back. Used for remote exchange where the permits should be manually added according to the
    /// downstream actor.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv_raw(&mut self) -> Option<MessageWithPermits> {
        self.rx.recv().await
    }

    /// Get a reference to the inner [`Permits`] to manually add permits.
    pub fn permits(&self) -> Arc<Permits> {
        self.permits.clone()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // Close the `permits` semaphores so that all pending `acquire` on the sender side will fail
        // immediately.
        self.permits.close();
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::pin::pin;

    use futures::FutureExt;

    use super::*;
    use crate::executor::DispatcherBarrier as Barrier;

    #[test]
    fn test_channel_close() {
        let (tx, mut rx) = channel(0, 0, 1);

        let send = || {
            tx.send(Message::BarrierBatch(vec![
                Barrier::with_prev_epoch_for_test(514, 114),
            ]))
        };

        assert_matches!(send().now_or_never(), Some(Ok(_))); // send successfully
        assert_matches!(
            rx.recv().now_or_never(),
            Some(Some(Message::BarrierBatch(_)))
        ); // recv successfully

        assert_matches!(send().now_or_never(), Some(Ok(_))); // send successfully
                                                             // do not recv, so that the channel is full

        let mut send_fut = pin!(send());
        assert_matches!((&mut send_fut).now_or_never(), None); // would block due to no permits
        drop(rx);
        assert_matches!(send_fut.now_or_never(), Some(Err(_))); // channel closed
    }
}
