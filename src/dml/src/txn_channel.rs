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

use std::sync::Arc;

use futures::FutureExt;
use risingwave_common::transaction::transaction_message::TxnMsg;
use tokio::sync::{mpsc, oneshot, Semaphore};

pub struct PermitValue(u32);

pub struct TxnMsgWithPermits {
    pub txn_msg: TxnMsg,
    pub notificator: oneshot::Sender<usize>,
    pub permit_value: Option<PermitValue>,
}

/// Create a channel for transaction messages.
pub fn txn_channel(max_chunk_permits: usize) -> (Sender, Receiver) {
    // Use an unbounded channel since we manage the permits manually.
    let (tx, rx) = mpsc::unbounded_channel();

    let records = Semaphore::new(max_chunk_permits);
    let permits = Arc::new(Permits { records });

    (
        Sender {
            tx,
            permits: permits.clone(),
            max_chunk_permits,
        },
        Receiver { rx, permits },
    )
}

/// Semaphore-based permits to control the back-pressure.
///
/// The number of messages in the transaction channel is limited by these semaphores.
#[derive(Debug)]
pub struct Permits {
    /// The permits for records in chunks.
    records: Semaphore,
}

impl Permits {
    /// Add permits back to the semaphores.
    pub fn add_permits(&self, permit_value: PermitValue) {
        self.records.add_permits(permit_value.0 as usize)
    }
}

/// The sender of the transaction channel with permit-based back-pressure.
#[derive(Debug, Clone)]
pub struct Sender {
    pub tx: mpsc::UnboundedSender<TxnMsgWithPermits>,
    permits: Arc<Permits>,

    /// The maximum permits required by a chunk. If there're too many rows in a chunk, we only
    /// acquire these permits.
    max_chunk_permits: usize,
}

impl Sender {
    /// Send a message, waiting until there are enough permits.
    /// Used to send transaction data messages.
    ///
    /// Returns error if the receive half of the channel is closed, including the message passed.
    pub async fn send(
        &self,
        txn_msg: TxnMsg,
        notificator: oneshot::Sender<usize>,
    ) -> Result<(), mpsc::error::SendError<TxnMsg>> {
        // The semaphores should never be closed.
        let permits = match &txn_msg {
            TxnMsg::Data(_, c) => {
                let card = c.cardinality().clamp(1, self.max_chunk_permits);
                if card == self.max_chunk_permits {
                    tracing::warn!(
                        cardinality = c.cardinality(),
                        "large chunk in transaction channel"
                    )
                }
                self.permits
                    .records
                    .acquire_many(card as _)
                    .await
                    .unwrap()
                    .forget();
                Some(PermitValue(card as _))
            }
            TxnMsg::Begin(_) | TxnMsg::Rollback(_) | TxnMsg::End(..) => None,
        };

        self.tx
            .send(TxnMsgWithPermits {
                txn_msg,
                notificator,
                permit_value: permits,
            })
            .map_err(|e| mpsc::error::SendError(e.0.txn_msg))
    }

    /// Send a message without permit acquiring.
    /// Used to send transaction control messages.
    ///
    /// Returns error if the receive half of the channel is closed, including the message passed.
    pub fn send_immediate(
        &self,
        txn_msg: TxnMsg,
        notificator: oneshot::Sender<usize>,
    ) -> Result<(), mpsc::error::SendError<TxnMsg>> {
        self.send(txn_msg, notificator)
            .now_or_never()
            .expect("cannot send immediately")
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// The receiver of the txn channel with permit-based back-pressure.
#[derive(Debug)]
pub struct Receiver {
    rx: mpsc::UnboundedReceiver<TxnMsgWithPermits>,
    permits: Arc<Permits>,
}

impl Receiver {
    /// Receive the next message for this receiver, with the permits of this message added back.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv(&mut self) -> Option<(TxnMsg, oneshot::Sender<usize>)> {
        let TxnMsgWithPermits {
            txn_msg,
            notificator,
            permit_value: permits,
        } = self.rx.recv().await?;

        if let Some(permits) = permits {
            self.permits.add_permits(permits);
        }

        Some((txn_msg, notificator))
    }
}
