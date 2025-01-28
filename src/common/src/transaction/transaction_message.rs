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

use enum_as_inner::EnumAsInner;
use tokio::sync::oneshot;

use crate::array::StreamChunk;
use crate::transaction::transaction_id::TxnId;
use crate::transaction::transaction_message::TxnMsg::{Begin, Data, End, Rollback};
use crate::util::epoch::Epoch;

#[derive(Debug, EnumAsInner)]
pub enum TxnMsg {
    Begin(TxnId),
    Data(TxnId, StreamChunk),
    End(TxnId, Option<oneshot::Sender<Epoch>>),
    Rollback(TxnId),
}

impl TxnMsg {
    pub fn txn_id(&self) -> TxnId {
        match self {
            Begin(txn_id) => *txn_id,
            Data(txn_id, _) => *txn_id,
            End(txn_id, _) => *txn_id,
            Rollback(txn_id) => *txn_id,
        }
    }

    pub fn as_stream_chunk(&self) -> Option<&StreamChunk> {
        match self {
            Begin(_) | End(..) | Rollback(_) => None,
            Data(_, chunk) => Some(chunk),
        }
    }
}
