// Copyright 2024 RisingWave Labs
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

use risingwave_common::types::{Datum, Scalar, Timestamptz};
use risingwave_pb::connector_service::CdcMessage;

use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

#[derive(Debug, Clone)]
pub struct DebeziumCdcMeta {
    pub full_table_name: String,
    // extracted from `payload.source.ts_ms`, the time that the change event was made in the database
    pub source_ts_ms: i64,
    // Whether the message is a transaction metadata
    pub is_transaction_meta: bool,
}

impl DebeziumCdcMeta {
    pub fn extract_timestamp(&self) -> Option<Datum> {
        Some(
            Timestamptz::from_millis(self.source_ts_ms)
                .unwrap()
                .to_scalar_value(),
        )
        .into()
    }
}

impl From<CdcMessage> for SourceMessage {
    fn from(message: CdcMessage) -> Self {
        SourceMessage {
            key: if message.key.is_empty() {
                None // only data message has key
            } else {
                Some(message.key.as_bytes().to_vec())
            },
            payload: if message.payload.is_empty() {
                None // heartbeat message
            } else {
                Some(message.payload.as_bytes().to_vec())
            },
            offset: message.offset,
            split_id: message.partition.into(),
            meta: SourceMeta::DebeziumCdc(DebeziumCdcMeta {
                full_table_name: message.full_table_name,
                source_ts_ms: message.source_ts_ms,
                is_transaction_meta: message.is_transaction_meta,
            }),
        }
    }
}
