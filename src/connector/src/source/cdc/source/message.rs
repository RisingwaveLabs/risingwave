// Copyright 2023 RisingWave Labs
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

use bytes::Bytes;
use risingwave_pb::connector_service::CdcMessage;

use crate::source::base::SourceMessage;
use crate::source::SourceMeta;

impl From<CdcMessage> for SourceMessage {
    fn from(message: CdcMessage) -> Self {
        SourceMessage {
            payload: Some(Bytes::from(message.payload)),
            offset: message.offset,
            split_id: message.partition.into(),
            meta: SourceMeta::Empty,
        }
    }
}
