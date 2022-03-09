use anyhow::anyhow;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use serde::{Deserialize, Serialize};

use crate::base::{SourceMessage, SourceOffset};

#[derive(Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

impl SourceMessage for KafkaMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.payload.as_ref().map(|payload| payload.as_ref()))
    }

    fn offset(&self) -> anyhow::Result<Option<SourceOffset>> {
        Ok(Some(SourceOffset::Number(self.offset)))
    }

    fn serialize(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl<'a> From<BorrowedMessage<'a>> for KafkaMessage {
    fn from(message: BorrowedMessage<'a>) -> Self {
        KafkaMessage {
            partition: message.partition(),
            offset: message.offset(),
            key: message.key().map(|key| key.to_vec()),
            payload: message.payload().map(|m| m.to_vec()),
        }
    }
}
