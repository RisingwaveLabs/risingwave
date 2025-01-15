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

use std::collections::HashMap;

use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};

use crate::connector_common::{AwsAuthProps, KafkaConnectionProps, KafkaPrivateLinkCommon};

mod client_context;
pub mod enumerator;
pub mod private_link;
pub mod source;
pub mod split;
pub mod stats;

pub use client_context::*;
pub use enumerator::*;
pub use source::*;
pub use split::*;
use with_options::WithOptions;

use crate::connector_common::{KafkaCommon, RdKafkaPropertiesCommon};
use crate::source::SourceProperties;

pub const KAFKA_CONNECTOR: &str = "kafka";
pub const KAFKA_PROPS_BROKER_KEY: &str = "properties.bootstrap.server";
pub const KAFKA_PROPS_BROKER_KEY_ALIAS: &str = "kafka.brokers";
pub const PRIVATELINK_CONNECTION: &str = "privatelink";

/// Properties for the rdkafka library. Leave a field as `None` to use the default value.
/// These properties are not intended to be exposed to users in the majority of cases.
///
/// See also <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>
#[serde_as]
#[derive(Clone, Debug, Deserialize, Default, WithOptions)]
pub struct RdKafkaPropertiesConsumer {
    /// Minimum number of messages per topic+partition librdkafka tries to maintain in the local
    /// consumer queue.
    #[serde(rename = "properties.queued.min.messages")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queued_min_messages: Option<usize>,

    #[serde(rename = "properties.queued.max.messages.kbytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub queued_max_messages_kbytes: Option<usize>,

    /// Maximum time the broker may wait to fill the Fetch response with `fetch.min.`bytes of
    /// messages.
    #[serde(rename = "properties.fetch.wait.max.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub fetch_wait_max_ms: Option<usize>,

    /// How long to postpone the next fetch request for a topic+partition in case the current fetch
    /// queue thresholds (`queued.min.messages` or `queued.max.messages.kbytes`) have been
    /// exceeded. This property may need to be decreased if the queue thresholds are set low
    /// and the application is experiencing long (~1s) delays between messages. Low values may
    /// increase CPU utilization.
    #[serde(rename = "properties.fetch.queue.backoff.ms")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub fetch_queue_backoff_ms: Option<usize>,

    /// Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in
    /// batches by the consumer and if the first message batch in the first non-empty partition of
    /// the Fetch request is larger than this value, then the message batch will still be returned
    /// to ensure the consumer can make progress. The maximum message batch size accepted by the
    /// broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker
    /// topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least
    /// `message.max.bytes` (consumer config).
    #[serde(rename = "properties.fetch.max.bytes")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub fetch_max_bytes: Option<usize>,

    /// Whether to automatically and periodically commit offsets in the background.
    ///
    /// Note that RisingWave does NOT rely on committed offsets. Committing offset is only for exposing the
    /// progress for monitoring. Setting this to false can avoid creating consumer groups.
    ///
    /// default: true
    #[serde(rename = "properties.enable.auto.commit")]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub enable_auto_commit: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct KafkaProperties {
    /// This parameter is not intended to be exposed to users.
    /// This parameter specifies only for one parallelism. The parallelism of kafka source
    /// is equal to the parallelism passed into compute nodes. So users need to calculate
    /// how many bytes will be consumed in total across all the parallelism by themselves.
    #[serde(rename = "bytes.per.second", alias = "kafka.bytes.per.second")]
    pub bytes_per_second: Option<String>,

    /// This parameter is not intended to be exposed to users.
    /// This parameter specifies only for one parallelism. The parallelism of kafka source
    /// is equal to the parallelism passed into compute nodes. So users need to calculate
    /// how many messages will be consumed in total across all the parallelism by themselves.
    #[serde(rename = "max.num.messages", alias = "kafka.max.num.messages")]
    pub max_num_messages: Option<String>,

    #[serde(rename = "scan.startup.mode", alias = "kafka.scan.startup.mode")]
    pub scan_startup_mode: Option<String>,

    #[serde(
        rename = "scan.startup.timestamp.millis",
        alias = "kafka.time.offset",
        alias = "scan.startup.timestamp_millis" // keep for compatibility
    )]
    pub time_offset: Option<String>,

    /// Specify a custom consumer group id prefix for the source.
    /// Defaults to `rw-consumer`.
    ///
    /// Notes:
    /// - Each job (materialized view) will have a separated consumer group and
    ///   contains a generated suffix in the group id.
    ///   The consumer group will be `{group_id_prefix}-{fragment_id}`.
    /// - The consumer group is solely for monintoring progress in some external
    ///   Kafka tools, and for authorization. RisingWave does not rely on committed
    ///   offsets, and does not join the consumer group. It just reports offsets
    ///   to the group.
    #[serde(rename = "group.id.prefix")]
    pub group_id_prefix: Option<String>,

    /// This parameter is used to tell `KafkaSplitReader` to produce `UpsertMessage`s, which
    /// combine both key and value fields of the Kafka message.
    /// TODO: Currently, `Option<bool>` can not be parsed here.
    #[serde(rename = "upsert")]
    pub upsert: Option<String>,

    #[serde(flatten)]
    pub common: KafkaCommon,

    #[serde(flatten)]
    pub connection: KafkaConnectionProps,

    #[serde(flatten)]
    pub rdkafka_properties_common: RdKafkaPropertiesCommon,

    #[serde(flatten)]
    pub rdkafka_properties_consumer: RdKafkaPropertiesConsumer,

    #[serde(flatten)]
    pub privatelink_common: KafkaPrivateLinkCommon,

    #[serde(flatten)]
    pub aws_auth_props: AwsAuthProps,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

impl SourceProperties for KafkaProperties {
    type Split = KafkaSplit;
    type SplitEnumerator = KafkaSplitEnumerator;
    type SplitReader = KafkaSplitReader;

    const SOURCE_NAME: &'static str = KAFKA_CONNECTOR;
}

impl crate::source::UnknownFields for KafkaProperties {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl KafkaProperties {
    pub fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        self.rdkafka_properties_common.set_client(c);
        self.rdkafka_properties_consumer.set_client(c);
    }

    pub fn group_id(&self, fragment_id: u32) -> String {
        format!(
            "{}-{}",
            self.group_id_prefix.as_deref().unwrap_or("rw-consumer"),
            fragment_id
        )
    }
}

const KAFKA_ISOLATION_LEVEL: &str = "read_committed";

impl RdKafkaPropertiesConsumer {
    pub fn set_client(&self, c: &mut rdkafka::ClientConfig) {
        if let Some(v) = &self.queued_min_messages {
            c.set("queued.min.messages", v.to_string());
        }
        if let Some(v) = &self.queued_max_messages_kbytes {
            c.set("queued.max.messages.kbytes", v.to_string());
        }
        if let Some(v) = &self.fetch_wait_max_ms {
            c.set("fetch.wait.max.ms", v.to_string());
        }
        if let Some(v) = &self.fetch_queue_backoff_ms {
            c.set("fetch.queue.backoff.ms", v.to_string());
        }
        if let Some(v) = &self.fetch_max_bytes {
            c.set("fetch.max.bytes", v.to_string());
        }
        if let Some(v) = &self.enable_auto_commit {
            c.set("enable.auto.commit", v.to_string());
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use maplit::btreemap;

    use super::*;

    #[test]
    fn test_parse_config_consumer_common() {
        let config: BTreeMap<String, String> = btreemap! {
            // common
            "properties.bootstrap.server".to_owned() => "127.0.0.1:9092".to_owned(),
            "topic".to_owned() => "test".to_owned(),
            // kafka props
            "scan.startup.mode".to_owned() => "earliest".to_owned(),
            // RdKafkaPropertiesCommon
            "properties.message.max.bytes".to_owned() => "12345".to_owned(),
            "properties.receive.message.max.bytes".to_owned() => "54321".to_owned(),
            // RdKafkaPropertiesConsumer
            "properties.queued.min.messages".to_owned() => "114514".to_owned(),
            "properties.queued.max.messages.kbytes".to_owned() => "114514".to_owned(),
            "properties.fetch.wait.max.ms".to_owned() => "114514".to_owned(),
            "properties.fetch.max.bytes".to_owned() => "114514".to_owned(),
            "properties.enable.auto.commit".to_owned() => "true".to_owned(),
            "properties.fetch.queue.backoff.ms".to_owned() => "114514".to_owned(),
            // PrivateLink
            "broker.rewrite.endpoints".to_owned() => "{\"broker1\": \"10.0.0.1:8001\"}".to_owned(),
        };

        let props: KafkaProperties =
            serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();

        assert_eq!(props.scan_startup_mode, Some("earliest".to_owned()));
        assert_eq!(
            props.rdkafka_properties_common.receive_message_max_bytes,
            Some(54321)
        );
        assert_eq!(
            props.rdkafka_properties_common.message_max_bytes,
            Some(12345)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.queued_min_messages,
            Some(114514)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.queued_max_messages_kbytes,
            Some(114514)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.fetch_wait_max_ms,
            Some(114514)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.fetch_max_bytes,
            Some(114514)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.enable_auto_commit,
            Some(true)
        );
        assert_eq!(
            props.rdkafka_properties_consumer.fetch_queue_backoff_ms,
            Some(114514)
        );
        let hashmap: BTreeMap<String, String> = btreemap! {
            "broker1".to_owned() => "10.0.0.1:8001".to_owned()
        };
        assert_eq!(props.privatelink_common.broker_rewrite_map, Some(hashmap));
    }
}
