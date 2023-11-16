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

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use urlencoding::encode;

const PERSISTENT_DOMAIN: &str = "persistent";
const NON_PERSISTENT_DOMAIN: &str = "non-persistent";
const PUBLIC_TENANT: &str = "public";
const DEFAULT_NAMESPACE: &str = "default";
const PARTITIONED_TOPIC_SUFFIX: &str = "-partition-";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Hash)]
/// `ParsedTopic` is a parsed topic name, Generated by `parse_topic`.
pub struct Topic {
    pub domain: String,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
    pub partition_index: Option<i32>,
}

impl ToString for Topic {
    fn to_string(&self) -> String {
        format!(
            "{}://{}/{}/{}",
            self.domain, self.tenant, self.namespace, self.topic
        )
    }
}

impl Topic {
    pub fn is_partitioned_topic(&self) -> bool {
        self.partition_index.is_none()
    }

    pub fn rest_path(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.domain,
            self.tenant,
            self.namespace,
            encode(&self.topic)
        )
    }

    pub fn sub_topic(&self, partition: i32) -> Result<Topic> {
        if partition < 0 {
            return Err(anyhow!("invalid partition index number"));
        }

        if self.topic.contains(PARTITIONED_TOPIC_SUFFIX) {
            return Ok(self.clone());
        }

        Ok(Topic {
            domain: self.domain.clone(),
            tenant: self.tenant.clone(),
            namespace: self.namespace.clone(),
            topic: format!("{}{}{}", self.topic, PARTITIONED_TOPIC_SUFFIX, partition),
            partition_index: Some(partition),
        })
    }

    pub fn topic_str_without_partition(&self) -> Result<String> {
        if self.topic.contains(PARTITIONED_TOPIC_SUFFIX) {
            let parts: Vec<&str> = self.topic.split(PARTITIONED_TOPIC_SUFFIX).collect();
            Ok(parts[0].to_string())
        } else {
            Ok(self.topic.clone())
        }
    }
}

/// `get_partition_index` returns the partition index of the topic.
pub fn get_partition_index(topic: &str) -> Result<Option<i32>> {
    if topic.contains(PARTITIONED_TOPIC_SUFFIX) {
        let partition = topic
            .split('-')
            .last()
            .unwrap()
            .parse::<i32>()
            .map_err(|e| anyhow!(e))?;

        Ok(Some(partition))
    } else {
        Ok(None)
    }
}

/// `parse_topic` parses a topic name into its components.
/// The short topic name can be:
/// - `<topic>`
/// - `<tenant>/<namespace>/<topic>`
/// The fully qualified topic name can be:
/// `<domain>://<tenant>/<namespace>/<topic>`
pub fn parse_topic(topic: &str) -> Result<Topic> {
    let mut complete_topic = topic.to_string();

    if !topic.contains("://") {
        let parts: Vec<&str> = topic.split('/').collect();
        complete_topic = match parts.len() {
            1 => format!(
                "{}://{}/{}/{}",
                PERSISTENT_DOMAIN, PUBLIC_TENANT, DEFAULT_NAMESPACE, parts[0],
            ),
            3 => format!("{}://{}", PERSISTENT_DOMAIN, topic),
            _ => {
                return Err(anyhow!(
                    "Invalid short topic name '{}', \
                it should be in the format of <tenant>/<namespace>/<topic> or <topic>",
                    topic
                ));
            }
        };
    }

    let parts: Vec<&str> = complete_topic.splitn(2, "://").collect();

    let domain = match parts[0] {
        PERSISTENT_DOMAIN | NON_PERSISTENT_DOMAIN => parts[0],
        _ => {
            return Err(anyhow!(
                "The domain only can be specified as 'persistent' or 'non-persistent'. Input domain is '{}'",
                parts[0]
            ));
        }
    };

    let rest = parts[1];
    let parts: Vec<&str> = rest.splitn(3, '/').collect();

    if parts.len() != 3 {
        return Err(anyhow!(
            "invalid topic name '{}', it should be in the format of <tenant>/<namespace>/<topic>",
            rest
        ));
    }

    let parsed_topic = Topic {
        domain: domain.to_string(),
        tenant: parts[0].to_string(),
        namespace: parts[1].to_string(),
        topic: parts[2].to_string(),
        partition_index: get_partition_index(complete_topic.as_str())?,
    };

    if parsed_topic.topic.is_empty() {
        return Err(anyhow!("topic name cannot be empty".to_string(),));
    }

    Ok(parsed_topic)
}

#[cfg(test)]
mod test {
    use crate::source::pulsar::topic::{get_partition_index, parse_topic};

    #[test]
    fn test_parse_topic() {
        assert_eq!(
            parse_topic("success").unwrap().to_string(),
            "persistent://public/default/success".to_string()
        );
        assert_eq!(
            parse_topic("tenant/namespace/success").unwrap().to_string(),
            "persistent://tenant/namespace/success".to_string()
        );
        assert_eq!(
            parse_topic("persistent://tenant/namespace/success")
                .unwrap()
                .to_string(),
            "persistent://tenant/namespace/success".to_string()
        );
        assert_eq!(
            parse_topic("non-persistent://tenant/namespace/success")
                .unwrap()
                .to_string(),
            "non-persistent://tenant/namespace/success".to_string()
        );

        assert_eq!(
            parse_topic("non-persistent://tenant/namespace/success")
                .unwrap()
                .partition_index,
            None
        );

        assert_eq!(
            parse_topic("non-persistent://tenant/namespace/success-partition-1")
                .unwrap()
                .partition_index,
            Some(1)
        );
        assert_eq!(
            parse_topic("non-persistent://tenant/namespace/success-partition-1-partition-2")
                .unwrap()
                .partition_index,
            Some(2)
        );
    }

    #[test]
    fn test_get_partition_index() {
        assert_eq!(get_partition_index("success").unwrap(), None);
        assert_eq!(get_partition_index("success-partition-1").unwrap(), Some(1));
        assert_eq!(
            get_partition_index("success-partition-1-partition-2").unwrap(),
            Some(2)
        );
    }
}
