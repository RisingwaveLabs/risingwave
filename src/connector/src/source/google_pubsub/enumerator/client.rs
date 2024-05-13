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

use anyhow::Context;
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscription::{SeekTo, SubscriptionConfig};
use risingwave_common::bail;

use crate::error::ConnectorResult;
use crate::source::base::SplitEnumerator;
use crate::source::google_pubsub::split::PubsubSplit;
use crate::source::google_pubsub::PubsubProperties;
use crate::source::SourceEnumeratorContextRef;

pub struct PubsubSplitEnumerator {
    subscription: String,
    split_count: u32,
}

#[async_trait]
impl SplitEnumerator for PubsubSplitEnumerator {
    type Properties = PubsubProperties;
    type Split = PubsubSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<PubsubSplitEnumerator> {
        let split_count = match &properties.parallelism {
            Some(parallelism) => {
                let parallelism = parallelism
                    .parse::<u32>()
                    .context("error when parsing parallelism")?;
                if parallelism < 1 {
                    bail!("parallelism must be >= 1");
                }
                parallelism
            }
            None => 1,
        };

        if properties.credentials.is_none() && properties.emulator_host.is_none() {
            bail!("credentials must be set if not using the pubsub emulator")
        }

        let sub = properties.subscription_client().await?;
        if !sub
            .exists(None)
            .await
            .context("error checking subscription validity")?
        {
            bail!("subscription {} does not exist", &sub.id())
        }

        // We need the `retain_acked_messages` configuration to be true to seek back to timestamps
        // as done in the [`PubsubSplitReader`] and here.
        let (_, subscription_config) = sub
            .config(None)
            .await
            .context("failed to fetch subscription config")?;
        if let SubscriptionConfig {
            retain_acked_messages: false,
            ..
        } = subscription_config
        {
            bail!("subscription must be configured with retain_acked_messages set to true")
        }

        let seek_to = match (properties.start_offset, properties.start_snapshot) {
            (None, None) => None,
            (Some(start_offset), None) => {
                let ts = start_offset
                    .parse::<i64>()
                    .context("error parsing start_offset")
                    .map(|nanos| Utc.timestamp_nanos(nanos).into())?;
                Some(SeekTo::Timestamp(ts))
            }
            (None, Some(snapshot)) => Some(SeekTo::Snapshot(snapshot)),
            (Some(_), Some(_)) => {
                bail!("specify at most one of start_offset or start_snapshot")
            }
        };

        if let Some(seek_to) = seek_to {
            sub.seek(seek_to, None)
                .await
                .context("error seeking subscription")?;
        }

        Ok(Self {
            subscription: properties.subscription.to_owned(),
            split_count,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<PubsubSplit>> {
        tracing::debug!("enumerating pubsub splits");
        let splits: Vec<PubsubSplit> = (0..self.split_count)
            .map(|i| PubsubSplit {
                index: i,
                subscription: self.subscription.to_owned(),
                start_offset: None,
                __deprecated_stop_offset: None,
            })
            .collect();

        Ok(splits)
    }
}
