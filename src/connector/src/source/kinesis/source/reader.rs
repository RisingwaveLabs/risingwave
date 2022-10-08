// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as KinesisClient;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use risingwave_common::util::select_all;

use crate::source::kinesis::source::message::KinesisMessage;
use crate::source::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::source::kinesis::{build_client, KinesisProperties};
use crate::source::{
    BoxSourceStream, Column, ConnectorState, SourceMessage, SplitId, SplitImpl, SplitReader,
};

pub struct KinesisMultiSplitReader {
    /// splits are not allowed to be empty, otherwise connector source should create
    /// DummySplitReader which is always idling.
    readers: Vec<KinesisSplitReader>,
}

#[derive(Debug, Clone)]
pub struct KinesisSplitReader {
    client: KinesisClient,
    stream_name: String,
    shard_id: SplitId,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    start_position: KinesisOffset,
    end_position: KinesisOffset,
}

impl KinesisSplitReader {
    pub async fn new(properties: KinesisProperties, split: KinesisSplit) -> Result<Self> {
        let stream_name = properties.stream_name.clone();
        let client = build_client(properties).await?;
        Ok(Self {
            client,
            stream_name,
            shard_id: split.shard_id,
            shard_iter: None,
            latest_offset: None,
            start_position: split.start_position,
            end_position: split.end_position,
        })
    }

    #[try_stream(boxed, ok = SourceMessage, error = anyhow::Error)]
    pub async fn into_stream(mut self) {
        self.new_shard_iter().await?;
        loop {
            match self.get_records().await {
                Ok(resp) => {
                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let records = resp.records().unwrap();
                    for record in records {
                        let msg = SourceMessage::from(KinesisMessage::new(
                            self.shard_id.clone(),
                            record.clone(),
                        ));
                        self.latest_offset = Some(msg.offset.clone());
                        yield msg;
                    }
                    if records.is_empty() {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                }
                Err(SdkError::ServiceError { err, .. }) if err.is_expired_iterator_exception() => {
                    self.new_shard_iter().await?;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(e) => return Err(anyhow!(e)),
            }
        }
    }

    async fn new_shard_iter(&mut self) -> Result<()> {
        let (starting_seq_num, iter_type) = if self.latest_offset.is_some() {
            (
                self.latest_offset.take(),
                ShardIteratorType::AfterSequenceNumber,
            )
        } else {
            match &self.start_position {
                KinesisOffset::Earliest => (None, ShardIteratorType::TrimHorizon),
                KinesisOffset::SequenceNumber(seq) => {
                    (Some(seq.clone()), ShardIteratorType::AfterSequenceNumber)
                }
                _ => unreachable!(),
            }
        };

        let resp = self
            .client
            .get_shard_iterator()
            .stream_name(self.stream_name.clone())
            .shard_id(self.shard_id.as_ref())
            .shard_iterator_type(iter_type)
            .set_starting_sequence_number(starting_seq_num)
            .send()
            .await?;

        self.shard_iter = resp.shard_iterator().map(String::from);

        Ok(())
    }

    async fn get_records(
        &mut self,
    ) -> core::result::Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        self.client
            .get_records()
            .set_shard_iterator(self.shard_iter.take())
            .send()
            .await
    }
}

#[async_trait]
impl SplitReader for KinesisMultiSplitReader {
    type Properties = KinesisProperties;

    async fn new(
        properties: KinesisProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let splits = state
            .unwrap()
            .into_iter()
            .map(|split| match split {
                SplitImpl::Kinesis(ks) => Ok(ks),
                _ => Err(anyhow!("expect KinesisSplit, got {:?}", split)),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            readers: splits
                .into_iter()
                .map(|split| KinesisSplitReader::new(properties.clone(), split))
                .collect::<FuturesUnordered<_>>()
                .try_collect::<Vec<_>>()
                .await?,
        })
    }

    fn into_stream(self) -> BoxSourceStream {
        self.into_stream()
    }
}

impl KinesisMultiSplitReader {
    #[try_stream(boxed, ok = SourceMessage, error = anyhow::Error)]
    async fn into_stream(self) {
        let join_stream = select_all(self.readers.into_iter().map(|split| split.into_stream()));
        #[for_await]
        for msg in join_stream {
            yield msg.inspect_err(|e| {
                tracing::error!("split encountered error: {e:?}, shutting down stream");
            })?;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter::Iterator;

    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_single_thread_kinesis_reader() -> Result<()> {
        let properties = KinesisProperties {
            assume_role_arn: None,
            credentials_access_key: None,
            credentials_secret_access_key: None,
            stream_name: "kinesis_debug".to_string(),
            stream_region: "cn-northwest-1".to_string(),
            endpoint: None,
            session_token: None,
            assume_role_external_id: None,
        };

        let mut trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            },
        )
        .await?
        .into_stream();
        println!("{:?}", trim_horizen_reader.next().await.unwrap()?);

        let mut offset_reader = KinesisSplitReader::new(
            properties.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::SequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_position: KinesisOffset::None,
            },
        )
        .await?
        .into_stream();
        println!("{:?}", offset_reader.next().await.unwrap()?);

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_multi_splits() -> Result<()> {
        let properties = KinesisProperties {
            assume_role_arn: None,
            credentials_access_key: None,
            credentials_secret_access_key: None,
            stream_name: "kinesis_debug".to_string(),
            stream_region: "cn-northwest-1".to_string(),
            endpoint: None,
            session_token: None,
            assume_role_external_id: None,
        };

        let splits = vec!["shardId-000000000000", "shardId-000000000001"]
            .iter()
            .map(|split| {
                SplitImpl::Kinesis(KinesisSplit {
                    shard_id: split.to_string().into(),
                    start_position: KinesisOffset::Earliest,
                    end_position: KinesisOffset::None,
                })
            })
            .collect::<Vec<_>>();

        let mut reader = KinesisMultiSplitReader::new(properties, Some(splits), None)
            .await?
            .into_stream();
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("1: {:?}", reader.next().await.unwrap());
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("2: {:?}", reader.next().await.unwrap());
        Ok(())
    }
}
