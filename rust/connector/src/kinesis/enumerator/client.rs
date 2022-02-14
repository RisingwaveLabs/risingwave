use anyhow::Result;
use async_trait::async_trait;
use aws_sdk_kinesis::model::Shard;
use aws_sdk_kinesis::Client as kinesis_client;

use crate::base::SplitEnumerator;
use crate::kinesis::split::{KinesisOffset, KinesisSplit};

pub struct KinesisSplitEnumerator {
    stream_name: String,
    client: kinesis_client,
}

#[async_trait]
impl SplitEnumerator for KinesisSplitEnumerator {
    type Split = KinesisSplit;

    async fn list_splits(&mut self) -> Result<Vec<Self::Split>> {
        let mut next_token: Option<String> = None;
        let mut shard_collect: Vec<Shard> = Vec::new();

        loop {
            let list_shard_output = self
                .client
                .list_shards()
                .set_next_token(next_token)
                .stream_name(&self.stream_name)
                .send()
                .await?;
            match list_shard_output.shards {
                Some(shard) => shard_collect.extend(shard),
                None => {
                    return Err(anyhow::Error::msg(format!(
                        "no shards in stream {}",
                        &self.stream_name
                    )));
                }
            }

            match list_shard_output.next_token {
                Some(token) => next_token = Some(token),
                None => break,
            }
        }
        Ok(shard_collect
            .into_iter()
            .map(|x| KinesisSplit {
                shard_id: x.shard_id().unwrap_or_default().to_string(),
                start_position: KinesisOffset::None,
                end_position: KinesisOffset::None,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_kinesis::Region;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_kinesis_split_enumerator() -> Result<()> {
        let stream_name = "kinesis_test_stream".to_string();
        let config = aws_config::from_env()
            .region(Region::new("cn-north-1"))
            .load()
            .await;
        let client = aws_sdk_kinesis::Client::new(&config);
        let mut enumerator = KinesisSplitEnumerator {
            stream_name,
            client,
        };
        let list_splits_resp = enumerator.list_splits().await?;
        // println!("{:#?}", list_splits_resp);
        assert_eq!(list_splits_resp.len(), 4);
        Ok(())
    }
}
