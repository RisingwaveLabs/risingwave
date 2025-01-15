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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::swap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{
    KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext, KAFKA_ISOLATION_LEVEL,
};
use crate::source::{
    into_chunk_stream, BackfillInfo, BoxSourceChunkStream, Column, SourceContextRef, SplitId,
    SplitImpl, SplitMetaData, SplitReader,
};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<RwConsumerContext>,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
    backfill_info: HashMap<SplitId, BackfillInfo>,
    splits: Vec<KafkaSplit>,
    sync_call_timeout: Duration,
    bytes_per_second: usize,
    max_num_messages: usize,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        properties: KafkaProperties,
        splits: Vec<KafkaSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.connection.brokers;
        let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);

        config.set("group.id", properties.group_id(source_ctx.fragment_id));

        let ctx_common = KafkaContextCommon::new(
            broker_rewrite_map,
            Some(format!(
                "fragment-{}-source-{}-actor-{}",
                source_ctx.fragment_id, source_ctx.source_id, source_ctx.actor_id
            )),
            // thread consumer will keep polling in the background, we don't need to call `poll`
            // explicitly
            Some(source_ctx.metrics.rdkafka_native_metric.clone()),
            properties.aws_auth_props,
            properties.connection.is_aws_msk_iam(),
        )
        .await?;

        let client_ctx = RwConsumerContext::new(ctx_common);
        let consumer: StreamConsumer<RwConsumerContext> = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(client_ctx)
            .await
            .context("failed to create kafka consumer")?;

        let mut tpl = TopicPartitionList::with_capacity(splits.len());

        let mut offsets = HashMap::new();
        let mut backfill_info = HashMap::new();
        for split in splits.clone() {
            offsets.insert(split.id(), (split.start_offset, split.stop_offset));

            if let Some(offset) = split.start_offset {
                tpl.add_partition_offset(
                    split.topic.as_str(),
                    split.partition,
                    Offset::Offset(offset + 1),
                )?;
            } else {
                tpl.add_partition(split.topic.as_str(), split.partition);
            }

            let (low, high) = consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    properties.common.sync_call_timeout,
                )
                .await?;
            tracing::debug!("fetch kafka watermarks: low: {low}, high: {high}, split: {split:?}");
            // note: low is inclusive, high is exclusive
            if low == high {
                backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
            } else {
                debug_assert!(high > 0);
                backfill_info.insert(
                    split.id(),
                    BackfillInfo::HasDataToBackfill {
                        latest_offset: (high - 1).to_string(),
                    },
                );
            }
        }
        tracing::debug!("backfill_info: {:?}", backfill_info);

        consumer.assign(&tpl)?;

        // The two parameters below are only used by developers for performance testing purposes,
        // so we panic here on purpose if the input is not correctly recognized.
        let bytes_per_second = match properties.bytes_per_second {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("bytes.per.second expect usize"),
        };
        let max_num_messages = match properties.max_num_messages {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("max.num.messages expect usize"),
        };

        Ok(Self {
            consumer,
            offsets,
            splits,
            backfill_info,
            bytes_per_second,
            sync_call_timeout: properties.common.sync_call_timeout,
            max_num_messages,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        self.backfill_info.clone()
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        let mut latest_splits: Vec<SplitImpl> = Vec::new();
        let mut tpl = TopicPartitionList::with_capacity(self.splits.len());
        for mut split in self.splits.clone() {
            // we can't get latest offset if we use Offset::End, so we just fetch watermark here.
            let (_low, high) = self
                .consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    self.sync_call_timeout,
                )
                .await?;
            tpl.add_partition_offset(split.topic.as_str(), split.partition, Offset::Offset(high))?;
            split.start_offset = Some(high - 1);
            latest_splits.push(split.into());
        }
        // replace the previous assignment
        self.consumer.assign(&tpl)?;
        Ok(latest_splits)
    }
}

impl KafkaSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        if self.offsets.values().all(|(start_offset, stop_offset)| {
            match (start_offset, stop_offset) {
                (Some(start), Some(stop)) if (*start + 1) >= *stop => true,
                (_, Some(stop)) if *stop == 0 => true,
                _ => false,
            }
        }) {
            yield Vec::new();
            return Ok(());
        };

        let mut stop_offsets: HashMap<_, _> = self
            .offsets
            .iter()
            .flat_map(|(split_id, (_, stop_offset))| {
                stop_offset.map(|offset| (split_id.clone() as SplitId, offset))
            })
            .collect();

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        let mut bytes_current_second = 0;
        let mut num_messages = 0;
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        let mut res = Vec::with_capacity(max_chunk_size);
        // ingest kafka message header can be expensive, do it only when required
        let require_message_header = self.parser_config.common.rw_columns.iter().any(|col_desc| {
            matches!(
                col_desc.additional_column.column_type,
                Some(AdditionalColumnType::Headers(_) | AdditionalColumnType::HeaderInner(_))
            )
        });

        let mut latest_message_id_metrics: HashMap<String, LabelGuardedIntGauge<3>> =
            HashMap::new();

        #[for_await]
        'for_outer_loop: for msgs in self.consumer.stream().ready_chunks(max_chunk_size) {
            let msgs: Vec<_> = msgs
                .into_iter()
                .collect::<std::result::Result<_, KafkaError>>()?;

            let mut split_msg_offsets = HashMap::new();

            for msg in &msgs {
                split_msg_offsets.insert(msg.partition(), msg.offset());
            }

            for (partition, offset) in split_msg_offsets {
                let split_id = partition.to_string();
                latest_message_id_metrics
                    .entry(split_id.clone())
                    .or_insert_with(|| {
                        self.source_ctx
                            .metrics
                            .latest_message_id
                            .with_guarded_label_values(&[
                                // source name is not available here
                                &self.source_ctx.source_id.to_string(),
                                &self.source_ctx.actor_id.to_string(),
                                &split_id,
                            ])
                    })
                    .set(offset);
            }

            for msg in msgs {
                let cur_offset = msg.offset();
                bytes_current_second += match &msg.payload() {
                    None => 0,
                    Some(payload) => payload.len(),
                };
                num_messages += 1;
                let source_message =
                    SourceMessage::from_kafka_message(&msg, require_message_header);
                let split_id = source_message.split_id.clone();
                res.push(source_message);

                if let Entry::Occupied(o) = stop_offsets.entry(split_id) {
                    let stop_offset = *o.get();

                    if cur_offset == stop_offset - 1 {
                        tracing::debug!(
                            "stop offset reached for split {}, stop reading, offset: {}, stop offset: {}",
                            o.key(),
                            cur_offset,
                            stop_offset
                        );

                        o.remove();

                        if stop_offsets.is_empty() {
                            yield res;
                            break 'for_outer_loop;
                        }

                        continue;
                    }
                }

                if bytes_current_second > self.bytes_per_second {
                    // swap to make compiler happy
                    let mut cur = Vec::with_capacity(res.capacity());
                    swap(&mut cur, &mut res);
                    yield cur;
                    interval.tick().await;
                    bytes_current_second = 0;
                    res.clear();
                }
                if num_messages >= self.max_num_messages {
                    yield res;
                    break 'for_outer_loop;
                }
            }
            let mut cur = Vec::with_capacity(res.capacity());
            swap(&mut cur, &mut res);
            yield cur;
            // don't clear `bytes_current_second` here as it is only related to `.tick()`.
            // yield in the outer loop so that we can always guarantee that some messages are read
            // every `MAX_CHUNK_SIZE`.
        }
    }
}
