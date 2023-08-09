use std::time::Duration;

use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, ClientContext, Message, TopicPartitionList};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _rebalance: &Rebalance<'_>) {
        // println!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, _rebalance: &Rebalance<'_>) {
        // println!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, _result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        // println!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

pub struct KafkaSink {
    client_config: FutureProducer,
    output_topic: String,
}

impl KafkaSink {
    pub(crate) fn new(brokers: String, output_topic: String) -> KafkaSink {
        KafkaSink {
            client_config: ClientConfig::new()
                .set("group.id", "recwave-recommender")
                .set("bootstrap.servers", brokers.clone())
                .set("queue.buffering.max.ms", "0") // Do not buffer
                .create()
                .expect("Producer creation failed"),
            output_topic,
        }
    }

    pub async fn send(&self, record_id: String, payload: String) {
        let output_topic = self.output_topic.clone();
        let record = FutureRecord::to(&*output_topic)
            .payload(payload.as_bytes())
            .key(record_id.as_bytes());
        self.client_config
            .send(record, Duration::from_secs(1))
            .await
            .expect("Failed to create send message request");
        println!("Sent payload {}", payload);
    }

    pub async fn mock_consume() {
        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", "recwave-recommender")
            .set("bootstrap.servers", "localhost:9092")
            .create_with_context(CustomContext)
            .expect("Failed to create consumer");
        consumer
            .subscribe(&vec!["recwave"])
            .expect("Failed to subscribe");
        println!("Ready to consume");

        loop {
            match consumer
                .recv()
                .await
                .expect("Failed to poll")
                .payload_view::<str>()
            {
                Some(Ok(payload)) => {
                    println!("Received message: {}", payload);
                }
                Some(Err(e)) => {
                    println!("Failed to decode message: {}", e);
                }
                None => {
                    println!("Received empty message");
                }
            }
        }
    }
}
