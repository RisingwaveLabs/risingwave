from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import sys
import json
import os


def create_topic(kafka_conf, topic_name):
    client = AdminClient(kafka_conf)
    topic_list = []
    topic_list.append(
        NewTopic(topic_name, num_partitions=1, replication_factor=1))
    client.create_topics(topic_list)

def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.value(), err))
        return

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("datagen.py <brokerlist> <schema-registry-url> <file> <name-strategy>")
    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    file = sys.argv[3]
    topic = os.path.basename(file).split(".")[0]
    if sys.argv[4] not in ['topic', 'record', 'topic-record', '', None]:
        print("name strategy must be one of: topic, record, topic_record")
        exit(1)
    if sys.argv[4] != 'topic':
        topic = topic + "-" + sys.argv[4]

    print("broker_list: {}".format(broker_list))
    print("schema_registry_url: {}".format(schema_registry_url))
    print("topic: {}".format(topic))
    print("name strategy: {}".format(sys.argv[4] if sys.argv[4] is not None else 'topic'))

    schema_registry_conf = {'url': schema_registry_url}
    kafka_conf = {'bootstrap.servers': broker_list}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    create_topic(kafka_conf=kafka_conf, topic_name=topic)
    producer = Producer(kafka_conf)
    value_serializer = None
    with open(file) as file:
        for (i, line) in enumerate(file):
            if i == 0:
                value_serializer = JSONSerializer(schema_registry_client=schema_registry_client,
                                                  schema_str=line)
            else:
                producer.produce(topic=topic, partition=0,
                                 value=value_serializer(
                                 json.loads(line), SerializationContext(topic, MessageField.VALUE)),
                                 on_delivery=delivery_report)

    producer.flush()
