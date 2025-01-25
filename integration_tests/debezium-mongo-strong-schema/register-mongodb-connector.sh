#!/bin/sh

set -e

echo 'Waiting for Kafka Connect to start...'
while ! curl -s http://debezium:8083/ | grep -q 'version'; do
	sleep 5
done

echo 'Kafka Connect started.'
echo 'Registering MongoDB connector...'

# Add these two fields if the mongodb is authenticated:
#    "mongodb.user": "admin",
#    "mongodb.password": "admin123",
#
response=$(curl -s -X POST -H 'Content-Type: application/json' --data '{
  "name": "mongodb-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "tasks.max": "1",
    "mongodb.hosts": "mongodb:27017",
    "mongodb.name": "dbserver1",
    "database.history.kafka.bootstrap.servers": "message_queue:29092"
  }
}' http://debezium:8083/connectors)

if echo "$response" | grep -q '"error_code"'; then
	echo 'Failed to register MongoDB connector: ' "$response"
	exit 1
else
	echo 'MongoDB connector registered.'
fi
