CREATE TABLE users (
    _id INT64 PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    address VARCHAR(255)
    ) WITH (
    connector = 'kafka',
    kafka.topic = 'dbserver1.random_data.users',
    kafka.brokers = 'message_queue:29092',
    kafka.scan.startup.mode = 'earliest',
    strong_schema = 'true'
) FORMAT DEBEZIUM_MONGO ENCODE JSON;
