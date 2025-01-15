set sink_decouple = false;

CREATE SINK bhv_es7_sink
FROM
    bhv_mv WITH (
    connector = 'elasticsearch',
    type = 'upsert',
    index = 'test',
    url = 'http://elasticsearch7:9200',
    username = 'elastic',
    password = 'risingwave'
);

CREATE SINK bhv_es8_sink
FROM
    bhv_mv WITH (
    connector = 'elasticsearch',
    type = 'upsert',
    index = 'test',
    url = 'http://elasticsearch8:9200',
    username = 'elastic',
    password = 'risingwave'
);

CREATE SINK es7_types_sink
FROM
    es_types WITH (
    connector = 'elasticsearch',
    type = 'upsert',
    index = 'test_types',
    primary_key = 'types_id',
    url = 'http://elasticsearch7:9200',
    username = 'elastic',
    password = 'risingwave'
);

CREATE SINK es8_types_sink
FROM
    es_types WITH (
    connector = 'elasticsearch',
    type = 'upsert',
    index = 'test_types',
    primary_key = 'types_id',
    url = 'http://elasticsearch8:9200',
    username = 'elastic',
    password = 'risingwave'
);
