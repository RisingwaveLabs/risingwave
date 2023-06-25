#!/usr/bin/env sh
#!/usr/bin/env bash

# Exits as soon as any line fails.
# set -euo pipefailp

echo "--- check elasticsearch"
curl http://elasticsearch:9200

echo "--- testing sink"
sqllogictest -p 4566 -d dev './e2e_test/sink/elasticsearch/elasticsearch_sink.slt'

sleep 5

echo "testing sink result"
curl -XGET "http://elasticsearch:9200/test/_search" -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}' > ./e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
python3 e2e_test/sink/elasticsearch/elasticsearch.py e2e_test/sink/elasticsearch/elasticsearch_sink.result e2e_test/sink/elasticsearch/elasticsearch_sink.tmp.result
if [ $? -ne 0 ]; then
  echo "The output is not as expected."
  exit 1
fi
