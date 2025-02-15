#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

while getopts 'p:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        \? )
            echo "Invalid Option: -$OPTARG" 1>&2
            exit 1
            ;;
        : )
            echo "Invalid option: $OPTARG requires an argument" 1>&2
            ;;
    esac
done
shift $((OPTIND -1))

download_and_prepare_rw "$profile" source

echo "--- starting risingwave cluster"
risedev ci-start ci-sink-test
apt-get update -y && apt-get install -y redis-server
sleep 1

echo "--- testing sinks"
sqllogictest -p 4566 -d dev './e2e_test/sink/redis_sink.slt'
sleep 1

redis-cli -h redis-server -p 6379 get {\"v1\":1} >> ./query_result.txt
redis-cli -h redis-server -p 6379 get V1:1 >> ./query_result.txt
redis-cli -h redis-server -p 6379 GEOPOS key_is:1_1 test >> ./query_result2.txt

# check sink destination using shell
if cat ./query_result.txt | tr '\n' '\0' | xargs -0 -n1 bash -c '[[ "$0" == "{\"v1\":1,\"v2\":1,\"v3\":1,\"v4\":1.100000023841858,\"v5\":1.2,\"v6\":\"test\",\"v7\":734869,\"v8\":\"2013-01-01T01:01:01.000000Z\",\"v9\":false}" || "$0" == "V2:1,V3:1" ]]'; then
    echo "Redis sink check passed"
else
    cat ./query_result.txt
  echo "The output is not as expected."
  exit 1
fi

LINE_COUNT=$(cat ./query_result2.txt | wc -l);
if [ "$LINE_COUNT" -eq 2 ]; then
    echo "Redis sink check passed"
else
    cat ./query_result2.txt
    echo "The output is not as expected."
    exit 1
fi

echo "--- testing cluster sinks"
redis-server ./ci/redis-conf/redis-7000.conf --daemonize yes
redis-server ./ci/redis-conf/redis-7001.conf --daemonize yes
redis-server ./ci/redis-conf/redis-7002.conf --daemonize yes

echo "yes" | redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002

sqllogictest -p 4566 -d dev './e2e_test/sink/redis_cluster_sink.slt'

redis-cli -c --cluster call 127.0.0.1:7000 keys \* >> ./query_result_1.txt

line_count=$(wc -l < query_result_1.txt)
if [ "$line_count" -eq 16 ]; then
    echo "Redis sink check passed"
else
    cat ./query_result_1.txt
    echo "The output is not as expected."
    exit 1
fi

echo "--- Kill cluster"
risedev ci-kill