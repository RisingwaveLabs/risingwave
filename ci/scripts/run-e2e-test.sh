#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

while getopts 'p:m:' opt; do
    case ${opt} in
        p )
            profile=$OPTARG
            ;;
        m )
            mode=$OPTARG
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

if [[ $mode == "standalone" ]]; then
  source ci/scripts/standalone-utils.sh
fi

if [[ $mode == "single-node" ]]; then
  source ci/scripts/single-node-utils.sh
fi

cluster_start() {
  if [[ $mode == "standalone" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    risedev dev standalone-minio-sqlite &
    PID=$!
    sleep 1
    start_standalone "$PREFIX_LOG"/standalone.log &
    wait $PID
  elif [[ $mode == "single-node" ]]; then
    mkdir -p "$PREFIX_LOG"
    risedev clean-data
    risedev pre-start-dev
    start_single_node "$PREFIX_LOG"/single-node.log &
    # Give it a while to make sure the single-node is ready.
    sleep 10
  else
    risedev ci-start "$mode"
  fi
}

cluster_stop() {
  if [[ $mode == "standalone" ]]
  then
    stop_standalone
    # Don't check standalone logs, they will exceed the limit.
    risedev kill
  elif [[ $mode == "single-node" ]]
  then
    stop_single_node
  else
    risedev ci-kill
  fi
}

download_and_prepare_rw "$profile" common

echo "--- Download artifacts"
download-and-decompress-artifact e2e_test_generated ./
download-and-decompress-artifact risingwave_e2e_extended_mode_test-"$profile" target/debug/
mkdir -p e2e_test/udf/wasm/target/wasm32-wasi/release/
buildkite-agent artifact download udf.wasm e2e_test/udf/wasm/target/wasm32-wasi/release/
buildkite-agent artifact download udf.jar ./
mv target/debug/risingwave_e2e_extended_mode_test-"$profile" target/debug/risingwave_e2e_extended_mode_test

chmod +x ./target/debug/risingwave_e2e_extended_mode_test

echo "--- e2e, $mode, streaming"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_stream::common::table::state_table=warn" \
cluster_start

exit 88

# Please make sure the regression is expected before increasing the timeout.
sqllogictest -p 4566 -d dev './e2e_test/streaming/**/*.slt' --junit "streaming-${profile}"
sqllogictest -p 4566 -d dev './e2e_test/backfill/sink/different_pk_and_dist_key.slt'

echo "--- Kill cluster"
exit 88
cluster_stop

echo "--- e2e, $mode, batch"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev './e2e_test/ddl/**/*.slt' --junit "batch-ddl-${profile}" --label "can-use-recover"
sqllogictest -p 4566 -d dev './e2e_test/background_ddl/basic.slt' --junit "batch-ddl-${profile}"

if [[ $mode != "single-node" ]]; then
  sqllogictest -p 4566 -d dev './e2e_test/visibility_mode/*.slt' --junit "batch-${profile}"
fi

sqllogictest -p 4566 -d dev './e2e_test/ttl/ttl.slt'
sqllogictest -p 4566 -d dev './e2e_test/dml/*.slt'
sqllogictest -p 4566 -d dev './e2e_test/database/prepare.slt'
sqllogictest -p 4566 -d test './e2e_test/database/test.slt'

echo "--- e2e, $mode, python_client"
python3 -m pip install --break-system-packages psycopg
python3 ./e2e_test/python_client/main.py

echo "--- e2e, $mode, subscription"
python3 -m pip install --break-system-packages psycopg2-binary
sqllogictest -p 4566 -d dev './e2e_test/subscription/check_sql_statement.slt'
python3 ./e2e_test/subscription/main.py

echo "--- e2e, $mode, Apache Superset"
sqllogictest -p 4566 -d dev './e2e_test/superset/*.slt' --junit "batch-${profile}"

echo "--- e2e, $mode, external python udf"
python3 -m pip install --break-system-packages arrow-udf==0.2.1
python3 e2e_test/udf/test.py &
sleep 1
sqllogictest -p 4566 -d dev './e2e_test/udf/external_udf.slt'
pkill python3

sqllogictest -p 4566 -d dev './e2e_test/udf/alter_function.slt'
sqllogictest -p 4566 -d dev './e2e_test/udf/graceful_shutdown_python.slt'
# FIXME: flaky test
# sqllogictest -p 4566 -d dev './e2e_test/udf/retry_python.slt'

echo "--- e2e, $mode, external java udf"
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar udf.jar &
sleep 1
sqllogictest -p 4566 -d dev './e2e_test/udf/external_udf.slt'
pkill java

echo "--- e2e, $mode, embedded udf"
sqllogictest -p 4566 -d dev './e2e_test/udf/wasm_udf.slt'
sqllogictest -p 4566 -d dev './e2e_test/udf/rust_udf.slt'
sqllogictest -p 4566 -d dev './e2e_test/udf/js_udf.slt'
sqllogictest -p 4566 -d dev './e2e_test/udf/python_udf.slt'

echo "--- Kill cluster"
cluster_stop

echo "--- e2e, $mode, generated"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev './e2e_test/generated/**/*.slt' --junit "generated-${profile}"

echo "--- Kill cluster"
cluster_stop

# only run if mode is not single-node or standalone
if [[ "$mode" != "single-node" && "$mode" != "standalone" ]]; then
  echo "--- e2e, ci-3cn-1fe-with-recovery, error ui"
  RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
  risedev ci-start ci-3cn-1fe-with-recovery
  sqllogictest -p 4566 -d dev './e2e_test/error_ui/simple/**/*.slt'
  sqllogictest -p 4566 -d dev -e postgres-extended './e2e_test/error_ui/extended/**/*.slt'

  echo "--- Kill cluster"
  risedev ci-kill
fi

echo "--- e2e, $mode, extended query"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
cluster_start
sqllogictest -p 4566 -d dev -e postgres-extended './e2e_test/extended_mode/**/*.slt'
RUST_BACKTRACE=1 target/debug/risingwave_e2e_extended_mode_test --host 127.0.0.1 \
  -p 4566 \
  -u root

echo "--- Kill cluster"
cluster_stop

if [[ "$RUN_COMPACTION" -eq "1" ]]; then
    echo "--- e2e, ci-compaction-test, nexmark_q7"
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    risedev ci-start ci-compaction-test
    # Please make sure the regression is expected before increasing the timeout.
    sqllogictest -p 4566 -d dev './e2e_test/compaction/ingest_rows.slt'

    # We should ingest about 100 version deltas before the test
    echo "--- Wait for data ingestion"

    export RW_HUMMOCK_URL="hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001"
    export RW_META_ADDR="http://127.0.0.1:5690"

    # Poll the current version id until we have around 100 version deltas
    delta_log_cnt=0
    while [ "$delta_log_cnt" -le 90 ]
    do
        delta_log_cnt="$(./target/debug/risingwave risectl hummock list-version --verbose | grep -w '^ *id:' | grep -o '[0-9]\+' | head -n 1)"
        echo "Current version $delta_log_cnt"
        sleep 5
    done

    echo "--- Pause source and disable commit new epochs"
    ./target/debug/risingwave risectl meta pause
    ./target/debug/risingwave risectl hummock disable-commit-epoch

    echo "--- Start to run compaction test"
    download-and-decompress-artifact compaction-test-"$profile" target/debug/
    mv target/debug/compaction-test-"$profile" target/debug/compaction-test
    chmod +x ./target/debug/compaction-test
    # Use the config of ci-compaction-test for replay.
    config_path=".risingwave/config/risingwave.toml"
    RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
    ./target/debug/compaction-test --ci-mode --state-store hummock+minio://hummockadmin:hummockadmin@127.0.0.1:9301/hummock001 --config-path "${config_path}"

    echo "--- Kill cluster"
    cluster_stop
fi

if [[ "$mode" == "standalone" ]]; then
  run_sql() {
    psql -h localhost -p 4566 -d dev -U root -c "$@"
  }
  compactor_is_online() {
    set +e
    grep -q "risingwave_cmd_all::standalone: starting compactor-node thread" "${PREFIX_LOG}/standalone.log"
    local EXIT_CODE=$?
    set -e
    return $EXIT_CODE
  }

  echo "--- e2e, standalone, cluster-persistence-test"
  cluster_start
  run_sql "CREATE TABLE t (v1 int);
  INSERT INTO t VALUES (1);
  INSERT INTO t VALUES (2);
  INSERT INTO t VALUES (3);
  INSERT INTO t VALUES (4);
  INSERT INTO t VALUES (5);
  flush;"

  EXPECTED=$(run_sql "SELECT * FROM t ORDER BY v1;")
  echo -e "Expected:\n$EXPECTED"

  echo "Restarting standalone"
  restart_standalone

  ACTUAL=$(run_sql "SELECT * FROM t ORDER BY v1;")
  echo -e "Actual:\n$ACTUAL"

  if [[ "$EXPECTED" != "$ACTUAL" ]]; then
    echo "ERROR: Expected did not match Actual."
    exit 1
  else
    echo "PASSED"
  fi

  echo "--- Kill cluster"
  cluster_stop

  wait

  # Test that we can optionally include nodes in standalone mode.
  echo "--- e2e, standalone, cluster-opts-test"

  echo "test standalone without compactor"
  mkdir -p "$PREFIX_LOG"
  risedev clean-data
  risedev pre-start-dev
  risedev dev standalone-minio-sqlite-compactor &
  PID=$!
  sleep 1
  start_standalone_without_compactor "$PREFIX_LOG"/standalone.log &
  wait $PID
  wait_standalone
  if compactor_is_online
  then
    echo "ERROR: Compactor should not be online."
    exit 1
  fi
  cluster_stop
  echo "test standalone without compactor [TEST PASSED]"

  wait

  echo "test standalone with compactor"
  mkdir -p "$PREFIX_LOG"
  risedev clean-data
  risedev pre-start-dev
  risedev dev standalone-minio-sqlite &
  PID=$!
  sleep 1
  start_standalone "$PREFIX_LOG"/standalone.log &
  wait $PID
  wait_standalone
  if ! compactor_is_online
  then
    echo "ERROR: Compactor should be online."
    exit 1
  fi
  cluster_stop
  echo "test standalone with compactor [TEST PASSED]"

  # Make sure any remaining background task exits.
  wait
fi

echo "--- Upload JUnit test results"
buildkite-agent artifact upload "*-junit.xml"
