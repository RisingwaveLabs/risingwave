#!/usr/bin/env bash

# NOTE(kwannoel):
# Do not run this script directly, it is meant to be sourced.
# Backwards compatibility tests consist of the following parts:
#
# 1. Setup old cluster binaries.
# 2. Seed old cluster.
# 3. Setup new cluster binaries.
# 4. Run validation on new cluster.
#
# Steps 1,3 are specific to the execution environment, CI / Local.
# This script only provides utilities for 2, 4.

################################### ENVIRONMENT CONFIG

# Duration to wait for recovery (seconds)
RECOVERY_DURATION=20

# Setup test directory
TEST_DIR=.risingwave/backwards-compat-tests/
KAFKA_PATH=.risingwave/bin/kafka
mkdir -p $TEST_DIR
cp -r backwards-compat-tests/slt/* $TEST_DIR

wait_kafka_exit() {
  # Follow kafka-server-stop.sh
  while [[ -n "$(ps ax | grep ' kafka\.Kafka ' | grep java | grep -v grep | awk '{print $1}')" ]]; do
    echo "Waiting for kafka to exit"
    sleep 1
  done
}

wait_zookeeper_exit() {
  # Follow zookeeper-server-stop.sh
  while [[ -n "$(ps ax | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}')" ]]; do
    echo "Waiting for zookeeper to exit"
    sleep 1
  done
}

kill_kafka() {
  $KAFKA_PATH/bin/kafka-server-stop.sh
  wait_kafka_exit
}

kill_zookeeper() {
  $KAFKA_PATH/bin/zookeeper-server-stop.sh
  wait_zookeeper_exit
}

wait_for_process() {
    process_name="$1"

    while pgrep -x "$process_name" > /dev/null; do
        echo "Process $process_name is still running... Wait for 1 sec"
        sleep 1
    done
}

wait_all_process_exit() {
  wait_for_process meta-node
  wait_for_process compute-node
  wait_for_process frontend
  wait_for_process compactor
  echo "All processes has exited."
}

# Older versions of RW may not gracefully kill kafka.
# So we duplicate the definition here.
kill_cluster() {
  # Kill other components
  echo "kill risingwave nodes"
  tmux list-windows -t risedev -F "#{window_name} #{pane_id}" \
  | grep -v 'kafka' \
  | grep -v 'zookeeper' \
  | awk '{ print $2 }' \
  | xargs -I {} tmux send-keys -t {} C-c C-d

  set +e
  if [[ -n $(tmux list-windows -t risedev | grep kafka) ]];
  then
    echo "kill kafka"
    kill_kafka

    echo "kill zookeeper"
    kill_zookeeper

    # Kill their tmux sessions
    tmux list-windows -t risedev -F "#{pane_id}" | xargs -I {} tmux send-keys -t {} C-c C-d
  fi
  set -e

  echo "kill risedev"
  tmux kill-session -t risedev
  test $? -eq 0 || { echo "Failed to stop all RiseDev components."; exit 1; }
  wait_all_process_exit
}

run_sql () {
  psql -h localhost -p 4566 -d dev -U root -c "$@"
}

check_version() {
  local VERSION=$1
  local raw_version=$(run_sql "SELECT version();")
  echo "--- Version"
  echo "$raw_version"
  local version=$(echo $raw_version | grep -i risingwave | sed 's/^.*risingwave-\([0-9]*\.[0-9]*\.[0-9]\).*$/\1/i')
  if [[ "$version" != "$VERSION" ]]; then
    echo "Version mismatch, expected $VERSION, got $version"
    exit 1
  fi
}

create_kafka_topic() {
  "$KAFKA_PATH"/bin/kafka-topics.sh \
    --create \
    --topic backwards_compat_test_kafka_source --bootstrap-server localhost:29092
}

insert_json_kafka() {
  local JSON=$1
  echo "$JSON" | "$KAFKA_PATH"/bin/kafka-console-producer.sh \
    --topic backwards_compat_test_kafka_source \
    --bootstrap-server localhost:29092 \
    --property "parse.key=true" \
    --property "key.separator=,"
}

seed_json_kafka() {
  insert_json_kafka '{"user_id": 1},{"timestamp": "2023-07-28 07:11:00", "user_id": 1, "page_id": 1, "action": "gtrgretrg"}'
  insert_json_kafka '{"user_id": 2},{"timestamp": "2023-07-28 07:11:00", "user_id": 2, "page_id": 1, "action": "fsdfgerrg"}'
  insert_json_kafka '{"user_id": 3},{"timestamp": "2023-07-28 07:11:00", "user_id": 3, "page_id": 1, "action": "sdfergtth"}'
  insert_json_kafka '{"user_id": 4},{"timestamp": "2023-07-28 06:54:00", "user_id": 4, "page_id": 2, "action": "erwerhghj"}'
  insert_json_kafka '{"user_id": 5},{"timestamp": "2023-07-28 06:54:00", "user_id": 5, "page_id": 2, "action": "kiku7ikkk"}'
  insert_json_kafka '{"user_id": 6},{"timestamp": "2023-07-28 06:54:00", "user_id": 6, "page_id": 3, "action": "6786745ge"}'
  insert_json_kafka '{"user_id": 7},{"timestamp": "2023-07-28 06:54:00", "user_id": 7, "page_id": 3, "action": "fgbgfnyyy"}'
  insert_json_kafka '{"user_id": 8},{"timestamp": "2023-07-28 06:54:00", "user_id": 8, "page_id": 4, "action": "werwerwwe"}'
  insert_json_kafka '{"user_id": 9},{"timestamp": "2023-07-28 06:54:00", "user_id": 9, "page_id": 4, "action": "yjtyjtyyy"}'
}

# https://stackoverflow.com/a/4024263
version_le() {
    printf '%s\n' "$1" "$2" | sort -C -V
}

version_lt() {
    ! version_le "$2" "$1"
}

################################### Entry Points

get_old_version() {
   # For backwards compat test we assume we are testing the latest version of RW (i.e. latest main commit)
   # against the Nth latest release candidate, where N > 1. N can be larger,
   # in case some old cluster did not upgrade.
   if [[ -z $VERSION_OFFSET ]]
   then
       local VERSION_OFFSET=1
   fi

   # First we obtain a list of versions from git branch names.
   # Then we normalize them to semver format (MAJOR.MINOR.PATCH).
   echo "--- git branch origin output"
   git branch -r | grep origin

   # Extract X.Y.Z tags
   echo "--- VERSION BRANCHES"
   local tags=$(git tag | grep -E "^v[0-9]+\.[0-9]+\.[0-9]+$" | tr -d 'v' | tr -d ' ')
   echo "$tags"

   # Then we sort them in descending order.
   echo "--- VERSIONS"
   local sorted_versions=$(echo -e "$tags" | sort -t '.' -n)
   echo "$sorted_versions"

   # Then we take the Nth latest version.
   # We set $OLD_VERSION to this.
   OLD_VERSION=$(echo -e "$sorted_versions" | tail -n $VERSION_OFFSET | head -1)
}

get_new_version() {
  # Next, for $NEW_VERSION we just scrape it from `workspace.package.version`.
  NEW_VERSION=$(cat Cargo.toml | grep "\[workspace\.package\]" -A 5 | sed -n 's/version = \"\([0-9]*\.[0-9]*\.[0-9]*\).*/\1/p' | tr -d ' ')
}

# Get $OLD_VERSION and $NEW_VERSION for Risingwave
get_rw_versions() {
  get_old_version
  get_new_version

  # FIXME(kwannoel): This check does not always hold.
  # The new/current version may not be up-to-date.
  # The new version is derived from Cargo.toml, which may not be up-to-date.
  # The old version are derived from git tags, which are up-to-date.
  # Then we assert that `$OLD_VERSION` <= `$NEW_VERSION`.
  #  if version_le "$OLD_VERSION" "$NEW_VERSION"
  #  then
  #    echo "OLD_VERSION: $OLD_VERSION"
  #    echo "NEW_VERSION: $NEW_VERSION"
  #  else
  #    echo "ERROR: $OLD_VERSION >= $NEW_VERSION"
  #    exit 1
  #  fi
}

# Setup table and materialized view.
# Run updates and deletes on the table.
# Get the results.
seed_old_cluster() {
  echo "--- Start cluster on old_version: $OLD_VERSION"
  # Caller should make sure the test env has these.
  # They are called here because the current tests
  # may not be backwards compatible, so we need to call
  # them in old cluster environment.
  cp -r e2e_test/streaming/nexmark $TEST_DIR
  cp -r e2e_test/nexmark/* $TEST_DIR/nexmark

  cp -r e2e_test/batch/tpch $TEST_DIR
  cp -r e2e_test/tpch/* $TEST_DIR/tpch

  ./risedev clean-data
  ./risedev d full-without-monitoring && rm .risingwave/log/*

  check_version "$OLD_VERSION"

  echo "--- BASIC TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/seed.slt"

  echo "--- BASIC TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_original.slt"

  echo "--- NEXMARK TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/seed.slt"

  echo "--- NEXMARK TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_original.slt"

  echo "--- TPCH TEST: Seeding old cluster with data"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/seed.slt"

  echo "--- TPCH TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_original.slt"

  echo "--- KAFKA TEST: Seeding old cluster with data"
  create_kafka_topic
  seed_json_kafka
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/seed.slt"
  # use the old syntax for version at most 1.5.4
  if version_le "$OLD_VERSION" "1.5.4" ; then
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/upsert/deprecate_upsert.slt"
  else
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/upsert/include_key_as.slt"
  fi

  echo "--- KAFKA TEST: wait 5s for kafka to process data"
  sleep 5

  echo "--- KAFKA TEST: Validating old cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_original.slt"

  # Test invalid WITH options, if OLD_VERSION <= 1.5.0
  if version_le "$OLD_VERSION" "1.5.0" ; then
    echo "--- KAFKA TEST (invalid options): Seeding old cluster with data"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/seed.slt"

    echo "--- KAFKA TEST (invalid options): wait 5s for kafka to process data"
    sleep 5

    echo "--- KAFKA TEST (invalid options): Validating old cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/validate_original.slt"
  fi

  echo "--- Killing cluster"
  kill_cluster
  echo "--- Killed cluster"
}

validate_new_cluster() {
  echo "--- Start cluster on latest"
  ./risedev d full-without-monitoring

  echo "--- Wait ${RECOVERY_DURATION}s for Recovery on Old Cluster Data"
  sleep $RECOVERY_DURATION

  check_version "$NEW_VERSION"

  echo "--- BASIC TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/basic/validate_restart.slt"

  echo "--- NEXMARK TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/nexmark-backwards-compat/validate_restart.slt"

  echo "--- TPCH TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/tpch-backwards-compat/validate_restart.slt"

  echo "--- KAFKA TEST: Seeding new cluster with data"
  seed_json_kafka

  echo "--- KAFKA TEST: Validating new cluster"
  sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/validate_restart.slt"

  # Test invalid WITH options, if OLD_VERSION <= 1.5.0
  if version_le "$OLD_VERSION" "1.5.0" ; then
    echo "--- KAFKA TEST (invalid options): Validating new cluster"
    sqllogictest -d dev -h localhost -p 4566 "$TEST_DIR/kafka/invalid_options/validate_restart.slt"
  fi

  kill_cluster
}