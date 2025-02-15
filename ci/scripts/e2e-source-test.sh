#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

source ci/scripts/common.sh

# prepare environment
export CONNECTOR_LIBS_PATH="./connector-node/libs"

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

echo "--- Download connector node package"
buildkite-agent artifact download risingwave-connector.tar.gz ./
mkdir ./connector-node
tar xf ./risingwave-connector.tar.gz -C ./connector-node

echo "--- Install dependencies"
python3 -m pip install --break-system-packages -r ./e2e_test/requirements.txt
apt-get -y install jq

echo "--- e2e, inline test"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info,risingwave_meta=info" \
risedev ci-start ci-inline-source-test

# check if run debug only test
if [ "$profile" == "ci-dev" ]; then
    echo "--- Run debug mode only tests"
    risedev slt './e2e_test/debug_mode_only/debug_splits.slt'
fi

risedev slt './e2e_test/source_inline/**/*.slt' -j4
risedev slt './e2e_test/source_inline/**/*.slt.serial'

echo "--- Kill cluster"
risedev ci-kill

echo "--- Prepare data"
cp src/connector/src/test_data/simple-schema.avsc ./avro-simple-schema.avsc
cp src/connector/src/test_data/complex-schema.avsc ./avro-complex-schema.avsc
cp src/connector/src/test_data/complex-schema.json ./json-complex-schema


echo "--- e2e, ci-1cn-1fe, mysql & postgres cdc"

# import data to mysql
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source_legacy/cdc/mysql_cdc.sql

# import data to postgres
export PGHOST=db PGPORT=5432 PGUSER=postgres PGPASSWORD=postgres PGDATABASE=cdc_test
createdb
psql < ./e2e_test/source_legacy/cdc/postgres_cdc.sql

echo "--- starting risingwave cluster"
RUST_LOG="debug,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-1cn-1fe-with-recovery

echo "--- Install sql server client"
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
apt-get update -y
ACCEPT_EULA=Y DEBIAN_FRONTEND=noninteractive apt-get install -y mssql-tools unixodbc-dev
export PATH="/opt/mssql-tools/bin/:$PATH"
sleep 2

echo "--- mongodb cdc test"
# install the mongo shell
wget --no-verbose http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
wget --no-verbose https://repo.mongodb.org/apt/ubuntu/dists/focal/mongodb-org/4.4/multiverse/binary-amd64/mongodb-org-shell_4.4.28_amd64.deb
dpkg -i libssl1.1_1.1.1f-1ubuntu2_amd64.deb
dpkg -i mongodb-org-shell_4.4.28_amd64.deb

echo '> ping mongodb'
echo 'db.runCommand({ping: 1})' | mongo mongodb://mongodb:27017
echo '> rs config'
echo 'rs.conf()' | mongo mongodb://mongodb:27017
echo '> run test..'
risedev slt './e2e_test/source_legacy/cdc/mongodb/**/*.slt'

echo "--- inline cdc test"
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
export SQLCMDSERVER=sqlserver-server SQLCMDUSER=SA SQLCMDPASSWORD="SomeTestOnly@SA" SQLCMDDBNAME=mydb SQLCMDPORT=1433
risedev slt './e2e_test/source_legacy/cdc_inline/**/*.slt'

echo "--- mysql & postgres cdc validate test"
risedev slt './e2e_test/source_legacy/cdc/cdc.validate.mysql.slt'
risedev slt './e2e_test/source_legacy/cdc/cdc.validate.postgres.slt'

echo "--- cdc share source test"
# cdc share stream test cases
export MYSQL_HOST=mysql MYSQL_TCP_PORT=3306 MYSQL_PWD=123456
risedev slt './e2e_test/source_legacy/cdc/cdc.share_stream.slt'

echo "--- mysql & postgres load and check"
risedev slt './e2e_test/source_legacy/cdc/cdc.load.slt'
# wait for cdc loading
sleep 10
risedev slt './e2e_test/source_legacy/cdc/cdc.check.slt'

# kill cluster
risedev kill
echo "> cluster killed "

echo "--- mysql & postgres recovery check"
# insert into mytest database (cdc.share_stream.slt)
mysql --protocol=tcp -u root mytest -e "INSERT INTO products
       VALUES (default,'RisingWave','Next generation Streaming Database'),
              (default,'Materialize','The Streaming Database You Already Know How to Use');
       UPDATE products SET name = 'RW' WHERE id <= 103;
       INSERT INTO orders VALUES (default, '2022-12-01 15:08:22', 'Sam', 1000.52, 110, false);"


# insert new rows
mysql --host=mysql --port=3306 -u root -p123456 < ./e2e_test/source_legacy/cdc/mysql_cdc_insert.sql
echo "> inserted new rows into mysql"

psql < ./e2e_test/source_legacy/cdc/postgres_cdc_insert.sql
echo "> inserted new rows into postgres"

# start cluster w/o clean-data
unset RISINGWAVE_CI
export RUST_LOG="risingwave_stream=debug,risingwave_batch=info,risingwave_storage=info"

risedev dev ci-1cn-1fe-with-recovery
echo "> wait for cluster recovery finish"
sleep 20
echo "> check mviews after cluster recovery"
# check results
risedev slt './e2e_test/source_legacy/cdc/cdc.check_new_rows.slt'

# drop relations
risedev slt './e2e_test/source_legacy/cdc/cdc_share_stream_drop.slt'

echo "--- Kill cluster"
risedev ci-kill
export RISINGWAVE_CI=true

echo "--- e2e, ci-kafka-plus-pubsub, legacy kafka tests"
RUST_LOG="info,risingwave_stream=info,risingwave_batch=info,risingwave_storage=info" \
risedev ci-start ci-kafka
./e2e_test/source_legacy/basic/scripts/prepare_ci_kafka.sh
risedev slt './e2e_test/source_legacy/basic/*.slt'
risedev slt './e2e_test/source_legacy/basic/old_row_format_syntax/*.slt'

echo "--- Run CH-benCHmark"
risedev slt './e2e_test/ch_benchmark/batch/ch_benchmark.slt'
risedev slt './e2e_test/ch_benchmark/streaming/*.slt'

risedev ci-kill
echo "--- cluster killed "

echo "--- starting risingwave cluster for webhook source test"
risedev ci-start ci-1cn-1fe-with-recovery
sleep 5
# check results
risedev slt "e2e_test/webhook/webhook_source.slt"

risedev kill

risedev dev ci-1cn-1fe-with-recovery
echo "--- wait for cluster recovery finish"
sleep 20
risedev slt "e2e_test/webhook/webhook_source_recovery.slt"

risedev ci-kill
echo "--- cluster killed "
