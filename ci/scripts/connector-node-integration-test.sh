#!/usr/bin/env bash

# Exits as soon as any line fails.
set -euo pipefail

# Loop through the command line arguments
while [ $# -gt 0 ]; do
  case "$1" in
    -java)
      # Check if there is a parameter following -java
      if [ -n "$2" ]; then
        # Output the value of the parameter following -java
        echo "The value of 'java' parameter is: $2"
        shift 2
      else
        # Display an error message if no parameter is provided
        echo "Error: No value provided for 'java' parameter."
        shift
      fi
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


exit

RISINGWAVE_ROOT=${PWD}

echo "--- build connector node"
cd ${RISINGWAVE_ROOT}/java
mvn --batch-mode --update-snapshots clean package

echo "--- install postgresql client"
sudo apt install postgresql postgresql-contrib libpq-dev
sudo systemctl start postgresql || sudo pg_ctlcluster 12 main start
# disable password encryption
sudo -u postgres psql -c "CREATE ROLE test LOGIN SUPERUSER PASSWORD 'connector';"
sudo -u postgres createdb test
sudo -u postgres psql -d test -c "CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR (50) NOT NULL);"

echo "--- starting minio"
echo "setting up minio"
wget https://dl.minio.io/server/minio/release/linux-amd64/minio > /dev/null
chmod +x minio
sudo ./minio server /tmp/minio &
# wait for minio to start
sleep 3
wget https://dl.minio.io/client/mc/release/linux-amd64/mc > /dev/null
chmod +x mc
MC_PATH=${PWD}/mc
${MC_PATH} config host add minio http://127.0.0.1:9000 minioadmin minioadmin

echo "--- starting connector-node service"
cd ${RISINGWAVE_ROOT}/java/connector-node/assembly/target/
tar xvf risingwave-connector-1.0.0.tar.gz > /dev/null
sh ./start-service.sh &
sleep 3
cd ../../


echo "--- prepare integration tests"
cd ${RISINGWAVE_ROOT}/java/connector-node
pip3 install grpcio grpcio-tools psycopg2 psycopg2-binary pyspark==3.3
cd python-client && bash gen-stub.sh

echo "--- running jdbc integration tests"
if python3 integration_tests.py --file_sink; then
  echo "File sink test passed"
else
  echo "File sink test failed"
  exit 1
fi

if python3 integration_tests.py --jdbc_sink; then
  echo "Jdbc sink test passed"
else
  echo "Jdbc sink test failed"
  exit 1
fi
cd ..
echo "all jdbc tests passed"

echo "running iceberg integration tests"
${MC_PATH} mb minio/bucket

# test append-only mode
cd python-client
python3 pyspark-util.py create_iceberg
if python3 integration_tests.py --iceberg_sink; then
  python3 pyspark-util.py test_iceberg
  echo "Iceberg sink test passed"
else
  echo "Iceberg sink test failed"
  exit 1
fi
python3 pyspark-util.py drop_iceberg

# test upsert mode
python3 pyspark-util.py create_iceberg
if python3 integration_tests.py --upsert_iceberg_sink --input_file="./data/upsert_sink_input.json"; then
  python3 pyspark-util.py test_upsert_iceberg --input_file="./data/upsert_sink_input.json"
  echo "Upsert iceberg sink test passed"
else
  echo "Upsert iceberg sink test failed"
  exit 1
fi
python3 pyspark-util.py drop_iceberg

# clean up minio
cd ..
${MC_PATH} rm -r -force minio/bucket
${MC_PATH} rb minio/bucket
echo "all iceberg tests passed"

echo "running deltalake integration tests"
${MC_PATH} mb minio/bucket

cd python-client
# test append-only mode
python3 pyspark-util.py create_deltalake
if python3 integration_tests.py --deltalake_sink; then
  python3 pyspark-util.py test_deltalake
  echo "Deltalake sink test passed"
else
  echo "Deltalake sink test failed"
  exit 1
fi

# clean up minio
cd ..
${MC_PATH} rm -r -force minio/bucket
${MC_PATH} rb minio/bucket
echo "all deltalake tests passed"
