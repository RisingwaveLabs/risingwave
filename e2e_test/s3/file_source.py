import os
import sys
import csv
import json
import random
import psycopg2

# from handle_incremental_file import upload_to_s3_bucket, check_for_new_files
from time import sleep
import time
from io import StringIO
from minio import Minio
import gzip
from functools import partial

def gen_data(file_num, item_num_per_file):
    assert item_num_per_file % 2 == 0, \
        f'item_num_per_file should be even to ensure sum(mark) == 0: {item_num_per_file}'
    return [
        [{
            'id': file_id * item_num_per_file + item_id,
            'name': f'{file_id}_{item_id}',
            'sex': item_id % 2,
            'mark': (-1) ** (item_id % 2),
        } for item_id in range(item_num_per_file)]
        for file_id in range(file_num)
    ]

def format_json(data):
    return [
        '\n'.join([json.dumps(item) for item in file])
        for file in data
    ]


def format_csv(data, with_header):
    csv_files = []

    for file_data in data:
        ostream = StringIO()
        writer = csv.DictWriter(ostream, fieldnames=file_data[0].keys())
        if with_header:
            writer.writeheader()
        for item_data in file_data:
            writer.writerow(item_data)
        csv_files.append(ostream.getvalue())
    return csv_files

def do_test(config, file_num, item_num_per_file, prefix, fmt, need_drop_table=True):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f's3_test_{fmt}'

    def _encode():
        if fmt == 'json':
            return 'JSON'
        else:
            return f"CSV (delimiter = ',', without_header = {str('without' in fmt).lower()})"

    def _include_clause():
        if fmt == 'json':
            return 'INCLUDE payload as rw_payload'
        else:
            return ''

    # Execute a SELECT statement
    cur.execute(f'''CREATE TABLE {_table()}(
        id int,
        name TEXT,
        sex int,
        mark int,
    )
    {_include_clause()}
    WITH (
        connector = 's3',
        match_pattern = '{prefix}*.{fmt}',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}',
        refresh.interval.sec = 1
    ) FORMAT PLAIN ENCODE {_encode()};''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_table()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 30s")
        sleep(30)

    stmt = f'select count(*), sum(id), sum(sex), sum(mark) from {_table()}'
    print(f'Execute {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)
    _assert_eq('sum(sex)', result[2], total_rows / 2)
    _assert_eq('sum(mark)', result[3], 0)

    # only do payload check for json format, which enables INCLUDE CLAUSE
    if fmt == 'json':
        # check rw_payload
        print('Check rw_payload')
        stmt = f"select id, name, sex, mark, rw_payload from {_table()} limit 1;"
        cur.execute(stmt)
        result = cur.fetchone()
        print("Got one line with rw_payload: ", result)
        payload = result[4]
        _assert_eq('id', payload['id'], result[0])
        _assert_eq('name', payload['name'], result[1])
        _assert_eq('sex', payload['sex'], result[2])
        _assert_eq('mark', payload['mark'], result[3])

        print('Test pass')

    if need_drop_table:
        cur.execute(f'drop table {_table()}')
    cur.close()
    conn.close()

FORMATTER = {
        'json': format_json,
        'csv_with_header': partial(format_csv, with_header=True),
        'csv_without_header': partial(format_csv, with_header=False),
    }


def test_batch_read(config, file_num, item_num_per_file, prefix, fmt):
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _source():
        return f's3_test_{fmt}'

    def _encode():
        if fmt == 'json':
            return 'JSON'
        else:
            return f"CSV (delimiter = ',', without_header = {str('without' in fmt).lower()})"

    # Execute a SELECT statement
    if fmt == 'json':
        cur.execute(f'''CREATE SOURCE {_source()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        ) WITH (
            connector = 's3',
            match_pattern = '{prefix}*.{fmt}.gz',
        s3.region_name = '{config['S3_REGION']}',
        s3.bucket_name = '{config['S3_BUCKET']}',
        s3.credentials.access = '{config['S3_ACCESS_KEY']}',
        s3.credentials.secret = '{config['S3_SECRET_KEY']}',
        s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
        ) FORMAT PLAIN ENCODE {_encode()};''')
    else:
        cur.execute(f'''CREATE SOURCE {_source()}(
            id int,
            name TEXT,
            sex int,
            mark int,
        ) WITH (
            connector = 's3',
            match_pattern = '{prefix}*.{fmt}',
            s3.region_name = '{config['S3_REGION']}',
            s3.bucket_name = '{config['S3_BUCKET']}',
            s3.credentials.access = '{config['S3_ACCESS_KEY']}',
            s3.credentials.secret = '{config['S3_SECRET_KEY']}',
            s3.endpoint_url = 'https://{config['S3_ENDPOINT']}'
        ) FORMAT PLAIN ENCODE {_encode()};''')

    total_rows = file_num * item_num_per_file
    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f'select count(*) from {_source()}')
        result = cur.fetchone()
        if result[0] == total_rows:
            break
        print(f"[retry {retry_no}] Now got {result[0]} rows in source, {total_rows} expected, wait 30s")
        sleep(30)

    stmt = f'select count(*), sum(id), sum(sex), sum(mark) from {_source()}'
    print(f'Execute {stmt}')
    cur.execute(stmt)
    result = cur.fetchone()

    print('Got:', result)

    def _assert_eq(field, got, expect):
        assert got == expect, f'{field} assertion failed: got {got}, expect {expect}.'

    _assert_eq('count(*)', result[0], total_rows)
    _assert_eq('sum(id)', result[1], (total_rows - 1) * total_rows / 2)
    _assert_eq('sum(sex)', result[2], total_rows / 2)
    _assert_eq('sum(mark)', result[3], 0)

    print('Test batch read pass')

    cur.execute(f'drop source {_source()}')
    cur.close()
    conn.close()


def upload_to_s3_bucket(config, minio_client, run_id, files, start_bias):
    _local = lambda idx, start_bias: f"data_{idx + start_bias}.{fmt}"
    _s3 = lambda idx, start_bias: f"{run_id}_data_{idx + start_bias}.{fmt}"
    for idx, file_str in enumerate(files):
        with open(_local(idx, start_bias), "w") as f:
            f.write(file_str)
            os.fsync(f.fileno())

        minio_client.fput_object(
            config["S3_BUCKET"], _s3(idx, start_bias), _local(idx, start_bias)
        )


def check_for_new_files(file_num, item_num_per_file, fmt):
    conn = psycopg2.connect(host="localhost", port="4566", user="root", database="dev")

    # Open a cursor to execute SQL statements
    cur = conn.cursor()

    def _table():
        return f"s3_test_{fmt}"

    total_rows = file_num * item_num_per_file

    MAX_RETRIES = 40
    for retry_no in range(MAX_RETRIES):
        cur.execute(f"select count(*) from {_table()}")
        result = cur.fetchone()
        if result[0] == total_rows:
            return True
        print(
            f"[retry {retry_no}] Now got {result[0]} rows in table, {total_rows} expected, wait 10s"
        )
        time.sleep(10)
    return False


if __name__ == "__main__":
    FILE_NUM = 4001
    ITEM_NUM_PER_FILE = 2
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)

    fmt = sys.argv[1]
    assert fmt in FORMATTER, f"Unsupported format: {fmt}"
    formatted_files = FORMATTER[fmt](data)

    config = json.loads(os.environ["S3_SOURCE_TEST_CONF"])
    client = Minio(
        config["S3_ENDPOINT"],
        access_key=config["S3_ACCESS_KEY"],
        secret_key=config["S3_SECRET_KEY"],
        secure=True,
    )
    run_id = str(random.randint(1000, 9999))
    
    _local = lambda idx: f'data_{idx}.{fmt}'
    if fmt == 'json':
        _s3 = lambda idx: f"{run_id}_data_{idx}.{fmt}.gz"
        for idx, file_str in enumerate(formatted_files):
            with open(_local(idx), "w") as f:
                with gzip.open(_local(idx) + '.gz', 'wb') as f_gz:
                    f_gz.write(file_str.encode('utf-8'))  
                    os.fsync(f.fileno())

            client.fput_object(
                config["S3_BUCKET"],
                _s3(idx),
                _local(idx) + '.gz'
            )
    else:
        _s3 = lambda idx: f"{run_id}_data_{idx}.{fmt}"
        for idx, file_str in enumerate(formatted_files):
            with open(_local(idx), "w") as f:
                f.write(file_str)
                os.fsync(f.fileno())

            client.fput_object(
                config["S3_BUCKET"],
                _s3(idx),
                _local(idx) 
            )

    # put s3 files
    for idx, file_str in enumerate(formatted_files):
        with open(_local(idx), "w") as f:
            with gzip.open(_local(idx) + '.gz', 'wb') as f_gz:
                f_gz.write(file_str.encode('utf-8'))  
                os.fsync(f.fileno())

        client.fput_object(
            config["S3_BUCKET"],
            _s3(idx),
            _local(idx) + '.gz'
        )

    # do test
    print("Test streaming file source...\n")
    do_test(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    print("Test batch read file source...\n")
    test_batch_read(config, FILE_NUM, ITEM_NUM_PER_FILE, run_id, fmt)

    # test file source handle incremental files
    data = gen_data(FILE_NUM, ITEM_NUM_PER_FILE)
    fmt = "json"

    split_idx = 51
    data_batch1 = data[:split_idx]
    data_batch2 = data[split_idx:]
    run_id = str(random.randint(1000, 9999))
    print(f"S3 Source New File Test: run ID: {run_id} to buckek")

    formatted_batch1 = FORMATTER[fmt](data_batch1)
    upload_to_s3_bucket(config, client, run_id, formatted_batch1, 0)

    # config in do_test that fs source's list interval is 1s
    do_test(
        config, len(data_batch1), ITEM_NUM_PER_FILE, run_id, fmt, need_drop_table=False
    )

    formatted_batch2 = FORMATTER[fmt](data_batch2)
    upload_to_s3_bucket(config, client, run_id, formatted_batch2, split_idx)

    success_flag = check_for_new_files(FILE_NUM, ITEM_NUM_PER_FILE, fmt)
    if success_flag:
        print("Test(add new file) pass")
    else:
        print("Test(add new file) fail")

    _s3 = lambda idx, start_bias: f"{run_id}_data_{idx + start_bias}.{fmt}"

    # clean up s3 files
    for idx, _ in enumerate(formatted_files):
        client.remove_object(config["S3_BUCKET"], _s3(idx, 0))
