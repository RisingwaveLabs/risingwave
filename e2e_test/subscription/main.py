import subprocess
import psycopg2
import threading
import time


def execute_slt(slt):
    if slt is None or slt == "":
        return
    cmd = f"sqllogictest -p 4566 -d dev {slt}"
    print(f"Command line is [{cmd}]")
    subprocess.run(cmd,
                   shell=True,
                   check=True)
    time.sleep(3)

def create_table_subscription():
    execute_slt("./e2e_test/subscription/create_table_and_subscription.slt")

def drop_table_subscription():
    execute_slt("./e2e_test/subscription/drop_table_and_subscription.slt")

def execute_query(sql,conn):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    rows = cur.fetchall()
    cur.close()
    return rows

def execute_insert(sql,conn):
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()

def check_rows_data(expect_vec,row,status):
    value_len  = len(row)
    for index, value in enumerate(row):
        if index == value_len  - 1:
            continue
        if index == value_len  - 2:
            assert value == status,f"expect {value} but got {status}"
            continue
        assert value == expect_vec[index],f"expect {expect_vec[index]} but got {value}"

def test_cursor_snapshot():
    print(f"test_cursor_snapshot")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub full",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()


def test_cursor_snapshot_log_store():
    print(f"test_cursor_snapshot_log_store")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub full",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_begin():
    print(f"test_cursor_since_begin")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since begin()",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_now():
    print(f"test_cursor_since_now")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since now()",conn)
    time.sleep(2)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_without_since():
    print(f"test_cursor_since_now")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub",conn)
    time.sleep(2)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_since_rw_timestamp():
    print(f"test_cursor_since_rw_timestamp")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub since begin()",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_1 = row[0][valuelen - 1]
    check_rows_data([4,4],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_2 = row[0][valuelen - 1] - 1
    check_rows_data([5,5],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    valuelen = len(row[0])
    rw_timestamp_3 = row[0][valuelen - 1] + 1
    check_rows_data([6,6],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_1}",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row[0],"Insert")
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_2}",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5],row[0],"Insert")
    execute_insert("close cur",conn)

    execute_insert(f"declare cur subscription cursor for sub since {rw_timestamp_3}",conn)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    execute_insert("close cur",conn)

    drop_table_subscription()

def test_cursor_op():
    print(f"test_cursor_op")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub full",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("update t1 set v2 = 10 where v1 = 4",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4],row[0],"UpdateDelete")
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,10],row[0],"UpdateInsert")
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("delete from t1 where v1 = 4",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,10],row[0],"Delete")
    row = execute_query("fetch next from cur",conn)
    assert row == []

    execute_insert("close cur",conn)
    drop_table_subscription()

def test_cursor_with_table_alter():
    print(f"test_cursor_with_table_alter")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub full",conn)
    execute_insert("alter table t1 add v3 int",conn)
    execute_insert("insert into t1 values(4,4,4)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([1,2],row[0],"Insert")
    row = execute_query("fetch next from cur",conn)
    assert row == []
    row = execute_query("fetch next from cur",conn)
    check_rows_data([4,4,4],row[0],"Insert")
    execute_insert("insert into t1 values(5,5,5)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    check_rows_data([5,5,5],row[0],"Insert")
    execute_insert("alter table t1 drop column v2",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch next from cur",conn)
    assert row == []
    row = execute_query("fetch next from cur",conn)
    check_rows_data([6,6],row[0],"Insert")
    drop_table_subscription()

def test_cursor_fetch_n():
    print(f"test_cursor_fetch_n")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub full",conn)
    execute_insert("insert into t1 values(4,4)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(5,5)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(6,6)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(7,7)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(8,8)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(9,9)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t1 values(10,10)",conn)
    execute_insert("flush",conn)
    execute_insert("update t1 set v2 = 100 where v1 = 10",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 6 from cur",conn)
    assert len(row) == 6
    check_rows_data([1,2],row[0],"Insert")
    check_rows_data([4,4],row[1],"Insert")
    check_rows_data([5,5],row[2],"Insert")
    check_rows_data([6,6],row[3],"Insert")
    check_rows_data([7,7],row[4],"Insert")
    check_rows_data([8,8],row[5],"Insert")
    row = execute_query("fetch 6 from cur",conn)
    assert len(row) == 4
    check_rows_data([9,9],row[0],"Insert")
    check_rows_data([10,10],row[1],"Insert")
    check_rows_data([10,10],row[2],"UpdateDelete")
    check_rows_data([10,100],row[3],"UpdateInsert")
    drop_table_subscription()

def test_rebuild_table():
    print(f"test_rebuild_table")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )

    execute_insert("declare cur subscription cursor for sub2 full",conn)
    execute_insert("insert into t2 values(1,1)",conn)
    execute_insert("flush",conn)
    execute_insert("update t2 set v2 = 100 where v1 = 1",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 4 from cur",conn)
    assert len(row) == 3
    check_rows_data([1,1],row[0],"Insert")
    check_rows_data([1,1],row[1],"UpdateDelete")
    check_rows_data([1,100],row[2],"UpdateInsert")
    drop_table_subscription()

def test_block_cursor():
    print(f"test_block_cursor")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("declare cur subscription cursor for sub2 full",conn)
    execute_insert("insert into t2 values(1,1)",conn)
    execute_insert("flush",conn)
    execute_insert("update t2 set v2 = 100 where v1 = 1",conn)
    execute_insert("flush",conn)
    start_time = time.time()
    row = execute_query("fetch 100 from cur with (timeout = '30s')",conn)
    assert (time.time() - start_time) < 3
    assert len(row) == 3
    check_rows_data([1,1],row[0],"Insert")
    check_rows_data([1,1],row[1],"UpdateDelete")
    check_rows_data([1,100],row[2],"UpdateInsert")

    # Test block cursor fetches data successfully
    thread = threading.Thread(target=insert_into_table)
    thread.start()
    row = execute_query("fetch 100 from cur with (timeout = '5s')",conn)
    check_rows_data([10,10],row[0],"Insert")
    thread.join()

    # Test block cursor timeout
    row = execute_query("fetch 100 from cur with (timeout = '5s')",conn)
    assert row == []

    drop_table_subscription()

def insert_into_table():
    time.sleep(2)
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t2 values(10,10)",conn)

def test_order_table_with_pk():
    print(f"test_order_table_with_pk")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t2 values(6,6),(3,3),(5,5),(4,4),(7,7)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub2 full",conn)
    row = execute_query("fetch 5 from cur",conn)
    assert len(row) == 5
    check_rows_data([3,3],row[0],"Insert")
    check_rows_data([4,4],row[1],"Insert")
    check_rows_data([5,5],row[2],"Insert")
    check_rows_data([6,6],row[3],"Insert")
    check_rows_data([7,7],row[4],"Insert")
    execute_insert("insert into t2 values(16,16),(13,13),(15,15),(14,14),(17,17)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 5 from cur",conn)
    assert len(row) == 5
    check_rows_data([13,13],row[0],"Insert")
    check_rows_data([14,14],row[1],"Insert")
    check_rows_data([15,15],row[2],"Insert")
    check_rows_data([16,16],row[3],"Insert")
    check_rows_data([17,17],row[4],"Insert")
    execute_insert("update t2 set v2 = 100 where v1 > 10",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 10 from cur",conn)
    assert len(row) == 10
    check_rows_data([13,13],row[0],"UpdateDelete")
    check_rows_data([13,100],row[1],"UpdateInsert")
    check_rows_data([14,14],row[2],"UpdateDelete")
    check_rows_data([14,100],row[3],"UpdateInsert")
    check_rows_data([15,15],row[4],"UpdateDelete")
    check_rows_data([15,100],row[5],"UpdateInsert")
    check_rows_data([16,16],row[6],"UpdateDelete")
    check_rows_data([16,100],row[7],"UpdateInsert")
    check_rows_data([17,17],row[8],"UpdateDelete")
    check_rows_data([17,100],row[9],"UpdateInsert")
    drop_table_subscription()

def test_order_table_with_row_id():
    print(f"test_order_table_with_pk")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t1 values(6,6),(3,3),(5,5),(4,4),(7,7)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub full",conn)
    row = execute_query("fetch 10 from cur",conn)
    ex_row = execute_query("select v1, v2 from t1 order by _row_id",conn)
    assert len(row) == 6
    assert len(ex_row) == 6
    check_rows_data(ex_row[0],row[0],"Insert")
    check_rows_data(ex_row[1],row[1],"Insert")
    check_rows_data(ex_row[2],row[2],"Insert")
    check_rows_data(ex_row[3],row[3],"Insert")
    check_rows_data(ex_row[4],row[4],"Insert")
    execute_insert("insert into t1 values(16,16),(13,13),(15,15),(14,14),(17,17)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 5 from cur",conn)
    ex_row = execute_query("select v1, v2 from t1 where v1 > 10 order by _row_id",conn)
    assert len(row) == 5
    assert len(ex_row) == 5
    check_rows_data(ex_row[0],row[0],"Insert")
    check_rows_data(ex_row[1],row[1],"Insert")
    check_rows_data(ex_row[2],row[2],"Insert")
    check_rows_data(ex_row[3],row[3],"Insert")
    check_rows_data(ex_row[4],row[4],"Insert")
    drop_table_subscription()

def test_order_mv():
    print(f"test_order_mv")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t4 values(6,6,6,6),(3,3,3,3),(5,5,5,5),(4,4,4,4),(7,7,7,7)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub4 full",conn)
    row = execute_query("fetch 5 from cur",conn)
    ex_row = execute_query("select v4, v2 from t4 order by _row_id",conn)
    assert len(row) == 5
    assert len(ex_row) == 5
    check_rows_data(ex_row[0],row[0],"Insert")
    check_rows_data(ex_row[1],row[1],"Insert")
    check_rows_data(ex_row[2],row[2],"Insert")
    check_rows_data(ex_row[3],row[3],"Insert")
    check_rows_data(ex_row[4],row[4],"Insert")
    execute_insert("insert into t4 values(16,16,16,16),(13,13,13,13),(15,15,15,15),(14,14,14,14),(17,17,17,17)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 5 from cur",conn)
    ex_row = execute_query("select v4, v2 from t4 where v2 > 10 order by _row_id",conn)
    assert len(row) == 5
    assert len(ex_row) == 5
    check_rows_data(ex_row[0],row[0],"Insert")
    check_rows_data(ex_row[1],row[1],"Insert")
    check_rows_data(ex_row[2],row[2],"Insert")
    check_rows_data(ex_row[3],row[3],"Insert")
    check_rows_data(ex_row[4],row[4],"Insert")
    drop_table_subscription()

def test_order_multi_pk():
    print(f"test_order_mutil_pk")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t5 values(6,6,6,6),(6,3,3,3),(5,5,5,5),(5,4,4,4),(7,7,7,7)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub5 full",conn)
    row = execute_query("fetch 5 from cur",conn)
    assert len(row) == 5
    check_rows_data([5,4,4,4],row[0],"Insert")
    check_rows_data([5,5,5,5],row[1],"Insert")
    check_rows_data([6,3,3,3],row[2],"Insert")
    check_rows_data([6,6,6,6],row[3],"Insert")
    check_rows_data([7,7,7,7],row[4],"Insert")
    execute_insert("insert into t5 values(16,16,16,16),(16,13,13,13),(15,15,15,15),(15,14,14,14),(17,17,17,17)",conn)
    execute_insert("flush",conn)
    row = execute_query("fetch 5 from cur",conn)
    assert len(row) == 5
    check_rows_data([15,14,14,14],row[0],"Insert")
    check_rows_data([15,15,15,15],row[1],"Insert")
    check_rows_data([16,13,13,13],row[2],"Insert")
    check_rows_data([16,16,16,16],row[3],"Insert")
    check_rows_data([17,17,17,17],row[4],"Insert")
    drop_table_subscription()

def test_explain_cursor():
    print(f"test_explain_cursor")
    create_table_subscription()
    conn = psycopg2.connect(
        host="localhost",
        port="4566",
        user="root",
        database="dev"
    )
    execute_insert("insert into t5 values(1,1,1,1)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t5 values(2,2,2,2)",conn)
    execute_insert("flush",conn)
    execute_insert("declare cur subscription cursor for sub5 full",conn)
    execute_insert("insert into t5 values(3,3,3,3)",conn)
    execute_insert("flush",conn)
    execute_insert("insert into t5 values(4,4,4,4)",conn)
    execute_insert("flush",conn)
    plan = execute_query("explain fetch next from cur",conn)
    assert plan[0][0] == "BatchExchange { order: [t5.v1 ASC, t5.v2 ASC], dist: Single }"
    assert plan[1][0] == "└─BatchScan { table: t5, columns: [v1, v2, v3, v4] }"
    execute_query("fetch next from cur",conn)
    plan = execute_query("explain fetch next from cur",conn)
    assert plan[0][0] == "BatchExchange { order: [t5.v1 ASC, t5.v2 ASC], dist: Single }"
    assert plan[1][0] == "└─BatchScan { table: t5, columns: [v1, v2, v3, v4], scan_ranges: [(v1, v2) > (Int32(1), Int32(1))] }"
    execute_query("fetch next from cur",conn)
    execute_query("fetch next from cur",conn)
    plan = execute_query("explain fetch next from cur",conn)
    print(plan)
    assert plan[0][0] == "BatchExchange { order: [t5.v1 ASC, t5.v2 ASC], dist: Single }"
    assert "└─BatchLogSeqScan { table: t5, columns: [v1, v2, v3, v4, op]" in plan[1][0]
    assert "scan_ranges: [(v1, v2) > (Int32(3), Int32(3))] }" in plan[1][0]
    execute_query("fetch next from cur",conn)
    drop_table_subscription()

if __name__ == "__main__":
    test_cursor_snapshot()
    test_cursor_op()
    test_cursor_snapshot_log_store()
    test_cursor_since_rw_timestamp()
    test_cursor_since_now()
    test_cursor_without_since()
    test_cursor_since_begin()
    test_cursor_with_table_alter()
    test_cursor_fetch_n()
    test_rebuild_table()
    test_order_table_with_pk()
    test_order_table_with_row_id()
    test_order_mv()
    test_order_multi_pk()
    test_block_cursor()
    test_explain_cursor()
