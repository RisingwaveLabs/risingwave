import subprocess
import sys

relations = ['demo.demo_bhv_table']

failed_cases = []
for rel in relations:
    sql = f'SELECT COUNT(*) FROM {rel};'
    print(f"Running SQL: {sql} ON Starrocks")
    command = f'mysql -uroot -P9030 -h127.0.0.1 -e "{sql}"'
    output = subprocess.check_output(
        ["docker", "compose", "exec", "starrocks-fe", "bash", "-c", command])
    # output:
    # COUNT(*)
    # 0
    rows = int(output.decode('utf-8').split('\n')[1])
    print(f"{rows} rows in {rel}")
    if rows < 1:
        failed_cases.append(rel)

if len(failed_cases) != 0:
    print(f"Data check failed for case {failed_cases}")
    sys.exit(1)
