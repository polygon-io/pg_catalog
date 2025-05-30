import subprocess
import time
import yaml
import psycopg
import pytest

PORT = 5450
CONN_STR = f"host=127.0.0.1 port={PORT} dbname=pgtry user=dbuser password=pencil sslmode=disable"

@pytest.fixture(scope="module")
def server(tmp_path_factory):
    cap_file = tmp_path_factory.mktemp("cap") / "capture.yaml"
    proc = subprocess.Popen([
        "cargo", "run", "--quiet", "--",
        "pg_catalog_data/pg_schema",
        "--default-catalog", "pgtry",
        "--default-schema", "public",
        "--host", "127.0.0.1",
        "--port", str(PORT),
        "--capture", str(cap_file),
    ], text=True)

    for _ in range(12):
        try:
            with psycopg.connect(CONN_STR):
                break
        except Exception:
            time.sleep(5)
    else:
        proc.terminate()
        raise RuntimeError("server failed to start")

    yield proc, cap_file
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_datacl_capture(server):
    proc, cap_file = server
    query = (
        "SELECT db.oid,db.* FROM pg_catalog.pg_database db WHERE 1 = 1 "
        "AND datallowconn AND NOT datistemplate OR db.datname ='pgtry' "
        "ORDER BY db.datname"
    )
    with psycopg.connect(CONN_STR) as conn:
        cur = conn.cursor()
        cur.execute(query)
        # no fetch - we just want the server to execute

    time.sleep(1)
    with open(cap_file) as f:
        data = yaml.safe_load(f)

    entry = next(e for e in data if e["query"].startswith("SELECT db.oid"))
    assert entry["result"][0]["datacl"] == "{{=Tc/dbuser,dbuser=CTc/dbuser}}"
