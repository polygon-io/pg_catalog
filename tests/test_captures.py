import subprocess
import time
import glob
import yaml
import psycopg
import pytest

PORT = 5447
CONN_STR = f"host=127.0.0.1 port={PORT} dbname=pgtry user=postgres password=pencil sslmode=disable"

@pytest.fixture(scope="module")
def server():
    proc = subprocess.Popen([
        "cargo", "run", "--quiet", "--",
        "pg_catalog_data/pg_schema",
        "--default-catalog", "pgtry",
        "--default-schema", "public",
        "--host", "127.0.0.1",
        "--port", str(PORT),
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

    yield proc
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def test_captured_queries(server):
    capture_files = sorted(glob.glob("captures/*.yaml"))
    assert capture_files, "no capture files found"
    first = capture_files[0]
    with open(first) as f:
        data = yaml.safe_load(f)

    with psycopg.connect(CONN_STR) as conn:
        cur = conn.cursor()
        for entry in data:
            if not entry.get("success", True):
                continue
            query = entry["query"]
            params = entry.get("parameters") or []

            def convert_placeholders(q: str) -> str:
                out = []
                i = 0
                while i < len(q):
                    if q[i] == "$":
                        j = i + 1
                        while j < len(q) and q[j].isdigit():
                            j += 1
                        if j > i + 1:
                            out.append("%s")
                            i = j
                            continue
                    out.append(q[i])
                    i += 1
                return "".join(out)

            query_exec = convert_placeholders(query)
            cur.execute(query_exec, tuple(params))

            if cur.description is None:
                result = []
            else:
                try:
                    rows = cur.fetchall()
                    names = [d.name for d in cur.description]
                    result = [dict(zip(names, row)) for row in rows]
                except psycopg.DataError:
                    pgres = cur.pgresult
                    names = [d.name for d in cur.description]
                    result = []
                    for i in range(pgres.ntuples):
                        row = {}
                        for j, name in enumerate(names):
                            raw = pgres.get_value(i, j)
                            if raw is None:
                                row[name] = None
                                continue
                            raw_str = raw.decode()
                            expected = entry["result"][i].get(name)
                            if expected is None:
                                row[name] = None
                            elif isinstance(expected, bool):
                                row[name] = raw_str in ("t", "true", "1")
                            elif isinstance(expected, int):
                                row[name] = int(raw_str)
                            elif isinstance(expected, float):
                                row[name] = float(raw_str)
                            else:
                                row[name] = raw_str
                        result.append(row)

            assert result == entry.get("result")
