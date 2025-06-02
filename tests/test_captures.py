import subprocess
import time
import glob
import yaml
import psycopg
import pytest

PORT = 5447
CONN_STR = f"host=127.0.0.1 port={PORT} dbname=pgtry user=dbuser password=pencil sslmode=disable"

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


def get_results(cur):
    if cur.description is None:
        return []
    rows = cur.fetchall()
    names = [d.name for d in cur.description]
    result = [dict(zip(names, row)) for row in rows]
    return result
    # pgres = cur.pgresult
    # names = [d.name for d in cur.description]
    # result = []
    # for i in range(pgres.ntuples):
    #     row = {}
    #     for j, name in enumerate(names):
    #         raw_value = pgres.get_value(i, j)
    #         row[name] = raw_value
    #     result.append(row)
    # return result
    

# @pytest.mark.skip(reason="capture replay not stable")
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
            """
            We convert $1 to %s queries, because psycopg3 wants that. 
            It sends them as parameters to the server - as it should. 
            pyscopg3 doesn't do string interpolation
            """
            query_exec = convert_placeholders(query)
            cur.execute(query_exec, tuple(params))
            results = get_results(cur)
            expected_results = entry.get("result")

            for (expected_row, row) in zip(expected_results, results):
                print("row", row)
                print("expected", expected_row)
                if row != expected_row:
                    import ipdb; ipdb.set_trace()


            # if results != expected_results:
            #     print("results", results)
            #     print("expected_results", expected_results)
            #     import ipdb; ipdb.set_trace()


            # assert result == entry.get("result")
