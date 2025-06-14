import os
import subprocess

EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), "..")


def run_example(query: str) -> str:
    result = subprocess.run(
        ["cargo", "run", "--quiet", "--", query],
        cwd=EXAMPLE_DIR,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout


def test_sqlite_query():
    out = run_example("SELECT name FROM users ORDER BY id")
    assert "Alice" in out and "Bob" in out


def test_pg_catalog_query():
    out = run_example("SELECT datname FROM pg_catalog.pg_database LIMIT 1")
    assert "postgres" in out
