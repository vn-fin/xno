from __future__ import annotations

import os
import threading
from typing import Any, Iterable, Optional

import duckdb

_DB_LOCK = threading.Lock()
_CONN: Optional[duckdb.DuckDBPyConnection] = None


def _db_path() -> str:
    """Return database path or ':memory:' if not set."""
    return os.environ.get("XNO_DUCKDB_PATH", ":memory:")


def get_conn() -> duckdb.DuckDBPyConnection:
    """
    Return a singleton DuckDB connection. Thread-safe lazy init.
    """
    global _CONN
    if _CONN is None:
        with _DB_LOCK:
            if _CONN is None:
                path = _db_path()
                _CONN = duckdb.connect(database=path, read_only=False)
                # optional PRAGMAS can be set here, e.g. enable_httpfs for remote reads
                # _CONN.execute("PRAGMA memory_limit='4GB'")
    return _CONN


def execute(sql: str, params: Optional[Iterable[Any]] = None) -> None:
    """
    Execute a SQL statement. Use params for parameterized queries.
    """
    conn = get_conn()
    if params:
        conn.execute(sql, params)
    else:
        conn.execute(sql)

def fetch_numpy(sql: str, params: Optional[Iterable[Any]] = None) -> Any:
    """
    Execute SQL and return results as numpy arrays.
    """
    conn = get_conn()
    if params:
        result = conn.execute(sql, params)
    else:
        result = conn.execute(sql)
    return result.fetchnumpy()

def flush_arrow_to_duckdb(batch, table_name: str = "ohlcv_data") -> None:
    """
    Flush a pyarrow RecordBatch to DuckDB in-memory table 'ohlcv_data'.
    """
    conn = get_conn()
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM batch")
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM batch")


def close() -> None:
    """
    Close the singleton connection and reset state.
    """
    global _CONN
    with _DB_LOCK:
        if _CONN is not None:
            try:
                _CONN.close()
            finally:
                _CONN = None

# convenience alias
connect = get_conn
