"""DuckDB connection management for the BI API.

Loads the four SAS data product CSVs into an in-memory DuckDB database at
startup and exposes a dependency that yields a connection per request.

The CSV path is resolved relative to the project root using pathlib, matching
the pattern used in export_data.py (DATA_DIR = Path(__file__).resolve().parent / "data").
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import duckdb

# app/db.py -> app/ -> project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "03_sas_data_products"

# Mapping of DuckDB table name -> source CSV file name.
TABLE_FILES: dict[str, str] = {
    "customer_master_profile": "customer_master_profile.csv",
    "customer_segments": "customer_segments.csv",
    "transaction_analytics": "transaction_analytics.csv",
    "customer_risk_scores": "customer_risk_scores.csv",
}

# Module-level singleton connection. DuckDB in-memory databases are scoped to a
# single connection, so all request handlers share this one connection.
_connection: duckdb.DuckDBPyConnection | None = None


def init_db() -> duckdb.DuckDBPyConnection:
    """Initialize the in-memory DuckDB database from the SAS data product CSVs.

    Idempotent: safe to call multiple times; tables are (re)created from the CSVs.
    Returns the shared connection.
    """
    global _connection

    if _connection is None:
        _connection = duckdb.connect(":memory:")

    for table, filename in TABLE_FILES.items():
        csv_path = DATA_DIR / filename
        if not csv_path.exists():
            raise FileNotFoundError(
                f"Expected data product CSV not found: {csv_path}. "
                "Run export_data.py to generate the pipeline data."
            )
        _connection.execute(
            f"CREATE OR REPLACE TABLE {table} AS "
            "SELECT * FROM read_csv_auto(?, header=true, sample_size=-1)",
            [str(csv_path)],
        )

    return _connection


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB connection, initializing it if necessary."""
    if _connection is None:
        return init_db()
    return _connection


def get_db() -> Iterator[duckdb.DuckDBPyConnection]:
    """FastAPI dependency that yields a DuckDB connection for a request."""
    yield get_connection()


def fetch_all(
    con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
) -> list[dict]:
    """Execute a query and return rows as a list of column->value dicts."""
    cur = con.execute(sql, params or [])
    columns = [d[0] for d in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_one(
    con: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
) -> dict | None:
    """Execute a query and return the first row as a dict, or None."""
    rows = fetch_all(con, sql, params)
    return rows[0] if rows else None
