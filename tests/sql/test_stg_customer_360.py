"""Parity test for stg.usp_load_stg_customer_360 (AZSYN-02).

Runs the Synapse proc on SQL Server 2022 (Docker) against the committed
data/01_source_tables CSVs with @AsOfDate = '2026-04-10' and compares the result
to data/02_bteq_staging/stg_customer_360.csv.

Synapse -> SQL Server shim (SQL Server does not support Synapse-only syntax):
  1. Strip "WITH (DISTRIBUTION = ..., <index>)" table options.
  2. Rewrite "CREATE TABLE <t> AS SELECT <cols> FROM ..." (CTAS) into
     "SELECT <cols> INTO <t> FROM ..." (INTO inserted before the first FROM at
     parenthesis depth 0).
Everything else in the proc runs unchanged.

Connection: set MSSQL_HOST/MSSQL_PORT/MSSQL_USER/MSSQL_PASSWORD to reuse a
running server; otherwise a throwaway mcr.microsoft.com/mssql/server:2022-latest
container is started. The test skips cleanly if pymssql or Docker is missing.
"""

from __future__ import annotations

import calendar
import datetime as dt
import math
import os
import re
import shutil
import socket
import subprocess
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest

pymssql = pytest.importorskip("pymssql")

REPO = Path(__file__).resolve().parents[2]
PROC_FILE = REPO / "synapse" / "sql" / "procs" / "stg.usp_load_stg_customer_360.sql"
SRC_DIR = REPO / "data" / "01_source_tables"
EXPECTED_CSV = REPO / "data" / "02_bteq_staging" / "stg_customer_360.csv"
AS_OF_DATE = dt.date(2026, 4, 10)
IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
SA_PASSWORD = "AzSyn02!Parity#2026"
TOL = 0.01

# Source + log DDL (Synapse types per contract; distribution options omitted for SQL Server).
SETUP_DDL = [
    "CREATE SCHEMA core",
    "CREATE SCHEMA stg",
    """CREATE TABLE core.CUSTOMERS (
        CUSTOMER_ID BIGINT NOT NULL, FIRST_NAME VARCHAR(60), LAST_NAME VARCHAR(60),
        DATE_OF_BIRTH DATE, SSN_HASH CHAR(64), EMAIL VARCHAR(120), PHONE_PRIMARY VARCHAR(20),
        CUSTOMER_SINCE DATE, CUSTOMER_STATUS CHAR(1), SEGMENT_CODE VARCHAR(10), BRANCH_ID INT,
        CREATED_TS DATETIME2(6), UPDATED_TS DATETIME2(6))""",
    """CREATE TABLE core.ACCOUNTS (
        ACCOUNT_ID BIGINT NOT NULL, CUSTOMER_ID BIGINT NOT NULL, ACCOUNT_TYPE VARCHAR(20),
        ACCOUNT_STATUS CHAR(1), OPEN_DATE DATE, CLOSE_DATE DATE, CURRENT_BALANCE DECIMAL(15,2),
        AVAILABLE_BALANCE DECIMAL(15,2), CREDIT_LIMIT DECIMAL(15,2), INTEREST_RATE DECIMAL(5,4),
        BRANCH_ID INT, CREATED_TS DATETIME2(6), UPDATED_TS DATETIME2(6))""",
    """CREATE TABLE core.ADDRESSES (
        ADDRESS_ID BIGINT NOT NULL, CUSTOMER_ID BIGINT NOT NULL, ADDRESS_TYPE VARCHAR(10),
        ADDRESS_LINE_1 VARCHAR(100), ADDRESS_LINE_2 VARCHAR(100), CITY VARCHAR(60),
        STATE_CODE CHAR(2), ZIP_CODE VARCHAR(10), COUNTRY_CODE CHAR(2) DEFAULT 'US',
        IS_PRIMARY CHAR(1) DEFAULT 'N', EFFECTIVE_DATE DATE, EXPIRATION_DATE DATE,
        CREATED_TS DATETIME2(6), UPDATED_TS DATETIME2(6))""",
    """CREATE TABLE stg.ETL_RUN_LOG (
        JOB_NAME VARCHAR(100) NOT NULL, STEP_NAME VARCHAR(100), STATUS VARCHAR(20) NOT NULL,
        ROW_COUNT BIGINT, START_TS DATETIME2(6), END_TS DATETIME2(6), MESSAGE VARCHAR(4000))""",
]

SOURCES = {
    "core.CUSTOMERS": "customers.csv",
    "core.ACCOUNTS": "accounts.csv",
    "core.ADDRESSES": "addresses.csv",
}

# Expected DDL of ETL_STAGING_DB.STG_CUSTOMER_360 mapped to T-SQL (name, type, precision/length, scale).
EXPECTED_SCHEMA = [
    ("CUSTOMER_ID", "bigint"), ("FIRST_NAME", "varchar(60)"), ("LAST_NAME", "varchar(60)"),
    ("DATE_OF_BIRTH", "date"), ("AGE", "smallint"), ("CUSTOMER_SINCE", "date"),
    ("TENURE_MONTHS", "int"), ("CUSTOMER_STATUS", "char(1)"), ("SEGMENT_CODE", "varchar(10)"),
    ("BRANCH_ID", "int"), ("PRIMARY_ADDRESS", "varchar(200)"), ("CITY", "varchar(60)"),
    ("STATE_CODE", "char(2)"), ("ZIP_CODE", "varchar(10)"), ("NUM_ACCOUNTS", "smallint"),
    ("NUM_ACTIVE_ACCOUNTS", "smallint"), ("HAS_CHECKING", "char(1)"), ("HAS_SAVINGS", "char(1)"),
    ("HAS_CREDIT", "char(1)"), ("HAS_LOAN", "char(1)"), ("TOTAL_BALANCE", "decimal(18,2)"),
    ("TOTAL_CREDIT_LIMIT", "decimal(18,2)"), ("CREDIT_UTILIZATION_PCT", "decimal(5,2)"),
    ("LOAD_TS", "datetime2(6)"),
]

EXACT_COLS = [
    "first_name", "last_name", "date_of_birth", "customer_since", "customer_status",
    "segment_code", "branch_id", "primary_address", "city", "state_code", "zip_code",
    "num_accounts", "num_active_accounts", "has_checking", "has_savings", "has_credit", "has_loan",
]
NUMERIC_COLS = ["total_balance", "total_credit_limit", "credit_utilization_pct"]


# --------------------------------------------------------------------------- shim
def _find_top_level_from(sql: str, start: int) -> int:
    depth = 0
    for m in re.finditer(r"\(|\)|\bFROM\b|'(?:[^']|'')*'|--[^\n]*", sql[start:], re.IGNORECASE):
        tok = m.group(0)
        if tok == "(":
            depth += 1
        elif tok == ")":
            depth -= 1
        elif tok.upper() == "FROM" and depth == 0:
            return start + m.start()
    raise ValueError("no top-level FROM found in CTAS")


def synapse_to_sqlserver(sql: str) -> str:
    """Apply the documented Synapse -> SQL Server shim to a proc body."""
    sql = re.sub(r"\bWITH\s*\(\s*DISTRIBUTION\s*=.*?\)\s*(?:,[^()]*?(?:\([^()]*\))?[^()]*?)*\)", "", sql,
                 flags=re.IGNORECASE | re.DOTALL)
    ctas = re.compile(r"CREATE\s+TABLE\s+([\w.\[\]]+)\s+AS\s+SELECT\b", re.IGNORECASE)
    while (m := ctas.search(sql)) is not None:
        table, select_end = m.group(1), m.end()
        from_pos = _find_top_level_from(sql, select_end)
        sql = (sql[: m.start()] + "SELECT" + sql[select_end:from_pos]
               + f"INTO {table}\n        " + sql[from_pos:])
    return sql


def test_shim_rewrites_ctas() -> None:
    src = ("CREATE TABLE stg.T\nWITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)\n"
           "AS\nSELECT CAST(x AS INT) AS X, (SELECT 1 FROM y) AS Y FROM z WHERE a IN (SELECT b FROM c);")
    out = synapse_to_sqlserver(src)
    assert "DISTRIBUTION" not in out and "CREATE TABLE" not in out
    assert re.search(r"AS Y INTO stg\.T\s+FROM z", out)


# --------------------------------------------------------------------------- Teradata reference semantics
def teradata_age(dob: dt.date, as_of: dt.date) -> int:
    """CAST((as_of - dob) / 365.25 AS SMALLINT): scale-2 decimal division, then truncation."""
    q = (Decimal((as_of - dob).days) / Decimal("365.25")).quantize(Decimal("0.01"))
    return int(q)


def teradata_months_between(d1: dt.date, d2: dt.date) -> float:
    def last(d: dt.date) -> bool:
        return d.day == calendar.monthrange(d.year, d.month)[1]
    whole = (d1.year - d2.year) * 12 + (d1.month - d2.month)
    if d1.day == d2.day or (last(d1) and last(d2)):
        return float(whole)
    return whole + (d1.day - d2.day) / 31


def boundary_years(d1: dt.date, d2: dt.date) -> int:
    return d1.year - d2.year


def boundary_months(d1: dt.date, d2: dt.date) -> int:
    return (d1.year - d2.year) * 12 + (d1.month - d2.month)


# --------------------------------------------------------------------------- server fixture
def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _connect(host: str, port: int, user: str, password: str, database: str = "master"):
    return pymssql.connect(server=host, port=port, user=user, password=password,
                           database=database, autocommit=True, login_timeout=5)


@pytest.fixture(scope="module")
def server():
    if os.environ.get("MSSQL_HOST"):
        yield (os.environ["MSSQL_HOST"], int(os.environ.get("MSSQL_PORT", "1433")),
               os.environ.get("MSSQL_USER", "sa"), os.environ["MSSQL_PASSWORD"])
        return
    if shutil.which("docker") is None or subprocess.run(["docker", "info"], check=False, capture_output=True).returncode:
        pytest.skip("Docker is not available")
    name, port = f"azsyn02-mssql-{uuid.uuid4().hex[:8]}", _free_port()
    run = subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", "ACCEPT_EULA=Y",
         "-e", f"MSSQL_SA_PASSWORD={SA_PASSWORD}", "-p", f"127.0.0.1:{port}:1433", IMAGE],
        check=False, capture_output=True, text=True)
    if run.returncode:
        pytest.skip(f"could not start SQL Server container: {run.stderr.strip()}")
    try:
        deadline = time.time() + 180
        while True:
            try:
                _connect("127.0.0.1", port, "sa", SA_PASSWORD).close()
                break
            except pymssql.Error:
                if time.time() > deadline:
                    raise
                time.sleep(2)
        yield ("127.0.0.1", port, "sa", SA_PASSWORD)
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


def _nullify(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or v is pd.NaT:
        return None
    return v


def _load_csv(cur, table: str, csv: str) -> int:
    df = pd.read_csv(SRC_DIR / csv, dtype=str, keep_default_na=False)
    df = df.replace({"": None})
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    rows = [tuple(_nullify(v) for v in r) for r in df.itertuples(index=False, name=None)]
    cur.executemany(f"INSERT INTO {table} ({', '.join(c.upper() for c in cols)}) VALUES ({placeholders})", rows)
    return len(rows)


@pytest.fixture(scope="module")
def db(server):
    host, port, user, password = server
    dbname = f"azsyn02_{uuid.uuid4().hex[:8]}"
    with _connect(host, port, user, password) as conn:
        conn.cursor().execute(f"CREATE DATABASE {dbname}")
    conn = _connect(host, port, user, password, dbname)
    cur = conn.cursor()
    for stmt in SETUP_DDL:
        cur.execute(stmt)
    for table, csv in SOURCES.items():
        _load_csv(cur, table, csv)
    cur.execute(synapse_to_sqlserver(PROC_FILE.read_text()))
    yield conn
    conn.close()
    with _connect(host, port, user, password) as c:
        c.cursor().execute(f"ALTER DATABASE {dbname} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {dbname}")


@pytest.fixture(scope="module")
def loaded(db):
    cur = db.cursor()
    cur.execute("EXEC stg.usp_load_stg_customer_360 @AsOfDate = %s", (AS_OF_DATE.isoformat(),))
    cur.execute("SELECT * FROM stg.STG_CUSTOMER_360")
    actual = pd.DataFrame(cur.fetchall(), columns=[d[0].lower() for d in cur.description])
    expected = pd.read_csv(EXPECTED_CSV, dtype=str, keep_default_na=False).replace({"": None})
    return db, actual, expected


# --------------------------------------------------------------------------- tests
def test_output_schema_matches_ddl(loaded) -> None:
    db, _, _ = loaded
    cur = db.cursor()
    cur.execute("""
        SELECT c.name,
               CASE WHEN t.name IN ('varchar','char') THEN t.name + '(' + CAST(c.max_length AS VARCHAR) + ')'
                    WHEN t.name = 'decimal' THEN 'decimal(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')'
                    WHEN t.name = 'datetime2' THEN 'datetime2(' + CAST(c.scale AS VARCHAR) + ')'
                    ELSE t.name END,
               c.is_nullable
        FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID('stg.STG_CUSTOMER_360') ORDER BY c.column_id""")
    rows = cur.fetchall()
    assert [(r[0], r[1]) for r in rows] == EXPECTED_SCHEMA
    assert rows[0][2] == 0, "CUSTOMER_ID must be NOT NULL"


def test_row_count_and_keys(loaded) -> None:
    _, actual, expected = loaded
    assert len(actual) == len(expected) == 478
    assert set(actual.customer_id.astype(int)) == set(expected.customer_id.astype(int))
    assert actual.customer_id.is_unique


def _merged(actual: pd.DataFrame, expected: pd.DataFrame) -> pd.DataFrame:
    a = actual.copy()
    a["customer_id"] = a.customer_id.astype(int)
    e = expected.copy()
    e["customer_id"] = e.customer_id.astype(int)
    return a.merge(e, on="customer_id", suffixes=("_act", "_exp"), validate="1:1")


def _norm(v) -> str | None:
    v = _nullify(v)
    if v is None:
        return None
    if isinstance(v, (dt.date, dt.datetime)):
        return v.isoformat()[:10]
    s = str(v)
    try:
        f = float(s)
        if f.is_integer():
            return str(int(f))
    except ValueError:
        pass
    return s


def test_exact_columns_match(loaded) -> None:
    _, actual, expected = loaded
    m = _merged(actual, expected)
    report = {}
    for col in EXACT_COLS:
        diff = m[m[f"{col}_act"].map(_norm) != m[f"{col}_exp"].map(_norm)]
        report[col] = len(diff)
    assert all(v == 0 for v in report.values()), report


def test_numeric_columns_within_tolerance(loaded) -> None:
    _, actual, expected = loaded
    m = _merged(actual, expected)
    for col in NUMERIC_COLS:
        delta = (m[f"{col}_act"].astype(float) - m[f"{col}_exp"].astype(float)).abs()
        assert delta.max() <= TOL, f"{col}: max |diff| {delta.max()}"


def test_age_and_tenure_follow_teradata_semantics(loaded) -> None:
    """AGE / TENURE_MONTHS reproduce Teradata day arithmetic and MONTHS_BETWEEN.

    The committed CSV was produced by a non-Teradata reimplementation that used
    calendar-boundary counts (year(asof) - year(dob) and DATEDIFF(MONTH)). Rows where
    the two semantics disagree are expected residual diffs; this test checks that the
    proc matches Teradata exactly and that every residual diff is explained by the
    boundary-count semantics of the CSV generator.
    """
    _, actual, expected = loaded
    m = _merged(actual, expected)
    dob = pd.to_datetime(m.date_of_birth_exp).dt.date
    since = pd.to_datetime(m.customer_since_exp).dt.date

    td_age = dob.map(lambda d: teradata_age(d, AS_OF_DATE))
    td_tenure = since.map(lambda d: math.trunc(teradata_months_between(AS_OF_DATE, d)))
    assert (m.age_act.astype(int) == td_age).all()
    assert (m.tenure_months_act.astype(int) == td_tenure).all()

    csv_age, csv_tenure = m.age_exp.astype(int), m.tenure_months_exp.astype(int)
    assert (csv_age == dob.map(lambda d: boundary_years(AS_OF_DATE, d))).all()
    assert (csv_tenure == since.map(lambda d: boundary_months(AS_OF_DATE, d))).all()

    age_diff = (m.age_act.astype(int) - csv_age).abs()
    tenure_diff = (m.tenure_months_act.astype(int) - csv_tenure).abs()
    assert age_diff.max() <= 1 and tenure_diff.max() <= 1
    print(f"\nAGE: {int((age_diff == 0).sum())}/{len(m)} exact vs CSV, {int((age_diff > 0).sum())} off by 1 "
          f"(calendar-year boundary in CSV generator)")
    print(f"TENURE_MONTHS: {int((tenure_diff == 0).sum())}/{len(m)} exact vs CSV, {int((tenure_diff > 0).sum())} "
          f"off by 1 (DATEDIFF(MONTH) boundary count in CSV generator)")


def test_load_ts_and_run_log(loaded) -> None:
    db, actual, _ = loaded
    assert actual.load_ts.nunique() == 1
    cur = db.cursor()
    cur.execute("SELECT JOB_NAME, STEP_NAME, STATUS, ROW_COUNT, START_TS, END_TS, MESSAGE "
                "FROM stg.ETL_RUN_LOG WHERE STATUS = 'SUCCESS'")
    rows = cur.fetchall()
    assert len(rows) == 1
    job, step, status, count, start_ts, end_ts, msg = rows[0]
    assert (job, step, status, count, msg) == ("01_stg_customer_360", "FULL_LOAD", "SUCCESS", 478, None)
    assert start_ts <= end_ts


def test_default_as_of_date_runs(loaded) -> None:
    db, _, _ = loaded
    db.cursor().execute("EXEC stg.usp_load_stg_customer_360")
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM stg.STG_CUSTOMER_360")
    assert cur.fetchone()[0] == 478


def test_zero_row_guard_logs_failure_and_throws(loaded) -> None:
    db, _, _ = loaded
    cur = db.cursor()
    cur.execute("SELECT * INTO #cust_backup FROM core.CUSTOMERS; DELETE FROM core.CUSTOMERS")
    try:
        with pytest.raises(pymssql.Error, match="STG_CUSTOMER_360 loaded 0 rows"):
            cur.execute("EXEC stg.usp_load_stg_customer_360 @AsOfDate = '2026-04-10'")
        cur.execute("SELECT STATUS, MESSAGE FROM stg.ETL_RUN_LOG WHERE STATUS = 'FAILED'")
        rows = cur.fetchall()
        assert len(rows) == 1 and "loaded 0 rows" in rows[0][1]
    finally:
        cur.execute("INSERT INTO core.CUSTOMERS SELECT * FROM #cust_backup; DROP TABLE #cust_backup")
