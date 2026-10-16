"""Parity test: stg.usp_load_stg_risk_factors (Synapse T-SQL) vs bteq/03_stg_risk_factors.bteq.

Runs the proc on SQL Server 2022 (Docker) against data/01_source_tables and compares
the result with data/02_bteq_staging/stg_risk_factors.csv at AS_OF_DATE 2026-04-10.

Connection:
  * AZSYN_MSSQL_HOST / AZSYN_MSSQL_PORT / AZSYN_MSSQL_USER / AZSYN_MSSQL_PASSWORD
    point the test at an existing SQL Server, otherwise
  * a throwaway mcr.microsoft.com/mssql/server:2022-latest container is started.
  The module is skipped if pymssql is not installed or Docker is unavailable.

Synapse -> SQL Server shim (synapse_to_sqlserver):
  SQL Server has no CTAS and no WITH (DISTRIBUTION = ..., HEAP | CLUSTERED COLUMNSTORE INDEX)
  table options, so every
      CREATE TABLE <t> WITH (...) AS <select>;
  is rewritten to
      SELECT * INTO <t> FROM (<select>) AS ctas_src;
  Nothing else in the proc is changed. Column names, order and types still come from
  the explicit CASTs in the CTAS select list, which is what Synapse would create.
"""
from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pymssql = pytest.importorskip("pymssql")

REPO = Path(__file__).resolve().parents[2]
PROC_FILE = REPO / "synapse" / "sql" / "procs" / "stg.usp_load_stg_risk_factors.sql"
STAGING_DDL = REPO / "ddl" / "01_staging_tables.sql"
SOURCE_DIR = REPO / "data" / "01_source_tables"
EXPECTED_CSV = REPO / "data" / "02_bteq_staging" / "stg_risk_factors.csv"

AS_OF_DATE = "2026-04-10"
EXPECTED_ROWS = 478
TOLERANCE = 0.01
TEST_DB = "AZSYN04_TEST"
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
WORK_TABLES = ("stg.WRK_DAILY_BALANCE", "stg.WRK_PAYMENT_HISTORY")

# Source DDL, mapped per the AZSYN contract (distribution/index options dropped for SQL Server).
SOURCE_DDL = {
    "core.CUSTOMERS": """
        CUSTOMER_ID BIGINT NOT NULL, FIRST_NAME VARCHAR(60), LAST_NAME VARCHAR(60),
        DATE_OF_BIRTH DATE, SSN_HASH CHAR(64), EMAIL VARCHAR(120), PHONE_PRIMARY VARCHAR(20),
        CUSTOMER_SINCE DATE, CUSTOMER_STATUS CHAR(1), SEGMENT_CODE VARCHAR(10), BRANCH_ID INT,
        CREATED_TS DATETIME2(6), UPDATED_TS DATETIME2(6)""",
    "core.ACCOUNTS": """
        ACCOUNT_ID BIGINT NOT NULL, CUSTOMER_ID BIGINT NOT NULL, ACCOUNT_TYPE VARCHAR(20),
        ACCOUNT_STATUS CHAR(1), OPEN_DATE DATE, CLOSE_DATE DATE, CURRENT_BALANCE DECIMAL(15,2),
        AVAILABLE_BALANCE DECIMAL(15,2), CREDIT_LIMIT DECIMAL(15,2), INTEREST_RATE DECIMAL(5,4),
        BRANCH_ID INT, CREATED_TS DATETIME2(6), UPDATED_TS DATETIME2(6)""",
    "core.CUSTOMER_BUREAU_SCORES": """
        CUSTOMER_ID BIGINT NOT NULL, EXTERNAL_CREDIT_SCORE SMALLINT, REPORT_DATE DATE""",
    "txn.TRANSACTIONS": """
        TRANSACTION_ID BIGINT NOT NULL, ACCOUNT_ID BIGINT NOT NULL, TRANSACTION_TYPE_CD VARCHAR(10),
        TRANSACTION_DATE DATE, TRANSACTION_TS DATETIME2(6), AMOUNT DECIMAL(15,2),
        RUNNING_BALANCE DECIMAL(15,2), MERCHANT_NAME VARCHAR(100), MERCHANT_CATEGORY VARCHAR(60),
        CHANNEL_CODE VARCHAR(10), REFERENCE_NUM VARCHAR(40), STATUS_CODE CHAR(1),
        CREATED_TS DATETIME2(6)""",
    "txn.TRANSACTION_TYPES": """
        TRANSACTION_TYPE_CD VARCHAR(10) NOT NULL, DESCRIPTION VARCHAR(60), CATEGORY VARCHAR(30),
        IS_REVENUE CHAR(1) DEFAULT 'N', EFFECTIVE_DATE DATE, EXPIRATION_DATE DATE""",
    "stg.ETL_RUN_LOG": """
        JOB_NAME VARCHAR(100) NOT NULL, STEP_NAME VARCHAR(100), STATUS VARCHAR(20) NOT NULL,
        ROW_COUNT BIGINT, START_TS DATETIME2(6), END_TS DATETIME2(6), MESSAGE VARCHAR(4000)""",
}
SOURCE_CSV = {
    "core.CUSTOMERS": "customers.csv",
    "core.ACCOUNTS": "accounts.csv",
    "core.CUSTOMER_BUREAU_SCORES": "customer_bureau_scores.csv",
    "txn.TRANSACTIONS": "transactions.csv",
    "txn.TRANSACTION_TYPES": "transaction_types.csv",
}


# --------------------------------------------------------------------------- shim
def _skip_ws_and_comments(sql: str, i: int) -> int:
    while i < len(sql):
        if sql[i].isspace():
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j + 1
        elif sql.startswith("/*", i):
            i = sql.index("*/", i + 2) + 2
        else:
            break
    return i


def _scan_until(sql: str, i: int, stop: str) -> int:
    """Index of the first `stop` char at paren depth 0 from i, skipping strings and comments."""
    depth = 0
    while i < len(sql):
        ch = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j < 0 else j + 1
            continue
        if sql.startswith("/*", i):
            i = sql.index("*/", i + 2) + 2
            continue
        if ch == "'":
            i += 1
            while True:
                j = sql.index("'", i)
                if sql.startswith("''", j):
                    i = j + 2
                    continue
                i = j + 1
                break
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            if depth == 0 and stop == ")":
                return i
            depth -= 1
        elif ch == stop and depth == 0:
            return i
        i += 1
    raise ValueError(f"unterminated construct, expected {stop!r}")


_CTAS_HEAD = re.compile(r"CREATE\s+TABLE\s+([\w\.\[\]]+)\s+WITH\s*\(", re.IGNORECASE)


def synapse_to_sqlserver(sql: str) -> str:
    """Rewrite Synapse CTAS (CREATE TABLE t WITH (...) AS select;) to SELECT * INTO."""
    out, pos = [], 0
    for m in _CTAS_HEAD.finditer(sql):
        if m.start() < pos:
            continue
        with_end = _scan_until(sql, m.end(), ")")
        j = _skip_ws_and_comments(sql, with_end + 1)
        if not re.match(r"AS\b", sql[j:], re.IGNORECASE):
            raise ValueError(f"CTAS for {m.group(1)} has no AS <select>")
        body_start = j + 2
        body_end = _scan_until(sql, body_start, ";")
        out.append(sql[pos:m.start()])
        out.append(f"SELECT * INTO {m.group(1)} FROM ({sql[body_start:body_end].strip()}\n) AS ctas_src")
        pos = body_end
    out.append(sql[pos:])
    rewritten = "".join(out)
    assert not re.search(r"\bDISTRIBUTION\s*=", rewritten, re.IGNORECASE), "unshimmed WITH (DISTRIBUTION)"
    return rewritten


def test_shim_rewrites_ctas():
    src = ("CREATE TABLE stg.X\nWITH (DISTRIBUTION = HASH(CUSTOMER_ID), HEAP)\nAS\n"
           "SELECT a, ';' AS s FROM (SELECT 1 AS a) d /* ; */ WHERE a IN (1, 2);\nSELECT 2;")
    got = synapse_to_sqlserver(src)
    assert got.startswith("SELECT * INTO stg.X FROM (SELECT a, ';' AS s FROM (SELECT 1 AS a) d")
    assert ") AS ctas_src;\nSELECT 2;" in got
    assert "HASH" not in got


# --------------------------------------------------------------------------- server
def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _connect(host, port, user, password, database="master", timeout_s=0):
    deadline = time.time() + timeout_s
    while True:
        try:
            return pymssql.connect(server=host, port=port, user=user, password=password,
                                   database=database, autocommit=True, login_timeout=5)
        except pymssql.Error:
            if time.time() >= deadline:
                raise
            time.sleep(2)


@pytest.fixture(scope="module")
def server():
    if os.environ.get("AZSYN_MSSQL_HOST"):
        yield dict(host=os.environ["AZSYN_MSSQL_HOST"],
                   port=int(os.environ.get("AZSYN_MSSQL_PORT", "1433")),
                   user=os.environ.get("AZSYN_MSSQL_USER", "sa"),
                   password=os.environ["AZSYN_MSSQL_PASSWORD"])
        return
    if shutil.which("docker") is None or subprocess.run(
            ["docker", "info"], capture_output=True).returncode != 0:
        pytest.skip("Docker is not available and AZSYN_MSSQL_HOST is not set")
    name = f"azsyn04-pytest-{uuid.uuid4().hex[:8]}"
    password = f"Az{uuid.uuid4().hex[:12]}!9"
    port = _free_port()
    run = subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", name, "-e", "ACCEPT_EULA=Y",
         "-e", f"MSSQL_SA_PASSWORD={password}", "-p", f"127.0.0.1:{port}:1433", MSSQL_IMAGE],
        capture_output=True, text=True)
    if run.returncode != 0:
        pytest.skip(f"could not start {MSSQL_IMAGE}: {run.stderr.strip()}")
    try:
        _connect("127.0.0.1", port, "sa", password, timeout_s=180).close()
        yield dict(host="127.0.0.1", port=port, user="sa", password=password)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _sql_literal(v) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def _load_csv(cur, table: str, csv_path: Path, batch: int = 1000) -> int:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df = df.replace({"": None})
    cols = ", ".join(c.upper() for c in df.columns)
    rows = df.itertuples(index=False, name=None)
    n = 0
    chunk: list[str] = []
    for row in rows:
        chunk.append("(" + ", ".join(_sql_literal(v) for v in row) + ")")
        if len(chunk) == batch:
            cur.execute(f"INSERT INTO {table} ({cols}) VALUES " + ", ".join(chunk))
            n += len(chunk)
            chunk = []
    if chunk:
        cur.execute(f"INSERT INTO {table} ({cols}) VALUES " + ", ".join(chunk))
        n += len(chunk)
    return n


@pytest.fixture(scope="module")
def conn(server):
    master = _connect(**server)
    mcur = master.cursor()
    mcur.execute(f"IF DB_ID('{TEST_DB}') IS NOT NULL BEGIN "
                 f"ALTER DATABASE {TEST_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                 f"DROP DATABASE {TEST_DB}; END")
    mcur.execute(f"CREATE DATABASE {TEST_DB}")
    db = _connect(**server, database=TEST_DB)
    cur = db.cursor()
    for schema in ("core", "txn", "stg"):
        cur.execute(f"EXEC('CREATE SCHEMA {schema}')")
    for table, cols in SOURCE_DDL.items():
        cur.execute(f"CREATE TABLE {table} ({cols})")
    for table, fname in SOURCE_CSV.items():
        _load_csv(cur, table, SOURCE_DIR / fname)
    cur.execute(synapse_to_sqlserver(PROC_FILE.read_text()))
    yield db
    db.close()
    mcur.execute(f"ALTER DATABASE {TEST_DB} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                 f"DROP DATABASE {TEST_DB}")
    master.close()


def _run_proc(conn, as_of=AS_OF_DATE):
    cur = conn.cursor()
    cur.execute("EXEC stg.usp_load_stg_risk_factors @AsOfDate = %s", (as_of,))
    while cur.nextset():
        pass


def _query_df(conn, sql: str):
    cur = conn.cursor(as_dict=True)
    cur.execute(sql)
    return pd.DataFrame(cur.fetchall())


def _object_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT OBJECT_ID(%s)", (name,))
    return cur.fetchone()[0] is not None


# --------------------------------------------------------------------------- tests
def test_source_rows_loaded(conn):
    counts = _query_df(conn, """
        SELECT 'customers' t, COUNT(*) n FROM core.CUSTOMERS UNION ALL
        SELECT 'accounts', COUNT(*) FROM core.ACCOUNTS UNION ALL
        SELECT 'transactions', COUNT(*) FROM txn.TRANSACTIONS UNION ALL
        SELECT 'transaction_types', COUNT(*) FROM txn.TRANSACTION_TYPES UNION ALL
        SELECT 'bureau', COUNT(*) FROM core.CUSTOMER_BUREAU_SCORES""").set_index("t")["n"]
    assert counts.to_dict() == {"customers": 500, "accounts": 1251, "transactions": 80528,
                                "transaction_types": 8, "bureau": 500}


# Columns where data/02_bteq_staging/stg_risk_factors.csv does not follow the BTEQ.
# The CSV came from a DuckDB reimplementation (export_data.py -> local/duckdb/run_demo.py,
# not in the repo). The proc follows the BTEQ and the contract; these tests prove that
# each gap is fully explained by the generator's semantics (REFERENCE_SEMANTICS_SQL
# reproduces the CSV column exactly from the same source rows).
REFERENCE_SEMANTICS_SQL = {
    # BTEQ: CAST(MONTHS_BETWEEN(CURRENT_DATE, COALESCE(last_late, OPEN_DATE)) AS INTEGER),
    # Teradata semantics (fraction over a 31-day month, truncated). The CSV uses a month
    # boundary count (DuckDB date_diff('month') == T-SQL DATEDIFF(MONTH)), which is one
    # higher whenever DAY(as_of) < DAY(OPEN_DATE). No payment is ever late, so
    # last_late is always OPEN_DATE.
    "months_since_last_late": """
        SELECT c.CUSTOMER_ID, COALESCE(m.V, 999) AS V
        FROM core.CUSTOMERS c
        LEFT JOIN (
            SELECT acct.CUSTOMER_ID, MIN(DATEDIFF(MONTH, acct.OPEN_DATE, @AsOf)) AS V
            FROM txn.TRANSACTIONS t
            JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            JOIN txn.TRANSACTION_TYPES tt ON t.TRANSACTION_TYPE_CD = tt.TRANSACTION_TYPE_CD
            WHERE acct.ACCOUNT_TYPE IN ('CREDIT', 'LOAN') AND tt.CATEGORY = 'CREDIT'
              AND t.STATUS_CODE = 'P' AND t.TRANSACTION_DATE >= DATEADD(MONTH, -24, @AsOf)
            GROUP BY acct.CUSTOMER_ID) m ON m.CUSTOMER_ID = c.CUSTOMER_ID
        WHERE c.CUSTOMER_STATUS IN ('A', 'I')""",
    # BTEQ: distinct merchants in the last 30 days that the account had NOT used before
    # as_of - 30 (correlated NOT IN on TRANSACTIONS t2). The CSV skips that exclusion and
    # counts every distinct merchant in the last 30 days.
    "new_merchant_cnt_30d": """
        SELECT c.CUSTOMER_ID, COALESCE(m.V, 0) AS V
        FROM core.CUSTOMERS c
        LEFT JOIN (
            SELECT acct.CUSTOMER_ID, COUNT(DISTINCT t.MERCHANT_NAME) AS V
            FROM txn.TRANSACTIONS t
            JOIN core.ACCOUNTS acct ON t.ACCOUNT_ID = acct.ACCOUNT_ID
            WHERE t.STATUS_CODE = 'P' AND t.TRANSACTION_DATE >= DATEADD(DAY, -30, @AsOf)
            GROUP BY acct.CUSTOMER_ID) m ON m.CUSTOMER_ID = c.CUSTOMER_ID
        WHERE c.CUSTOMER_STATUS IN ('A', 'I')""",
}


@pytest.fixture(scope="module")
def parity(conn):
    _run_proc(conn)
    got = _query_df(conn, "SELECT * FROM stg.STG_RISK_FACTORS")
    got.columns = [c.lower() for c in got.columns]
    exp = pd.read_csv(EXPECTED_CSV)
    merged = exp.merge(got, on="customer_id", how="outer", suffixes=("_exp", "_got"),
                       indicator=True)
    return got, exp, merged


def _diff(merged, col):
    return (merged[f"{col}_exp"].astype(float) - merged[f"{col}_got"].astype(float)).abs()


def test_parity_row_count_and_keys(parity):
    got, exp, merged = parity
    assert len(got) == EXPECTED_ROWS == len(exp)
    assert got.customer_id.is_unique
    assert set(got.customer_id) == set(exp.customer_id)
    assert (merged["_merge"] == "both").all()
    assert got.load_ts.notna().all()


def test_parity_numeric_columns(parity):
    _, exp, merged = parity
    numeric_cols = [c for c in exp.columns if c not in ("customer_id", "load_ts")]
    report, failures = [], []
    for col in numeric_cols:
        diff = _diff(merged, col)
        n_bad = int((diff > TOLERANCE).sum())
        tag = " (known generator deviation)" if col in REFERENCE_SEMANTICS_SQL else ""
        report.append(f"{col:<24} max_abs_diff={diff.max():.6f} "
                      f"rows_over_tol={n_bad}/{len(merged)}{tag}")
        if n_bad and col not in REFERENCE_SEMANTICS_SQL:
            failures.append(col)
    print(f"\nSTG_RISK_FACTORS parity @ {AS_OF_DATE}, rows={len(merged)}")
    print("\n".join(report))
    assert not failures, "columns outside tolerance: " + ", ".join(failures)


@pytest.mark.parametrize("col", sorted(REFERENCE_SEMANTICS_SQL))
def test_known_deviation_is_fully_explained(conn, parity, col):
    _, exp, merged = parity
    ref = _query_df(conn, f"DECLARE @AsOf DATE = '{AS_OF_DATE}'; {REFERENCE_SEMANTICS_SQL[col]}")
    ref.columns = ["customer_id", "ref"]
    chk = merged.merge(ref, on="customer_id")
    assert len(chk) == EXPECTED_ROWS
    assert (chk[f"{col}_exp"].astype(float) == chk["ref"].astype(float)).all(), \
        f"CSV {col} is not explained by the reference-generator semantics"
    gap = chk[f"{col}_exp"].astype(float) - chk[f"{col}_got"].astype(float)
    if col == "months_since_last_late":
        assert gap.isin([0, 1]).all()
    else:
        assert (gap >= 0).all()
    print(f"\n{col}: CSV == reference-generator semantics on {len(chk)}/{len(chk)} rows; "
          f"proc == CSV on {int((gap == 0).sum())}/{len(chk)} rows")


def _ddl_columns(table: str) -> list[tuple[str, str]]:
    text = STAGING_DDL.read_text()
    block = re.search(rf"CREATE\s+MULTISET\s+TABLE\s+ETL_STAGING_DB\.{table}\b.*?\((.*?)\n\)",
                      text, re.S | re.I).group(1)
    cols = []
    for line in block.splitlines():
        m = re.match(r"\s*(\w+)\s+([A-Z]+(?:\(\d+(?:,\d+)?\))?)", line)
        if m:
            cols.append((m.group(1), m.group(2)))
    return cols


def _td_to_sqlserver(td_type: str) -> str:
    mapping = {"INTEGER": "int", "BIGINT": "bigint", "SMALLINT": "smallint",
               "BYTEINT": "tinyint", "DATE": "date", "TIMESTAMP(6)": "datetime2(6)"}
    if td_type in mapping:
        return mapping[td_type]
    m = re.match(r"DECIMAL\((\d+),(\d+)\)", td_type)
    assert m, td_type
    return f"decimal({m.group(1)},{m.group(2)})"


def test_output_columns_match_ddl(conn):
    _run_proc(conn)
    cols = _query_df(conn, """
        SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = 'STG_RISK_FACTORS'
        ORDER BY ORDINAL_POSITION""")

    def render(r):
        if r.DATA_TYPE == "decimal":
            return f"decimal({int(r.NUMERIC_PRECISION)},{int(r.NUMERIC_SCALE)})"
        if r.DATA_TYPE == "datetime2":
            return f"datetime2({int(r.DATETIME_PRECISION)})"
        return r.DATA_TYPE

    got = [(r.COLUMN_NAME, render(r)) for r in cols.itertuples()]
    expected = [(name, _td_to_sqlserver(t)) for name, t in _ddl_columns("STG_RISK_FACTORS")]
    assert got == expected


def test_success_drops_work_tables_and_logs(conn):
    conn.cursor().execute("DELETE FROM stg.ETL_RUN_LOG")
    _run_proc(conn)
    for t in WORK_TABLES:
        assert not _object_exists(conn, t), f"{t} not dropped"
    log = _query_df(conn, "SELECT * FROM stg.ETL_RUN_LOG")
    assert len(log) == 1
    row = log.iloc[0]
    assert (row.JOB_NAME, row.STEP_NAME, row.STATUS, int(row.ROW_COUNT)) == \
        ("03_stg_risk_factors", "FULL_LOAD", "SUCCESS", EXPECTED_ROWS)
    assert row.START_TS <= row.END_TS


def test_failure_drops_work_tables_and_logs_failed(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM stg.ETL_RUN_LOG")
    cur.execute("SELECT CUSTOMER_ID, CUSTOMER_STATUS INTO #status_backup FROM core.CUSTOMERS")
    cur.execute("UPDATE core.CUSTOMERS SET CUSTOMER_STATUS = 'C'")
    try:
        with pytest.raises(pymssql.Error, match="STG_RISK_FACTORS loaded 0 rows"):
            _run_proc(conn)
    finally:
        cur.execute("UPDATE c SET CUSTOMER_STATUS = b.CUSTOMER_STATUS FROM core.CUSTOMERS c "
                    "JOIN #status_backup b ON b.CUSTOMER_ID = c.CUSTOMER_ID")
        cur.execute("DROP TABLE #status_backup")
    for t in WORK_TABLES:
        assert not _object_exists(conn, t), f"{t} not dropped after failure"
    log = _query_df(conn, "SELECT * FROM stg.ETL_RUN_LOG")
    assert len(log) == 1
    assert log.iloc[0].STATUS == "FAILED"
    assert "loaded 0 rows" in log.iloc[0].MESSAGE
