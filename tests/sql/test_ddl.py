"""AZSYN-01: Synapse DDL + source data load tests.

Static checks (no Docker):
  * every Synapse-only clause the contract requires (DISTRIBUTION / CCI / HEAP /
    CLUSTERED INDEX / PARTITION / CREATE STATISTICS) is present in the committed DDL;
  * Synapse column names, order, types, NOT NULL and DEFAULTs match ddl/*.sql
    (parsed Teradata DDL + contract type mapping) and the two contract-frozen tables;
  * COPY INTO column lists match the DDL and the CSV headers in data/01_source_tables.

Live checks (SQL Server 2022 in Docker, skipped cleanly if Docker is unavailable):
  * the 4 DDL files apply in order (and re-apply idempotently) through the shim;
  * copy_into_source_tables.sql runs through the shim and loads every CSV with the
    expected row counts, null counts, numeric totals and date/timestamp ranges.

Run: pytest tests/sql/test_ddl.py
Env: AZSYN_MSSQL_CONTAINER (default azsyn-mssql-test), AZSYN_MSSQL_PORT (default 14333),
     AZSYN_KEEP_MSSQL=1 keeps the container after the run.

SQL Server shim (SQL Server lacks Synapse dedicated-pool syntax):
  1. Scripts are split into batches on GO lines (as sqlcmd/SSMS/Synapse Studio do).
  2. CREATE TABLE ... WITH (DISTRIBUTION = ..., <index>, PARTITION (...)): the WITH
     clause is removed. Distribution has no SQL Server equivalent and is dropped.
     The index option is emulated so it is still exercised: CLUSTERED COLUMNSTORE
     INDEX -> CREATE CLUSTERED COLUMNSTORE INDEX, CLUSTERED INDEX (c) -> CREATE
     CLUSTERED INDEX, HEAP -> nothing. PARTITION (c RANGE RIGHT FOR VALUES (...)) ->
     CREATE PARTITION FUNCTION/SCHEME with the same boundaries + ON scheme(c).
  3. CTAS (CREATE TABLE t WITH (...) AS SELECT ...) -> SELECT * INTO t FROM (SELECT ...) s.
     Not used by these files; kept for parity with the other AZSYN units.
  4. COPY INTO t (cols) FROM '<adls>/x.csv' WITH (<opts>) -> BULK INSERT t FROM
     '<container path>/x.csv' WITH (<mapped opts>), with @SourceRoot pointed at the CSVs
     copied into the container. Option mapping: FILE_TYPE='CSV' -> FORMAT='CSV',
     FIRSTROW/FIELDTERMINATOR/ROWTERMINATOR/FIELDQUOTE/MAXERRORS kept, ENCODING='UTF8'
     dropped (CODEPAGE is unsupported on Linux; the CSVs are asserted pure ASCII, where
     UTF-8 and the Latin1 code page agree), DATEFORMAT='ymd' -> SET DATEFORMAT ymd, CREDENTIAL dropped,
     KEEPNULLS added (COPY INTO loads empty fields as NULL). BULK INSERT maps fields by
     position, so the test asserts the COPY INTO field numbers are exactly 1..n in
     table-column order.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import re
import shutil
import subprocess
import time
from decimal import Decimal
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LEGACY_DDL = [REPO / "ddl" / f for f in ("00_source_tables.sql", "01_staging_tables.sql", "02_data_product_tables.sql")]
SYN_DDL_DIR = REPO / "synapse" / "sql" / "ddl"
SYN_DDL = [
    SYN_DDL_DIR / f
    for f in ("00_schemas.sql", "01_source_tables.sql", "02_staging_tables.sql", "03_data_product_tables.sql")
]
COPY_SCRIPT = REPO / "synapse" / "sql" / "load" / "copy_into_source_tables.sql"
CSV_DIR = REPO / "data" / "01_source_tables"

SCHEMA_MAP = {"CORE_BANKING_DB": "core", "TXN_PROCESSING_DB": "txn", "ETL_STAGING_DB": "stg", "DATA_PRODUCTS_DB": "dp"}

# Contract-frozen tables with no legacy DDL: (column, sql type, not null, default)
FROZEN_TABLES = {
    "core.CUSTOMER_BUREAU_SCORES": [
        ("CUSTOMER_ID", "BIGINT", True, None),
        ("EXTERNAL_CREDIT_SCORE", "SMALLINT", False, None),
        ("REPORT_DATE", "DATE", False, None),
    ],
    "stg.ETL_RUN_LOG": [
        ("JOB_NAME", "VARCHAR(100)", True, None),
        ("STEP_NAME", "VARCHAR(100)", False, None),
        ("STATUS", "VARCHAR(20)", True, None),
        ("ROW_COUNT", "BIGINT", False, None),
        ("START_TS", "DATETIME2(6)", False, None),
        ("END_TS", "DATETIME2(6)", False, None),
        ("MESSAGE", "VARCHAR(4000)", False, None),
    ],
}

SOURCE_FILES = {
    "core.CUSTOMERS": "customers.csv",
    "core.ACCOUNTS": "accounts.csv",
    "core.ADDRESSES": "addresses.csv",
    "core.CUSTOMER_BUREAU_SCORES": "customer_bureau_scores.csv",
    "txn.TRANSACTIONS": "transactions.csv",
    "txn.TRANSACTION_TYPES": "transaction_types.csv",
}
EXPECTED_ROWS = {
    "core.CUSTOMERS": 500,
    "core.ACCOUNTS": 1251,
    "core.ADDRESSES": 1000,
    "txn.TRANSACTIONS": 80528,
    "txn.TRANSACTION_TYPES": 8,
    "core.CUSTOMER_BUREAU_SCORES": 500,
}

MONTHLY_BOUNDARIES = [dt.date(y, m, 1) for y in range(2020, 2031) for m in range(1, 13)]  # 2020-01-01 .. 2030-12-01
HASH_CUST_CCI = {"distribution": "HASH(CUSTOMER_ID)", "index": "CLUSTERED COLUMNSTORE INDEX", "partition": None}
EXPECTED_OPTIONS = {
    "core.CUSTOMERS": HASH_CUST_CCI,
    "core.ACCOUNTS": HASH_CUST_CCI,
    "core.ADDRESSES": HASH_CUST_CCI,
    "core.CUSTOMER_BUREAU_SCORES": HASH_CUST_CCI,
    "txn.TRANSACTIONS": {
        "distribution": "HASH(ACCOUNT_ID)",
        "index": "CLUSTERED COLUMNSTORE INDEX",
        "partition": ("TRANSACTION_DATE", "RIGHT", MONTHLY_BOUNDARIES),
    },
    "txn.TRANSACTION_TYPES": {
        "distribution": "REPLICATE",
        "index": "CLUSTERED INDEX (TRANSACTION_TYPE_CD)",
        "partition": None,
    },
    "stg.STG_CUSTOMER_360": HASH_CUST_CCI,
    "stg.STG_TXN_SUMMARY": HASH_CUST_CCI,
    "stg.STG_RISK_FACTORS": HASH_CUST_CCI,
    "stg.ETL_RUN_LOG": {"distribution": "ROUND_ROBIN", "index": "HEAP", "partition": None},
    "dp.CUSTOMER_SEGMENTS": HASH_CUST_CCI,
    "dp.TRANSACTION_ANALYTICS": HASH_CUST_CCI,
    "dp.CUSTOMER_RISK_SCORES": HASH_CUST_CCI,
    "dp.CUSTOMER_MASTER_PROFILE": HASH_CUST_CCI,
}


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def _strip_comments(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _balanced(text: str, start: int) -> int:
    """Index just past the ')' matching the '(' at text[start]."""
    assert text[start] == "("
    depth = 0
    in_str = False
    for i in range(start, len(text)):
        ch = text[i]
        if ch == "'":
            in_str = not in_str
        elif not in_str and ch == "(":
            depth += 1
        elif not in_str and ch == ")":
            depth -= 1
            if depth == 0:
                return i + 1
    raise ValueError("unbalanced parentheses")


def _split_top_level(text: str, sep: str = ",") -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur).strip())
    return parts


_TYPE_RE = r"(BIGINT|INTEGER|INT|SMALLINT|BYTEINT|TINYINT|DATETIME2\(\d+\)|DATE|TIMESTAMP\(\d+\)|DECIMAL\(\d+,\d+\)|CHAR\(\d+\)|VARCHAR\(\d+\))"
_COL_RE = re.compile(r"^\s*([A-Z][A-Z0-9_]*)\s+" + _TYPE_RE + r"(.*)$")


def _parse_columns(block: str) -> list[tuple[str, str, bool, str | None]]:
    cols = []
    for line in _strip_comments(block).splitlines():
        m = _COL_RE.match(line)
        if not m:
            continue
        name, typ, rest = m.group(1), m.group(2), m.group(3)
        default = re.search(r"DEFAULT\s+'([^']*)'", rest)
        cols.append((name, typ, bool(re.search(r"\bNOT\s+NULL\b", rest)), default.group(1) if default else None))
    return cols


def parse_teradata_ddl() -> dict[str, list[tuple[str, str, bool, str | None]]]:
    """{'core.CUSTOMERS': [(col, teradata_type, not_null, default), ...]} from ddl/*.sql."""
    tables = {}
    for path in LEGACY_DDL:
        text = path.read_text()
        for m in re.finditer(r"CREATE\s+MULTISET\s+TABLE\s+(\w+)\.(\w+)[^(]*", text):
            start = text.index("(", m.end() - 1)
            block = text[start + 1 : _balanced(text, start) - 1]
            tables[f"{SCHEMA_MAP[m.group(1)]}.{m.group(2)}"] = _parse_columns(block)
    return tables


def parse_teradata_stats() -> set[tuple[str, tuple[str, ...]]]:
    stats = set()
    for path in LEGACY_DDL:
        for m in re.finditer(r"COLLECT\s+STATISTICS\s+COLUMN\s*\(([^)]*)\)\s*ON\s+(\w+)\.(\w+)", path.read_text()):
            cols = tuple(c.strip() for c in m.group(1).split(","))
            stats.add((f"{SCHEMA_MAP[m.group(2)]}.{m.group(3)}", cols))
    return stats


def map_teradata_type(td: str) -> str:
    """Contract v1 type mapping (Teradata -> Synapse T-SQL)."""
    if td == "INTEGER":
        return "INT"
    if td == "BYTEINT":
        return "TINYINT"
    m = re.fullmatch(r"TIMESTAMP\((\d+)\)", td)
    if m:
        return f"DATETIME2({m.group(1)})"
    return td  # BIGINT, SMALLINT, DATE, DECIMAL(p,s), CHAR(n), VARCHAR(n)


def parse_create_tables(sql: str) -> dict[str, dict]:
    """Parse Synapse CREATE TABLE statements: columns + WITH table options."""
    out = {}
    text = _strip_comments(sql)
    for m in re.finditer(r"CREATE\s+TABLE\s+(\w+\.\w+)\s*", text):
        start = m.end()
        end = _balanced(text, start)
        cols = _parse_columns(text[start + 1 : end - 1])
        opts = {"distribution": None, "index": None, "partition": None}
        w = re.match(r"\s*WITH\s*", text[end:])
        if w:
            ostart = end + w.end()
            oend = _balanced(text, ostart)
            for opt in _split_top_level(text[ostart + 1 : oend - 1]):
                norm = " ".join(opt.split())
                if norm.upper().startswith("DISTRIBUTION"):
                    opts["distribution"] = norm.split("=", 1)[1].strip().replace(" ", "")
                elif norm.upper().startswith("PARTITION"):
                    pm = re.match(
                        r"PARTITION\s*\(\s*(\w+)\s+RANGE\s+(LEFT|RIGHT)\s+FOR\s+VALUES\s*\((.*)\)\s*\)$",
                        norm,
                        re.IGNORECASE,
                    )
                    assert pm, f"unparseable partition clause: {norm}"
                    values = [dt.date.fromisoformat(v.strip().strip("'")) for v in pm.group(3).split(",")]
                    opts["partition"] = (pm.group(1), pm.group(2).upper(), values)
                else:
                    opts["index"] = re.sub(r"\s*\(\s*", " (", norm).replace(" )", ")")
        out[m.group(1)] = {"columns": cols, "options": opts}
    return out


def parse_synapse_stats(sql: str) -> set[tuple[str, str, tuple[str, ...]]]:
    return {
        (m.group(2), m.group(1), tuple(c.strip() for c in m.group(3).split(",")))
        for m in re.finditer(r"CREATE\s+STATISTICS\s+(\w+)\s+ON\s+(\w+\.\w+)\s*\(([^)]*)\)", _strip_comments(sql))
    }


def all_synapse_tables() -> dict[str, dict]:
    tables = {}
    for path in SYN_DDL:
        tables.update(parse_create_tables(path.read_text()))
    return tables


def parse_copy_into(sql: str) -> dict[str, dict]:
    """{'core.CUSTOMERS': {'columns': [(name, field_no)], 'file': 'customers.csv'}} from the load script."""
    out = {}
    for m in re.finditer(
        r"COPY\s+INTO\s+(\w+\.\w+)\s*\((.*?)\)\s*FROM\s+'''\s*\+\s*@SourceRoot\s*\+\s*N'/([\w.]+)''", sql, re.DOTALL
    ):
        cols = []
        for item in m.group(2).split(","):
            name, num = item.split()
            cols.append((name, int(num)))
        out[m.group(1)] = {"columns": cols, "file": m.group(3)}
    return out


def parse_copy_options(sql: str) -> dict[str, str]:
    m = re.search(r"DECLARE\s+@CopyOptions\s+NVARCHAR\(\d+\)\s*=\s*N'(.*?)'\s*\+\s*@Credential;", sql, re.DOTALL)
    assert m, "@CopyOptions declaration not found"
    opts = {}
    for item in m.group(1).split(",\n"):
        item = item.strip().rstrip(",")
        if not item:
            continue
        k, v = item.split("=", 1)
        opts[k.strip().upper()] = v.strip().replace("''", "'").strip("'")
    return opts


def csv_header(name: str) -> list[str]:
    with open(CSV_DIR / name, newline="") as f:
        return next(csv.reader(f))


def csv_rows(name: str) -> list[dict[str, str]]:
    with open(CSV_DIR / name, newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# SQL Server shim
# ---------------------------------------------------------------------------
def split_batches(sql: str) -> list[str]:
    return [
        b.strip()
        for b in re.split(r"^\s*GO\s*;?\s*$", sql, flags=re.MULTILINE | re.IGNORECASE)
        if _strip_comments(b).strip()
    ]


def ctas_to_select_into(sql: str) -> str:
    """CREATE TABLE t WITH (...) AS <select> -> SELECT * INTO t FROM (<select>) AS ctas_src."""
    m = re.search(r"CREATE\s+TABLE\s+([\w.\[\]#]+)\s+WITH\s*(?=\()", sql, re.IGNORECASE)
    if not m:
        return sql
    end = _balanced(sql, m.end())
    a = re.match(r"\s*AS\s+", sql[end:], re.IGNORECASE)
    if not a:
        return sql
    body = sql[end + a.end() :].strip().rstrip(";")
    if body.startswith("(") and _balanced(body, 0) == len(body):
        body = body[1:-1].strip()
    return f"{sql[: m.start()]}SELECT * INTO {m.group(1)} FROM ({body}) AS ctas_src;"


def shim_create_table(batch: str) -> list[str]:
    """Rewrite one Synapse CREATE TABLE batch into SQL Server batches (see module docstring)."""
    m = re.search(r"CREATE\s+TABLE\s+(\w+)\.(\w+)\s*", batch)
    if not m:
        return [batch]
    schema, table = m.group(1), m.group(2)
    fq = f"{schema}.{table}"
    col_end = _balanced(batch, m.end())
    w = re.match(r"\s*WITH\s*", batch[col_end:])
    if not w:
        return [batch]
    ostart = col_end + w.end()
    oend = _balanced(batch, ostart)
    opts = parse_create_tables(batch)[fq]["options"]
    cols = {c[0]: c[1] for c in parse_create_tables(batch)[fq]["columns"]}

    pre, on_clause, post = [], "", []
    if opts["partition"]:
        pcol, side, values = opts["partition"]
        pf, ps = f"pf_{table}", f"ps_{table}"
        vals = ", ".join(f"'{v.isoformat()}'" for v in values)
        pre.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{pf}') "
            f"CREATE PARTITION FUNCTION {pf} ({cols[pcol]}) AS RANGE {side} FOR VALUES ({vals});"
        )
        pre.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{ps}') "
            f"CREATE PARTITION SCHEME {ps} AS PARTITION {pf} ALL TO ([PRIMARY]);"
        )
        on_clause = f" ON {ps}({pcol})"
    has_ci = f"EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'{fq}') AND index_id = 1)"
    idx = opts["index"] or ""
    if idx == "CLUSTERED COLUMNSTORE INDEX":
        post.append(f"IF NOT {has_ci} CREATE CLUSTERED COLUMNSTORE INDEX cci_{table} ON {fq}{on_clause};")
    elif idx.startswith("CLUSTERED INDEX"):
        icols = idx[len("CLUSTERED INDEX") :].strip()
        post.append(f"IF NOT {has_ci} CREATE CLUSTERED INDEX ci_{table} ON {fq} {icols}{on_clause};")
    create = batch[:col_end] + on_clause + batch[oend:]
    return pre + [create] + post


def shim_ddl(sql: str) -> list[str]:
    out = []
    for batch in split_batches(sql):
        batch = ctas_to_select_into(batch)
        out.extend(shim_create_table(batch))
    return out


def shim_copy_script(sql: str, source_root: str) -> list[str]:
    """Rewrite the COPY INTO load script into a SQL Server batch using BULK INSERT."""
    opts = parse_copy_options(sql)
    assert opts["FILE_TYPE"] == "CSV" and opts["ENCODING"] == "UTF8" and opts["DATEFORMAT"] == "ymd"
    mapping = [
        "FORMAT = ''CSV''",
        f"FIRSTROW = {opts['FIRSTROW']}",
        f"FIELDTERMINATOR = ''{opts['FIELDTERMINATOR']}''",
        f"ROWTERMINATOR = ''{opts['ROWTERMINATOR']}''",
        f"FIELDQUOTE = ''{opts['FIELDQUOTE']}''",
        f"MAXERRORS = {opts['MAXERRORS']}",
        "KEEPNULLS",
        "TABLOCK",
    ]
    bulk_opts = ", ".join(mapping)
    sql, n = re.subn(
        r"N'COPY\s+INTO\s+(\w+\.\w+)\s*\(.*?\)\s*FROM\s+'''\s*\+\s*@SourceRoot\s*\+\s*N'/([\w.]+)''\s*WITH\s*\('\s*\+\s*@CopyOptions\s*\+\s*N'\);'",
        lambda m: f"N'BULK INSERT {m.group(1)} FROM ''' + @SourceRoot + N'/{m.group(2)}'' WITH ({bulk_opts});'",
        sql,
        flags=re.DOTALL,
    )
    assert n == len(SOURCE_FILES), f"expected {len(SOURCE_FILES)} COPY INTO statements, rewrote {n}"
    sql, n = re.subn(
        r"DECLARE\s+@SourceRoot\s+NVARCHAR\(\d+\)\s*=.*?;",
        f"DECLARE @SourceRoot NVARCHAR(1000) = N'{source_root}';",
        sql,
        count=1,
        flags=re.DOTALL,
    )
    assert n == 1
    return ["SET DATEFORMAT ymd;\n" + sql]


# ---------------------------------------------------------------------------
# Static tests (no Docker)
# ---------------------------------------------------------------------------
def test_shim_ctas_rewrite():
    ctas = "CREATE TABLE stg.X\nWITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)\nAS SELECT a, b FROM t WHERE c = 1;"
    assert ctas_to_select_into(ctas) == "SELECT * INTO stg.X FROM (SELECT a, b FROM t WHERE c = 1) AS ctas_src;"
    plain = "CREATE TABLE stg.X (A INT) WITH (HEAP);"
    assert ctas_to_select_into(plain) == plain


def test_all_tables_translated():
    td = set(parse_teradata_ddl())
    syn = set(all_synapse_tables())
    assert len(td) == 12
    assert syn == td | set(FROZEN_TABLES), (
        f"missing={td | set(FROZEN_TABLES) - syn} extra={syn - td - set(FROZEN_TABLES)}"
    )


def test_synapse_only_clauses_present():
    tables = all_synapse_tables()
    for fq, expected in EXPECTED_OPTIONS.items():
        assert tables[fq]["options"] == expected, f"{fq}: {tables[fq]['options']} != {expected}"
    part = tables["txn.TRANSACTIONS"]["options"]["partition"]
    assert len(part[2]) == 132 and part[2][0] == dt.date(2020, 1, 1) and part[2][-1] == dt.date(2030, 12, 1)


def test_columns_match_teradata_ddl():
    syn = all_synapse_tables()
    for fq, td_cols in parse_teradata_ddl().items():
        expected = [(n, map_teradata_type(t), nn, d) for n, t, nn, d in td_cols]
        assert syn[fq]["columns"] == expected, fq
    for fq, cols in FROZEN_TABLES.items():
        assert syn[fq]["columns"] == cols, fq


def test_statistics_match_collect_statistics():
    synapse = set()
    for path in SYN_DDL:
        synapse |= parse_synapse_stats(path.read_text())
    expected = {(fq, f"st_{fq.split('.')[1]}_{'_'.join(cols)}", cols) for fq, cols in parse_teradata_stats()}
    assert len(expected) == 11
    assert synapse == expected


def test_copy_into_matches_ddl_and_csv_headers():
    script = COPY_SCRIPT.read_text()
    copies = parse_copy_into(script)
    assert {fq: c["file"] for fq, c in copies.items()} == SOURCE_FILES
    syn = all_synapse_tables()
    for fq, spec in copies.items():
        ddl_cols = [c[0] for c in syn[fq]["columns"]]
        names = [c[0] for c in spec["columns"]]
        assert names == ddl_cols, fq
        assert [c[1] for c in spec["columns"]] == list(range(1, len(ddl_cols) + 1)), fq
        assert [h.upper() for h in csv_header(spec["file"])] == ddl_cols, spec["file"]
    opts = parse_copy_options(script)
    assert opts["FILE_TYPE"] == "CSV"
    assert opts["FIRSTROW"] == "2"
    assert opts["FIELDTERMINATOR"] == ","
    assert opts["ROWTERMINATOR"] == "0x0A"  # CSVs are LF-only; '\n' would become '\r\n' in COPY INTO
    assert opts["FIELDQUOTE"] == '"'
    assert opts["DATEFORMAT"] == "ymd"
    assert "@StorageAccount" in script and "@Container" in script and "dfs.core.windows.net" in script
    for name in SOURCE_FILES.values():
        raw = (CSV_DIR / name).read_bytes()
        assert b"\r" not in raw, name
        assert raw.isascii(), name  # UTF-8 == Latin1 for VARCHAR columns


def test_csv_formats_match_load_options():
    """Dates are yyyy-mm-dd and timestamps yyyy-mm-dd hh:mi:ss[.ffffff] in every CSV."""
    tables = all_synapse_tables()
    for fq, name in SOURCE_FILES.items():
        types = {c[0]: c[1] for c in tables[fq]["columns"]}
        for row in csv_rows(name):
            for k, v in row.items():
                t = types[k.upper()]
                if v == "":
                    continue
                if t == "DATE":
                    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", v), (name, k, v)
                elif t.startswith("DATETIME2"):
                    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?", v), (name, k, v)


# ---------------------------------------------------------------------------
# SQL Server 2022 (Docker) fixture
# ---------------------------------------------------------------------------
MSSQL_IMAGE = "mcr.microsoft.com/mssql/server:2022-latest"
SA_PASSWORD = "AzSyn_Test_2026!"  # throwaway local test container only
DB_NAME = "azsyn_test"
CONTAINER_DATA = "/var/opt/mssql/azsyn/01_source_tables"


def _docker_ok() -> bool:
    if not shutil.which("docker"):
        return False
    return subprocess.run(["docker", "info"], capture_output=True, check=False).returncode == 0


@pytest.fixture(scope="module")
def mssql():
    pymssql = pytest.importorskip("pymssql")
    if not _docker_ok():
        pytest.skip("Docker is not available")
    name = os.environ.get("AZSYN_MSSQL_CONTAINER", "azsyn-mssql-test")
    port = int(os.environ.get("AZSYN_MSSQL_PORT", "14333"))
    running = subprocess.run(
        ["docker", "ps", "-q", "--filter", f"name=^{name}$"], capture_output=True, text=True, check=False
    ).stdout.strip()
    started = False
    if not running:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        r = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                f"MSSQL_SA_PASSWORD={SA_PASSWORD}",
                "-p",
                f"{port}:1433",
                MSSQL_IMAGE,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if r.returncode != 0:
            pytest.skip(f"could not start SQL Server container: {r.stderr.strip()}")
        started = True
    else:
        mapped = subprocess.run(
            ["docker", "port", name, "1433/tcp"], capture_output=True, text=True, check=False
        ).stdout
        port = int(mapped.strip().splitlines()[0].rsplit(":", 1)[1])

    conn, deadline, last = None, time.time() + 180, None
    while time.time() < deadline:
        try:
            conn = pymssql.connect(
                server="127.0.0.1", port=port, user="sa", password=SA_PASSWORD, autocommit=True, login_timeout=5
            )
            break
        except Exception as exc:  # noqa: BLE001 - server still starting
            last = exc
            time.sleep(3)
    if conn is None:
        pytest.fail(f"SQL Server did not become ready: {last}")

    subprocess.run(["docker", "exec", "-u", "0", name, "mkdir", "-p", CONTAINER_DATA], check=True)
    for f in SOURCE_FILES.values():
        subprocess.run(
            ["docker", "cp", str(CSV_DIR / f), f"{name}:{CONTAINER_DATA}/{f}"], check=True, capture_output=True
        )
    subprocess.run(["docker", "exec", "-u", "0", name, "chmod", "-R", "a+rX", "/var/opt/mssql/azsyn"], check=True)

    cur = conn.cursor()
    cur.execute(
        f"IF DB_ID('{DB_NAME}') IS NOT NULL BEGIN ALTER DATABASE {DB_NAME} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {DB_NAME}; END"
    )
    cur.execute(f"CREATE DATABASE {DB_NAME} COLLATE SQL_Latin1_General_CP1_CI_AS")  # Synapse default collation
    cur.execute(f"USE {DB_NAME}")
    try:
        yield conn
    finally:
        conn.close()
        if started and not os.environ.get("AZSYN_KEEP_MSSQL"):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


def run_batches(conn, batches: list[str]) -> None:
    cur = conn.cursor()
    for b in batches:
        try:
            cur.execute(b)
            while cur.nextset():
                pass
        except Exception as exc:
            raise AssertionError(f"batch failed: {exc}\n---\n{b[:1500]}") from exc


def query(conn, sql: str) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchall()


@pytest.fixture(scope="module")
def loaded(mssql):
    for path in SYN_DDL:
        run_batches(mssql, shim_ddl(path.read_text()))
    run_batches(mssql, shim_copy_script(COPY_SCRIPT.read_text(), CONTAINER_DATA))
    return mssql


# ---------------------------------------------------------------------------
# Live tests
# ---------------------------------------------------------------------------
def test_ddl_applies_in_order_and_is_idempotent(loaded):
    for path in SYN_DDL:  # second application must be a no-op
        run_batches(loaded, shim_ddl(path.read_text()))
    schemas = {r[0] for r in query(loaded, "SELECT name FROM sys.schemas")}
    assert {"core", "txn", "stg", "dp"} <= schemas
    tables = {
        f"{s}.{t}"
        for s, t in query(
            loaded, "SELECT s.name, t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id"
        )
    }
    assert tables == set(EXPECTED_OPTIONS)
    idx = {
        f"{s}.{t}": d
        for s, t, d in query(
            loaded,
            "SELECT s.name, t.name, i.type_desc FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "
            "JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)",
        )
    }
    for fq, exp in EXPECTED_OPTIONS.items():
        want = {"CLUSTERED COLUMNSTORE INDEX": "CLUSTERED COLUMNSTORE", "HEAP": "HEAP"}.get(exp["index"], "CLUSTERED")
        assert idx[fq] == want, fq
    parts = query(
        loaded, "SELECT COUNT(*) FROM sys.partitions WHERE object_id = OBJECT_ID('txn.TRANSACTIONS') AND index_id = 1"
    )[0][0]
    assert parts == 133  # 132 RANGE RIGHT boundaries
    stats = {
        (f"{s}.{t}", n)
        for s, t, n in query(
            loaded,
            "SELECT s.name, t.name, st.name FROM sys.stats st JOIN sys.tables t ON t.object_id = st.object_id "
            "JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE st.name LIKE 'st[_]%'",
        )
    }
    assert stats == {
        (fq, n) for fq, n, _ in {(fq, f"st_{fq.split('.')[1]}_{'_'.join(c)}", c) for fq, c in parse_teradata_stats()}
    }


_SYS_TYPE = {"bigint": "BIGINT", "int": "INT", "smallint": "SMALLINT", "tinyint": "TINYINT", "date": "DATE"}


def _sys_type(name: str, max_length: int, precision: int, scale: int) -> str:
    if name in _SYS_TYPE:
        return _SYS_TYPE[name]
    if name in ("char", "varchar"):
        return f"{name.upper()}({max_length})"
    if name == "decimal":
        return f"DECIMAL({precision},{scale})"
    if name == "datetime2":
        return f"DATETIME2({scale})"
    raise AssertionError(f"unexpected type {name}")


def test_live_columns_match_teradata_ddl(loaded):
    rows = query(
        loaded,
        "SELECT s.name, t.name, c.name, ty.name, c.max_length, c.precision, c.scale, c.is_nullable, dc.definition "
        "FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "JOIN sys.types ty ON ty.user_type_id = c.user_type_id "
        "LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id ORDER BY s.name, t.name, c.column_id",
    )
    live: dict[str, list] = {}
    for s, t, c, ty, ml, p, sc, nullable, dflt in rows:
        d = re.fullmatch(r"\('(.*)'\)", dflt).group(1) if dflt else None
        live.setdefault(f"{s}.{t}", []).append((c, _sys_type(ty, ml, p, sc), not nullable, d))
    expected = {
        fq: [(n, map_teradata_type(t), nn, d) for n, t, nn, d in cols] for fq, cols in parse_teradata_ddl().items()
    }
    expected.update(FROZEN_TABLES)
    assert live == expected


def test_source_row_counts(loaded):
    counts = {fq: query(loaded, f"SELECT COUNT_BIG(*) FROM {fq}")[0][0] for fq in SOURCE_FILES}
    print("row counts:", counts)
    assert counts == EXPECTED_ROWS
    log = query(
        loaded, "SELECT STEP_NAME, STATUS, ROW_COUNT FROM stg.ETL_RUN_LOG WHERE JOB_NAME = 'copy_into_source_tables'"
    )
    assert sorted(log) == sorted((fq, "SUCCESS", n) for fq, n in EXPECTED_ROWS.items())


def _iso121(value: str, typ: str) -> str:
    """CSV date/timestamp text -> SQL Server CONVERT(..., 121) text (DATETIME2(6) has 6 fraction digits)."""
    if typ == "DATE":
        return value
    base, _, frac = value.partition(".")
    return f"{base}.{frac.ljust(6, '0')}"


def test_source_values_match_csv(loaded):
    """Per column: NULL count, and SUM (numerics) / MIN+MAX (dates, timestamps) equal the CSV."""
    tables = all_synapse_tables()
    for fq, name in SOURCE_FILES.items():
        rows = csv_rows(name)
        for col, typ, _, _ in tables[fq]["columns"]:
            vals = [r[col.lower()] for r in rows]
            present = [v for v in vals if v != ""]
            live_nulls = query(loaded, f"SELECT COUNT(*) FROM {fq} WHERE {col} IS NULL")[0][0]
            assert live_nulls == len(vals) - len(present), (fq, col)
            if not present:
                continue
            if typ.startswith("DECIMAL") or typ in ("BIGINT", "INT", "SMALLINT", "TINYINT"):
                live_sum = query(loaded, f"SELECT SUM(CAST({col} AS DECIMAL(38,6))) FROM {fq}")[0][0]
                assert Decimal(live_sum) == sum(Decimal(v) for v in present), (fq, col)
            elif typ == "DATE" or typ.startswith("DATETIME2"):
                lo, hi = query(
                    loaded,
                    f"SELECT CONVERT(VARCHAR(30), MIN({col}), 121), CONVERT(VARCHAR(30), MAX({col}), 121) FROM {fq}",
                )[0]
                assert (lo, hi) == (_iso121(min(present), typ), _iso121(max(present), typ)), (fq, col)
            else:
                n_distinct = query(loaded, f"SELECT COUNT(DISTINCT {col}) FROM {fq}")[0][0]
                assert n_distinct == len({v.upper() for v in present}), (fq, col)


def test_as_of_date_and_text_fidelity(loaded):
    assert str(query(loaded, "SELECT MAX(TRANSACTION_DATE) FROM txn.TRANSACTIONS")[0][0]) == "2026-04-10"
    row = query(
        loaded,
        "SELECT MERCHANT_NAME, CONVERT(VARCHAR(30), CREATED_TS, 121) FROM txn.TRANSACTIONS WHERE TRANSACTION_ID = 1",
    )[0]
    assert row == ("Macdonald, Willis and Silva", "2026-04-10 11:28:08.174535")
    max_part = query(
        loaded,
        "SELECT MIN($PARTITION.pf_TRANSACTIONS(TRANSACTION_DATE)), MAX($PARTITION.pf_TRANSACTIONS(TRANSACTION_DATE)) FROM txn.TRANSACTIONS",
    )[0]
    # 2025-04 .. 2026-04 -> partitions 65 .. 77 (partition 1 holds < 2020-01-01)
    assert max_part == (65, 77)
