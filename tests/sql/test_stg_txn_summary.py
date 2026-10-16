"""SQL Server parity tests for the Synapse transaction-summary procedure.

The SQL Server shim rewrites Synapse CTAS into SQL Server SELECT INTO while
preserving the procedure, statistics, and error-handling statements. The local
container binds to 127.0.0.1:14336 by default; override the host port with
``AZSYN03_MSSQL_PORT``. It is removed at teardown unless
``AZSYN_KEEP_MSSQL=1``; remove a kept container with ``docker rm -f azsyn03-mssql``.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pandas as pd
import pytest

pymssql = pytest.importorskip("pymssql", reason="pymssql is required for SQL Server parity tests")


ROOT = Path(__file__).resolve().parents[2]
DATABASE = "azsyn03_test"
CONTAINER_NAME = "azsyn03-mssql"
DEFAULT_CONTAINER_PORT = 14336
CONTAINER_PASSWORD = "AzSyn03_Test!2026_Password"
AS_OF_DATE = "2026-04-10"
PERIOD_START = "2025-04-10"


def synapse_to_sqlserver(sql: str) -> str:
    """Rewrite each balanced-parenthesis Synapse CTAS clause to SELECT INTO."""
    import re

    pattern = re.compile(
        r"\bCREATE\s+TABLE\s+([A-Za-z0-9_.\[\]]+)\s+WITH\s*\(",
        re.IGNORECASE,
    )
    output: list[str] = []
    cursor = 0
    matches = list(pattern.finditer(sql))
    if not matches:
        raise ValueError("No Synapse CTAS statement was found")

    for match in matches:
        output.append(sql[cursor : match.start()])
        open_paren = match.end() - 1
        depth = 0
        close_paren = None
        for position in range(open_paren, len(sql)):
            if sql[position] == "(":
                depth += 1
            elif sql[position] == ")":
                depth -= 1
                if depth == 0:
                    close_paren = position
                    break
        if close_paren is None:
            raise ValueError("Unbalanced parentheses in CTAS WITH clause")

        as_start = close_paren + 1
        while as_start < len(sql) and sql[as_start].isspace():
            as_start += 1
        if sql[as_start : as_start + 2].upper() != "AS":
            raise ValueError("CTAS WITH clause was not followed by AS")
        select_start = as_start + 2
        while select_start < len(sql) and sql[select_start].isspace():
            select_start += 1
        statement_end = sql.find(";", select_start)
        if statement_end == -1:
            raise ValueError("CTAS SELECT must end with a semicolon")

        select_sql = sql[select_start:statement_end].strip()
        output.append(
            f"SELECT * INTO {match.group(1)} FROM (\n"
            f"{select_sql}\n"
            ") AS ctas_src;"
        )
        cursor = statement_end + 1

    output.append(sql[cursor:])
    return "".join(output)


def _docker(*args: str, timeout: int = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _connection(host: str, port: int, password: str, database: str = "master"):
    return pymssql.connect(
        server=host,
        port=port,
        user="sa",
        password=password,
        database=database,
        login_timeout=5,
        timeout=30,
        autocommit=True,
    )


def _prepare_local_container() -> tuple[str, int, str, bool]:
    host = os.environ.get("MSSQL_HOST")
    password = os.environ.get("MSSQL_SA_PASSWORD", CONTAINER_PASSWORD)
    if host:
        return host, int(os.environ.get("MSSQL_PORT", "1433")), password, False
    port = int(os.environ.get("AZSYN03_MSSQL_PORT", DEFAULT_CONTAINER_PORT))

    docker_path = shutil.which("docker")
    if docker_path is None:
        pytest.skip("Docker is unavailable and MSSQL_HOST is not set")
    info = _docker("info")
    if info.returncode != 0:
        pytest.skip(f"Docker is unavailable: {info.stderr.strip() or info.stdout.strip()}")

    image = "mcr.microsoft.com/mssql/server:2022-latest"
    image_status = _docker("image", "inspect", image)
    if image_status.returncode != 0:
        pulled = _docker("pull", image, timeout=600)
        if pulled.returncode != 0:
            pytest.skip(f"Could not pull SQL Server image: {pulled.stderr.strip()}")

    existing = _docker("inspect", CONTAINER_NAME)
    if existing.returncode == 0:
        port_binding = _docker("port", CONTAINER_NAME, "1433")
        bindings = {binding.strip() for binding in port_binding.stdout.splitlines()}
        if port_binding.returncode != 0 or bindings != {f"127.0.0.1:{port}"}:
            removed = _docker("rm", "-f", CONTAINER_NAME)
            if removed.returncode != 0:
                pytest.skip(
                    f"Could not recreate {CONTAINER_NAME} with a loopback-only binding: "
                    f"{removed.stderr.strip()}"
                )
            existing = _docker("inspect", CONTAINER_NAME)

    if existing.returncode == 0:
        running = _docker("inspect", "--format", "{{.State.Running}}", CONTAINER_NAME)
        if running.stdout.strip() != "true":
            started = _docker("start", CONTAINER_NAME)
            if started.returncode != 0:
                pytest.skip(f"Could not start {CONTAINER_NAME}: {started.stderr.strip()}")
    else:
        started = _docker(
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-e",
            "ACCEPT_EULA=Y",
            "-e",
            f"MSSQL_SA_PASSWORD={password}",
            "-p",
            f"127.0.0.1:{port}:1433",
            image,
        )
        if started.returncode != 0:
            pytest.skip(f"Could not start SQL Server container: {started.stderr.strip()}")

    return "127.0.0.1", port, password, True


@pytest.fixture(scope="session")
def sqlserver_connection():
    manages_container = False
    connection = None
    database_connection = None
    try:
        host, port, password, manages_container = _prepare_local_container()
        deadline = time.monotonic() + 120
        last_error = "SQL Server did not accept a connection"
        while time.monotonic() < deadline:
            try:
                connection = _connection(host, port, password)
                break
            except Exception as error:  # driver errors vary during SQL Server startup
                last_error = str(error)
                time.sleep(2)
        if connection is None:
            pytest.skip(f"SQL Server at {host}:{port} is unreachable: {last_error}")

        cursor = connection.cursor()
        cursor.execute(
            f"IF DB_ID('{DATABASE}') IS NOT NULL "
            f"BEGIN ALTER DATABASE [{DATABASE}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
            f"DROP DATABASE [{DATABASE}]; END; "
            f"CREATE DATABASE [{DATABASE}];"
        )
        connection.close()
        connection = None
        database_connection = _connection(host, port, password, DATABASE)
        cursor = database_connection.cursor()
        for schema_name in ("core", "txn", "stg"):
            cursor.execute(f"CREATE SCHEMA {schema_name};")
        cursor.execute(
            """
            CREATE TABLE core.ACCOUNTS (
                ACCOUNT_ID BIGINT NOT NULL,
                CUSTOMER_ID BIGINT NOT NULL,
                ACCOUNT_TYPE VARCHAR(20),
                ACCOUNT_STATUS CHAR(1),
                OPEN_DATE DATE,
                CLOSE_DATE DATE,
                CURRENT_BALANCE DECIMAL(15,2),
                AVAILABLE_BALANCE DECIMAL(15,2),
                CREDIT_LIMIT DECIMAL(15,2),
                INTEREST_RATE DECIMAL(5,4),
                BRANCH_ID INT,
                CREATED_TS DATETIME2(6),
                UPDATED_TS DATETIME2(6)
            );
            CREATE TABLE txn.TRANSACTIONS (
                TRANSACTION_ID BIGINT NOT NULL,
                ACCOUNT_ID BIGINT NOT NULL,
                TRANSACTION_TYPE_CD VARCHAR(10),
                TRANSACTION_DATE DATE,
                TRANSACTION_TS DATETIME2(6),
                AMOUNT DECIMAL(15,2),
                RUNNING_BALANCE DECIMAL(15,2),
                MERCHANT_NAME VARCHAR(100),
                MERCHANT_CATEGORY VARCHAR(60),
                CHANNEL_CODE VARCHAR(10),
                REFERENCE_NUM VARCHAR(40),
                STATUS_CODE CHAR(1),
                CREATED_TS DATETIME2(6)
            );
            CREATE TABLE txn.TRANSACTION_TYPES (
                TRANSACTION_TYPE_CD VARCHAR(10) NOT NULL,
                DESCRIPTION VARCHAR(60),
                CATEGORY VARCHAR(30),
                IS_REVENUE CHAR(1) NOT NULL DEFAULT 'N',
                EFFECTIVE_DATE DATE,
                EXPIRATION_DATE DATE
            );
            CREATE TABLE stg.ETL_RUN_LOG (
                JOB_NAME VARCHAR(100) NOT NULL,
                STEP_NAME VARCHAR(100),
                STATUS VARCHAR(20) NOT NULL,
                ROW_COUNT BIGINT,
                START_TS DATETIME2(6),
                END_TS DATETIME2(6),
                MESSAGE VARCHAR(4000)
            );
            """
        )

        source_tables = (
            ("accounts", "core.ACCOUNTS", 1251),
            ("transactions", "txn.TRANSACTIONS", 80528),
            ("transaction_types", "txn.TRANSACTION_TYPES", 8),
        )
        for file_stem, table_name, expected_count in source_tables:
            source = pd.read_csv(
                ROOT / "data" / "01_source_tables" / f"{file_stem}.csv",
                dtype=str,
                keep_default_na=False,
            )
            columns = [column.upper() for column in source.columns]
            values = [
                tuple(None if value == "" else value for value in row)
                for row in source.itertuples(index=False, name=None)
            ]
            parameter_row = "(" + ", ".join(["%s"] * len(columns)) + ")"
            batch_size = 150
            for start in range(0, len(values), batch_size):
                batch = values[start : start + batch_size]
                placeholders = ", ".join([parameter_row] * len(batch))
                parameters = tuple(value for row in batch for value in row)
                cursor.execute(
                    f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES {placeholders}",
                    parameters,
                )
            cursor.execute(f"SELECT COUNT_BIG(*) FROM {table_name}")
            loaded_count = cursor.fetchone()[0]
            assert loaded_count == expected_count, (
                f"{table_name} loaded {loaded_count} rows, expected {expected_count}"
            )

        yield database_connection
    finally:
        try:
            if database_connection is not None:
                database_connection.close()
            if connection is not None:
                connection.close()
        finally:
            if (
                manages_container
                and os.environ.get("AZSYN_KEEP_MSSQL") != "1"
                and shutil.which("docker") is not None
            ):
                _docker("rm", "-f", CONTAINER_NAME)


@pytest.fixture(scope="module")
def summary_run(sqlserver_connection):
    cursor = sqlserver_connection.cursor()
    procedure_path = (
        ROOT / "synapse" / "sql" / "procs" / "stg.usp_load_stg_txn_summary.sql"
    )
    procedure_sql = procedure_path.read_text(encoding="utf-8")
    cursor.execute(synapse_to_sqlserver(procedure_sql))
    cursor.execute(
        "EXEC stg.usp_load_stg_txn_summary @AsOfDate = '2026-04-10';"
    )
    cursor.execute(
        "SELECT * FROM stg.STG_TXN_SUMMARY ORDER BY CUSTOMER_ID, ACCOUNT_ID"
    )
    actual = pd.DataFrame.from_records(
        cursor.fetchall(), columns=[column[0] for column in cursor.description]
    )
    expected = pd.read_csv(
        ROOT / "data" / "02_bteq_staging" / "stg_txn_summary.csv",
        dtype=str,
        keep_default_na=False,
    )
    expected.columns = [column.upper() for column in expected.columns]
    for column in expected.columns:
        expected[column] = expected[column].map(
            lambda value: None if value == "" else value
        )
    yield sqlserver_connection, actual, expected


def _is_null(value) -> bool:
    return value is None or pd.isna(value)


def _compact_diff(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    columns: list[str],
    numeric_columns: set[str] | None = None,
    tolerance: float = 0.0,
) -> str:
    numeric_columns = numeric_columns or set()
    report = []
    for column in columns:
        mismatches = []
        max_abs_diff = 0.0
        for row_index, (actual_value, expected_value) in enumerate(
            zip(actual[column], expected[column])
        ):
            actual_null = _is_null(actual_value)
            expected_null = _is_null(expected_value)
            if actual_null or expected_null:
                is_mismatch = actual_null != expected_null
                diff = None
            elif column in numeric_columns:
                diff = abs(float(actual_value) - float(expected_value))
                max_abs_diff = max(max_abs_diff, diff)
                is_mismatch = diff > tolerance
            else:
                diff = None
                is_mismatch = actual_value != expected_value
            if is_mismatch:
                mismatches.append(
                    {
                        "row": row_index,
                        "actual": actual_value,
                        "expected": expected_value,
                        "abs_diff": diff,
                    }
                )
        report.append(
            f"{column}: mismatches={len(mismatches)}, "
            f"max_abs_diff={max_abs_diff:.6f}, first_5={mismatches[:5]}"
        )
    return "\n".join(report)


def _category_spend_by_account(cursor) -> dict[int, dict[str, float]]:
    cursor.execute(
        """
        SELECT ACCOUNT_ID, MERCHANT_CATEGORY, SUM(ABS(AMOUNT)) AS CATEGORY_SPEND
        FROM txn.TRANSACTIONS
        WHERE TRANSACTION_DATE BETWEEN %s AND %s
          AND STATUS_CODE = 'P'
          AND MERCHANT_CATEGORY IS NOT NULL
        GROUP BY ACCOUNT_ID, MERCHANT_CATEGORY
        """,
        (PERIOD_START, AS_OF_DATE),
    )
    spend: dict[int, dict[str, float]] = {}
    for account_id, category, amount in cursor.fetchall():
        spend.setdefault(int(account_id), {})[category] = float(amount)
    return spend


def test_ctas_shim_rewrites_balanced_with_clause():
    source = (
        "CREATE TABLE stg.sample\n"
        "WITH (DISTRIBUTION = HASH(CUSTOMER_ID), CLUSTERED COLUMNSTORE INDEX)\n"
        "AS\nSELECT CAST(1 AS INT) AS CUSTOMER_ID;"
    )
    rewritten = synapse_to_sqlserver(source)
    assert rewritten == (
        "SELECT * INTO stg.sample FROM (\n"
        "SELECT CAST(1 AS INT) AS CUSTOMER_ID\n"
        ") AS ctas_src;"
    )
    with pytest.raises(ValueError, match="No Synapse CTAS"):
        synapse_to_sqlserver("CREATE TABLE stg.sample (ID INT);")


def test_stg_txn_summary_parity_and_runtime_behaviors(summary_run):
    connection, actual, expected = summary_run
    cursor = connection.cursor()

    assert list(actual.columns) == list(expected.columns.str.upper())
    assert len(actual) == 1251 == len(expected)
    key_columns = ["CUSTOMER_ID", "ACCOUNT_ID"]
    for frame in (actual, expected):
        frame["CUSTOMER_ID"] = pd.to_numeric(frame["CUSTOMER_ID"]).astype("int64")
        frame["ACCOUNT_ID"] = pd.to_numeric(frame["ACCOUNT_ID"]).astype("int64")
    actual_keys = set(map(tuple, actual[key_columns].itertuples(index=False, name=None)))
    expected_keys = set(
        map(tuple, expected[key_columns].itertuples(index=False, name=None))
    )
    assert actual_keys == expected_keys, (
        f"Key-set mismatch: missing={list(expected_keys - actual_keys)[:5]}, "
        f"unexpected={list(actual_keys - expected_keys)[:5]}"
    )
    assert not actual.duplicated(key_columns).any(), "Summary key is not unique"
    assert not expected.duplicated(key_columns).any(), "Expected summary key is not unique"
    actual = actual.sort_values(key_columns).reset_index(drop=True)
    expected = expected.sort_values(key_columns).reset_index(drop=True)

    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH,
               NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'stg' AND TABLE_NAME = 'STG_TXN_SUMMARY'
        ORDER BY ORDINAL_POSITION
        """
    )
    schema = cursor.fetchall()
    expected_schema = [
        ("CUSTOMER_ID", "bigint", None, 19, 0, None),
        ("ACCOUNT_ID", "bigint", None, 19, 0, None),
        ("ACCOUNT_TYPE", "varchar", 20, None, None, None),
        ("SUMMARY_PERIOD_START", "date", None, None, None, 0),
        ("SUMMARY_PERIOD_END", "date", None, None, None, 0),
        ("TXN_COUNT_TOTAL", "int", None, 10, 0, None),
        ("TXN_COUNT_DEBIT", "int", None, 10, 0, None),
        ("TXN_COUNT_CREDIT", "int", None, 10, 0, None),
        ("TXN_COUNT_FEE", "int", None, 10, 0, None),
        ("AMT_TOTAL_DEBIT", "decimal", None, 18, 2, None),
        ("AMT_TOTAL_CREDIT", "decimal", None, 18, 2, None),
        ("AMT_TOTAL_FEES", "decimal", None, 18, 2, None),
        ("AMT_AVG_DEBIT", "decimal", None, 15, 2, None),
        ("AMT_AVG_CREDIT", "decimal", None, 15, 2, None),
        ("AMT_MAX_SINGLE_DEBIT", "decimal", None, 15, 2, None),
        ("AMT_MAX_SINGLE_CREDIT", "decimal", None, 15, 2, None),
        ("DISTINCT_MERCHANTS", "int", None, 10, 0, None),
        ("TOP_MERCHANT_CATEGORY", "varchar", 60, None, None, None),
        ("PCT_ATM", "decimal", None, 5, 2, None),
        ("PCT_POS", "decimal", None, 5, 2, None),
        ("PCT_WEB", "decimal", None, 5, 2, None),
        ("PCT_MOBILE", "decimal", None, 5, 2, None),
        ("DAYS_SINCE_LAST_TXN", "int", None, 10, 0, None),
        ("LOAD_TS", "datetime2", None, None, None, 6),
    ]
    schema_shape = [
        (
            row[0],
            row[1],
            row[2],
            row[3] if row[3] is not None else None,
            row[4] if row[4] is not None else None,
            row[5],
        )
        for row in schema
    ]
    assert schema_shape == expected_schema
    assert [row[6] for row in schema[:2]] == ["NO", "NO"]

    exact_columns = [
        "ACCOUNT_TYPE",
        "SUMMARY_PERIOD_START",
        "SUMMARY_PERIOD_END",
        "TXN_COUNT_TOTAL",
        "TXN_COUNT_DEBIT",
        "TXN_COUNT_CREDIT",
        "TXN_COUNT_FEE",
        "DISTINCT_MERCHANTS",
        "DAYS_SINCE_LAST_TXN",
    ]
    for column in ("SUMMARY_PERIOD_START", "SUMMARY_PERIOD_END"):
        actual[column] = pd.to_datetime(actual[column]).dt.strftime("%Y-%m-%d")
    for column in exact_columns:
        if column not in ("SUMMARY_PERIOD_START", "SUMMARY_PERIOD_END", "ACCOUNT_TYPE"):
            actual[column] = pd.to_numeric(actual[column])
            expected[column] = pd.to_numeric(expected[column])

    numeric_columns = [
        "AMT_TOTAL_DEBIT",
        "AMT_TOTAL_CREDIT",
        "AMT_TOTAL_FEES",
        "AMT_AVG_DEBIT",
        "AMT_AVG_CREDIT",
        "AMT_MAX_SINGLE_DEBIT",
        "AMT_MAX_SINGLE_CREDIT",
        "PCT_ATM",
        "PCT_POS",
        "PCT_WEB",
        "PCT_MOBILE",
    ]
    for column in numeric_columns:
        actual[column] = pd.to_numeric(actual[column], errors="coerce")
        expected[column] = pd.to_numeric(expected[column], errors="coerce")

    category_column = "TOP_MERCHANT_CATEGORY"
    category_mismatches = []
    spend_by_account = _category_spend_by_account(cursor)
    for row_index, (actual_category, expected_category) in enumerate(
        zip(actual[category_column], expected[category_column])
    ):
        if _is_null(actual_category) and _is_null(expected_category):
            continue
        if actual_category != expected_category:
            account_id = int(actual.at[row_index, "ACCOUNT_ID"])
            account_spend = spend_by_account.get(account_id, {})
            actual_spend = account_spend.get(actual_category)
            expected_spend = account_spend.get(expected_category)
            is_tie = (
                actual_spend is not None
                and expected_spend is not None
                and actual_spend == expected_spend
            )
            category_mismatches.append(
                {
                    "customer_id": int(actual.at[row_index, "CUSTOMER_ID"]),
                    "account_id": account_id,
                    "actual": actual_category,
                    "expected": expected_category,
                    "actual_spend": actual_spend,
                    "expected_spend": expected_spend,
                    "spend_tie": is_tie,
                }
            )

    diff_report = _compact_diff(
        actual,
        expected,
        exact_columns + numeric_columns,
        numeric_columns=set(numeric_columns),
        tolerance=0.01,
    )
    print("Parity mismatch counts and numeric maximum absolute differences:")
    print(diff_report)
    print(f"{category_column}: mismatches={len(category_mismatches)}")
    parity_failures = []
    for column in exact_columns:
        column_report = _compact_diff(actual, expected, [column])
        if "mismatches=0" not in column_report:
            parity_failures.append(column_report)
    for column in numeric_columns:
        column_report = _compact_diff(
            actual,
            expected,
            [column],
            numeric_columns={column},
            tolerance=0.01,
        )
        if "mismatches=0" not in column_report:
            parity_failures.append(column_report)
    if category_mismatches:
        parity_failures.append(
            f"{category_column}: mismatches={len(category_mismatches)}, "
            f"first_5={category_mismatches[:5]}"
        )
    assert not parity_failures, (
        "Parity mismatch report (per-column mismatch count and first 5 examples):\n"
        + "\n".join(parity_failures)
    )

    cursor.execute(
        """
        SELECT name FROM sys.stats
        WHERE object_id = OBJECT_ID('stg.STG_TXN_SUMMARY')
        """
    )
    stats = {row[0] for row in cursor.fetchall()}
    expected_stats = {
        "st_STG_TXN_SUMMARY_CUSTOMER_ID",
        "st_STG_TXN_SUMMARY_CUSTOMER_ID_ACCOUNT_ID",
        "st_STG_TXN_SUMMARY_ACCOUNT_TYPE",
    }
    assert expected_stats <= stats

    cursor.execute(
        """
        SELECT STATUS, ROW_COUNT, START_TS, END_TS
        FROM stg.ETL_RUN_LOG
        WHERE JOB_NAME = '02_stg_txn_summary' AND STEP_NAME = 'FULL_LOAD'
        ORDER BY END_TS DESC
        """
    )
    success_log = cursor.fetchone()
    assert success_log is not None
    assert success_log[0] == "SUCCESS"
    assert success_log[1] == 1251
    assert success_log[2] <= success_log[3]

    with pytest.raises(pymssql.Error, match="STG_TXN_SUMMARY loaded 0 rows"):
        cursor.execute(
            "EXEC stg.usp_load_stg_txn_summary @AsOfDate = '2000-01-01';"
        )
    cursor.execute(
        """
        SELECT STATUS, MESSAGE, ROW_COUNT
        FROM stg.ETL_RUN_LOG
        WHERE JOB_NAME = '02_stg_txn_summary' AND STEP_NAME = 'FULL_LOAD'
        ORDER BY END_TS DESC
        """
    )
    failed_log = cursor.fetchone()
    assert failed_log == ("FAILED", "STG_TXN_SUMMARY loaded 0 rows", 0)

    with pytest.raises(pymssql.Error):
        cursor.execute(
            "EXEC stg.usp_load_stg_txn_summary @AsOfDate = '0001-01-01';"
        )
    cursor.execute(
        """
        SELECT TOP (1) STATUS, MESSAGE
        FROM stg.ETL_RUN_LOG
        WHERE JOB_NAME = '02_stg_txn_summary'
        ORDER BY END_TS DESC
        """
    )
    date_error_log = cursor.fetchone()
    assert date_error_log is not None
    assert date_error_log[0] == "FAILED"
    assert date_error_log[1] is not None

    cursor.execute(
        "EXEC stg.usp_load_stg_txn_summary "
        "@AsOfDate = '2026-04-10', @LookbackMonths = 1;"
    )
    cursor.execute(
        "SELECT COUNT_BIG(*), MIN(SUMMARY_PERIOD_START), MAX(SUMMARY_PERIOD_END) "
        "FROM stg.STG_TXN_SUMMARY"
    )
    lookback_count, lookback_start, lookback_end = cursor.fetchone()
    assert lookback_count <= len(expected)
    assert lookback_start.isoformat() == "2026-03-10"
    assert lookback_end.isoformat() == AS_OF_DATE
