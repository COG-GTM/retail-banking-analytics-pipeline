"""Tests for synapse/spark/common/pipeline_utils.py (local backend, no Synapse needed).

Run: pytest tests/spark/test_pipeline_utils.py
"""

import ast
import dataclasses
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from synapse.spark.common import pipeline_utils as pu  # noqa: E402

SOURCE_DIR = REPO_ROOT / "data" / "01_source_tables"
STAGING_DIR = REPO_ROOT / "data" / "02_bteq_staging"
MODULE_PATH = REPO_ROOT / "synapse" / "spark" / "common" / "pipeline_utils.py"

ETL_RUN_LOG_COLUMNS = ["JOB_NAME", "STEP_NAME", "STATUS", "ROW_COUNT", "START_TS", "END_TS", "MESSAGE"]


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_pipeline_utils")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session


@pytest.fixture
def local_dir(tmp_path, monkeypatch):
    (tmp_path / "core").mkdir()
    (tmp_path / "stg").mkdir()
    shutil.copy(SOURCE_DIR / "customers.csv", tmp_path / "core" / "customers.csv")
    shutil.copy(SOURCE_DIR / "accounts.csv", tmp_path / "core" / "accounts.csv")
    shutil.copy(STAGING_DIR / "stg_customer_360.csv", tmp_path / "stg" / "stg_customer_360.csv")
    monkeypatch.setenv(pu.LOCAL_DATA_DIR_ENV, str(tmp_path))
    pu._step_start_ts.clear()
    return tmp_path


def _csv_header(path):
    with open(path) as fh:
        return [c.upper() for c in fh.readline().strip().split(",")]


# --------------------------------------------------------------------------- contract signatures

CONTRACT_SIGNATURES = {
    "get_run_params": "def get_run_params(argv: list[str] | None = None) -> RunParams",
    "read_table": "def read_table(spark, schema: str, table: str) -> DataFrame",
    "write_table": "def write_table(df, schema: str, table: str, mode: str = \"overwrite\", "
                   "pre_delete_where: str | None = None) -> None",
    "log_step": "def log_step(spark, job_name: str, step_name: str, status: str, "
                "row_count: int | None = None, msg: str | None = None) -> None",
    "validate_table": "def validate_table(df, key_cols: list[str], not_null_cols: list[str], "
                      "min_rows: int = 1) -> None",
}


def _module_ast():
    return ast.parse(MODULE_PATH.read_text())


def test_function_signatures_match_contract():
    source = MODULE_PATH.read_text()
    defs = {n.name: n for n in _module_ast().body if isinstance(n, ast.FunctionDef)}
    for name, expected in CONTRACT_SIGNATURES.items():
        node = defs[name]
        # Literal source text of the def header (from "def" up to the body's colon).
        segment = ast.get_source_segment(source, node).split("\n")
        header_lines = segment[: node.body[0].lineno - node.lineno]
        header = " ".join(line.strip() for line in header_lines).rstrip(":")
        assert header == expected


def test_run_params_and_validation_error_match_contract():
    classes = {n.name: n for n in _module_ast().body if isinstance(n, ast.ClassDef)}
    run_params = classes["RunParams"]
    assert [ast.unparse(d) for d in run_params.decorator_list] == ["dataclass(frozen=True)"]
    fields = [ast.unparse(s) for s in run_params.body if isinstance(s, ast.AnnAssign)]
    assert fields == [
        "as_of_date: date",
        "env: str",
        "lookback_months: int = 12",
        "risk_score_threshold: int = 700",
    ]
    assert [ast.unparse(b) for b in classes["ValidationError"].bases] == ["Exception"]


def test_package_init_files_are_empty():
    for rel in ["synapse", "synapse/spark", "synapse/spark/jobs", "synapse/spark/common"]:
        assert (REPO_ROOT / rel / "__init__.py").read_text() == ""


# --------------------------------------------------------------------------- get_run_params


def test_get_run_params_defaults():
    before = datetime.now(timezone.utc).date()
    params = pu.get_run_params([])
    after = datetime.now(timezone.utc).date()
    assert params.as_of_date in {before, after}
    assert params.env == "dev"
    assert params.lookback_months == 12
    assert params.risk_score_threshold == 700


def test_get_run_params_overrides():
    params = pu.get_run_params([
        "--as-of-date", "2026-04-10", "--env", "prod",
        "--lookback-months", "6", "--risk-score-threshold", "650",
    ])
    assert params == pu.RunParams(date(2026, 4, 10), "prod", 6, 650)


def test_get_run_params_reads_sys_argv_when_none(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["job.py", "--as-of-date", "2026-04-10", "--env", "uat"])
    params = pu.get_run_params()
    assert (params.as_of_date, params.env) == (date(2026, 4, 10), "uat")


@pytest.mark.parametrize("argv", [["--env", "qa"], ["--as-of-date", "2026-13-01"], ["--as-of-date", "10/04/2026"]])
def test_get_run_params_rejects_bad_values(argv):
    with pytest.raises(SystemExit):
        pu.get_run_params(argv)


def test_run_params_is_frozen():
    params = pu.get_run_params(["--as-of-date", "2026-04-10"])
    with pytest.raises(dataclasses.FrozenInstanceError):
        params.env = "prod"


# --------------------------------------------------------------------------- local backend I/O


def test_read_table_local_csv(spark, local_dir):
    df = pu.read_table(spark, "core", "CUSTOMERS")
    assert df.count() == 500
    assert df.columns == _csv_header(SOURCE_DIR / "customers.csv")
    types = dict(df.dtypes)
    assert types["DATE_OF_BIRTH"] == "date"
    assert types["CREATED_TS"] == "timestamp"


def test_read_table_flat_layout(spark, tmp_path, monkeypatch):
    monkeypatch.setenv(pu.LOCAL_DATA_DIR_ENV, str(SOURCE_DIR))
    assert pu.read_table(spark, "txn", "TRANSACTION_TYPES").count() > 0


def test_read_table_missing_raises(spark, local_dir):
    with pytest.raises(FileNotFoundError):
        pu.read_table(spark, "dp", "CUSTOMER_SEGMENTS")


def test_local_round_trip(spark, local_dir):
    src = pu.read_table(spark, "stg", "STG_CUSTOMER_360")
    pu.write_table(src, "dp", "ROUND_TRIP")

    out_path = local_dir / "dp" / "ROUND_TRIP.parquet"
    assert out_path.is_dir()
    back = pu.read_table(spark, "dp", "ROUND_TRIP")
    assert back.columns == src.columns
    assert back.dtypes == src.dtypes
    assert back.count() == src.count() == 478
    assert back.exceptAll(src).count() == 0 and src.exceptAll(back).count() == 0

    pu.write_table(src.limit(10), "dp", "ROUND_TRIP", mode="append")
    assert pu.read_table(spark, "dp", "ROUND_TRIP").count() == 488
    pu.write_table(src.limit(10), "dp", "ROUND_TRIP")
    assert pu.read_table(spark, "dp", "ROUND_TRIP").count() == 10


def test_overwrite_from_same_table(spark, local_dir):
    pu.write_table(pu.read_table(spark, "core", "ACCOUNTS"), "core", "ACCOUNTS_COPY")
    derived = pu.read_table(spark, "core", "ACCOUNTS_COPY").filter("CUSTOMER_ID <= 100")
    expected = derived.count()
    pu.write_table(derived, "core", "ACCOUNTS_COPY")
    assert pu.read_table(spark, "core", "ACCOUNTS_COPY").count() == expected


def test_write_with_pre_delete_where_deletes_then_appends(spark, local_dir):
    schema = T.StructType([
        T.StructField("REPORTING_PERIOD", T.StringType()),
        T.StructField("CUSTOMER_ID", T.LongType()),
        T.StructField("AMOUNT", T.DecimalType(15, 2)),
    ])
    from decimal import Decimal
    initial = spark.createDataFrame(
        [("2026-03", 1, Decimal("1.00")), ("2026-04", 1, Decimal("2.00")),
         ("2026-04", 2, Decimal("3.00")), (None, 3, Decimal("4.00"))], schema)
    pu.write_table(initial, "dp", "PERIODIC")

    reload = spark.createDataFrame([("2026-04", 1, Decimal("9.99"))], schema)
    pu.write_table(reload, "dp", "PERIODIC", pre_delete_where="REPORTING_PERIOD = '2026-04'")

    rows = {(r.REPORTING_PERIOD, r.CUSTOMER_ID, r.AMOUNT) for r in pu.read_table(spark, "dp", "PERIODIC").collect()}
    # 2026-04 rows replaced; 2026-03 kept; NULL-predicate row kept (like SQL DELETE).
    assert rows == {("2026-03", 1, Decimal("1.00")), ("2026-04", 1, Decimal("9.99")), (None, 3, Decimal("4.00"))}

    # pre_delete_where against a table that doesn't exist yet just writes the data.
    pu.write_table(reload, "dp", "PERIODIC_NEW", pre_delete_where="1 = 1")
    assert pu.read_table(spark, "dp", "PERIODIC_NEW").count() == 1


@pytest.mark.parametrize("schema,table", [("dp; DROP TABLE x", "T"), ("dp", "T--"), ("dp", "")])
def test_identifiers_are_validated(spark, local_dir, schema, table):
    with pytest.raises(ValueError):
        pu.read_table(spark, schema, table)


def test_write_table_rejects_bad_mode(spark, local_dir):
    df = pu.read_table(spark, "core", "CUSTOMERS")
    with pytest.raises(ValueError):
        pu.write_table(df, "dp", "X", mode="upsert")


# --------------------------------------------------------------------------- Synapse backend wiring


def test_synapse_backend_uses_connector(monkeypatch):
    monkeypatch.delenv(pu.LOCAL_DATA_DIR_ENV, raising=False)
    monkeypatch.delenv(pu.SQL_SERVER_ENV, raising=False)
    monkeypatch.setattr(pu, "_import_synapse_connector", lambda: mock.Mock(SERVER="server"))
    statements = []
    monkeypatch.setattr(pu, "_execute_sql_pool", lambda spark, sql: statements.append(sql))

    spark = mock.MagicMock()
    pu.read_table(spark, "stg", "STG_CUSTOMER_360")
    spark.read.synapsesql.assert_called_once_with("sqlpool_retail.stg.STG_CUSTOMER_360")

    df = mock.MagicMock()
    pu.write_table(df, "dp", "TRANSACTION_ANALYTICS", pre_delete_where="REPORTING_PERIOD = '2026-04'")
    assert statements == ["DELETE FROM dp.TRANSACTION_ANALYTICS WHERE REPORTING_PERIOD = '2026-04';"]
    df.write.mode.assert_called_with("append")
    df.write.mode.return_value.synapsesql.assert_called_with("sqlpool_retail.dp.TRANSACTION_ANALYTICS")

    statements.clear()
    pu.write_table(df, "dp", "CUSTOMER_SEGMENTS")
    assert statements == [
        "IF OBJECT_ID(N'dp.CUSTOMER_SEGMENTS', N'U') IS NOT NULL TRUNCATE TABLE dp.CUSTOMER_SEGMENTS;"
    ]
    df.write.mode.assert_called_with("append")

    statements.clear()
    pu.write_table(df, "dp", "CUSTOMER_SEGMENTS", mode="append")
    assert statements == []


# --------------------------------------------------------------------------- validate_table


def test_validate_table_passes_on_staging(spark, local_dir):
    df = pu.read_table(spark, "stg", "STG_CUSTOMER_360")
    pu.validate_table(df, ["CUSTOMER_ID"], ["CUSTOMER_ID", "LAST_NAME"], min_rows=478)


def test_validate_table_composite_key_and_case_insensitive(spark, local_dir):
    df = pu.read_table(spark, "core", "ACCOUNTS")
    pu.validate_table(df, ["customer_id", "account_id"], ["account_id"])
    with pytest.raises(pu.ValidationError):
        pu.validate_table(df, ["CUSTOMER_ID"], [])


def test_validate_table_min_rows(spark, local_dir):
    df = pu.read_table(spark, "stg", "STG_CUSTOMER_360")
    with pytest.raises(pu.ValidationError, match=r"row count 478 < min_rows 1000"):
        pu.validate_table(df, ["CUSTOMER_ID"], [], min_rows=1000)


def test_validate_table_reports_every_failure(spark):
    df = spark.createDataFrame(
        [(1, "a", None), (1, None, None), (2, None, "x")],
        "CUSTOMER_ID long, LAST_NAME string, SEGMENT string",
    )
    with pytest.raises(pu.ValidationError) as exc:
        pu.validate_table(df, ["CUSTOMER_ID"], ["CUSTOMER_ID", "LAST_NAME", "SEGMENT", "NOPE"], min_rows=5)
    err = exc.value
    assert err.failures == [
        "missing columns: NOPE",
        "row count 3 < min_rows 5",
        "1 duplicate key groups on (CUSTOMER_ID)",
        "LAST_NAME has 2 NULL values",
        "SEGMENT has 2 NULL values",
    ]
    for failure in err.failures:
        assert failure in str(err)


def test_validate_table_empty_df(spark):
    df = spark.createDataFrame([], "CUSTOMER_ID long")
    with pytest.raises(pu.ValidationError, match="row count 0 < min_rows 1"):
        pu.validate_table(df, ["CUSTOMER_ID"], ["CUSTOMER_ID"])
    pu.validate_table(df, [], [], min_rows=0)


# --------------------------------------------------------------------------- log_step


def test_log_step_writes_etl_run_log_row(spark, local_dir, caplog):
    caplog.set_level("INFO", logger="synapse.pipeline")
    t0 = datetime.now(timezone.utc) - timedelta(seconds=1)
    pu.log_step(spark, "01_sas_customer_segments", "LOAD", "START", msg="Loading dp.CUSTOMER_SEGMENTS")
    pu.log_step(spark, "01_sas_customer_segments", "LOAD", "SUCCESS", row_count=407, msg="done")
    pu.log_step(spark, "01_sas_customer_segments", "VALIDATE", "ERROR", msg="x" * 5000)

    log_path = local_dir / "stg" / "ETL_RUN_LOG.parquet"
    assert log_path.is_dir()
    log = spark.read.parquet(str(log_path))
    assert log.columns == ETL_RUN_LOG_COLUMNS
    assert [f.dataType for f in log.schema.fields] == [
        T.StringType(), T.StringType(), T.StringType(), T.LongType(),
        T.TimestampType(), T.TimestampType(), T.StringType(),
    ]

    rows = sorted(log.collect(), key=lambda r: (r.END_TS, r.STATUS))
    assert [(r.JOB_NAME, r.STEP_NAME, r.STATUS, r.ROW_COUNT) for r in rows] == [
        ("01_sas_customer_segments", "LOAD", "START", None),
        ("01_sas_customer_segments", "LOAD", "SUCCESS", 407),
        ("01_sas_customer_segments", "VALIDATE", "ERROR", None),
    ]
    start, success, error = rows
    assert success.START_TS == start.START_TS
    assert success.END_TS >= success.START_TS
    assert start.START_TS >= t0.replace(tzinfo=None)
    assert len(error.MESSAGE) == 4000

    text = caplog.text
    assert "01_sas_customer_segments | LOAD | SUCCESS | done | Rows: 407" in text
    assert any(r.levelname == "ERROR" and "VALIDATE" in r.getMessage() for r in caplog.records)


def test_log_step_via_read_table(spark, local_dir):
    pu.log_step(spark, "02_sas_txn_analytics", "FULL_LOAD", "SUCCESS", row_count=500)
    log = pu.read_table(spark, "stg", "ETL_RUN_LOG")
    assert log.columns == ETL_RUN_LOG_COLUMNS
    assert log.count() == 1
