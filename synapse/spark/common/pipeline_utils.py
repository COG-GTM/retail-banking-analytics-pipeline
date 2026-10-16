"""Shared Spark helpers for the Synapse retail-banking pipeline.

Replaces the SAS macros in sas/macros/:
  connect_teradata.sas -> read_table / write_table
  log_step.sas         -> log_step
  validate_table.sas   -> validate_table / ValidationError
plus get_run_params, which replaces CURRENT_DATE / today() with an explicit as-of date.

Backends
  Synapse (default): dedicated SQL pool "sqlpool_retail" via the Synapse Dedicated SQL Pool
    connector (com.microsoft.spark.sqlanalytics, .synapsesql("sqlpool_retail.<schema>.<table>")).
    DELETE/TRUNCATE statements run on the pool over pyodbc with the workspace AAD token.
  Local: set PIPELINE_LOCAL_DATA_DIR=<dir>. Tables are read from
    <dir>/<schema>/<table>.parquet (written by write_table) or <dir>/<schema>/<table>.csv, then
    <dir>/<table>.parquet / <dir>/<table>.csv (names matched case-insensitively), and written to
    <dir>/<schema>/<TABLE>.parquet.
"""

import argparse
import logging
import os
import re
import shutil
import struct
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

LOCAL_DATA_DIR_ENV = "PIPELINE_LOCAL_DATA_DIR"
SQL_POOL = "sqlpool_retail"
SQL_SERVER_ENV = "SYNAPSE_SQL_SERVER"
ODBC_DRIVER_ENV = "PIPELINE_ODBC_DRIVER"
DEFAULT_ODBC_DRIVER = "ODBC Driver 18 for SQL Server"

ETL_RUN_LOG_SCHEMA = StructType(
    [
        StructField("JOB_NAME", StringType(), False),
        StructField("STEP_NAME", StringType(), True),
        StructField("STATUS", StringType(), False),
        StructField("ROW_COUNT", LongType(), True),
        StructField("START_TS", TimestampType(), True),
        StructField("END_TS", TimestampType(), True),
        StructField("MESSAGE", StringType(), True),
    ]
)
_ETL_RUN_LOG_LENGTHS = {"JOB_NAME": 100, "STEP_NAME": 100, "STATUS": 20, "MESSAGE": 4000}

_WRITE_MODES = {"overwrite", "append", "error", "errorifexists", "ignore"}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SQL_COPT_SS_ACCESS_TOKEN = 1256

logger = logging.getLogger("synapse.pipeline")
_step_start_ts: dict[tuple[str, str], datetime] = {}


@dataclass(frozen=True)
class RunParams:
    as_of_date: date
    env: str
    lookback_months: int = 12
    risk_score_threshold: int = 700


class ValidationError(Exception):
    def __init__(self, message: str, failures: list[str] | None = None):
        super().__init__(message)
        self.failures = list(failures or [])


# --------------------------------------------------------------------------- run parameters


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}, expected YYYY-MM-DD") from exc


def get_run_params(argv: list[str] | None = None) -> RunParams:
    parser = argparse.ArgumentParser(description="Retail banking Synapse Spark job parameters")
    parser.add_argument("--as-of-date", type=_parse_date, default=None,
                        help="Business date YYYY-MM-DD (default: today, UTC)")
    parser.add_argument("--env", choices=["dev", "uat", "prod"], default="dev")
    parser.add_argument("--lookback-months", type=int, default=12)
    parser.add_argument("--risk-score-threshold", type=int, default=700)
    args = parser.parse_args(argv)
    return RunParams(
        as_of_date=args.as_of_date or datetime.now(timezone.utc).date(),
        env=args.env,
        lookback_months=args.lookback_months,
        risk_score_threshold=args.risk_score_threshold,
    )


# --------------------------------------------------------------------------- table I/O


def _check_identifier(kind: str, name: str) -> str:
    if not isinstance(name, str) or not _IDENTIFIER.match(name):
        raise ValueError(f"invalid {kind} name {name!r}")
    return name


def _local_dir() -> str | None:
    return os.environ.get(LOCAL_DATA_DIR_ENV) or None


def _find_ci(directory: str, name: str) -> str | None:
    if not os.path.isdir(directory):
        return None
    for entry in sorted(os.listdir(directory)):
        if entry.lower() == name.lower():
            return os.path.join(directory, entry)
    return None


def _resolve_local_input(base: str, schema: str, table: str) -> tuple[str, str]:
    schema_dir = _find_ci(base, schema)
    for directory in ([schema_dir] if schema_dir else []) + [base]:
        for ext, fmt in ((".parquet", "parquet"), (".csv", "csv")):
            path = _find_ci(directory, table + ext)
            if path:
                return path, fmt
    raise FileNotFoundError(
        f"no local data for {schema}.{table} under {base} "
        f"(looked for {schema}/{table}.parquet|.csv and {table}.parquet|.csv)"
    )


def _local_output_path(base: str, schema: str, table: str) -> str:
    schema_dir = _find_ci(base, schema) or os.path.join(base, schema)
    existing = _find_ci(schema_dir, table + ".parquet")
    return existing or os.path.join(schema_dir, table + ".parquet")


def _read_local(spark, schema: str, table: str) -> DataFrame:
    path, fmt = _resolve_local_input(_local_dir(), schema, table)
    if fmt == "parquet":
        df = spark.read.parquet(path)
    else:
        df = spark.read.csv(path, header=True, inferSchema=True)
    return df.toDF(*[c.upper() for c in df.columns])


def _write_local(df: DataFrame, schema: str, table: str, mode: str,
                 pre_delete_where: str | None) -> None:
    path = _local_output_path(_local_dir(), schema, table)
    exists = os.path.exists(path)
    if pre_delete_where is None and not (mode == "overwrite" and exists):
        df.write.mode(mode).parquet(path)
        return
    combined = df
    if pre_delete_where is not None and exists:
        existing = df.sparkSession.read.parquet(path)
        kept = existing.filter(~F.coalesce(F.expr(pre_delete_where), F.lit(False)))
        combined = kept.unionByName(df, allowMissingColumns=True)
    # Write to a temp path first so a DataFrame derived from the target can overwrite it.
    tmp_path = f"{path}.tmp-{uuid.uuid4().hex}"
    combined.write.mode("overwrite").parquet(tmp_path)
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    os.replace(tmp_path, path)


def _import_synapse_connector():
    # Importing the module registers .synapsesql() on DataFrameReader / DataFrameWriter.
    import com.microsoft.spark.sqlanalytics  # noqa: F401
    from com.microsoft.spark.sqlanalytics.Constants import Constants
    return Constants


def _sql_server(spark) -> str:
    server = os.environ.get(SQL_SERVER_ENV)
    if server:
        return server
    workspace = spark.conf.get("spark.synapse.workspace.name", None)
    if not workspace:
        raise RuntimeError(f"cannot resolve the SQL endpoint: set {SQL_SERVER_ENV}")
    return f"{workspace}.sql.azuresynapse.net"


def _execute_sql_pool(spark, statement: str) -> None:
    import pyodbc
    from notebookutils import mssparkutils

    token = mssparkutils.credentials.getToken("DW").encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
    driver = os.environ.get(ODBC_DRIVER_ENV, DEFAULT_ODBC_DRIVER)
    conn_str = (f"Driver={{{driver}}};Server=tcp:{_sql_server(spark)},1433;"
                f"Database={SQL_POOL};Encrypt=yes;TrustServerCertificate=no;")
    conn = pyodbc.connect(conn_str, attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                          autocommit=True)
    try:
        conn.cursor().execute(statement)
    finally:
        conn.close()


def _with_server_option(builder, constants):
    server = os.environ.get(SQL_SERVER_ENV)
    return builder.option(constants.SERVER, server) if server else builder


def read_table(spark, schema: str, table: str) -> DataFrame:
    _check_identifier("schema", schema)
    _check_identifier("table", table)
    if _local_dir():
        return _read_local(spark, schema, table)
    constants = _import_synapse_connector()
    return _with_server_option(spark.read, constants).synapsesql(f"{SQL_POOL}.{schema}.{table}")


def write_table(df, schema: str, table: str, mode: str = "overwrite", pre_delete_where: str | None = None) -> None:
    _check_identifier("schema", schema)
    _check_identifier("table", table)
    mode = mode.lower()
    if mode not in _WRITE_MODES:
        raise ValueError(f"unsupported write mode {mode!r}; expected one of {sorted(_WRITE_MODES)}")
    if pre_delete_where is not None and not pre_delete_where.strip():
        raise ValueError("pre_delete_where must be a non-empty predicate or None")

    if _local_dir():
        _write_local(df, schema, table, mode, pre_delete_where)
        return

    constants = _import_synapse_connector()
    qualified = f"{schema}.{table}"
    if pre_delete_where is not None:
        _execute_sql_pool(df.sparkSession, f"DELETE FROM {qualified} WHERE {pre_delete_where};")
        mode = "append"
    elif mode == "overwrite":
        # TRUNCATE + append keeps the table's DISTRIBUTION / CCI DDL; the connector's
        # overwrite mode would drop and re-create the table with default settings.
        _execute_sql_pool(
            df.sparkSession,
            f"IF OBJECT_ID(N'{qualified}', N'U') IS NOT NULL TRUNCATE TABLE {qualified};",
        )
        mode = "append"
    _with_server_option(df.write, constants).mode(mode).synapsesql(f"{SQL_POOL}.{qualified}")


# --------------------------------------------------------------------------- audit log


def _fit(value: str | None, column: str) -> str | None:
    return None if value is None else str(value)[: _ETL_RUN_LOG_LENGTHS[column]]


def log_step(spark, job_name: str, step_name: str, status: str, row_count: int | None = None, msg: str | None = None) -> None:
    if not job_name or not status:
        raise ValueError("job_name and status are required")
    now = datetime.now(timezone.utc)
    key = (job_name, step_name)
    if status.upper() == "START":
        _step_start_ts[key] = now
        start_ts = now
    else:
        start_ts = _step_start_ts.pop(key, now)

    line = f"[PIPELINE] {now.isoformat(timespec='milliseconds')} | {job_name} | {step_name} | {status}"
    if msg:
        line += f" | {msg}"
    if row_count is not None:
        line += f" | Rows: {row_count}"
    log = logger.error if status.upper() in ("ERROR", "FAILED") else logger.info
    log(line)

    row = (
        _fit(job_name, "JOB_NAME"),
        _fit(step_name, "STEP_NAME"),
        _fit(status, "STATUS"),
        None if row_count is None else int(row_count),
        start_ts,
        now,
        _fit(msg, "MESSAGE"),
    )
    write_table(spark.createDataFrame([row], ETL_RUN_LOG_SCHEMA), "stg", "ETL_RUN_LOG", mode="append")


# --------------------------------------------------------------------------- validation


def validate_table(df, key_cols: list[str], not_null_cols: list[str], min_rows: int = 1) -> None:
    key_cols = list(key_cols or [])
    not_null_cols = list(not_null_cols or [])
    failures: list[str] = []

    columns = {c.upper(): c for c in df.columns}
    missing = [c for c in dict.fromkeys(key_cols + not_null_cols) if c.upper() not in columns]
    if missing:
        failures.append(f"missing columns: {', '.join(missing)}")
    keys = [columns[c.upper()] for c in key_cols if c.upper() in columns]
    not_null = [columns[c.upper()] for c in not_null_cols if c.upper() in columns]

    aggs = [F.count(F.lit(1)).alias("__rows")]
    aggs += [F.sum(F.col(c).isNull().cast("long")).alias(f"__null_{i}") for i, c in enumerate(not_null)]
    stats = df.agg(*aggs).collect()[0]

    n_rows = stats["__rows"]
    if n_rows < min_rows:
        failures.append(f"row count {n_rows} < min_rows {min_rows}")
    else:
        logger.info("[validate_table] Row count OK: %s (min: %s)", n_rows, min_rows)

    if keys:
        dup_groups = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
        if dup_groups:
            failures.append(f"{dup_groups} duplicate key groups on ({', '.join(keys)})")
        else:
            logger.info("[validate_table] Key uniqueness OK on (%s)", ", ".join(keys))

    for i, col in enumerate(not_null):
        nulls = stats[f"__null_{i}"] or 0
        if nulls:
            failures.append(f"{col} has {nulls} NULL values")

    if failures:
        message = f"validate_table failed with {len(failures)} issue(s): " + "; ".join(failures)
        logger.error("[validate_table] %s", message)
        raise ValidationError(message, failures)
    logger.info("[validate_table] All checks passed")
