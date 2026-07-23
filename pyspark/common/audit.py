"""Structured audit logging.

Replaces the SAS ``%log_step`` macro + ``WORK.PIPELINE_AUDIT`` / ``ETL_RUN_LOG``.
``init_audit`` creates the ``etl_staging.etl_run_log`` Delta table; ``log_step``
appends one row per step AND emits a structured JSON log line (never a bare
``print``) so runs are traceable both in the lakehouse and in the driver logs.

NOTE (parallel-migration stub): Ticket 3 owns the canonical implementation.
This contract-compatible stub keeps ``init_audit`` / ``log_step`` signatures
identical so it is a drop-in for the owning ticket's version.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
from typing import Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql import types as T

from common.config import Config
from common.spark import table_path, write_delta

_AUDIT_TABLE = "etl_run_log"

_AUDIT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), True),
        T.StructField("job_name", T.StringType(), True),
        T.StructField("step", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("row_count", T.LongType(), True),
        T.StructField("message", T.StringType(), True),
        T.StructField("log_ts", T.TimestampType(), True),
    ]
)

_logger = logging.getLogger("retail_banking.pipeline")
if not _logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False


def init_audit(spark: SparkSession, cfg: Config) -> None:
    """Create the ``etl_staging.etl_run_log`` Delta table if absent."""
    from delta.tables import DeltaTable

    path = table_path(cfg, cfg.schema_stg, _AUDIT_TABLE)
    if not DeltaTable.isDeltaTable(spark, path):
        empty = spark.createDataFrame([], schema=_AUDIT_SCHEMA)
        empty.write.format("delta").mode("overwrite").save(path)


def log_step(
    spark: SparkSession,
    cfg: Config,
    run_id: str,
    job_name: str,
    step: str,
    status: str,
    row_count: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Append one audit row and emit a structured JSON log line."""
    now = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
    row = Row(
        run_id=run_id,
        job_name=job_name,
        step=step,
        status=status,
        row_count=int(row_count) if row_count is not None else None,
        message=message,
        log_ts=now,
    )
    df = spark.createDataFrame([row], schema=_AUDIT_SCHEMA)
    write_delta(df, cfg, cfg.schema_stg, _AUDIT_TABLE, mode="append")

    _logger.info(
        json.dumps(
            {
                "run_id": run_id,
                "job_name": job_name,
                "step": step,
                "status": status,
                "row_count": row_count,
                "message": message,
                "log_ts": now.isoformat(),
            }
        )
    )
