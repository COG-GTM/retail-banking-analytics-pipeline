"""ETL run auditing / structured logging.

Minimal stub matching the shared Ticket-3 contract so this ticket's PR is
self-contained; the owning ticket's version supersedes it at merge. Replaces the
SAS ``%log_step`` macro + ``ETL_RUN_LOG`` table.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import Config

_LOGGER = logging.getLogger("retail_banking.etl")
if not _LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _LOGGER.addHandler(_handler)
    _LOGGER.setLevel(logging.INFO)

_RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("job_name", StringType(), False),
        StructField("step", StringType(), False),
        StructField("status", StringType(), False),
        StructField("row_count", LongType(), True),
        StructField("message", StringType(), True),
        StructField("log_ts", TimestampType(), False),
    ]
)


def _run_log_table(cfg: Config) -> str:
    return cfg.table(cfg.schema_stg, "etl_run_log")


def init_audit(spark: SparkSession, cfg: Config) -> None:
    """Create the ``etl_staging.etl_run_log`` Delta table if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_run_log_table(cfg)} (
            run_id STRING,
            job_name STRING,
            step STRING,
            status STRING,
            row_count BIGINT,
            message STRING,
            log_ts TIMESTAMP
        ) USING DELTA
        """
    )


def log_step(
    spark: SparkSession,
    cfg: Config,
    run_id: str,
    job_name: str,
    step: str,
    status: str,
    row_count: int | None = None,
    message: str | None = None,
) -> None:
    """Append one audit row to ``etl_run_log`` and emit a structured JSON log line."""
    log_ts = _dt.datetime.now()
    payload = {
        "run_id": run_id,
        "job_name": job_name,
        "step": step,
        "status": status,
        "row_count": row_count,
        "message": message,
        "log_ts": log_ts.isoformat(),
    }
    _LOGGER.info(json.dumps(payload))

    try:
        row = Row(
            run_id=run_id,
            job_name=job_name,
            step=step,
            status=status,
            row_count=int(row_count) if row_count is not None else None,
            message=message,
            log_ts=log_ts,
        )
        df = spark.createDataFrame([row], schema=_RUN_LOG_SCHEMA)
        df.write.format("delta").mode("append").saveAsTable(_run_log_table(cfg))
    except Exception as exc:  # pragma: no cover - audit must never break a job
        _LOGGER.warning(json.dumps({"event": "audit_write_failed", "error": str(exc)}))


__all__ = ["init_audit", "log_step"]
