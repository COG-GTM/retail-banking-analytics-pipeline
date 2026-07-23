"""ETL audit logging.

Minimal stub matching the shared Ticket-3 contract so this ticket's PR is
self-contained. Owned by Ticket 3 (shared utilities); superseded at merge.

Replaces the SAS ``%log_step`` macro + ``ETL_RUN_LOG`` table: every step appends
one row to ``etl_staging.etl_run_log`` AND emits a structured JSON log line.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .config import Config

_LOGGER = logging.getLogger("retail_banking.audit")
if not _LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(message)s"))
    _LOGGER.addHandler(_handler)
_LOGGER.setLevel(logging.INFO)

_AUDIT_SCHEMA = StructType(
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


def _audit_table(cfg: Config) -> str:
    return cfg.table(cfg.schema_stg, "etl_run_log")


def init_audit(spark: SparkSession, cfg: Config) -> None:
    """Create the ``etl_staging.etl_run_log`` Delta table if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_audit_table(cfg)} (
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
    row_count: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Append one audit row and emit a structured JSON log line."""
    log_ts = datetime.now(timezone.utc)
    row = (run_id, job_name, step, status, row_count, message, log_ts)
    spark.createDataFrame([row], schema=_AUDIT_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(_audit_table(cfg))

    _LOGGER.info(
        json.dumps(
            {
                "run_id": run_id,
                "job_name": job_name,
                "step": step,
                "status": status,
                "row_count": row_count,
                "message": message,
                "log_ts": log_ts.isoformat(),
            }
        )
    )


__all__ = ["init_audit", "log_step"]
