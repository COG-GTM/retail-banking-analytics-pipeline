"""ETL audit trail + structured logging.

Replaces the SAS ``%log_step`` macro and the ``ETL_RUN_LOG`` table.
``init_audit`` creates ``etl_staging.etl_run_log``; ``log_step`` appends one row
per step AND emits a structured JSON log line (no bare ``print``).

NOTE (parallel-ticket stub): tickets 1/2/3 own the canonical version; this is a
minimal contract-compatible implementation for Ticket 9.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_LOGGER_NAME = "retail_banking.pipeline"

_RUN_LOG_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("step", StringType(), True),
        StructField("status", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("message", StringType(), True),
        StructField("log_ts", TimestampType(), True),
    ]
)


def _get_logger(cfg=None) -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    if cfg is not None:
        logger.setLevel(getattr(logging, str(cfg.log_level).upper(), logging.INFO))
    return logger


def _run_log_table(cfg) -> str:
    return cfg.table(cfg.schema_stg, "etl_run_log")


def init_audit(spark: SparkSession, cfg) -> None:
    """Create the ``etl_staging.etl_run_log`` Delta table if it does not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    table = _run_log_table(cfg)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
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
    cfg,
    run_id: str,
    job_name: str,
    step: str,
    status: str,
    row_count: int | None = None,
    message: str | None = None,
) -> None:
    """Append one audit row and emit a structured JSON log line."""
    log_ts = datetime.now(timezone.utc)
    row = Row(
        run_id=run_id,
        job_name=job_name,
        step=step,
        status=status,
        row_count=int(row_count) if row_count is not None else None,
        message=message,
        log_ts=log_ts,
    )
    spark.createDataFrame([row], schema=_RUN_LOG_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(_run_log_table(cfg))

    payload = {
        "run_id": run_id,
        "job_name": job_name,
        "step": step,
        "status": status,
        "row_count": row_count,
        "message": message,
        "log_ts": log_ts.isoformat(),
    }
    _get_logger(cfg).info(json.dumps(payload))
