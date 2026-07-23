"""ETL audit trail and structured logging.

Replaces the SAS ``%log_step`` / ``%init_audit`` macros, the ``ETL_RUN_LOG``
table and the in-session ``WORK.PIPELINE_AUDIT`` dataset. Each step both appends
a durable row to ``etl_staging.etl_run_log`` (a Delta table) and emits a
structured JSON log line carrying the ``run_id`` — no bare ``print``.
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

from common.config import Config

AUDIT_TABLE = "etl_run_log"

VALID_STATUSES = frozenset({"START", "SUCCESS", "WARNING", "ERROR"})

AUDIT_SCHEMA = StructType(
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

_LOGGER_NAME = "retail_banking.audit"


def _get_logger() -> logging.Logger:
    """Return a module logger that emits each record as a single JSON line."""
    logger = logging.getLogger(_LOGGER_NAME)
    if not getattr(logger, "_rb_configured", False):
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger._rb_configured = True  # type: ignore[attr-defined]
    return logger


def _audit_table_name(cfg: Config) -> str:
    return cfg.table(cfg.schema_stg, AUDIT_TABLE)


def init_audit(spark: SparkSession, cfg: Config) -> str:
    """Create the ``etl_staging.etl_run_log`` Delta table if it does not exist.

    Also ensures the staging schema exists so the table can be created on a fresh
    catalog. Idempotent. Returns the fully-qualified table name.
    """
    table = _audit_table_name(cfg)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            run_id    STRING,
            job_name  STRING,
            step      STRING,
            status    STRING,
            row_count BIGINT,
            message   STRING,
            log_ts    TIMESTAMP
        ) USING DELTA
        """
    )
    return table


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
    """Append one audit row and emit a structured JSON log line.

    ``status`` is one of START | SUCCESS | WARNING | ERROR. The audit row is
    written to the Delta table (append, one row) and the same payload is logged
    as JSON at a severity derived from ``status``.
    """
    status = status.upper()
    if status not in VALID_STATUSES:
        raise ValueError(
            f"invalid status {status!r}; expected one of {sorted(VALID_STATUSES)}"
        )

    log_ts = datetime.now(timezone.utc)
    row = (run_id, job_name, step, status, row_count, message, log_ts)
    spark.createDataFrame([row], schema=AUDIT_SCHEMA).write.format("delta").mode(
        "append"
    ).saveAsTable(_audit_table_name(cfg))

    payload = {
        "run_id": run_id,
        "job_name": job_name,
        "step": step,
        "status": status,
        "row_count": row_count,
        "message": message,
        "log_ts": log_ts.isoformat(),
    }
    logger = _get_logger()
    level = logging.ERROR if status == "ERROR" else (
        logging.WARNING if status == "WARNING" else logging.INFO
    )
    logger.log(level, json.dumps(payload))
