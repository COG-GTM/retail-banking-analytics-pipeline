"""Audit trail + structured logging (audit contract).

Replaces the SAS ``%log_step`` macro and the BTEQ ``ETL_RUN_LOG`` inserts.
``init_audit`` creates the Delta ``etl_staging.etl_run_log`` table; ``log_step``
appends one row AND emits a structured JSON log line (no bare ``print``).

Minimal shared stub owned by Ticket 3; signatures kept identical to the contract.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
from typing import Optional

from pyspark.sql import Row, SparkSession

_ETL_RUN_LOG = "etl_run_log"

_RUN_LOG_DDL_COLS = (
    "run_id STRING, "
    "job_name STRING, "
    "step STRING, "
    "status STRING, "
    "row_count BIGINT, "
    "message STRING, "
    "log_ts TIMESTAMP"
)


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": _dt.datetime.fromtimestamp(
                record.created, _dt.timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("run_id", "job_name", "step", "status", "row_count"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return json.dumps(payload)


def get_logger(name: str = "rbap", level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(name)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger


def init_audit(spark: SparkSession, cfg) -> str:
    """Create the ``etl_staging.etl_run_log`` Delta table if it is absent.

    Returns the fully-qualified table name.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    table = cfg.table(cfg.schema_stg, _ETL_RUN_LOG)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({_RUN_LOG_DDL_COLS}) USING delta"
    )
    return table


def log_step(
    spark: SparkSession,
    cfg,
    run_id: str,
    job_name: str,
    step: str,
    status: str,
    row_count: Optional[int] = None,
    message: Optional[str] = None,
) -> None:
    """Append one audit row to ``etl_run_log`` and emit a structured log line."""
    log_ts = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
    row = Row(
        run_id=run_id,
        job_name=job_name,
        step=step,
        status=status,
        row_count=int(row_count) if row_count is not None else None,
        message=message,
        log_ts=log_ts,
    )
    table = cfg.table(cfg.schema_stg, _ETL_RUN_LOG)
    (
        spark.createDataFrame([row], schema=_RUN_LOG_DDL_COLS)
        .write.format("delta")
        .mode("append")
        .saveAsTable(table)
    )

    logger = get_logger("rbap.audit", getattr(cfg, "log_level", "INFO"))
    level = logging.ERROR if status == "ERROR" else (
        logging.WARNING if status == "WARNING" else logging.INFO
    )
    logger.log(
        level,
        message or status,
        extra={
            "run_id": run_id,
            "job_name": job_name,
            "step": step,
            "status": status,
            "row_count": row_count,
        },
    )
