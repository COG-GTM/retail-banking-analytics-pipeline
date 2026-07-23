"""Ticket 3 - Audit logging.

Reimplements the two legacy logging mechanisms as a single Delta audit table:

* SAS ``%log_step`` / ``%init_audit`` (WORK.PIPELINE_AUDIT), and
* the BTEQ ``ETL_RUN_LOG`` audit inserts.

``init_audit`` creates the Delta audit table if it does not exist; ``log_step``
prints a structured line (the ``%put NOTE:`` equivalent) and appends one row to
that table so every pipeline step is traceable.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_TABLE_NAME = "etl_run_log"

_LOGGER = logging.getLogger("retail_banking_analytics")
if not _LOGGER.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("%(asctime)s [PIPELINE] %(message)s"))
    _LOGGER.addHandler(_handler)
    _LOGGER.setLevel(logging.INFO)

AUDIT_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("step_name", StringType(), True),
        StructField("status", StringType(), True),
        StructField("message", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("start_ts", TimestampType(), True),
        StructField("end_ts", TimestampType(), True),
        StructField("log_ts", TimestampType(), False),
    ]
)


def audit_table_fqn(config) -> str:
    """Fully-qualified name of the Delta audit table in the staging schema."""
    return config.staging(AUDIT_TABLE_NAME)


def init_audit(spark: SparkSession, config) -> str:
    """Create the Delta audit table if it does not already exist."""
    fqn = audit_table_fqn(config)
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {fqn} (
            job_name   STRING,
            step_name  STRING,
            status     STRING,
            message    STRING,
            row_count  BIGINT,
            start_ts   TIMESTAMP,
            end_ts     TIMESTAMP,
            log_ts     TIMESTAMP
        ) USING DELTA
        """
    )
    return fqn


def log_step(
    spark: SparkSession,
    config,
    job_name: str,
    status: str,
    step_name: Optional[str] = None,
    message: str = "",
    row_count: Optional[int] = None,
    start_ts: Optional[datetime] = None,
    end_ts: Optional[datetime] = None,
    persist: bool = True,
) -> None:
    """Emit a structured audit record (console + Delta table).

    Mirrors ``%log_step`` / the BTEQ ``ETL_RUN_LOG`` insert. Set ``persist=False``
    to only log to the console (used when the audit table is unavailable).
    """
    now = datetime.now()
    parts = [job_name]
    if step_name:
        parts.append(step_name)
    parts.append(status)
    line = " | ".join(parts)
    if message:
        line += f" :: {message}"
    if row_count is not None:
        line += f" :: rows={row_count}"
    _LOGGER.info(line)

    if not persist:
        return

    row = Row(
        job_name=job_name,
        step_name=step_name,
        status=status,
        message=message or None,
        row_count=int(row_count) if row_count is not None else None,
        start_ts=start_ts,
        end_ts=end_ts,
        log_ts=now,
    )
    df = spark.createDataFrame([row], schema=AUDIT_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(audit_table_fqn(config))
