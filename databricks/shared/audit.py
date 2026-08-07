"""Run auditing.

Replaces ``%log_step`` / ``%init_audit`` (``sas/macros/log_step.sas``) and the
``INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG`` statements at the tail of every BTEQ
script. Records land in a single Delta table ``<catalog>.<ops>.ETL_RUN_LOG`` and
are also emitted as structured JSON on stdout so they show up in the Databricks
job driver logs.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from pyspark.sql import SparkSession

from shared.config import PipelineConfig
from shared.logging_utils import get_logger, log_event
from shared.schemas import ETL_RUN_LOG_SCHEMA

RUN_LOG_TABLE = "ETL_RUN_LOG"

_logger = get_logger()


def ensure_run_log(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Create the audit table if it does not exist (``%init_audit``)."""
    columns = ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in ETL_RUN_LOG_SCHEMA.fields)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {cfg.ops(RUN_LOG_TABLE)} ({columns}) "
        f"USING {cfg.table_format}"
    )


def log_step(
    spark: SparkSession,
    cfg: PipelineConfig,
    job_name: str,
    step_name: str,
    status: str,
    message: str = "",
    row_count: int | None = None,
    duration_sec: float | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> None:
    """Append one audit record and emit the equivalent structured log line."""
    now = datetime.now()
    record = {
        "RUN_ID": cfg.run_id,
        "JOB_NAME": job_name,
        "STEP_NAME": step_name,
        "STATUS": status,
        "MESSAGE": message,
        "ROW_COUNT": int(row_count) if row_count is not None else None,
        "DURATION_SEC": Decimal(f"{duration_sec:.3f}") if duration_sec is not None else None,
        "START_TS": start_ts or now,
        "END_TS": end_ts or now,
    }
    log_event(
        _logger,
        "etl_step",
        run_id=cfg.run_id,
        job=job_name,
        step=step_name,
        status=status,
        message=message or None,
        row_count=record["ROW_COUNT"],
        duration_sec=round(duration_sec, 3) if duration_sec is not None else None,
    )
    spark.createDataFrame([record], schema=ETL_RUN_LOG_SCHEMA).write.format(
        cfg.table_format
    ).mode("append").saveAsTable(cfg.ops(RUN_LOG_TABLE))


@contextmanager
def step(
    spark: SparkSession,
    cfg: PipelineConfig,
    job_name: str,
    step_name: str,
    row_count_fn: Callable[[], int] | None = None,
) -> Iterator[dict[str, Any]]:
    """Context manager wrapping a pipeline step with START/SUCCESS/ERROR rows.

    The yielded dict accepts ``row_count`` and ``message`` so the body can report
    what it produced::

        with step(spark, cfg, JOB, "FULL_LOAD") as ctx:
            ctx["row_count"] = df.count()
    """
    start = datetime.now()
    started = time.monotonic()
    log_step(spark, cfg, job_name, step_name, "START", start_ts=start, end_ts=start)
    ctx: dict[str, Any] = {"row_count": None, "message": ""}
    try:
        yield ctx
    except Exception as exc:  # noqa: BLE001 - re-raised after auditing
        log_step(
            spark,
            cfg,
            job_name,
            step_name,
            "ERROR",
            message=f"{type(exc).__name__}: {exc}"[:1000],
            duration_sec=time.monotonic() - started,
            start_ts=start,
        )
        raise
    row_count = ctx.get("row_count")
    if row_count is None and row_count_fn is not None:
        row_count = row_count_fn()
    log_step(
        spark,
        cfg,
        job_name,
        step_name,
        "SUCCESS",
        message=ctx.get("message", ""),
        row_count=row_count,
        duration_sec=time.monotonic() - started,
        start_ts=start,
    )
