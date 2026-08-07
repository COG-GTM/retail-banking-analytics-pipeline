"""Delta-backed ETL audit trail — replaces ``%init_audit`` / ``%log_step``.

Every step appends a START row and then either a SUCCESS row (with the row count
of the table it produced) or an ERROR row carrying the exception text, to
``<catalog>._ops.etl_run_log``. The same information the SAS macro wrote to
``WORK.PIPELINE_AUDIT`` plus the wall-clock duration of the step.
"""
from __future__ import annotations

import traceback
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_TABLE = "etl_run_log"

AUDIT_SCHEMA = StructType(
    [
        StructField("RUN_ID", StringType(), True),
        StructField("JOB_NAME", StringType(), True),
        StructField("STEP_NAME", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("MESSAGE", StringType(), True),
        StructField("ROW_COUNT", LongType(), True),
        StructField("DURATION_SEC", IntegerType(), True),
        StructField("START_TS", TimestampType(), True),
        StructField("END_TS", TimestampType(), True),
        StructField("LOG_TS", TimestampType(), True),
    ]
)


def create_audit_table(spark: SparkSession, table: str) -> None:
    """Create the audit table if it does not exist yet."""
    (
        spark.createDataFrame([], AUDIT_SCHEMA)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")
        .saveAsTable(table)
    )


class AuditLogger:
    """Append-only audit logger for one pipeline task."""

    def __init__(self, spark: SparkSession, table: str, job_name: str, run_id: str = ""):
        self.spark = spark
        self.table = table
        self.job_name = job_name
        self.run_id = run_id

    def _append(
        self,
        step: str,
        status: str,
        message: str = "",
        row_count: int | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        start = start_ts or now
        end = end_ts or now
        duration = int((end - start).total_seconds())
        row = [
            (
                self.run_id,
                self.job_name,
                step,
                status,
                message,
                None if row_count is None else int(row_count),
                duration,
                start,
                end,
                now,
            )
        ]
        df = self.spark.createDataFrame(row, AUDIT_SCHEMA)
        try:
            df.write.format("delta").mode("append").saveAsTable(self.table)
        except Exception as exc:  # audit must never break the pipeline
            print(f"WARNING: [audit] could not write to {self.table}: {exc}")
        print(f"[PIPELINE] {now.isoformat()} | {self.job_name} | {step} | {status} | {message}")

    def start(self, step: str, message: str = "") -> None:
        self._append(step, "START", message)

    def success(self, step: str, message: str = "", row_count: int | None = None) -> None:
        self._append(step, "SUCCESS", message, row_count)

    def error(self, step: str, message: str = "") -> None:
        self._append(step, "ERROR", message)

    @contextmanager
    def step(self, step: str, message: str = ""):
        """Context manager emitting START / SUCCESS / ERROR around a step.

        The block may set ``ctx.row_count`` so the SUCCESS row carries it::

            with audit.step("FULL_LOAD") as ctx:
                ctx.row_count = df.count()
        """
        started = datetime.now(timezone.utc)
        self._append(step, "START", message, start_ts=started, end_ts=started)
        ctx = _StepContext()
        try:
            yield ctx
        except Exception as exc:
            self._append(
                step,
                "ERROR",
                f"{type(exc).__name__}: {exc}"[:1000],
                start_ts=started,
                end_ts=datetime.now(timezone.utc),
            )
            traceback.print_exc()
            raise
        self._append(
            step,
            "SUCCESS",
            ctx.message or message,
            row_count=ctx.row_count,
            start_ts=started,
            end_ts=datetime.now(timezone.utc),
        )


class _StepContext:
    def __init__(self) -> None:
        self.row_count: int | None = None
        self.message: str = ""
