"""Structured pipeline logging.

Replaces ``%log_step`` / ``%init_audit``: every step emits a log line and an
audit row. The rows are buffered in-process (the WORK.PIPELINE_AUDIT
equivalent) and appended to the shared run-log table so the audit trail
survives the Spark session.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline_utils.config import PipelineConfig
from pipeline_utils.snowflake_io import SnowflakeIO

LOGGER = logging.getLogger("retail_banking.pipeline")

START = "START"
SUCCESS = "SUCCESS"
WARNING = "WARNING"
ERROR = "ERROR"

RUN_LOG_SCHEMA = StructType(
    [
        StructField("RUN_ID", StringType(), False),
        StructField("JOB_NAME", StringType(), False),
        StructField("STEP_NAME", StringType(), False),
        StructField("STATUS", StringType(), False),
        StructField("MESSAGE", StringType(), True),
        StructField("ROW_COUNT", IntegerType(), True),
        StructField("LOG_TS", TimestampType(), False),
    ]
)


@dataclass(frozen=True)
class RunLogEntry:
    run_id: str
    job_name: str
    step_name: str
    status: str
    message: str | None
    row_count: int | None
    log_ts: datetime

    def as_row(self) -> Row:
        return Row(
            RUN_ID=self.run_id,
            JOB_NAME=self.job_name,
            STEP_NAME=self.step_name,
            STATUS=self.status,
            MESSAGE=self.message,
            ROW_COUNT=self.row_count,
            LOG_TS=self.log_ts,
        )


class RunLogger:
    """Collects audit entries for one job run and persists them to Snowflake."""

    def __init__(
        self,
        spark: SparkSession,
        job_name: str,
        config: PipelineConfig,
        io: SnowflakeIO | None = None,
        run_id: str | None = None,
    ) -> None:
        self._spark = spark
        self._config = config
        self._io = io
        self.job_name = job_name
        self.run_id = run_id or str(uuid.uuid4())
        self._entries: list[RunLogEntry] = []

    @property
    def entries(self) -> Sequence[RunLogEntry]:
        return tuple(self._entries)

    def log_step(
        self,
        step: str,
        status: str,
        msg: str | None = None,
        rowcount: int | None = None,
    ) -> RunLogEntry:
        entry = RunLogEntry(
            run_id=self.run_id,
            job_name=self.job_name,
            step_name=step,
            status=status,
            message=msg,
            row_count=rowcount,
            log_ts=datetime.now(timezone.utc),
        )
        self._entries.append(entry)

        level = logging.ERROR if status == ERROR else (
            logging.WARNING if status == WARNING else logging.INFO
        )
        LOGGER.log(
            level,
            "[PIPELINE] %s | %s | %s | %s%s",
            entry.log_ts.isoformat(timespec="milliseconds"),
            self.job_name,
            step,
            status,
            f" | {msg}" if msg else "",
        )
        if rowcount is not None:
            LOGGER.log(level, "[PIPELINE] Rows: %s", rowcount)
        return entry

    def audit_trail(self) -> list[dict[str, object]]:
        """The WORK.PIPELINE_AUDIT print-out, as plain dictionaries."""
        return [
            {
                "RUN_ID": e.run_id,
                "JOB_NAME": e.job_name,
                "STEP_NAME": e.step_name,
                "STATUS": e.status,
                "MESSAGE": e.message,
                "ROW_COUNT": e.row_count,
                "LOG_TS": e.log_ts,
            }
            for e in self._entries
        ]

    def flush(self) -> int:
        """Append buffered entries to the shared run-log table.

        Returns the number of rows written. Flushing is always safe to call
        (including from an error handler): an empty buffer is a no-op.
        """
        if not self._entries:
            return 0
        rows = [entry.as_row() for entry in self._entries]
        df = self._spark.createDataFrame(rows, schema=RUN_LOG_SCHEMA)
        if self._io is not None:
            self._io.append_table(
                df, self._config.run_log_schema, self._config.run_log_table
            )
        written = len(rows)
        self._entries.clear()
        return written
