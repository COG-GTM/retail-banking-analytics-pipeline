"""Structured pipeline logging.

Replaces ``sas/macros/log_step.sas`` (``%log_step`` / ``%init_audit``): every
call emits a log line and appends a row to the shared Snowflake run-log table
(``DATA_PRODUCTS_<ENV>.DATA_PRODUCTS.PIPELINE_RUN_LOG``) instead of the
in-session ``WORK.PIPELINE_AUDIT`` dataset.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

LOGGER = logging.getLogger(__name__)


class Status:
    START = "START"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"
    ERROR = "ERROR"

    ALL = (START, SUCCESS, WARNING, ERROR)


@dataclass(frozen=True)
class RunLogRecord:
    """One row of the run-log table (the SAS WORK.PIPELINE_AUDIT equivalent)."""

    run_id: str
    job_name: str
    status: str
    message: str
    row_count: Optional[int]
    log_ts: datetime

    def as_row(self) -> tuple:
        return (
            self.run_id,
            self.job_name,
            self.status,
            self.message,
            self.row_count,
            self.log_ts,
        )


RUN_LOG_COLUMNS = ["RUN_ID", "JOB_NAME", "STATUS", "MESSAGE", "ROW_COUNT", "LOG_TS"]


class RunLogger:
    """Collect run-log records and flush them to the shared run-log table.

    Records are buffered in memory and flushed either explicitly or when
    ``autoflush`` is set, so a job that aborts mid-run still records why.
    """

    MESSAGE_MAX_LEN = 200
    JOB_NAME_MAX_LEN = 40

    def __init__(
        self,
        job_name: str,
        run_id: str = "",
        sink: Optional["RunLogSink"] = None,
        autoflush: bool = True,
        clock=lambda: datetime.now(timezone.utc),
    ) -> None:
        self.job_name = job_name
        self.run_id = run_id
        self.sink = sink
        self.autoflush = autoflush
        self._clock = clock
        self._records: List[RunLogRecord] = []

    @property
    def records(self) -> List[RunLogRecord]:
        return list(self._records)

    def log_step(
        self,
        step: str,
        status: str,
        msg: str = "",
        rowcount: Optional[int] = None,
    ) -> RunLogRecord:
        if status not in Status.ALL:
            raise ValueError(f"Unknown status '{status}', expected one of {Status.ALL}")

        record = RunLogRecord(
            run_id=self.run_id,
            job_name=step[: self.JOB_NAME_MAX_LEN],
            status=status,
            message=msg[: self.MESSAGE_MAX_LEN],
            row_count=rowcount,
            log_ts=self._clock(),
        )
        self._records.append(record)

        level = logging.ERROR if status == Status.ERROR else (
            logging.WARNING if status == Status.WARNING else logging.INFO
        )
        LOGGER.log(
            level,
            "[PIPELINE] %s | %s | %s%s",
            record.job_name,
            record.status,
            record.message,
            "" if record.row_count is None else f" | rows={record.row_count}",
        )

        if self.autoflush:
            self.flush()
        return record

    def flush(self) -> int:
        """Persist buffered records; returns the number of rows written."""
        if not self._records or self.sink is None:
            return 0
        pending, self._records = self._records, []
        self.sink.write(pending)
        return len(pending)

    def audit_trail(self) -> List[RunLogRecord]:
        """The SAS ``proc print data=WORK.PIPELINE_AUDIT`` equivalent."""
        return self.records


class RunLogSink:
    """Persist run-log records to the shared Snowflake run-log table."""

    def __init__(self, spark, snowflake_io, table_fqn: str) -> None:
        self._spark = spark
        self._io = snowflake_io
        self._table_fqn = table_fqn

    def write(self, records: List[RunLogRecord]) -> None:
        if not records:
            return
        df = self._spark.createDataFrame(
            [record.as_row() for record in records], schema=self._schema()
        )
        self._io.write_table(df, self._table_fqn, mode="append")

    @staticmethod
    def _schema():
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        return StructType(
            [
                StructField("RUN_ID", StringType(), True),
                StructField("JOB_NAME", StringType(), True),
                StructField("STATUS", StringType(), True),
                StructField("MESSAGE", StringType(), True),
                StructField("ROW_COUNT", LongType(), True),
                StructField("LOG_TS", TimestampType(), True),
            ]
        )


class InMemoryRunLogSink(RunLogSink):
    """Sink used by unit tests and ``--dry-run`` executions."""

    def __init__(self) -> None:  # pylint: disable=super-init-not-called
        self.written: List[RunLogRecord] = []

    def write(self, records: List[RunLogRecord]) -> None:
        self.written.extend(records)
