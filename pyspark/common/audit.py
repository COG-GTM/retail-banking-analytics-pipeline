"""Audit trail + structured logging.

Ports two legacy artefacts:

* ``sas/macros/log_step.sas`` -> :func:`log_step` and the in-session
  ``PIPELINE_AUDIT`` dataset (:data:`PIPELINE_AUDIT_SCHEMA`).
* the BTEQ ``ETL_RUN_LOG`` audit inserts -> :data:`ETL_RUN_LOG_SCHEMA` and
  :meth:`AuditLog.run_log_row`.

Logging is structured JSON with a per-run correlation id (Rules R6 -- no bare
``print``), so every stage is traceable across the DAG.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import uuid
from dataclasses import dataclass, field

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# WORK.PIPELINE_AUDIT (log_step.sas: JOB_NAME $40, STATUS $10, MESSAGE $200,
# ROW_COUNT num, LOG_TS datetime).
PIPELINE_AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("job_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("row_count", LongType(), True),
    StructField("log_ts", TimestampType(), True),
])

# ETL_STAGING_DB.ETL_RUN_LOG (BTEQ inserts: JOB_NAME, STEP_NAME, STATUS,
# ROW_COUNT, START_TS, END_TS).
ETL_RUN_LOG_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("job_name", StringType(), True),
    StructField("step_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("row_count", IntegerType(), True),
    StructField("start_ts", TimestampType(), True),
    StructField("end_ts", TimestampType(), True),
])

VALID_STATUSES = ("START", "SUCCESS", "WARNING", "ERROR")


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": _dt.datetime.fromtimestamp(record.created, _dt.timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("run_id", "job_name", "status", "row_count"):
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


@dataclass
class AuditRecord:
    job_name: str
    status: str
    message: str = ""
    row_count: int | None = None
    step_name: str = "FULL_LOAD"
    start_ts: _dt.datetime | None = None
    end_ts: _dt.datetime | None = None
    log_ts: _dt.datetime = field(default_factory=lambda: _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None))


@dataclass
class AuditLog:
    """Accumulates audit records for a run and materialises them as DataFrames.

    Mirrors ``%init_audit`` (creating the empty structured dataset) plus every
    ``%log_step`` call and BTEQ ``ETL_RUN_LOG`` insert.
    """

    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    log_level: str = "INFO"
    records: list[AuditRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._logger = get_logger("rbap.audit", self.log_level)

    def log_step(
        self,
        step: str,
        status: str,
        msg: str = "",
        rowcount: int | None = None,
    ) -> AuditRecord:
        """Port of ``%log_step`` -- structured log line + audit append."""
        if status not in VALID_STATUSES:
            raise ValueError(f"invalid status {status!r}; expected one of {VALID_STATUSES}")
        record = AuditRecord(job_name=step, status=status, message=msg, row_count=rowcount)
        self.records.append(record)
        level = logging.ERROR if status == "ERROR" else (
            logging.WARNING if status == "WARNING" else logging.INFO
        )
        self._logger.log(
            level,
            msg or status,
            extra={"run_id": self.run_id, "job_name": step, "status": status, "row_count": rowcount},
        )
        return record

    def run_log_row(
        self,
        job_name: str,
        row_count: int,
        step_name: str = "FULL_LOAD",
        status: str = "SUCCESS",
        start_ts: _dt.datetime | None = None,
        end_ts: _dt.datetime | None = None,
    ) -> AuditRecord:
        """Port of the BTEQ ``INSERT INTO ETL_RUN_LOG`` completion record."""
        now = _dt.datetime.now(_dt.timezone.utc).replace(tzinfo=None)
        record = AuditRecord(
            job_name=job_name, status=status, row_count=row_count,
            step_name=step_name, start_ts=start_ts or now, end_ts=end_ts or now,
        )
        self.records.append(record)
        return record

    # -- Materialisers --------------------------------------------------------
    def pipeline_audit_df(self, spark: SparkSession):
        rows = [
            (self.run_id, r.job_name, r.status, r.message,
             None if r.row_count is None else int(r.row_count), r.log_ts)
            for r in self.records
        ]
        return spark.createDataFrame(rows, schema=PIPELINE_AUDIT_SCHEMA)

    def etl_run_log_df(self, spark: SparkSession):
        rows = [
            (self.run_id, r.job_name, r.step_name, r.status,
             None if r.row_count is None else int(r.row_count), r.start_ts, r.end_ts)
            for r in self.records if r.start_ts is not None
        ]
        return spark.createDataFrame(rows, schema=ETL_RUN_LOG_SCHEMA)
