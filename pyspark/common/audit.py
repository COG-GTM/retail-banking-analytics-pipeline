"""Audit trail.

Port of ``sas/macros/log_step.sas`` (``%log_step`` / ``%init_audit``) and of the
``INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG`` statements that close every BTEQ script.

``PIPELINE_AUDIT`` collects one row per step (the SAS in-session trail), ``ETL_RUN_LOG``
one row per job (the BTEQ audit insert). Both are buffered in memory and flushed through the
:class:`~common.io.DataIO` in use, so a run against PostgreSQL persists its audit trail to the
database exactly like the legacy run did.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession

from common import schemas
from common.io import DataIO
from common.schemas import enforce_schema

LOGGER = logging.getLogger(__name__)

START = "START"
SUCCESS = "SUCCESS"
WARNING = "WARNING"
ERROR = "ERROR"


@dataclass(frozen=True)
class AuditRecord:
    """One ``%log_step`` call."""

    job_name: str
    status: str
    message: str
    row_count: int | None
    log_ts: datetime


@dataclass(frozen=True)
class RunLogRecord:
    """One ``ETL_RUN_LOG`` insert."""

    job_name: str
    step_name: str
    status: str
    row_count: int | None
    start_ts: datetime
    end_ts: datetime


@dataclass
class AuditLog:
    """Buffered audit trail shared by every job in a run."""

    run_timestamp: str = ""
    steps: list[AuditRecord] = field(default_factory=list)
    runs: list[RunLogRecord] = field(default_factory=list)

    def log_step(
        self,
        step: str,
        status: str,
        msg: str = "",
        rowcount: int | None = None,
        *,
        now: datetime | None = None,
    ) -> AuditRecord:
        """Port of ``%log_step(step=, status=, msg=, rowcount=)``."""

        record = AuditRecord(
            job_name=step,
            status=status,
            message=msg,
            row_count=rowcount,
            log_ts=now or datetime.now(),
        )
        self.steps.append(record)
        LOGGER.info(
            "[PIPELINE] %s | %s | %s%s",
            record.log_ts.isoformat(timespec="milliseconds"),
            step,
            status,
            f" | rows={rowcount}" if rowcount is not None else "",
        )
        if msg:
            LOGGER.info("[PIPELINE] %s", msg)
        return record

    def log_run(
        self,
        job_name: str,
        step_name: str,
        status: str,
        row_count: int | None,
        start_ts: datetime,
        end_ts: datetime,
    ) -> RunLogRecord:
        """Port of the ``INSERT INTO ETL_STAGING_DB.ETL_RUN_LOG`` tail of each BTEQ script."""

        record = RunLogRecord(
            job_name=job_name,
            step_name=step_name,
            status=status,
            row_count=row_count,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        self.runs.append(record)
        return record

    # -- persistence -------------------------------------------------------------------

    def steps_dataframe(self, spark: SparkSession) -> DataFrame:
        rows = [(r.job_name, r.status, r.message, r.row_count, r.log_ts) for r in self.steps] or []
        df = spark.createDataFrame(rows, schema=schemas.PIPELINE_AUDIT.spark_schema())
        return enforce_schema(df, schemas.PIPELINE_AUDIT)

    def runs_dataframe(self, spark: SparkSession) -> DataFrame:
        rows = [
            (r.job_name, r.step_name, r.status, r.row_count, r.start_ts, r.end_ts)
            for r in self.runs
        ] or []
        df = spark.createDataFrame(rows, schema=schemas.ETL_RUN_LOG.spark_schema())
        return enforce_schema(df, schemas.ETL_RUN_LOG)

    def flush(self, spark: SparkSession, io: DataIO, *, mode: str = "append") -> dict[str, int]:
        """Persist both audit tables through the configured IO layer."""

        written = {
            schemas.PIPELINE_AUDIT.name: io.write_spec(
                self.steps_dataframe(spark), schemas.PIPELINE_AUDIT, mode=mode
            ),
            schemas.ETL_RUN_LOG.name: io.write_spec(
                self.runs_dataframe(spark), schemas.ETL_RUN_LOG, mode=mode
            ),
        }
        LOGGER.info("flushed audit trail: %s", written)
        return written

    def as_dicts(self) -> dict[str, list[dict[str, object]]]:
        return {
            "steps": [asdict(record) for record in self.steps],
            "runs": [asdict(record) for record in self.runs],
        }
