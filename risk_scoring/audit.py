"""In-session audit trail — the PySpark port of ``sas/macros/log_step.sas``.

``%init_audit`` created an empty ``WORK.PIPELINE_AUDIT`` dataset with structure;
``%log_step`` wrote a banner to the SAS log *and* inserted one row into it.
:class:`AuditLog` keeps both halves: structured records emitted through the
``logging`` module and an accumulating trail exposed as a DataFrame.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger(__name__)

#: ``%log_step`` accepted exactly these four statuses.
VALID_STATUSES = ("START", "SUCCESS", "WARNING", "ERROR")

#: Schema of ``WORK.PIPELINE_AUDIT``.
PIPELINE_AUDIT_SCHEMA = StructType([
    StructField("JOB_NAME", StringType(), True),
    StructField("STATUS", StringType(), True),
    StructField("MESSAGE", StringType(), True),
    StructField("ROW_COUNT", LongType(), True),
    StructField("LOG_TS", TimestampType(), True),
])

_LEVELS = {"ERROR": logging.ERROR, "WARNING": logging.WARNING}


@dataclass(frozen=True)
class AuditRecord:
    """One ``%log_step`` invocation.

    ``job_name`` carries the ``step=`` argument because the SAS macro inserted
    ``step`` into the ``JOB_NAME`` column.
    """

    job_name: str
    status: str
    message: str
    row_count: int | None
    log_ts: datetime


class AuditLog:
    """``%init_audit`` plus ``%log_step``, scoped to one pipeline run."""

    def __init__(self, job_name: str) -> None:
        self.job_name = job_name
        self.run_id = uuid.uuid4().hex
        self._records: list[AuditRecord] = []

    @property
    def records(self) -> list[AuditRecord]:
        return list(self._records)

    def log_step(
        self,
        *,
        step: str,
        status: str,
        msg: str = "",
        rowcount: int | None = None,
    ) -> None:
        if status not in VALID_STATUSES:
            raise ValueError(
                f"Invalid audit status {status!r}; expected one of "
                f"{', '.join(VALID_STATUSES)}"
            )
        record = AuditRecord(
            job_name=step,
            status=status,
            message=msg,
            row_count=rowcount,
            log_ts=datetime.now(timezone.utc),
        )
        self._records.append(record)
        logger.log(
            _LEVELS.get(status, logging.INFO),
            "pipeline_audit job=%s run_id=%s step=%s status=%s rows=%s msg=%s",
            self.job_name,
            self.run_id,
            step,
            status,
            "" if rowcount is None else rowcount,
            msg,
        )

    def to_dataframe(self, spark: SparkSession) -> DataFrame:
        """Return the trail as ``WORK.PIPELINE_AUDIT``.

        The explicit schema keeps the empty case working, mirroring
        ``%init_audit``'s zero-row dataset with structure.
        """
        rows = [
            (r.job_name, r.status, r.message, r.row_count, r.log_ts)
            for r in self._records
        ]
        return spark.createDataFrame(rows, schema=PIPELINE_AUDIT_SCHEMA)
