"""Structured, run-scoped logging and an in-session audit trail.

Replaces the SAS ``%log_step`` / ``%init_audit`` macros and the
``WORK.PIPELINE_AUDIT`` dataset. Every message is emitted through the standard
:mod:`logging` framework (never bare ``print``) and tagged with the run id and
pipeline step so log lines are traceable across a run.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

_LOGGER_NAME = "retail_banking_analytics"


def get_logger(run_id: str = "-") -> logging.LoggerAdapter:
    """Return a run-scoped logger adapter.

    Configures a single stream handler the first time it is called so that
    importing this module does not clobber a host application's logging setup.
    """
    logger = logging.getLogger(_LOGGER_NAME)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-7s | run=%(run_id)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logging.LoggerAdapter(logger, {"run_id": run_id})


@dataclass
class AuditRecord:
    job_name: str
    status: str
    message: str
    row_count: Optional[int]
    log_ts: str


@dataclass
class PipelineAudit:
    """In-session audit trail, the analogue of ``WORK.PIPELINE_AUDIT``.

    Use :meth:`log_step` to emit a structured log line *and* append an audit
    record in one call, mirroring the SAS ``%log_step`` macro.
    """

    run_id: str = "-"
    records: List[AuditRecord] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._log = get_logger(self.run_id)

    def log_step(
        self,
        step: str,
        status: str,
        msg: str = "",
        rowcount: Optional[int] = None,
    ) -> None:
        record = AuditRecord(
            job_name=step,
            status=status,
            message=msg,
            row_count=rowcount,
            log_ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        )
        self.records.append(record)

        parts = [f"{step}", f"{status}"]
        if msg:
            parts.append(msg)
        if rowcount is not None:
            parts.append(f"rows={rowcount}")
        line = " | ".join(parts)

        level = {
            "START": logging.INFO,
            "SUCCESS": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
        }.get(status.upper(), logging.INFO)
        self._log.log(level, line)
