"""Structured JSON logging shared by every notebook and helper.

Replaces the free-form ``%put`` / ``.REMARK`` output of the SAS and BTEQ jobs.
Every line is a single JSON object carrying the correlation fields needed to
tie a message back to a Databricks run: ``run_id``, ``job``, ``event``.

Nothing here logs row-level data — only counts, table names and status — so no
customer identifier can leak into the driver log.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

LOGGER_NAME = "retail_banking_pipeline"


def get_logger(name: str = LOGGER_NAME) -> logging.Logger:
    """Return the pipeline logger, configured for one JSON object per line."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    level: int = logging.INFO,
    **fields: Any,
) -> None:
    """Emit one structured event; ``fields`` are merged into the JSON object."""
    payload = {
        "ts": datetime.now().isoformat(timespec="milliseconds"),
        "event": event,
        **{k: v for k, v in fields.items() if v is not None},
    }
    logger.log(level, json.dumps(payload, default=str))
