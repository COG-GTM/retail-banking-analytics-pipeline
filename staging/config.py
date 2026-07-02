"""Environment-driven configuration for the PySpark staging layer.

No warehouse/catalog/schema names, paths, or run parameters are hardcoded in
the transforms; they are resolved here from environment variables with
sensible defaults that mirror ``config/pipeline_config.cfg``. This keeps the
jobs reproducible (a pinned ``as_of_date`` makes an otherwise
``CURRENT_DATE``-dependent pipeline deterministic) and portable across
local/dev/prod without code changes.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# Repo root = parent of this package directory.
REPO_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_SOURCE_DIR = REPO_ROOT / "data" / "01_source_tables"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "02_bteq_staging"


def _parse_as_of_date() -> date:
    """Resolve the logical run date used in place of Teradata ``CURRENT_DATE``.

    Order of precedence: ``PIPELINE_AS_OF_DATE`` (ISO ``YYYY-MM-DD``) ->
    ``RUN_DATE`` (``YYYYMMDD``, as emitted by ``pipeline_config.cfg``) ->
    today's date.
    """
    iso = os.environ.get("PIPELINE_AS_OF_DATE")
    if iso:
        return date.fromisoformat(iso)
    run_date = os.environ.get("RUN_DATE")
    if run_date:
        return datetime.strptime(run_date, "%Y%m%d").date()
    return date.today()


@dataclass(frozen=True)
class StagingConfig:
    """Resolved configuration for a single staging pipeline run."""

    source_dir: str
    output_dir: str
    output_format: str
    lookback_months: int
    as_of_date: date
    run_id: str
    app_name: str
    spark_master: str
    log_level: str

    @property
    def as_of_date_iso(self) -> str:
        return self.as_of_date.isoformat()


def load_config() -> StagingConfig:
    """Build a :class:`StagingConfig` from the process environment."""
    run_id = os.environ.get(
        "PIPELINE_RUN_ID",
        os.environ.get("RUN_TIMESTAMP", datetime.now().strftime("%Y%m%d_%H%M%S")),
    )
    return StagingConfig(
        source_dir=os.environ.get("STG_SOURCE_DIR", str(DEFAULT_SOURCE_DIR)),
        output_dir=os.environ.get("STG_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)),
        output_format=os.environ.get("STG_OUTPUT_FORMAT", "csv").lower(),
        lookback_months=int(os.environ.get("LOOKBACK_MONTHS", "12")),
        as_of_date=_parse_as_of_date(),
        run_id=run_id,
        app_name=os.environ.get("STG_APP_NAME", "retail_banking_staging"),
        spark_master=os.environ.get("SPARK_MASTER", "local[*]"),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    )
