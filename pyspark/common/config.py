"""Pipeline configuration.

Provides the frozen :class:`Config` dataclass and :func:`get_config`, the single
source of truth for every catalog / schema / table name, tunable and runtime
parameter used by the PySpark port of the retail-banking analytics pipeline.

On Databricks, values are read from job parameters / notebook widgets
(``dbutils.widgets``); locally they fall back to environment variables and then
to sensible defaults, so the same code runs unchanged in tests, CI and
production.

NOTE (parallel-migration stub): Tickets 1/2/3 own the canonical implementation
of this module.  This is a minimal, contract-compatible stub so Ticket 10's PR
is self-contained and its tests pass; keep signatures identical so it is a
drop-in for the owning ticket's version.
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _repo_root() -> Path:
    # .../pyspark/common/config.py -> repo root is two levels above pyspark/
    return Path(__file__).resolve().parents[2]


def _widget(key: str) -> Optional[str]:
    """Read a Databricks widget / job parameter, if running on Databricks."""
    try:  # pragma: no cover - only exercised on Databricks
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        return DBUtils(spark).widgets.get(key)
    except Exception:
        return None


def _param(key: str, default: str) -> str:
    """Resolve a parameter: Databricks widget -> env var -> default."""
    value = _widget(key)
    if value is not None and value != "":
        return value
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Config:
    """Immutable configuration for a single pipeline run."""

    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: str = field(default_factory=lambda: _dt.date.today().isoformat())
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    # Local-execution knobs (ignored on Databricks, where Unity Catalog manages
    # storage).  ``warehouse_dir`` is the root for path-based Delta tables and
    # ``data_dir`` points at the repo's seed CSVs used to bootstrap local runs.
    warehouse_dir: str = field(
        default_factory=lambda: str(_repo_root() / "pyspark" / ".delta-warehouse")
    )
    data_dir: str = field(default_factory=lambda: str(_repo_root() / "data"))

    def table(self, schema: str, name: str) -> str:
        """Return the fully-qualified Unity Catalog name ``catalog.schema.name``."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars / defaults."""
    defaults = Config()
    return Config(
        catalog=_param("catalog", defaults.catalog),
        schema_core=_param("schema_core", defaults.schema_core),
        schema_txn=_param("schema_txn", defaults.schema_txn),
        schema_stg=_param("schema_stg", defaults.schema_stg),
        schema_dp=_param("schema_dp", defaults.schema_dp),
        lookback_months=int(_param("lookback_months", str(defaults.lookback_months))),
        risk_score_threshold=int(
            _param("risk_score_threshold", str(defaults.risk_score_threshold))
        ),
        run_date=_param("run_date", defaults.run_date),
        log_level=_param("log_level", defaults.log_level),
        secret_scope=_param("secret_scope", defaults.secret_scope),
        warehouse_dir=_param("warehouse_dir", defaults.warehouse_dir),
        data_dir=_param("data_dir", defaults.data_dir),
    )
