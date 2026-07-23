"""Pipeline configuration.

Minimal stub matching the shared Ticket-2 contract so this ticket's PR is
self-contained; the owning ticket's version supersedes it at merge. Keep field
names / signatures identical to the contract.

`Config` is a frozen dataclass sourced from Databricks job parameters / notebook
widgets (``dbutils.widgets``) with environment-variable fallback for local runs.
No catalog / schema / table names are hardcoded anywhere else in the codebase --
everything flows from here.
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field, fields


def _dbutils_widget(key: str) -> str | None:
    """Return a Databricks widget/job value if running on Databricks, else None."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        dbutils = DBUtils(spark)
        value = dbutils.widgets.get(key)
        return value or None
    except Exception:
        return None


def _resolve(key: str, default: str) -> str:
    """Resolve a config value: Databricks widget > env var > default."""
    return _dbutils_widget(key) or os.environ.get(key.upper()) or default


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration."""

    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: _dt.date = field(default_factory=_dt.date.today)
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return the fully-qualified Unity Catalog identifier ``catalog.schema.name``."""
        return f"{self.catalog}.{schema}.{name}"


def _coerce(name: str, raw: str) -> object:
    """Coerce a resolved string into the dataclass field's Python type."""
    if name in ("lookback_months", "risk_score_threshold"):
        return int(raw)
    if name == "run_date":
        return _dt.date.fromisoformat(raw)
    return raw


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars with defaults."""
    defaults = Config()
    overrides: dict[str, object] = {}
    for f in fields(Config):
        default_val = getattr(defaults, f.name)
        raw = _resolve(f.name, default_val.isoformat() if isinstance(default_val, _dt.date) else str(default_val))
        overrides[f.name] = _coerce(f.name, raw)
    return Config(**overrides)  # type: ignore[arg-type]
