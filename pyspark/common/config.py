"""Pipeline configuration for the Databricks port.

``get_config()`` reads Databricks job parameters / notebook widgets
(``dbutils.widgets``) with an environment-variable fallback for local runs, and
returns a frozen :class:`Config`.  Every catalog / schema / table reference in
the pipeline flows from this object -- there are no hardcoded catalog, schema,
table names, paths or credentials elsewhere (Providence rule R5).

NOTE (parallel-ticket stub): tickets 1/2/3 own the canonical version of this
module.  This is the minimal contract-compatible implementation so Ticket 9 is
self-contained; signatures and field names match the shared contract exactly so
the owning ticket's version supersedes it cleanly at merge.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


def _widget_or_env(key: str, default: str) -> str:
    """Resolve a config value from a notebook widget, then env var, then default.

    On Databricks ``dbutils`` is injected into the global namespace; locally it
    is absent, so we fall back to an upper-cased environment variable and
    finally the supplied default.
    """
    try:
        dbutils = globals().get("dbutils") or __builtins__["dbutils"]  # type: ignore[index]
        value = dbutils.widgets.get(key)
        if value is not None and str(value) != "":
            return str(value)
    except Exception:
        pass
    env_value = os.environ.get(key.upper())
    if env_value is not None and env_value != "":
        return env_value
    return default


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
    run_date: str = date.today().isoformat()
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return the fully-qualified ``catalog.schema.table`` identifier."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    """Build a :class:`Config` from widgets/env with sensible local defaults."""
    return Config(
        catalog=_widget_or_env("catalog", "retail_banking"),
        schema_core=_widget_or_env("schema_core", "core_banking"),
        schema_txn=_widget_or_env("schema_txn", "txn_processing"),
        schema_stg=_widget_or_env("schema_stg", "etl_staging"),
        schema_dp=_widget_or_env("schema_dp", "data_products"),
        lookback_months=int(_widget_or_env("lookback_months", "12")),
        risk_score_threshold=int(_widget_or_env("risk_score_threshold", "700")),
        run_date=_widget_or_env("run_date", date.today().isoformat()),
        log_level=_widget_or_env("log_level", "INFO"),
        secret_scope=_widget_or_env("secret_scope", "retail_banking"),
    )
