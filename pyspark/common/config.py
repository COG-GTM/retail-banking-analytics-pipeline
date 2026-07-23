"""Pipeline configuration.

MINIMAL SHARED STUB (owned by Ticket 2 — config/secrets). Kept intentionally
small so Ticket 8's PR is self-contained and its tests pass; the owning ticket's
implementation supersedes this at merge. Signatures/field names match the shared
interface contract exactly.

``get_config()`` reads Databricks job parameters / notebook widgets
(``dbutils.widgets``) when available, with environment-variable fallback for
local runs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date


def _widget(name: str) -> str | None:
    """Return a Databricks widget value if running on a cluster, else ``None``."""
    try:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]  # type: ignore[index]
        value = dbutils.widgets.get(name)
        return value or None
    except Exception:
        return None


def _get(name: str, env: str, default: str) -> str:
    return _widget(name) or os.environ.get(env, default)


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration resolved from widgets/env with defaults."""

    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: date = field(default_factory=date.today)
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return a fully-qualified Unity Catalog name ``catalog.schema.name``."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars with defaults."""
    run_date_raw = _get("run_date", "RB_RUN_DATE", "")
    run_date = date.fromisoformat(run_date_raw) if run_date_raw else date.today()
    return Config(
        catalog=_get("catalog", "RB_CATALOG", "retail_banking"),
        schema_core=_get("schema_core", "RB_SCHEMA_CORE", "core_banking"),
        schema_txn=_get("schema_txn", "RB_SCHEMA_TXN", "txn_processing"),
        schema_stg=_get("schema_stg", "RB_SCHEMA_STG", "etl_staging"),
        schema_dp=_get("schema_dp", "RB_SCHEMA_DP", "data_products"),
        lookback_months=int(_get("lookback_months", "RB_LOOKBACK_MONTHS", "12")),
        risk_score_threshold=int(
            _get("risk_score_threshold", "RB_RISK_SCORE_THRESHOLD", "700")
        ),
        run_date=run_date,
        log_level=_get("log_level", "RB_LOG_LEVEL", "INFO"),
        secret_scope=_get("secret_scope", "RB_SECRET_SCOPE", "retail_banking"),
    )
