"""Pipeline configuration.

NOTE: This is a minimal stub owned by Ticket 2 (config/secrets). It is included
here so Ticket 3's utilities and tests are self-contained; the Ticket 2 version
supersedes it at merge. Signatures/field names match the shared contract exactly.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, fields
from datetime import date


def _widget_or_env(key: str, default: str) -> str:
    """Read a Databricks notebook/job parameter, falling back to env vars.

    Resolution order: ``dbutils.widgets`` (Databricks) -> environment variable
    -> ``default``. The env var name is the upper-cased field name.
    """
    try:  # pragma: no cover - only exercised on Databricks
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]  # type: ignore[index]
        value = dbutils.widgets.get(key)
        if value:
            return value
    except Exception:
        pass
    return os.environ.get(key.upper(), default)


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration.

    All catalog/schema/table names flow from here — no hardcoding elsewhere.
    """

    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: str = ""
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return a fully-qualified Unity Catalog identifier ``catalog.schema.name``."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars with defaults."""
    defaults = {f.name: f.default for f in fields(Config)}
    return Config(
        catalog=_widget_or_env("catalog", defaults["catalog"]),
        schema_core=_widget_or_env("schema_core", defaults["schema_core"]),
        schema_txn=_widget_or_env("schema_txn", defaults["schema_txn"]),
        schema_stg=_widget_or_env("schema_stg", defaults["schema_stg"]),
        schema_dp=_widget_or_env("schema_dp", defaults["schema_dp"]),
        lookback_months=int(_widget_or_env("lookback_months", str(defaults["lookback_months"]))),
        risk_score_threshold=int(
            _widget_or_env("risk_score_threshold", str(defaults["risk_score_threshold"]))
        ),
        run_date=_widget_or_env("run_date", date.today().isoformat()),
        log_level=_widget_or_env("log_level", defaults["log_level"]),
        secret_scope=_widget_or_env("secret_scope", defaults["secret_scope"]),
    )
