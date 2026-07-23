"""Pipeline configuration.

Minimal stub matching the shared Ticket-2 contract so this ticket's PR is
self-contained. Owned by Ticket 2 (config/secrets); the owning ticket's version
supersedes this at merge. Keep field names/signatures identical to the contract.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from typing import Optional


def _widget(key: str) -> Optional[str]:
    """Read a Databricks notebook/job widget value if running on Databricks."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        dbutils = DBUtils(spark)
        value = dbutils.widgets.get(key)
        return value or None
    except Exception:
        return None


def _param(key: str, default: str) -> str:
    """Resolve a config value: Databricks widget -> env var -> default."""
    return _widget(key) or os.environ.get(key.upper()) or default


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration.

    All catalog/schema/table references flow through this object; no hardcoded
    names, paths, or credentials live anywhere else.
    """

    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: str = field(default_factory=lambda: date.today().isoformat())
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return a fully-qualified ``catalog.schema.table`` identifier."""
        return f"{self.catalog}.{schema}.{name}"


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars with defaults."""
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
    )


def get_secret(cfg: Config, key: str) -> Optional[str]:
    """Fetch a secret from the Databricks secret scope, env-var fallback."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            dbutils = DBUtils(spark)
            return dbutils.secrets.get(cfg.secret_scope, key)
    except Exception:
        pass
    return os.environ.get(key.upper())


__all__ = ["Config", "get_config", "get_secret"]
