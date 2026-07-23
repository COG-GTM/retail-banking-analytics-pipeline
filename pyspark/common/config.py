"""Pipeline configuration (Config contract).

Replaces the shell ``config/pipeline_config.cfg`` + BTEQ/SAS environment lookups.
``get_config()`` reads Databricks job parameters / notebook widgets
(``dbutils.widgets``) with an environment-variable fallback for local runs, so a
single immutable object drives every job. No catalog/schema/table name, path, or
credential is hardcoded anywhere else.

This is a minimal shared stub matching the ticket's Config contract; it is owned
by Ticket 2 and will be superseded at merge. Signatures are kept identical.
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field
from typing import Optional


def _today() -> _dt.date:
    return _dt.date.today()


def _parse_date(value) -> _dt.date:
    if value is None or value == "":
        return _today()
    if isinstance(value, _dt.date):
        return value
    return _dt.date.fromisoformat(str(value).strip())


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
    run_date: _dt.date = field(default_factory=_today)
    log_level: str = "INFO"
    secret_scope: str = "retail_banking"

    def table(self, schema: str, name: str) -> str:
        """Return the fully-qualified ``catalog.schema.name`` identifier."""
        return f"{self.catalog}.{schema}.{name}"


def _widget(key: str) -> Optional[str]:
    """Read a Databricks notebook widget / job parameter if available."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return None
        dbutils = DBUtils(spark)
        value = dbutils.widgets.get(key)
        return value if value != "" else None
    except Exception:
        return None


def _get(key: str, env_key: str, default: Optional[str]) -> Optional[str]:
    """Widget value first (Databricks), then env var, then default (local)."""
    value = _widget(key)
    if value is not None:
        return value
    return os.environ.get(env_key, default)


def get_config() -> Config:
    """Build a :class:`Config` from widgets/job params with env-var fallback."""
    return Config(
        catalog=_get("catalog", "RBAP_CATALOG", "retail_banking"),
        schema_core=_get("schema_core", "RBAP_SCHEMA_CORE", "core_banking"),
        schema_txn=_get("schema_txn", "RBAP_SCHEMA_TXN", "txn_processing"),
        schema_stg=_get("schema_stg", "RBAP_SCHEMA_STG", "etl_staging"),
        schema_dp=_get("schema_dp", "RBAP_SCHEMA_DP", "data_products"),
        lookback_months=int(_get("lookback_months", "RBAP_LOOKBACK_MONTHS", "12")),
        risk_score_threshold=int(
            _get("risk_score_threshold", "RBAP_RISK_SCORE_THRESHOLD", "700")
        ),
        run_date=_parse_date(_get("run_date", "RBAP_RUN_DATE", None)),
        log_level=_get("log_level", "RBAP_LOG_LEVEL", "INFO"),
        secret_scope=_get("secret_scope", "RBAP_SECRET_SCOPE", "retail_banking"),
    )
