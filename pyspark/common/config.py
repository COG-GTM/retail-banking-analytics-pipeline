"""Pipeline configuration for the Databricks port of the retail-banking pipeline.

This replaces the legacy shell ``config/pipeline_config.cfg`` (Teradata DB refs,
hardcoded ``{SAS004}`` passwords, SAS/Teradata connection details) with a single,
frozen, testable :class:`Config` object.

``get_config()`` reads Databricks job parameters / notebook widgets
(``dbutils.widgets``) when running on Databricks, and falls back to environment
variables for local runs, so the same code path works in both places. No catalog,
schema, table name, path, or credential is hardcoded anywhere else in the port
(Providence Rule R5): everything flows from this object, and secrets come from
``dbutils.secrets.get(secret_scope, key)`` with an env-var fallback.
"""

from __future__ import annotations

import datetime as _dt
import os
from dataclasses import dataclass, field
from typing import Optional

# Legacy Teradata DB -> Unity Catalog schema mapping (see ddl/*.sql):
#   CORE_BANKING_DB   -> {catalog}.core_banking
#   TXN_PROCESSING_DB -> {catalog}.txn_processing
#   ETL_STAGING_DB    -> {catalog}.etl_staging
#   DATA_PRODUCTS_DB  -> {catalog}.data_products
_DEFAULTS: dict[str, str] = {
    "catalog": "retail_banking",
    "schema_core": "core_banking",
    "schema_txn": "txn_processing",
    "schema_stg": "etl_staging",
    "schema_dp": "data_products",
    "lookback_months": "12",
    "risk_score_threshold": "700",
    "log_level": "INFO",
    "secret_scope": "retail_banking",
}


@dataclass(frozen=True)
class Config:
    """Immutable configuration for a single pipeline run."""

    catalog: str = _DEFAULTS["catalog"]
    schema_core: str = _DEFAULTS["schema_core"]
    schema_txn: str = _DEFAULTS["schema_txn"]
    schema_stg: str = _DEFAULTS["schema_stg"]
    schema_dp: str = _DEFAULTS["schema_dp"]
    lookback_months: int = int(_DEFAULTS["lookback_months"])
    risk_score_threshold: int = int(_DEFAULTS["risk_score_threshold"])
    run_date: _dt.date = field(default_factory=_dt.date.today)
    log_level: str = _DEFAULTS["log_level"]
    secret_scope: str = _DEFAULTS["secret_scope"]

    def table(self, schema: str, name: str) -> str:
        """Return the fully qualified Unity Catalog name ``catalog.schema.name``."""
        return f"{self.catalog}.{schema}.{name}"

    @property
    def schemas(self) -> tuple[str, ...]:
        """The four schemas that replace the four legacy Teradata databases."""
        return (self.schema_core, self.schema_txn, self.schema_stg, self.schema_dp)

    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Fetch a secret from the Databricks secret scope, env-var fallback.

        Eliminates the legacy hardcoded ``{SAS004}`` Teradata passwords: on
        Databricks values come from ``dbutils.secrets.get(secret_scope, key)``;
        locally they come from the environment.
        """
        dbutils = _get_dbutils()
        if dbutils is not None:
            try:
                return dbutils.secrets.get(scope=self.secret_scope, key=key)
            except Exception:  # noqa: BLE001 - fall back to env for local/dev runs
                pass
        return os.environ.get(key, default)


def _get_dbutils():
    """Return the Databricks ``dbutils`` handle if available, else ``None``."""
    try:
        import IPython

        ipython = IPython.get_ipython()
        if ipython is not None:
            dbutils = ipython.user_ns.get("dbutils")
            if dbutils is not None:
                return dbutils
    except Exception:  # noqa: BLE001 - IPython/dbutils absent in local runs
        pass
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:  # noqa: BLE001 - not running on Databricks
        pass
    return None


def _read_param(key: str, default: Optional[str]) -> Optional[str]:
    """Read a Databricks widget value, falling back to an env var, then default.

    Env-var names are the upper-cased field names (e.g. ``CATALOG``,
    ``RISK_SCORE_THRESHOLD``).
    """
    dbutils = _get_dbutils()
    if dbutils is not None:
        try:
            value = dbutils.widgets.get(key)
            if value is not None and value != "":
                return value
        except Exception:  # noqa: BLE001 - widget not defined; use fallback
            pass
    return os.environ.get(key.upper(), default)


def _parse_date(value: Optional[str]) -> _dt.date:
    if not value:
        return _dt.date.today()
    return _dt.date.fromisoformat(value.strip())


def get_config() -> Config:
    """Build a :class:`Config` from Databricks widgets / env vars."""
    return Config(
        catalog=_read_param("catalog", _DEFAULTS["catalog"]),
        schema_core=_read_param("schema_core", _DEFAULTS["schema_core"]),
        schema_txn=_read_param("schema_txn", _DEFAULTS["schema_txn"]),
        schema_stg=_read_param("schema_stg", _DEFAULTS["schema_stg"]),
        schema_dp=_read_param("schema_dp", _DEFAULTS["schema_dp"]),
        lookback_months=int(_read_param("lookback_months", _DEFAULTS["lookback_months"])),
        risk_score_threshold=int(
            _read_param("risk_score_threshold", _DEFAULTS["risk_score_threshold"])
        ),
        run_date=_parse_date(_read_param("run_date", None)),
        log_level=_read_param("log_level", _DEFAULTS["log_level"]),
        secret_scope=_read_param("secret_scope", _DEFAULTS["secret_scope"]),
    )
