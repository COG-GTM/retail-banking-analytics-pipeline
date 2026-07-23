"""Ticket 2 - Configuration and secrets.

Replaces ``config/pipeline_config.cfg`` (Teradata/SAS environment variables with
hard-coded ``{SAS004}`` passwords) with a Databricks-native configuration model:

* Runtime parameters come from **Databricks widgets / job parameters** (``dbutils``)
  when running on a cluster, and fall back to environment variables / defaults so
  the same code is unit-testable off-cluster.
* Sensitive values are resolved from **Databricks Secrets** (``dbutils.secrets``)
  - there are no passwords stored in source anymore.

The four legacy Teradata databases are mapped onto Unity Catalog schemas inside a
single catalog (see :data:`LEGACY_DATABASE_MAP`).
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime

# Legacy Teradata database  ->  Unity Catalog schema.
LEGACY_DATABASE_MAP = {
    "CORE_BANKING_DB": "core_banking",
    "TXN_PROCESSING_DB": "txn_processing",
    "ETL_STAGING_DB": "etl_staging",
    "DATA_PRODUCTS_DB": "data_products",
}

DEFAULT_CATALOG = "retail_banking_analytics"
DEFAULT_SECRET_SCOPE = "retail_banking_analytics"

# Parameter defaults carried over verbatim from pipeline_config.cfg.
DEFAULT_LOOKBACK_MONTHS = 12
DEFAULT_RISK_SCORE_THRESHOLD = 700


def get_dbutils(spark=None):
    """Return a ``dbutils`` handle when running on Databricks, else ``None``.

    Works both inside a notebook (where ``dbutils`` is a global) and from a
    plain Python module via ``DBUtils(spark)``.
    """
    try:  # notebook global
        return dbutils  # type: ignore[name-defined]  # noqa: F821
    except NameError:
        pass
    if spark is not None:
        try:
            from pyspark.dbutils import DBUtils  # type: ignore

            return DBUtils(spark)
        except Exception:
            return None
    return None


def _widget(dbutils, name: str, default: str) -> str:
    """Read a widget / job parameter, falling back to env var then default."""
    if dbutils is not None:
        try:
            value = dbutils.widgets.get(name)
            if value is not None and value != "":
                return value
        except Exception:
            pass
    return os.environ.get(name.upper(), default)


@dataclass(frozen=True)
class PipelineConfig:
    """Resolved pipeline configuration.

    Table locations are always fully-qualified three-level Unity Catalog names
    (``catalog.schema.table``); nothing here references Teradata.
    """

    catalog: str = DEFAULT_CATALOG
    core_schema: str = "core_banking"
    txn_schema: str = "txn_processing"
    staging_schema: str = "etl_staging"
    products_schema: str = "data_products"
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS
    risk_score_threshold: int = DEFAULT_RISK_SCORE_THRESHOLD
    secret_scope: str = DEFAULT_SECRET_SCOPE
    run_date: date = field(default_factory=date.today)
    load_ts: datetime = field(default_factory=datetime.now)
    _dbutils: object = field(default=None, repr=False, compare=False)

    # -- fully-qualified table helpers ------------------------------------
    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def core(self, name: str) -> str:
        return self.table(self.core_schema, name)

    def txn(self, name: str) -> str:
        return self.table(self.txn_schema, name)

    def staging(self, name: str) -> str:
        return self.table(self.staging_schema, name)

    def product(self, name: str) -> str:
        return self.table(self.products_schema, name)

    @property
    def schemas(self) -> list[str]:
        return [self.core_schema, self.txn_schema, self.staging_schema, self.products_schema]

    # -- secrets ----------------------------------------------------------
    def secret(self, key: str) -> str:
        """Resolve a secret from the Databricks secret scope.

        Falls back to an ``RBA_SECRET_<KEY>`` environment variable for local
        development. Never returns a value baked into source control.
        """
        if self._dbutils is not None:
            try:
                return self._dbutils.secrets.get(scope=self.secret_scope, key=key)
            except Exception:
                pass
        env_key = f"RBA_SECRET_{key.upper()}"
        if env_key in os.environ:
            return os.environ[env_key]
        raise KeyError(
            f"Secret '{key}' not found in scope '{self.secret_scope}' or env '{env_key}'"
        )


def load_config(dbutils=None, spark=None, **overrides) -> PipelineConfig:
    """Build a :class:`PipelineConfig` from widgets / job params / env / defaults.

    Parameters supplied via ``overrides`` win over everything (useful for tests
    and for the local orchestrator).
    """
    dbutils = dbutils if dbutils is not None else get_dbutils(spark)

    catalog = _widget(dbutils, "catalog", DEFAULT_CATALOG)
    core_schema = _widget(dbutils, "core_schema", "core_banking")
    txn_schema = _widget(dbutils, "txn_schema", "txn_processing")
    staging_schema = _widget(dbutils, "staging_schema", "etl_staging")
    products_schema = _widget(dbutils, "products_schema", "data_products")
    lookback_months = int(_widget(dbutils, "lookback_months", str(DEFAULT_LOOKBACK_MONTHS)))
    risk_threshold = int(
        _widget(dbutils, "risk_score_threshold", str(DEFAULT_RISK_SCORE_THRESHOLD))
    )
    secret_scope = _widget(dbutils, "secret_scope", DEFAULT_SECRET_SCOPE)

    run_date_str = _widget(dbutils, "run_date", "")
    run_date = (
        datetime.strptime(run_date_str, "%Y-%m-%d").date() if run_date_str else date.today()
    )

    cfg_kwargs = dict(
        catalog=catalog,
        core_schema=core_schema,
        txn_schema=txn_schema,
        staging_schema=staging_schema,
        products_schema=products_schema,
        lookback_months=lookback_months,
        risk_score_threshold=risk_threshold,
        secret_scope=secret_scope,
        run_date=run_date,
        _dbutils=dbutils,
    )
    cfg_kwargs.update(overrides)
    return PipelineConfig(**cfg_kwargs)
