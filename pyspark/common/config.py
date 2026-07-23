"""Pipeline configuration and secret access.

Replaces the legacy ``config/pipeline_config.cfg`` bash exports and the hardcoded
``{SAS004}`` encrypted Teradata passwords from ``sas/macros/connect_teradata.sas``.

Configuration resolution order (first hit wins):

1. Databricks job parameters / notebook widgets (``dbutils.widgets.get``), when
   running on a Databricks cluster.
2. Environment variables (used for local runs and CI).
3. The frozen defaults declared on :class:`Config`.

Secrets are resolved via :func:`get_secret`, which reads from
``dbutils.secrets.get(cfg.secret_scope, key)`` on Databricks and falls back to
environment variables locally. No credentials are hardcoded anywhere.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, fields
from datetime import date
from typing import Any, Optional

__all__ = ["Config", "get_config", "get_secret", "MissingSecretError"]


class MissingSecretError(RuntimeError):
    """Raised when a required secret cannot be resolved from any source."""


@dataclass(frozen=True)
class Config:
    """Immutable pipeline configuration.

    Field values are resolved by :func:`get_config`; construct via that factory
    rather than instantiating directly so that widget/env overrides are applied.
    """

    # -- Unity Catalog -------------------------------------------------------
    catalog: str = "retail_banking"
    schema_core: str = "core_banking"
    schema_txn: str = "txn_processing"
    schema_stg: str = "etl_staging"
    schema_dp: str = "data_products"

    # -- Run parameters ------------------------------------------------------
    lookback_months: int = 12
    risk_score_threshold: int = 700
    run_date: str = ""
    log_level: str = "INFO"

    # -- Secret management ---------------------------------------------------
    secret_scope: str = "retail_banking"

    def __post_init__(self) -> None:
        # Default run_date to today (UTC) when not supplied. Uses object.__setattr__
        # because the dataclass is frozen.
        if not self.run_date:
            object.__setattr__(self, "run_date", date.today().isoformat())

    def table(self, schema: str, name: str) -> str:
        """Return a fully-qualified Unity Catalog identifier ``catalog.schema.name``.

        ``schema`` and ``name`` are lower_snake_case object names. Example::

            cfg.table(cfg.schema_stg, "stg_customer_360")
            # -> "retail_banking.etl_staging.stg_customer_360"
        """
        if not schema or not name:
            raise ValueError("schema and name are required to build a table identifier")
        return f"{self.catalog}.{schema}.{name}"


# Type coercion per field name so widget/env string values land as the right type.
_INT_FIELDS = {"lookback_months", "risk_score_threshold"}


def _get_dbutils() -> Optional[Any]:
    """Return the ambient ``dbutils`` handle on Databricks, else ``None``.

    ``dbutils`` is injected into the notebook global namespace on Databricks and is
    not importable in a plain Python process, so we probe for it defensively.
    """
    try:
        import IPython  # type: ignore

        ip = IPython.get_ipython()
        if ip is not None and "dbutils" in ip.user_ns:
            return ip.user_ns["dbutils"]
    except Exception:
        pass
    try:  # pragma: no cover - only reachable inside a Databricks runtime
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except Exception:
        pass
    return None


def _widget_value(dbutils: Any, key: str) -> Optional[str]:
    """Read a Databricks widget/job parameter, returning ``None`` if unset."""
    if dbutils is None:
        return None
    try:
        value = dbutils.widgets.get(key)
    except Exception:
        return None
    if value is None or value == "":
        return None
    return value


def _resolve(key: str, dbutils: Any, env: dict[str, str]) -> Optional[str]:
    """Resolve a single config value: widget first, then ``PIPELINE_<KEY>`` env var."""
    widget = _widget_value(dbutils, key)
    if widget is not None:
        return widget
    env_value = env.get(f"PIPELINE_{key.upper()}")
    if env_value is not None and env_value != "":
        return env_value
    return None


def get_config(
    *,
    dbutils: Any = None,
    env: Optional[dict[str, str]] = None,
) -> Config:
    """Build a :class:`Config` from Databricks widgets/job params + env fallback.

    Parameters are optional and primarily exist for testing:

    - ``dbutils``: inject a Databricks ``dbutils`` handle. When omitted, the ambient
      handle is auto-detected (``None`` off-Databricks).
    - ``env``: mapping used for env-var fallback (defaults to ``os.environ``).

    Env overrides use the ``PIPELINE_<FIELD>`` convention, e.g.
    ``PIPELINE_CATALOG``, ``PIPELINE_LOOKBACK_MONTHS``, ``PIPELINE_RISK_SCORE_THRESHOLD``.
    """
    if env is None:
        env = dict(os.environ)
    if dbutils is None:
        dbutils = _get_dbutils()

    overrides: dict[str, Any] = {}
    for field in fields(Config):
        raw = _resolve(field.name, dbutils, env)
        if raw is None:
            continue
        if field.name in _INT_FIELDS:
            try:
                overrides[field.name] = int(raw)
            except ValueError as exc:
                raise ValueError(
                    f"config field '{field.name}' must be an integer, got {raw!r}"
                ) from exc
        else:
            overrides[field.name] = raw

    return Config(**overrides)


def get_secret(
    cfg: Config,
    key: str,
    *,
    dbutils: Any = None,
    env: Optional[dict[str, str]] = None,
    default: Optional[str] = None,
) -> str:
    """Resolve a secret by ``key`` from the Databricks secret scope, env fallback.

    Resolution order:

    1. ``dbutils.secrets.get(cfg.secret_scope, key)`` (Databricks).
    2. Environment variable named ``key`` (local/CI runs).
    3. The supplied ``default``, if any.

    Raises :class:`MissingSecretError` when the secret cannot be resolved and no
    ``default`` is provided.

    Required secret keys for this pipeline (populate in the ``retail_banking``
    secret scope on Databricks, or as env vars locally):

    - ``td_password``: legacy Teradata service-account password (only needed while
      migrating data off Teradata; eliminates the hardcoded ``{SAS004}`` values).
    """
    if env is None:
        env = dict(os.environ)
    if dbutils is None:
        dbutils = _get_dbutils()

    if dbutils is not None:
        try:
            value = dbutils.secrets.get(cfg.secret_scope, key)
            if value is not None and value != "":
                return value
        except Exception:
            pass

    env_value = env.get(key)
    if env_value is not None and env_value != "":
        return env_value

    if default is not None:
        return default

    raise MissingSecretError(
        f"secret '{key}' not found in scope '{cfg.secret_scope}' or environment"
    )
