"""Secret access.

Replaces the SAS ``{SAS004}`` encoded passwords and ``logmech=LDAP`` LIBNAME
statements in ``sas/macros/connect_teradata.sas``.

Access to bronze/silver/gold tables needs no credential at all — Unity Catalog
governs it. The only place a secret is still required is the optional direct
JDBC read from the legacy Teradata system during bronze ingestion, and that
value is fetched from a Databricks secret scope at run time.
"""

from __future__ import annotations

from typing import Any

from shared.config import PipelineConfig, _dbutils


def get_secret(cfg: PipelineConfig, key: str, spark: Any = None) -> str:
    """Read ``key`` from the configured Databricks secret scope."""
    dbu = _dbutils(spark)
    if dbu is None:
        raise RuntimeError(
            f"Cannot read secret '{cfg.secret_scope}/{key}': dbutils is unavailable. "
            "Secret-backed ingestion only runs on Databricks."
        )
    return dbu.secrets.get(scope=cfg.secret_scope, key=key)


def jdbc_options(cfg: PipelineConfig, spark: Any = None) -> dict[str, str]:
    """JDBC options for the legacy Teradata reader (source_format='jdbc')."""
    return {
        "url": get_secret(cfg, "teradata-jdbc-url", spark),
        "user": get_secret(cfg, "teradata-user", spark),
        "password": get_secret(cfg, "teradata-password", spark),
        "driver": "com.teradata.jdbc.TeraDriver",
    }
