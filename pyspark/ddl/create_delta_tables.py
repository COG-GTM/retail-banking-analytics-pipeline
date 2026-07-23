"""Unity Catalog + Delta bootstrap for the retail-banking analytics pipeline.

Replaces the four legacy Teradata databases (``CORE_BANKING_DB``,
``TXN_PROCESSING_DB``, ``ETL_STAGING_DB``, ``DATA_PRODUCTS_DB``) with one Unity
Catalog and four schemas, and creates every Delta table ported from
``ddl/00_source_tables.sql``, ``ddl/01_staging_tables.sql`` and
``ddl/02_data_product_tables.sql``.

All catalog/schema/table names flow from :class:`~common.config.Config` (Rule
R5); the companion ``*.sql`` files use ``${catalog}`` / ``${schema_*}``
placeholders substituted at runtime. Idempotent: every statement uses
``CREATE ... IF NOT EXISTS``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from common.config import Config

logging.basicConfig(level=logging.INFO)
_LOGGER = logging.getLogger("ddl.create_delta_tables")

_DDL_DIR = Path(__file__).resolve().parent
_DDL_FILES = (
    "00_source_tables.sql",
    "01_staging_tables.sql",
    "02_data_product_tables.sql",
)


def _log(step: str, status: str, **fields) -> None:
    """Emit a structured JSON log line (Rule R6, no bare print)."""
    _LOGGER.info(json.dumps({"job": "create_delta_tables", "step": step, "status": status, **fields}))


def _substitute(sql_text: str, cfg: Config) -> str:
    replacements = {
        "${catalog}": cfg.catalog,
        "${schema_core}": cfg.schema_core,
        "${schema_txn}": cfg.schema_txn,
        "${schema_stg}": cfg.schema_stg,
        "${schema_dp}": cfg.schema_dp,
    }
    for token, value in replacements.items():
        sql_text = sql_text.replace(token, value)
    return sql_text


def _statements(sql_text: str):
    """Yield executable statements: drop full-line ``--`` comments, split on ``;``."""
    lines = [line for line in sql_text.splitlines() if not line.strip().startswith("--")]
    for raw in "\n".join(lines).split(";"):
        stmt = raw.strip()
        if stmt:
            yield stmt


def _create_namespaces(spark: SparkSession, cfg: Config) -> None:
    """Create the catalog and the four schemas.

    ``CREATE CATALOG`` is a Unity Catalog / Databricks construct that vanilla
    local Spark does not support, so it is best-effort: a failure locally is
    logged and skipped (schemas are created directly against the session
    catalog instead).
    """
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
        _log("create_catalog", "SUCCESS", catalog=cfg.catalog)
    except Exception as exc:  # noqa: BLE001 - unsupported on local Spark
        _log("create_catalog", "SKIPPED", catalog=cfg.catalog, message=str(exc).splitlines()[0])

    for schema in cfg.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")
        _log("create_schema", "SUCCESS", schema=f"{cfg.catalog}.{schema}")


def run(spark: SparkSession, cfg: Config) -> None:
    """Bootstrap the catalog, schemas, and all Delta tables. Idempotent."""
    _log("bootstrap", "START", catalog=cfg.catalog)
    _create_namespaces(spark, cfg)

    for file_name in _DDL_FILES:
        sql_text = _substitute((_DDL_DIR / file_name).read_text(), cfg)
        for stmt in _statements(sql_text):
            spark.sql(stmt)
        _log("apply_ddl", "SUCCESS", file=file_name)

    _log("bootstrap", "SUCCESS", catalog=cfg.catalog)


def main() -> None:
    from common.config import get_config
    from common.spark import get_spark

    cfg = get_config()
    spark = get_spark("create-delta-tables")
    run(spark, cfg)


if __name__ == "__main__":
    main()
