"""Helper to execute the ``.sql`` DDL files with ``${...}`` placeholder
substitution (Ticket 1).

Databricks ``%sql`` cells substitute widget values automatically; when the DDL
is driven from Python (the setup notebook, the local orchestrator and tests) this
helper performs the same substitution and runs each statement via ``spark.sql``.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List

from pyspark.sql import SparkSession

DDL_DIR = Path(__file__).resolve().parent.parent / "ddl"


def _substitutions(config) -> dict:
    return {
        "catalog": config.catalog,
        "core_schema": config.core_schema,
        "txn_schema": config.txn_schema,
        "staging_schema": config.staging_schema,
        "products_schema": config.products_schema,
    }


def render_sql(sql_text: str, config) -> str:
    for key, value in _substitutions(config).items():
        sql_text = sql_text.replace("${" + key + "}", value)
    return sql_text


def _strip_comments(statement: str) -> str:
    return re.sub(r"--[^\n]*", "", statement)


def split_statements(sql_text: str) -> List[str]:
    # Strip line comments first so a ';' inside a comment cannot split a statement.
    cleaned = _strip_comments(sql_text)
    return [stmt.strip() for stmt in cleaned.split(";") if stmt.strip()]


def run_sql_file(spark: SparkSession, filename: str, config) -> None:
    """Render and execute every statement in ``ddl/<filename>``.

    ``CREATE CATALOG`` is skipped for the built-in local ``spark_catalog`` (used
    by tests / local runs) where catalogs cannot be created.
    """
    path = DDL_DIR / filename
    sql_text = render_sql(path.read_text(), config)
    local_catalog = config.catalog == "spark_catalog"
    for statement in split_statements(sql_text):
        if local_catalog and _strip_comments(statement).lstrip().upper().startswith(
            "CREATE CATALOG"
        ):
            continue
        spark.sql(statement)


def create_all(spark: SparkSession, config) -> None:
    """Create catalog, schemas and every table (setup for Ticket 1)."""
    run_sql_file(spark, "00_unity_catalog_setup.sql", config)
    run_sql_file(spark, "01_source_tables.sql", config)
    run_sql_file(spark, "02_staging_tables.sql", config)
    run_sql_file(spark, "03_data_product_tables.sql", config)
