"""Bootstrap Delta tables from the templated ``*.sql`` files in this directory.

Minimal shared stub owned by Ticket 1. Each ``.sql`` file may reference the
``{catalog}`` / ``{schema_stg}`` / ``{schema_dp}`` / ``{schema_core}`` /
``{schema_txn}`` placeholders, which are filled from :class:`Config` so no
catalog/schema name is hardcoded.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from common.config import Config, get_config
from common.spark import get_spark

_DDL_DIR = Path(__file__).resolve().parent


def _render(sql: str, cfg: Config) -> str:
    return sql.format(
        catalog=cfg.catalog,
        schema_core=cfg.schema_core,
        schema_txn=cfg.schema_txn,
        schema_stg=cfg.schema_stg,
        schema_dp=cfg.schema_dp,
    )


def create_all(spark: SparkSession, cfg: Config) -> None:
    """Create every schema and Delta table declared under ``ddl/``."""
    for schema in (
        cfg.schema_core,
        cfg.schema_txn,
        cfg.schema_stg,
        cfg.schema_dp,
    ):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

    for sql_file in sorted(_DDL_DIR.glob("*.sql")):
        rendered = _render(sql_file.read_text(), cfg)
        for statement in rendered.split(";"):
            if statement.strip():
                spark.sql(statement)


if __name__ == "__main__":
    _cfg = get_config()
    create_all(get_spark(), _cfg)
