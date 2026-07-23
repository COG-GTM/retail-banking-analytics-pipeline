"""Bootstrap Delta schemas/tables for the pipeline.

Minimal stub matching the shared Ticket-1 contract so this ticket's PR is
self-contained. Owned by Ticket 1 (DDL/catalog); superseded at merge.
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from common.config import Config, get_config
from common.spark import get_spark

_DDL_DIR = Path(__file__).resolve().parent


def create_schemas(spark: SparkSession, cfg: Config) -> None:
    for schema in (
        cfg.schema_core,
        cfg.schema_txn,
        cfg.schema_stg,
        cfg.schema_dp,
    ):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")


def create_stg_risk_factors(spark: SparkSession, cfg: Config) -> None:
    ddl = (_DDL_DIR / "stg_risk_factors.sql").read_text()
    table = cfg.table(cfg.schema_stg, "stg_risk_factors")
    spark.sql(ddl.format(table=table))


def main() -> None:
    cfg = get_config()
    spark = get_spark("create_delta_tables")
    create_schemas(spark, cfg)
    create_stg_risk_factors(spark, cfg)


if __name__ == "__main__":
    main()
