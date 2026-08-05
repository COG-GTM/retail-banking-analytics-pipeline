"""Shared helpers for the PySpark migration of the BTEQ + SAS pipeline.

Provides a SparkSession factory, CSV readers for each source layer, and the
run-date parameter that replaces Teradata's CURRENT_DATE so historical runs
can be reproduced exactly (the checked-in golden data was generated with
RUN_DATE = 2026-04-10).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

# Repository root (this file lives in <repo>/pyspark/)
REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
SOURCE_DIR = DATA_DIR / "01_source_tables"
STAGING_DIR = DATA_DIR / "02_bteq_staging"
PRODUCTS_DIR = DATA_DIR / "03_sas_data_products"

# The golden CSVs were generated as-of this date; equivalent of $RUN_DATE.
DEFAULT_RUN_DATE = dt.date(2026, 4, 10)
# Equivalent of $LOOKBACK_MONTHS in config/pipeline_config.cfg.
DEFAULT_LOOKBACK_MONTHS = 12


def get_spark(app_name: str = "retail-banking-pyspark") -> SparkSession:
    """Create (or reuse) a local SparkSession with sensible defaults."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Small demo volumes: avoid 200 shuffle partitions overhead.
        .config("spark.sql.shuffle.partitions", "8")
        # Match Teradata's session timezone behaviour (dates, not tz-aware).
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read a CSV with header + schema inference (demo-scale data)."""
    return spark.read.csv(str(path), header=True, inferSchema=True)


def load_source_tables(spark: SparkSession) -> dict[str, DataFrame]:
    """Register all Bronze-layer source tables as temp views and return them.

    Mirrors CORE_BANKING_DB / TXN_PROCESSING_DB tables referenced by the
    BTEQ scripts.
    """
    tables = {}
    for name in (
        "customers",
        "accounts",
        "addresses",
        "transactions",
        "transaction_types",
        "customer_bureau_scores",
    ):
        df = read_csv(spark, SOURCE_DIR / f"{name}.csv")
        df.createOrReplaceTempView(name)
        tables[name] = df
    return tables


def load_staging_tables(spark: SparkSession) -> dict[str, DataFrame]:
    """Register the Silver-layer (BTEQ staging) golden tables as temp views."""
    tables = {}
    for name in ("stg_customer_360", "stg_txn_summary", "stg_risk_factors"):
        df = read_csv(spark, STAGING_DIR / f"{name}.csv")
        df.createOrReplaceTempView(name)
        tables[name] = df
    return tables
