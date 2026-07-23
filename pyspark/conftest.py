"""Shared pytest fixtures for the retail-banking PySpark suite.

A single local Delta-enabled SparkSession is reused across the session. Tables
are written to a temporary warehouse so runs are isolated and idempotent.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest

# Local runs use the built-in (Delta-aware) ``spark_catalog`` for three-level
# ``catalog.schema.table`` names. Must be set before ``get_config`` is called.
os.environ.setdefault("RB_CATALOG", "spark_catalog")
os.environ.setdefault("SPARK_MASTER", "local[1]")
os.environ.setdefault(
    "SPARK_WAREHOUSE_DIR", tempfile.mkdtemp(prefix="rb_warehouse_")
)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from common.config import Config, get_config  # noqa: E402
from common.spark import get_spark  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

# Schema of ETL_STAGING_DB.STG_TXN_SUMMARY (see ddl/01_staging_tables.sql).
STG_TXN_SUMMARY_SCHEMA = StructType(
    [
        StructField("customer_id", LongType(), False),
        StructField("account_id", LongType(), False),
        StructField("account_type", StringType(), True),
        StructField("summary_period_start", DateType(), True),
        StructField("summary_period_end", DateType(), True),
        StructField("txn_count_total", IntegerType(), True),
        StructField("txn_count_debit", DoubleType(), True),
        StructField("txn_count_credit", DoubleType(), True),
        StructField("txn_count_fee", DoubleType(), True),
        StructField("amt_total_debit", DoubleType(), True),
        StructField("amt_total_credit", DoubleType(), True),
        StructField("amt_total_fees", DoubleType(), True),
        StructField("amt_avg_debit", DoubleType(), True),
        StructField("amt_avg_credit", DoubleType(), True),
        StructField("amt_max_single_debit", DoubleType(), True),
        StructField("amt_max_single_credit", DoubleType(), True),
        StructField("distinct_merchants", IntegerType(), True),
        StructField("top_merchant_category", StringType(), True),
        StructField("pct_atm", DoubleType(), True),
        StructField("pct_pos", DoubleType(), True),
        StructField("pct_web", DoubleType(), True),
        StructField("pct_mobile", DoubleType(), True),
        StructField("days_since_last_txn", IntegerType(), True),
        StructField("load_ts", StringType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = get_spark("retail-banking-tests")
    yield session
    session.stop()


@pytest.fixture()
def cfg() -> Config:
    return get_config()


@pytest.fixture()
def seed_stg_txn_summary(spark, cfg):
    """Return a helper that writes a STG_TXN_SUMMARY Delta table for a test.

    With no ``rows`` argument the real seed CSV is loaded; otherwise a DataFrame
    is built from the supplied row dicts (schema-validated).
    """

    def _seed(rows: list[dict] | None = None):
        if rows is None:
            df = spark.read.csv(
                str(DATA_DIR / "02_bteq_staging" / "stg_txn_summary.csv"),
                header=True,
                schema=STG_TXN_SUMMARY_SCHEMA,
            )
        else:
            ordered = [
                tuple(r.get(f.name) for f in STG_TXN_SUMMARY_SCHEMA.fields)
                for r in rows
            ]
            df = spark.createDataFrame(ordered, STG_TXN_SUMMARY_SCHEMA)

        spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
        table = cfg.table(cfg.schema_stg, "stg_txn_summary")
        df.write.format("delta").mode("overwrite").saveAsTable(table)
        return df

    return _seed
