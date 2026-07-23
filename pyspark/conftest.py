"""Shared pytest fixtures: Delta-enabled SparkSession + seeded source tables.

Source tables are loaded from the repo's ``data/01_source_tables/*.csv`` seeds
into the local ``spark_catalog`` under the schemas named by :class:`Config`.
"""
from __future__ import annotations

import glob
import os
from pathlib import Path

import pytest

# Pin JDK 17 (required by Spark 3.5) before any JVM is started.
_JDK17 = "/usr/lib/jvm/java-17-openjdk-amd64"
if os.path.isdir(_JDK17):
    os.environ.setdefault("JAVA_HOME", _JDK17)
    os.environ["PATH"] = f"{_JDK17}/bin:" + os.environ.get("PATH", "")

# Local runs resolve the session catalog, not a Unity catalog.
os.environ.setdefault("CATALOG", "spark_catalog")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import Config, get_config  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO_ROOT / "data" / "01_source_tables"

_CUSTOMERS = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("date_of_birth", DateType()),
        StructField("ssn_hash", StringType()),
        StructField("email", StringType()),
        StructField("phone_primary", StringType()),
        StructField("customer_since", DateType()),
        StructField("customer_status", StringType()),
        StructField("segment_code", StringType()),
        StructField("branch_id", IntegerType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]
)

_ACCOUNTS = StructType(
    [
        StructField("account_id", LongType()),
        StructField("customer_id", LongType()),
        StructField("account_type", StringType()),
        StructField("account_status", StringType()),
        StructField("open_date", DateType()),
        StructField("close_date", DateType()),
        StructField("current_balance", DecimalType(15, 2)),
        StructField("available_balance", DecimalType(15, 2)),
        StructField("credit_limit", DecimalType(15, 2)),
        StructField("interest_rate", DecimalType(5, 4)),
        StructField("branch_id", IntegerType()),
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]
)

_TRANSACTIONS = StructType(
    [
        StructField("transaction_id", LongType()),
        StructField("account_id", LongType()),
        StructField("transaction_type_cd", StringType()),
        StructField("transaction_date", DateType()),
        StructField("transaction_ts", TimestampType()),
        StructField("amount", DecimalType(15, 2)),
        StructField("running_balance", DecimalType(15, 2)),
        StructField("merchant_name", StringType()),
        StructField("merchant_category", StringType()),
        StructField("channel_code", StringType()),
        StructField("reference_num", StringType()),
        StructField("status_code", StringType()),
        StructField("created_ts", TimestampType()),
    ]
)

_TRANSACTION_TYPES = StructType(
    [
        StructField("transaction_type_cd", StringType()),
        StructField("description", StringType()),
        StructField("category", StringType()),
        StructField("is_revenue", StringType()),
        StructField("effective_date", DateType()),
        StructField("expiration_date", DateType()),
    ]
)

_BUREAU = StructType(
    [
        StructField("customer_id", LongType()),
        StructField("external_credit_score", IntegerType()),
        StructField("report_date", DateType()),
    ]
)

_SOURCE_TABLES = {
    ("schema_core", "customers"): ("customers.csv", _CUSTOMERS),
    ("schema_core", "accounts"): ("accounts.csv", _ACCOUNTS),
    ("schema_core", "customer_bureau_scores"): ("customer_bureau_scores.csv", _BUREAU),
    ("schema_txn", "transactions"): ("transactions.csv", _TRANSACTIONS),
    ("schema_txn", "transaction_types"): ("transaction_types.csv", _TRANSACTION_TYPES),
}


@pytest.fixture(scope="session")
def spark(tmp_path_factory) -> SparkSession:
    warehouse = tmp_path_factory.mktemp("spark_warehouse")
    os.environ["SPARK_LOCAL_DIRS"] = str(tmp_path_factory.mktemp("spark_local"))
    # Point Derby metastore + Delta warehouse at a temp dir before the JVM starts.
    os.environ["SPARK_DELTA_WAREHOUSE"] = str(warehouse)
    session = (
        SparkSession.builder.appName("test_stg_risk_factors")
        .master("local[2]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={warehouse}/metastore_db;create=true",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        session = configure_spark_with_delta_pip(session)
    except Exception:
        pass
    spark = session.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def cfg() -> Config:
    os.environ["CATALOG"] = "spark_catalog"
    os.environ["RUN_DATE"] = "2026-04-10"
    return get_config()


@pytest.fixture(scope="session")
def seeded(spark, cfg):
    """Create schemas and load all source seed tables (session-scoped)."""
    for schema_attr in ("schema_core", "schema_txn", "schema_stg", "schema_dp"):
        schema = getattr(cfg, schema_attr)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

    for (schema_attr, name), (fname, schema) in _SOURCE_TABLES.items():
        df = (
            spark.read.option("header", True)
            .schema(schema)
            .csv(str(SOURCE_DIR / fname))
        )
        target = cfg.table(getattr(cfg, schema_attr), name)
        df.write.format("delta").mode("overwrite").saveAsTable(target)
    return cfg
