"""Seed the ``core_banking`` source Delta tables from the repo sample CSVs."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = REPO_ROOT / "data" / "01_source_tables"

_CUSTOMERS = StructType([
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
    StructField("created_ts", StringType()),
    StructField("updated_ts", StringType()),
])

_ACCOUNTS = StructType([
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
    StructField("created_ts", StringType()),
    StructField("updated_ts", StringType()),
])

_ADDRESSES = StructType([
    StructField("address_id", LongType()),
    StructField("customer_id", LongType()),
    StructField("address_type", StringType()),
    StructField("address_line_1", StringType()),
    StructField("address_line_2", StringType()),
    StructField("city", StringType()),
    StructField("state_code", StringType()),
    StructField("zip_code", StringType()),
    StructField("country_code", StringType()),
    StructField("is_primary", StringType()),
    StructField("effective_date", DateType()),
    StructField("expiration_date", DateType()),
    StructField("created_ts", StringType()),
    StructField("updated_ts", StringType()),
])

_TABLES = {
    "customers": _CUSTOMERS,
    "accounts": _ACCOUNTS,
    "addresses": _ADDRESSES,
}


def _read_csv(spark: SparkSession, name: str, schema: StructType):
    return spark.read.csv(
        str(SOURCE_DIR / f"{name}.csv"), header=True, schema=schema
    )


def seed_core_banking(spark: SparkSession, cfg) -> None:
    """Load customers/accounts/addresses CSVs into Delta ``core_banking`` tables."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_core}")
    for name, schema in _TABLES.items():
        df = _read_csv(spark, name, schema)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(cfg.table(cfg.schema_core, name))
        )
