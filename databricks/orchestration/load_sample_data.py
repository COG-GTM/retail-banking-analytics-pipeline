"""Load the repository's sample source CSVs (``data/01_source_tables``) into the
Unity Catalog source Delta tables.

Used by the local orchestrator and the end-to-end test so the ported pipeline can
run against real data without a Databricks cluster. Not part of the production
pipeline (on Databricks the source tables are populated upstream).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = REPO_ROOT / "data" / "01_source_tables"

_SCHEMAS = {
    "customers": StructType([
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
    ]),
    "accounts": StructType([
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
    ]),
    "addresses": StructType([
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
        StructField("created_ts", TimestampType()),
        StructField("updated_ts", TimestampType()),
    ]),
    "customer_bureau_scores": StructType([
        StructField("customer_id", LongType()),
        StructField("external_credit_score", IntegerType()),
        StructField("report_date", DateType()),
    ]),
    "transaction_types": StructType([
        StructField("transaction_type_cd", StringType()),
        StructField("description", StringType()),
        StructField("category", StringType()),
        StructField("is_revenue", StringType()),
        StructField("effective_date", DateType()),
        StructField("expiration_date", DateType()),
    ]),
    "transactions": StructType([
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
    ]),
}

# is_revenue arrives as true/false in the CSV; keep as STRING to match the DDL.
_BOOL_STRING_COLUMNS = {"transaction_types": ["is_revenue"]}

# Which Unity Catalog schema each source table lands in.
_TXN_TABLES = {"transactions", "transaction_types"}


def load_sample_sources(spark: SparkSession, config, source_dir: Path = SOURCE_DIR) -> None:
    """Read each source CSV and overwrite the corresponding source Delta table."""
    for table_name, schema in _SCHEMAS.items():
        csv_path = source_dir / f"{table_name}.csv"
        # Read is_revenue as boolean then cast to string so 'true'/'false' parse.
        read_schema = schema
        if table_name in _BOOL_STRING_COLUMNS:
            read_schema = StructType([
                StructField(f.name, BooleanType() if f.name in _BOOL_STRING_COLUMNS[table_name] else f.dataType)
                for f in schema.fields
            ])
        df = (
            spark.read.option("header", True)
            .schema(read_schema)
            .csv(str(csv_path))
        )
        for col_name in _BOOL_STRING_COLUMNS.get(table_name, []):
            df = df.withColumn(col_name, df[col_name].cast("string"))
        schema_name = config.txn_schema if table_name in _TXN_TABLES else config.core_schema
        fqn = config.table(schema_name, table_name)
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fqn)
