from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ..config import RunConfig
from ..tables import write_overwrite

SOURCE_SCHEMAS = {
    "customers": StructType(
        [
            StructField("customer_id", LongType(), False),
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
    ),
    "accounts": StructType(
        [
            StructField("account_id", LongType(), False),
            StructField("customer_id", LongType(), False),
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
    ),
    "addresses": StructType(
        [
            StructField("address_id", LongType(), False),
            StructField("customer_id", LongType(), False),
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
        ]
    ),
    "customer_bureau_scores": StructType(
        [
            StructField("customer_id", LongType(), False),
            StructField("external_credit_score", IntegerType()),
            StructField("report_date", DateType()),
        ]
    ),
    "transactions": StructType(
        [
            StructField("transaction_id", LongType(), False),
            StructField("account_id", LongType(), False),
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
    ),
    "transaction_types": StructType(
        [
            StructField("transaction_type_cd", StringType(), False),
            StructField("description", StringType()),
            StructField("category", StringType()),
            StructField("is_revenue", StringType()),
            StructField("effective_date", DateType()),
            StructField("expiration_date", DateType()),
        ]
    ),
}

_CORE_TABLES = {"customers", "accounts", "addresses", "customer_bureau_scores"}


def _fqn(cfg: RunConfig, table: str) -> str:
    schema = cfg.bronze_core_schema if table in _CORE_TABLES else cfg.bronze_txn_schema
    return cfg.fqn(schema, table)


def _read(spark: SparkSession, path: str, table: str) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("mode", "FAILFAST")
        .schema(SOURCE_SCHEMAS[table])
        .csv(path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )


def ingest_batch(spark: SparkSession, cfg: RunConfig, source_dir: str) -> None:
    for table in SOURCE_SCHEMAS:
        write_overwrite(
            _read(spark, str(Path(source_dir) / f"{table}.csv"), table), _fqn(cfg, table)
        )


def ingest_autoloader(
    spark: SparkSession, cfg: RunConfig, landing_dir: str, checkpoint_dir: str
) -> None:
    for table, schema in SOURCE_SCHEMAS.items():
        (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .schema(schema)
            .load(str(Path(landing_dir) / f"{table}.csv"))
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_source_file", input_file_name())
            .writeStream.trigger(availableNow=True)
            .option("checkpointLocation", str(Path(checkpoint_dir) / table))
            .toTable(_fqn(cfg, table))
            .awaitTermination()
        )


def copy_into_sql(cfg: RunConfig, table: str, path: str) -> str:
    return (
        f"COPY INTO {cfg.fqn(cfg.bronze_core_schema if table in _CORE_TABLES else cfg.bronze_txn_schema, table)} "
        f"FROM '{path}' FILEFORMAT = CSV FORMAT_OPTIONS ('header' = 'true')"
    )
