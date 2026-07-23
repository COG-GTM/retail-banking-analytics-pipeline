"""Staging job: build ``etl_staging.stg_customer_360`` (Customer 360).

Port of ``bteq/01_stg_customer_360.bteq``. Joins customers, their primary HOME
address and account roll-ups from ``core_banking`` into one denormalized silver
row per active/inactive customer, then writes it idempotently to Delta.

Legacy construct -> PySpark mapping
-----------------------------------
* ``QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date
  DESC) = 1`` (primary HOME address)      -> ``Window`` + ``row_number()`` filter.
* ``GROUP BY customer_id`` account CASE/SUM/MAX roll-ups -> ``groupBy.agg``.
* ``CAST((CURRENT_DATE - dob) / 365.25 AS SMALLINT)``     -> ``datediff`` /365.25.
* ``CAST(MONTHS_BETWEEN(CURRENT_DATE, customer_since) AS INTEGER)`` -> ``months_between``.
* ``CURRENT_DATE``                                        -> ``cfg.run_date`` (reproducible).
* ``DROP/CREATE ... WITH DATA``                           -> Delta ``overwrite`` (idempotent).
* ``INSERT INTO ETL_RUN_LOG`` + validation count          -> ``log_step`` / ``validate_table``.
"""

from __future__ import annotations

import uuid

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common.audit import init_audit, log_step
from common.validation import validate_table

JOB_NAME = "01_stg_customer_360"
TABLE_NAME = "stg_customer_360"

# Final column order / types, matching ddl STG_CUSTOMER_360.
_OUTPUT_TYPES = {
    "customer_id": "bigint",
    "first_name": "string",
    "last_name": "string",
    "date_of_birth": "date",
    "age": "smallint",
    "customer_since": "date",
    "tenure_months": "int",
    "customer_status": "string",
    "segment_code": "string",
    "branch_id": "int",
    "primary_address": "string",
    "city": "string",
    "state_code": "string",
    "zip_code": "string",
    "num_accounts": "smallint",
    "num_active_accounts": "smallint",
    "has_checking": "string",
    "has_savings": "string",
    "has_credit": "string",
    "has_loan": "string",
    "total_balance": "decimal(18,2)",
    "total_credit_limit": "decimal(18,2)",
    "credit_utilization_pct": "decimal(5,2)",
    "load_ts": "timestamp",
}


def _primary_address(spark: SparkSession, cfg) -> DataFrame:
    """Most recent, unexpired HOME address per customer (row_number == 1)."""
    addresses = spark.table(cfg.table(cfg.schema_core, "addresses"))
    run_date = F.lit(cfg.run_date.isoformat()).cast("date")
    line_2 = F.trim(F.col("address_line_2"))
    line_2 = F.when((line_2.isNull()) | (line_2 == ""), None).otherwise(line_2)

    window = Window.partitionBy("customer_id").orderBy(F.col("effective_date").desc())
    return (
        addresses.where(
            (F.col("address_type") == "HOME")
            & (
                F.col("expiration_date").isNull()
                | (F.col("expiration_date") > run_date)
            )
        )
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .select(
            "customer_id",
            F.concat(
                F.trim(F.col("address_line_1")),
                F.coalesce(F.concat(F.lit(", "), line_2), F.lit("")),
            ).alias("primary_address"),
            "city",
            "state_code",
            "zip_code",
        )
    )


def _account_rollups(spark: SparkSession, cfg) -> DataFrame:
    """Per-customer account portfolio metrics (COUNT/SUM/MAX CASE roll-ups)."""
    accounts = spark.table(cfg.table(cfg.schema_core, "accounts"))

    def has_type(account_type: str):
        return F.max(
            F.when(F.col("account_type") == account_type, F.lit("Y")).otherwise(F.lit("N"))
        )

    credit_balance = F.sum(
        F.when(
            F.col("account_type") == "CREDIT", F.coalesce(F.col("current_balance"), F.lit(0))
        ).otherwise(F.lit(0))
    )
    return accounts.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("num_accounts"),
        F.sum(F.when(F.col("account_status") == "O", F.lit(1)).otherwise(F.lit(0))).alias(
            "num_active_accounts"
        ),
        has_type("CHECKING").alias("has_checking"),
        has_type("SAVINGS").alias("has_savings"),
        has_type("CREDIT").alias("has_credit"),
        has_type("LOAN").alias("has_loan"),
        F.sum(F.coalesce(F.col("current_balance"), F.lit(0))).alias("total_balance"),
        F.sum(
            F.when(
                F.col("account_type") == "CREDIT",
                F.coalesce(F.col("credit_limit"), F.lit(0)),
            ).otherwise(F.lit(0))
        ).alias("total_credit_limit"),
        credit_balance.alias("credit_balance"),
    )


def build(spark: SparkSession, cfg) -> DataFrame:
    """Return the STG_CUSTOMER_360 DataFrame (no side effects)."""
    customers = spark.table(cfg.table(cfg.schema_core, "customers"))
    addr = _primary_address(spark, cfg)
    acct = _account_rollups(spark, cfg)
    run_date = F.lit(cfg.run_date.isoformat()).cast("date")

    joined = (
        customers.where(F.col("customer_status").isin("A", "I"))
        .join(addr, on="customer_id", how="left")
        .join(acct, on="customer_id", how="left")
    )

    credit_util = (
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("credit_balance") / F.col("total_credit_limit") * 100),
        )
        .otherwise(F.lit(0))
        .cast("decimal(5,2)")
    )

    projected = joined.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("date_of_birth"),
        F.round(F.datediff(run_date, F.col("date_of_birth")) / F.lit(365.25)).alias("age"),
        F.col("customer_since"),
        F.round(F.months_between(run_date, F.col("customer_since"))).alias("tenure_months"),
        F.col("customer_status"),
        F.col("segment_code"),
        F.col("branch_id"),
        F.col("primary_address"),
        F.col("city"),
        F.col("state_code"),
        F.col("zip_code"),
        F.col("num_accounts"),
        F.col("num_active_accounts"),
        F.col("has_checking"),
        F.col("has_savings"),
        F.col("has_credit"),
        F.col("has_loan"),
        F.col("total_balance"),
        F.col("total_credit_limit"),
        credit_util.alias("credit_utilization_pct"),
        F.current_timestamp().alias("load_ts"),
    )

    return projected.select(
        *[F.col(name).cast(dtype).alias(name) for name, dtype in _OUTPUT_TYPES.items()]
    )


def run(spark: SparkSession, cfg) -> DataFrame:
    """Build, validate and idempotently write ``etl_staging.stg_customer_360``."""
    run_id = uuid.uuid4().hex
    init_audit(spark, cfg)
    log_step(spark, cfg, run_id, JOB_NAME, "build", "START")

    df = build(spark, cfg).cache()
    validate_table(
        df,
        min_rows=1,
        not_null_cols=["customer_id"],
        unique_keys=["customer_id"],
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    target = cfg.table(cfg.schema_stg, TABLE_NAME)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )

    result = spark.table(target)
    row_count = result.count()
    validate_table(result, min_rows=1, not_null_cols=["customer_id"], unique_keys=["customer_id"])
    log_step(spark, cfg, run_id, JOB_NAME, "FULL_LOAD", "SUCCESS", row_count=row_count)
    df.unpersist()
    return result
