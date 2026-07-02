"""stg_customer_360 - PySpark port of ``bteq/01_stg_customer_360.bteq``.

Builds a denormalized customer-360 staging table by joining customer, account
and address data.

Teradata -> Spark translation notes:
  * ``QUALIFY ROW_NUMBER() OVER (...) = 1`` (latest HOME address) ->
    :class:`~pyspark.sql.Window` + ``row_number()`` filter.
  * ``MONTHS_BETWEEN`` -> ``F.months_between``.
  * ``CAST(... AS SMALLINT/INTEGER)`` -> ``.cast()`` (truncates toward zero,
    matching Teradata integer casts).
  * ``a.ADDRESS_LINE_1 || COALESCE(', ' || a.ADDRESS_LINE_2, '')`` ->
    ``concat`` + ``coalesce`` (NULL-propagating concat mirrors Teradata ``||``).
  * ``CREATE MULTISET TABLE ... WITH DATA`` / ``COLLECT STATISTICS`` /
    ``PRIMARY INDEX`` -> dropped (physical-storage directives with no Spark
    equivalent); the result is written via :func:`staging.spark_utils.write_staging`.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, ShortType

from staging.config import StagingConfig
from staging.spark_utils import get_logger, read_all_sources, write_staging

TABLE_NAME = "stg_customer_360"

OUTPUT_COLUMNS = [
    "customer_id", "first_name", "last_name", "date_of_birth", "age",
    "customer_since", "tenure_months", "customer_status", "segment_code",
    "branch_id", "primary_address", "city", "state_code", "zip_code",
    "num_accounts", "num_active_accounts", "has_checking", "has_savings",
    "has_credit", "has_loan", "total_balance", "total_credit_limit",
    "credit_utilization_pct", "load_ts",
]


def transform(
    customers: DataFrame,
    accounts: DataFrame,
    addresses: DataFrame,
    as_of_date: date,
) -> DataFrame:
    as_of = F.lit(as_of_date).cast("date")
    # DECIMAL(15,2) zero used as coalesce/else default so monetary aggregates
    # stay exact DECIMAL (a DOUBLE literal would promote them to DOUBLE).
    money_zero = F.lit(0).cast(DecimalType(15, 2))

    # Latest non-expired HOME address per customer (QUALIFY ROW_NUMBER = 1).
    addr_window = Window.partitionBy("customer_id").orderBy(F.col("effective_date").desc())
    addr = (
        addresses.filter(
            (F.col("address_type") == "HOME")
            & (F.col("expiration_date").isNull() | (F.col("expiration_date") > as_of))
        )
        .withColumn("_rn", F.row_number().over(addr_window))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_id", "address_line_1", "address_line_2",
            "city", "state_code", "zip_code",
        )
    )

    # Aggregated account portfolio metrics per customer.
    acct_agg = accounts.groupBy("customer_id").agg(
        F.count(F.lit(1)).alias("num_accounts"),
        F.sum(F.when(F.col("account_status") == "O", 1).otherwise(0)).alias("num_active_accounts"),
        F.max(F.when(F.col("account_type") == "CHECKING", "Y").otherwise("N")).alias("has_checking"),
        F.max(F.when(F.col("account_type") == "SAVINGS", "Y").otherwise("N")).alias("has_savings"),
        F.max(F.when(F.col("account_type") == "CREDIT", "Y").otherwise("N")).alias("has_credit"),
        F.max(F.when(F.col("account_type") == "LOAN", "Y").otherwise("N")).alias("has_loan"),
        F.sum(F.coalesce(F.col("current_balance"), money_zero)).alias("total_balance"),
        F.sum(
            F.when(F.col("account_type") == "CREDIT", F.coalesce(F.col("credit_limit"), money_zero))
            .otherwise(money_zero)
        ).alias("total_credit_limit"),
        F.sum(
            F.when(F.col("account_type") == "CREDIT", F.coalesce(F.col("current_balance"), money_zero))
            .otherwise(money_zero)
        ).alias("credit_balance"),
    )

    address_expr = F.concat(
        F.trim(F.col("address_line_1")),
        F.coalesce(F.concat(F.lit(", "), F.trim(F.col("address_line_2"))), F.lit("")),
    )

    credit_util_expr = (
        F.when(
            F.col("total_credit_limit") > 0,
            (F.col("credit_balance") / F.col("total_credit_limit") * 100).cast(DecimalType(5, 2)),
        )
        .otherwise(F.lit(0.00).cast(DecimalType(5, 2)))
    )

    result = (
        customers.alias("c")
        .filter(F.col("customer_status").isin("A", "I"))
        .join(addr.alias("a"), on="customer_id", how="left")
        .join(acct_agg.alias("acct"), on="customer_id", how="left")
        .select(
            F.col("customer_id"),
            F.col("c.first_name").alias("first_name"),
            F.col("c.last_name").alias("last_name"),
            F.col("c.date_of_birth").alias("date_of_birth"),
            (F.datediff(as_of, F.col("c.date_of_birth")) / 365.25).cast(ShortType()).alias("age"),
            F.col("c.customer_since").alias("customer_since"),
            F.months_between(as_of, F.col("c.customer_since")).cast("int").alias("tenure_months"),
            F.col("c.customer_status").alias("customer_status"),
            F.col("c.segment_code").alias("segment_code"),
            F.col("c.branch_id").alias("branch_id"),
            address_expr.alias("primary_address"),
            F.col("a.city").alias("city"),
            F.col("a.state_code").alias("state_code"),
            F.col("a.zip_code").alias("zip_code"),
            F.col("acct.num_accounts").alias("num_accounts"),
            F.col("acct.num_active_accounts").alias("num_active_accounts"),
            F.col("acct.has_checking").alias("has_checking"),
            F.col("acct.has_savings").alias("has_savings"),
            F.col("acct.has_credit").alias("has_credit"),
            F.col("acct.has_loan").alias("has_loan"),
            F.col("acct.total_balance").alias("total_balance"),
            F.col("acct.total_credit_limit").alias("total_credit_limit"),
            credit_util_expr.alias("credit_utilization_pct"),
            F.current_timestamp().alias("load_ts"),
        )
    )
    return result.select(*OUTPUT_COLUMNS)


def run(spark: SparkSession, cfg: StagingConfig) -> int:
    logger = get_logger(cfg, "staging.stg_customer_360")
    logger.info("step start", step=TABLE_NAME, status="START")
    src = read_all_sources(spark, cfg)
    df = transform(src["customers"], src["accounts"], src["addresses"], cfg.as_of_date)
    df = df.cache()
    row_count = df.count()
    write_staging(df, cfg, TABLE_NAME, logger)
    logger.info("step complete", step=TABLE_NAME, status="SUCCESS", row_count=row_count)
    return row_count
