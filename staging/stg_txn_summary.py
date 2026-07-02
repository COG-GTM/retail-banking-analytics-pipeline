"""stg_txn_summary - PySpark port of ``bteq/02_stg_txn_summary.bteq``.

Aggregates transaction-level data into per-customer/account summary metrics
over a configurable lookback window.

Teradata -> Spark translation notes:
  * ``CREATE VOLATILE TABLE VT_RUN_PARAMS`` (period bounds) -> literal columns
    derived from the configured ``as_of_date`` and ``lookback_months``.
  * ``ADD_MONTHS`` -> ``F.add_months``.
  * ``QUALIFY ROW_NUMBER() OVER (... ORDER BY SUM(...) OVER (...))`` for the
    top merchant category -> a per-(account, category) spend aggregation +
    ``row_number()`` window (deterministic tie-break on category name).
  * ``NULLIFZERO(COUNT(*))`` -> guarded division (group counts are always >= 1).
  * ``AVG(CASE ... ELSE NULL END)`` -> ``F.avg`` over a NULL-producing
    expression (NULLs are ignored, matching Teradata).
  * ``CAST(... AS DECIMAL(5,2))`` channel-mix percentages preserved; other
    aggregates keep full precision exactly as the BTEQ did (no extra casts).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from staging.config import StagingConfig
from staging.spark_utils import get_logger, read_all_sources, write_staging

TABLE_NAME = "stg_txn_summary"

OUTPUT_COLUMNS = [
    "customer_id", "account_id", "account_type", "summary_period_start",
    "summary_period_end", "txn_count_total", "txn_count_debit",
    "txn_count_credit", "txn_count_fee", "amt_total_debit", "amt_total_credit",
    "amt_total_fees", "amt_avg_debit", "amt_avg_credit", "amt_max_single_debit",
    "amt_max_single_credit", "distinct_merchants", "top_merchant_category",
    "pct_atm", "pct_pos", "pct_web", "pct_mobile", "days_since_last_txn",
    "load_ts",
]


def _pct(channel: str) -> "F.Column":
    numerator = F.sum(F.when(F.col("channel_code") == channel, 1).otherwise(0)) * 100.0
    return (numerator / F.count(F.lit(1))).cast(DecimalType(5, 2))


def transform(
    transactions: DataFrame,
    transaction_types: DataFrame,
    accounts: DataFrame,
    as_of_date: date,
    lookback_months: int,
) -> DataFrame:
    as_of = F.lit(as_of_date).cast("date")
    # DECIMAL(15,2) zero default keeps monetary aggregates exact (a DOUBLE
    # literal would promote the surrounding expression to DOUBLE).
    money_zero = F.lit(0).cast(DecimalType(15, 2))
    period_start = F.add_months(as_of, -lookback_months)
    period_end = as_of

    in_period = (
        (F.col("transaction_date") >= period_start)
        & (F.col("transaction_date") <= period_end)
        & (F.col("status_code") == "P")
    )

    # Top merchant category per account (by total absolute posted spend).
    top_spend = (
        transactions.filter(in_period & F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs(F.col("amount"))).alias("cat_spend"))
    )
    top_win = Window.partitionBy("account_id").orderBy(
        F.col("cat_spend").desc(), F.col("merchant_category").asc()
    )
    top_cat = (
        top_spend.withColumn("_rn", F.row_number().over(top_win))
        .filter(F.col("_rn") == 1)
        .select(
            "account_id",
            F.col("merchant_category").alias("top_merchant_category"),
        )
    )

    base = (
        transactions.alias("t")
        .join(accounts.alias("acct"), on="account_id", how="inner")
        .join(transaction_types.alias("tt"), on="transaction_type_cd", how="inner")
        .filter(in_period)
    )

    is_debit = F.col("tt.category") == "DEBIT"
    is_credit = F.col("tt.category") == "CREDIT"
    is_fee = F.col("tt.category") == "FEE"

    agg = base.groupBy(
        F.col("acct.customer_id").alias("customer_id"),
        F.col("account_id"),
        F.col("acct.account_type").alias("account_type"),
    ).agg(
        F.count(F.lit(1)).alias("txn_count_total"),
        F.sum(F.when(is_debit, 1).otherwise(0)).alias("txn_count_debit"),
        F.sum(F.when(is_credit, 1).otherwise(0)).alias("txn_count_credit"),
        F.sum(F.when(is_fee, 1).otherwise(0)).alias("txn_count_fee"),
        F.sum(F.when(is_debit, F.abs(F.col("t.amount"))).otherwise(money_zero)).alias("amt_total_debit"),
        F.sum(F.when(is_credit, F.col("t.amount")).otherwise(money_zero)).alias("amt_total_credit"),
        F.sum(F.when(is_fee, F.abs(F.col("t.amount"))).otherwise(money_zero)).alias("amt_total_fees"),
        F.avg(F.when(is_debit, F.abs(F.col("t.amount")))).alias("amt_avg_debit"),
        F.avg(F.when(is_credit, F.col("t.amount"))).alias("amt_avg_credit"),
        F.max(F.when(is_debit, F.abs(F.col("t.amount"))).otherwise(money_zero)).alias("amt_max_single_debit"),
        F.max(F.when(is_credit, F.col("t.amount")).otherwise(money_zero)).alias("amt_max_single_credit"),
        F.countDistinct(F.col("t.merchant_name")).alias("distinct_merchants"),
        _pct("ATM").alias("pct_atm"),
        _pct("POS").alias("pct_pos"),
        _pct("WEB").alias("pct_web"),
        _pct("MOB").alias("pct_mobile"),
        F.datediff(as_of, F.max(F.col("t.transaction_date"))).cast("int").alias("days_since_last_txn"),
    )

    result = (
        agg.join(top_cat, on="account_id", how="left")
        .withColumn("summary_period_start", period_start)
        .withColumn("summary_period_end", period_end)
        .withColumn("load_ts", F.current_timestamp())
    )
    return result.select(*OUTPUT_COLUMNS)


def run(spark: SparkSession, cfg: StagingConfig) -> int:
    logger = get_logger(cfg, "staging.stg_txn_summary")
    logger.info("step start", step=TABLE_NAME, status="START")
    src = read_all_sources(spark, cfg)
    df = transform(
        src["transactions"], src["transaction_types"], src["accounts"],
        cfg.as_of_date, cfg.lookback_months,
    )
    df = df.cache()
    row_count = df.count()
    write_staging(df, cfg, TABLE_NAME, logger)
    logger.info("step complete", step=TABLE_NAME, status="SUCCESS", row_count=row_count)
    return row_count
