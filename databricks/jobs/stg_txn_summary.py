"""Ticket 5 - Transaction summary staging.

PySpark port of ``bteq/02_stg_txn_summary.bteq``.

Inputs  : txn_processing.transactions, txn_processing.transaction_types,
          core_banking.accounts
Output  : etl_staging.stg_txn_summary  (Delta)

The Teradata ``VT_RUN_PARAMS`` volatile table (the configurable lookback window)
becomes the ``lookback_months`` / ``run_date`` parameters. The top-merchant-category
QUALIFY sub-query becomes a window function.
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

OUTPUT_COLUMNS = [
    "customer_id", "account_id", "account_type", "summary_period_start",
    "summary_period_end", "txn_count_total", "txn_count_debit", "txn_count_credit",
    "txn_count_fee", "amt_total_debit", "amt_total_credit", "amt_total_fees",
    "amt_avg_debit", "amt_avg_credit", "amt_max_single_debit", "amt_max_single_credit",
    "distinct_merchants", "top_merchant_category", "pct_atm", "pct_pos", "pct_web",
    "pct_mobile", "days_since_last_txn", "load_ts",
]


def _top_merchant_category(posted_in_window: DataFrame) -> DataFrame:
    """Top merchant category per account by total absolute spend (QUALIFY)."""
    by_cat = (
        posted_in_window.where(F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs(F.col("amount"))).alias("_cat_spend"))
    )
    w = Window.partitionBy("account_id").orderBy(
        F.col("_cat_spend").desc(), F.col("merchant_category").asc()
    )
    return (
        by_cat.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .select("account_id", F.col("merchant_category").alias("top_merchant_category"))
    )


def build_stg_txn_summary(
    transactions: DataFrame,
    transaction_types: DataFrame,
    accounts: DataFrame,
    lookback_months: int,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    period_start = F.add_months(F.lit(run_date), -lookback_months)
    period_end = F.lit(run_date)

    posted_in_window = transactions.where(
        (F.col("status_code") == "P")
        & (F.col("transaction_date") >= period_start)
        & (F.col("transaction_date") <= period_end)
    )

    top_cat = _top_merchant_category(posted_in_window)

    is_debit = F.col("category") == "DEBIT"
    is_credit = F.col("category") == "CREDIT"
    is_fee = F.col("category") == "FEE"
    total = F.count(F.lit(1))

    def pct(channel: str) -> "F.Column":
        return (
            F.sum(F.when(F.col("channel_code") == channel, 1).otherwise(0)) * 100.0 / total
        ).cast("decimal(5,2)")

    aggregated = (
        posted_in_window.join(
            accounts.select("account_id", "customer_id", "account_type"),
            "account_id",
            "inner",
        )
        .join(transaction_types.select("transaction_type_cd", "category"), "transaction_type_cd", "inner")
        .groupBy("customer_id", "account_id", "account_type")
        .agg(
            total.cast("int").alias("txn_count_total"),
            F.sum(F.when(is_debit, 1).otherwise(0)).cast("int").alias("txn_count_debit"),
            F.sum(F.when(is_credit, 1).otherwise(0)).cast("int").alias("txn_count_credit"),
            F.sum(F.when(is_fee, 1).otherwise(0)).cast("int").alias("txn_count_fee"),
            F.sum(F.when(is_debit, F.abs(F.col("amount"))).otherwise(0))
            .cast("decimal(18,2)").alias("amt_total_debit"),
            F.sum(F.when(is_credit, F.col("amount")).otherwise(0))
            .cast("decimal(18,2)").alias("amt_total_credit"),
            F.sum(F.when(is_fee, F.abs(F.col("amount"))).otherwise(0))
            .cast("decimal(18,2)").alias("amt_total_fees"),
            F.avg(F.when(is_debit, F.abs(F.col("amount")))).cast("decimal(15,2)").alias("amt_avg_debit"),
            F.avg(F.when(is_credit, F.col("amount"))).cast("decimal(15,2)").alias("amt_avg_credit"),
            F.max(F.when(is_debit, F.abs(F.col("amount"))).otherwise(0))
            .cast("decimal(15,2)").alias("amt_max_single_debit"),
            F.max(F.when(is_credit, F.col("amount")).otherwise(0))
            .cast("decimal(15,2)").alias("amt_max_single_credit"),
            F.countDistinct(F.col("merchant_name")).cast("int").alias("distinct_merchants"),
            pct("ATM").alias("pct_atm"),
            pct("POS").alias("pct_pos"),
            pct("WEB").alias("pct_web"),
            pct("MOB").alias("pct_mobile"),
            F.datediff(period_end, F.max(F.col("transaction_date"))).cast("int").alias(
                "days_since_last_txn"
            ),
        )
    )

    return (
        aggregated.join(top_cat, "account_id", "left")
        .select(
            F.col("customer_id"),
            F.col("account_id"),
            F.col("account_type"),
            period_start.cast("date").alias("summary_period_start"),
            period_end.cast("date").alias("summary_period_end"),
            F.col("txn_count_total"),
            F.col("txn_count_debit"),
            F.col("txn_count_credit"),
            F.col("txn_count_fee"),
            F.col("amt_total_debit"),
            F.col("amt_total_credit"),
            F.col("amt_total_fees"),
            F.col("amt_avg_debit"),
            F.col("amt_avg_credit"),
            F.col("amt_max_single_debit"),
            F.col("amt_max_single_credit"),
            F.col("distinct_merchants"),
            F.col("top_merchant_category"),
            F.col("pct_atm"),
            F.col("pct_pos"),
            F.col("pct_web"),
            F.col("pct_mobile"),
            F.col("days_since_last_txn"),
            F.lit(load_ts).cast("timestamp").alias("load_ts"),
        )
    )
