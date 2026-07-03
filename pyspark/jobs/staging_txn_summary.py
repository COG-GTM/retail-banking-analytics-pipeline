"""Staging job 02 -- STG_TXN_SUMMARY.

Faithful PySpark port of ``bteq/02_stg_txn_summary.bteq``: per-customer/account
transaction-summary metrics over a configurable lookback window (``LOOKBACK_MONTHS``).

BTEQ -> PySpark mapping:
* ``VT_RUN_PARAMS`` (``ADD_MONTHS(CURRENT_DATE, -LOOKBACK_MONTHS)`` .. ``CURRENT_DATE``)
  -> ``config.lookback_start`` / ``config.run_date`` literals; the window +
  ``STATUS_CODE = 'P'`` predicate is pushed down as an early filter (scalability).
* ``INNER JOIN ACCOUNTS`` (customer_id, account_type) on ``account_id``.
* ``INNER JOIN TRANSACTION_TYPES`` on ``transaction_type_cd`` for ``category``,
  broadcast (``F.broadcast``) as the small dimension.
* Debit/fee dollar aggregates use ``ABS(amount)``; credit aggregates use the raw
  signed ``amount`` (matches the BTEQ exactly).
* Averages use ``AVG(CASE WHEN ... THEN ... ELSE NULL END)`` semantics -- NULLs are
  ignored by ``avg`` so the denominator is the matching-category count, not ``COUNT(*)``.
* ``TOP_MERCHANT_CATEGORY``: the BTEQ ``top_cat`` subquery ranks merchant categories
  per ACCOUNT_ID by total ``ABS`` spend (window+status='P', non-null category) and
  keeps ``ROW_NUMBER() = 1`` -> computed here as an account-level aggregate + window
  rank, left-joined back on ``account_id``.
* ``CAST(CURRENT_DATE - MAX(transaction_date) AS INTEGER)`` -> :func:`dates.days_since_expr`.
* Channel mix ``* 100.0 / NULLIFZERO(COUNT(*))`` -> ``... / count(*)`` (each group has
  >= 1 row); final ``DECIMAL(5,2)`` cast applied by ``enforce_schema``.

Mirrors :mod:`jobs.staging_customer_360`: pure ``transform`` functions +
a thin :func:`run` wiring I/O, validation, audit and the schema contract.
"""

from __future__ import annotations

import argparse
import datetime as _dt

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common import schemas
from common.audit import AuditLog
from common.config import PipelineConfig
from common.dates import days_since_expr, load_timestamp
from common.io import DataIO, LocalDataIO
from common.spark import build_spark
from common.validation import abort_on_failure, validate_table

JOB_NAME = "02_stg_txn_summary"
TARGET = "STG_TXN_SUMMARY"

_POSTED = "P"


def posted_window_txns(transactions: DataFrame, config: PipelineConfig) -> DataFrame:
    """Posted transactions within the lookback window (BTEQ ``WHERE`` + ``VT_RUN_PARAMS``).

    Filters early (``transaction_date BETWEEN period_start AND period_end AND
    status_code = 'P'``) so only the needed date slice is scanned downstream.
    """
    return transactions.filter(
        (F.col("transaction_date") >= F.lit(config.lookback_start))
        & (F.col("transaction_date") <= F.lit(config.run_date))
        & (F.col("status_code") == F.lit(_POSTED))
    )


def top_merchant_category(posted: DataFrame) -> DataFrame:
    """Top merchant category per ``account_id`` by total ABS spend (BTEQ ``top_cat``).

    Aggregates absolute spend per ``(account_id, merchant_category)``, keeps the
    highest-spend category per account (``ROW_NUMBER() = 1``); ``merchant_category``
    is the deterministic tiebreaker. NULL categories are excluded.
    """
    spend = (
        posted
        .filter(F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs(F.col("amount"))).alias("_spend"))
    )
    win = Window.partitionBy("account_id").orderBy(
        F.col("_spend").desc(), F.col("merchant_category").asc()
    )
    return (
        spend
        .withColumn("_rn", F.row_number().over(win))
        .filter(F.col("_rn") == 1)
        .select("account_id", F.col("merchant_category").alias("top_merchant_category"))
    )


def _category_count(category: str) -> F.Column:
    return F.sum(F.when(F.col("category") == category, 1).otherwise(0))


def _category_abs_sum(category: str) -> F.Column:
    return F.sum(
        F.when(F.col("category") == category, F.abs(F.col("amount"))).otherwise(F.lit(0))
    )


def _channel_pct(code: str) -> F.Column:
    hits = F.sum(F.when(F.col("channel_code") == code, 1).otherwise(0))
    return hits * F.lit(100.0) / F.count(F.lit(1))


def summarise(posted: DataFrame, config: PipelineConfig) -> DataFrame:
    """Group posted+enriched transactions into per-account summary metrics."""
    run_date = config.run_date
    return (
        posted.groupBy("customer_id", "account_id", "account_type").agg(
            F.count(F.lit(1)).alias("txn_count_total"),
            _category_count("DEBIT").alias("txn_count_debit"),
            _category_count("CREDIT").alias("txn_count_credit"),
            _category_count("FEE").alias("txn_count_fee"),
            _category_abs_sum("DEBIT").alias("amt_total_debit"),
            # Credits use the raw signed amount (NOT abs), matching the BTEQ.
            F.sum(
                F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(F.lit(0))
            ).alias("amt_total_credit"),
            _category_abs_sum("FEE").alias("amt_total_fees"),
            F.avg(
                F.when(F.col("category") == "DEBIT", F.abs(F.col("amount")))
            ).alias("amt_avg_debit"),
            F.avg(
                F.when(F.col("category") == "CREDIT", F.col("amount"))
            ).alias("amt_avg_credit"),
            F.max(
                F.when(F.col("category") == "DEBIT", F.abs(F.col("amount"))).otherwise(F.lit(0))
            ).alias("amt_max_single_debit"),
            F.max(
                F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(F.lit(0))
            ).alias("amt_max_single_credit"),
            F.countDistinct(F.col("merchant_name")).alias("distinct_merchants"),
            _channel_pct("ATM").alias("pct_atm"),
            _channel_pct("POS").alias("pct_pos"),
            _channel_pct("WEB").alias("pct_web"),
            _channel_pct("MOB").alias("pct_mobile"),
            F.max(F.col("transaction_date")).alias("_max_txn_date"),
        )
        .withColumn("days_since_last_txn", days_since_expr("_max_txn_date", run_date))
    )


def transform(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    config: PipelineConfig,
) -> DataFrame:
    """Build STG_TXN_SUMMARY (schema-enforced to the DDL)."""
    posted = posted_window_txns(transactions, config)

    acct = accounts.select("account_id", "customer_id", "account_type")
    types = transaction_types.select("transaction_type_cd", "category")

    enriched = (
        posted.alias("t")
        .join(acct.alias("a"), "account_id", "inner")
        .join(F.broadcast(types.alias("tt")), "transaction_type_cd", "inner")
    )

    summary = summarise(enriched, config)
    top_cat = top_merchant_category(posted)

    df = (
        summary
        .join(top_cat, "account_id", "left")
        .withColumn("summary_period_start", F.lit(config.lookback_start))
        .withColumn("summary_period_end", F.lit(config.run_date))
        .withColumn("load_ts", F.lit(load_timestamp()).cast("timestamp"))
    )
    return schemas.enforce_schema(df, schemas.STG_TXN_SUMMARY)


def run(
    spark: SparkSession,
    io: DataIO,
    config: PipelineConfig,
    audit: AuditLog | None = None,
) -> DataFrame:
    """Read sources, transform, validate, and write STG_TXN_SUMMARY."""
    audit = audit or AuditLog(log_level=config.log_level)
    audit.log_step(JOB_NAME, "START", "Beginning transaction-summary staging")

    transactions = io.read_source("TRANSACTIONS")
    accounts = io.read_source("ACCOUNTS")
    transaction_types = io.read_source("TRANSACTION_TYPES")

    out = transform(transactions, accounts, transaction_types, config).cache()
    n = out.count()

    result = validate_table(
        out,
        TARGET,
        key_cols=["customer_id", "account_id"],
        not_null=["customer_id", "account_id"],
        min_rows=1,
        audit=audit,
    )
    abort_on_failure(result)

    io.write_staging(out, TARGET)
    audit.run_log_row(JOB_NAME, n)
    audit.log_step(JOB_NAME, "SUCCESS", "Staging table written", rowcount=n)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build STG_TXN_SUMMARY")
    parser.add_argument("--source-dir", required=True)
    parser.add_argument("--lake-dir", required=True)
    parser.add_argument("--run-date", default=None)
    args = parser.parse_args(argv)

    config = PipelineConfig.from_env().with_overrides(
        **({"run_date": _dt.date.fromisoformat(args.run_date)} if args.run_date else {})
    )
    spark = build_spark(JOB_NAME)
    io = LocalDataIO(spark, config, args.source_dir, args.lake_dir)
    run(spark, io, config)


if __name__ == "__main__":
    main()
