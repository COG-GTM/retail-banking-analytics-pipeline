"""Staging job 02 — ``etl_staging.stg_txn_summary``.

Faithful PySpark port of ``bteq/02_stg_txn_summary.bteq``: per-customer/account
transaction-summary metrics aggregated over a configurable lookback window
(``Config.lookback_months``), from posted transactions only.

Legacy construct → PySpark mapping
----------------------------------
* ``VT_RUN_PARAMS`` volatile table (``ADD_MONTHS(CURRENT_DATE, -LOOKBACK_MONTHS)``
  .. ``CURRENT_DATE``) → ``summary_period_start`` / ``summary_period_end`` literals
  derived from ``Config.run_date`` + ``Config.lookback_months`` (:func:`_add_months`).
  The window + ``STATUS_CODE = 'P'`` predicate is pushed down as an early filter.
* ``INNER JOIN CORE_BANKING_DB.ACCOUNTS`` on ``account_id`` → ``customer_id`` /
  ``account_type``. ``INNER JOIN TXN_PROCESSING_DB.TRANSACTION_TYPES`` on
  ``transaction_type_cd`` → ``category`` (broadcast small dimension).
* Debit / fee dollar aggregates use ``ABS(amount)``; credit aggregates use the raw
  signed ``amount`` — matching the BTEQ exactly.
* ``AVG(CASE WHEN ... THEN ... ELSE NULL END)`` → ``avg(when(...))``; NULLs are
  ignored so the denominator is the matching-category count, not ``COUNT(*)``.
* ``TOP_MERCHANT_CATEGORY``: the BTEQ ``top_cat`` subquery ranks merchant categories
  per ``ACCOUNT_ID`` by total ``ABS`` spend (window + ``status='P'``, non-null
  category), ``QUALIFY ROW_NUMBER() = 1`` → account-level aggregate + windowed rank,
  left-joined back on ``account_id`` (``merchant_category`` asc as the deterministic
  tiebreaker).
* ``CAST(CURRENT_DATE - MAX(transaction_date) AS INTEGER)`` → ``datediff(run_date, max)``.
* Channel mix ``SUM(CASE ...) * 100.0 / NULLIFZERO(COUNT(*))`` → ``sum(...) * 100 /
  count(*)`` rounded to 2 dp. ``ATM``/``POS``/``WEB``/``MOB`` only, so the four
  percentages sum to <= 100 (``ACH`` and other channels are excluded, as in the BTEQ).

Output is written idempotently (Delta ``overwrite``) to ``etl_staging.stg_txn_summary``;
run steps are wrapped with ``log_step`` and the result validated with ``validate_table``
(not-null + unique ``(customer_id, account_id)``) before write.
"""

from __future__ import annotations

import calendar
import datetime as _dt
import uuid

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.audit import init_audit, log_step
from common.config import Config, get_config
from common.spark import get_spark
from common.validation import validate_table

JOB_NAME = "02_stg_txn_summary"
TARGET_TABLE = "stg_txn_summary"
_POSTED = "P"
_DEBIT_CHANNELS = {"pct_atm": "ATM", "pct_pos": "POS", "pct_web": "WEB", "pct_mobile": "MOB"}


def _add_months(d: _dt.date, months: int) -> _dt.date:
    """Teradata ``ADD_MONTHS`` semantics: shift months, clamping the day of month."""
    total = d.month - 1 + months
    year = d.year + total // 12
    month = total % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return _dt.date(year, month, day)


def _period_start(cfg: Config) -> _dt.date:
    return _add_months(cfg.run_date, -cfg.lookback_months)


def posted_window_txns(transactions: DataFrame, cfg: Config) -> DataFrame:
    """Posted transactions within the lookback window (BTEQ ``WHERE`` + ``VT_RUN_PARAMS``)."""
    return transactions.where(
        (F.col("transaction_date") >= F.lit(_period_start(cfg)))
        & (F.col("transaction_date") <= F.lit(cfg.run_date))
        & (F.col("status_code") == F.lit(_POSTED))
    )


def top_merchant_category(posted: DataFrame) -> DataFrame:
    """Top merchant category per ``account_id`` by total ABS spend (BTEQ ``top_cat``)."""
    spend = (
        posted.where(F.col("merchant_category").isNotNull())
        .groupBy("account_id", "merchant_category")
        .agg(F.sum(F.abs(F.col("amount"))).alias("_spend"))
    )
    ranked = Window.partitionBy("account_id").orderBy(
        F.col("_spend").desc(), F.col("merchant_category").asc()
    )
    return (
        spend.withColumn("_rn", F.row_number().over(ranked))
        .where(F.col("_rn") == 1)
        .select("account_id", F.col("merchant_category").alias("top_merchant_category"))
    )


def _cat_count(category: str) -> F.Column:
    return F.sum(F.when(F.col("category") == category, 1).otherwise(0))


def _cat_abs_sum(category: str) -> F.Column:
    return F.sum(
        F.when(F.col("category") == category, F.abs(F.col("amount"))).otherwise(F.lit(0.0))
    )


def _channel_pct(code: str) -> F.Column:
    hits = F.sum(F.when(F.col("channel_code") == code, 1).otherwise(0))
    return F.round(hits * F.lit(100.0) / F.count(F.lit(1)), 2)


def summarise(enriched: DataFrame, cfg: Config) -> DataFrame:
    """Group enriched posted transactions into per-account summary metrics."""
    merchant = F.when(F.trim(F.col("merchant_name")) == "", None).otherwise(
        F.col("merchant_name")
    )
    aggregated = enriched.groupBy("customer_id", "account_id", "account_type").agg(
        F.count(F.lit(1)).alias("txn_count_total"),
        _cat_count("DEBIT").alias("txn_count_debit"),
        _cat_count("CREDIT").alias("txn_count_credit"),
        _cat_count("FEE").alias("txn_count_fee"),
        _cat_abs_sum("DEBIT").alias("amt_total_debit"),
        F.sum(
            F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(F.lit(0.0))
        ).alias("amt_total_credit"),
        _cat_abs_sum("FEE").alias("amt_total_fees"),
        F.avg(F.when(F.col("category") == "DEBIT", F.abs(F.col("amount")))).alias(
            "amt_avg_debit"
        ),
        F.avg(F.when(F.col("category") == "CREDIT", F.col("amount"))).alias("amt_avg_credit"),
        F.max(
            F.when(F.col("category") == "DEBIT", F.abs(F.col("amount"))).otherwise(F.lit(0.0))
        ).alias("amt_max_single_debit"),
        F.max(
            F.when(F.col("category") == "CREDIT", F.col("amount")).otherwise(F.lit(0.0))
        ).alias("amt_max_single_credit"),
        F.countDistinct(merchant).alias("distinct_merchants"),
        _channel_pct("ATM").alias("pct_atm"),
        _channel_pct("POS").alias("pct_pos"),
        _channel_pct("WEB").alias("pct_web"),
        _channel_pct("MOB").alias("pct_mobile"),
        F.max(F.col("transaction_date")).alias("_max_txn_date"),
    )
    return aggregated.withColumn(
        "days_since_last_txn",
        F.datediff(F.lit(cfg.run_date), F.col("_max_txn_date")).cast("int"),
    )


def _enforce_schema(df: DataFrame, cfg: Config) -> DataFrame:
    """Project/cast to the STG_TXN_SUMMARY DDL contract, in DDL column order."""
    return df.select(
        F.col("customer_id").cast("bigint").alias("customer_id"),
        F.col("account_id").cast("bigint").alias("account_id"),
        F.col("account_type").cast("string").alias("account_type"),
        F.lit(_period_start(cfg)).cast("date").alias("summary_period_start"),
        F.lit(cfg.run_date).cast("date").alias("summary_period_end"),
        F.col("txn_count_total").cast("int").alias("txn_count_total"),
        F.col("txn_count_debit").cast("int").alias("txn_count_debit"),
        F.col("txn_count_credit").cast("int").alias("txn_count_credit"),
        F.col("txn_count_fee").cast("int").alias("txn_count_fee"),
        F.col("amt_total_debit").cast("decimal(18,2)").alias("amt_total_debit"),
        F.col("amt_total_credit").cast("decimal(18,2)").alias("amt_total_credit"),
        F.col("amt_total_fees").cast("decimal(18,2)").alias("amt_total_fees"),
        F.col("amt_avg_debit").cast("decimal(15,2)").alias("amt_avg_debit"),
        F.col("amt_avg_credit").cast("decimal(15,2)").alias("amt_avg_credit"),
        F.col("amt_max_single_debit").cast("decimal(15,2)").alias("amt_max_single_debit"),
        F.col("amt_max_single_credit").cast("decimal(15,2)").alias("amt_max_single_credit"),
        F.col("distinct_merchants").cast("int").alias("distinct_merchants"),
        F.col("top_merchant_category").cast("string").alias("top_merchant_category"),
        F.col("pct_atm").cast("decimal(5,2)").alias("pct_atm"),
        F.col("pct_pos").cast("decimal(5,2)").alias("pct_pos"),
        F.col("pct_web").cast("decimal(5,2)").alias("pct_web"),
        F.col("pct_mobile").cast("decimal(5,2)").alias("pct_mobile"),
        F.col("days_since_last_txn").cast("int").alias("days_since_last_txn"),
        F.current_timestamp().alias("load_ts"),
    )


def transform(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    cfg: Config,
) -> DataFrame:
    """Pure transform: build the STG_TXN_SUMMARY DataFrame from source DataFrames."""
    posted = posted_window_txns(transactions, cfg)

    acct = accounts.select("account_id", "customer_id", "account_type")
    types = transaction_types.select("transaction_type_cd", "category")

    enriched = posted.join(acct, "account_id", "inner").join(
        F.broadcast(types), "transaction_type_cd", "inner"
    )

    summary = summarise(enriched, cfg)
    top_cat = top_merchant_category(posted)

    joined = summary.join(top_cat, "account_id", "left")
    return _enforce_schema(joined, cfg)


def run(spark: SparkSession, cfg: Config) -> DataFrame:
    """Read sources, transform, validate, and write ``etl_staging.stg_txn_summary``."""
    run_id = str(uuid.uuid4())
    init_audit(spark, cfg)
    log_step(
        spark, cfg, run_id, JOB_NAME, "FULL_LOAD", "STARTED",
        message="Beginning transaction-summary staging",
    )

    try:
        transactions = spark.table(cfg.table(cfg.schema_txn, "transactions"))
        accounts = spark.table(cfg.table(cfg.schema_core, "accounts"))
        transaction_types = spark.table(cfg.table(cfg.schema_txn, "transaction_types"))

        out = transform(transactions, accounts, transaction_types, cfg)
        validate_table(
            out,
            min_rows=1,
            not_null_cols=["customer_id", "account_id"],
            unique_keys=["customer_id", "account_id"],
        )

        target = cfg.table(cfg.schema_stg, TARGET_TABLE)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
        out.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)

        written = spark.table(target)
        row_count = written.count()
        log_step(
            spark, cfg, run_id, JOB_NAME, "FULL_LOAD", "SUCCESS",
            row_count=row_count, message=f"Wrote {target}",
        )
        return written
    except Exception as exc:
        log_step(spark, cfg, run_id, JOB_NAME, "FULL_LOAD", "FAILED", message=str(exc))
        raise


def main() -> None:
    cfg = get_config()
    spark = get_spark(JOB_NAME)
    run(spark, cfg)


if __name__ == "__main__":
    main()
