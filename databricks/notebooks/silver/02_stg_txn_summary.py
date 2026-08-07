# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — `STG_TXN_SUMMARY`
# MAGIC
# MAGIC Port of `bteq/02_stg_txn_summary.bteq`.
# MAGIC
# MAGIC | BTEQ construct | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `CREATE VOLATILE TABLE VT_RUN_PARAMS` + `CROSS JOIN rp` | `lookback_months` job parameter (default 12) materialised as two literal columns |
# MAGIC | `ADD_MONTHS(CURRENT_DATE, -${LOOKBACK_MONTHS})` | `add_months(current_date(), -lookback_months)` |
# MAGIC | `NULLIFZERO(COUNT(*))` | `nullif(count(*), 0)` |
# MAGIC | `QUALIFY ROW_NUMBER() OVER (... ORDER BY SUM(...) OVER (...) DESC) = 1` | spend aggregated per (account, category) then `row_number()` over the aggregate |
# MAGIC | `CURRENT_DATE - MAX(TRANSACTION_DATE)` | `datediff(current_date(), max(TRANSACTION_DATE))` |
# MAGIC
# MAGIC The top-category tie-break is `MERCHANT_CATEGORY` ascending; Teradata left
# MAGIC ties non-deterministic, so this only makes reruns reproducible.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import validate_dataframe
from shared.runtime import (
    PipelineConfig,
    exit_if_skipped,
    get_int_param,
    get_param,
    in_databricks,
)
from shared.schemas import STG_TXN_SUMMARY, conform

JOB_NAME = "02_stg_txn_summary"
TARGET_TABLE = "STG_TXN_SUMMARY"

# COMMAND ----------


def top_merchant_category(transactions: DataFrame, period_start, period_end) -> DataFrame:
    """Highest-spend merchant category per account within the lookback window."""
    window = Window.partitionBy("ACCOUNT_ID").orderBy(
        F.col("_SPEND").desc(), F.col("MERCHANT_CATEGORY").asc()
    )
    return (
        transactions.where(
            (F.col("STATUS_CODE") == "P")
            & F.col("MERCHANT_CATEGORY").isNotNull()
            & F.col("TRANSACTION_DATE").between(period_start, period_end)
        )
        .groupBy("ACCOUNT_ID", "MERCHANT_CATEGORY")
        .agg(F.sum(F.abs(F.col("AMOUNT"))).alias("_SPEND"))
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .select("ACCOUNT_ID", "MERCHANT_CATEGORY")
    )


def build_stg_txn_summary(
    transactions: DataFrame,
    accounts: DataFrame,
    transaction_types: DataFrame,
    lookback_months: int = 12,
) -> DataFrame:
    period_start = F.add_months(F.current_date(), -lookback_months)
    period_end = F.current_date()

    t = transactions.alias("t")
    acct = accounts.alias("acct")
    tt = transaction_types.alias("tt")
    top_cat = top_merchant_category(transactions, period_start, period_end).alias("top_cat")

    joined = (
        t.join(acct, F.col("t.ACCOUNT_ID") == F.col("acct.ACCOUNT_ID"), "inner")
        .join(tt, F.col("t.TRANSACTION_TYPE_CD") == F.col("tt.TRANSACTION_TYPE_CD"), "inner")
        .join(top_cat, F.col("t.ACCOUNT_ID") == F.col("top_cat.ACCOUNT_ID"), "left")
        .where(
            F.col("t.TRANSACTION_DATE").between(period_start, period_end)
            & (F.col("t.STATUS_CODE") == "P")
        )
    )

    def category_amount(category: str, signed: bool) -> F.Column:
        amount = F.col("t.AMOUNT") if signed else F.abs(F.col("t.AMOUNT"))
        return F.when(F.col("tt.CATEGORY") == category, amount)

    def channel_pct(*codes: str) -> F.Column:
        hits = F.sum(F.when(F.col("t.CHANNEL_CODE").isin(*codes), 1).otherwise(0))
        return (hits * F.lit(100.0) / F.nullif(F.count(F.lit(1)), F.lit(0))).cast("decimal(5,2)")

    aggregated = joined.groupBy(
        F.col("acct.CUSTOMER_ID").alias("CUSTOMER_ID"),
        F.col("acct.ACCOUNT_ID").alias("ACCOUNT_ID"),
        F.col("acct.ACCOUNT_TYPE").alias("ACCOUNT_TYPE"),
        F.col("top_cat.MERCHANT_CATEGORY").alias("_TOP_CATEGORY"),
    ).agg(
        F.count(F.lit(1)).alias("TXN_COUNT_TOTAL"),
        F.sum(F.when(F.col("tt.CATEGORY") == "DEBIT", 1).otherwise(0)).alias("TXN_COUNT_DEBIT"),
        F.sum(F.when(F.col("tt.CATEGORY") == "CREDIT", 1).otherwise(0)).alias("TXN_COUNT_CREDIT"),
        F.sum(F.when(F.col("tt.CATEGORY") == "FEE", 1).otherwise(0)).alias("TXN_COUNT_FEE"),
        F.sum(category_amount("DEBIT", signed=False).otherwise(F.lit(0))).alias("AMT_TOTAL_DEBIT"),
        F.sum(category_amount("CREDIT", signed=True).otherwise(F.lit(0))).alias("AMT_TOTAL_CREDIT"),
        F.sum(category_amount("FEE", signed=False).otherwise(F.lit(0))).alias("AMT_TOTAL_FEES"),
        F.avg(category_amount("DEBIT", signed=False)).alias("AMT_AVG_DEBIT"),
        F.avg(category_amount("CREDIT", signed=True)).alias("AMT_AVG_CREDIT"),
        F.max(category_amount("DEBIT", signed=False).otherwise(F.lit(0))).alias(
            "AMT_MAX_SINGLE_DEBIT"
        ),
        F.max(category_amount("CREDIT", signed=True).otherwise(F.lit(0))).alias(
            "AMT_MAX_SINGLE_CREDIT"
        ),
        F.countDistinct(F.col("t.MERCHANT_NAME")).alias("DISTINCT_MERCHANTS"),
        F.max(F.col("top_cat.MERCHANT_CATEGORY")).alias("TOP_MERCHANT_CATEGORY"),
        channel_pct("ATM").alias("PCT_ATM"),
        channel_pct("POS").alias("PCT_POS"),
        channel_pct("WEB").alias("PCT_WEB"),
        channel_pct("MOB").alias("PCT_MOBILE"),
        F.datediff(F.current_date(), F.max(F.col("t.TRANSACTION_DATE"))).alias(
            "DAYS_SINCE_LAST_TXN"
        ),
    )

    result = aggregated.select(
        "CUSTOMER_ID",
        "ACCOUNT_ID",
        "ACCOUNT_TYPE",
        period_start.alias("SUMMARY_PERIOD_START"),
        period_end.alias("SUMMARY_PERIOD_END"),
        "TXN_COUNT_TOTAL",
        "TXN_COUNT_DEBIT",
        "TXN_COUNT_CREDIT",
        "TXN_COUNT_FEE",
        "AMT_TOTAL_DEBIT",
        "AMT_TOTAL_CREDIT",
        "AMT_TOTAL_FEES",
        "AMT_AVG_DEBIT",
        "AMT_AVG_CREDIT",
        "AMT_MAX_SINGLE_DEBIT",
        "AMT_MAX_SINGLE_CREDIT",
        "DISTINCT_MERCHANTS",
        "TOP_MERCHANT_CATEGORY",
        "PCT_ATM",
        "PCT_POS",
        "PCT_WEB",
        "PCT_MOBILE",
        "DAYS_SINCE_LAST_TXN",
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, STG_TXN_SUMMARY)


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_silver", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    lookback = get_int_param("lookback_months")
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.silver(TARGET_TABLE)

    with audit.step("FULL_LOAD", f"lookback_months={lookback} -> {target}") as ctx:
        df = build_stg_txn_summary(
            spark.table(cfg.bronze("TRANSACTIONS")),
            spark.table(cfg.bronze("ACCOUNTS")),
            spark.table(cfg.bronze("TRANSACTION_TYPES")),
            lookback_months=lookback,
        )
        df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target)
        spark.sql(f"OPTIMIZE {target} ZORDER BY (CUSTOMER_ID)")
        ctx.row_count = validate_dataframe(
            spark.table(target),
            target,
            key_cols=["CUSTOMER_ID", "ACCOUNT_ID"],
            not_null=["CUSTOMER_ID", "ACCOUNT_ID"],
            min_rows=1,
        )
