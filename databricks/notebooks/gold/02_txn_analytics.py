# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — `TRANSACTION_ANALYTICS`
# MAGIC
# MAGIC Port of `sas/02_sas_txn_analytics.sas`.
# MAGIC
# MAGIC | SAS step | PySpark equivalent |
# MAGIC |---|---|
# MAGIC | `PROC SQL` account -> customer roll-up | `aggregate_to_customer()` |
# MAGIC | `DATA` step trend / revenue placeholders | `add_trend_and_revenue()` |
# MAGIC | `PROC RANK groups=100` | `ntile(100).over(orderBy(TOTAL_DEBIT_AMT)) - 1` (PROC RANK emits 0-99, `ntile` emits 1-100) |
# MAGIC | `PROC MEANS median= qrange=` | `approxQuantile(["TOTAL_DEBIT_AMT"], [0.25, 0.5, 0.75], 0.0)` |
# MAGIC | `DELETE ... WHERE REPORTING_PERIOD = '<period>'` + `PROC APPEND` | Delta `replaceWhere` on `REPORTING_PERIOD` (period-scoped overwrite) |

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
from shared.schemas import TRANSACTION_ANALYTICS, conform

JOB_NAME = "02_txn_analytics"
TARGET_TABLE = "TRANSACTION_ANALYTICS"
MODEL_VERSION = "TXN_V2.1"

# COMMAND ----------


def aggregate_to_customer(txn_summary: DataFrame) -> DataFrame:
    """SAS STEP 2 — account-level staging rolled up to one row per customer."""
    total_txns = F.sum("TXN_COUNT_TOTAL")
    digital_txns = F.sum(
        F.col("TXN_COUNT_TOTAL")
        * (F.coalesce(F.col("PCT_WEB"), F.lit(0)) + F.coalesce(F.col("PCT_MOBILE"), F.lit(0)))
        / 100
    )

    return txn_summary.groupBy("CUSTOMER_ID").agg(
        F.countDistinct("ACCOUNT_ID").alias("TOTAL_ACCOUNTS"),
        F.sum(F.when(F.col("DAYS_SINCE_LAST_TXN") <= 30, 1).otherwise(0)).alias("ACTIVE_ACCOUNTS"),
        total_txns.alias("TOTAL_TRANSACTIONS"),
        F.sum("AMT_TOTAL_DEBIT").alias("TOTAL_DEBIT_AMT"),
        F.sum("AMT_TOTAL_CREDIT").alias("TOTAL_CREDIT_AMT"),
        (F.sum("AMT_TOTAL_CREDIT") - F.sum("AMT_TOTAL_DEBIT")).alias("NET_CASH_FLOW"),
        F.when(
            total_txns > 0,
            F.sum(F.col("AMT_TOTAL_DEBIT") + F.col("AMT_TOTAL_CREDIT")) / total_txns,
        )
        .otherwise(F.lit(0))
        .alias("AVG_TRANSACTION_SIZE"),
        F.sum("AMT_TOTAL_FEES").alias("TOTAL_FEES"),
        F.max("TOP_MERCHANT_CATEGORY").alias("TOP_SPEND_CATEGORY"),
        F.when(total_txns > 0, digital_txns / total_txns * 100)
        .otherwise(F.lit(0))
        .alias("DIGITAL_TXN_PCT"),
    )


def add_trend_and_revenue(cust_txn: DataFrame) -> DataFrame:
    """SAS STEP 3 — spend trend plus the fee / interest revenue proxies."""
    trend = (
        F.when(F.col("NET_CASH_FLOW") > F.col("AVG_TRANSACTION_SIZE") * 5, "UP")
        .when(F.col("NET_CASH_FLOW") < -F.col("AVG_TRANSACTION_SIZE") * 5, "DOWN")
        .otherwise("STABLE")
    )
    return (
        cust_txn.withColumn("MONTHLY_SPEND_TREND", trend)
        .withColumn("FEE_INCOME", F.col("TOTAL_FEES"))
        .withColumn("INTEREST_INCOME", F.col("TOTAL_DEBIT_AMT") * 0.02)
        .withColumn("REVENUE_CONTRIBUTION", F.col("FEE_INCOME") + F.col("TOTAL_DEBIT_AMT") * 0.02)
    )


def add_percentile_and_anomaly(cust_txn: DataFrame) -> DataFrame:
    """SAS STEPS 4-5 — spend percentile and the median + 3*IQR anomaly rule."""
    ranked = cust_txn.withColumn(
        "SPEND_PERCENTILE",
        F.ntile(100).over(Window.orderBy(F.col("TOTAL_DEBIT_AMT").asc())) - 1,
    )

    q1, median, q3 = ranked.approxQuantile("TOTAL_DEBIT_AMT", [0.25, 0.5, 0.75], 0.0)
    iqr = q3 - q1

    if iqr <= 0:
        return ranked.withColumn("ANOMALY_FLAG", F.lit("N"))

    threshold = median + 3 * iqr
    return ranked.withColumn(
        "ANOMALY_FLAG",
        F.when(F.col("TOTAL_DEBIT_AMT") > F.lit(threshold), "Y").otherwise("N"),
    )


def build_txn_analytics(txn_summary: DataFrame, reporting_period: str) -> DataFrame:
    enriched = add_percentile_and_anomaly(add_trend_and_revenue(aggregate_to_customer(txn_summary)))
    result = enriched.select(
        F.col("CUSTOMER_ID"),
        F.lit(reporting_period).alias("REPORTING_PERIOD"),
        F.col("TOTAL_ACCOUNTS"),
        F.col("ACTIVE_ACCOUNTS"),
        F.col("TOTAL_TRANSACTIONS"),
        F.col("TOTAL_DEBIT_AMT"),
        F.col("TOTAL_CREDIT_AMT"),
        F.col("NET_CASH_FLOW"),
        F.col("AVG_TRANSACTION_SIZE"),
        F.col("MONTHLY_SPEND_TREND"),
        F.col("SPEND_PERCENTILE"),
        F.col("TOP_SPEND_CATEGORY"),
        F.col("DIGITAL_TXN_PCT"),
        F.col("FEE_INCOME"),
        F.col("INTEREST_INCOME"),
        F.col("REVENUE_CONTRIBUTION"),
        F.col("ANOMALY_FLAG"),
        F.lit(MODEL_VERSION).alias("MODEL_VERSION"),
        F.current_date().alias("EFFECTIVE_DATE"),
        F.current_timestamp().alias("LOAD_TS"),
    )
    return conform(result, TRANSACTION_ANALYTICS)


# COMMAND ----------

if in_databricks() and not exit_if_skipped("skip_gold", JOB_NAME):
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))
    target = cfg.gold(TARGET_TABLE)
    period = spark.sql("SELECT date_format(current_date(), 'yyyy-MM')").collect()[0][0]

    with audit.step("TXN_ANALYTICS", f"period={period} -> {target}") as ctx:
        df = build_txn_analytics(spark.table(cfg.silver("STG_TXN_SUMMARY")), period)

        writer = df.write.format("delta")
        if spark.catalog.tableExists(target):
            # Mirrors DELETE FROM ... WHERE REPORTING_PERIOD = '<period>' + APPEND
            writer.mode("overwrite").option(
                "replaceWhere", f"REPORTING_PERIOD = '{period}'"
            ).saveAsTable(target)
        else:
            writer.mode("overwrite").option("overwriteSchema", "true").partitionBy(
                "REPORTING_PERIOD"
            ).saveAsTable(target)

        ctx.row_count = validate_dataframe(
            spark.table(target).where(F.col("REPORTING_PERIOD") == period),
            target,
            key_cols=["CUSTOMER_ID"],
            not_null=["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"],
            min_rows=get_int_param("min_gold_rows", "1000"),
        )
