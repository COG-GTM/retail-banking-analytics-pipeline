"""Ticket 8 - Transaction analytics.

PySpark port of ``sas/02_sas_txn_analytics.sas``.

Inputs  : etl_staging.stg_txn_summary
Output  : data_products.transaction_analytics  (Delta, partitioned by reporting_period)

    * PROC RANK groups=100 -> percent_rank (spend percentile)
    * PROC MEANS median/qrange (IQR) -> approxQuantile
    * anomaly when total_debit_amt > median + 3*IQR (IQR > 0)
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MODEL_VERSION = "TXN_V2.1"

OUTPUT_COLUMNS = [
    "customer_id", "total_accounts", "active_accounts", "total_transactions",
    "total_debit_amt", "total_credit_amt", "net_cash_flow", "avg_transaction_size",
    "monthly_spend_trend", "spend_percentile", "top_spend_category", "digital_txn_pct",
    "fee_income", "interest_income", "revenue_contribution", "anomaly_flag",
    "model_version", "effective_date", "load_ts", "reporting_period",
]


def _aggregate_to_customer(stg_txn_summary: DataFrame) -> DataFrame:
    total_txn = F.sum("txn_count_total")
    return stg_txn_summary.groupBy("customer_id").agg(
        F.countDistinct("account_id").cast("smallint").alias("total_accounts"),
        F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0)).cast("smallint").alias(
            "active_accounts"
        ),
        total_txn.cast("int").alias("total_transactions"),
        F.sum("amt_total_debit").cast("decimal(18,2)").alias("total_debit_amt"),
        F.sum("amt_total_credit").cast("decimal(18,2)").alias("total_credit_amt"),
        (F.sum("amt_total_credit") - F.sum("amt_total_debit")).cast("decimal(18,2)").alias(
            "net_cash_flow"
        ),
        F.when(
            total_txn > 0,
            F.sum(F.col("amt_total_debit") + F.col("amt_total_credit")) / total_txn,
        ).otherwise(F.lit(0)).cast("decimal(15,2)").alias("avg_transaction_size"),
        F.sum("amt_total_fees").cast("decimal(18,2)").alias("total_fees"),
        F.max("top_merchant_category").alias("top_spend_category"),
        F.when(
            total_txn > 0,
            F.sum(F.col("txn_count_total") * (F.col("pct_web") + F.col("pct_mobile")) / 100)
            / total_txn * 100,
        ).otherwise(F.lit(0)).cast("decimal(5,2)").alias("digital_txn_pct"),
    )


def build_transaction_analytics(
    stg_txn_summary: DataFrame,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    cust = _aggregate_to_customer(stg_txn_summary)

    avg5 = F.col("avg_transaction_size") * 5
    trend = (
        F.when(F.col("net_cash_flow") > avg5, "UP")
        .when(F.col("net_cash_flow") < -avg5, "DOWN")
        .otherwise("STABLE")
    )
    fee_income = F.col("total_fees")
    interest_income = F.col("total_debit_amt") * 0.02
    revenue = fee_income + interest_income

    with_trend = cust.select(
        "*",
        trend.alias("monthly_spend_trend"),
        fee_income.cast("decimal(15,2)").alias("fee_income"),
        interest_income.cast("decimal(15,2)").alias("interest_income"),
        revenue.cast("decimal(15,2)").alias("revenue_contribution"),
    )

    spend_w = Window.orderBy(F.col("total_debit_amt").asc())
    ranked = with_trend.withColumn(
        "spend_percentile",
        (F.percent_rank().over(spend_w) * 100).cast("decimal(5,2)"),
    )

    q1, median, q3 = ranked.approxQuantile(
        "total_debit_amt", [0.25, 0.5, 0.75], 0.0
    )
    iqr = (q3 - q1) if (q1 is not None and q3 is not None) else 0.0
    threshold = (median + 3 * iqr) if median is not None else None

    if threshold is not None and iqr > 0:
        anomaly = F.when(F.col("total_debit_amt") > F.lit(threshold), "Y").otherwise("N")
    else:
        anomaly = F.lit("N")

    return ranked.select(
        F.col("customer_id"),
        F.col("total_accounts"),
        F.col("active_accounts"),
        F.col("total_transactions"),
        F.col("total_debit_amt"),
        F.col("total_credit_amt"),
        F.col("net_cash_flow"),
        F.col("avg_transaction_size"),
        F.col("monthly_spend_trend"),
        F.col("spend_percentile"),
        F.col("top_spend_category"),
        F.col("digital_txn_pct"),
        F.col("fee_income"),
        F.col("interest_income"),
        F.col("revenue_contribution"),
        anomaly.alias("anomaly_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(run_date).cast("date").alias("effective_date"),
        F.lit(load_ts).cast("timestamp").alias("load_ts"),
        F.date_format(F.lit(run_date), "yyyy-MM").alias("reporting_period"),
    )
