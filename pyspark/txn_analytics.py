"""PySpark translation of sas/02_sas_txn_analytics.sas.

Rolls the account-level STG_TXN_SUMMARY up to customer level, derives spend
trend / revenue components, ranks customers into spend percentiles (PROC
RANK), and flags anomalies with the median + 3*IQR rule (PROC MEANS).
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

MODEL_VERSION = "TXN_V2.1"


def build_transaction_analytics(
    spark: SparkSession,
    stg_txn_summary: DataFrame,
    run_date: dt.date = DEFAULT_RUN_DATE,
) -> DataFrame:
    """Return the TRANSACTION_ANALYTICS data product."""
    reporting_period = run_date.strftime("%Y-%m")

    # STEP 2: aggregate account-level rows to customer level
    cust = stg_txn_summary.groupBy("customer_id").agg(
        F.countDistinct("account_id").alias("total_accounts"),
        # Accounts with a transaction in the last 30 days count as active
        F.sum(F.when(F.col("days_since_last_txn") <= 30, 1).otherwise(0)).alias(
            "active_accounts"
        ),
        F.sum("txn_count_total").alias("total_transactions"),
        F.sum("amt_total_debit").alias("total_debit_amt"),
        F.sum("amt_total_credit").alias("total_credit_amt"),
        (F.sum("amt_total_credit") - F.sum("amt_total_debit")).alias("net_cash_flow"),
        F.when(
            F.sum("txn_count_total") > 0,
            (F.sum("amt_total_debit") + F.sum("amt_total_credit"))
            / F.sum("txn_count_total"),
        )
        .otherwise(0.0)
        .alias("avg_transaction_size"),
        F.sum("amt_total_fees").alias("total_fees"),
        F.max("top_merchant_category").alias("top_spend_category"),
        # Weighted digital (WEB + MOBILE) share of transactions
        F.when(
            F.sum("txn_count_total") > 0,
            F.sum(F.col("txn_count_total") * (F.col("pct_web") + F.col("pct_mobile")) / 100)
            / F.sum("txn_count_total")
            * 100,
        )
        .otherwise(0.0)
        .alias("digital_txn_pct"),
    )

    # STEP 3: spend trend from net cash flow direction
    trend = (
        cust.withColumn(
            "monthly_spend_trend",
            F.when(F.col("net_cash_flow") > F.col("avg_transaction_size") * 5, "UP")
            .when(F.col("net_cash_flow") < -F.col("avg_transaction_size") * 5, "DOWN")
            .otherwise("STABLE"),
        )
        .withColumn("fee_income", F.bround(F.col("total_fees"), 2))
        # Simplified interest proxy: 2% of debit volume.
        # bround = banker's rounding (HALF_EVEN), matching the legacy output.
        # Scale-then-rint (HALF_EVEN on the scaled double) reproduces the
        # legacy engine's floating-point rounding on half-cent edge cases.
        .withColumn(
            "interest_income",
            F.bround(F.col("total_debit_amt") * 0.02 * 100, 0) / 100,
        )
        .withColumn(
            "revenue_contribution",
            F.bround(F.col("fee_income") + F.col("interest_income"), 2),
        )
    )

    # STEP 4: percentile ranking of spend (PROC RANK groups=100 equivalent)
    ranked = trend.withColumn(
        "spend_percentile",
        F.round(F.cume_dist().over(Window.orderBy("total_debit_amt")) * 100, 1),
    )

    # STEP 5: anomaly flag via median + 3*IQR (PROC MEANS median/qrange)
    stats = ranked.agg(
        F.expr("percentile_approx(total_debit_amt, 0.5, 100000)").alias("med"),
        (
            F.expr("percentile_approx(total_debit_amt, 0.75, 100000)")
            - F.expr("percentile_approx(total_debit_amt, 0.25, 100000)")
        ).alias("iqr"),
    ).collect()[0]

    return (
        ranked.withColumn(
            "anomaly_flag",
            F.when(
                (F.col("total_debit_amt") > stats["med"] + 3 * stats["iqr"])
                & (F.lit(stats["iqr"]) > 0),
                "Y",
            ).otherwise("N"),
        )
        .withColumn("reporting_period", F.lit(reporting_period))
        .withColumn("model_version", F.lit(MODEL_VERSION))
        .withColumn("effective_date", F.lit(run_date))
        .withColumn("load_ts", F.current_timestamp())
    )
