"""Ticket 10 - Customer master profile (golden record).

PySpark port of ``sas/04_sas_data_products.sas``.

Inputs  : etl_staging.stg_customer_360, data_products.customer_segments,
          data_products.transaction_analytics, data_products.customer_risk_scores
Output  : data_products.customer_master_profile  (Delta)

The SAS 4-way DATA-step MERGE (with _base/_seg/_txn/_risk indicators and default
handling for missing upstream products) becomes left joins with coalesced defaults.
"""
from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

MODEL_VERSION = "MASTER_V1.5"

OUTPUT_COLUMNS = [
    "customer_id", "full_name", "age", "state_code", "customer_since", "tenure_months",
    "customer_status", "segment_name", "lifetime_value_score", "engagement_score",
    "total_accounts", "active_accounts", "total_balance", "total_credit_limit",
    "credit_utilization_pct", "monthly_transactions", "monthly_spend", "net_cash_flow",
    "top_spend_category", "digital_txn_pct", "composite_risk_score", "risk_tier",
    "probability_of_default", "watch_list_flag", "cross_sell_flag", "upsell_flag",
    "retention_risk_flag", "model_version", "effective_date", "load_ts",
]


def build_master_profile(
    stg_customer_360: DataFrame,
    customer_segments: DataFrame,
    transaction_analytics: DataFrame,
    customer_risk_scores: DataFrame,
    run_date: date,
    load_ts: datetime,
) -> DataFrame:
    base = stg_customer_360.where(F.col("customer_status") == "A").select(
        F.col("customer_id"),
        F.concat(F.trim(F.col("first_name")), F.lit(" "), F.trim(F.col("last_name"))).alias(
            "full_name"
        ),
        F.col("age"),
        F.col("state_code"),
        F.col("customer_since"),
        F.col("tenure_months"),
        F.col("customer_status"),
        F.col("num_accounts").alias("total_accounts"),
        F.col("num_active_accounts").alias("active_accounts"),
        F.col("total_balance"),
        F.col("total_credit_limit"),
        F.col("credit_utilization_pct"),
    )

    segments = customer_segments.select(
        "customer_id", "segment_name", "lifetime_value_score", "engagement_score",
        "cross_sell_flag", "upsell_flag", "retention_risk_flag",
    )
    txn = transaction_analytics.where(F.col("effective_date") == F.lit(run_date)).select(
        F.col("customer_id"),
        F.col("total_transactions").alias("monthly_transactions"),
        F.col("total_debit_amt").alias("monthly_spend"),
        F.col("net_cash_flow"),
        F.col("top_spend_category"),
        F.col("digital_txn_pct"),
    )
    risk = customer_risk_scores.select(
        "customer_id", "composite_risk_score", "risk_tier",
        "probability_of_default", "watch_list_flag",
    )

    return (
        base.join(segments, "customer_id", "left")
        .join(txn, "customer_id", "left")
        .join(risk, "customer_id", "left")
        .select(
            F.col("customer_id"),
            F.col("full_name"),
            F.col("age"),
            F.col("state_code"),
            F.col("customer_since"),
            F.col("tenure_months"),
            F.col("customer_status"),
            F.coalesce(F.col("segment_name"), F.lit("UNCLASSIFIED")).alias("segment_name"),
            F.coalesce(F.col("lifetime_value_score"), F.lit(0)).cast("decimal(10,2)").alias(
                "lifetime_value_score"
            ),
            F.coalesce(F.col("engagement_score"), F.lit(0)).cast("decimal(5,2)").alias(
                "engagement_score"
            ),
            F.col("total_accounts"),
            F.col("active_accounts"),
            F.col("total_balance"),
            F.col("total_credit_limit"),
            F.col("credit_utilization_pct"),
            F.coalesce(F.col("monthly_transactions"), F.lit(0)).cast("int").alias(
                "monthly_transactions"
            ),
            F.coalesce(F.col("monthly_spend"), F.lit(0)).cast("decimal(18,2)").alias("monthly_spend"),
            F.coalesce(F.col("net_cash_flow"), F.lit(0)).cast("decimal(18,2)").alias("net_cash_flow"),
            F.coalesce(F.col("top_spend_category"), F.lit("")).alias("top_spend_category"),
            F.coalesce(F.col("digital_txn_pct"), F.lit(0)).cast("decimal(5,2)").alias(
                "digital_txn_pct"
            ),
            F.col("composite_risk_score").cast("decimal(6,2)").alias("composite_risk_score"),
            F.coalesce(F.col("risk_tier"), F.lit("UNKNOWN")).alias("risk_tier"),
            F.col("probability_of_default").cast("decimal(7,6)").alias("probability_of_default"),
            F.coalesce(F.col("watch_list_flag"), F.lit("N")).alias("watch_list_flag"),
            F.coalesce(F.col("cross_sell_flag"), F.lit("N")).alias("cross_sell_flag"),
            F.coalesce(F.col("upsell_flag"), F.lit("N")).alias("upsell_flag"),
            F.coalesce(F.col("retention_risk_flag"), F.lit("N")).alias("retention_risk_flag"),
            F.lit(MODEL_VERSION).alias("model_version"),
            F.lit(run_date).cast("date").alias("effective_date"),
            F.lit(load_ts).cast("timestamp").alias("load_ts"),
        )
    )
