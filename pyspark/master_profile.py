"""PySpark translation of sas/04_sas_data_products.sas.

Assembles the CUSTOMER_MASTER_PROFILE golden record by joining the base
customer-360 attributes with the three upstream data products (segments,
transaction analytics, risk scores), defaulting any missing sections.
"""
from __future__ import annotations

import datetime as dt

from common import DEFAULT_RUN_DATE
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

MODEL_VERSION = "MASTER_V1.5"


def build_customer_master_profile(
    spark: SparkSession,
    stg_customer_360: DataFrame,
    customer_segments: DataFrame,
    transaction_analytics: DataFrame,
    customer_risk_scores: DataFrame,
    run_date: dt.date = DEFAULT_RUN_DATE,
) -> DataFrame:
    """Return the CUSTOMER_MASTER_PROFILE golden record."""
    # STEP 1: base customer attributes (active customers only)
    base = stg_customer_360.filter(F.col("customer_status") == "A").select(
        "customer_id",
        F.concat_ws(" ", F.trim("first_name"), F.trim("last_name")).alias("full_name"),
        "age",
        "state_code",
        "customer_since",
        "tenure_months",
        "customer_status",
        F.col("num_accounts").alias("total_accounts"),
        F.col("num_active_accounts").alias("active_accounts"),
        "total_balance",
        "total_credit_limit",
        "credit_utilization_pct",
    )

    segments = customer_segments.select(
        "customer_id",
        "segment_name",
        "lifetime_value_score",
        "engagement_score",
        "cross_sell_flag",
        "upsell_flag",
        "retention_risk_flag",
    )

    # Current-period analytics only (SAS: WHERE EFFECTIVE_DATE = today())
    txn = transaction_analytics.filter(F.col("effective_date") == F.lit(run_date)).select(
        "customer_id",
        F.col("total_transactions").alias("monthly_transactions"),
        F.col("total_debit_amt").alias("monthly_spend"),
        "net_cash_flow",
        "top_spend_category",
        "digital_txn_pct",
    )

    risk = customer_risk_scores.select(
        "customer_id",
        "composite_risk_score",
        "risk_tier",
        "probability_of_default",
        "watch_list_flag",
    )

    # STEP 2: left-join everything onto the base (SAS MERGE ... IF _base)
    merged = (
        base.join(segments, "customer_id", "left")
        .join(txn, "customer_id", "left")
        .join(risk, "customer_id", "left")
    )

    # Default any missing sections exactly as the SAS DATA step does
    return merged.select(
        "customer_id",
        "full_name",
        "age",
        "state_code",
        "customer_since",
        "tenure_months",
        "customer_status",
        F.coalesce("segment_name", F.lit("UNCLASSIFIED")).alias("segment_name"),
        F.coalesce("lifetime_value_score", F.lit(0.0)).alias("lifetime_value_score"),
        F.coalesce("engagement_score", F.lit(0.0)).alias("engagement_score"),
        "total_accounts",
        "active_accounts",
        "total_balance",
        "total_credit_limit",
        "credit_utilization_pct",
        F.coalesce("monthly_transactions", F.lit(0)).alias("monthly_transactions"),
        F.coalesce("monthly_spend", F.lit(0.0)).alias("monthly_spend"),
        F.coalesce("net_cash_flow", F.lit(0.0)).alias("net_cash_flow"),
        F.coalesce("top_spend_category", F.lit("")).alias("top_spend_category"),
        F.coalesce("digital_txn_pct", F.lit(0.0)).alias("digital_txn_pct"),
        "composite_risk_score",  # stays NULL when risk section missing
        F.coalesce("risk_tier", F.lit("UNKNOWN")).alias("risk_tier"),
        "probability_of_default",  # stays NULL when risk section missing
        F.coalesce("watch_list_flag", F.lit("N")).alias("watch_list_flag"),
        F.coalesce("cross_sell_flag", F.lit("N")).alias("cross_sell_flag"),
        F.coalesce("upsell_flag", F.lit("N")).alias("upsell_flag"),
        F.coalesce("retention_risk_flag", F.lit("N")).alias("retention_risk_flag"),
        F.lit(MODEL_VERSION).alias("model_version"),
        F.lit(run_date).alias("effective_date"),
        F.current_timestamp().alias("load_ts"),
    )
