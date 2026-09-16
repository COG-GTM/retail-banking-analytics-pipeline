from __future__ import annotations

from datetime import date

import pyspark.sql.functions as F


def build_customer_master_profile(stg_customer_360, segments,
                                  txn_analytics, risk_scores, run_date: date):
    """Port of sas/04_sas_data_products.sas (phase3d): 4-way LEFT JOIN
    onto STG_CUSTOMER_360 (status='A') with COALESCE defaults."""
    b = stg_customer_360.filter(F.col("customer_status") == "A").alias("b")
    s = segments.alias("s")
    t = txn_analytics.alias("t")
    r = risk_scores.alias("r")

    cid = F.col("b.customer_id")
    return (b.join(s, cid == F.col("s.customer_id"), "left")
        .join(t, cid == F.col("t.customer_id"), "left")
        .join(r, cid == F.col("r.customer_id"), "left")
        .select(
            cid.alias("customer_id"),
            F.concat(F.col("b.first_name"), F.lit(" "),
                     F.col("b.last_name")).alias("full_name"),
            F.col("b.age"),
            F.col("b.state_code"),
            F.col("b.customer_since"),
            F.col("b.tenure_months"),
            F.col("b.customer_status"),
            F.coalesce(F.col("s.segment_name"), F.lit("UNCLASSIFIED"))
                .alias("segment_name"),
            F.coalesce(F.col("s.lifetime_value_score"), F.lit(0))
                .alias("lifetime_value_score"),
            F.coalesce(F.col("s.engagement_score"), F.lit(0))
                .alias("engagement_score"),
            F.col("b.num_accounts").alias("total_accounts"),
            F.col("b.num_active_accounts").alias("active_accounts"),
            F.col("b.total_balance"),
            F.col("b.total_credit_limit"),
            F.col("b.credit_utilization_pct"),
            F.coalesce(F.col("t.total_transactions"), F.lit(0))
                .alias("monthly_transactions"),
            F.coalesce(F.col("t.total_debit_amt"), F.lit(0))
                .alias("monthly_spend"),
            F.coalesce(F.col("t.net_cash_flow"), F.lit(0))
                .alias("net_cash_flow"),
            F.col("t.top_spend_category"),
            F.coalesce(F.col("t.digital_txn_pct"), F.lit(0))
                .alias("digital_txn_pct"),
            F.col("r.composite_risk_score"),
            F.coalesce(F.col("r.risk_tier"), F.lit("UNKNOWN"))
                .alias("risk_tier"),
            F.col("r.probability_of_default"),
            F.coalesce(F.col("r.watch_list_flag"), F.lit("N"))
                .alias("watch_list_flag"),
            F.coalesce(F.col("s.cross_sell_flag"), F.lit("N"))
                .alias("cross_sell_flag"),
            F.coalesce(F.col("s.upsell_flag"), F.lit("N"))
                .alias("upsell_flag"),
            F.coalesce(F.col("s.retention_risk_flag"), F.lit("N"))
                .alias("retention_risk_flag"),
            F.lit("MASTER_V1.5").alias("model_version"),
            F.lit(run_date).alias("effective_date"),
            F.current_timestamp().alias("load_ts"),
        ))
