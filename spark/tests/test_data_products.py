"""Tests for the golden-record assembly job (04)."""
from __future__ import annotations

import math

from spark.jobs.data_products import OUTPUT_COLUMNS, build_master_profile


def _base(spark):
    return spark.createDataFrame(
        [
            (1, "Ada Lovelace", 30, "CA", "2015-01-01", 100, "A", 3, 2, 5000.0, 10000.0, 20.0),
            (2, "Alan Turing", 45, "NY", "2010-01-01", 160, "A", 2, 1, 1000.0, 5000.0, 10.0),
        ],
        ["customer_id", "full_name", "age", "state_code", "customer_since",
         "tenure_months", "customer_status", "total_accounts", "active_accounts",
         "total_balance", "total_credit_limit", "credit_utilization_pct"],
    )


def _segments(spark):
    # only customer 1 has a segment
    return spark.createDataFrame(
        [(1, "PREMIUM_WEALTH", 999.0, 88.0, "Y", "N", "N")],
        ["customer_id", "segment_name", "lifetime_value_score", "engagement_score",
         "cross_sell_flag", "upsell_flag", "retention_risk_flag"],
    )


def _txn(spark):
    # only customer 1 has txn activity
    return spark.createDataFrame(
        [(1, 50, 1500.0, -200.0, "RETAIL", 40.0)],
        ["customer_id", "monthly_transactions", "monthly_spend", "net_cash_flow",
         "top_spend_category", "digital_txn_pct"],
    )


def _risk(spark):
    # only customer 1 has a risk score
    return spark.createDataFrame(
        [(1, 42.5, "ELEVATED", 0.3, "N")],
        ["customer_id", "composite_risk_score", "risk_tier",
         "probability_of_default", "watch_list_flag"],
    )


def test_master_profile_merge_and_defaults(spark, config):
    result = build_master_profile(
        _base(spark), _segments(spark), _txn(spark), _risk(spark), config
    )
    assert result.columns == OUTPUT_COLUMNS
    assert result.count() == 2

    rows = {r["customer_id"]: r for r in result.collect()}

    # customer 1: fully populated
    c1 = rows[1]
    assert c1["segment_name"] == "PREMIUM_WEALTH"
    assert math.isclose(c1["monthly_spend"], 1500.0)
    assert c1["risk_tier"] == "ELEVATED"

    # customer 2: missing segment/txn/risk -> SAS defaults applied
    c2 = rows[2]
    assert c2["segment_name"] == "UNCLASSIFIED"
    assert c2["lifetime_value_score"] == 0.0
    assert c2["monthly_transactions"] == 0
    assert c2["top_spend_category"] == ""
    assert c2["risk_tier"] == "UNKNOWN"
    assert c2["composite_risk_score"] is None
    assert c2["watch_list_flag"] == "N"
    assert c2["model_version"] == config.master_model_version
