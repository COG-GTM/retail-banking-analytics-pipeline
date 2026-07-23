"""Unit tests for the CUSTOMER_MASTER_PROFILE golden-record transform."""

from __future__ import annotations

import decimal

import pytest

from jobs.data_products import JOB_NAME, MODEL_VERSION, build_master_profile
from jobs import customer_segments, risk_scoring, stg_customer_360, txn_analytics
from jobs.data_products import run as data_products_run
from common.spark import read_delta


def _base_rows(spark):
    cols = [
        "customer_id", "first_name", "last_name", "age", "state_code",
        "customer_since", "tenure_months", "customer_status",
        "num_accounts", "num_active_accounts", "total_balance",
        "total_credit_limit", "credit_utilization_pct",
    ]
    data = [
        # Active + present in all dimensions
        (1, "Ada", "Lovelace", 40, "CA", "2020-01-01", 60, "A",
         3, 2, 15000.50, 5000.00, 22.50),
        # Active but missing from every dimension -> defaults
        (2, "Grace", "Hopper", 55, "NY", "2015-06-15", 120, "A",
         1, 1, 800.00, 0.00, 0.00),
        # Inactive -> filtered out entirely
        (3, "Alan", "Turing", 41, "TX", "2019-03-03", 40, "I",
         2, 0, 100.00, 0.00, 0.00),
    ]
    return spark.createDataFrame(data, cols)


def _segments(spark):
    cols = [
        "customer_id", "segment_name", "lifetime_value_score", "engagement_score",
        "cross_sell_flag", "upsell_flag", "retention_risk_flag",
        "effective_date", "load_ts",
    ]
    data = [
        (1, "HIGH_VALUE", 9000.00, 88.00, "Y", "N", "N",
         "2026-04-10", "2026-04-10 00:00:00"),
    ]
    return spark.createDataFrame(data, cols)


def _txn(spark):
    cols = [
        "customer_id", "total_transactions", "total_debit_amt", "net_cash_flow",
        "top_spend_category", "digital_txn_pct", "effective_date", "load_ts",
    ]
    data = [
        # Two periods for customer 1: only the latest (2026-04-10) must survive.
        (1, 50, 4000.00, -1000.00, "GROCERIES", 75.00,
         "2026-03-10", "2026-03-10 00:00:00"),
        (1, 80, 7000.00, -2000.00, "TRAVEL", 90.00,
         "2026-04-10", "2026-04-10 00:00:00"),
    ]
    return spark.createDataFrame(data, cols)


def _risk(spark):
    cols = [
        "customer_id", "composite_risk_score", "risk_tier",
        "probability_of_default", "watch_list_flag", "effective_date", "load_ts",
    ]
    data = [
        (1, 42.50, "MODERATE", 0.030000, "N", "2026-04-10", "2026-04-10 00:00:00"),
    ]
    return spark.createDataFrame(data, cols)


@pytest.fixture()
def profile_rows(spark):
    df = build_master_profile(
        _base_rows(spark), _segments(spark), _txn(spark), _risk(spark),
        run_date="2026-04-10",
    )
    return {r["customer_id"]: r for r in df.collect()}


def test_inactive_customers_filtered(profile_rows):
    assert set(profile_rows) == {1, 2}


def test_full_join_values(profile_rows):
    row = profile_rows[1]
    assert row["full_name"] == "Ada Lovelace"
    assert row["segment_name"] == "HIGH_VALUE"
    assert row["total_accounts"] == 3
    assert row["active_accounts"] == 2
    assert row["risk_tier"] == "MODERATE"
    assert row["watch_list_flag"] == "N"
    assert row["cross_sell_flag"] == "Y"


def test_latest_txn_period_only(profile_rows):
    row = profile_rows[1]
    # April period, not the older March one.
    assert row["monthly_transactions"] == 80
    assert row["top_spend_category"] == "TRAVEL"


def test_missing_dimension_defaults(profile_rows):
    row = profile_rows[2]
    assert row["segment_name"] == "UNCLASSIFIED"
    assert row["lifetime_value_score"] == decimal.Decimal("0.00")
    assert row["engagement_score"] == decimal.Decimal("0.00")
    assert row["monthly_transactions"] == 0
    assert row["monthly_spend"] == decimal.Decimal("0.00")
    assert row["top_spend_category"] == ""
    assert row["digital_txn_pct"] == decimal.Decimal("0.00")
    assert row["composite_risk_score"] is None
    assert row["risk_tier"] == "UNKNOWN"
    assert row["probability_of_default"] is None
    assert row["watch_list_flag"] == "N"
    assert row["cross_sell_flag"] == "N"


def test_metadata_columns(profile_rows):
    row = profile_rows[1]
    assert row["model_version"] == MODEL_VERSION
    assert str(row["effective_date"]) == "2026-04-10"
    assert row["load_ts"] is not None


def test_output_schema_contract(spark):
    df = build_master_profile(
        _base_rows(spark), _segments(spark), _txn(spark), _risk(spark),
        run_date="2026-04-10",
    )
    assert df.columns[0] == "customer_id"
    assert df.columns[-3:] == ["model_version", "effective_date", "load_ts"]
    assert len(df.columns) == 30


def test_run_persists_delta_from_seeds(spark, cfg):
    """End-to-end via the job's run(): seed upstream, then build the golden record."""
    stg_customer_360.run(spark, cfg)
    customer_segments.run(spark, cfg)
    txn_analytics.run(spark, cfg)
    risk_scoring.run(spark, cfg)

    result = data_products_run(spark, cfg)
    assert result.count() > 0
    assert result.filter(result.customer_id.isNull()).count() == 0

    # Idempotency: re-running overwrites, does not append.
    first = result.count()
    again = data_products_run(spark, cfg)
    assert again.count() == first

    reread = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile")
    assert reread.count() == first
