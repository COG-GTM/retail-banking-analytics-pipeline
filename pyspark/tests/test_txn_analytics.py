"""Unit tests for jobs.txn_analytics (port of sas/02_sas_txn_analytics.sas)."""

from __future__ import annotations

import pytest

from jobs.txn_analytics import (
    OUTPUT_COLUMNS,
    _reporting_period,
    build_analytics,
    run,
)


def _row(**overrides) -> dict:
    """Build a STG_TXN_SUMMARY row with sensible numeric defaults."""
    base = {
        "customer_id": 1,
        "account_id": 1,
        "account_type": "CHECKING",
        "summary_period_start": None,
        "summary_period_end": None,
        "txn_count_total": 0,
        "txn_count_debit": 0.0,
        "txn_count_credit": 0.0,
        "txn_count_fee": 0.0,
        "amt_total_debit": 0.0,
        "amt_total_credit": 0.0,
        "amt_total_fees": 0.0,
        "amt_avg_debit": 0.0,
        "amt_avg_credit": 0.0,
        "amt_max_single_debit": 0.0,
        "amt_max_single_credit": 0.0,
        "distinct_merchants": 0,
        "top_merchant_category": "OTHER",
        "pct_atm": 0.0,
        "pct_pos": 0.0,
        "pct_web": 0.0,
        "pct_mobile": 0.0,
        "days_since_last_txn": 0,
        "load_ts": "2026-01-01 00:00:00",
    }
    base.update(overrides)
    return base


def test_run_produces_expected_contract(spark, cfg, seed_stg_txn_summary):
    """End-to-end run on the seed CSV honours the output contract."""
    stg = seed_stg_txn_summary()
    result = run(spark, cfg).cache()

    assert result.columns == OUTPUT_COLUMNS

    expected_customers = stg.select("customer_id").distinct().count()
    assert result.count() == expected_customers

    # One row per customer (unique key) and partition column populated.
    assert result.select("customer_id").distinct().count() == result.count()
    period = _reporting_period(cfg)
    assert result.select("reporting_period").distinct().collect() == [
        (period,)
    ]

    # spend_percentile is a genuine percent_rank in [0, 1].
    bounds = result.selectExpr(
        "min(spend_percentile) lo", "max(spend_percentile) hi"
    ).first()
    assert bounds["lo"] >= 0.0
    assert bounds["hi"] <= 1.0

    flags = {r["anomaly_flag"] for r in result.select("anomaly_flag").collect()}
    assert flags <= {"Y", "N"}
    trends = {
        r["monthly_spend_trend"]
        for r in result.select("monthly_spend_trend").collect()
    }
    assert trends <= {"UP", "DOWN", "STABLE"}
    result.unpersist()


def test_customer_aggregation(spark, cfg, seed_stg_txn_summary):
    """Account-level rows roll up to the expected customer-level metrics."""
    seed_stg_txn_summary(
        [
            _row(
                customer_id=100, account_id=1, txn_count_total=10,
                amt_total_debit=1000.0, amt_total_credit=500.0,
                amt_total_fees=50.0, pct_web=20.0, pct_mobile=30.0,
                days_since_last_txn=5, top_merchant_category="TRAVEL",
            ),
            _row(
                customer_id=100, account_id=2, txn_count_total=20,
                amt_total_debit=2000.0, amt_total_credit=3000.0,
                amt_total_fees=0.0, pct_web=10.0, pct_mobile=10.0,
                days_since_last_txn=40, top_merchant_category="GROCERY",
            ),
        ]
    )
    out = {r["customer_id"]: r for r in run(spark, cfg).collect()}[100]

    assert out["total_accounts"] == 2
    assert out["active_accounts"] == 1
    assert out["total_transactions"] == 30
    assert out["total_debit_amt"] == pytest.approx(3000.0)
    assert out["total_credit_amt"] == pytest.approx(3500.0)
    assert out["net_cash_flow"] == pytest.approx(500.0)
    assert out["avg_transaction_size"] == pytest.approx(6500.0 / 30)
    assert out["top_spend_category"] == "TRAVEL"
    assert out["digital_txn_pct"] == pytest.approx(30.0)
    assert out["monthly_spend_trend"] == "STABLE"
    assert out["fee_income"] == pytest.approx(50.0)
    assert out["interest_income"] == pytest.approx(60.0)
    assert out["revenue_contribution"] == pytest.approx(110.0)


def test_spend_trend_direction(spark, cfg, seed_stg_txn_summary):
    """Net cash flow beyond +/-5x avg txn size drives UP / DOWN trends."""
    seed_stg_txn_summary(
        [
            # avg txn size = 100; net = +9000 -> UP
            _row(
                customer_id=1, account_id=1, txn_count_total=100,
                amt_total_debit=500.0, amt_total_credit=9500.0,
            ),
            # avg txn size = 100; net = -9000 -> DOWN
            _row(
                customer_id=2, account_id=1, txn_count_total=100,
                amt_total_debit=9500.0, amt_total_credit=500.0,
            ),
        ]
    )
    trends = {r["customer_id"]: r["monthly_spend_trend"] for r in run(spark, cfg).collect()}
    assert trends[1] == "UP"
    assert trends[2] == "DOWN"


def test_anomaly_flag_iqr(spark, cfg, seed_stg_txn_summary):
    """A clear IQR outlier is flagged 'Y'; in-range customers stay 'N'."""
    normal = [
        _row(customer_id=i, account_id=1, txn_count_total=1, amt_total_debit=amt)
        for i, amt in enumerate(
            [100.0, 110.0, 120.0, 130.0, 140.0, 150.0, 160.0, 170.0, 180.0],
            start=1,
        )
    ]
    outlier = _row(
        customer_id=99, account_id=1, txn_count_total=1, amt_total_debit=100000.0
    )
    seed_stg_txn_summary(normal + [outlier])

    flags = {r["customer_id"]: r["anomaly_flag"] for r in run(spark, cfg).collect()}
    assert flags[99] == "Y"
    assert sum(1 for v in flags.values() if v == "Y") == 1


def test_no_anomaly_when_iqr_zero(spark, cfg, seed_stg_txn_summary):
    """When IQR is 0 (uniform spend) nothing is flagged."""
    seed_stg_txn_summary(
        [
            _row(customer_id=i, account_id=1, txn_count_total=1, amt_total_debit=1000.0)
            for i in range(1, 6)
        ]
    )
    flags = [r["anomaly_flag"] for r in run(spark, cfg).collect()]
    assert set(flags) == {"N"}


def test_spend_percentile_extremes(spark, cfg, seed_stg_txn_summary):
    """percent_rank spans [0, 1] with the lowest spender at 0 and highest at 1."""
    seed_stg_txn_summary(
        [
            _row(customer_id=i, account_id=1, txn_count_total=1, amt_total_debit=float(i * 100))
            for i in range(1, 6)
        ]
    )
    pct = {r["customer_id"]: r["spend_percentile"] for r in run(spark, cfg).collect()}
    assert pct[1] == pytest.approx(0.0)
    assert pct[5] == pytest.approx(1.0)
    assert all(0.0 <= v <= 1.0 for v in pct.values())


def test_idempotent_rerun(spark, cfg, seed_stg_txn_summary):
    """Re-running the same period overwrites in place (no duplicate rows)."""
    seed_stg_txn_summary()
    first = run(spark, cfg).count()
    second = run(spark, cfg).count()
    assert first == second

    table = cfg.table(cfg.schema_dp, "transaction_analytics")
    total = spark.table(table).count()
    distinct = spark.table(table).select(
        "customer_id", "reporting_period"
    ).distinct().count()
    assert total == distinct


def test_partitioned_by_reporting_period(spark, cfg, seed_stg_txn_summary):
    """The written Delta table is partitioned by reporting_period."""
    seed_stg_txn_summary()
    run(spark, cfg)
    table = cfg.table(cfg.schema_dp, "transaction_analytics")
    detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
    assert "reporting_period" in detail["partitionColumns"]


def test_build_analytics_pure_transform(spark, cfg, seed_stg_txn_summary):
    """build_analytics is a pure transform independent of table I/O."""
    stg = seed_stg_txn_summary()
    out = build_analytics(stg, cfg)
    assert out.columns == OUTPUT_COLUMNS
    assert out.count() == stg.select("customer_id").distinct().count()
