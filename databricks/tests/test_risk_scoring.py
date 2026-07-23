"""Ticket 9 - Risk scoring business logic (composite score, tiers, drivers)."""
from __future__ import annotations

from datetime import date, datetime

import pytest

from jobs.risk_scoring import build_customer_risk_scores, prepare_features
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_RF_SCHEMA = (
    "customer_id long, external_credit_score int, avg_daily_balance_30d decimal(15,2), "
    "avg_daily_balance_90d decimal(15,2), debit_velocity_7d decimal(15,2), "
    "debit_velocity_30d decimal(15,2), credit_util_ratio decimal(5,2), "
    "payment_ontime_pct decimal(5,2), balance_volatility decimal(10,4), "
    "account_overdraft_cnt int, large_withdrawal_cnt int, high_risk_merchant_cnt int, "
    "payment_late_cnt int"
)
_C360_SCHEMA = (
    "customer_id long, tenure_months int, num_active_accounts smallint, "
    "total_balance decimal(18,2), customer_status string"
)

_RF_DEFAULTS = dict(
    external_credit_score=700, avg_daily_balance_30d=1000, avg_daily_balance_90d=1000,
    debit_velocity_7d=100, debit_velocity_30d=400, credit_util_ratio=10,
    payment_ontime_pct=100, balance_volatility=0, account_overdraft_cnt=0,
    large_withdrawal_cnt=0, high_risk_merchant_cnt=0, payment_late_cnt=0,
)

_ORDER = [
    "customer_id", "external_credit_score", "avg_daily_balance_30d", "avg_daily_balance_90d",
    "debit_velocity_7d", "debit_velocity_30d", "credit_util_ratio", "payment_ontime_pct",
    "balance_volatility", "account_overdraft_cnt", "large_withdrawal_cnt",
    "high_risk_merchant_cnt", "payment_late_cnt",
]


def _risk_factors(spark, *customers):
    rows = []
    for cid, overrides in customers:
        d = dict(_RF_DEFAULTS)
        d.update(overrides)
        d["customer_id"] = cid
        rows.append(tuple(d[k] for k in _ORDER))
    return make_df(spark, _RF_SCHEMA, rows)


def _c360(spark, *ids):
    return make_df(spark, _C360_SCHEMA, [(cid, 60, 2, 5000, "A") for cid in ids])


def test_prepare_features_imputes_bureau_and_defaults_ratios(spark):
    rf = _risk_factors(
        spark,
        (1, dict(external_credit_score=0, avg_daily_balance_90d=0, debit_velocity_30d=0)),
    )
    feat = prepare_features(rf, _c360(spark, 1)).collect()[0]
    assert feat.external_credit_score == 680.0            # imputed
    assert feat.bureau_score_norm == pytest.approx((680 - 300) / 550 * 100)
    assert feat.balance_trend_ratio == 1.0                 # 90d <= 0 -> 1
    assert feat.velocity_ratio == 1.0                      # 30d <= 0 -> 1


def test_default_flag_uses_late_gt_2(spark):
    rf = _risk_factors(spark, (1, dict(payment_late_cnt=3)), (2, dict(payment_late_cnt=2)))
    feats = {r.customer_id: r.default_flag for r in prepare_features(rf, _c360(spark, 1, 2)).collect()}
    assert feats[1] == 1.0 and feats[2] == 0.0


def test_composite_tier_and_driver_tiebreak(spark):
    # bureau=680 -> norm 69.09; payment 100 -> behaviour 0; 7d/30d=70/300 -> ratio 1 -> velocity 0.
    rf = _risk_factors(
        spark,
        (1, dict(external_credit_score=680, debit_velocity_7d=70, debit_velocity_30d=300)),
    )
    out = build_customer_risk_scores(rf, _c360(spark, 1), RUN_DATE, LOAD_TS).collect()[0]
    assert float(out.credit_risk_component) == pytest.approx(30.91, abs=0.01)
    assert float(out.bureau_score_component) == pytest.approx(69.09, abs=0.01)
    assert float(out.composite_risk_score) == pytest.approx(15.45, abs=0.01)
    assert out.risk_tier == "LOW"
    # credit_risk == (100 - bureau_component) always -> SAS strict '>' keeps the
    # earlier index (CREDIT_UTILIZATION) primary, BUREAU_SCORE secondary.
    assert out.primary_risk_driver == "CREDIT_UTILIZATION"
    assert out.secondary_risk_driver == "BUREAU_SCORE"


def test_velocity_can_dominate_as_primary_driver(spark):
    # velocity_ratio = (7d*30/7)/30d. 7d=700,30d=1000 -> 3.0 -> velocity_risk=(3-1)*50=100.
    rf = _risk_factors(spark, (1, dict(debit_velocity_7d=700, debit_velocity_30d=1000)))
    out = build_customer_risk_scores(rf, _c360(spark, 1), RUN_DATE, LOAD_TS).collect()[0]
    assert float(out.velocity_risk_component) == pytest.approx(100.0, abs=0.01)
    assert out.primary_risk_driver == "TRANSACTION_VELOCITY"


def test_review_required_when_high_composite_and_high_velocity(spark):
    # Worst-case bureau (300) + 0% on-time + velocity ratio 3 -> composite 100 (CRITICAL),
    # velocity_ratio > 2 -> review_required = Y.
    rf = _risk_factors(spark, (1, dict(
        external_credit_score=300, payment_ontime_pct=0,
        debit_velocity_7d=700, debit_velocity_30d=1000,
    )))
    out = build_customer_risk_scores(rf, _c360(spark, 1), RUN_DATE, LOAD_TS).collect()[0]
    assert out.risk_tier == "CRITICAL"
    assert out.review_required_flag == "Y"


def test_single_label_fallback_zero_probability(spark):
    # All default_flag = 0 -> LR cannot fit -> base-rate probability 0.
    rf = _risk_factors(spark, (1, {}), (2, {}))
    out = build_customer_risk_scores(rf, _c360(spark, 1, 2), RUN_DATE, LOAD_TS)
    assert all(float(r.probability_of_default) == 0.0 for r in out.collect())


def test_logistic_regression_fits_with_two_labels(spark):
    # Mixed default_flag (late>2 for some) -> LogisticRegression fits and yields
    # calibrated probabilities in [0, 1].
    customers = [(cid, dict(payment_late_cnt=(5 if cid % 2 else 0))) for cid in range(1, 9)]
    rf = _risk_factors(spark, *customers)
    ids = [cid for cid, _ in customers]
    out = build_customer_risk_scores(rf, _c360(spark, *ids), RUN_DATE, LOAD_TS).collect()
    assert len(out) == 8
    assert all(0.0 <= float(r.probability_of_default) <= 1.0 for r in out)
