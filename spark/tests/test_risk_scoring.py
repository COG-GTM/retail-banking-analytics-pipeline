"""Tests for the risk scoring job (03)."""
from __future__ import annotations

import math

from spark.jobs.risk_scoring import (
    MODEL_PREDICTORS,
    classify,
    prepare_features,
    score_probability_of_default,
)

RAW_COLUMNS = [
    "customer_id", "external_credit_score", "avg_daily_balance_30d",
    "avg_daily_balance_90d", "debit_velocity_7d", "debit_velocity_30d",
    "payment_late_cnt",
]


def test_prepare_features_imputation_and_ratios(spark, config):
    df = spark.createDataFrame(
        [
            (1, 0, 100.0, 200.0, 0.0, 0.0, 3),      # missing bureau -> imputed
            (2, 680, 300.0, 200.0, 14.0, 60.0, 1),  # normal
        ],
        RAW_COLUMNS,
    )
    out = {r["customer_id"]: r for r in prepare_features(df, config).collect()}

    c1 = out[1]
    assert c1["external_credit_score"] == config.default_bureau_score
    assert math.isclose(c1["bureau_score_norm"], (config.default_bureau_score - 300) / 550 * 100)
    # debit_velocity_30d == 0 -> velocity_ratio defaults to 1.0
    assert math.isclose(c1["velocity_ratio"], 1.0)
    # payment_late_cnt 3 > 2 -> default_flag 1
    assert c1["default_flag"] == 1
    assert out[2]["default_flag"] == 0


def test_classify_composite_tier_and_drivers(spark, config):
    df = spark.createDataFrame(
        [(3, 85.0909090909091, 100.0, 1.2222697623353138, 0.0)],
        ["customer_id", "bureau_score_norm", "payment_ontime_pct", "velocity_ratio", "prob_default"],
    )
    row = classify(df, config).first()
    assert math.isclose(row["credit_risk_component"], 14.90909, abs_tol=1e-4)
    assert math.isclose(row["composite_risk_score"], 9.12, abs_tol=1e-2)
    assert row["risk_tier"] == "LOW"
    # SAS top-two driver loop: strict > tie-break keeps first-seen as primary
    assert row["primary_risk_driver"] == "CREDIT_UTILIZATION"
    assert row["secondary_risk_driver"] == "BUREAU_SCORE"


def test_score_probability_of_default_two_classes(spark, config, audit):
    base = {c: 0.0 for c in MODEL_PREDICTORS}
    rows = []
    for i in range(20):
        vals = dict(base)
        default = 1 if i % 2 == 0 else 0
        # make the classes separable
        vals["credit_util_ratio"] = 0.9 if default else 0.1
        vals["payment_ontime_pct"] = 50.0 if default else 99.0
        rows.append((i, default, vals["credit_util_ratio"], vals["payment_ontime_pct"],
                     *[base[c] for c in MODEL_PREDICTORS if c not in ("credit_util_ratio", "payment_ontime_pct")]))

    other = [c for c in MODEL_PREDICTORS if c not in ("credit_util_ratio", "payment_ontime_pct")]
    cols = ["customer_id", "default_flag", "credit_util_ratio", "payment_ontime_pct"] + other
    df = spark.createDataFrame(rows, cols)

    scored = score_probability_of_default(df, audit)
    assert "prob_default" in scored.columns
    assert scored.count() == 20
    probs = [r["prob_default"] for r in scored.collect()]
    assert all(0.0 <= p <= 1.0 for p in probs)
