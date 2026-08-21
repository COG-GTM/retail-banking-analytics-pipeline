"""Unit tests for the migrated risk scoring logic (TICKET-08 / MBA-2209)."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from risk_scoring.config import RiskScoringParams  # noqa: E402
from risk_scoring.features import build_features, extract_risk_raw  # noqa: E402
from risk_scoring.model import FittedModel, stepwise_logistic  # noqa: E402
from risk_scoring.reconcile import reconcile  # noqa: E402
from risk_scoring.scoring import score_customers  # noqa: E402
from risk_scoring.validation import validate_risk_scores  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("risk-scoring-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def params() -> RiskScoringParams:
    return RiskScoringParams(min_rows=1)


def _risk_factors(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, 720, 100.0, 50.0, 1000.0, 900.0, 10.0, 0.2, 100.0, 0, 0, 0, 0),
            (2, 0, 500.0, 100.0, 500.0, 1000.0, 90.0, 0.9, 40.0, 5, 3, 4, 2),
            (3, None, 300.0, 300.0, 800.0, 0.0, 50.0, 0.5, 70.0, 3, 1, 2, 1),
        ],
        "CUSTOMER_ID int, EXTERNAL_CREDIT_SCORE int, DEBIT_VELOCITY_7D double, "
        "DEBIT_VELOCITY_30D double, AVG_DAILY_BALANCE_30D double, AVG_DAILY_BALANCE_90D double, "
        "BALANCE_VOLATILITY double, CREDIT_UTIL_RATIO double, PAYMENT_ONTIME_PCT double, "
        "PAYMENT_LATE_CNT int, ACCOUNT_OVERDRAFT_CNT int, LARGE_WITHDRAWAL_CNT int, "
        "HIGH_RISK_MERCHANT_CNT int",
    )


def _customer_360(spark: SparkSession):
    return spark.createDataFrame(
        [
            (1, 60, 2, 5000.0, "A"),
            (2, 12, 1, 100.0, "A"),
            (3, 24, 3, 900.0, "A"),
            (4, 24, 3, 900.0, "C"),
        ],
        "CUSTOMER_ID int, TENURE_MONTHS int, NUM_ACTIVE_ACCOUNTS int, TOTAL_BALANCE double, "
        "CUSTOMER_STATUS string",
    )


def test_extract_keeps_only_active_customers(spark, params):
    raw = extract_risk_raw(_risk_factors(spark), _customer_360(spark))
    assert sorted(row["CUSTOMER_ID"] for row in raw.collect()) == [1, 2, 3]


def test_feature_derivations_match_sas_rules(spark, params):
    features = build_features(extract_risk_raw(_risk_factors(spark), _customer_360(spark)), params)
    rows = {row["CUSTOMER_ID"]: row for row in features.collect()}

    # Non-positive and missing bureau scores are imputed with 680 before normalisation.
    assert rows[2]["EXTERNAL_CREDIT_SCORE"] == pytest.approx(680.0)
    assert rows[3]["EXTERNAL_CREDIT_SCORE"] == pytest.approx(680.0)
    assert rows[1]["BUREAU_SCORE_NORM"] == pytest.approx((720 - 300) / 550 * 100)

    # Divide-by-zero guards fall back to a ratio of 1, as in the SAS DATA step.
    assert rows[3]["BALANCE_TREND_RATIO"] == pytest.approx(1.0)
    assert rows[1]["VELOCITY_RATIO"] == pytest.approx(100.0 * (30 / 7) / 50.0)

    # DEFAULT_FLAG is PAYMENT_LATE_CNT > 2.
    assert [rows[i]["DEFAULT_FLAG"] for i in (1, 2, 3)] == [0.0, 1.0, 1.0]


def _fitted_model() -> FittedModel:
    return FittedModel(
        selected_features=("BUREAU_SCORE_NORM",),
        intercept=0.0,
        coefficients={"BUREAU_SCORE_NORM": 0.0},
        std_errors={"INTERCEPT": 0.1, "BUREAU_SCORE_NORM": 0.1},
        p_values={"INTERCEPT": 0.01, "BUREAU_SCORE_NORM": 0.01},
        n_observations=3,
        n_events=2,
        converged=True,
        log_likelihood=-1.0,
    )


def test_composite_score_and_tier_match_sas_formula(spark, params):
    features = build_features(extract_risk_raw(_risk_factors(spark), _customer_360(spark)), params)
    scored = {
        row["CUSTOMER_ID"]: row
        for row in score_customers(features, _fitted_model(), params, "2026-04-10").collect()
    }

    row = scored[1]
    bureau_norm = (720 - 300) / 550 * 100
    credit = max(0.0, min(100.0, 100 - bureau_norm))
    velocity = max(0.0, min(100.0, (100.0 * (30 / 7) / 50.0 - 1) * 50))
    expected = round(
        credit * 0.30 + 0.0 * 0.25 + velocity * 0.15 + (100 - bureau_norm) * 0.20 + 0.0 * 0.10, 2
    )
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(expected)
    assert 20 <= expected < 40 and row["RISK_TIER"] == "MODERATE"
    assert row["PROBABILITY_OF_DEFAULT"] == pytest.approx(0.5)
    assert row["SCORE_DELTA_30D"] == 0.0
    assert str(row["EFFECTIVE_DATE"]) == "2026-04-10"


def test_risk_drivers_follow_sas_tie_breaking(spark, params):
    features = build_features(extract_risk_raw(_risk_factors(spark), _customer_360(spark)), params)
    scored = {
        row["CUSTOMER_ID"]: row
        for row in score_customers(features, _fitted_model(), params).collect()
    }

    # Customer 2 has a saturated velocity component, then payment behaviour, so the SAS loop
    # ranks TRANSACTION_VELOCITY first and PAYMENT_BEHAVIOUR second.
    row = scored[2]
    assert row["PRIMARY_RISK_DRIVER"] == "TRANSACTION_VELOCITY"
    assert row["SECONDARY_RISK_DRIVER"] == "PAYMENT_BEHAVIOUR"

    # CREDIT_RISK_COMPONENT and 100 - BUREAU_SCORE_COMPONENT are always equal; the tie goes to the
    # later array element, matching the driver assignments in the SAS data product.
    assert scored[1]["PRIMARY_RISK_DRIVER"] == "TRANSACTION_VELOCITY"
    assert scored[1]["SECONDARY_RISK_DRIVER"] == "BUREAU_SCORE"


def test_tier_boundaries_and_threshold_are_parameters(spark):
    params = RiskScoringParams(
        min_rows=1,
        risk_score_threshold=700.0,
        tier_boundaries=((10.0, "LOW"), (20.0, "MODERATE")),
        top_tier="ELEVATED",
    )
    features = build_features(extract_risk_raw(_risk_factors(spark), _customer_360(spark)), params)
    scores = score_customers(features, _fitted_model(), params)
    assert set(row["RISK_TIER"] for row in scores.collect()) <= {"LOW", "MODERATE", "ELEVATED"}

    report = validate_risk_scores(scores, params, features=features)
    assert report.passed, report.failures
    assert report.subprime_bureau_pct == pytest.approx(66.6667, abs=1e-3)


def test_validation_fails_on_row_count(spark, params):
    features = build_features(extract_risk_raw(_risk_factors(spark), _customer_360(spark)), params)
    strict = RiskScoringParams(min_rows=1000)
    report = validate_risk_scores(score_customers(features, _fitted_model(), strict), strict)
    assert not report.passed
    assert any("row count" in failure for failure in report.failures)


def test_stepwise_selects_only_significant_variables():
    rng = np.random.default_rng(7)
    signal = rng.normal(size=400)
    noise = rng.normal(size=400)
    probability = 1.0 / (1.0 + np.exp(-(0.2 + 1.5 * signal)))
    frame = pd.DataFrame(
        {
            "DEFAULT_FLAG": (rng.uniform(size=400) < probability).astype(float),
            "SIGNAL": signal,
            "NOISE": noise,
        }
    )
    params = RiskScoringParams(candidate_features=("SIGNAL", "NOISE"))
    model = stepwise_logistic(frame, params)
    assert "SIGNAL" in model.selected_features
    assert "NOISE" not in model.selected_features
    assert model.converged


def test_reconcile_flags_score_breaches():
    baseline = pd.DataFrame(
        {
            "CUSTOMER_ID": [1, 2],
            "COMPOSITE_RISK_SCORE": [10.0, 20.0],
            "PROBABILITY_OF_DEFAULT": [0.1, 0.2],
            "RISK_TIER": ["LOW", "MODERATE"],
            "PRIMARY_RISK_DRIVER": ["BUREAU_SCORE", "BUREAU_SCORE"],
            "SECONDARY_RISK_DRIVER": ["CREDIT_UTILIZATION", "CREDIT_UTILIZATION"],
        }
    )
    candidate = baseline.copy()
    candidate.loc[1, "COMPOSITE_RISK_SCORE"] = 25.0
    candidate.loc[1, "RISK_TIER"] = "ELEVATED"

    result = reconcile(candidate, baseline)
    assert result.matched_rows == 2
    assert result.score_breaches == 1
    assert result.tier_agreement_pct == 50.0
    assert not result.passed
