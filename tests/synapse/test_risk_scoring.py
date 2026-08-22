"""Unit tests for the PySpark risk scoring job (MBA-2209 / TICKET-08)."""

from __future__ import annotations

import math
import random

import pytest
from pyspark.sql import functions as F

from synapse.risk_scoring.features import join_risk_inputs, prepare_features
from synapse.risk_scoring.model import fit_stepwise, score_probability
from synapse.risk_scoring.scoring import OUTPUT_COLUMNS, _top_two_drivers, classify_risk
from synapse.risk_scoring.validation import validate_risk_scores

RISK_FACTOR_SCHEMA = """
    CUSTOMER_ID long, ACCOUNT_OVERDRAFT_CNT int, LARGE_WITHDRAWAL_CNT int,
    AVG_DAILY_BALANCE_30D double, AVG_DAILY_BALANCE_90D double,
    BALANCE_VOLATILITY double, CREDIT_UTIL_RATIO double, PAYMENT_ONTIME_PCT double,
    PAYMENT_LATE_CNT int, EXTERNAL_CREDIT_SCORE int, DEBIT_VELOCITY_7D double,
    DEBIT_VELOCITY_30D double, HIGH_RISK_MERCHANT_CNT int
"""

CUSTOMER_360_SCHEMA = """
    CUSTOMER_ID long, TENURE_MONTHS int, NUM_ACTIVE_ACCOUNTS int,
    TOTAL_BALANCE double, CUSTOMER_STATUS string
"""


def _risk_row(customer_id: int, **overrides):
    row = {
        "CUSTOMER_ID": customer_id,
        "ACCOUNT_OVERDRAFT_CNT": 0,
        "LARGE_WITHDRAWAL_CNT": 0,
        "AVG_DAILY_BALANCE_30D": 1000.0,
        "AVG_DAILY_BALANCE_90D": 1000.0,
        "BALANCE_VOLATILITY": 10.0,
        "CREDIT_UTIL_RATIO": 0.2,
        "PAYMENT_ONTIME_PCT": 95.0,
        "PAYMENT_LATE_CNT": 0,
        "EXTERNAL_CREDIT_SCORE": 720,
        "DEBIT_VELOCITY_7D": 100.0,
        "DEBIT_VELOCITY_30D": 428.57,
        "HIGH_RISK_MERCHANT_CNT": 0,
    }
    row.update(overrides)
    return tuple(row.values())


def _frames(spark, risk_rows, customer_rows):
    return (
        spark.createDataFrame(risk_rows, RISK_FACTOR_SCHEMA),
        spark.createDataFrame(customer_rows, CUSTOMER_360_SCHEMA),
    )


def test_join_keeps_only_active_customers(spark):
    risk, customers = _frames(
        spark,
        [_risk_row(1), _risk_row(2)],
        [(1, 24, 2, 5000.0, "A"), (2, 36, 1, 100.0, "C")],
    )
    joined = join_risk_inputs(risk, customers)
    assert [r["CUSTOMER_ID"] for r in joined.collect()] == [1]


def test_prepare_features_matches_sas_data_step(spark):
    risk, customers = _frames(
        spark,
        [
            _risk_row(1, EXTERNAL_CREDIT_SCORE=0, PAYMENT_LATE_CNT=3),
            _risk_row(2, AVG_DAILY_BALANCE_90D=0.0, DEBIT_VELOCITY_30D=0.0),
        ],
        [(1, 24, 2, 5000.0, "A"), (2, 36, 1, 100.0, "A")],
    )
    features = prepare_features(join_risk_inputs(risk, customers)).orderBy("CUSTOMER_ID").collect()

    imputed, degenerate = features
    # bureau score imputed to 680 then normalised to (680-300)/550*100
    assert imputed["EXTERNAL_CREDIT_SCORE"] == 680.0
    assert imputed["BUREAU_SCORE_NORM"] == pytest.approx((680 - 300) / 550 * 100)
    assert imputed["DEFAULT_FLAG"] == 1.0
    # ratios fall back to 1 when the denominator is not positive
    assert degenerate["BALANCE_TREND_RATIO"] == 1.0
    assert degenerate["VELOCITY_RATIO"] == 1.0
    assert degenerate["DEFAULT_FLAG"] == 0.0


def test_velocity_ratio_annualises_seven_day_window(spark):
    risk, customers = _frames(
        spark,
        [_risk_row(1, DEBIT_VELOCITY_7D=100.0, DEBIT_VELOCITY_30D=200.0)],
        [(1, 24, 2, 5000.0, "A")],
    )
    row = prepare_features(join_risk_inputs(risk, customers)).first()
    assert row["VELOCITY_RATIO"] == pytest.approx(100.0 * (30 / 7) / 200.0)


@pytest.mark.parametrize(
    ("components", "expected"),
    [
        ((80.0, 60.0, 10.0, 20.0), ("CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR")),
        ((10.0, 20.0, 90.0, 15.0), ("TRANSACTION_VELOCITY", "PAYMENT_BEHAVIOUR")),
        ((5.0, 5.0, 5.0, 5.0), ("CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR")),
        ((0.0, 0.0, 0.0, 0.0), ("", "")),
        ((30.0, 0.0, 0.0, 0.0), ("CREDIT_UTILIZATION", "")),
    ],
)
def test_top_two_drivers_replicates_sas_loop(components, expected):
    assert _top_two_drivers(*components) == expected


def test_classify_risk_composite_score_and_tier(spark):
    risk, customers = _frames(
        spark,
        [
            _risk_row(1, EXTERNAL_CREDIT_SCORE=820, PAYMENT_ONTIME_PCT=100.0),
            _risk_row(
                2,
                EXTERNAL_CREDIT_SCORE=320,
                PAYMENT_ONTIME_PCT=10.0,
                DEBIT_VELOCITY_7D=1000.0,
                DEBIT_VELOCITY_30D=500.0,
                PAYMENT_LATE_CNT=9,
            ),
        ],
        [(1, 24, 2, 5000.0, "A"), (2, 36, 1, 100.0, "A")],
    )
    features = prepare_features(join_risk_inputs(risk, customers)).withColumn(
        "PROB_DEFAULT", F.lit(0.75)
    )
    rows = {r["CUSTOMER_ID"]: r for r in classify_risk(features, "RISK_V4.0").collect()}

    low = rows[1]
    bureau_norm = (820 - 300) / 550 * 100
    expected_low = round(
        max(0.0, min(100.0, 100 - bureau_norm)) * 0.30
        + 0.0 * 0.25
        + 0.0 * 0.15
        + (100 - bureau_norm) * 0.20
        + 0.0 * 0.10,
        2,
    )
    assert low["COMPOSITE_RISK_SCORE"] == pytest.approx(expected_low)
    assert low["RISK_TIER"] == "LOW"
    assert low["PROBABILITY_OF_DEFAULT"] == pytest.approx(0.75)
    assert low["WATCH_LIST_FLAG"] == "N"

    high = rows[2]
    assert high["RISK_TIER"] in {"HIGH", "CRITICAL"}
    # velocity ratio 8.57 clamps the velocity component to 100, the largest of the four
    assert high["PRIMARY_RISK_DRIVER"] == "TRANSACTION_VELOCITY"
    assert high["SECONDARY_RISK_DRIVER"] == "CREDIT_UTILIZATION"
    assert high["REVIEW_REQUIRED_FLAG"] == "Y"
    assert set(OUTPUT_COLUMNS) == set(high.asDict())


def test_stepwise_selects_signal_and_scores_probabilities(spark):
    random.seed(11)
    rows = []
    for i in range(400):
        bureau = int(min(850, max(300, random.gauss(660, 70))))
        # the bureau score drives the default probability, everything else is noise
        odds = math.exp(6.0 - 0.010 * bureau)
        late = 5 if random.random() < odds / (1 + odds) else 0
        rows.append(
            _risk_row(
                i,
                PAYMENT_LATE_CNT=late,
                EXTERNAL_CREDIT_SCORE=bureau,
                PAYMENT_ONTIME_PCT=round(random.uniform(70.0, 100.0), 2),
                BALANCE_VOLATILITY=round(random.uniform(0.0, 50.0), 2),
                HIGH_RISK_MERCHANT_CNT=random.randint(0, 3),
            )
        )
    risk, customers = _frames(
        spark, rows, [(i, random.randint(1, 240), 2, 1000.0, "A") for i in range(400)]
    )
    features = prepare_features(join_risk_inputs(risk, customers)).cache()

    result = fit_stepwise(
        features,
        candidate_features=("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY", "TENURE_MONTHS"),
        slentry=0.10,
        slstay=0.05,
    )
    assert "BUREAU_SCORE_NORM" in result.selected_features
    assert set(result.coefficients) == set(result.selected_features)
    assert "selected_features" in result.as_audit_record("RISK_V4.0")

    scored = score_probability(features, result)
    probabilities = [r["PROB_DEFAULT"] for r in scored.select("PROB_DEFAULT").collect()]
    assert all(0.0 <= p <= 1.0 and not math.isnan(p) for p in probabilities)


def test_stepwise_falls_back_to_base_rate_for_single_class_target(spark):
    risk, customers = _frames(
        spark,
        [_risk_row(i, PAYMENT_LATE_CNT=0) for i in range(10)],
        [(i, 12, 1, 100.0, "A") for i in range(10)],
    )
    features = prepare_features(join_risk_inputs(risk, customers))
    result = fit_stepwise(features, candidate_features=("BUREAU_SCORE_NORM",))
    assert result.selected_features == []
    scored = score_probability(features, result)
    assert scored.select(F.max("PROB_DEFAULT")).first()[0] == 0.0


def test_validation_flags_row_count_and_duplicates(spark):
    risk, customers = _frames(
        spark,
        [_risk_row(1), _risk_row(1)],
        [(1, 24, 2, 5000.0, "A")],
    )
    features = prepare_features(join_risk_inputs(risk, customers)).withColumn(
        "PROB_DEFAULT", F.lit(0.1)
    )
    report = validate_risk_scores(classify_risk(features, "RISK_V4.0"), min_rows=1000)
    assert not report.passed
    assert any("row count" in f for f in report.failures)
    assert any("not unique" in f for f in report.failures)
    assert sum(report.tier_distribution.values()) == 2
