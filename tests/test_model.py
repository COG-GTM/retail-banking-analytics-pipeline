"""Tests for ``risk_scoring.model`` (STEP 3 — PROC LOGISTIC replacement)."""

from __future__ import annotations

import math
import random

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

from risk_scoring.config import PipelineConfig
from risk_scoring.model import ModelOptions, train_and_score
from risk_scoring.schemas import MODEL_PREDICTORS, MODEL_TARGET, PROB_DEFAULT

# Values that keep every non-signal predictor constant, so the score test for
# them is exactly zero and selection stays deterministic.
BASELINE = {
    "BUREAU_SCORE_NORM": 69.09,
    "CREDIT_UTIL_RATIO": 0.30,
    "PAYMENT_ONTIME_PCT": 100.0,
    "BALANCE_VOLATILITY": 0.25,
    "VELOCITY_RATIO": 1.0,
    "ACCOUNT_OVERDRAFT_CNT": 0.0,
    "LARGE_WITHDRAWAL_CNT": 0.0,
    "HIGH_RISK_MERCHANT_CNT": 0.0,
    "TENURE_MONTHS": 48.0,
}

RISK_SCHEMA = StructType(
    [StructField("CUSTOMER_ID", LongType(), False)]
    + [StructField(name, DoubleType(), True) for name in MODEL_PREDICTORS]
    + [StructField(MODEL_TARGET, IntegerType(), True)]
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("risk_scoring_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session


@pytest.fixture(scope="session")
def config() -> PipelineConfig:
    return PipelineConfig()


@pytest.fixture(scope="session")
def fast_options() -> ModelOptions:
    """Fewer iterations than production; the fixtures are tiny and well behaved."""
    return ModelOptions(max_iter=50, tol=1e-6)


def make_row(customer_id: int, target: int | None = 0, **overrides):
    values = dict(BASELINE)
    values.update(overrides)
    return (customer_id, *[values[name] for name in MODEL_PREDICTORS], target)


def make_df(spark: SparkSession, rows):
    return spark.createDataFrame(rows, schema=RISK_SCHEMA)


def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


@pytest.fixture(scope="session")
def signal_rows():
    """200 rows where CREDIT_UTIL_RATIO drives DEFAULT_FLAG and TENURE_MONTHS is noise.

    The relationship is probabilistic rather than separable — perfect separation
    would send the MLE to infinity and make the Wald statistics meaningless.
    """
    rng = random.Random(20240501)
    rows = []
    for i in range(200):
        util = i / 199.0
        probability = logistic(-2.5 + 5.0 * util)
        target = 1 if rng.random() < probability else 0
        rows.append(
            make_row(
                1000 + i,
                target,
                CREDIT_UTIL_RATIO=util,
                TENURE_MONTHS=float(rng.randint(1, 240)),
            )
        )
    return rows


@pytest.fixture(scope="session")
def signal_result(spark, config, fast_options, signal_rows):
    return train_and_score(make_df(spark, signal_rows), config, options=fast_options)


def test_every_row_survives_with_a_double_probability(signal_result, signal_rows):
    scored = signal_result.scored
    assert scored.count() == len(signal_rows)
    assert scored.columns[-1] == PROB_DEFAULT
    assert dict(scored.dtypes)[PROB_DEFAULT] == "double"


def test_input_columns_are_preserved_unchanged(spark, signal_rows, signal_result):
    source = make_df(spark, signal_rows)
    assert signal_result.scored.columns == [*source.columns, PROB_DEFAULT]
    assert dict(signal_result.scored.dtypes)[MODEL_TARGET] == "int"


def test_probabilities_are_within_the_unit_interval(signal_result):
    bounds = signal_result.scored.selectExpr(
        f"min({PROB_DEFAULT}) AS LO", f"max({PROB_DEFAULT}) AS HI"
    ).first()
    assert 0.0 <= bounds["LO"] <= 1.0
    assert 0.0 <= bounds["HI"] <= 1.0


def test_probability_is_p_of_the_event_not_its_complement(signal_result):
    """``descending``: high-risk rows must score higher than low-risk rows.

    Catches an inverted ``probability[0]`` / ``probability[1]`` extraction.
    """
    scored = signal_result.scored
    lowest = scored.orderBy("CREDIT_UTIL_RATIO").first()
    highest = scored.orderBy(scored["CREDIT_UTIL_RATIO"].desc()).first()
    assert highest[PROB_DEFAULT] > lowest[PROB_DEFAULT]

    high_mean = scored.filter("CREDIT_UTIL_RATIO > 0.75").selectExpr(
        f"avg({PROB_DEFAULT})"
    ).first()[0]
    low_mean = scored.filter("CREDIT_UTIL_RATIO < 0.25").selectExpr(
        f"avg({PROB_DEFAULT})"
    ).first()[0]
    assert high_mean > low_mean


def test_signal_predictor_is_selected_and_positively_signed(signal_result):
    assert "CREDIT_UTIL_RATIO" in signal_result.selected_features
    assert signal_result.coefficients["CREDIT_UTIL_RATIO"] > 0.0
    assert any("CREDIT_UTIL_RATIO entered" in line for line in signal_result.steps)


def test_stepwise_leaves_the_irrelevant_noise_predictor_out(signal_result):
    """TENURE_MONTHS is pseudo-random noise: it must not meet slentry=0.10."""
    assert "TENURE_MONTHS" not in signal_result.selected_features
    assert "TENURE_MONTHS" not in signal_result.coefficients


def test_full_model_can_be_fitted_without_stepwise(spark, config, signal_rows):
    result = train_and_score(
        make_df(spark, signal_rows),
        config,
        options=ModelOptions(stepwise=False, max_iter=50),
    )
    assert result.selected_features == list(MODEL_PREDICTORS)
    assert set(result.coefficients) == set(MODEL_PREDICTORS)
    assert result.scored.filter(f"{PROB_DEFAULT} IS NULL").count() == 0


def test_degenerate_single_class_target_does_not_raise(spark, config, fast_options):
    """The committed extract has DEFAULT_FLAG = 0 everywhere; this is that path."""
    rows = [
        make_row(i, 0, CREDIT_UTIL_RATIO=0.1 * (i % 9), TENURE_MONTHS=float(i))
        for i in range(30)
    ]
    result = train_and_score(make_df(spark, rows), config, options=fast_options)

    assert result.selected_features == []
    assert result.coefficients == {}
    assert result.intercept == -math.inf
    assert any("unidentifiable" in line for line in result.steps)

    scored = result.scored
    assert scored.count() == len(rows)
    assert scored.filter(f"{PROB_DEFAULT} IS NULL").count() == 0
    assert scored.selectExpr(f"max({PROB_DEFAULT})").first()[0] == 0.0


def test_degenerate_all_events_target_scores_one(spark, config, fast_options):
    rows = [make_row(i, 1, CREDIT_UTIL_RATIO=0.1 * (i % 9)) for i in range(20)]
    result = train_and_score(make_df(spark, rows), config, options=fast_options)

    assert result.intercept == math.inf
    assert result.scored.selectExpr(f"min({PROB_DEFAULT})").first()[0] == 1.0


def test_rows_with_a_null_predictor_survive_with_a_null_probability(
    spark, config, fast_options, signal_rows
):
    rows = list(signal_rows)
    rows.append(make_row(99001, 0, BALANCE_VOLATILITY=None))
    rows.append(make_row(99002, 1, TENURE_MONTHS=None))
    result = train_and_score(make_df(spark, rows), config, options=fast_options)

    scored = result.scored
    assert scored.count() == len(rows)
    unscored = {
        row["CUSTOMER_ID"]: row[PROB_DEFAULT]
        for row in scored.filter("CUSTOMER_ID >= 99000").collect()
    }
    assert unscored == {99001: None, 99002: None}
    assert scored.filter(f"{PROB_DEFAULT} IS NULL").count() == 2


def test_no_fittable_rows_yields_null_probabilities(spark, config, fast_options):
    rows = [make_row(i, 0, CREDIT_UTIL_RATIO=None) for i in range(5)]
    result = train_and_score(make_df(spark, rows), config, options=fast_options)

    assert result.selected_features == []
    assert math.isnan(result.intercept)
    assert result.scored.count() == len(rows)
    assert result.scored.filter(f"{PROB_DEFAULT} IS NOT NULL").count() == 0


def test_missing_model_column_is_rejected(spark, config):
    df = make_df(spark, [make_row(1, 0)]).drop("VELOCITY_RATIO")
    with pytest.raises(ValueError, match="VELOCITY_RATIO"):
        train_and_score(df, config)


def test_scoring_is_deterministic_across_runs(spark, config, fast_options, signal_rows):
    df = make_df(spark, signal_rows)
    first = train_and_score(df, config, options=fast_options)
    second = train_and_score(df, config, options=fast_options)

    assert first.selected_features == second.selected_features
    assert first.scored.exceptAll(second.scored).count() == 0
