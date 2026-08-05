"""Unit tests for the pure transforms of ``jobs/sas_risk_scoring.py``."""

from __future__ import annotations

import math
import random
from datetime import date

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)

from common import schemas
from common.config import RiskScoringConstants
from jobs.sas_risk_scoring import (
    CANDIDATE_FEATURES,
    DRIVER_LABELS,
    _fit_logistic,
    has_both_classes,
    select_features_stepwise,
    transform_customer_risk_scores,
    transform_probability_of_default,
    transform_risk_classified,
    transform_risk_drivers,
    transform_risk_features,
    transform_risk_raw,
    wald_p_values,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)

SCORED_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType()),
        StructField("BUREAU_SCORE_NORM", DoubleType()),
        StructField("PAYMENT_ONTIME_PCT", DoubleType()),
        StructField("VELOCITY_RATIO", DoubleType()),
        StructField("PROB_DEFAULT", DoubleType()),
    ]
)

COMPONENT_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType()),
        StructField("CREDIT_RISK_COMPONENT", DoubleType()),
        StructField("BEHAVIOUR_RISK_COMPONENT", DoubleType()),
        StructField("VELOCITY_RISK_COMPONENT", DoubleType()),
        StructField("BUREAU_SCORE_COMPONENT", DoubleType()),
    ]
)

TRAINING_SCHEMA = StructType(
    [
        StructField("CUSTOMER_ID", LongType()),
        StructField("BUREAU_SCORE_NORM", DoubleType()),
        StructField("BALANCE_VOLATILITY", DoubleType()),
        StructField("DEFAULT_FLAG", IntegerType()),
    ]
)


def scored_df(spark, rows):
    return spark.createDataFrame(rows, schema=SCORED_SCHEMA)


def by_id(df):
    return {row["CUSTOMER_ID"]: row for row in df.collect()}


# ------------------------------------------------------------------------------------------
# Step 1: extract
# ------------------------------------------------------------------------------------------


@pytest.fixture
def risk_factors(make_df):
    return make_df(
        schemas.STG_RISK_FACTORS,
        [
            {"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": 680, "PAYMENT_ONTIME_PCT": 100.00},
            {"CUSTOMER_ID": 2, "EXTERNAL_CREDIT_SCORE": 700, "PAYMENT_ONTIME_PCT": 100.00},
            {"CUSTOMER_ID": 3, "EXTERNAL_CREDIT_SCORE": 720, "PAYMENT_ONTIME_PCT": 100.00},
            {"CUSTOMER_ID": 99, "EXTERNAL_CREDIT_SCORE": 800, "PAYMENT_ONTIME_PCT": 100.00},
        ],
    )


@pytest.fixture
def customer_360(make_df):
    return make_df(
        schemas.STG_CUSTOMER_360,
        [
            {"CUSTOMER_ID": 1, "CUSTOMER_STATUS": "A", "TENURE_MONTHS": 12},
            {"CUSTOMER_ID": 2, "CUSTOMER_STATUS": "I", "TENURE_MONTHS": 24},
            {"CUSTOMER_ID": 3, "CUSTOMER_STATUS": "A", "TENURE_MONTHS": 36},
            {"CUSTOMER_ID": 4, "CUSTOMER_STATUS": "A", "TENURE_MONTHS": 48},
        ],
    )


def test_risk_raw_inner_joins_active_customers_only(risk_factors, customer_360):
    raw = transform_risk_raw(risk_factors, customer_360)

    rows = by_id(raw)
    # 2 is inactive, 4 has no risk factors, 99 has no customer-360 row
    assert sorted(rows) == [1, 3]
    assert rows[3]["TENURE_MONTHS"] == 36
    assert set(rows[3].asDict()) >= {"NUM_ACTIVE_ACCOUNTS", "TOTAL_BALANCE", "CUSTOMER_STATUS"}


# ------------------------------------------------------------------------------------------
# Step 2: feature preparation
# ------------------------------------------------------------------------------------------


@pytest.fixture
def features(make_df, customer_360):
    factors = make_df(
        schemas.STG_RISK_FACTORS,
        [
            # imputed: score of 0 -> 680; 90D balance drives the trend ratio; 30D velocity > 0
            {
                "CUSTOMER_ID": 1,
                "EXTERNAL_CREDIT_SCORE": 0,
                "AVG_DAILY_BALANCE_30D": 500.00,
                "AVG_DAILY_BALANCE_90D": 1000.00,
                "DEBIT_VELOCITY_7D": 70.00,
                "DEBIT_VELOCITY_30D": 300.00,
                "PAYMENT_LATE_CNT": 0,
            },
            # imputed: missing score -> 680; zero 90D balance and zero 30D velocity -> ratios of 1
            {
                "CUSTOMER_ID": 3,
                "EXTERNAL_CREDIT_SCORE": None,
                "AVG_DAILY_BALANCE_30D": 500.00,
                "AVG_DAILY_BALANCE_90D": 0.00,
                "DEBIT_VELOCITY_7D": 70.00,
                "DEBIT_VELOCITY_30D": 0.00,
                "PAYMENT_LATE_CNT": 0,
            },
        ],
    )
    return transform_risk_features(transform_risk_raw(factors, customer_360))


def test_features_impute_bureau_score_and_normalise_it(features):
    rows = by_id(features)

    # `if EXTERNAL_CREDIT_SCORE <= 0 or missing then 680`, then (680-300)/(850-300)*100
    assert rows[1]["EXTERNAL_CREDIT_SCORE"] == pytest.approx(680.0)
    assert rows[3]["EXTERNAL_CREDIT_SCORE"] == pytest.approx(680.0)
    assert rows[1]["BUREAU_SCORE_NORM"] == pytest.approx((680 - 300) / 550 * 100)


@pytest.mark.parametrize(
    ("score", "expected_norm"),
    [(300, 0.0), (850, 100.0), (575, 50.0), (900, pytest.approx(109.0909, abs=1e-4))],
)
def test_bureau_score_normalisation_endpoints(make_df, customer_360, score, expected_norm):
    factors = make_df(
        schemas.STG_RISK_FACTORS, [{"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": score}]
    )
    prepared = transform_risk_features(transform_risk_raw(factors, customer_360))

    assert by_id(prepared)[1]["BUREAU_SCORE_NORM"] == expected_norm


def test_features_derive_balance_trend_and_velocity_ratios(features):
    rows = by_id(features)

    assert rows[1]["BALANCE_TREND_RATIO"] == pytest.approx(0.5)
    assert rows[1]["VELOCITY_RATIO"] == pytest.approx(70.0 * (30 / 7) / 300.0)
    # both denominators are 0 -> the legacy `else 1` branch
    assert rows[3]["BALANCE_TREND_RATIO"] == pytest.approx(1.0)
    assert rows[3]["VELOCITY_RATIO"] == pytest.approx(1.0)


@pytest.mark.parametrize(("late_count", "expected"), [(None, 0), (0, 0), (2, 0), (3, 1)])
def test_default_flag_is_late_payments_over_two(make_df, customer_360, late_count, expected):
    factors = make_df(
        schemas.STG_RISK_FACTORS, [{"CUSTOMER_ID": 1, "PAYMENT_LATE_CNT": late_count}]
    )
    prepared = transform_risk_features(transform_risk_raw(factors, customer_360))

    assert by_id(prepared)[1]["DEFAULT_FLAG"] == expected


# ------------------------------------------------------------------------------------------
# Step 3: stepwise logistic regression
# ------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def training_data(spark):
    """A small, deliberately noisy two-class sample: feature 1 informative, feature 2 noise."""

    rng = random.Random(20260410)
    rows = []
    for customer_id in range(1, 201):
        informative = rng.uniform(-3.0, 3.0)
        noise = rng.uniform(-3.0, 3.0)
        probability = 1.0 / (1.0 + math.exp(-informative))
        rows.append((customer_id, informative, noise, int(rng.random() < probability)))
    return spark.createDataFrame(rows, schema=TRAINING_SCHEMA).persist()


def test_single_class_target_selects_no_feature(features):
    """The upstream BTEQ payment logic makes an all-zero target the normal case."""

    assert has_both_classes(features) is False
    assert select_features_stepwise(features) == ()


def test_stepwise_enters_the_informative_feature_and_rejects_noise(training_data):
    assert has_both_classes(training_data) is True

    selected = select_features_stepwise(
        training_data, ("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY"), slentry=0.10, slstay=0.05
    )

    assert selected == ("BUREAU_SCORE_NORM",)


@pytest.fixture(scope="module")
def two_signal_data(spark):
    """Two independent informative candidates: both enter and both survive elimination."""

    rng = random.Random(19700101)
    rows = []
    for customer_id in range(1, 301):
        first = rng.uniform(-3.0, 3.0)
        second = rng.uniform(-3.0, 3.0)
        probability = 1.0 / (1.0 + math.exp(-(first + second)))
        rows.append((customer_id, first, second, int(rng.random() < probability)))
    return spark.createDataFrame(rows, schema=TRAINING_SCHEMA).persist()


def test_stepwise_keeps_every_variable_that_clears_slstay(two_signal_data):
    selected = select_features_stepwise(
        two_signal_data, ("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY"), slentry=0.10, slstay=0.05
    )

    assert set(selected) == {"BUREAU_SCORE_NORM", "BALANCE_VOLATILITY"}


def test_wald_p_value_of_a_degenerate_coefficient_is_one(training_data):
    """A constant covariate makes the information matrix singular; it must never enter."""

    features = ("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY")
    constant = training_data.withColumn("BALANCE_VOLATILITY", F.lit(0.0))
    model, assembled = _fit_logistic(constant, features)

    p_values = wald_p_values(model, assembled, features)

    assert p_values["BALANCE_VOLATILITY"] == 1.0
    assert p_values["BUREAU_SCORE_NORM"] < 0.10


def test_stepwise_enters_nothing_when_no_candidate_clears_slentry(training_data):
    assert (
        select_features_stepwise(
            training_data, ("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY"), slentry=0.0, slstay=0.05
        )
        == ()
    )


def test_stepwise_backward_elimination_removes_a_variable_that_cannot_stay(training_data):
    """``slstay=0`` makes every retained variable removable, which stops the search."""

    selected = select_features_stepwise(
        training_data, ("BUREAU_SCORE_NORM", "BALANCE_VOLATILITY"), slentry=1.0, slstay=0.0
    )

    assert len(selected) == 1


def test_probability_of_default_falls_back_to_the_base_rate(training_data, features):
    """No selected feature -> intercept-only model -> the base rate of the event."""

    base_rate = transform_probability_of_default(training_data, ()).collect()[0]["PROB_DEFAULT"]
    expected = training_data.filter("DEFAULT_FLAG = 1").count() / training_data.count()
    assert base_rate == pytest.approx(expected)

    # the all-zero target of the committed extract yields exactly 0.0
    assert {
        row["PROB_DEFAULT"] for row in transform_probability_of_default(features, ()).collect()
    } == {0.0}


def test_probability_of_default_scores_every_row_from_the_fitted_model(training_data):
    scored = transform_probability_of_default(training_data, ("BUREAU_SCORE_NORM",))

    rows = scored.collect()
    assert len(rows) == training_data.count()
    assert all(0.0 <= row["PROB_DEFAULT"] <= 1.0 for row in rows)
    # `descending`: the event is DEFAULT_FLAG = 1, so a higher feature value scores higher
    ordered = sorted(rows, key=lambda row: row["BUREAU_SCORE_NORM"])
    assert ordered[0]["PROB_DEFAULT"] < ordered[-1]["PROB_DEFAULT"]


def test_candidate_features_keep_the_legacy_declaration_order():
    assert CANDIDATE_FEATURES == (
        "BUREAU_SCORE_NORM",
        "CREDIT_UTIL_RATIO",
        "PAYMENT_ONTIME_PCT",
        "BALANCE_VOLATILITY",
        "VELOCITY_RATIO",
        "ACCOUNT_OVERDRAFT_CNT",
        "LARGE_WITHDRAWAL_CNT",
        "HIGH_RISK_MERCHANT_CNT",
        "TENURE_MONTHS",
    )


# ------------------------------------------------------------------------------------------
# Step 4: drivers
# ------------------------------------------------------------------------------------------


def drivers(spark, credit, behaviour, velocity, bureau):
    row = transform_risk_drivers(
        spark.createDataFrame([(1, credit, behaviour, velocity, bureau)], schema=COMPONENT_SCHEMA)
    ).collect()[0]
    return row["PRIMARY_RISK_DRIVER"], row["SECONDARY_RISK_DRIVER"]


def test_driver_labels_match_the_legacy_array():
    assert DRIVER_LABELS == (
        "CREDIT_UTILIZATION",
        "PAYMENT_BEHAVIOUR",
        "TRANSACTION_VELOCITY",
        "BUREAU_SCORE",
    )


def test_drivers_are_blank_when_every_component_is_zero(spark):
    """``_max1``/``_max2`` start at 0 and the tests are strictly ``>``."""

    assert drivers(spark, 0.0, 0.0, 0.0, 100.0) == ("", "")


def test_drivers_pick_the_top_two_components(spark):
    # behaviour is the largest, velocity second; element 4 (100-80=20) beats neither
    assert drivers(spark, 10.0, 60.0, 30.0, 80.0) == ("PAYMENT_BEHAVIOUR", "TRANSACTION_VELOCITY")


def test_a_new_maximum_demotes_the_previous_primary_label(spark):
    # credit enters first, velocity then overtakes it and pushes it into secondary
    assert drivers(spark, 20.0, 0.0, 90.0, 80.0) == ("TRANSACTION_VELOCITY", "CREDIT_UTILIZATION")


def test_ties_keep_the_earlier_array_index(spark):
    # all of credit, behaviour and element 4 are 40: the first takes primary, the second
    # secondary, and the third cannot displace either
    assert drivers(spark, 40.0, 40.0, 0.0, 60.0) == ("CREDIT_UTILIZATION", "PAYMENT_BEHAVIOUR")


def test_element_four_can_only_ever_be_the_secondary_driver(spark):
    """``100 - BUREAU_SCORE_COMPONENT`` duplicates ``CREDIT_RISK_COMPONENT``, so with strict
    ``>`` it never displaces the primary it ties with."""

    assert drivers(spark, 30.0, 0.0, 0.0, 70.0) == ("CREDIT_UTILIZATION", "BUREAU_SCORE")
    assert drivers(spark, 30.0, 0.0, 10.0, 70.0) == ("CREDIT_UTILIZATION", "BUREAU_SCORE")


def test_a_zero_component_never_becomes_a_driver(spark):
    # only credit is non-zero (element 4 is 100-100=0), so there is no secondary
    assert drivers(spark, 15.0, 0.0, 0.0, 100.0) == ("CREDIT_UTILIZATION", "")


# ------------------------------------------------------------------------------------------
# Step 4: components, composite score, tier and flags
# ------------------------------------------------------------------------------------------


def classify(spark, rows, **kwargs):
    return by_id(transform_risk_classified(scored_df(spark, rows), run_date=RUN_DATE, **kwargs))


def test_components_are_clamped_to_zero_one_hundred(spark, load_ts):
    rows = classify(
        spark,
        [
            (1, 120.0, 110.0, 1.0, 0.0),  # both normalisations overshoot
            (2, -10.0, -5.0, 0.5, 0.0),  # and undershoot
        ],
        load_ts=load_ts,
    )

    assert float(rows[1]["CREDIT_RISK_COMPONENT"]) == 0.00
    assert float(rows[1]["BUREAU_SCORE_COMPONENT"]) == 100.00
    assert float(rows[1]["BEHAVIOUR_RISK_COMPONENT"]) == 0.00
    assert float(rows[1]["PAYMENT_HISTORY_COMPONENT"]) == 100.00
    assert float(rows[1]["VELOCITY_RISK_COMPONENT"]) == 0.00
    assert float(rows[2]["CREDIT_RISK_COMPONENT"]) == 100.00
    assert float(rows[2]["BUREAU_SCORE_COMPONENT"]) == 0.00
    assert float(rows[2]["BEHAVIOUR_RISK_COMPONENT"]) == 100.00
    assert float(rows[2]["PAYMENT_HISTORY_COMPONENT"]) == 0.00


def test_composite_double_counts_bureau_and_payment_history(spark, load_ts):
    """The two inverted terms duplicate components 1 and 2 - the legacy quirk, ported as is."""

    row = classify(spark, [(1, 40.0, 30.0, 2.0, 0.0)], load_ts=load_ts)[1]

    credit, behaviour, velocity = 60.0, 70.0, 50.0
    expected = round(
        credit * 0.30 + behaviour * 0.25 + velocity * 0.15 + credit * 0.20 + behaviour * 0.10, 2
    )
    assert float(row["COMPOSITE_RISK_SCORE"]) == expected


@pytest.mark.parametrize(
    ("bureau_norm", "ontime_pct", "velocity_ratio", "expected_score", "expected_tier"),
    [
        (100.0, 100.0, 1.0, 0.00, "LOW"),
        (61.0, 100.0, 1.0, 19.50, "LOW"),  # just under the first cutoff
        (60.0, 100.0, 1.0, 20.00, "MODERATE"),  # exactly on it
        (21.0, 100.0, 1.0, 39.50, "MODERATE"),
        (20.0, 100.0, 1.0, 40.00, "ELEVATED"),
        (0.0, 100.0, 2 + 1 / 3, 60.00, "HIGH"),  # exactly on the third cutoff
        (0.0, 15.0, 1.0, 79.75, "HIGH"),
        (0.0, 14.0, 1.0, 80.10, "CRITICAL"),
    ],
)
def test_risk_tier_cutoffs(
    spark, load_ts, bureau_norm, ontime_pct, velocity_ratio, expected_score, expected_tier
):
    row = classify(spark, [(1, bureau_norm, ontime_pct, velocity_ratio, 0.0)], load_ts=load_ts)[1]

    assert float(row["COMPOSITE_RISK_SCORE"]) == expected_score
    assert row["RISK_TIER"] == expected_tier


def test_probability_of_default_is_rounded_and_null_safe(spark, load_ts):
    rows = classify(
        spark, [(1, 50.0, 100.0, 1.0, 0.12345678), (2, 50.0, 100.0, 1.0, None)], load_ts=load_ts
    )

    assert float(rows[1]["PROBABILITY_OF_DEFAULT"]) == 0.123457
    assert float(rows[2]["PROBABILITY_OF_DEFAULT"]) == 0.0


def test_watch_list_flag_needs_critical_and_a_high_probability(spark, load_ts):
    rows = classify(
        spark,
        [
            (1, 0.0, 0.0, 3.0, 0.51),  # CRITICAL and PD > 0.5
            (2, 0.0, 0.0, 3.0, 0.50),  # CRITICAL but PD exactly on the threshold
            (3, 100.0, 100.0, 1.0, 0.99),  # high PD but LOW tier
        ],
        load_ts=load_ts,
    )

    assert rows[1]["WATCH_LIST_FLAG"] == "Y"
    assert rows[2]["WATCH_LIST_FLAG"] == "N"
    assert rows[3]["WATCH_LIST_FLAG"] == "N"


def test_review_required_flag_needs_score_sixty_and_a_velocity_spike(spark, load_ts):
    rows = classify(
        spark,
        [
            (1, 0.0, 100.0, 3.0, 0.0),  # composite 65 and velocity ratio 3.0
            (2, 0.0, 100.0, 2.0, 0.0),  # velocity ratio exactly on the threshold
            (3, 50.0, 100.0, 5.0, 0.0),  # velocity spike but composite 37.00
        ],
        load_ts=load_ts,
    )

    assert rows[1]["REVIEW_REQUIRED_FLAG"] == "Y"
    assert rows[2]["REVIEW_REQUIRED_FLAG"] == "N"
    assert rows[3]["REVIEW_REQUIRED_FLAG"] == "N"


def test_metadata_columns_and_the_score_delta_placeholder(spark, load_ts):
    row = classify(spark, [(1, 50.0, 100.0, 1.0, 0.0)], load_ts=load_ts)[1]

    assert float(row["SCORE_DELTA_30D"]) == 0.00
    assert row["MODEL_VERSION"] == "RISK_V4.0"
    assert row["EFFECTIVE_DATE"] == RUN_DATE


def test_output_matches_the_ddl_contract(spark, load_ts):
    result = transform_risk_classified(
        scored_df(spark, [(1, 50.0, 100.0, 1.0, 0.0)]), run_date=RUN_DATE, load_ts=load_ts
    )

    assert result.columns == list(schemas.CUSTOMER_RISK_SCORES.column_names)


def test_recalibrated_constants_flow_through_the_transform(spark, load_ts):
    """The weights and cutoffs are configuration, not magic numbers."""

    risk = RiskScoringConstants(tier_low_max=100.0)
    row = classify(spark, [(1, 0.0, 0.0, 1.0, 0.0)], risk=risk, load_ts=load_ts)[1]

    assert float(row["COMPOSITE_RISK_SCORE"]) == 85.00
    assert row["RISK_TIER"] == "LOW"


def test_end_to_end_composition_on_in_memory_frames(risk_factors, customer_360, load_ts):
    result = transform_customer_risk_scores(
        risk_factors, customer_360, run_date=RUN_DATE, load_ts=load_ts
    )

    rows = by_id(result)
    assert sorted(rows) == [1, 3]
    assert result.columns == list(schemas.CUSTOMER_RISK_SCORES.column_names)
    # single-class target -> base rate of 0.0 for everyone
    assert {float(row["PROBABILITY_OF_DEFAULT"]) for row in rows.values()} == {0.0}
