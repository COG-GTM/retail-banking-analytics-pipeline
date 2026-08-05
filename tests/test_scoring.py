"""Unit tests for ``risk_scoring.scoring`` (STEP 4 of 03_sas_risk_scoring.sas).

Every case is a hand-traced SAS result: the clamps, the weighted composite, the
tier cutoffs, the ``do i = 1 to 4`` driver loop and the two flags.
"""

from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

from risk_scoring import schemas
from risk_scoring.config import PipelineConfig
from risk_scoring.scoring import classify_risk

MODEL_VERSION = "RISK_TEST_9.9"

#: A stale staging LOAD_TS, to prove STEP 4 overwrites it with the run timestamp.
STALE_LOAD_TS = dt.datetime(2001, 1, 1, 0, 0, 0)

INPUT_SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), False),
    StructField("BUREAU_SCORE_NORM", DoubleType(), True),
    StructField("PAYMENT_ONTIME_PCT", DoubleType(), True),
    StructField("VELOCITY_RATIO", DoubleType(), True),
    StructField("PROB_DEFAULT", DoubleType(), True),
    StructField("CREDIT_UTIL_RATIO", DoubleType(), True),
    StructField("LOAD_TS", TimestampType(), True),
])


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_scoring")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="module")
def config() -> PipelineConfig:
    return PipelineConfig(model_version=MODEL_VERSION)


def _input_row(
    customer_id: int,
    *,
    bureau_score_norm: float = 50.0,
    payment_ontime_pct: float = 100.0,
    velocity_ratio: float = 1.0,
    prob_default: float | None = 0.05,
) -> tuple:
    return (
        customer_id,
        bureau_score_norm,
        payment_ontime_pct,
        velocity_ratio,
        prob_default,
        0.25,
        STALE_LOAD_TS,
    )


def _score(spark: SparkSession, config: PipelineConfig, *rows: tuple) -> list[dict]:
    scored = classify_risk(spark.createDataFrame(list(rows), INPUT_SCHEMA), config)
    return [row.asDict() for row in scored.orderBy("CUSTOMER_ID").collect()]


def _only(spark: SparkSession, config: PipelineConfig, **kwargs) -> dict:
    return _score(spark, config, _input_row(1, **kwargs))[0]


# Inverse helpers: pick the input that produces a wanted component value, so the
# driver/tier cases read as component vectors rather than raw features.
def _bureau_norm_for(credit_component: float) -> float:
    return 100.0 - credit_component


def _ontime_for(behaviour_component: float) -> float:
    return 100.0 - behaviour_component


def _velocity_for(velocity_component: float) -> float:
    return 1.0 + velocity_component / 50.0


# --------------------------------------------------------------------------- #
# Component clamps                                                             #
# --------------------------------------------------------------------------- #


def test_credit_and_bureau_components_clamp_high_bureau_score(spark, config):
    """BUREAU_SCORE_NORM = 120: 100 - 120 = -20 -> 0, and 120 -> 100."""
    row = _only(spark, config, bureau_score_norm=120.0)
    assert row["CREDIT_RISK_COMPONENT"] == pytest.approx(0.0)
    assert row["BUREAU_SCORE_COMPONENT"] == pytest.approx(100.0)


def test_credit_and_bureau_components_clamp_negative_bureau_score(spark, config):
    """BUREAU_SCORE_NORM = -50: 100 - (-50) = 150 -> 100, and -50 -> 0."""
    row = _only(spark, config, bureau_score_norm=-50.0)
    assert row["CREDIT_RISK_COMPONENT"] == pytest.approx(100.0)
    assert row["BUREAU_SCORE_COMPONENT"] == pytest.approx(0.0)


def test_behaviour_and_history_components_clamp_high_ontime_pct(spark, config):
    """PAYMENT_ONTIME_PCT = 150: 100 - 150 = -50 -> 0, and 150 -> 100."""
    row = _only(spark, config, payment_ontime_pct=150.0)
    assert row["BEHAVIOUR_RISK_COMPONENT"] == pytest.approx(0.0)
    assert row["PAYMENT_HISTORY_COMPONENT"] == pytest.approx(100.0)


def test_behaviour_and_history_components_clamp_negative_ontime_pct(spark, config):
    """PAYMENT_ONTIME_PCT = -20: 100 - (-20) = 120 -> 100, and -20 -> 0."""
    row = _only(spark, config, payment_ontime_pct=-20.0)
    assert row["BEHAVIOUR_RISK_COMPONENT"] == pytest.approx(100.0)
    assert row["PAYMENT_HISTORY_COMPONENT"] == pytest.approx(0.0)


def test_velocity_component_clamps_above_one_hundred(spark, config):
    """VELOCITY_RATIO = 5: (5 - 1) * 50 = 200 -> 100."""
    row = _only(spark, config, velocity_ratio=5.0)
    assert row["VELOCITY_RISK_COMPONENT"] == pytest.approx(100.0)


def test_velocity_component_clamps_below_zero(spark, config):
    """VELOCITY_RATIO = 0.5: (0.5 - 1) * 50 = -25 -> 0."""
    row = _only(spark, config, velocity_ratio=0.5)
    assert row["VELOCITY_RISK_COMPONENT"] == pytest.approx(0.0)


def test_velocity_component_within_range_is_untouched(spark, config):
    row = _only(spark, config, velocity_ratio=1.6)
    assert row["VELOCITY_RISK_COMPONENT"] == pytest.approx(30.0)


# --------------------------------------------------------------------------- #
# Composite arithmetic                                                         #
# --------------------------------------------------------------------------- #


def test_composite_matches_hand_computed_row(spark, config):
    """BSN=70, POP=80, VR=1.6 -> components 30/20/30/70/80.

    30*0.30 + 20*0.25 + 30*0.15 + (100-70)*0.20 + (100-80)*0.10
    = 9 + 5 + 4.5 + 6 + 2 = 26.5
    """
    row = _only(spark, config, bureau_score_norm=70.0, payment_ontime_pct=80.0, velocity_ratio=1.6)
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(26.5)
    assert row["RISK_TIER"] == "MODERATE"


def test_composite_is_rounded_to_two_decimals(spark, config):
    """CUSTOMER_ID=3 of the reference extract: components 14.909.../0/11.113.../14.909...

    14.909090909090907*0.30 + 11.11348811676569*0.15 + 14.909090909090907*0.20
    = 9.121568672060307 -> 9.12
    """
    row = _only(
        spark,
        config,
        bureau_score_norm=_bureau_norm_for(14.909090909090907),
        payment_ontime_pct=100.0,
        velocity_ratio=_velocity_for(11.11348811676569),
    )
    assert row["COMPOSITE_RISK_SCORE"] == 9.12
    assert row["RISK_TIER"] == "LOW"


def test_composite_keeps_the_redundant_clamped_terms(spark, config):
    """At the clamp boundary the duplicated terms diverge, so they must not be folded.

    BSN=-50 -> CREDIT=100 but BUREAU_SCORE_COMPONENT=0, so (100-BUREAU)=100 too;
    POP=-20 -> BEHAVIOUR=100 and (100-PAYMENT_HISTORY)=100.
    100*0.30 + 100*0.25 + 0*0.15 + 100*0.20 + 100*0.10 = 85
    A "simplified" 0.5*CREDIT + 0.35*BEHAVIOUR form would agree here, so also
    check the asymmetric side: BSN=120 -> CREDIT=0, BUREAU=100, (100-BUREAU)=0.
    """
    high, low = _score(
        spark,
        config,
        _input_row(1, bureau_score_norm=-50.0, payment_ontime_pct=-20.0),
        _input_row(2, bureau_score_norm=120.0, payment_ontime_pct=150.0),
    )
    assert high["COMPOSITE_RISK_SCORE"] == pytest.approx(85.0)
    assert low["COMPOSITE_RISK_SCORE"] == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Risk tiers                                                                   #
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("credit", "behaviour", "velocity", "composite", "tier"),
    [
        (39.98, 0.0, 0.0, 19.99, "LOW"),
        (40.0, 0.0, 0.0, 20.0, "MODERATE"),
        (79.98, 0.0, 0.0, 39.99, "MODERATE"),
        (80.0, 0.0, 0.0, 40.0, "ELEVATED"),
        (80.0, 40.0, 39.8, 59.97, "ELEVATED"),
        (80.0, 40.0, 40.0, 60.0, "HIGH"),
        (100.0, 60.0, 59.8, 79.97, "HIGH"),
        (100.0, 60.0, 60.0, 80.0, "CRITICAL"),
        (100.0, 100.0, 100.0, 100.0, "CRITICAL"),
    ],
)
def test_tier_boundaries_belong_to_the_upper_tier(
    spark, config, credit, behaviour, velocity, composite, tier
):
    row = _only(
        spark,
        config,
        bureau_score_norm=_bureau_norm_for(credit),
        payment_ontime_pct=_ontime_for(behaviour),
        velocity_ratio=_velocity_for(velocity),
    )
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(composite)
    assert row["RISK_TIER"] == tier


# --------------------------------------------------------------------------- #
# Top-two risk drivers                                                         #
# --------------------------------------------------------------------------- #


def _drivers(spark, config, *, credit: float, behaviour: float, velocity: float) -> tuple:
    """Score one row described by its component vector and return the two drivers.

    The fourth SAS array entry, ``100 - BUREAU_SCORE_COMPONENT``, is not free:
    for an unclamped BUREAU_SCORE_NORM it always equals ``CREDIT_RISK_COMPONENT``.
    """
    row = _only(
        spark,
        config,
        bureau_score_norm=_bureau_norm_for(credit),
        payment_ontime_pct=_ontime_for(behaviour),
        velocity_ratio=_velocity_for(velocity),
    )
    return row["PRIMARY_RISK_DRIVER"], row["SECONDARY_RISK_DRIVER"]


def test_credit_bureau_tie_gives_primary_to_the_earlier_array_index(spark, config):
    """[40, 0, 0, 40]: strict `>` keeps index 1 as primary, index 4 takes secondary."""
    assert _drivers(spark, config, credit=40.0, behaviour=0.0, velocity=0.0) == (
        "CREDIT_UTILIZATION",
        "BUREAU_SCORE",
    )


def test_all_components_equal_and_non_zero(spark, config):
    """[50, 50, 50, 50]: only the first two comparisons ever fire."""
    assert _drivers(spark, config, credit=50.0, behaviour=50.0, velocity=50.0) == (
        "CREDIT_UTILIZATION",
        "PAYMENT_BEHAVIOUR",
    )


def test_all_components_zero_leaves_both_drivers_unset(spark, config):
    """[0, 0, 0, 0]: `_max1`/`_max2` start at 0, so no strict `>` ever fires."""
    assert _drivers(spark, config, credit=0.0, behaviour=0.0, velocity=0.0) == (None, None)


def test_single_non_zero_component_leaves_secondary_unset(spark, config):
    """[0, 0, 50, 0]: only PRIMARY_RISK_DRIVER is assigned."""
    assert _drivers(spark, config, credit=0.0, behaviour=0.0, velocity=50.0) == (
        "TRANSACTION_VELOCITY",
        None,
    )


def test_two_non_zero_components_order_by_magnitude(spark, config):
    """[30, 70, 0, 30]: behaviour wins primary, credit beats the tied bureau entry."""
    assert _drivers(spark, config, credit=30.0, behaviour=70.0, velocity=0.0) == (
        "PAYMENT_BEHAVIOUR",
        "CREDIT_UTILIZATION",
    )


def test_velocity_only_beats_a_tied_credit_bureau_pair(spark, config):
    """CUSTOMER_ID=5 of the reference extract: [40.909..., 0, 81.284..., 40.909...].

    Index 4 ties index 1's value but loses the strict `>` against `_max2`.
    """
    assert _drivers(
        spark,
        config,
        credit=40.90909090909091,
        behaviour=0.0,
        velocity=81.28429109195974,
    ) == ("TRANSACTION_VELOCITY", "CREDIT_UTILIZATION")


def test_hand_traced_reference_row_three(spark, config):
    """CUSTOMER_ID=3 of the reference extract: [14.909..., 0, 11.113..., 14.909...]."""
    assert _drivers(
        spark,
        config,
        credit=14.909090909090907,
        behaviour=0.0,
        velocity=11.11348811676569,
    ) == ("CREDIT_UTILIZATION", "BUREAU_SCORE")


def test_clamped_bureau_component_breaks_the_credit_tie(spark, config):
    """BSN=120 -> CREDIT=0 while (100 - BUREAU_SCORE_COMPONENT)=0 as well.

    BSN=-50 -> CREDIT=100 and (100 - BUREAU_SCORE_COMPONENT)=100, still tied, so
    the only clamp-driven divergence is downward; assert both ends explicitly.
    """
    clamped_high, clamped_low = _score(
        spark,
        config,
        _input_row(1, bureau_score_norm=120.0, payment_ontime_pct=90.0),
        _input_row(2, bureau_score_norm=-50.0, payment_ontime_pct=90.0),
    )
    assert clamped_high["PRIMARY_RISK_DRIVER"] == "PAYMENT_BEHAVIOUR"
    assert clamped_high["SECONDARY_RISK_DRIVER"] is None
    assert clamped_low["PRIMARY_RISK_DRIVER"] == "CREDIT_UTILIZATION"
    assert clamped_low["SECONDARY_RISK_DRIVER"] == "BUREAU_SCORE"


# --------------------------------------------------------------------------- #
# Probability, flags and metadata                                              #
# --------------------------------------------------------------------------- #


def test_probability_of_default_is_rounded_to_six_decimals(spark, config):
    row = _only(spark, config, prob_default=0.12345678)
    assert row["PROBABILITY_OF_DEFAULT"] == 0.123457


def test_null_probability_of_default_becomes_zero(spark, config):
    row = _only(spark, config, prob_default=None)
    assert row["PROBABILITY_OF_DEFAULT"] == 0.0


@pytest.mark.parametrize(
    ("credit", "behaviour", "velocity", "prob_default", "expected"),
    [
        (100.0, 60.0, 60.0, 0.5, "N"),  # CRITICAL, strict `>` fails at exactly 0.5
        (100.0, 60.0, 60.0, 0.51, "Y"),  # CRITICAL and above the threshold
        (80.0, 40.0, 40.0, 0.9, "N"),  # HIGH, so the tier test fails
    ],
)
def test_watch_list_flag(spark, config, credit, behaviour, velocity, prob_default, expected):
    row = _only(
        spark,
        config,
        bureau_score_norm=_bureau_norm_for(credit),
        payment_ontime_pct=_ontime_for(behaviour),
        velocity_ratio=_velocity_for(velocity),
        prob_default=prob_default,
    )
    assert row["WATCH_LIST_FLAG"] == expected


def test_review_required_flag_needs_velocity_strictly_above_two(spark, config):
    """BSN=30, POP=50, VR=2.0 -> composite 35 + 17.5 + 7.5 = 60, but VR is not > 2."""
    row = _only(spark, config, bureau_score_norm=30.0, payment_ontime_pct=50.0, velocity_ratio=2.0)
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(60.0)
    assert row["REVIEW_REQUIRED_FLAG"] == "N"


def test_review_required_flag_set_when_both_conditions_hold(spark, config):
    """VR=2.01 -> velocity component 50.5, composite 60.07(5) and VR > 2.

    ``(2.01 - 1) * 50`` is ``50.49999999999999`` in IEEE-754, so the weighted sum
    lands just below ``60.075`` and rounds down — SAS computes the same doubles.
    """
    row = _only(spark, config, bureau_score_norm=30.0, payment_ontime_pct=50.0, velocity_ratio=2.01)
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(60.07)
    assert row["REVIEW_REQUIRED_FLAG"] == "Y"


def test_review_required_flag_needs_composite_at_least_sixty(spark, config):
    """VR=3 alone is not enough: composite 29.99 stays below the cutoff."""
    row = _only(
        spark,
        config,
        bureau_score_norm=_bureau_norm_for(29.98),
        payment_ontime_pct=100.0,
        velocity_ratio=3.0,
    )
    assert row["COMPOSITE_RISK_SCORE"] == pytest.approx(29.99)
    assert row["REVIEW_REQUIRED_FLAG"] == "N"


def test_metadata_columns(spark, config):
    row = _only(spark, config)
    assert row["SCORE_DELTA_30D"] == 0.0
    assert row["MODEL_VERSION"] == MODEL_VERSION
    assert row["EFFECTIVE_DATE"] == dt.date.today()
    assert row["LOAD_TS"] > STALE_LOAD_TS


# --------------------------------------------------------------------------- #
# Output contract                                                              #
# --------------------------------------------------------------------------- #


def test_output_columns_match_the_target_contract(spark, config):
    scored = classify_risk(
        spark.createDataFrame([_input_row(1)], INPUT_SCHEMA), PipelineConfig()
    )
    assert tuple(scored.columns) == schemas.CUSTOMER_RISK_SCORES_COLUMNS


def test_numeric_output_columns_stay_double(spark, config):
    scored = classify_risk(spark.createDataFrame([_input_row(1)], INPUT_SCHEMA), config)
    types = dict(scored.dtypes)
    for column in (
        "COMPOSITE_RISK_SCORE",
        "PROBABILITY_OF_DEFAULT",
        "CREDIT_RISK_COMPONENT",
        "BEHAVIOUR_RISK_COMPONENT",
        "VELOCITY_RISK_COMPONENT",
        "BUREAU_SCORE_COMPONENT",
        "PAYMENT_HISTORY_COMPONENT",
        "SCORE_DELTA_30D",
    ):
        assert types[column] == "double", column
    assert types["EFFECTIVE_DATE"] == "date"
    assert types["LOAD_TS"] == "timestamp"


def test_row_count_is_preserved_and_rows_are_independent(spark, config):
    rows = _score(
        spark,
        config,
        _input_row(1, bureau_score_norm=70.0, payment_ontime_pct=80.0, velocity_ratio=1.6),
        _input_row(2, bureau_score_norm=20.0, payment_ontime_pct=40.0, velocity_ratio=2.2),
    )
    assert [row["CUSTOMER_ID"] for row in rows] == [1, 2]
    assert rows[0]["COMPOSITE_RISK_SCORE"] == pytest.approx(26.5)
    assert rows[1]["COMPOSITE_RISK_SCORE"] == pytest.approx(70.0)
    assert rows[1]["RISK_TIER"] == "HIGH"
