"""Unit tests for the CUSTOMER_RISK_SCORES transforms (SAS 03)."""

from __future__ import annotations

import datetime as _dt

import pytest
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StructField,
    StructType,
)

from common.config import PipelineConfig
from jobs import dp_risk_scoring as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)

_FEATURE_SCHEMA = StructType([
    StructField("customer_id", LongType(), False),
    StructField("bureau_score_norm", DoubleType(), True),
    StructField("payment_ontime_pct", DoubleType(), True),
    StructField("velocity_ratio", DoubleType(), True),
])


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _features(spark, rows):
    """Minimal feature frame carrying only what score_and_classify reads."""
    return spark.createDataFrame(
        [(r["customer_id"], r["bureau_score_norm"], r["payment_ontime_pct"], r["velocity_ratio"]) for r in rows],
        schema=_FEATURE_SCHEMA,
    )


def _prob(spark, mapping):
    schema = StructType([
        StructField("customer_id", LongType(), False),
        StructField("prob_default", DoubleType(), True),
    ])
    return spark.createDataFrame([(k, v) for k, v in mapping.items()], schema=schema)


def _score(spark, cfg, rows, probs=None):
    feats = _features(spark, rows)
    ids = [r["customer_id"] for r in rows]
    prob = _prob(spark, probs or {i: 0.0 for i in ids})
    out = job.score_and_classify(feats, prob, cfg)
    return {r.customer_id: r for r in out.collect()}


# --------------------------------------------------------------------------- #
# STEP 4 -- composite arithmetic, clamping, tiers, drivers, flags             #
# --------------------------------------------------------------------------- #
def test_composite_exact_known_input(spark, cfg):
    # credit=60, behaviour=20, velocity=100, (100-bureau)=60, (100-ph)=20
    # composite = 60*.30 + 20*.25 + 100*.15 + 60*.20 + 20*.10 = 52.00
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
    ])[1]
    assert float(row.composite_risk_score) == 52.00
    assert row.risk_tier == "ELEVATED"
    assert row.credit_risk_component == 60.0
    assert row.behaviour_risk_component == 20.0
    assert row.velocity_risk_component == 100.0
    assert row.bureau_score_component == 40.0
    assert row.payment_history_component == 80.0
    # drivers: [CU=60, PB=20, TV=100, BS=60] -> primary TV, secondary CU
    assert row.primary_risk_driver == "TRANSACTION_VELOCITY"
    assert row.secondary_risk_driver == "CREDIT_UTILIZATION"


def test_component_clamping(spark, cfg):
    # bureau_score_norm > 100 and payment_ontime_pct > 100 must clamp to [0,100];
    # velocity_ratio < 1 => negative raw velocity clamps up to 0.
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 150.0, "payment_ontime_pct": 120.0, "velocity_ratio": 0.2},
    ])[1]
    assert row.credit_risk_component == 0.0        # clamp(100-150)
    assert row.behaviour_risk_component == 0.0      # clamp(100-120)
    assert row.velocity_risk_component == 0.0        # clamp((0.2-1)*50)
    assert row.bureau_score_component == 100.0       # clamp(150)
    assert row.payment_history_component == 100.0    # clamp(120)


@pytest.mark.parametrize("x,tier", [
    (20.0, "MODERATE"),
    (40.0, "ELEVATED"),
    (60.0, "HIGH"),
    (80.0, "CRITICAL"),
])
def test_tier_boundaries_are_upper_bucket(spark, cfg, x, tier):
    # All five weighted component-values equal x => composite == x exactly,
    # so a boundary lands in the upper bucket ("< cutoff" is the lower tier).
    bnorm = 100.0 - x
    pomt = 100.0 - x
    vr = 1.0 + x / 50.0
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": bnorm, "payment_ontime_pct": pomt, "velocity_ratio": vr},
    ])[1]
    assert float(row.composite_risk_score) == x
    assert row.risk_tier == tier


def test_low_tier_below_20(spark, cfg):
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 90.0, "payment_ontime_pct": 90.0, "velocity_ratio": 1.0},
    ])[1]
    # credit=10, behaviour=10, velocity=0, (100-bureau)=10, (100-ph)=10
    # composite = 10*.30 + 10*.25 + 0*.15 + 10*.20 + 10*.10 = 8.50 -> LOW
    assert float(row.composite_risk_score) == 8.50
    assert row.risk_tier == "LOW"


def test_driver_tie_first_max_wins(spark, cfg):
    # components [CU=15, PB=0, TV=11, BS=15]: CU and BS tie at 15; first-max
    # (CU) is primary, and BS (later, > runner-up) becomes secondary.
    # bnorm=85 -> credit=15, bureau_component=85 -> (100-85)=15 (BS driver)
    # pomt=100 -> behaviour=0 ; velocity_ratio: (vr-1)*50=11 -> vr=1.22
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 85.0, "payment_ontime_pct": 100.0, "velocity_ratio": 1.22},
    ])[1]
    assert row.primary_risk_driver == "CREDIT_UTILIZATION"
    assert row.secondary_risk_driver == "BUREAU_SCORE"


def test_watch_list_flag_requires_critical_and_high_pd(spark, cfg):
    rows = [
        {"customer_id": 1, "bureau_score_norm": 0.0, "payment_ontime_pct": 0.0, "velocity_ratio": 3.0},  # CRITICAL
        {"customer_id": 2, "bureau_score_norm": 0.0, "payment_ontime_pct": 0.0, "velocity_ratio": 3.0},  # CRITICAL
    ]
    out = _score(spark, cfg, rows, probs={1: 0.6, 2: 0.4})
    assert out[1].risk_tier == "CRITICAL" and out[1].watch_list_flag == "Y"  # pd 0.6 > 0.5
    assert out[2].risk_tier == "CRITICAL" and out[2].watch_list_flag == "N"  # pd 0.4


def test_watch_list_not_set_when_not_critical(spark, cfg):
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
    ], probs={1: 0.9})[1]
    assert row.risk_tier == "ELEVATED" and row.watch_list_flag == "N"


def test_review_required_flag(spark, cfg):
    rows = [
        # composite 60 (HIGH) and velocity_ratio 2.2 > 2.0 -> Y
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 40.0, "velocity_ratio": 2.2},
        # composite >= 60 but velocity_ratio 2.0 (not > 2.0) -> N
        {"customer_id": 2, "bureau_score_norm": 40.0, "payment_ontime_pct": 40.0, "velocity_ratio": 2.0},
    ]
    out = _score(spark, cfg, rows)
    assert out[1].review_required_flag == "Y"
    assert out[2].review_required_flag == "N"


def test_probability_rounded_and_coalesced(spark, cfg):
    out = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
        {"customer_id": 2, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
    ], probs={1: 0.12345678, 2: None})
    assert float(out[1].probability_of_default) == 0.123457   # rounded to 6 dp
    assert float(out[2].probability_of_default) == 0.0        # NULL -> 0


def test_static_columns(spark, cfg):
    row = _score(spark, cfg, [
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
    ])[1]
    assert row.model_version == "RISK_V4.0"
    assert row.effective_date == RUN_DATE
    assert float(row.score_delta_30d) == 0.0


def test_output_schema_matches_ddl(spark, cfg):
    from common import schemas
    feats = _features(spark, [
        {"customer_id": 1, "bureau_score_norm": 40.0, "payment_ontime_pct": 80.0, "velocity_ratio": 3.0},
    ])
    out = job.score_and_classify(feats, _prob(spark, {1: 0.0}), cfg)
    assert out.columns == schemas.CUSTOMER_RISK_SCORES.column_names


# --------------------------------------------------------------------------- #
# STEP 2 -- feature prep: bureau imputation/normalisation, ratio guards        #
# --------------------------------------------------------------------------- #
def _prepared(spark, row):
    df = fx.stg_risk_factors(spark, [row])
    return job.prepare_features(df).collect()[0]


def test_bureau_imputation_missing(spark):
    # NULL and <=0 both impute to 680 -> norm = (680-300)/550*100
    for score in (None, 0, -5):
        r = _prepared(spark, {"customer_id": 1, "external_credit_score": score})
        assert r.external_credit_score_imp == 680.0
        assert abs(r.bureau_score_norm - (380.0 / 550.0 * 100.0)) < 1e-9


def test_bureau_normalisation_bounds(spark):
    lo = _prepared(spark, {"customer_id": 1, "external_credit_score": 300})
    hi = _prepared(spark, {"customer_id": 1, "external_credit_score": 850})
    assert abs(lo.bureau_score_norm - 0.0) < 1e-9
    assert abs(hi.bureau_score_norm - 100.0) < 1e-9


def test_balance_trend_ratio_guard(spark):
    guarded = _prepared(spark, {"customer_id": 1, "avg_daily_balance_30d": 500, "avg_daily_balance_90d": 0})
    assert guarded.balance_trend_ratio == 1.0
    normal = _prepared(spark, {"customer_id": 1, "avg_daily_balance_30d": 120, "avg_daily_balance_90d": 100})
    assert abs(normal.balance_trend_ratio - 1.2) < 1e-9


def test_velocity_ratio_guard(spark):
    guarded = _prepared(spark, {"customer_id": 1, "debit_velocity_7d": 50, "debit_velocity_30d": 0})
    assert guarded.velocity_ratio == 1.0
    normal = _prepared(spark, {"customer_id": 1, "debit_velocity_7d": 100, "debit_velocity_30d": 300})
    assert abs(normal.velocity_ratio - (100.0 * (30.0 / 7.0) / 300.0)) < 1e-9


def test_default_flag_threshold(spark):
    assert _prepared(spark, {"customer_id": 1, "payment_late_cnt": 3}).default_flag == 1
    assert _prepared(spark, {"customer_id": 1, "payment_late_cnt": 2}).default_flag == 0


# --------------------------------------------------------------------------- #
# STEP 1 -- extract only active customers                                      #
# --------------------------------------------------------------------------- #
def test_extract_inner_join_active_only(spark):
    rf = fx.stg_risk_factors(spark, [
        {"customer_id": 1}, {"customer_id": 2}, {"customer_id": 3},
    ])
    c360 = fx.stg_customer_360(spark, [
        {"customer_id": 1, "customer_status": "A", "tenure_months": 12},
        {"customer_id": 2, "customer_status": "I", "tenure_months": 5},   # inactive dropped
        # customer 3 absent from 360 -> inner join drops it
    ])
    out = job.extract_risk_input(rf, c360)
    assert {r.customer_id for r in out.collect()} == {1}
