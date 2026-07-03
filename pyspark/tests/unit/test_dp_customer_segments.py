"""Unit tests for the CUSTOMER_SEGMENTS transforms (SAS 01_sas_customer_segments).

The KMeans step is intentionally excluded from these pure-logic tests; the
label-assignment rule is exercised directly via :func:`segment_labels` on
synthetic cluster profiles.
"""

from __future__ import annotations

import datetime as _dt
import math
from decimal import Decimal

import pytest

from common import schemas
from common.config import PipelineConfig
from jobs import dp_customer_segments as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _stg(spark, rows):
    """Build an STG_CUSTOMER_360 DataFrame from partial dicts (status defaults A)."""
    for r in rows:
        r.setdefault("customer_status", "A")
    return fx.make_df(spark, schemas.STG_CUSTOMER_360, rows)


# --------------------------------------------------------------------------- #
# Feature engineering                                                          #
# --------------------------------------------------------------------------- #
def test_filters_to_active_customers(spark, cfg):
    out = job.engineer_features(
        _stg(spark, [
            {"customer_id": 1, "customer_status": "A"},
            {"customer_id": 2, "customer_status": "I"},
            {"customer_id": 3, "customer_status": "C"},
        ]),
        cfg,
    )
    assert {r.customer_id for r in out.collect()} == {1}


@pytest.mark.parametrize("flags,expected", [
    (("N", "N", "N", "N"), 0.0),
    (("Y", "N", "N", "N"), 0.25),
    (("Y", "Y", "N", "N"), 0.50),
    (("Y", "Y", "Y", "Y"), 1.0),
])
def test_product_breadth(spark, cfg, flags, expected):
    hc, hs, hcr, hl = flags
    out = job.engineer_features(_stg(spark, [
        {"customer_id": 1, "has_checking": hc, "has_savings": hs,
         "has_credit": hcr, "has_loan": hl},
    ]), cfg)
    assert out.collect()[0].product_breadth == pytest.approx(expected)


@pytest.mark.parametrize("tenure,group", [
    (0, "NEW (<1yr)"), (11, "NEW (<1yr)"),
    (12, "DEVELOPING (1-3yr)"), (35, "DEVELOPING (1-3yr)"),
    (36, "ESTABLISHED (3-7yr)"), (83, "ESTABLISHED (3-7yr)"),
    (84, "LOYAL (7yr+)"), (240, "LOYAL (7yr+)"),
])
def test_tenure_group(spark, cfg, tenure, group):
    out = job.engineer_features(_stg(spark, [
        {"customer_id": 1, "tenure_months": tenure},
    ]), cfg)
    assert out.collect()[0].tenure_group == group


@pytest.mark.parametrize("age,group", [
    (18, "GEN_Z"), (24, "GEN_Z"),
    (25, "MILLENNIAL"), (40, "MILLENNIAL"),
    (41, "GEN_X"), (56, "GEN_X"),
    (57, "BOOMER"), (75, "BOOMER"),
    (76, "SILENT"), (95, "SILENT"),
])
def test_age_group(spark, cfg, age, group):
    out = job.engineer_features(_stg(spark, [
        {"customer_id": 1, "age": age},
    ]), cfg)
    assert out.collect()[0].age_group == group


@pytest.mark.parametrize("balance,tier", [
    ("0.00", "LOW"), ("999.99", "LOW"),
    ("1000.00", "MODERATE"), ("9999.99", "MODERATE"),
    ("10000.00", "AFFLUENT"), ("99999.99", "AFFLUENT"),
    ("100000.00", "HIGH_NET_WORTH"), ("5000000.00", "HIGH_NET_WORTH"),
])
def test_balance_tier(spark, cfg, balance, tier):
    out = job.engineer_features(_stg(spark, [
        {"customer_id": 1, "total_balance": Decimal(balance)},
    ]), cfg)
    assert out.collect()[0].balance_tier == tier


def test_log_balance_and_acct_ratio(spark, cfg):
    out = job.engineer_features(_stg(spark, [
        # balance 0 -> log(max(0,1)) = log(1) = 0; ratio 2/4 = 0.5
        {"customer_id": 1, "total_balance": Decimal("0.00"),
         "num_accounts": 4, "num_active_accounts": 2},
        # balance e^3; ratio 3/3 = 1.0
        {"customer_id": 2, "total_balance": Decimal(str(round(math.e ** 3, 2))),
         "num_accounts": 3, "num_active_accounts": 3},
    ]), cfg)
    rows = {r.customer_id: r for r in out.collect()}
    assert rows[1].log_balance == pytest.approx(0.0)
    assert rows[1].acct_ratio == pytest.approx(0.5)
    assert rows[2].log_balance == pytest.approx(3.0, abs=1e-3)
    assert rows[2].acct_ratio == pytest.approx(1.0)


def test_acct_ratio_guards_zero_accounts(spark, cfg):
    out = job.engineer_features(_stg(spark, [
        {"customer_id": 1, "num_accounts": 0, "num_active_accounts": 0},
    ]), cfg)
    # divisor is max(num_accounts, 1) so this never divides by zero.
    assert out.collect()[0].acct_ratio == pytest.approx(0.0)


# --------------------------------------------------------------------------- #
# Cluster labelling (ordered by avg log balance DESC)                         #
# --------------------------------------------------------------------------- #
def _profiles(spark, rows):
    return spark.createDataFrame(rows, "cluster int, log_balance double")


def test_segment_labels_orders_by_avg_balance(spark):
    # cluster 3 highest avg balance -> PREMIUM_WEALTH; cluster 1 lowest -> VALUE_BASIC.
    df = _profiles(spark, [
        (0, 5.0), (0, 5.0),   # avg 5.0 -> rank 3
        (1, 1.0),             # avg 1.0 -> rank 5
        (2, 3.0), (2, 3.0),   # avg 3.0 -> rank 4
        (3, 9.0), (3, 7.0),   # avg 8.0 -> rank 1 (highest)
        (4, 6.0),             # avg 6.0 -> rank 2
    ])
    labels = {r.cluster: r.segment_name for r in job.segment_labels(df).collect()}
    assert labels == {
        3: "PREMIUM_WEALTH",
        4: "ENGAGED_MAINSTREAM",
        0: "GROWING_DIGITAL",
        2: "CREDIT_DEPENDENT",
        1: "VALUE_BASIC",
    }


def test_segment_labels_subsegment_is_zero(spark):
    df = _profiles(spark, [(0, 2.0), (1, 1.0)])
    subs = {r.subsegment_id for r in job.segment_labels(df).collect()}
    assert subs == {0}


# --------------------------------------------------------------------------- #
# Scores + action flags (SAS STEP 6)                                          #
# --------------------------------------------------------------------------- #
_LABELLED_SCHEMA = (
    "customer_id long, cluster int, segment_name string, subsegment_id int, "
    "log_balance double, tenure_months int, product_breadth double, "
    "acct_ratio double, digital_adoption_score double, tenure_group string, "
    "age_group string, balance_tier string"
)


def _labelled(spark, rows):
    base = {
        "customer_id": 1, "cluster": 0, "segment_name": "PREMIUM_WEALTH",
        "subsegment_id": 0, "log_balance": 1.0, "tenure_months": 12,
        "product_breadth": 0.5, "acct_ratio": 0.5, "digital_adoption_score": 0.0,
        "tenure_group": "DEVELOPING (1-3yr)", "age_group": "GEN_X",
        "balance_tier": "MODERATE",
    }
    materialised = [{**base, **r} for r in rows]
    cols = [c.split()[0] for c in _LABELLED_SCHEMA.split(", ")]
    tuples = [tuple(m[c] for c in cols) for m in materialised]
    return spark.createDataFrame(tuples, _LABELLED_SCHEMA)


def test_scores_rounding(spark, cfg):
    df = _labelled(spark, [
        {"customer_id": 1, "log_balance": 2.0, "tenure_months": 10,
         "product_breadth": 0.25, "acct_ratio": 0.3333},
    ])
    row = job.build_output(df, cfg).collect()[0]
    # 2.0 * 10 * 0.25 * 10 = 50.00
    assert row.lifetime_value_score == Decimal("50.00")
    # 0.3333 * 100 = 33.33
    assert row.engagement_score == Decimal("33.33")
    # 0.25 * 100 = 25.00
    assert row.product_breadth_index == Decimal("25.00")
    assert row.digital_adoption_score == Decimal("0.00")


def test_cross_sell_flag(spark, cfg):
    df = _labelled(spark, [
        # low breadth (<0.5) AND good ratio (>=0.75) -> Y
        {"customer_id": 1, "product_breadth": 0.25, "acct_ratio": 0.80},
        # low breadth but weak ratio -> N
        {"customer_id": 2, "product_breadth": 0.25, "acct_ratio": 0.50},
        # good ratio but high breadth -> N
        {"customer_id": 3, "product_breadth": 0.75, "acct_ratio": 0.90},
        # boundary: breadth exactly 0.50 is NOT < 0.50 -> N
        {"customer_id": 4, "product_breadth": 0.50, "acct_ratio": 0.90},
    ])
    flags = {r.customer_id: r.cross_sell_flag for r in job.build_output(df, cfg).collect()}
    assert flags == {1: "Y", 2: "N", 3: "N", 4: "N"}


def test_upsell_flag(spark, cfg):
    df = _labelled(spark, [
        # MODERATE + non-NEW -> Y
        {"customer_id": 1, "balance_tier": "MODERATE", "tenure_group": "DEVELOPING (1-3yr)"},
        # MODERATE but NEW -> N
        {"customer_id": 2, "balance_tier": "MODERATE", "tenure_group": "NEW (<1yr)"},
        # not MODERATE -> N
        {"customer_id": 3, "balance_tier": "AFFLUENT", "tenure_group": "LOYAL (7yr+)"},
    ])
    flags = {r.customer_id: r.upsell_flag for r in job.build_output(df, cfg).collect()}
    assert flags == {1: "Y", 2: "N", 3: "N"}


def test_retention_risk_flag(spark, cfg):
    df = _labelled(spark, [
        # low ratio (<0.5) AND long tenure (>=60) -> Y
        {"customer_id": 1, "acct_ratio": 0.40, "tenure_months": 60},
        # low ratio but short tenure -> N
        {"customer_id": 2, "acct_ratio": 0.40, "tenure_months": 59},
        # long tenure but good ratio -> N
        {"customer_id": 3, "acct_ratio": 0.50, "tenure_months": 120},
    ])
    flags = {r.customer_id: r.retention_risk_flag for r in job.build_output(df, cfg).collect()}
    assert flags == {1: "Y", 2: "N", 3: "N"}


def test_build_output_static_columns(spark, cfg):
    row = job.build_output(_labelled(spark, [{"customer_id": 7, "cluster": 3}]), cfg).collect()[0]
    assert row.customer_id == 7
    assert row.segment_id == 3            # segment_id == raw cluster id
    assert row.model_version == "SEG_V3.2"
    assert row.effective_date == RUN_DATE
    assert row.channel_preference == ""


def test_build_output_schema_matches_ddl(spark, cfg):
    out = job.build_output(_labelled(spark, [{"customer_id": 1}]), cfg)
    schemas.assert_schema(out, schemas.CUSTOMER_SEGMENTS)
