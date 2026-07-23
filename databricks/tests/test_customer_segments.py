"""Ticket 7 - Customer segmentation (feature engineering + MLlib clustering)."""
from __future__ import annotations

from datetime import date, datetime

from jobs.customer_segments import (
    SEGMENT_LABELS,
    build_customer_segments,
    engineer_features,
)
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_C360_SCHEMA = (
    "customer_id long, customer_status string, age smallint, tenure_months int, "
    "credit_utilization_pct decimal(5,2), total_balance decimal(18,2), "
    "num_accounts smallint, num_active_accounts smallint, has_checking string, "
    "has_savings string, has_credit string, has_loan string"
)


def _c360(spark, rows):
    return make_df(spark, _C360_SCHEMA, rows)


def test_feature_engineering(spark):
    rows = [
        (1, "A", 30, 6, 20, 5000, 4, 2, "Y", "Y", "Y", "Y"),   # all 4 products
        (2, "I", 40, 6, 20, 5000, 2, 2, "Y", "N", "N", "N"),   # inactive -> dropped
    ]
    feats = {r.customer_id: r for r in engineer_features(_c360(spark, rows)).collect()}
    assert set(feats) == {1}                       # only active customers
    f = feats[1]
    assert f.product_breadth == 1.0                # 4/4 products
    assert f.tenure_group == "NEW (<1yr)"          # 6 months
    assert f.age_group == "MILLENNIAL"             # 30
    assert f.acct_ratio == 0.5                      # 2 active / 4 accounts
    assert float(f.digital_adoption_score) == 0.0  # SAS placeholder


def test_balance_tier_boundaries(spark):
    rows = [
        (1, "A", 30, 6, 0, 500, 1, 1, "Y", "N", "N", "N"),       # LOW
        (2, "A", 30, 6, 0, 5000, 1, 1, "Y", "N", "N", "N"),      # MODERATE
        (3, "A", 30, 6, 0, 50000, 1, 1, "Y", "N", "N", "N"),     # AFFLUENT
        (4, "A", 30, 6, 0, 500000, 1, 1, "Y", "N", "N", "N"),    # HIGH_NET_WORTH
    ]
    tiers = {r.customer_id: r.balance_tier for r in engineer_features(_c360(spark, rows)).collect()}
    assert tiers == {1: "LOW", 2: "MODERATE", 3: "AFFLUENT", 4: "HIGH_NET_WORTH"}


def test_kmeans_produces_five_valid_segments(spark):
    # Enough spread-out customers for KMeans(k=5) to converge.
    rows = []
    for i in range(1, 26):
        rows.append((
            i, "A", 25 + i, 6 * i, (i % 5) * 5, float(i * 2000),
            4, (i % 4) + 1, "Y", "Y" if i % 2 else "N",
            "Y" if i % 3 else "N", "Y" if i % 4 else "N",
        ))
    out = build_customer_segments(_c360(spark, rows), RUN_DATE, LOAD_TS)
    collected = out.collect()
    assert len(collected) == 25
    assert {r.segment_name for r in collected} <= set(SEGMENT_LABELS)
    assert {r.segment_id for r in collected} <= {0, 1, 2, 3, 4}
    assert all(r.channel_preference == "" for r in collected)   # SAS placeholder
    assert all(r.model_version == "SEG_V3.2" for r in collected)
