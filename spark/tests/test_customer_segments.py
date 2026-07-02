"""Tests for the customer segmentation job (01)."""
from __future__ import annotations

import dataclasses
import math

from pyspark.sql import functions as F

from spark.jobs.customer_segments import (
    OUTPUT_COLUMNS,
    build_customer_segments,
    engineer_features,
)

COLUMNS = [
    "customer_id", "customer_status", "has_checking", "has_savings",
    "has_credit", "has_loan", "tenure_months", "age", "total_balance",
    "num_active_accounts", "num_accounts", "credit_utilization_pct",
]


def _rows(spark, n=12):
    data = []
    for i in range(1, n + 1):
        data.append((
            i, "A", "Y", "Y" if i % 2 else "N", "N", "N",
            float(6 + i * 12), float(20 + i * 3), float(i * 5000),
            float(min(i, 3)), 3.0, float(i * 5 % 100),
        ))
    # one inactive customer that must be filtered out
    data.append((99, "C", "Y", "Y", "Y", "Y", 120.0, 45.0, 9000.0, 2.0, 3.0, 10.0))
    return spark.createDataFrame(data, COLUMNS)


def test_engineer_features_derivations(spark):
    df = _rows(spark, 3)
    out = {r["customer_id"]: r for r in engineer_features(df).collect()}

    # inactive customer removed
    assert 99 not in out

    r1 = out[1]
    # product_breadth: checking=Y, savings=Y (odd i), credit=N, loan=N -> 2/4
    assert math.isclose(r1["product_breadth"], 0.5)
    # log_balance = ln(max(balance, 1))
    assert math.isclose(r1["log_balance"], math.log(5000.0))
    # acct_ratio = num_active / max(num_accounts, 1) = 1/3
    assert math.isclose(r1["acct_ratio"], 1.0 / 3.0)
    assert r1["age_group"] in {"GEN_Z", "MILLENNIAL", "GEN_X", "BOOMER", "SILENT"}


def test_build_outputs_contract_and_derived(spark, config):
    cfg = dataclasses.replace(config, n_clusters=3)
    df = _rows(spark, 12)
    result = build_customer_segments(df, cfg)

    # exact output contract
    assert result.columns == OUTPUT_COLUMNS
    # active customers only (12), inactive dropped
    assert result.count() == 12
    # segment ids are 1-based and within k
    ids = [r["segment_id"] for r in result.collect()]
    assert all(1 <= s <= cfg.n_clusters for s in ids)
    # segment names come from the configured label set
    names = {r["segment_name"] for r in result.collect()}
    from spark.jobs.customer_segments import SEGMENT_NAMES
    assert names.issubset(set(SEGMENT_NAMES))
    # engagement_score = round(acct_ratio * 100, 2); acct_ratio 1/3 -> 33.33
    eng = result.where(F.col("customer_id") == 1).first()["engagement_score"]
    assert math.isclose(eng, 33.33, abs_tol=0.01)
    # model + effective date come from config
    row = result.first()
    assert row["model_version"] == cfg.seg_model_version
    assert row["effective_date"] == cfg.effective_date
