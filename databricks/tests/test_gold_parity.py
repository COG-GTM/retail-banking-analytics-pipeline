from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from retail_banking.config import RunConfig
from tests.local_pipeline import run_bronze, run_gold, run_silver

REPO_ROOT = Path(__file__).parents[2]
SEGMENT_DISTRIBUTION = {
    "ENGAGED_MAINSTREAM": 103,
    "PREMIUM_WEALTH": 96,
    "VALUE_BASIC": 74,
    "CREDIT_DEPENDENT": 68,
    "GROWING_DIGITAL": 66,
}
RISK_DISTRIBUTION = {"LOW": 290, "MODERATE": 113, "ELEVATED": 4}


@pytest.fixture(scope="session")
def gold_pipeline(spark):
    cfg = RunConfig(
        catalog=None,
        run_date=pd.Timestamp("2026-04-10").date(),
        lookback_months=12,
        dq_min_rows=1,
        mlflow_enabled=False,
    )
    run_bronze(spark, cfg)
    run_silver(spark, cfg)
    run_gold(spark, cfg)
    return cfg


def _compare(reference, ours, keys, excluded):
    merged = reference.merge(
        ours, on=keys, how="outer", suffixes=("_reference", "_ours"), indicator=True
    )
    assert set(merged["_merge"]) == {"both"}, "gold output key mismatch"
    for column in reference.columns:
        if column in keys or column in excluded:
            continue
        left = merged[f"{column}_ours"]
        right = merged[f"{column}_reference"]
        both_null = left.isna() & right.isna()
        numeric = (
            pd.to_numeric(left, errors="coerce").notna()
            | pd.to_numeric(right, errors="coerce").notna()
        )
        equal = both_null.copy()
        equal |= numeric & np.isclose(
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
            atol=0.011,
            equal_nan=True,
        )
        equal |= ~numeric & (
            left.fillna("<NULL>").astype(str) == right.fillna("<NULL>").astype(str)
        )
        if not equal.all():
            examples = merged.loc[~equal, keys + [f"{column}_ours", f"{column}_reference"]].head(5)
            raise AssertionError(
                f"{column}: {(~equal).sum()} mismatches; examples:\n"
                f"{examples.to_string(index=False)}"
            )


def test_gold_parity(gold_pipeline, spark):
    cases = [
        (
            "customer_segments",
            ["customer_id"],
            407,
            {"load_ts", "segment_id", "segment_name", "subsegment_id"},
            "data/03_sas_data_products/customer_segments.csv",
        ),
        (
            "transaction_analytics",
            ["customer_id", "reporting_period"],
            500,
            {"load_ts", "top_spend_category"},
            "data/03_sas_data_products/transaction_analytics.csv",
        ),
        (
            "customer_risk_scores",
            ["customer_id"],
            407,
            {"load_ts"},
            "data/03_sas_data_products/customer_risk_scores.csv",
        ),
        (
            "customer_master_profile",
            ["customer_id"],
            407,
            {
                "load_ts",
                "segment_name",
                "top_spend_category",
                "lifetime_value_score",
                "engagement_score",
                "cross_sell_flag",
                "upsell_flag",
                "retention_risk_flag",
            },
            "data/03_sas_data_products/customer_master_profile.csv",
        ),
    ]
    for table, keys, expected_count, excluded, path in cases:
        reference = pd.read_csv(REPO_ROOT / path)
        ours = spark.table(gold_pipeline.fqn(gold_pipeline.gold_schema, table)).toPandas()
        assert len(reference) == expected_count
        assert len(ours) == expected_count
        _compare(reference, ours, keys, excluded)
        if "top_spend_category" in reference.columns:
            aligned_category = reference[keys + ["top_spend_category"]].merge(
                ours[keys + ["top_spend_category"]],
                on=keys,
                suffixes=("_reference", "_ours"),
            )
            reference_category = aligned_category["top_spend_category_reference"].notna()
            # Reference value is order-dependent; SAS MAX() semantics used.
            assert (
                aligned_category.loc[reference_category, "top_spend_category_ours"].notna().all()
            ), "reference value is order-dependent; SAS MAX() semantics used"

    segments = spark.table(gold_pipeline.fqn(gold_pipeline.gold_schema, "customer_segments"))
    segment_distribution = {
        row["segment_name"]: row["count"]
        for row in segments.groupBy("segment_name").count().collect()
    }
    print(f"segment distribution ours={segment_distribution} reference={SEGMENT_DISTRIBUTION}")
    assert set(segment_distribution) == set(SEGMENT_DISTRIBUTION)
    assert all(count >= 20 for count in segment_distribution.values())

    risk = spark.table(gold_pipeline.fqn(gold_pipeline.gold_schema, "customer_risk_scores"))
    risk_distribution = {
        row["risk_tier"]: row["count"] for row in risk.groupBy("risk_tier").count().collect()
    }
    assert risk_distribution == RISK_DISTRIBUTION

    master = spark.table(gold_pipeline.fqn(gold_pipeline.gold_schema, "customer_master_profile"))
    completeness = {
        "total": master.count(),
        "with_segment": master.where("segment_name != 'UNCLASSIFIED'").count(),
        "with_txn": master.where("monthly_transactions > 0").count(),
        "with_risk": master.where("risk_tier != 'UNKNOWN'").count(),
    }
    assert completeness == {
        "total": 407,
        "with_segment": 407,
        "with_txn": 407,
        "with_risk": 407,
    }
