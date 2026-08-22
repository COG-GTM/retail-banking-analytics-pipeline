"""Unit tests for the Synapse Spark customer segmentation job (MBA-2207)."""

from __future__ import annotations

import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "..", "synapse", "spark", "jobs"),
)

from customer_segments import (  # noqa: E402
    CLUSTER_FEATURES,
    OUTPUT_COLUMNS,
    SEGMENT_LABELS,
    ValidationError,
    build_output,
    cluster,
    engineer_features,
    label_segments,
    segment,
    standardise,
    validate,
)

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STAGING_CSV = os.path.join(REPO_ROOT, "data", "02_bteq_staging", "stg_customer_360.csv")


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.appName("mba-2207-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def staging(spark):
    df = spark.read.option("header", True).option("inferSchema", True).csv(STAGING_CSV)
    for column in df.columns:
        df = df.withColumnRenamed(column, column.upper())
    return df.where("CUSTOMER_STATUS = 'A'").cache()


def test_feature_engineering_rules(spark):
    rows = [
        # id, age, tenure, checking, savings, credit, loan, balance, accounts, active
        (1, 30, 6, "Y", "N", "N", "N", 500.0, 4, 1),
        (2, 60, 120, "Y", "Y", "Y", "Y", 250000.0, 4, 4),
        (3, 20, 24, "N", "N", "N", "N", 0.0, 0, 0),
    ]
    df = spark.createDataFrame(
        rows,
        "CUSTOMER_ID long, AGE int, TENURE_MONTHS int, HAS_CHECKING string, "
        "HAS_SAVINGS string, HAS_CREDIT string, HAS_LOAN string, TOTAL_BALANCE double, "
        "NUM_ACCOUNTS int, NUM_ACTIVE_ACCOUNTS int",
    )
    out = {r["CUSTOMER_ID"]: r for r in engineer_features(df).collect()}

    assert out[1]["PRODUCT_BREADTH"] == 0.25
    assert out[1]["TENURE_GROUP"] == "NEW (<1yr)"
    assert out[1]["AGE_GROUP"] == "MILLENNIAL"
    assert out[1]["BALANCE_TIER"] == "LOW"
    assert out[1]["ACCT_RATIO"] == 0.25

    assert out[2]["PRODUCT_BREADTH"] == 1.0
    assert out[2]["TENURE_GROUP"] == "LOYAL (7yr+)"
    assert out[2]["AGE_GROUP"] == "BOOMER"
    assert out[2]["BALANCE_TIER"] == "HIGH_NET_WORTH"

    # log(max(balance, 1)) and division guarded against zero accounts
    assert out[3]["LOG_BALANCE"] == 0.0
    assert out[3]["ACCT_RATIO"] == 0.0
    assert out[3]["DIGITAL_ADOPTION_SCORE"] == 0.0
    assert out[3]["AGE_GROUP"] == "GEN_Z"


def test_standardise_produces_zero_mean_unit_variance(staging):
    scaled = standardise(engineer_features(staging))
    stats = scaled.selectExpr(
        *[f"avg({c}) as MEAN_{c}" for c in CLUSTER_FEATURES],
        *[f"stddev({c}) as STD_{c}" for c in CLUSTER_FEATURES],
    ).collect()[0]
    for column in CLUSTER_FEATURES:
        assert abs(stats[f"MEAN_{column}"]) < 1e-9
        assert abs(stats[f"STD_{column}"] - 1.0) < 1e-9
    # raw copies survive for the business rules
    assert "RAW_PRODUCT_BREADTH" in scaled.columns


def test_clusters_are_labelled_by_descending_balance(staging):
    clustered = cluster(standardise(engineer_features(staging))).cache()
    labels = label_segments(clustered).collect()

    assert len(labels) == 5
    assert {row["SEGMENT_NAME"] for row in labels} == set(SEGMENT_LABELS)
    assert {row["CLUSTER"] for row in labels} == {1, 2, 3, 4, 5}
    assert all(row["SUBSEGMENT_ID"] == 0 for row in labels)


def test_output_schema_and_action_flags(staging):
    result = segment(staging).cache()

    assert result.columns == OUTPUT_COLUMNS
    assert result.count() == staging.count()
    assert result.select("CUSTOMER_ID").distinct().count() == result.count()

    flags = result.selectExpr(
        "sum(case when CROSS_SELL_FLAG not in ('Y','N') then 1 else 0 end) as BAD_CROSS",
        "sum(case when UPSELL_FLAG not in ('Y','N') then 1 else 0 end) as BAD_UP",
        "sum(case when RETENTION_RISK_FLAG not in ('Y','N') then 1 else 0 end) as BAD_RET",
        "sum(case when SEGMENT_ID between 1 and 5 then 0 else 1 end) as BAD_ID",
        "sum(case when MODEL_VERSION = 'SEG_V3.2' then 0 else 1 end) as BAD_VERSION",
    ).collect()[0]
    assert all(value == 0 for value in flags.asDict().values())

    # Raw score basis keeps ENGAGEMENT_SCORE inside the DECIMAL(5,2) domain.
    bounds = result.selectExpr(
        "min(ENGAGEMENT_SCORE) as LO", "max(ENGAGEMENT_SCORE) as HI"
    ).collect()[0]
    assert bounds["LO"] >= 0.0
    assert bounds["HI"] <= 100.0


def test_upsell_and_retention_rules(staging):
    result = segment(staging)
    violations = result.where(
        "(UPSELL_FLAG = 'Y' and (BALANCE_TIER <> 'MODERATE' or TENURE_GROUP = 'NEW (<1yr)')) "
        "or (CROSS_SELL_FLAG = 'Y' and PRODUCT_BREADTH_INDEX >= 50)"
    ).count()
    assert violations == 0


def test_same_seed_is_reproducible(staging):
    first = segment(staging, seed=42).select("CUSTOMER_ID", "SEGMENT_ID", "SEGMENT_NAME")
    second = segment(staging, seed=42).select("CUSTOMER_ID", "SEGMENT_ID", "SEGMENT_NAME")
    assert first.exceptAll(second).count() == 0
    assert second.exceptAll(first).count() == 0


def test_standardised_score_basis_differs_from_raw(staging):
    features = standardise(engineer_features(staging))
    clustered = cluster(features).cache()
    labels = label_segments(clustered).cache()

    raw = build_output(clustered, labels, score_basis="raw")
    std = build_output(clustered, labels, score_basis="standardised")
    assert raw.select("ENGAGEMENT_SCORE").exceptAll(std.select("ENGAGEMENT_SCORE")).count() > 0

    with pytest.raises(ValueError):
        build_output(clustered, labels, score_basis="zscore")


def test_validation_gates_row_count_and_nulls(staging):
    result = segment(staging).cache()

    metrics = validate(result, min_rows=10)
    assert metrics["ROW_COUNT"] == metrics["DISTINCT_CUSTOMERS"]

    with pytest.raises(ValidationError):
        validate(result, min_rows=10_000_000)

    with pytest.raises(ValidationError):
        validate(result.unionByName(result.limit(1)), min_rows=10)
