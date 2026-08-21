"""Unit tests for the migrated customer segmentation Synapse Spark job."""

from __future__ import annotations

import importlib.util
import pathlib
import sys

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

JOB_PATH = (
    pathlib.Path(__file__).resolve().parents[2] / "synapse" / "spark" / "jobs" / "01_customer_segments.py"
)


def _load_job():
    spec = importlib.util.spec_from_file_location("customer_segments_job", JOB_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


job = _load_job()


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_customer_segments")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _customer(customer_id, **overrides):
    row = {
        "CUSTOMER_ID": customer_id,
        "AGE": 40,
        "TENURE_MONTHS": 60,
        "CUSTOMER_STATUS": "A",
        "SEGMENT_CODE": "RETAIL",
        "STATE_CODE": "NC",
        "NUM_ACCOUNTS": 4,
        "NUM_ACTIVE_ACCOUNTS": 4,
        "HAS_CHECKING": "Y",
        "HAS_SAVINGS": "Y",
        "HAS_CREDIT": "N",
        "HAS_LOAN": "N",
        "TOTAL_BALANCE": 5000.0,
        "TOTAL_CREDIT_LIMIT": 1000.0,
        "CREDIT_UTILIZATION_PCT": 10.0,
    }
    row.update(overrides)
    return row


def _source(spark, rows):
    return spark.createDataFrame(rows).select(*job.STG_COLUMNS)


def test_engineer_features_reproduces_sas_derivations(spark):
    rows = [
        _customer(1, TENURE_MONTHS=6, AGE=22, TOTAL_BALANCE=500.0, NUM_ACTIVE_ACCOUNTS=2),
        _customer(2, TENURE_MONTHS=24, AGE=30, TOTAL_BALANCE=5000.0),
        _customer(3, TENURE_MONTHS=48, AGE=50, TOTAL_BALANCE=50000.0),
        _customer(4, TENURE_MONTHS=120, AGE=80, TOTAL_BALANCE=500000.0, HAS_CREDIT="Y", HAS_LOAN="Y"),
    ]
    features = {r["CUSTOMER_ID"]: r for r in job.engineer_features(_source(spark, rows)).collect()}

    assert features[1]["TENURE_GROUP"] == "NEW (<1yr)"
    assert features[2]["TENURE_GROUP"] == "DEVELOPING (1-3yr)"
    assert features[3]["TENURE_GROUP"] == "ESTABLISHED (3-7yr)"
    assert features[4]["TENURE_GROUP"] == "LOYAL (7yr+)"

    assert features[1]["AGE_GROUP"] == "GEN_Z"
    assert features[2]["AGE_GROUP"] == "MILLENNIAL"
    assert features[3]["AGE_GROUP"] == "GEN_X"
    assert features[4]["AGE_GROUP"] == "SILENT"

    assert features[1]["BALANCE_TIER"] == "LOW"
    assert features[2]["BALANCE_TIER"] == "MODERATE"
    assert features[3]["BALANCE_TIER"] == "AFFLUENT"
    assert features[4]["BALANCE_TIER"] == "HIGH_NET_WORTH"

    assert features[2]["PRODUCT_BREADTH"] == pytest.approx(0.5)
    assert features[4]["PRODUCT_BREADTH"] == pytest.approx(1.0)
    assert features[1]["ACCT_RATIO"] == pytest.approx(0.5)
    assert features[2]["LOG_BALANCE"] == pytest.approx(8.517193, rel=1e-6)
    assert features[2]["DIGITAL_ADOPTION_SCORE"] == 0.0


def test_log_balance_floors_at_one(spark):
    rows = [_customer(1, TOTAL_BALANCE=0.0)]
    assert job.engineer_features(_source(spark, rows)).first()["LOG_BALANCE"] == 0.0


def test_scores_and_action_flags_match_sas_rules(spark):
    rows = [
        # cross-sell: breadth < 0.5 with strong engagement
        _customer(1, HAS_SAVINGS="N", NUM_ACCOUNTS=4, NUM_ACTIVE_ACCOUNTS=4),
        # upsell: MODERATE balance tier beyond the first year
        _customer(2, TOTAL_BALANCE=5000.0, TENURE_MONTHS=24),
        # retention risk: weak engagement on a long-tenured customer
        _customer(3, TENURE_MONTHS=72, NUM_ACCOUNTS=4, NUM_ACTIVE_ACCOUNTS=1, TOTAL_BALANCE=200000.0),
        # no flags: new MODERATE customer with full engagement
        _customer(4, TENURE_MONTHS=6, TOTAL_BALANCE=200000.0),
    ]
    features = job.engineer_features(_source(spark, rows))
    clustered = features.withColumn("CLUSTER", features["CUSTOMER_ID"] % 5)
    labels = job.label_clusters(clustered)
    segments = {
        r["CUSTOMER_ID"]: r for r in job.build_customer_segments(clustered, labels).collect()
    }

    assert segments[1]["CROSS_SELL_FLAG"] == "Y"
    assert segments[4]["CROSS_SELL_FLAG"] == "N"
    assert segments[2]["UPSELL_FLAG"] == "Y"
    assert segments[3]["RETENTION_RISK_FLAG"] == "Y"
    assert segments[4]["UPSELL_FLAG"] == "N"
    assert segments[4]["RETENTION_RISK_FLAG"] == "N"

    # LTV = log(balance) * tenure * breadth * 10, rounded to 2dp
    assert float(segments[2]["LIFETIME_VALUE_SCORE"]) == pytest.approx(1022.06, abs=0.01)
    assert float(segments[3]["ENGAGEMENT_SCORE"]) == pytest.approx(25.0)
    assert float(segments[2]["PRODUCT_BREADTH_INDEX"]) == pytest.approx(50.0)
    assert segments[2]["MODEL_VERSION"] == job.MODEL_VERSION
    assert list(segments[2].asDict().keys()) == job.OUTPUT_COLUMNS


def test_label_clusters_orders_by_descending_balance(spark):
    rows = [_customer(i, TOTAL_BALANCE=float(10 ** (i % 5 + 1))) for i in range(1, 21)]
    features = job.engineer_features(_source(spark, rows))
    clustered = features.withColumn("CLUSTER", features["CUSTOMER_ID"] % 5)
    labels = job.label_clusters(clustered).collect()

    profiles = {
        r["CLUSTER"]: r["AVG_BALANCE"]
        for r in clustered.groupBy("CLUSTER").avg("LOG_BALANCE").withColumnRenamed(
            "avg(LOG_BALANCE)", "AVG_BALANCE"
        ).collect()
    }
    by_name = {r["SEGMENT_NAME"]: r["CLUSTER"] for r in labels}
    assert sorted(by_name) == sorted(job.SEGMENT_LABELS)
    ordered = [profiles[by_name[name]] for name in job.SEGMENT_LABELS]
    assert ordered == sorted(ordered, reverse=True)
    assert all(r["SUBSEGMENT_ID"] == 0 for r in labels)


def test_clustering_is_reproducible_for_a_fixed_seed(spark):
    rows = [
        _customer(
            i,
            AGE=20 + i % 60,
            TENURE_MONTHS=i * 3,
            TOTAL_BALANCE=float(100 * i),
            NUM_ACCOUNTS=1 + i % 4,
            NUM_ACTIVE_ACCOUNTS=1 + i % 3,
            CREDIT_UTILIZATION_PCT=float(i % 100),
            HAS_CREDIT="Y" if i % 2 else "N",
        )
        for i in range(1, 121)
    ]
    features = job.engineer_features(_source(spark, rows))
    first = job.assign_clusters(features, seed=job.DEFAULT_SEED).select("CUSTOMER_ID", "CLUSTER")
    second = job.assign_clusters(features, seed=job.DEFAULT_SEED).select("CUSTOMER_ID", "CLUSTER")
    assert sorted(first.collect()) == sorted(second.collect())


def test_validate_output_rejects_low_row_counts_and_duplicates(spark):
    rows = [_customer(1), _customer(2)]
    features = job.engineer_features(_source(spark, rows))
    clustered = features.withColumn("CLUSTER", features["CUSTOMER_ID"] % 5)
    segments = job.build_customer_segments(clustered, job.label_clusters(clustered))

    assert job.validate_output(segments, min_rows=2) == 2
    with pytest.raises(job.ValidationError, match="row count"):
        job.validate_output(segments, min_rows=10)
    with pytest.raises(job.ValidationError, match="not unique"):
        job.validate_output(segments.unionByName(segments), min_rows=1)
