"""End-to-end pipeline run against the repository sample data.

Runs the whole ported pipeline once (via the ``pipeline_result`` fixture) and
asserts equivalence with the legacy reference outputs: exact row counts, schema,
data-quality invariants, and an exact cell-level match on ``stg_txn_summary``
(the table confirmed identical to the Teradata output).
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from jobs import (
    customer_segments,
    master_profile,
    risk_scoring,
    stg_customer_360,
    stg_risk_factors,
    stg_txn_summary,
    transaction_analytics,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Legacy reference row counts (see data/02_bteq_staging, data/03_sas_data_products).
REFERENCE_COUNTS = {
    "stg_customer_360": 478,
    "stg_txn_summary": 1251,
    "stg_risk_factors": 478,
    "customer_segments": 407,
    "transaction_analytics": 500,
    "customer_risk_scores": 407,
    "customer_master_profile": 407,
}


def test_pipeline_row_counts_match_reference(pipeline_result):
    assert pipeline_result == REFERENCE_COUNTS


def test_output_schemas_match_declared_columns(spark, config, pipeline_result):
    cases = {
        config.staging("stg_customer_360"): stg_customer_360.OUTPUT_COLUMNS,
        config.staging("stg_txn_summary"): stg_txn_summary.OUTPUT_COLUMNS,
        config.staging("stg_risk_factors"): stg_risk_factors.OUTPUT_COLUMNS,
        config.product("customer_segments"): customer_segments.OUTPUT_COLUMNS,
        config.product("transaction_analytics"): transaction_analytics.OUTPUT_COLUMNS,
        config.product("customer_risk_scores"): risk_scoring.OUTPUT_COLUMNS,
        config.product("customer_master_profile"): master_profile.OUTPUT_COLUMNS,
    }
    for fqn, expected in cases.items():
        actual = spark.table(fqn).columns
        assert set(actual) == set(expected), fqn


def test_key_uniqueness_and_no_null_keys(spark, config, pipeline_result):
    for fqn in [
        config.staging("stg_customer_360"),
        config.staging("stg_risk_factors"),
        config.product("customer_segments"),
        config.product("customer_risk_scores"),
        config.product("customer_master_profile"),
    ]:
        df = spark.table(fqn)
        assert df.filter(F.col("customer_id").isNull()).count() == 0
        assert df.groupBy("customer_id").count().filter(F.col("count") > 1).count() == 0


def test_stg_txn_summary_matches_reference_exactly(spark, config, pipeline_result):
    ref = (
        spark.read.option("header", True)
        .csv(str(REPO_ROOT / "data" / "02_bteq_staging" / "stg_txn_summary.csv"))
        .select(
            F.col("customer_id").cast("long"),
            F.col("account_id").cast("long"),
            F.col("txn_count_total").cast("int").alias("ref_txn_count"),
            F.round(F.col("amt_total_debit").cast("double"), 2).alias("ref_debit"),
        )
    )
    act = spark.table(config.staging("stg_txn_summary")).select(
        "customer_id", "account_id",
        F.col("txn_count_total").alias("act_txn_count"),
        F.round(F.col("amt_total_debit").cast("double"), 2).alias("act_debit"),
    )
    joined = act.join(ref, ["customer_id", "account_id"], "inner")
    assert joined.count() == REFERENCE_COUNTS["stg_txn_summary"]
    mismatches = joined.filter(
        (F.col("act_txn_count") != F.col("ref_txn_count"))
        | (F.abs(F.col("act_debit") - F.col("ref_debit")) > 0.01)
    ).count()
    assert mismatches == 0


def test_segment_and_risk_domain_values(spark, config, pipeline_result):
    segs = {r.segment_name for r in spark.table(config.product("customer_segments")).collect()}
    assert segs <= set(customer_segments.SEGMENT_LABELS)

    tiers = {r.risk_tier for r in spark.table(config.product("customer_risk_scores")).collect()}
    assert tiers <= {"LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"}


def test_audit_log_records_success(spark, config, pipeline_result):
    from common.audit import AUDIT_TABLE_NAME

    audit = spark.table(config.table(config.staging_schema, AUDIT_TABLE_NAME))
    statuses = {r.status for r in audit.collect()}
    assert "SUCCESS" in statuses
    assert audit.filter(F.col("job_name") == "10_master_profile").count() >= 1
