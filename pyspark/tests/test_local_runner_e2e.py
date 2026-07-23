"""End-to-end test: run the whole pipeline via local_runner and assert the
CUSTOMER_MASTER_PROFILE golden record produced from the repo's ``data/`` seeds.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from common.spark import read_delta
from orchestration.local_runner import run_pipeline


def test_pipeline_produces_master_profile(spark, cfg):
    results = run_pipeline(spark, cfg)

    assert set(results) >= {
        "create_delta_tables", "stg_customer_360", "stg_txn_summary",
        "stg_risk_factors", "customer_segments", "txn_analytics",
        "risk_scoring", "data_products",
    }

    master = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile")
    stg = read_delta(spark, cfg, cfg.schema_stg, "stg_customer_360")

    # Only active (status 'A') customers are carried into the golden record.
    expected = stg.filter(F.col("customer_status") == "A").count()
    assert expected > 0
    assert master.count() == expected

    # Key is unique and never null.
    assert master.filter(F.col("customer_id").isNull()).count() == 0
    assert master.groupBy("customer_id").count().filter(F.col("count") > 1).count() == 0


def test_defaults_applied_for_unmatched_customers(spark, cfg):
    run_pipeline(spark, cfg)
    master = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile")

    segments = read_delta(spark, cfg, cfg.schema_dp, "customer_segments")
    seg_ids = {r["customer_id"] for r in segments.select("customer_id").collect()}
    unmatched = master.filter(~F.col("customer_id").isin(list(seg_ids)))

    if unmatched.count() > 0:
        assert unmatched.filter(F.col("segment_name") != "UNCLASSIFIED").count() == 0


def test_audit_log_written(spark, cfg):
    run_pipeline(spark, cfg)
    audit = read_delta(spark, cfg, cfg.schema_stg, "etl_run_log")
    dp_rows = audit.filter(F.col("job_name") == "data_products")
    assert dp_rows.filter(F.col("status") == "SUCCESS").count() >= 1


def test_pipeline_idempotent(spark, cfg):
    run_pipeline(spark, cfg)
    first = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile").count()
    run_pipeline(spark, cfg)
    second = read_delta(spark, cfg, cfg.schema_dp, "customer_master_profile").count()
    assert first == second
