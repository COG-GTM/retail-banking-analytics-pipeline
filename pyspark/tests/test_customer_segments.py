"""Unit tests for the customer_segments data-product job (Ticket 7).

Seeds ``etl_staging.stg_customer_360`` from ``data/02_bteq_staging`` and runs
the full ``run(spark, cfg)`` job, asserting the k-means pipeline produces 5
segments with deterministic labels under the fixed seed.
"""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common.config import Config
from common.validation import ValidationError, validate_table
from conftest import read_seed_csv
from jobs import customer_segments as cs

EXPECTED_LABELS = set(cs.SEGMENT_LABELS.values())
OUTPUT_COLUMNS = [
    "customer_id", "segment_name", "segment_id", "subsegment_id",
    "lifetime_value_score", "engagement_score", "digital_adoption_score",
    "product_breadth_index", "tenure_group", "age_group", "balance_tier",
    "channel_preference", "cross_sell_flag", "upsell_flag",
    "retention_risk_flag", "model_version", "effective_date", "load_ts",
]


def _seed_staging(spark, cfg: Config) -> None:
    df = read_seed_csv(spark, "02_bteq_staging", "stg_customer_360")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema_stg}")
    target = cfg.table(cfg.schema_stg, "stg_customer_360")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )


@pytest.fixture()
def segments(spark, cfg):
    _seed_staging(spark, cfg)
    return cs.run(spark, cfg).cache()


def test_output_schema(segments):
    assert segments.columns == OUTPUT_COLUMNS


def test_only_active_customers_scored(spark, cfg, segments):
    staged = spark.read.table(cfg.table(cfg.schema_stg, "stg_customer_360"))
    active = staged.filter(F.col("customer_status") == "A").count()
    assert segments.count() == active
    assert active > 0


def test_produces_five_segments(segments):
    labels = {r["segment_name"] for r in segments.select("segment_name").distinct().collect()}
    assert labels == EXPECTED_LABELS
    ids = {r["segment_id"] for r in segments.select("segment_id").distinct().collect()}
    assert ids == {1, 2, 3, 4, 5}


def test_segment_id_maps_one_to_one_with_name(segments):
    pairs = segments.select("segment_id", "segment_name").distinct().collect()
    assert len(pairs) == cs.NUM_SEGMENTS
    for row in pairs:
        assert cs.SEGMENT_LABELS[row["segment_id"]] == row["segment_name"]


def test_customer_id_unique(segments):
    assert segments.select("customer_id").distinct().count() == segments.count()


def test_flags_and_metadata(segments):
    for flag in ("cross_sell_flag", "upsell_flag", "retention_risk_flag"):
        vals = {r[flag] for r in segments.select(flag).distinct().collect()}
        assert vals.issubset({"Y", "N"})
    versions = {r["model_version"] for r in segments.select("model_version").distinct().collect()}
    assert versions == {cs.MODEL_VERSION}


def test_deterministic_labels_across_runs(spark, cfg, segments):
    """Re-running with the same seed yields the identical customer->segment map."""
    first = {r["customer_id"]: r["segment_name"] for r in
             segments.select("customer_id", "segment_name").collect()}
    rerun = cs.run(spark, cfg)
    second = {r["customer_id"]: r["segment_name"] for r in
              rerun.select("customer_id", "segment_name").collect()}
    assert first == second


def test_idempotent_overwrite(spark, cfg, segments):
    """Overwrite semantics: row count is stable, not appended, across runs."""
    before = segments.count()
    cs.run(spark, cfg)
    after = spark.read.table(cfg.table(cfg.schema_dp, "customer_segments")).count()
    assert after == before


def test_validation_raises_on_nulls(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["customer_id", "segment_name"])
    with pytest.raises(ValidationError):
        validate_table(df, not_null_cols=["segment_name"])
