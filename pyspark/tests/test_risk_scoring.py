"""Unit tests for the risk-scoring job (Ticket 9), seeded from data/ CSVs."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from common.validation import ValidationError, validate_table
from jobs import risk_scoring

EXPECTED_COLUMNS = [
    "customer_id",
    "composite_risk_score",
    "risk_tier",
    "probability_of_default",
    "credit_risk_component",
    "behaviour_risk_component",
    "velocity_risk_component",
    "bureau_score_component",
    "payment_history_component",
    "primary_risk_driver",
    "secondary_risk_driver",
    "score_delta_30d",
    "watch_list_flag",
    "review_required_flag",
    "model_version",
    "effective_date",
    "load_ts",
]

VALID_TIERS = {"LOW", "MODERATE", "ELEVATED", "HIGH", "CRITICAL"}


@pytest.fixture(scope="module")
def scored(spark, seed_staging):
    return risk_scoring.run(spark, seed_staging)


def test_output_schema_matches_ddl(scored):
    assert scored.columns == EXPECTED_COLUMNS


def test_only_active_customers_scored(spark, seed_staging, scored):
    cfg = seed_staging
    active_ids = {
        r.customer_id
        for r in spark.table(cfg.table(cfg.schema_stg, "stg_customer_360"))
        .where(F.col("customer_status") == "A")
        .select("customer_id")
        .collect()
    }
    scored_ids = {r.customer_id for r in scored.select("customer_id").collect()}
    assert scored_ids  # non-empty
    assert scored_ids.issubset(active_ids)


def test_customer_id_is_unique(scored):
    assert scored.count() == scored.select("customer_id").distinct().count()


def test_score_and_probability_ranges(scored):
    stats = scored.select(
        F.min("composite_risk_score").alias("cmin"),
        F.max("composite_risk_score").alias("cmax"),
        F.min("probability_of_default").alias("pmin"),
        F.max("probability_of_default").alias("pmax"),
    ).first()
    assert 0.0 <= float(stats["cmin"]) <= float(stats["cmax"]) <= 100.0
    assert 0.0 <= float(stats["pmin"]) <= float(stats["pmax"]) <= 1.0


def test_tiers_are_valid_and_consistent_with_composite(scored):
    rows = scored.select("composite_risk_score", "risk_tier").collect()
    assert {r.risk_tier for r in rows}.issubset(VALID_TIERS)
    for r in rows:
        score = float(r.composite_risk_score)
        if score < 20:
            expected = "LOW"
        elif score < 40:
            expected = "MODERATE"
        elif score < 60:
            expected = "ELEVATED"
        elif score < 80:
            expected = "HIGH"
        else:
            expected = "CRITICAL"
        assert r.risk_tier == expected


def test_watch_list_flag_uses_config_threshold(spark, seed_staging, scored):
    threshold = seed_staging.risk_score_threshold
    # Re-derive external_credit_score per customer to check the watch-list rule.
    risk = spark.table(seed_staging.table(seed_staging.schema_stg, "stg_risk_factors"))
    ecs = {r.customer_id: r.external_credit_score for r in risk.collect()}
    for r in scored.select(
        "customer_id", "risk_tier", "probability_of_default", "watch_list_flag"
    ).collect():
        raw_ecs = ecs.get(r.customer_id)
        imputed = 680.0 if (raw_ecs is None or raw_ecs <= 0) else float(raw_ecs)
        expected_y = r.risk_tier == "CRITICAL" and (
            float(r.probability_of_default) > 0.5 or imputed < threshold
        )
        assert (r.watch_list_flag == "Y") == expected_y


def test_flags_and_metadata_domain(scored):
    for r in scored.select(
        "watch_list_flag", "review_required_flag", "model_version"
    ).collect():
        assert r.watch_list_flag in {"Y", "N"}
        assert r.review_required_flag in {"Y", "N"}
        assert r.model_version == "RISK_V4.0"


def test_deterministic_output_with_fixed_seed(spark, seed_staging, scored):
    """A second run yields identical scores/tiers (fixed-seed reproducibility)."""
    cols = [c for c in EXPECTED_COLUMNS if c != "load_ts"]
    first = {
        r["customer_id"]: tuple(r[c] for c in cols)
        for r in scored.select(*cols).collect()
    }
    rerun = risk_scoring.run(spark, seed_staging)
    second = {
        r["customer_id"]: tuple(r[c] for c in cols)
        for r in rerun.select(*cols).collect()
    }
    assert first == second


def test_rerun_is_idempotent(spark, seed_staging, scored):
    """Delta overwrite keeps a single row per customer across re-runs."""
    rerun = risk_scoring.run(spark, seed_staging)
    assert rerun.count() == scored.count()
    assert rerun.count() == rerun.select("customer_id").distinct().count()


def test_rank_drivers_matches_sas_top_two():
    # credit highest, behaviour second.
    assert risk_scoring._rank_drivers(80.0, 40.0, 10.0, 80.0) == (
        "CREDIT_UTILIZATION",
        "BUREAU_SCORE",
    )
    # all zero -> empty labels (SAS init state).
    assert risk_scoring._rank_drivers(0.0, 0.0, 0.0, 0.0) == ("", "")
    # velocity dominates.
    primary, secondary = risk_scoring._rank_drivers(10.0, 20.0, 90.0, 10.0)
    assert primary == "TRANSACTION_VELOCITY"
    assert secondary == "PAYMENT_BEHAVIOUR"


def test_validate_table_raises_on_duplicate_keys(spark):
    df = spark.createDataFrame([(1,), (1,)], ["customer_id"])
    with pytest.raises(ValidationError):
        validate_table(df, unique_keys=["customer_id"])
