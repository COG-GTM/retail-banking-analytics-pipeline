"""End-to-end run of the ported pipeline, compared against the legacy outputs.

The CSVs under ``data/02_bteq_staging`` and ``data/03_sas_data_products`` were
produced by the Teradata/SAS reference implementation from the same generated
source data, so they are the parity baseline. Known, deliberate deviations are
listed in ``databricks/README.md`` and asserted here as tolerances rather than
being silently ignored.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from conftest import GOLD_REFERENCE, SILVER_REFERENCE, load_notebook
from shared import schemas

EXPECTED_ROWS = {
    "00_ingest_source_tables": {
        "CUSTOMERS": 500,
        "ACCOUNTS": 1251,
        "ADDRESSES": 1000,
        "TRANSACTIONS": 80528,
        "TRANSACTION_TYPES": 8,
        "CUSTOMER_BUREAU_SCORES": 500,
    },
    "01_stg_customer_360": 478,
    "02_stg_txn_summary": 1251,
    "03_stg_risk_factors": 478,
    "01_customer_segments": 407,
    "02_txn_analytics": 500,
    "03_risk_scoring": 407,
    "04_data_products": 407,
}

# table -> (reference csv stem, columns compared exactly)
SILVER_PARITY = {
    "STG_CUSTOMER_360": (
        "stg_customer_360",
        ["NUM_ACCOUNTS", "NUM_ACTIVE_ACCOUNTS", "TOTAL_BALANCE", "TOTAL_CREDIT_LIMIT",
         "CREDIT_UTILIZATION_PCT"],
    ),
    "STG_RISK_FACTORS": (
        "stg_risk_factors",
        ["ACCOUNT_OVERDRAFT_CNT", "NSF_FEE_TOTAL", "LARGE_WITHDRAWAL_CNT", "CREDIT_UTIL_RATIO",
         "PAYMENT_ONTIME_PCT", "PAYMENT_LATE_CNT", "EXTERNAL_CREDIT_SCORE", "DEBIT_VELOCITY_7D",
         "DEBIT_VELOCITY_30D", "INTERNATIONAL_TXN_CNT", "HIGH_RISK_MERCHANT_CNT"],
    ),
}

GOLD_PARITY = {
    "CUSTOMER_SEGMENTS": (
        "customer_segments",
        ["ENGAGEMENT_SCORE", "PRODUCT_BREADTH_INDEX", "BALANCE_TIER", "CROSS_SELL_FLAG",
         "RETENTION_RISK_FLAG"],
    ),
    "TRANSACTION_ANALYTICS": (
        "transaction_analytics",
        ["TOTAL_ACCOUNTS", "ACTIVE_ACCOUNTS", "TOTAL_TRANSACTIONS", "TOTAL_DEBIT_AMT",
         "TOTAL_CREDIT_AMT", "NET_CASH_FLOW", "MONTHLY_SPEND_TREND", "SPEND_PERCENTILE",
         "FEE_INCOME", "ANOMALY_FLAG"],
    ),
    "CUSTOMER_RISK_SCORES": (
        "customer_risk_scores",
        ["COMPOSITE_RISK_SCORE", "RISK_TIER", "PROBABILITY_OF_DEFAULT", "CREDIT_RISK_COMPONENT",
         "BEHAVIOUR_RISK_COMPONENT", "VELOCITY_RISK_COMPONENT", "BUREAU_SCORE_COMPONENT",
         "PAYMENT_HISTORY_COMPONENT", "WATCH_LIST_FLAG", "REVIEW_REQUIRED_FLAG"],
    ),
    "CUSTOMER_MASTER_PROFILE": (
        "customer_master_profile",
        ["FULL_NAME", "STATE_CODE", "CUSTOMER_STATUS", "TOTAL_ACCOUNTS", "TOTAL_BALANCE",
         "MONTHLY_TRANSACTIONS", "MONTHLY_SPEND", "NET_CASH_FLOW", "RISK_TIER"],
    ),
}

TOLERANCE = Decimal("0.011")  # half-cent DECIMAL rounding on the certified columns


def read_reference(spark, directory, stem: str):
    df = spark.read.csv(str(directory / f"{stem}.csv"), header=True, inferSchema=True)
    return df.select([df[c].alias(c.upper()) for c in df.columns])


NUMERIC_TYPES = ("decimal", "double", "float", "int", "bigint", "smallint", "tinyint")


def assert_columns_match(actual, expected, columns: list[str], label: str) -> None:
    joined = actual.alias("a").join(expected.alias("e"), on="CUSTOMER_ID", how="inner")
    mismatches: dict[str, int] = {}
    for column in columns:
        left, right = joined[f"a.{column}"], joined[f"e.{column}"]
        if actual.schema[column].dataType.simpleString().startswith(NUMERIC_TYPES):
            delta = F.abs(left.cast("decimal(38,10)") - right.cast("decimal(38,10)"))
            predicate = delta > F.lit(TOLERANCE)
        else:
            predicate = left.eqNullSafe(right) == F.lit(False)
        count = joined.where(predicate).count()
        if count:
            mismatches[column] = count
    assert not mismatches, f"{label} differs from the legacy output: {mismatches}"


def test_row_counts_match_the_legacy_pipeline(pipeline) -> None:
    assert pipeline == EXPECTED_ROWS


@pytest.mark.parametrize("table", sorted(GOLD_PARITY))
def test_gold_schema_matches_the_contract(spark, cfg, pipeline, table: str) -> None:
    actual = spark.table(cfg.gold(table)).schema
    assert schemas.schema_diff(actual, schemas.GOLD_SCHEMAS[table]) == []


@pytest.mark.parametrize("table", sorted(GOLD_PARITY))
def test_gold_keys_are_unique_and_populated(spark, cfg, pipeline, table: str) -> None:
    df = spark.table(cfg.gold(table))
    assert df.count() == df.select("CUSTOMER_ID").distinct().count()
    assert df.where("CUSTOMER_ID IS NULL OR MODEL_VERSION IS NULL").count() == 0


@pytest.mark.parametrize("table", sorted(SILVER_PARITY))
def test_silver_matches_the_bteq_output(spark, cfg, pipeline, table: str) -> None:
    stem, columns = SILVER_PARITY[table]
    actual = spark.table(cfg.silver(table))
    expected = read_reference(spark, SILVER_REFERENCE, stem)
    assert actual.count() == expected.count()
    assert_columns_match(actual, expected, columns, table)


@pytest.mark.parametrize("table", sorted(GOLD_PARITY))
def test_gold_matches_the_sas_output(spark, cfg, pipeline, table: str) -> None:
    stem, columns = GOLD_PARITY[table]
    actual = spark.table(cfg.gold(table))
    expected = read_reference(spark, GOLD_REFERENCE, stem)
    assert actual.count() == expected.count()
    assert_columns_match(actual, expected, columns, table)


def test_rerunning_the_gold_layer_is_idempotent(spark, cfg, pipeline) -> None:
    """Overwrite semantics replace the SAS DELETE + PROC APPEND FORCE pattern."""
    before = spark.table(cfg.gold("CUSTOMER_MASTER_PROFILE")).drop("LOAD_TS").collect()
    load_notebook("notebooks/gold/04_data_products.py").run(spark, cfg)
    after = spark.table(cfg.gold("CUSTOMER_MASTER_PROFILE")).drop("LOAD_TS").collect()

    assert sorted(before, key=lambda r: r.CUSTOMER_ID) == sorted(after, key=lambda r: r.CUSTOMER_ID)


def test_post_run_validation_task_passes(spark, cfg, pipeline) -> None:
    """The final Workflow task: DDL contracts, DQ rules and reference parity."""
    validation = load_notebook("notebooks/validation/99_validate_data_products.py")
    counts = validation.run(spark, cfg, str(GOLD_REFERENCE))

    assert counts == {
        "CUSTOMER_SEGMENTS": 407,
        "TRANSACTION_ANALYTICS": 500,
        "CUSTOMER_RISK_SCORES": 407,
        "CUSTOMER_MASTER_PROFILE": 407,
    }


def test_segment_labels_are_the_five_certified_names(spark, cfg, pipeline) -> None:
    distinct = spark.table(cfg.gold("CUSTOMER_SEGMENTS")).select("SEGMENT_NAME").distinct()
    names = {r.SEGMENT_NAME for r in distinct.collect()}
    assert names == {
        "PREMIUM_WEALTH",
        "ENGAGED_MAINSTREAM",
        "GROWING_DIGITAL",
        "CREDIT_DEPENDENT",
        "VALUE_BASIC",
    }


def test_risk_tiers_respect_the_sas_cutoffs(spark, cfg, pipeline) -> None:
    bad = spark.table(cfg.gold("CUSTOMER_RISK_SCORES")).where(
        """
        NOT (
            (COMPOSITE_RISK_SCORE <  20 AND RISK_TIER = 'LOW')
         OR (COMPOSITE_RISK_SCORE >= 20 AND COMPOSITE_RISK_SCORE < 40 AND RISK_TIER = 'MODERATE')
         OR (COMPOSITE_RISK_SCORE >= 40 AND COMPOSITE_RISK_SCORE < 60 AND RISK_TIER = 'ELEVATED')
         OR (COMPOSITE_RISK_SCORE >= 60 AND COMPOSITE_RISK_SCORE < 80 AND RISK_TIER = 'HIGH')
         OR (COMPOSITE_RISK_SCORE >= 80 AND RISK_TIER = 'CRITICAL')
        )
        """
    )
    assert bad.count() == 0
