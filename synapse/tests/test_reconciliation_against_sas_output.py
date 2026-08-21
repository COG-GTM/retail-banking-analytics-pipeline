"""Row-for-row reconciliation against the SAS golden record.

Uses the sample extracts committed under ``data/`` (produced by the legacy
Teradata + SAS pipeline) as the reconciliation cohort: the PySpark job is fed
the same inputs and its output is compared column by column with
``data/03_sas_data_products/customer_master_profile.csv``.
"""

from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from jobs.data_products_master_profile import (
    build_master_profile,
    completeness_report,
    select_base,
    select_risk,
    select_segments,
    select_txn,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
EFFECTIVE_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 11, 28, 8)

# Columns compared as text after rounding numerics to the data product scale.
DECIMAL_SCALES = {
    "LIFETIME_VALUE_SCORE": 2,
    "ENGAGEMENT_SCORE": 2,
    "TOTAL_BALANCE": 2,
    "TOTAL_CREDIT_LIMIT": 2,
    "CREDIT_UTILIZATION_PCT": 2,
    "MONTHLY_SPEND": 2,
    "NET_CASH_FLOW": 2,
    "DIGITAL_TXN_PCT": 2,
    "COMPOSITE_RISK_SCORE": 2,
    "PROBABILITY_OF_DEFAULT": 6,
}

COMPARED_COLUMNS = [
    "CUSTOMER_ID",
    "FULL_NAME",
    "AGE",
    "STATE_CODE",
    "CUSTOMER_SINCE",
    "TENURE_MONTHS",
    "CUSTOMER_STATUS",
    "SEGMENT_NAME",
    "LIFETIME_VALUE_SCORE",
    "ENGAGEMENT_SCORE",
    "TOTAL_ACCOUNTS",
    "ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "TOTAL_CREDIT_LIMIT",
    "CREDIT_UTILIZATION_PCT",
    "MONTHLY_TRANSACTIONS",
    "MONTHLY_SPEND",
    "NET_CASH_FLOW",
    "TOP_SPEND_CATEGORY",
    "DIGITAL_TXN_PCT",
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "PROBABILITY_OF_DEFAULT",
    "WATCH_LIST_FLAG",
    "CROSS_SELL_FLAG",
    "UPSELL_FLAG",
    "RETENTION_RISK_FLAG",
    "MODEL_VERSION",
    "EFFECTIVE_DATE",
]


def read_csv(spark, relative_path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(
        str(DATA_DIR / relative_path)
    )
    return df.toDF(*[column.upper() for column in df.columns])


def normalise(row):
    values = {}
    for column in COMPARED_COLUMNS:
        value = row[column]
        if column in DECIMAL_SCALES and value is not None:
            value = round(float(value), DECIMAL_SCALES[column])
        elif value is not None and column in ("TOTAL_ACCOUNTS", "ACTIVE_ACCOUNTS"):
            value = int(float(value))
        elif value is not None:
            value = str(value).strip()
        values[column] = value
    return values


@pytest.fixture(scope="module")
def actual_and_expected(spark):
    base = select_base(read_csv(spark, "02_bteq_staging/stg_customer_360.csv"))
    segments = select_segments(read_csv(spark, "03_sas_data_products/customer_segments.csv"))
    txn = select_txn(
        read_csv(spark, "03_sas_data_products/transaction_analytics.csv"), EFFECTIVE_DATE
    )
    risk = select_risk(read_csv(spark, "03_sas_data_products/customer_risk_scores.csv"))

    actual = build_master_profile(
        base, segments, txn, risk, effective_date=EFFECTIVE_DATE, load_ts=LOAD_TS
    ).cache()
    expected = read_csv(spark, "03_sas_data_products/customer_master_profile.csv")
    return actual, expected


def test_row_counts_match(actual_and_expected):
    actual, expected = actual_and_expected
    assert actual.count() == expected.count()


def test_every_row_matches_the_sas_golden_record(actual_and_expected):
    actual, expected = actual_and_expected

    actual_rows = {row["CUSTOMER_ID"]: normalise(row) for row in actual.collect()}
    expected_rows = {
        int(row["CUSTOMER_ID"]): normalise(row) for row in expected.collect()
    }

    assert set(actual_rows) == set(expected_rows)

    differences = {
        customer_id: (actual_rows[customer_id], expected_rows[customer_id])
        for customer_id in expected_rows
        if actual_rows[customer_id] != expected_rows[customer_id]
    }
    assert not differences


def test_customers_missing_from_upstream_products_keep_the_base_grain(spark):
    """Every upstream product is present for the full sample cohort, so drop a
    slice of each one to exercise the ``IN=`` default paths at scale."""
    base = select_base(read_csv(spark, "02_bteq_staging/stg_customer_360.csv"))
    dropped = [row["CUSTOMER_ID"] for row in base.select("CUSTOMER_ID").take(10)]

    segments = select_segments(
        read_csv(spark, "03_sas_data_products/customer_segments.csv")
    ).where(~F.col("CUSTOMER_ID").isin(dropped[:5]))
    txn = select_txn(
        read_csv(spark, "03_sas_data_products/transaction_analytics.csv"), EFFECTIVE_DATE
    ).where(~F.col("CUSTOMER_ID").isin(dropped[:7]))
    risk = select_risk(
        read_csv(spark, "03_sas_data_products/customer_risk_scores.csv")
    ).where(~F.col("CUSTOMER_ID").isin(dropped))

    profile = build_master_profile(
        base, segments, txn, risk, effective_date=EFFECTIVE_DATE, load_ts=LOAD_TS
    )

    assert profile.count() == base.count()
    assert profile.where("SEGMENT_NAME = 'UNCLASSIFIED'").count() == 5
    assert profile.where("MONTHLY_TRANSACTIONS = 0 AND MONTHLY_SPEND = 0").count() == 7
    assert (
        profile.where(
            "RISK_TIER = 'UNKNOWN' AND COMPOSITE_RISK_SCORE IS NULL "
            "AND PROBABILITY_OF_DEFAULT IS NULL AND WATCH_LIST_FLAG = 'N'"
        ).count()
        == 10
    )


def test_completeness_metrics_match_the_sas_report(actual_and_expected):
    actual, expected = actual_and_expected
    assert completeness_report(actual) == completeness_report(expected)
