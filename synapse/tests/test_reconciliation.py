"""Row-for-row reconciliation against the SAS golden record.

The CSV extracts under data/ are the output of the legacy Teradata + SAS
pipeline; data/03_sas_data_products/customer_master_profile.csv is what
04_sas_data_products.sas produced from the other three extracts.
"""

import datetime as dt

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

EFFECTIVE_DATE = dt.date(2026, 4, 10)
TOLERANCE = 1e-6

NUMERIC_COLUMNS = {
    "AGE",
    "TENURE_MONTHS",
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
    "DIGITAL_TXN_PCT",
    "COMPOSITE_RISK_SCORE",
    "PROBABILITY_OF_DEFAULT",
}
COMPARED_COLUMNS = [
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
]


def read_csv(spark, path):
    df = spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    return df.toDF(*[c.upper() for c in df.columns])


@pytest.fixture(scope="module")
def sas_inputs(spark, sample_data_dir):
    return {
        "stg": read_csv(spark, sample_data_dir / "02_bteq_staging/stg_customer_360.csv"),
        "segments": read_csv(
            spark, sample_data_dir / "03_sas_data_products/customer_segments.csv"
        ),
        "txn": read_csv(
            spark, sample_data_dir / "03_sas_data_products/transaction_analytics.csv"
        ),
        "risk": read_csv(
            spark, sample_data_dir / "03_sas_data_products/customer_risk_scores.csv"
        ),
        "expected": read_csv(
            spark,
            sample_data_dir / "03_sas_data_products/customer_master_profile.csv",
        ),
    }


@pytest.fixture(scope="module")
def actual_profile(sas_inputs):
    return build_master_profile(
        select_base(sas_inputs["stg"]),
        select_segments(sas_inputs["segments"]),
        select_txn(sas_inputs["txn"], EFFECTIVE_DATE),
        select_risk(sas_inputs["risk"]),
        effective_date=EFFECTIVE_DATE,
    ).cache()


def as_dict(df):
    return {row["CUSTOMER_ID"]: row for row in df.collect()}


def test_same_customer_population_as_sas(actual_profile, sas_inputs):
    assert sorted(as_dict(actual_profile)) == sorted(as_dict(sas_inputs["expected"]))


def test_every_column_matches_the_sas_golden_record(actual_profile, sas_inputs):
    actual, expected = as_dict(actual_profile), as_dict(sas_inputs["expected"])
    mismatches = []
    for customer_id, expected_row in expected.items():
        actual_row = actual[customer_id]
        for column in COMPARED_COLUMNS:
            got, want = actual_row[column], expected_row[column]
            if column in NUMERIC_COLUMNS:
                if want is None or got is None:
                    equal = want is None and got is None
                else:
                    equal = abs(float(got) - float(want)) <= TOLERANCE
            else:
                equal = (got or "") == (want or "")
            if not equal:
                mismatches.append((customer_id, column, got, want))
    assert mismatches == []


def test_customers_missing_upstream_members_get_defaults(sas_inputs, actual_profile):
    """Every sample customer is present in all three products, so withhold one.

    Dropping a customer from each upstream product must default only that
    customer's member block and leave every other row identical to the SAS
    golden record.
    """
    base = select_base(sas_inputs["stg"])
    withheld = sorted(as_dict(actual_profile))[:3]
    no_segment, no_txn, no_risk = withheld

    profile = build_master_profile(
        base,
        select_segments(sas_inputs["segments"]).where(F.col("CUSTOMER_ID") != no_segment),
        select_txn(sas_inputs["txn"], EFFECTIVE_DATE).where(
            F.col("CUSTOMER_ID") != no_txn
        ),
        select_risk(sas_inputs["risk"]).where(F.col("CUSTOMER_ID") != no_risk),
        effective_date=EFFECTIVE_DATE,
    )
    rows = as_dict(profile)
    reference = as_dict(actual_profile)

    assert rows[no_segment]["SEGMENT_NAME"] == "UNCLASSIFIED"
    assert rows[no_segment]["LIFETIME_VALUE_SCORE"] == 0
    assert rows[no_segment]["CROSS_SELL_FLAG"] == "N"
    assert rows[no_txn]["MONTHLY_TRANSACTIONS"] == 0
    assert rows[no_txn]["TOP_SPEND_CATEGORY"] == ""
    assert rows[no_risk]["RISK_TIER"] == "UNKNOWN"
    assert rows[no_risk]["COMPOSITE_RISK_SCORE"] is None

    untouched = [cid for cid in rows if cid not in withheld]
    for cid in untouched:
        for column in COMPARED_COLUMNS:
            assert rows[cid][column] == reference[cid][column], (cid, column)


def test_completeness_metrics_match_the_sas_report(actual_profile, sas_inputs):
    assert completeness_report(actual_profile) == completeness_report(
        sas_inputs["expected"]
    )
