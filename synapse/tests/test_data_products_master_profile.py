from datetime import date, datetime
from decimal import Decimal

import pytest

from jobs.data_products_master_profile import (
    OUTPUT_COLUMNS,
    build_master_profile,
    completeness_report,
    risk_tier_distribution,
    segment_distribution,
    select_base,
    select_risk,
    select_segments,
    select_txn,
)

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 11, 28, 8)

STG_COLUMNS = [
    "CUSTOMER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "AGE",
    "STATE_CODE",
    "CUSTOMER_SINCE",
    "TENURE_MONTHS",
    "CUSTOMER_STATUS",
    "NUM_ACCOUNTS",
    "NUM_ACTIVE_ACCOUNTS",
    "TOTAL_BALANCE",
    "TOTAL_CREDIT_LIMIT",
    "CREDIT_UTILIZATION_PCT",
]

STG_ROWS = [
    (1, " Angel ", "Johnson ", 70, "TX", date(2013, 3, 20), 157, "A", 2, 2, 17603.62, 0.0, 0.0),
    (2, "Joshua", "Long", 62, "FL", date(2007, 1, 12), 231, "A", 4, 4, 2336.95, 746.63, 21.28),
    (3, "Jill", "Johnson", 47, "GA", date(2020, 9, 26), 67, "A", 3, 2, 68760.83, 69846.45, 45.14),
    # Closed customer: excluded by the SAS `where CUSTOMER_STATUS = 'A'` filter.
    (4, "Closed", "Account", 33, "OH", date(2019, 5, 1), 80, "C", 1, 0, 0.0, 0.0, 0.0),
]

SEGMENT_COLUMNS = [
    "CUSTOMER_ID",
    "SEGMENT_NAME",
    "LIFETIME_VALUE_SCORE",
    "ENGAGEMENT_SCORE",
    "CROSS_SELL_FLAG",
    "UPSELL_FLAG",
    "RETENTION_RISK_FLAG",
    "MODEL_VERSION",
]

SEGMENT_ROWS = [
    (1, "VALUE_BASIC", 7674.05, 100.0, "N", "N", "N", "SEG_V3.2"),
    (2, "ENGAGED_MAINSTREAM", 17917.75, 100.0, "Y", "N", "Y", "SEG_V3.2"),
]

TXN_COLUMNS = [
    "CUSTOMER_ID",
    "TOTAL_TRANSACTIONS",
    "TOTAL_DEBIT_AMT",
    "NET_CASH_FLOW",
    "TOP_SPEND_CATEGORY",
    "DIGITAL_TXN_PCT",
    "EFFECTIVE_DATE",
]

TXN_ROWS = [
    (1, 142, 16922.80, -9075.14, "TRAVEL", 38.02, RUN_DATE),
    (2, 184, 17436.98, -9655.92, "ENTERTAINMENT", 44.02, RUN_DATE),
    # Prior period row for customer 3: filtered out, so customer 3 gets defaults.
    (3, 99, 1000.00, -500.00, "RETAIL", 10.0, date(2026, 4, 9)),
]

RISK_COLUMNS = [
    "CUSTOMER_ID",
    "COMPOSITE_RISK_SCORE",
    "RISK_TIER",
    "PROBABILITY_OF_DEFAULT",
    "WATCH_LIST_FLAG",
    "REVIEW_REQUIRED_FLAG",
]

RISK_ROWS = [
    (1, 28.22, "MODERATE", 0.05, "N", "N"),
    (3, 32.65, "MODERATE", 0.05, "Y", "N"),
]


@pytest.fixture(scope="module")
def master_profile(spark):
    base = select_base(spark.createDataFrame(STG_ROWS, STG_COLUMNS))
    segments = select_segments(spark.createDataFrame(SEGMENT_ROWS, SEGMENT_COLUMNS))
    txn = select_txn(spark.createDataFrame(TXN_ROWS, TXN_COLUMNS), RUN_DATE)
    risk = select_risk(spark.createDataFrame(RISK_ROWS, RISK_COLUMNS))
    return build_master_profile(
        base, segments, txn, risk, effective_date=RUN_DATE, load_ts=LOAD_TS
    ).cache()


def rows_by_id(df):
    return {row["CUSTOMER_ID"]: row for row in df.collect()}


def test_only_active_customers_are_kept(master_profile):
    assert sorted(rows_by_id(master_profile)) == [1, 2, 3]


def test_output_schema_matches_the_data_product_contract(master_profile):
    assert master_profile.columns == [name for name, _ in OUTPUT_COLUMNS]
    types = dict(master_profile.dtypes)
    assert types["LIFETIME_VALUE_SCORE"] == "decimal(10,2)"
    assert types["PROBABILITY_OF_DEFAULT"] == "decimal(7,6)"
    assert types["EFFECTIVE_DATE"] == "date"
    assert types["LOAD_TS"] == "timestamp"


def test_full_name_is_trimmed_and_concatenated(master_profile):
    assert rows_by_id(master_profile)[1]["FULL_NAME"] == "Angel Johnson"


def test_customer_present_in_every_member_keeps_upstream_values(master_profile):
    row = rows_by_id(master_profile)[1]
    assert row["SEGMENT_NAME"] == "VALUE_BASIC"
    assert row["LIFETIME_VALUE_SCORE"] == Decimal("7674.05")
    assert row["MONTHLY_TRANSACTIONS"] == 142
    assert row["MONTHLY_SPEND"] == Decimal("16922.80")
    assert row["RISK_TIER"] == "MODERATE"
    assert row["WATCH_LIST_FLAG"] == "N"


def test_missing_risk_member_gets_sas_defaults(master_profile):
    row = rows_by_id(master_profile)[2]
    assert row["COMPOSITE_RISK_SCORE"] is None
    assert row["PROBABILITY_OF_DEFAULT"] is None
    assert row["RISK_TIER"] == "UNKNOWN"
    assert row["WATCH_LIST_FLAG"] == "N"
    # Segment and transaction members are present, so their values survive.
    assert row["SEGMENT_NAME"] == "ENGAGED_MAINSTREAM"
    assert row["MONTHLY_TRANSACTIONS"] == 184


def test_missing_segment_and_txn_members_get_sas_defaults(master_profile):
    row = rows_by_id(master_profile)[3]
    assert row["SEGMENT_NAME"] == "UNCLASSIFIED"
    assert row["LIFETIME_VALUE_SCORE"] == Decimal("0.00")
    assert row["ENGAGEMENT_SCORE"] == Decimal("0.00")
    assert row["CROSS_SELL_FLAG"] == "N"
    assert row["UPSELL_FLAG"] == "N"
    assert row["RETENTION_RISK_FLAG"] == "N"
    assert row["MONTHLY_TRANSACTIONS"] == 0
    assert row["MONTHLY_SPEND"] == Decimal("0.00")
    assert row["NET_CASH_FLOW"] == Decimal("0.00")
    assert row["TOP_SPEND_CATEGORY"] == ""
    assert row["DIGITAL_TXN_PCT"] == Decimal("0.00")
    # Risk member is present for customer 3.
    assert row["RISK_TIER"] == "MODERATE"


def test_null_measure_on_a_present_member_is_not_defaulted(spark):
    base = select_base(spark.createDataFrame(STG_ROWS[:1], STG_COLUMNS))
    segments = select_segments(
        spark.createDataFrame(
            [(1, None, None, None, "N", "N", "N", "SEG_V3.2")],
            "CUSTOMER_ID bigint, SEGMENT_NAME string, LIFETIME_VALUE_SCORE double, "
            "ENGAGEMENT_SCORE double, CROSS_SELL_FLAG string, UPSELL_FLAG string, "
            "RETENTION_RISK_FLAG string, MODEL_VERSION string",
        )
    )
    txn = select_txn(spark.createDataFrame([], ",".join(f"{c} string" for c in TXN_COLUMNS)), RUN_DATE)
    risk = select_risk(spark.createDataFrame(RISK_ROWS[:1], RISK_COLUMNS))

    row = build_master_profile(
        base, segments, txn, risk, effective_date=RUN_DATE, load_ts=LOAD_TS
    ).collect()[0]

    assert row["SEGMENT_NAME"] is None
    assert row["LIFETIME_VALUE_SCORE"] is None


def test_metadata_columns(master_profile):
    row = rows_by_id(master_profile)[1]
    assert row["MODEL_VERSION"] == "MASTER_V1.5"
    assert row["EFFECTIVE_DATE"] == RUN_DATE
    assert row["LOAD_TS"] == LOAD_TS


def test_completeness_report_matches_sas_metrics(master_profile):
    assert completeness_report(master_profile) == {
        "TOTAL": 3,
        "HAS_SEGMENT": 2,
        "HAS_TXN": 2,
        "HAS_RISK_SCORE": 2,
        "CROSS_SELL_ELIGIBLE": 1,
        "UPSELL_ELIGIBLE": 0,
        "RETENTION_AT_RISK": 1,
        "ON_WATCH_LIST": 1,
    }


def test_distribution_reports(master_profile):
    segments = {row["SEGMENT_NAME"]: row for row in segment_distribution(master_profile).collect()}
    assert segments["VALUE_BASIC"]["N"] == 1
    assert segments["UNCLASSIFIED"]["AVG_LTV"] == Decimal("0.00")

    tiers = {row["RISK_TIER"]: row for row in risk_tier_distribution(master_profile).collect()}
    assert tiers["MODERATE"]["N"] == 2
    assert tiers["UNKNOWN"]["AVG_SCORE"] is None
