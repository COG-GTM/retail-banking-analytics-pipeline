import datetime as dt

import pytest

from jobs.data_products_master_profile import (
    build_master_profile,
    completeness_report,
    run,
    select_base,
    select_txn,
)
from pipeline_utils.config import load_config
from pipeline_utils.run_log import ERROR, RunLogger
from pipeline_utils.validation import ValidationError

EFFECTIVE_DATE = dt.date(2026, 4, 10)

ENV = {
    "SNOWFLAKE_ACCOUNT": "acme-eu",
    "SNOWFLAKE_USER": "SVC_SYNAPSE",
    "AZURE_KEY_VAULT_URL": "https://kv.vault.azure.net/",
}

BASE_SCHEMA = (
    "CUSTOMER_ID long, FULL_NAME string, AGE int, STATE_CODE string, "
    "CUSTOMER_SINCE date, TENURE_MONTHS int, CUSTOMER_STATUS string, "
    "TOTAL_ACCOUNTS int, ACTIVE_ACCOUNTS int, TOTAL_BALANCE double, "
    "TOTAL_CREDIT_LIMIT double, CREDIT_UTILIZATION_PCT double"
)
SEGMENT_SCHEMA = (
    "CUSTOMER_ID long, SEGMENT_NAME string, LIFETIME_VALUE_SCORE double, "
    "ENGAGEMENT_SCORE double, CROSS_SELL_FLAG string, UPSELL_FLAG string, "
    "RETENTION_RISK_FLAG string"
)
TXN_SCHEMA = (
    "CUSTOMER_ID long, MONTHLY_TRANSACTIONS int, MONTHLY_SPEND double, "
    "NET_CASH_FLOW double, TOP_SPEND_CATEGORY string, DIGITAL_TXN_PCT double"
)
RISK_SCHEMA = (
    "CUSTOMER_ID long, COMPOSITE_RISK_SCORE double, RISK_TIER string, "
    "PROBABILITY_OF_DEFAULT double, WATCH_LIST_FLAG string"
)


@pytest.fixture
def frames(spark):
    base = spark.createDataFrame(
        [
            (1, "Ada Lovelace", 36, "NY", dt.date(2015, 1, 1), 120, "A", 3, 2, 100.0, 500.0, 20.0),
            (2, "Grace Hopper", 45, "TX", dt.date(2018, 6, 1), 60, "A", 1, 1, 50.0, 0.0, 0.0),
        ],
        BASE_SCHEMA,
    )
    segments = spark.createDataFrame(
        [(1, "AFFLUENT", 900.0, 80.0, "Y", "N", "N")], SEGMENT_SCHEMA
    )
    txn = spark.createDataFrame(
        [(1, 42, 1000.0, -200.0, "TRAVEL", 55.5)], TXN_SCHEMA
    )
    risk = spark.createDataFrame([(1, 30.5, "MODERATE", 0.05, "N")], RISK_SCHEMA)
    return base, segments, txn, risk


def test_missing_members_get_sas_defaults(frames):
    profile = build_master_profile(*frames, effective_date=EFFECTIVE_DATE)
    rows = {row["CUSTOMER_ID"]: row for row in profile.collect()}

    matched, unmatched = rows[1], rows[2]
    assert (matched["SEGMENT_NAME"], matched["RISK_TIER"]) == ("AFFLUENT", "MODERATE")
    assert matched["MONTHLY_TRANSACTIONS"] == 42

    assert unmatched["SEGMENT_NAME"] == "UNCLASSIFIED"
    assert unmatched["LIFETIME_VALUE_SCORE"] == 0
    assert unmatched["ENGAGEMENT_SCORE"] == 0
    assert unmatched["MONTHLY_TRANSACTIONS"] == 0
    assert unmatched["MONTHLY_SPEND"] == 0
    assert unmatched["TOP_SPEND_CATEGORY"] == ""
    assert unmatched["RISK_TIER"] == "UNKNOWN"
    assert unmatched["COMPOSITE_RISK_SCORE"] is None
    assert unmatched["PROBABILITY_OF_DEFAULT"] is None
    assert all(
        unmatched[flag] == "N"
        for flag in ("CROSS_SELL_FLAG", "UPSELL_FLAG", "RETENTION_RISK_FLAG", "WATCH_LIST_FLAG")
    )
    assert unmatched["MODEL_VERSION"] == "MASTER_V1.5"
    assert unmatched["EFFECTIVE_DATE"] == EFFECTIVE_DATE


def test_customers_absent_from_base_are_dropped(spark, frames):
    base, segments, txn, risk = frames
    orphan_segments = segments.union(
        spark.createDataFrame([(99, "AFFLUENT", 1.0, 1.0, "Y", "Y", "Y")], SEGMENT_SCHEMA)
    )
    profile = build_master_profile(
        base, orphan_segments, txn, risk, effective_date=EFFECTIVE_DATE
    )
    assert sorted(r["CUSTOMER_ID"] for r in profile.collect()) == [1, 2]


def test_select_base_keeps_only_active_customers_and_builds_full_name(spark):
    stg = spark.createDataFrame(
        [
            (1, " Ada ", "Lovelace ", 36, "NY", dt.date(2015, 1, 1), 120, "A", 3, 2, 1.0, 2.0, 3.0),
            (2, "Closed", "Account", 40, "TX", dt.date(2016, 1, 1), 80, "C", 1, 0, 0.0, 0.0, 0.0),
        ],
        "CUSTOMER_ID long, FIRST_NAME string, LAST_NAME string, AGE int, STATE_CODE string, "
        "CUSTOMER_SINCE date, TENURE_MONTHS int, CUSTOMER_STATUS string, "
        "NUM_ACCOUNTS int, NUM_ACTIVE_ACCOUNTS int, TOTAL_BALANCE double, "
        "TOTAL_CREDIT_LIMIT double, CREDIT_UTILIZATION_PCT double",
    )
    rows = select_base(stg).collect()
    assert len(rows) == 1
    assert rows[0]["FULL_NAME"] == "Ada Lovelace"
    assert rows[0]["TOTAL_ACCOUNTS"] == 3
    assert rows[0]["ACTIVE_ACCOUNTS"] == 2


def test_select_txn_keeps_current_period_only(spark):
    analytics = spark.createDataFrame(
        [
            (1, 10, 1.0, 2.0, "TRAVEL", 50.0, dt.date(2026, 4, 10)),
            (2, 20, 3.0, 4.0, "FUEL", 10.0, dt.date(2026, 4, 9)),
        ],
        "CUSTOMER_ID long, TOTAL_TRANSACTIONS int, TOTAL_DEBIT_AMT double, "
        "NET_CASH_FLOW double, TOP_SPEND_CATEGORY string, DIGITAL_TXN_PCT double, "
        "EFFECTIVE_DATE date",
    )
    rows = select_txn(analytics, EFFECTIVE_DATE).collect()
    assert [r["CUSTOMER_ID"] for r in rows] == [1]
    assert rows[0]["MONTHLY_SPEND"] == 1.0


def test_completeness_report_matches_sas_metrics(frames):
    profile = build_master_profile(*frames, effective_date=EFFECTIVE_DATE)
    assert completeness_report(profile) == {
        "TOTAL": 2,
        "HAS_SEGMENT": 1,
        "HAS_TXN": 1,
        "HAS_RISK_SCORE": 1,
        "CROSS_SELL_ELIGIBLE": 1,
        "UPSELL_ELIGIBLE": 0,
        "RETENTION_AT_RISK": 0,
        "ON_WATCH_LIST": 0,
    }


class StubIO:
    def __init__(self, tables):
        self.tables = tables
        self.written = []
        self.appended = []

    def read_table(self, schema, table):
        return self.tables[table]

    def overwrite_table(self, df, schema, table):
        self.written.append((schema, table, df.count()))

    def append_table(self, df, schema, table):
        self.appended.append((schema, table, df.collect()))


@pytest.fixture
def stub_io(spark, frames):
    _, segments, txn, risk = frames
    stg = spark.createDataFrame(
        [
            (1, "Ada", "Lovelace", 36, "NY", dt.date(2015, 1, 1), 120, "A", 3, 2, 100.0, 500.0, 20.0),
            (2, "Grace", "Hopper", 45, "TX", dt.date(2018, 6, 1), 60, "A", 1, 1, 50.0, 0.0, 0.0),
        ],
        "CUSTOMER_ID long, FIRST_NAME string, LAST_NAME string, AGE int, STATE_CODE string, "
        "CUSTOMER_SINCE date, TENURE_MONTHS int, CUSTOMER_STATUS string, "
        "NUM_ACCOUNTS int, NUM_ACTIVE_ACCOUNTS int, TOTAL_BALANCE double, "
        "TOTAL_CREDIT_LIMIT double, CREDIT_UTILIZATION_PCT double",
    )
    analytics = txn.selectExpr(
        "CUSTOMER_ID",
        "MONTHLY_TRANSACTIONS as TOTAL_TRANSACTIONS",
        "MONTHLY_SPEND as TOTAL_DEBIT_AMT",
        "NET_CASH_FLOW",
        "TOP_SPEND_CATEGORY",
        "DIGITAL_TXN_PCT",
        f"date'{EFFECTIVE_DATE.isoformat()}' as EFFECTIVE_DATE",
    )
    return StubIO(
        {
            "STG_CUSTOMER_360": stg,
            "CUSTOMER_SEGMENTS": segments,
            "TRANSACTION_ANALYTICS": analytics,
            "CUSTOMER_RISK_SCORES": risk,
        }
    )


def test_run_loads_golden_record_and_flushes_run_log(spark, stub_io):
    config = load_config(ENV)
    run_logger = RunLogger(spark, "04_MASTER_PROFILE", config, stub_io, run_id="r1")

    assert run(spark, config, stub_io, run_logger, EFFECTIVE_DATE, min_rows=1) == 2
    assert stub_io.written == [("DATA_PRODUCTS", "CUSTOMER_MASTER_PROFILE", 2)]

    run_logger.flush()
    _, _, rows = stub_io.appended[0]
    assert [r["STATUS"] for r in rows][-1] == "SUCCESS"


def test_validation_failure_aborts_run_and_is_recorded(spark, stub_io):
    config = load_config(ENV)
    run_logger = RunLogger(spark, "04_MASTER_PROFILE", config, stub_io, run_id="r2")

    with pytest.raises(ValidationError, match="row_count"):
        run(spark, config, stub_io, run_logger, EFFECTIVE_DATE, min_rows=1000)

    assert stub_io.written == []
    run_logger.flush()
    _, _, rows = stub_io.appended[0]
    assert rows[-1]["STATUS"] == ERROR
    assert "Validation failed" in rows[-1]["MESSAGE"]
