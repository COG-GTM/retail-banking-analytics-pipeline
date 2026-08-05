"""Unit tests for the pure transforms of ``jobs/sas_data_products.py``."""

from __future__ import annotations

from datetime import date

import pytest

from common import schemas
from common.audit import AuditLog
from jobs.sas_data_products import (
    data_quality_report,
    log_quality_report,
    merge_by_customer_id,
    transform_base,
    transform_completeness_check,
    transform_master_profile,
    transform_risk_scores,
    transform_risk_tier_distribution,
    transform_segment_distribution,
    transform_segments,
    transform_txn_analytics,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)


@pytest.fixture
def customer_360(make_df):
    return make_df(
        schemas.STG_CUSTOMER_360,
        [
            {
                "CUSTOMER_ID": 1,
                "FIRST_NAME": " Ada ",
                "LAST_NAME": " Lovelace ",
                "AGE": 36,
                "STATE_CODE": "IL",
                "CUSTOMER_SINCE": "2020-04-10",
                "TENURE_MONTHS": 72,
                "CUSTOMER_STATUS": "A",
                "NUM_ACCOUNTS": 3,
                "NUM_ACTIVE_ACCOUNTS": 2,
                "TOTAL_BALANCE": 1000.00,
                "TOTAL_CREDIT_LIMIT": 2000.00,
                "CREDIT_UTILIZATION_PCT": 25.00,
            },
            {
                "CUSTOMER_ID": 2,
                "FIRST_NAME": "Grace",
                "LAST_NAME": "Hopper",
                "AGE": 25,
                "CUSTOMER_STATUS": "I",
                "NUM_ACCOUNTS": 1,
            },
            {
                "CUSTOMER_ID": 3,
                "FIRST_NAME": None,
                "LAST_NAME": "Turing",
                "AGE": 46,
                "CUSTOMER_STATUS": "A",
                "NUM_ACCOUNTS": 1,
                "NUM_ACTIVE_ACCOUNTS": 1,
            },
        ],
    )


@pytest.fixture
def segments(make_df):
    return make_df(
        schemas.CUSTOMER_SEGMENTS,
        [
            {
                "CUSTOMER_ID": 1,
                "SEGMENT_NAME": "PREMIUM_WEALTH",
                "LIFETIME_VALUE_SCORE": 1234.56,
                "ENGAGEMENT_SCORE": 88.50,
                "CROSS_SELL_FLAG": "Y",
                "UPSELL_FLAG": "N",
                "RETENTION_RISK_FLAG": "Y",
            }
        ],
    )


@pytest.fixture
def transaction_analytics(make_df):
    return make_df(
        schemas.TRANSACTION_ANALYTICS,
        [
            {
                "CUSTOMER_ID": 1,
                "TOTAL_TRANSACTIONS": 42,
                "TOTAL_DEBIT_AMT": 987.65,
                "NET_CASH_FLOW": -100.00,
                "TOP_SPEND_CATEGORY": "TRAVEL",
                "DIGITAL_TXN_PCT": 51.25,
                "EFFECTIVE_DATE": "2026-04-10",
            },
            {
                "CUSTOMER_ID": 3,
                "TOTAL_TRANSACTIONS": 7,
                "TOTAL_DEBIT_AMT": 70.00,
                "NET_CASH_FLOW": 10.00,
                "TOP_SPEND_CATEGORY": "GROCERIES",
                "DIGITAL_TXN_PCT": 10.00,
                "EFFECTIVE_DATE": "2026-04-09",
            },
        ],
    )


@pytest.fixture
def risk_scores(make_df):
    return make_df(
        schemas.CUSTOMER_RISK_SCORES,
        [
            {
                "CUSTOMER_ID": 1,
                "COMPOSITE_RISK_SCORE": 33.33,
                "RISK_TIER": "MODERATE",
                "PROBABILITY_OF_DEFAULT": 0.050000,
                "WATCH_LIST_FLAG": "Y",
            }
        ],
    )


def test_base_keeps_only_active_customers_and_renames_account_counts(customer_360):
    rows = {row["CUSTOMER_ID"]: row for row in transform_base(customer_360).collect()}

    assert sorted(rows) == [1, 3]
    assert rows[1]["TOTAL_ACCOUNTS"] == 3
    assert rows[1]["ACTIVE_ACCOUNTS"] == 2
    assert rows[1]["_base"] is True


def test_base_full_name_is_trimmed_and_missing_names_are_empty_strings(customer_360):
    rows = {row["CUSTOMER_ID"]: row for row in transform_base(customer_360).collect()}

    assert rows[1]["FULL_NAME"] == "Ada Lovelace"
    # SAS character missings are empty strings, so the separator survives
    assert rows[3]["FULL_NAME"] == " Turing"


def test_base_full_name_is_truncated_to_the_declared_length(make_df):
    customers = make_df(
        schemas.STG_CUSTOMER_360,
        [
            {
                "CUSTOMER_ID": 9,
                "FIRST_NAME": "F" * 100,
                "LAST_NAME": "L" * 50,
                "CUSTOMER_STATUS": "A",
            }
        ],
    )

    full_name = transform_base(customers).collect()[0]["FULL_NAME"]

    assert len(full_name) == 120
    assert full_name == ("F" * 100 + " " + "L" * 19)


def test_txn_analytics_keeps_only_the_current_effective_date(transaction_analytics):
    rows = transform_txn_analytics(transaction_analytics, RUN_DATE).collect()

    assert [row["CUSTOMER_ID"] for row in rows] == [1]
    assert rows[0]["MONTHLY_TRANSACTIONS"] == 42
    assert float(rows[0]["MONTHLY_SPEND"]) == 987.65
    assert rows[0]["_txn"] is True


def test_txn_analytics_is_empty_when_the_product_was_built_on_another_date(
    transaction_analytics,
):
    """The legacy ``where EFFECTIVE_DATE = today()`` quirk: a later run date sees nothing."""

    assert transform_txn_analytics(transaction_analytics, date(2026, 4, 11)).count() == 0


def test_merge_keeps_only_base_rows_and_marks_source_presence(
    customer_360, segments, transaction_analytics, risk_scores
):
    merged = merge_by_customer_id(
        transform_base(customer_360),
        transform_segments(segments),
        transform_txn_analytics(transaction_analytics, RUN_DATE),
        transform_risk_scores(risk_scores),
    )
    rows = {row["CUSTOMER_ID"]: row for row in merged.collect()}

    assert sorted(rows) == [1, 3]
    assert (rows[1]["_seg"], rows[1]["_txn"], rows[1]["_risk"]) == (True, True, True)
    assert (rows[3]["_seg"], rows[3]["_txn"], rows[3]["_risk"]) == (False, False, False)


def test_merge_drops_sources_rows_without_a_base_row(make_df, segments):
    """A segment for a customer that is not active in the base never reaches the profile."""

    base_only_inactive = make_df(
        schemas.STG_CUSTOMER_360, [{"CUSTOMER_ID": 1, "CUSTOMER_STATUS": "I"}]
    )
    merged = merge_by_customer_id(
        transform_base(base_only_inactive),
        transform_segments(segments),
        transform_txn_analytics(make_df(schemas.TRANSACTION_ANALYTICS, []), RUN_DATE),
        transform_risk_scores(make_df(schemas.CUSTOMER_RISK_SCORES, [])),
    )

    assert merged.count() == 0


@pytest.fixture
def profile(customer_360, segments, transaction_analytics, risk_scores, load_ts):
    rows = transform_master_profile(
        customer_360,
        segments,
        transaction_analytics,
        risk_scores,
        run_date=RUN_DATE,
        load_ts=load_ts,
    ).collect()
    return {row["CUSTOMER_ID"]: row for row in rows}


def test_master_profile_carries_every_matched_upstream_value(profile):
    row = profile[1]

    assert row["FULL_NAME"] == "Ada Lovelace"
    assert (row["SEGMENT_NAME"], float(row["LIFETIME_VALUE_SCORE"])) == ("PREMIUM_WEALTH", 1234.56)
    assert float(row["ENGAGEMENT_SCORE"]) == 88.50
    assert (row["CROSS_SELL_FLAG"], row["UPSELL_FLAG"], row["RETENTION_RISK_FLAG"]) == (
        "Y",
        "N",
        "Y",
    )
    assert (row["MONTHLY_TRANSACTIONS"], float(row["MONTHLY_SPEND"])) == (42, 987.65)
    assert (row["TOP_SPEND_CATEGORY"], float(row["DIGITAL_TXN_PCT"])) == ("TRAVEL", 51.25)
    assert (float(row["COMPOSITE_RISK_SCORE"]), row["RISK_TIER"]) == (33.33, "MODERATE")
    assert (float(row["PROBABILITY_OF_DEFAULT"]), row["WATCH_LIST_FLAG"]) == (0.05, "Y")


def test_master_profile_applies_every_missing_source_default(profile):
    row = profile[3]

    # if not _seg
    assert row["SEGMENT_NAME"] == "UNCLASSIFIED"
    assert float(row["LIFETIME_VALUE_SCORE"]) == 0
    assert float(row["ENGAGEMENT_SCORE"]) == 0
    assert (row["CROSS_SELL_FLAG"], row["UPSELL_FLAG"], row["RETENTION_RISK_FLAG"]) == (
        "N",
        "N",
        "N",
    )
    # if not _txn (the 2026-04-09 row is filtered out before the merge)
    assert row["MONTHLY_TRANSACTIONS"] == 0
    assert float(row["MONTHLY_SPEND"]) == 0
    assert float(row["NET_CASH_FLOW"]) == 0
    assert row["TOP_SPEND_CATEGORY"] == ""
    assert float(row["DIGITAL_TXN_PCT"]) == 0
    # if not _risk - the SAS missings are NULL, not 0
    assert row["COMPOSITE_RISK_SCORE"] is None
    assert row["PROBABILITY_OF_DEFAULT"] is None
    assert row["RISK_TIER"] == "UNKNOWN"
    assert row["WATCH_LIST_FLAG"] == "N"


def test_master_profile_defaults_per_missing_source_not_per_missing_column(
    customer_360, make_df, load_ts
):
    """``if not _seg`` keys off the *source*: a present row with a NULL label keeps the NULL."""

    segments = make_df(
        schemas.CUSTOMER_SEGMENTS, [{"CUSTOMER_ID": 1, "SEGMENT_NAME": None, "ENGAGEMENT_SCORE": 5}]
    )
    rows = {
        row["CUSTOMER_ID"]: row
        for row in transform_master_profile(
            customer_360,
            segments,
            make_df(schemas.TRANSACTION_ANALYTICS, []),
            make_df(schemas.CUSTOMER_RISK_SCORES, []),
            run_date=RUN_DATE,
            load_ts=load_ts,
        ).collect()
    }

    assert rows[1]["SEGMENT_NAME"] is None
    assert rows[1]["LIFETIME_VALUE_SCORE"] is None
    assert float(rows[1]["ENGAGEMENT_SCORE"]) == 5
    assert rows[3]["SEGMENT_NAME"] == "UNCLASSIFIED"


def test_master_profile_stamps_metadata_and_matches_the_ddl_contract(profile, load_ts):
    row = profile[1]

    assert row["MODEL_VERSION"] == "MASTER_V1.5"
    assert row["EFFECTIVE_DATE"] == RUN_DATE
    assert row["LOAD_TS"].isoformat() == "2026-04-10T00:00:00"
    assert list(row.asDict()) == list(schemas.CUSTOMER_MASTER_PROFILE.column_names)


def test_segment_distribution_counts_rounds_and_orders_by_count(make_df, spark):
    master = make_df(
        schemas.CUSTOMER_MASTER_PROFILE,
        [
            {"CUSTOMER_ID": 1, "SEGMENT_NAME": "VALUE_BASIC", "LIFETIME_VALUE_SCORE": 10.005},
            {"CUSTOMER_ID": 2, "SEGMENT_NAME": "VALUE_BASIC", "LIFETIME_VALUE_SCORE": 20.00},
            {"CUSTOMER_ID": 3, "SEGMENT_NAME": "UNCLASSIFIED", "LIFETIME_VALUE_SCORE": 0},
        ],
    )

    rows = transform_segment_distribution(master).collect()

    assert [(row["SEGMENT_NAME"], row["N"]) for row in rows] == [
        ("VALUE_BASIC", 2),
        ("UNCLASSIFIED", 1),
    ]
    assert float(rows[0]["AVG_LTV"]) == 15.00
    assert float(rows[1]["AVG_LTV"]) == 0.00


def test_risk_tier_distribution_orders_by_average_score(make_df):
    master = make_df(
        schemas.CUSTOMER_MASTER_PROFILE,
        [
            {"CUSTOMER_ID": 1, "RISK_TIER": "LOW", "COMPOSITE_RISK_SCORE": 10.121},
            {"CUSTOMER_ID": 2, "RISK_TIER": "HIGH", "COMPOSITE_RISK_SCORE": 70.00},
            {"CUSTOMER_ID": 3, "RISK_TIER": "UNKNOWN", "COMPOSITE_RISK_SCORE": None},
        ],
    )

    rows = transform_risk_tier_distribution(master).collect()

    assert [row["RISK_TIER"] for row in rows] == ["HIGH", "LOW", "UNKNOWN"]
    assert float(rows[1]["AVG_SCORE"]) == 10.12
    assert rows[2]["AVG_SCORE"] is None


def test_completeness_check_uses_the_legacy_definitions(make_df):
    master = make_df(
        schemas.CUSTOMER_MASTER_PROFILE,
        [
            {
                "CUSTOMER_ID": 1,
                "SEGMENT_NAME": "VALUE_BASIC",
                "MONTHLY_TRANSACTIONS": 5,
                "RISK_TIER": "LOW",
                "CROSS_SELL_FLAG": "Y",
                "UPSELL_FLAG": "Y",
                "RETENTION_RISK_FLAG": "N",
                "WATCH_LIST_FLAG": "Y",
            },
            {
                "CUSTOMER_ID": 2,
                "SEGMENT_NAME": "UNCLASSIFIED",
                "MONTHLY_TRANSACTIONS": 0,
                "RISK_TIER": "UNKNOWN",
                "CROSS_SELL_FLAG": "N",
                "UPSELL_FLAG": "N",
                "RETENTION_RISK_FLAG": "Y",
                "WATCH_LIST_FLAG": "N",
            },
            {
                "CUSTOMER_ID": 3,
                "SEGMENT_NAME": None,
                "MONTHLY_TRANSACTIONS": None,
                "RISK_TIER": None,
                "CROSS_SELL_FLAG": None,
                "UPSELL_FLAG": None,
                "RETENTION_RISK_FLAG": None,
                "WATCH_LIST_FLAG": None,
            },
        ],
    )

    row = transform_completeness_check(master).collect()[0]

    assert row["TOTAL"] == 3
    # a NULL/empty label is `ne 'UNCLASSIFIED'` in SAS, so it counts as populated
    assert (row["HAS_SEGMENT"], row["HAS_RISK_SCORE"]) == (2, 2)
    # a missing transaction count is not `> 0`
    assert row["HAS_TXN"] == 1
    assert (row["CROSS_SELL_ELIGIBLE"], row["UPSELL_ELIGIBLE"]) == (1, 1)
    assert (row["RETENTION_AT_RISK"], row["ON_WATCH_LIST"]) == (1, 1)


def test_quality_report_is_logged_through_the_audit_trail(make_df):
    master = make_df(
        schemas.CUSTOMER_MASTER_PROFILE,
        [{"CUSTOMER_ID": 1, "SEGMENT_NAME": "VALUE_BASIC", "RISK_TIER": "LOW"}],
    )
    audit = AuditLog()

    collected = log_quality_report(audit, data_quality_report(master))

    assert set(collected) == {
        "Master Profile - Segment Distribution",
        "Master Profile - Risk Tier Distribution",
        "Master Profile - Completeness Check",
    }
    assert len(audit.steps) == 3
    assert all(record.status == "SUCCESS" for record in audit.steps)
    assert any("Completeness Check" in record.message for record in audit.steps)
