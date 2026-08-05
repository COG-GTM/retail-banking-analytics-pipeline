"""Unit tests for the pure transforms of ``jobs.stg_txn_summary``."""

from __future__ import annotations

from datetime import date

import pytest

from common import schemas
from jobs.stg_txn_summary import (
    run_params,
    transform_account_summary,
    transform_posted_transactions,
    transform_top_merchant_category,
    transform_txn_summary,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)
LOOKBACK_MONTHS = 12

TYPES = [
    {"TRANSACTION_TYPE_CD": "PUR", "CATEGORY": "DEBIT"},
    {"TRANSACTION_TYPE_CD": "DEP", "CATEGORY": "CREDIT"},
    {"TRANSACTION_TYPE_CD": "FEE", "CATEGORY": "FEE"},
    {"TRANSACTION_TYPE_CD": "INT", "CATEGORY": "INTEREST"},
]


@pytest.fixture
def transaction_types(make_df):
    return make_df(schemas.TRANSACTION_TYPES, TYPES)


@pytest.fixture
def accounts(make_df):
    return make_df(
        schemas.ACCOUNTS,
        [
            {"ACCOUNT_ID": 1, "CUSTOMER_ID": 100, "ACCOUNT_TYPE": "CHECKING"},
            {"ACCOUNT_ID": 2, "CUSTOMER_ID": 100, "ACCOUNT_TYPE": "SAVINGS"},
        ],
    )


def _txn(transaction_id: int, **overrides) -> dict[str, object]:
    row: dict[str, object] = {
        "TRANSACTION_ID": transaction_id,
        "ACCOUNT_ID": 1,
        "TRANSACTION_TYPE_CD": "PUR",
        "TRANSACTION_DATE": "2026-01-15",
        "AMOUNT": -10.00,
        "MERCHANT_NAME": "Acme",
        "MERCHANT_CATEGORY": "GROCERY",
        "CHANNEL_CODE": "POS",
        "STATUS_CODE": "P",
    }
    row.update(overrides)
    return row


def _summary(make_df, transaction_types, accounts, rows):
    transactions = make_df(schemas.TRANSACTIONS, rows)
    period_start, period_end = run_params(RUN_DATE, LOOKBACK_MONTHS)
    posted = transform_posted_transactions(transactions, period_start, period_end)
    return transform_account_summary(
        posted,
        accounts,
        transaction_types,
        transform_top_merchant_category(posted),
        run_date=RUN_DATE,
    )


def _by_account(df):
    return {row["ACCOUNT_ID"]: row for row in df.collect()}


def test_run_params_derive_the_window_from_the_configured_lookback(spark):
    period_start, period_end = run_params(RUN_DATE, LOOKBACK_MONTHS)

    row = spark.range(1).select(period_start.alias("start"), period_end.alias("end")).collect()[0]

    assert row["start"] == date(2025, 4, 10)
    assert row["end"] == RUN_DATE


def test_run_params_follows_a_changed_lookback(spark):
    period_start, _ = run_params(RUN_DATE, 3)

    assert spark.range(1).select(period_start).collect()[0][0] == date(2026, 1, 10)


def test_posted_transactions_keeps_only_posted_rows_inside_the_inclusive_window(make_df):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            _txn(1, TRANSACTION_DATE="2025-04-10"),  # PERIOD_START, inclusive
            _txn(2, TRANSACTION_DATE="2026-04-10"),  # PERIOD_END, inclusive
            _txn(3, TRANSACTION_DATE="2025-04-09"),  # one day before the window
            _txn(4, TRANSACTION_DATE="2026-04-11"),  # one day after the window
            _txn(5, STATUS_CODE="H"),
            _txn(6, STATUS_CODE="R"),
            _txn(7, TRANSACTION_DATE=None),
        ],
    )
    period_start, period_end = run_params(RUN_DATE, LOOKBACK_MONTHS)

    kept = transform_posted_transactions(transactions, period_start, period_end)

    assert sorted(row["TRANSACTION_ID"] for row in kept.collect()) == [1, 2]


def test_top_merchant_category_picks_the_highest_absolute_spend(make_df):
    posted = make_df(
        schemas.TRANSACTIONS,
        [
            _txn(1, MERCHANT_CATEGORY="GROCERY", AMOUNT=-100.00),
            _txn(2, MERCHANT_CATEGORY="GROCERY", AMOUNT=-50.00),
            _txn(3, MERCHANT_CATEGORY="TRAVEL", AMOUNT=-200.00),
            _txn(4, ACCOUNT_ID=2, MERCHANT_CATEGORY="FUEL", AMOUNT=-5.00),
        ],
    )

    rows = {row["ACCOUNT_ID"]: row[1] for row in transform_top_merchant_category(posted).collect()}

    # GROCERY totals 150 against TRAVEL's 200; the sign of AMOUNT is ignored (ABS)
    assert rows == {1: "TRAVEL", 2: "FUEL"}


def test_top_merchant_category_tiebreak_is_deterministic(make_df):
    posted = make_df(
        schemas.TRANSACTIONS,
        [
            _txn(1, MERCHANT_CATEGORY="TRAVEL", AMOUNT=-100.00),
            _txn(2, MERCHANT_CATEGORY="GROCERY", AMOUNT=100.00),
        ],
    )

    rows = transform_top_merchant_category(posted).collect()

    assert [row[1] for row in rows] == ["GROCERY"]


def test_top_merchant_category_ignores_null_categories(make_df):
    posted = make_df(
        schemas.TRANSACTIONS,
        [
            _txn(1, MERCHANT_CATEGORY=None, AMOUNT=-900.00),
            _txn(2, MERCHANT_CATEGORY="GROCERY", AMOUNT=-1.00),
            _txn(3, ACCOUNT_ID=2, MERCHANT_CATEGORY=None, AMOUNT=-5.00),
        ],
    )

    rows = {row["ACCOUNT_ID"]: row[1] for row in transform_top_merchant_category(posted).collect()}

    # account 2 has no categorised spend at all, so it gets no top_cat row
    assert rows == {1: "GROCERY"}


def test_account_summary_counts_amounts_and_averages_per_category(
    make_df, transaction_types, accounts
):
    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, TRANSACTION_TYPE_CD="PUR", AMOUNT=-100.00),
            _txn(2, TRANSACTION_TYPE_CD="PUR", AMOUNT=-300.00),
            _txn(3, TRANSACTION_TYPE_CD="DEP", AMOUNT=500.00),
            _txn(4, TRANSACTION_TYPE_CD="DEP", AMOUNT=100.00),
            _txn(5, TRANSACTION_TYPE_CD="FEE", AMOUNT=-35.00),
            _txn(6, TRANSACTION_TYPE_CD="INT", AMOUNT=2.00),
        ],
    )

    row = _by_account(summary)[1]

    assert row["CUSTOMER_ID"] == 100
    assert row["ACCOUNT_TYPE"] == "CHECKING"
    # INTEREST is in none of the three CASE branches but still counts towards the total
    assert row["TXN_COUNT_TOTAL"] == 6
    assert (row["TXN_COUNT_DEBIT"], row["TXN_COUNT_CREDIT"], row["TXN_COUNT_FEE"]) == (2, 2, 1)
    assert float(row["AMT_TOTAL_DEBIT"]) == 400.00
    assert float(row["AMT_TOTAL_CREDIT"]) == 600.00
    assert float(row["AMT_TOTAL_FEES"]) == 35.00
    assert float(row["AMT_AVG_DEBIT"]) == 200.00
    assert float(row["AMT_AVG_CREDIT"]) == 300.00
    assert float(row["AMT_MAX_SINGLE_DEBIT"]) == 300.00
    assert float(row["AMT_MAX_SINGLE_CREDIT"]) == 500.00


def test_account_summary_defaults_when_a_category_is_absent(make_df, transaction_types, accounts):
    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [_txn(1, TRANSACTION_TYPE_CD="DEP", AMOUNT=42.00)],
    )

    row = _by_account(summary)[1]

    # SUM/MAX of a CASE with ELSE 0 default to 0; AVG's CASE has ELSE NULL so it stays NULL
    assert float(row["AMT_TOTAL_DEBIT"]) == 0.00
    assert float(row["AMT_MAX_SINGLE_DEBIT"]) == 0.00
    assert row["AMT_AVG_DEBIT"] is None
    assert row["TXN_COUNT_DEBIT"] == 0


def test_account_summary_distinct_merchants_ignores_nulls(make_df, transaction_types, accounts):
    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, MERCHANT_NAME="Acme"),
            _txn(2, MERCHANT_NAME="Acme"),
            _txn(3, MERCHANT_NAME="Globex"),
            _txn(4, MERCHANT_NAME=None),
            _txn(5, MERCHANT_NAME=None),
        ],
    )

    assert _by_account(summary)[1]["DISTINCT_MERCHANTS"] == 2


def test_account_summary_channel_percentages_use_every_channel_code(
    make_df, transaction_types, accounts
):
    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, CHANNEL_CODE="ATM"),
            _txn(2, CHANNEL_CODE="POS"),
            _txn(3, CHANNEL_CODE="WEB"),
            _txn(4, CHANNEL_CODE="MOB"),
            _txn(5, CHANNEL_CODE="MOB"),
            _txn(6, CHANNEL_CODE="ACH"),
            _txn(7, CHANNEL_CODE=None),
        ],
    )

    row = _by_account(summary)[1]

    # CAST(... AS DECIMAL(5,2)) rounds 1/7 -> 14.29 and 2/7 -> 28.57; ACH and NULL are in the
    # denominator but in no bucket, so the four percentages do not add up to 100
    assert float(row["PCT_ATM"]) == 14.29
    assert float(row["PCT_POS"]) == 14.29
    assert float(row["PCT_WEB"]) == 14.29
    assert float(row["PCT_MOBILE"]) == 28.57


def test_account_summary_days_since_last_txn(make_df, transaction_types, accounts):
    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, TRANSACTION_DATE="2026-04-09"),
            _txn(2, TRANSACTION_DATE="2025-06-01"),
            _txn(3, ACCOUNT_ID=2, TRANSACTION_DATE="2026-04-10"),
        ],
    )

    rows = _by_account(summary)
    assert rows[1]["DAYS_SINCE_LAST_TXN"] == 1
    assert rows[2]["DAYS_SINCE_LAST_TXN"] == 0


def test_account_summary_drops_unknown_type_codes_but_top_category_keeps_them(
    make_df, transaction_types, accounts
):
    """The ``top_cat`` subquery is not inner-joined to ``TRANSACTION_TYPES``."""

    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, TRANSACTION_TYPE_CD="PUR", MERCHANT_CATEGORY="GROCERY", AMOUNT=-10.00),
            _txn(2, TRANSACTION_TYPE_CD="XXX", MERCHANT_CATEGORY="TRAVEL", AMOUNT=-1000.00),
        ],
    )

    row = _by_account(summary)[1]

    assert row["TXN_COUNT_TOTAL"] == 1
    assert float(row["AMT_TOTAL_DEBIT"]) == 10.00
    assert row["TOP_MERCHANT_CATEGORY"] == "TRAVEL"


def test_account_summary_emits_one_row_per_account(make_df, transaction_types, accounts):
    """``top_cat`` is joined on ACCOUNT_ID only and yields at most one row per account."""

    summary = _summary(
        make_df,
        transaction_types,
        accounts,
        [
            _txn(1, MERCHANT_CATEGORY="GROCERY"),
            _txn(2, MERCHANT_CATEGORY="TRAVEL"),
            _txn(3, ACCOUNT_ID=2, MERCHANT_CATEGORY=None),
            _txn(4, ACCOUNT_ID=3, MERCHANT_CATEGORY="FUEL"),
        ],
    )

    rows = _by_account(summary)

    # account 3 has transactions but no ACCOUNTS row: the inner join drops it
    assert sorted(rows) == [1, 2]
    assert rows[2]["TOP_MERCHANT_CATEGORY"] is None


def test_txn_summary_stamps_the_window_and_matches_the_ddl_contract(
    make_df, transaction_types, accounts, load_ts
):
    result = transform_txn_summary(
        make_df(schemas.TRANSACTIONS, [_txn(1), _txn(2, TRANSACTION_DATE="2024-01-01")]),
        transaction_types,
        accounts,
        run_date=RUN_DATE,
        lookback_months=LOOKBACK_MONTHS,
        load_ts=load_ts,
    )

    assert result.columns == list(schemas.STG_TXN_SUMMARY.column_names)
    row = result.collect()[0]
    assert row["SUMMARY_PERIOD_START"] == date(2025, 4, 10)
    assert row["SUMMARY_PERIOD_END"] == RUN_DATE
    assert row["TXN_COUNT_TOTAL"] == 1


def test_txn_summary_lookback_months_is_honoured(make_df, transaction_types, accounts, load_ts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [_txn(1, TRANSACTION_DATE="2026-04-01"), _txn(2, TRANSACTION_DATE="2025-10-01")],
    )

    rows = transform_txn_summary(
        transactions,
        transaction_types,
        accounts,
        run_date=RUN_DATE,
        lookback_months=3,
        load_ts=load_ts,
    ).collect()

    assert len(rows) == 1
    assert rows[0]["TXN_COUNT_TOTAL"] == 1
    assert rows[0]["SUMMARY_PERIOD_START"] == date(2026, 1, 10)
