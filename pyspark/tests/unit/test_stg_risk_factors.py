"""Unit tests for every pure transform of the ``03_stg_risk_factors`` port."""

from __future__ import annotations

from datetime import date

import pytest

from common import schemas
from jobs.stg_risk_factors import (
    transform_balance_metrics,
    transform_credit_exposure,
    transform_daily_balance,
    transform_debit_velocity,
    transform_large_withdrawals,
    transform_latest_bureau_score,
    transform_merchant_risk,
    transform_merchants_seen_before,
    transform_overdraft_nsf,
    transform_payment_history,
    transform_payment_summary,
    transform_risk_factors,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)
# Window boundaries for RUN_DATE, mirroring the BTEQ expressions.
THREE_MONTHS_AGO = "2026-01-10"  # ADD_MONTHS(CURRENT_DATE, -3)
SIX_MONTHS_AGO = "2025-10-10"  # ADD_MONTHS(CURRENT_DATE, -6)
TWELVE_MONTHS_AGO = "2025-04-10"  # ADD_MONTHS(CURRENT_DATE, -12)
TWENTY_FOUR_MONTHS_AGO = "2024-04-10"  # ADD_MONTHS(CURRENT_DATE, -24)
THIRTY_DAYS_AGO = "2026-03-11"  # CURRENT_DATE - 30
SEVEN_DAYS_AGO = "2026-04-03"  # CURRENT_DATE - 7


def txn(**overrides: object) -> dict[str, object]:
    """A posted purchase, overridable field by field."""

    row: dict[str, object] = {
        "TRANSACTION_ID": 1,
        "ACCOUNT_ID": 1,
        "TRANSACTION_TYPE_CD": "PUR",
        "TRANSACTION_DATE": "2026-04-01",
        "TRANSACTION_TS": "2026-04-01 10:00:00",
        "AMOUNT": -10.00,
        "RUNNING_BALANCE": 100.00,
        "MERCHANT_NAME": "Acme",
        "MERCHANT_CATEGORY": "GROCERY",
        "CHANNEL_CODE": "POS",
        "STATUS_CODE": "P",
    }
    row.update(overrides)
    return row


def account(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ACCOUNT_ID": 1,
        "CUSTOMER_ID": 1,
        "ACCOUNT_TYPE": "CHECKING",
        "ACCOUNT_STATUS": "O",
        "OPEN_DATE": "2020-01-15",
        "CURRENT_BALANCE": 0.00,
        "CREDIT_LIMIT": None,
    }
    row.update(overrides)
    return row


TYPES = [
    {"TRANSACTION_TYPE_CD": "PUR", "DESCRIPTION": "Purchase", "CATEGORY": "DEBIT"},
    {"TRANSACTION_TYPE_CD": "DEP", "DESCRIPTION": "Deposit", "CATEGORY": "CREDIT"},
    {"TRANSACTION_TYPE_CD": "NSF", "DESCRIPTION": "NSF Fee", "CATEGORY": "FEE"},
    {"TRANSACTION_TYPE_CD": "MFE", "DESCRIPTION": "Monthly Fee", "CATEGORY": "FEE"},
]


@pytest.fixture
def types(make_df):
    return make_df(schemas.TRANSACTION_TYPES, TYPES)


@pytest.fixture
def accounts(make_df):
    return make_df(schemas.ACCOUNTS, [account()])


# ---------------------------------------------------------------------------------------------
# WRK_DAILY_BALANCE
# ---------------------------------------------------------------------------------------------


def test_daily_balance_keeps_last_posted_transaction_of_each_account_day(make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(
                TRANSACTION_ID=1,
                TRANSACTION_DATE="2026-04-01",
                TRANSACTION_TS="2026-04-01 09:00:00",
                RUNNING_BALANCE=10.00,
            ),
            txn(
                TRANSACTION_ID=2,
                TRANSACTION_DATE="2026-04-01",
                TRANSACTION_TS="2026-04-01 18:00:00",
                RUNNING_BALANCE=20.00,
            ),
            txn(
                TRANSACTION_ID=3,
                TRANSACTION_DATE="2026-04-02",
                TRANSACTION_TS="2026-04-02 08:00:00",
                RUNNING_BALANCE=30.00,
            ),
        ],
    )

    rows = {
        row["TRANSACTION_DATE"]: row
        for row in transform_daily_balance(transactions, accounts, run_date=RUN_DATE).collect()
    }

    assert len(rows) == 2
    assert float(rows[date(2026, 4, 1)]["EOD_BALANCE"]) == 20.00
    assert float(rows[date(2026, 4, 2)]["EOD_BALANCE"]) == 30.00
    assert rows[date(2026, 4, 1)]["CUSTOMER_ID"] == 1


def test_daily_balance_tiebreak_on_transaction_id_is_deterministic(make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=7, RUNNING_BALANCE=70.00),
            txn(TRANSACTION_ID=9, RUNNING_BALANCE=90.00),
            txn(TRANSACTION_ID=8, RUNNING_BALANCE=80.00),
        ],
    )

    rows = transform_daily_balance(transactions, accounts, run_date=RUN_DATE).collect()

    assert [float(row["EOD_BALANCE"]) for row in rows] == [90.00]


def test_daily_balance_window_is_three_months_and_posted_only(make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE=THREE_MONTHS_AGO, RUNNING_BALANCE=1.00),
            txn(TRANSACTION_ID=2, TRANSACTION_DATE="2026-01-09", RUNNING_BALANCE=2.00),
            txn(TRANSACTION_ID=3, TRANSACTION_DATE="2026-02-01", STATUS_CODE="R"),
            txn(TRANSACTION_ID=4, TRANSACTION_DATE="2026-02-02", STATUS_CODE="H"),
        ],
    )

    rows = transform_daily_balance(transactions, accounts, run_date=RUN_DATE).collect()

    assert [str(row["TRANSACTION_DATE"]) for row in rows] == [THREE_MONTHS_AGO]


def test_daily_balance_drops_transactions_without_an_account(make_df, accounts):
    transactions = make_df(schemas.TRANSACTIONS, [txn(ACCOUNT_ID=999)])

    assert transform_daily_balance(transactions, accounts, run_date=RUN_DATE).count() == 0


# ---------------------------------------------------------------------------------------------
# WRK_PAYMENT_HISTORY
# ---------------------------------------------------------------------------------------------


def test_payment_history_counts_only_credit_category_on_credit_and_loan_accounts(make_df, types):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            account(ACCOUNT_ID=1, ACCOUNT_TYPE="CREDIT", OPEN_DATE="2024-06-15"),
            account(ACCOUNT_ID=2, ACCOUNT_TYPE="LOAN", OPEN_DATE="2023-01-31"),
            account(ACCOUNT_ID=3, ACCOUNT_TYPE="CHECKING", OPEN_DATE="2023-01-31"),
        ],
    )
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, ACCOUNT_ID=1, TRANSACTION_TYPE_CD="DEP"),
            txn(TRANSACTION_ID=2, ACCOUNT_ID=1, TRANSACTION_TYPE_CD="PUR"),
            txn(TRANSACTION_ID=3, ACCOUNT_ID=2, TRANSACTION_TYPE_CD="DEP"),
            txn(TRANSACTION_ID=4, ACCOUNT_ID=3, TRANSACTION_TYPE_CD="DEP"),
            txn(TRANSACTION_ID=5, ACCOUNT_ID=1, TRANSACTION_TYPE_CD="DEP", STATUS_CODE="R"),
            txn(
                TRANSACTION_ID=6,
                ACCOUNT_ID=1,
                TRANSACTION_TYPE_CD="DEP",
                TRANSACTION_DATE="2024-04-09",
            ),
        ],
    )

    rows = {
        row["ACCOUNT_ID"]: row
        for row in transform_payment_history(
            transactions, accounts, types, run_date=RUN_DATE
        ).collect()
    }

    assert set(rows) == {1, 2}
    assert rows[1]["TOTAL_PAYMENTS"] == 1
    assert rows[2]["TOTAL_PAYMENTS"] == 1


def test_payment_history_ontime_test_is_always_true_legacy_quirk(make_df, types):
    """LEGACY_INVENTORY.md 5.9: the due-date proxy is the payment's own month + 1."""

    accounts = make_df(
        schemas.ACCOUNTS, [account(ACCOUNT_ID=1, ACCOUNT_TYPE="CREDIT", OPEN_DATE="2024-05-31")]
    )
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_TYPE_CD="DEP", TRANSACTION_DATE="2024-06-30"),
            txn(TRANSACTION_ID=2, TRANSACTION_TYPE_CD="DEP", TRANSACTION_DATE="2025-02-28"),
            txn(TRANSACTION_ID=3, TRANSACTION_TYPE_CD="DEP", TRANSACTION_DATE="2026-04-10"),
        ],
    )

    row = transform_payment_history(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert row["TOTAL_PAYMENTS"] == 3
    assert row["ONTIME_PAYMENTS"] == 3
    assert row["LATE_PAYMENTS"] == 0


def test_payment_history_months_since_last_late_falls_back_to_the_open_date(make_df, types):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            account(ACCOUNT_ID=1, ACCOUNT_TYPE="CREDIT", OPEN_DATE="2025-04-10"),
            account(ACCOUNT_ID=2, ACCOUNT_TYPE="LOAN", OPEN_DATE="2025-04-11"),
        ],
    )
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, ACCOUNT_ID=1, TRANSACTION_TYPE_CD="DEP"),
            txn(TRANSACTION_ID=2, ACCOUNT_ID=2, TRANSACTION_TYPE_CD="DEP"),
        ],
    )

    rows = {
        row["ACCOUNT_ID"]: row
        for row in transform_payment_history(
            transactions, accounts, types, run_date=RUN_DATE
        ).collect()
    }

    # exactly 12 months for the 2025-04-10 account; the extra day truncates back to 11
    assert rows[1]["MONTHS_SINCE_LAST_LATE"] == 12
    assert rows[2]["MONTHS_SINCE_LAST_LATE"] == 11


# ---------------------------------------------------------------------------------------------
# Overdraft / NSF
# ---------------------------------------------------------------------------------------------


def test_overdraft_counts_every_negative_balance_transaction_on_any_account_type(make_df, types):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            account(ACCOUNT_ID=1, ACCOUNT_TYPE="CHECKING"),
            account(ACCOUNT_ID=2, ACCOUNT_TYPE="SAVINGS"),
        ],
    )
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, ACCOUNT_ID=1, RUNNING_BALANCE=-1.00),
            txn(TRANSACTION_ID=2, ACCOUNT_ID=1, RUNNING_BALANCE=-2.00),
            txn(TRANSACTION_ID=3, ACCOUNT_ID=2, RUNNING_BALANCE=-3.00),
            txn(TRANSACTION_ID=4, ACCOUNT_ID=1, RUNNING_BALANCE=0.00),
            txn(TRANSACTION_ID=5, ACCOUNT_ID=1, RUNNING_BALANCE=5.00),
        ],
    )

    row = transform_overdraft_nsf(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert row["OVERDRAFT_COUNT"] == 3


def test_nsf_total_matches_fee_category_with_nsf_in_the_description(make_df, accounts, types):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_TYPE_CD="NSF", AMOUNT=-35.00),
            txn(TRANSACTION_ID=2, TRANSACTION_TYPE_CD="NSF", AMOUNT=35.00),
            txn(TRANSACTION_ID=3, TRANSACTION_TYPE_CD="MFE", AMOUNT=-12.00),
            txn(TRANSACTION_ID=4, TRANSACTION_TYPE_CD="PUR", AMOUNT=-99.00),
        ],
    )

    row = transform_overdraft_nsf(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert float(row["NSF_TOTAL"]) == 70.00


def test_overdraft_window_is_twelve_months_and_posted_only(make_df, accounts, types):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE=TWELVE_MONTHS_AGO, RUNNING_BALANCE=-1.00),
            txn(TRANSACTION_ID=2, TRANSACTION_DATE="2025-04-09", RUNNING_BALANCE=-1.00),
            txn(
                TRANSACTION_ID=3,
                TRANSACTION_DATE="2026-01-01",
                RUNNING_BALANCE=-1.00,
                STATUS_CODE="H",
            ),
        ],
    )

    row = transform_overdraft_nsf(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert row["OVERDRAFT_COUNT"] == 1


# ---------------------------------------------------------------------------------------------
# Large withdrawals
# ---------------------------------------------------------------------------------------------


def test_large_withdrawals_threshold_is_inclusive_and_uses_absolute_amounts(
    make_df, accounts, types
):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, AMOUNT=-5000.00),
            txn(TRANSACTION_ID=2, AMOUNT=-4999.99),
            txn(TRANSACTION_ID=3, AMOUNT=-7500.50),
            txn(TRANSACTION_ID=4, AMOUNT=-6000.00, TRANSACTION_TYPE_CD="DEP"),
            txn(TRANSACTION_ID=5, AMOUNT=-6000.00, TRANSACTION_DATE="2025-04-09"),
        ],
    )

    row = transform_large_withdrawals(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert row["LARGE_WD_CNT"] == 2
    assert float(row["LARGE_WD_AMT"]) == 12500.50


def test_large_withdrawals_yields_no_row_when_nothing_qualifies(make_df, accounts, types):
    transactions = make_df(schemas.TRANSACTIONS, [txn(AMOUNT=-10.00)])

    assert (
        transform_large_withdrawals(transactions, accounts, types, run_date=RUN_DATE).count() == 0
    )


# ---------------------------------------------------------------------------------------------
# Balance metrics
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def daily_balance(spark, make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE="2026-04-01", RUNNING_BALANCE=100.00),
            txn(TRANSACTION_ID=2, TRANSACTION_DATE=THIRTY_DAYS_AGO, RUNNING_BALANCE=200.00),
            txn(TRANSACTION_ID=3, TRANSACTION_DATE="2026-03-10", RUNNING_BALANCE=300.00),
            txn(TRANSACTION_ID=4, TRANSACTION_DATE=THREE_MONTHS_AGO, RUNNING_BALANCE=400.00),
        ],
    )
    return transform_daily_balance(transactions, accounts, run_date=RUN_DATE)


def test_balance_metrics_use_their_own_windows_but_share_the_volatility_set(daily_balance):
    row = transform_balance_metrics(daily_balance, run_date=RUN_DATE).collect()[0]

    # 30-day window: 100 and 200 (the 2026-03-11 row is on the boundary and included)
    assert float(row["AVG_BAL_30D"]) == 150.00
    # 90-day window: CURRENT_DATE - 90 is 2026-01-10, so all four rows qualify
    assert float(row["AVG_BAL_90D"]) == 250.00
    # STDDEV_POP is computed over the whole three-month work set
    assert round(float(row["BAL_STDDEV"]), 6) == round(111.80339887498948, 6)


def test_balance_metrics_are_null_when_the_window_is_empty(make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [txn(TRANSACTION_ID=1, TRANSACTION_DATE="2026-01-20", RUNNING_BALANCE=50.00)],
    )
    balance = transform_daily_balance(transactions, accounts, run_date=RUN_DATE)

    row = transform_balance_metrics(balance, run_date=RUN_DATE).collect()[0]

    assert row["AVG_BAL_30D"] is None
    assert float(row["AVG_BAL_90D"]) == 50.00
    assert float(row["BAL_STDDEV"]) == 0.0


# ---------------------------------------------------------------------------------------------
# Credit exposure, payment summary, bureau
# ---------------------------------------------------------------------------------------------


def test_credit_exposure_sums_open_credit_accounts_only_and_treats_nulls_as_zero(make_df):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            account(
                ACCOUNT_ID=1, ACCOUNT_TYPE="CREDIT", CURRENT_BALANCE=500.00, CREDIT_LIMIT=2000.00
            ),
            account(ACCOUNT_ID=2, ACCOUNT_TYPE="CREDIT", CURRENT_BALANCE=None, CREDIT_LIMIT=None),
            account(
                ACCOUNT_ID=3,
                ACCOUNT_TYPE="CREDIT",
                ACCOUNT_STATUS="C",
                CURRENT_BALANCE=999.00,
                CREDIT_LIMIT=999.00,
            ),
            account(ACCOUNT_ID=4, ACCOUNT_TYPE="CHECKING", CURRENT_BALANCE=1.00, CREDIT_LIMIT=1.00),
        ],
    )

    row = transform_credit_exposure(accounts).collect()[0]

    assert float(row["TOTAL_CREDIT_BAL"]) == 500.00
    assert float(row["TOTAL_CREDIT_LIMIT"]) == 2000.00


def test_payment_summary_sums_accounts_and_takes_the_smallest_months_since_late(spark):
    history = spark.createDataFrame(
        [(1, 10, 4, 4, 0, 30), (1, 11, 6, 6, 0, 12), (2, 12, 1, 1, 0, 99)],
        "CUSTOMER_ID long, ACCOUNT_ID long, TOTAL_PAYMENTS long, ONTIME_PAYMENTS long, "
        "LATE_PAYMENTS long, MONTHS_SINCE_LAST_LATE int",
    )

    rows = {row["CUSTOMER_ID"]: row for row in transform_payment_summary(history).collect()}

    assert rows[1]["TOTAL_PAYMENTS"] == 10
    assert rows[1]["ONTIME_PAYMENTS"] == 10
    assert rows[1]["MONTHS_SINCE_LAST_LATE"] == 12
    assert rows[2]["MONTHS_SINCE_LAST_LATE"] == 99


def test_latest_bureau_score_picks_the_newest_report_with_a_deterministic_tiebreak(make_df):
    bureau = make_df(
        schemas.CUSTOMER_BUREAU_SCORES,
        [
            {"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": 600, "REPORT_DATE": "2025-01-01"},
            {"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": 700, "REPORT_DATE": "2026-01-01"},
            {"CUSTOMER_ID": 2, "EXTERNAL_CREDIT_SCORE": 500, "REPORT_DATE": "2026-02-02"},
            {"CUSTOMER_ID": 2, "EXTERNAL_CREDIT_SCORE": 800, "REPORT_DATE": "2026-02-02"},
        ],
    )

    rows = {
        row["CUSTOMER_ID"]: row["CREDIT_SCORE"]
        for row in transform_latest_bureau_score(bureau).collect()
    }

    assert rows == {1: 700, 2: 800}


# ---------------------------------------------------------------------------------------------
# Debit velocity
# ---------------------------------------------------------------------------------------------


def test_debit_velocity_splits_the_seven_and_thirty_day_windows(make_df, accounts, types):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE=SEVEN_DAYS_AGO, AMOUNT=-100.00),
            txn(TRANSACTION_ID=2, TRANSACTION_DATE="2026-04-02", AMOUNT=-50.00),
            txn(TRANSACTION_ID=3, TRANSACTION_DATE=THIRTY_DAYS_AGO, AMOUNT=-25.00),
            txn(TRANSACTION_ID=4, TRANSACTION_DATE="2026-03-10", AMOUNT=-1000.00),
            txn(
                TRANSACTION_ID=5,
                TRANSACTION_DATE="2026-04-05",
                AMOUNT=-500.00,
                TRANSACTION_TYPE_CD="DEP",
            ),
            txn(TRANSACTION_ID=6, TRANSACTION_DATE="2026-04-05", AMOUNT=-500.00, STATUS_CODE="R"),
        ],
    )

    row = transform_debit_velocity(transactions, accounts, types, run_date=RUN_DATE).collect()[0]

    assert float(row["DEBIT_7D"]) == 100.00
    assert float(row["DEBIT_30D"]) == 175.00


# ---------------------------------------------------------------------------------------------
# Merchant risk
# ---------------------------------------------------------------------------------------------


def test_merchants_seen_before_ignores_status_and_has_no_lower_date_bound(make_df):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE="2020-01-01", MERCHANT_NAME="Ancient"),
            txn(
                TRANSACTION_ID=2,
                TRANSACTION_DATE="2026-03-01",
                MERCHANT_NAME="Reversed",
                STATUS_CODE="R",
            ),
            txn(TRANSACTION_ID=3, TRANSACTION_DATE="2026-03-01", MERCHANT_NAME=None),
            txn(TRANSACTION_ID=4, TRANSACTION_DATE=THIRTY_DAYS_AGO, MERCHANT_NAME="OnCutoff"),
            txn(TRANSACTION_ID=5, TRANSACTION_DATE="2026-03-01", MERCHANT_NAME="Ancient"),
        ],
    )

    seen = {
        (row["ACCOUNT_ID"], row["MERCHANT_NAME"])
        for row in transform_merchants_seen_before(transactions, run_date=RUN_DATE).collect()
    }

    assert seen == {(1, "Ancient"), (1, "Reversed")}


def test_merchant_risk_new_merchant_set_is_per_account_but_counted_per_customer(make_df):
    accounts = make_df(
        schemas.ACCOUNTS,
        [account(ACCOUNT_ID=1, CUSTOMER_ID=1), account(ACCOUNT_ID=2, CUSTOMER_ID=1)],
    )
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            # history before the cut-off: account 1 knows Shared and Old
            txn(
                TRANSACTION_ID=1,
                ACCOUNT_ID=1,
                TRANSACTION_DATE="2026-02-01",
                MERCHANT_NAME="Shared",
            ),
            txn(TRANSACTION_ID=2, ACCOUNT_ID=1, TRANSACTION_DATE="2026-02-01", MERCHANT_NAME="Old"),
            # last 30 days
            txn(TRANSACTION_ID=3, ACCOUNT_ID=1, TRANSACTION_DATE="2026-04-01", MERCHANT_NAME="Old"),
            txn(
                TRANSACTION_ID=4,
                ACCOUNT_ID=2,
                TRANSACTION_DATE="2026-04-01",
                MERCHANT_NAME="Shared",
            ),
            txn(
                TRANSACTION_ID=5, ACCOUNT_ID=1, TRANSACTION_DATE="2026-04-02", MERCHANT_NAME="Fresh"
            ),
            txn(
                TRANSACTION_ID=6, ACCOUNT_ID=2, TRANSACTION_DATE="2026-04-02", MERCHANT_NAME="Fresh"
            ),
            txn(TRANSACTION_ID=7, ACCOUNT_ID=2, TRANSACTION_DATE="2026-04-03", MERCHANT_NAME=None),
        ],
    )

    row = transform_merchant_risk(transactions, accounts, run_date=RUN_DATE).collect()[0]

    # Shared is new to account 2, Fresh is new to both accounts but counted once; Old is not new
    assert row["NEW_MERCH_30D"] == 2


def test_merchant_risk_counts_international_and_high_risk_over_six_months(make_df, accounts):
    transactions = make_df(
        schemas.TRANSACTIONS,
        [
            txn(TRANSACTION_ID=1, TRANSACTION_DATE=SIX_MONTHS_AGO, CHANNEL_CODE="INTL"),
            txn(TRANSACTION_ID=2, TRANSACTION_DATE="2025-10-09", CHANNEL_CODE="INTL"),
            txn(
                TRANSACTION_ID=3,
                TRANSACTION_DATE="2026-01-01",
                CHANNEL_CODE="INTL",
                STATUS_CODE="R",
            ),
            txn(TRANSACTION_ID=4, TRANSACTION_DATE="2026-01-02", MERCHANT_CATEGORY="GAMBLING"),
            txn(
                TRANSACTION_ID=5,
                TRANSACTION_DATE="2026-01-03",
                MERCHANT_CATEGORY="WIRE_TRANSFER_INTL",
            ),
            txn(
                TRANSACTION_ID=6, TRANSACTION_DATE="2026-01-04", MERCHANT_CATEGORY="CRYPTO_EXCHANGE"
            ),
            txn(TRANSACTION_ID=7, TRANSACTION_DATE="2026-01-05", MERCHANT_CATEGORY="PAWN_SHOP"),
            txn(TRANSACTION_ID=8, TRANSACTION_DATE="2026-01-06", MERCHANT_CATEGORY="GROCERY"),
        ],
    )

    row = transform_merchant_risk(transactions, accounts, run_date=RUN_DATE).collect()[0]

    assert row["INTL_TXN_CNT"] == 1
    assert row["HIGH_RISK_CNT"] == 4
    assert row["NEW_MERCH_30D"] is None


def test_merchant_risk_ignores_transaction_types(make_df, accounts):
    """The merchant subquery is the only one that does not join TRANSACTION_TYPES."""

    transactions = make_df(
        schemas.TRANSACTIONS,
        [txn(TRANSACTION_ID=1, TRANSACTION_TYPE_CD="UNKNOWN_CODE", CHANNEL_CODE="INTL")],
    )

    assert (
        transform_merchant_risk(transactions, accounts, run_date=RUN_DATE).collect()[0][
            "INTL_TXN_CNT"
        ]
        == 1
    )


# ---------------------------------------------------------------------------------------------
# Whole transform
# ---------------------------------------------------------------------------------------------


@pytest.fixture
def customers(make_df):
    return make_df(
        schemas.CUSTOMERS,
        [
            {"CUSTOMER_ID": 1, "CUSTOMER_STATUS": "A"},
            {"CUSTOMER_ID": 2, "CUSTOMER_STATUS": "I"},
            {"CUSTOMER_ID": 3, "CUSTOMER_STATUS": "C"},
        ],
    )


def test_risk_factors_keeps_active_and_inactive_customers_only(customers, make_df, types, load_ts):
    result = transform_risk_factors(
        customers,
        make_df(schemas.ACCOUNTS, []),
        make_df(schemas.CUSTOMER_BUREAU_SCORES, []),
        make_df(schemas.TRANSACTIONS, []),
        types,
        run_date=RUN_DATE,
        load_ts=load_ts,
    )

    assert sorted(row["CUSTOMER_ID"] for row in result.collect()) == [1, 2]
    assert result.columns == list(schemas.STG_RISK_FACTORS.column_names)


def test_risk_factors_defaults_for_a_customer_without_any_activity(
    customers, make_df, types, load_ts
):
    row = (
        transform_risk_factors(
            customers,
            make_df(schemas.ACCOUNTS, []),
            make_df(schemas.CUSTOMER_BUREAU_SCORES, []),
            make_df(schemas.TRANSACTIONS, []),
            types,
            run_date=RUN_DATE,
            load_ts=load_ts,
        )
        .filter("CUSTOMER_ID = 1")
        .collect()[0]
    )

    assert row["ACCOUNT_OVERDRAFT_CNT"] == 0
    assert float(row["NSF_FEE_TOTAL"]) == 0.00
    assert row["LARGE_WITHDRAWAL_CNT"] == 0
    assert float(row["LARGE_WITHDRAWAL_AMT"]) == 0.00
    assert float(row["AVG_DAILY_BALANCE_30D"]) == 0.00
    assert float(row["AVG_DAILY_BALANCE_90D"]) == 0.00
    assert float(row["BALANCE_VOLATILITY"]) == 0.0000
    assert float(row["CREDIT_UTIL_RATIO"]) == 0.0000
    assert float(row["PAYMENT_ONTIME_PCT"]) == 100.00
    assert row["PAYMENT_LATE_CNT"] == 0
    assert row["MONTHS_SINCE_LAST_LATE"] == 999
    assert row["EXTERNAL_CREDIT_SCORE"] == 0
    assert float(row["DEBIT_VELOCITY_7D"]) == 0.00
    assert row["NEW_MERCHANT_CNT_30D"] == 0
    assert row["INTERNATIONAL_TXN_CNT"] == 0
    assert row["HIGH_RISK_MERCHANT_CNT"] == 0


def test_risk_factors_credit_utilisation_branches(customers, make_df, types, load_ts):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            account(
                ACCOUNT_ID=1,
                CUSTOMER_ID=1,
                ACCOUNT_TYPE="CREDIT",
                CURRENT_BALANCE=250.00,
                CREDIT_LIMIT=1000.00,
            ),
            account(
                ACCOUNT_ID=2,
                CUSTOMER_ID=2,
                ACCOUNT_TYPE="CREDIT",
                CURRENT_BALANCE=250.00,
                CREDIT_LIMIT=0.00,
            ),
        ],
    )

    rows = {
        row["CUSTOMER_ID"]: row
        for row in transform_risk_factors(
            customers,
            accounts,
            make_df(schemas.CUSTOMER_BUREAU_SCORES, []),
            make_df(schemas.TRANSACTIONS, []),
            types,
            run_date=RUN_DATE,
            load_ts=load_ts,
        ).collect()
    }

    assert float(rows[1]["CREDIT_UTIL_RATIO"]) == 0.2500
    assert float(rows[2]["CREDIT_UTIL_RATIO"]) == 0.0000


def test_risk_factors_uses_the_latest_bureau_score_and_never_imputes(
    customers, make_df, types, load_ts
):
    bureau = make_df(
        schemas.CUSTOMER_BUREAU_SCORES,
        [
            {"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": 640, "REPORT_DATE": "2024-01-01"},
            {"CUSTOMER_ID": 1, "EXTERNAL_CREDIT_SCORE": 720, "REPORT_DATE": "2026-03-01"},
        ],
    )

    rows = {
        row["CUSTOMER_ID"]: row["EXTERNAL_CREDIT_SCORE"]
        for row in transform_risk_factors(
            customers,
            make_df(schemas.ACCOUNTS, []),
            bureau,
            make_df(schemas.TRANSACTIONS, []),
            types,
            run_date=RUN_DATE,
            load_ts=load_ts,
        ).collect()
    }

    assert rows == {1: 720, 2: 0}
