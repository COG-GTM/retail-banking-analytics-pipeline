"""Ticket 5 - Transaction summary staging business logic."""
from __future__ import annotations

from datetime import date, datetime

from jobs.stg_txn_summary import build_stg_txn_summary
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_TXN_SCHEMA = (
    "transaction_id long, account_id long, transaction_type_cd string, "
    "transaction_date date, amount decimal(15,2), merchant_name string, "
    "merchant_category string, channel_code string, status_code string"
)
_TT_SCHEMA = "transaction_type_cd string, category string"
_ACCT_SCHEMA = "account_id long, customer_id long, account_type string"


def _build(spark, txns):
    transactions = make_df(spark, _TXN_SCHEMA, txns)
    ttypes = make_df(
        spark, _TT_SCHEMA, [("DR", "DEBIT"), ("CR", "CREDIT"), ("FE", "FEE")]
    )
    accounts = make_df(spark, _ACCT_SCHEMA, [(10, 1, "CHECKING")])
    return build_stg_txn_summary(
        transactions, ttypes, accounts, lookback_months=12,
        run_date=RUN_DATE, load_ts=LOAD_TS,
    )


def test_posted_only_and_lookback_window(spark):
    txns = [
        (1, 10, "DR", date(2026, 3, 1), -100, "M1", "GROCERY", "POS", "P"),
        (2, 10, "DR", date(2026, 3, 2), -50, "M2", "GROCERY", "ATM", "H"),   # held -> excluded
        (3, 10, "DR", date(2024, 1, 1), -999, "M3", "GROCERY", "POS", "P"),  # >12mo -> excluded
    ]
    row = _build(spark, txns).collect()[0]
    assert row.txn_count_total == 1
    assert float(row.amt_total_debit) == 100.0
    assert row.summary_period_start == date(2025, 4, 10)
    assert row.summary_period_end == RUN_DATE


def test_top_merchant_category_by_absolute_spend(spark):
    txns = [
        (1, 10, "DR", date(2026, 3, 1), -100, "A", "GROCERY", "POS", "P"),
        (2, 10, "DR", date(2026, 3, 2), -300, "B", "TRAVEL", "POS", "P"),
        (3, 10, "DR", date(2026, 3, 3), -50, "C", "GROCERY", "POS", "P"),
    ]
    row = _build(spark, txns).collect()[0]
    # TRAVEL total abs spend 300 > GROCERY 150.
    assert row.top_merchant_category == "TRAVEL"


def test_channel_percentages_and_days_since_last(spark):
    txns = [
        (1, 10, "DR", date(2026, 4, 1), -100, "A", "GROCERY", "WEB", "P"),
        (2, 10, "DR", date(2026, 4, 5), -100, "B", "GROCERY", "MOB", "P"),
        (3, 10, "CR", date(2026, 4, 9), 200, "C", "GROCERY", "ATM", "P"),
        (4, 10, "FE", date(2026, 4, 9), -10, None, None, "POS", "P"),
    ]
    row = _build(spark, txns).collect()[0]
    assert row.txn_count_total == 4
    assert row.txn_count_debit == 2 and row.txn_count_credit == 1 and row.txn_count_fee == 1
    assert float(row.pct_web) == 25.0 and float(row.pct_mobile) == 25.0
    assert float(row.amt_total_credit) == 200.0
    assert float(row.amt_total_fees) == 10.0
    assert row.days_since_last_txn == 1  # last txn 2026-04-09, run 2026-04-10
    assert row.distinct_merchants == 3  # A, B, C (null not counted)
