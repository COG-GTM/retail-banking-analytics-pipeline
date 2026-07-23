"""Ticket 6 - Risk-factor staging business logic."""
from __future__ import annotations

from datetime import date, datetime

from jobs.stg_risk_factors import build_stg_risk_factors
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_TXN_SCHEMA = (
    "account_id long, transaction_type_cd string, transaction_date date, "
    "transaction_ts timestamp, amount decimal(15,2), running_balance decimal(15,2), "
    "merchant_name string, merchant_category string, channel_code string, status_code string"
)
_ACCT_SCHEMA = (
    "account_id long, customer_id long, account_type string, account_status string, "
    "open_date date, current_balance decimal(15,2), credit_limit decimal(15,2)"
)
_TT_SCHEMA = "transaction_type_cd string, category string, description string"
_BUREAU_SCHEMA = "customer_id long, external_credit_score int, report_date date"
_CUST_SCHEMA = "customer_id long, customer_status string"


def _ts(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, 12, 0, 0)


def _run(spark):
    customers = make_df(spark, _CUST_SCHEMA, [(1, "A"), (2, "A"), (3, "A")])
    accounts = make_df(spark, _ACCT_SCHEMA, [
        (10, 1, "CHECKING", "O", date(2020, 1, 1), 100, None),
        (20, 2, "CREDIT", "O", date(2023, 1, 1), 250, 1000),
        (30, 3, "CHECKING", "O", date(2024, 1, 1), 100, None),
    ])
    ttypes = make_df(spark, _TT_SCHEMA, [
        ("DR", "DEBIT", "Debit purchase"),
        ("CR", "CREDIT", "Credit payment"),
        ("FE", "FEE", "NSF FEE"),
    ])
    txns = make_df(spark, _TXN_SCHEMA, [
        # customer 1 / account 10
        (10, "DR", date(2026, 4, 1), _ts(date(2026, 4, 1)), -6000, -100, "NEWSHOP", "GROCERY", "POS", "P"),
        (10, "DR", date(2026, 4, 2), _ts(date(2026, 4, 2)), -50, 500, "OLDSHOP", "GROCERY", "POS", "P"),
        (10, "DR", date(2026, 1, 1), _ts(date(2026, 1, 1)), -50, 500, "OLDSHOP", "GROCERY", "POS", "P"),
        (10, "FE", date(2026, 3, 15), _ts(date(2026, 3, 15)), -35, -20, None, None, "POS", "P"),
        (10, "DR", date(2026, 3, 20), _ts(date(2026, 3, 20)), -100, 200, "CASINO", "GAMBLING", "POS", "P"),
        (10, "DR", date(2026, 3, 25), _ts(date(2026, 3, 25)), -100, 200, "FOREIGN", "TRAVEL", "INTL", "P"),
        # customer 2 / account 20 - one on-time credit payment (no late)
        (20, "CR", date(2026, 3, 1), _ts(date(2026, 3, 1)), 100, 150, "BANK", None, "WEB", "P"),
        # customer 3 / account 30 - checking only, no payment history
        (30, "DR", date(2026, 3, 5), _ts(date(2026, 3, 5)), -20, 80, "SHOP", "GROCERY", "POS", "P"),
    ])
    bureau = make_df(spark, _BUREAU_SCHEMA, [
        (1, 700, date(2025, 1, 1)),
        (1, 720, date(2026, 1, 1)),
    ])
    out = build_stg_risk_factors(
        customers, accounts, txns, ttypes, bureau, RUN_DATE, LOAD_TS
    )
    return {r.customer_id: r for r in out.collect()}


def test_risk_factor_indicators(spark):
    res = _run(spark)
    c1 = res[1]
    assert c1.account_overdraft_cnt == 2          # running_balance < 0 twice
    assert c1.large_withdrawal_cnt == 1
    assert float(c1.large_withdrawal_amt) == 6000.0
    assert float(c1.nsf_fee_total) == 35.0
    assert c1.international_txn_cnt == 1           # channel_code INTL
    assert c1.high_risk_merchant_cnt == 1         # GAMBLING
    assert c1.external_credit_score == 720        # latest bureau report
    # NEWSHOP, CASINO, FOREIGN not seen >30d ago; OLDSHOP was -> 3 new merchants.
    assert c1.new_merchant_cnt_30d == 3


def test_months_since_last_late_account_age_fallback(spark):
    res = _run(spark)
    # customer 2 has payment history but no late payment -> account-age months
    # (~39), NOT the 999 sentinel reserved for "no payment history at all".
    assert res[2].months_since_last_late == 39
    assert float(res[2].payment_ontime_pct) == 100.0
    assert float(res[2].credit_util_ratio) == 0.25


def test_months_since_last_late_defaults_to_999_without_history(spark):
    res = _run(spark)
    c3 = res[3]
    assert c3.months_since_last_late == 999
    assert c3.payment_late_cnt == 0
    assert float(c3.payment_ontime_pct) == 100.0
