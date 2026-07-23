"""Ticket 4 - Customer 360 staging business logic."""
from __future__ import annotations

from datetime import date, datetime

from jobs.stg_customer_360 import build_stg_customer_360
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_CUST_SCHEMA = (
    "customer_id long, first_name string, last_name string, date_of_birth date, "
    "customer_since date, customer_status string, segment_code string, branch_id int"
)
_ACCT_SCHEMA = (
    "customer_id long, account_type string, account_status string, "
    "current_balance decimal(15,2), credit_limit decimal(15,2)"
)
_ADDR_SCHEMA = (
    "customer_id long, address_id long, address_type string, address_line_1 string, "
    "address_line_2 string, city string, state_code string, zip_code string, "
    "effective_date date, expiration_date date"
)


def _customers(spark, rows):
    return make_df(spark, _CUST_SCHEMA, rows)


def _accounts(spark, rows):
    return make_df(spark, _ACCT_SCHEMA, rows)


def _addresses(spark, rows):
    return make_df(spark, _ADDR_SCHEMA, rows)


def test_status_filter_excludes_closed_customers(spark):
    custs = _customers(spark, [
        (1, "A", "One", date(1985, 5, 1), date(2020, 1, 1), "A", "S", 10),
        (2, "In", "Active", date(1985, 5, 1), date(2020, 1, 1), "I", "S", 10),
        (3, "Closed", "Cust", date(1985, 5, 1), date(2020, 1, 1), "C", "S", 10),
    ])
    accts = _accounts(spark, [(1, "CHECKING", "O", 100, None)])
    addrs = _addresses(spark, [])
    out = build_stg_customer_360(custs, accts, addrs, RUN_DATE, LOAD_TS)
    ids = {r.customer_id for r in out.collect()}
    assert ids == {1, 2}  # 'C' excluded, 'A'/'I' kept


def test_age_is_truncated_not_rounded(spark):
    # 2026-04-10 minus 1985-05-01 = ~40.94 years -> SMALLINT truncation -> 40.
    custs = _customers(spark, [
        (1, "A", "One", date(1985, 5, 1), date(2020, 1, 1), "A", "S", 10),
    ])
    accts = _accounts(spark, [(1, "CHECKING", "O", 100, None)])
    out = build_stg_customer_360(custs, accts, _addresses(spark, []), RUN_DATE, LOAD_TS)
    assert out.collect()[0].age == 40


def test_tenure_months_and_portfolio_and_credit_util(spark):
    custs = _customers(spark, [
        (1, "A", "One", date(1990, 1, 1), date(2025, 4, 10), "A", "S", 10),
    ])
    accts = _accounts(spark, [
        (1, "CHECKING", "O", 500, None),
        (1, "CREDIT", "O", 250, 1000),   # 25% utilisation
        (1, "SAVINGS", "C", 0, None),     # closed -> not active
    ])
    out = build_stg_customer_360(custs, accts, _addresses(spark, []), RUN_DATE, LOAD_TS).collect()[0]
    assert out.tenure_months == 12
    assert out.num_accounts == 3
    assert out.num_active_accounts == 2
    assert out.has_checking == "Y" and out.has_credit == "Y" and out.has_loan == "N"
    assert float(out.total_balance) == 750.0
    assert float(out.credit_utilization_pct) == 25.0


def test_credit_utilization_guarded_against_zero_limit(spark):
    custs = _customers(spark, [
        (1, "A", "One", date(1990, 1, 1), date(2020, 1, 1), "A", "S", 10),
    ])
    accts = _accounts(spark, [(1, "CHECKING", "O", 500, None)])  # no credit -> limit 0
    out = build_stg_customer_360(custs, accts, _addresses(spark, []), RUN_DATE, LOAD_TS).collect()[0]
    assert float(out.credit_utilization_pct) == 0.0


def test_primary_address_picks_latest_non_expired_home(spark):
    custs = _customers(spark, [
        (1, "A", "One", date(1990, 1, 1), date(2020, 1, 1), "A", "S", 10),
    ])
    accts = _accounts(spark, [(1, "CHECKING", "O", 100, None)])
    addrs = _addresses(spark, [
        (1, 100, "HOME", "1 Old St", None, "Oldtown", "CA", "00001", date(2018, 1, 1), None),
        (1, 101, "HOME", "2 New Ave", "Apt 5", "Newtown", "NY", "10001", date(2024, 1, 1), None),
        (1, 102, "HOME", "3 Gone Rd", None, "Goneville", "TX", "70001", date(2025, 1, 1), date(2025, 6, 1)),
        (1, 103, "WORK", "4 Work Blvd", None, "Workcity", "WA", "98001", date(2025, 2, 1), None),
    ])
    out = build_stg_customer_360(custs, accts, addrs, RUN_DATE, LOAD_TS).collect()[0]
    # 102 expired (2025-06-01 < run), 103 not HOME -> latest eligible HOME is 101.
    assert out.city == "Newtown"
    assert out.primary_address == "2 New Ave, Apt 5"
