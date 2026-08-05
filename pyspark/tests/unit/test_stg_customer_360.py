"""Unit tests for the reference job's pure transforms - the template for every other job."""

from __future__ import annotations

from datetime import date

import pytest

from common import schemas
from jobs.stg_customer_360 import (
    transform_account_aggregates,
    transform_customer_360,
    transform_primary_address,
)

pytestmark = pytest.mark.unit

RUN_DATE = date(2026, 4, 10)


def test_primary_address_picks_latest_unexpired_home_address(make_df):
    addresses = make_df(
        schemas.ADDRESSES,
        [
            {
                "ADDRESS_ID": 1,
                "CUSTOMER_ID": 1,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "1 Old St",
                "CITY": "Oldtown",
                "EFFECTIVE_DATE": "2020-01-01",
            },
            {
                "ADDRESS_ID": 2,
                "CUSTOMER_ID": 1,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "2 New St",
                "CITY": "Newtown",
                "EFFECTIVE_DATE": "2025-01-01",
            },
            {
                "ADDRESS_ID": 3,
                "CUSTOMER_ID": 1,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "3 Newest St",
                "CITY": "Newesttown",
                "EFFECTIVE_DATE": "2026-01-01",
                "EXPIRATION_DATE": "2026-02-01",
            },
            {
                "ADDRESS_ID": 4,
                "CUSTOMER_ID": 1,
                "ADDRESS_TYPE": "WORK",
                "ADDRESS_LINE_1": "4 Work St",
                "CITY": "Worktown",
                "EFFECTIVE_DATE": "2026-03-01",
            },
        ],
    )

    rows = transform_primary_address(addresses, RUN_DATE).collect()

    assert len(rows) == 1
    assert rows[0]["ADDRESS_LINE_1"] == "2 New St"


def test_primary_address_tiebreak_is_deterministic(make_df):
    addresses = make_df(
        schemas.ADDRESSES,
        [
            {
                "ADDRESS_ID": 10,
                "CUSTOMER_ID": 7,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "10 Same Day Rd",
                "EFFECTIVE_DATE": "2025-05-05",
            },
            {
                "ADDRESS_ID": 11,
                "CUSTOMER_ID": 7,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "11 Same Day Rd",
                "EFFECTIVE_DATE": "2025-05-05",
            },
        ],
    )

    rows = transform_primary_address(addresses, RUN_DATE).collect()

    assert [row["ADDRESS_LINE_1"] for row in rows] == ["11 Same Day Rd"]


def test_account_aggregates_flags_products_and_sums_credit_only_limits(make_df):
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            {
                "ACCOUNT_ID": 1,
                "CUSTOMER_ID": 1,
                "ACCOUNT_TYPE": "CHECKING",
                "ACCOUNT_STATUS": "O",
                "CURRENT_BALANCE": 100.00,
                "CREDIT_LIMIT": 999.00,
            },
            {
                "ACCOUNT_ID": 2,
                "CUSTOMER_ID": 1,
                "ACCOUNT_TYPE": "CREDIT",
                "ACCOUNT_STATUS": "C",
                "CURRENT_BALANCE": 250.00,
                "CREDIT_LIMIT": 1000.00,
            },
            {
                "ACCOUNT_ID": 3,
                "CUSTOMER_ID": 1,
                "ACCOUNT_TYPE": "SAVINGS",
                "ACCOUNT_STATUS": "O",
                "CURRENT_BALANCE": None,
                "CREDIT_LIMIT": None,
            },
        ],
    )

    row = transform_account_aggregates(accounts).collect()[0]

    assert row["NUM_ACCOUNTS"] == 3
    assert row["NUM_ACTIVE_ACCOUNTS"] == 2
    assert (row["HAS_CHECKING"], row["HAS_SAVINGS"], row["HAS_CREDIT"], row["HAS_LOAN"]) == (
        "Y",
        "Y",
        "Y",
        "N",
    )
    assert float(row["TOTAL_BALANCE"]) == 350.00
    # only CREDIT accounts contribute to the limit and to the utilisation numerator
    assert float(row["TOTAL_CREDIT_LIMIT"]) == 1000.00
    assert float(row["CREDIT_BALANCE"]) == 250.00


@pytest.fixture
def customers(make_df):
    return make_df(
        schemas.CUSTOMERS,
        [
            {
                "CUSTOMER_ID": 1,
                "FIRST_NAME": "Ada",
                "LAST_NAME": "Lovelace",
                "DATE_OF_BIRTH": "1990-04-10",
                "CUSTOMER_SINCE": "2020-04-10",
                "CUSTOMER_STATUS": "A",
                "SEGMENT_CODE": "MASS",
                "BRANCH_ID": 7,
            },
            {
                "CUSTOMER_ID": 2,
                "FIRST_NAME": "Alan",
                "LAST_NAME": "Turing",
                "DATE_OF_BIRTH": "1980-01-01",
                "CUSTOMER_SINCE": "2010-01-01",
                "CUSTOMER_STATUS": "C",
                "SEGMENT_CODE": "MASS",
                "BRANCH_ID": 7,
            },
            {
                "CUSTOMER_ID": 3,
                "FIRST_NAME": "Grace",
                "LAST_NAME": "Hopper",
                "DATE_OF_BIRTH": "2000-06-01",
                "CUSTOMER_SINCE": "2024-06-01",
                "CUSTOMER_STATUS": "I",
                "SEGMENT_CODE": "DIGITAL",
                "BRANCH_ID": 8,
            },
        ],
    )


def test_customer_360_excludes_closed_customers_and_keeps_inactive(customers, make_df, load_ts):
    empty_addresses = make_df(schemas.ADDRESSES, [])
    empty_accounts = make_df(schemas.ACCOUNTS, [])

    result = transform_customer_360(
        customers, empty_addresses, empty_accounts, run_date=RUN_DATE, load_ts=load_ts
    )

    assert sorted(row["CUSTOMER_ID"] for row in result.collect()) == [1, 3]


def test_customer_360_age_tenure_and_utilisation(customers, make_df, load_ts):
    addresses = make_df(
        schemas.ADDRESSES,
        [
            {
                "ADDRESS_ID": 1,
                "CUSTOMER_ID": 1,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": " 1 Main St ",
                "ADDRESS_LINE_2": " Apt 2 ",
                "CITY": "Springfield",
                "STATE_CODE": "IL",
                "ZIP_CODE": "62701",
                "EFFECTIVE_DATE": "2024-01-01",
            },
            {
                "ADDRESS_ID": 2,
                "CUSTOMER_ID": 3,
                "ADDRESS_TYPE": "HOME",
                "ADDRESS_LINE_1": "9 Only St",
                "ADDRESS_LINE_2": None,
                "CITY": "Shelbyville",
                "STATE_CODE": "IL",
                "ZIP_CODE": "62702",
                "EFFECTIVE_DATE": "2024-01-01",
            },
        ],
    )
    accounts = make_df(
        schemas.ACCOUNTS,
        [
            {
                "ACCOUNT_ID": 1,
                "CUSTOMER_ID": 1,
                "ACCOUNT_TYPE": "CREDIT",
                "ACCOUNT_STATUS": "O",
                "CURRENT_BALANCE": 500.00,
                "CREDIT_LIMIT": 2000.00,
            }
        ],
    )

    rows = {
        row["CUSTOMER_ID"]: row
        for row in transform_customer_360(
            customers, addresses, accounts, run_date=RUN_DATE, load_ts=load_ts
        ).collect()
    }

    assert rows[1]["AGE"] == 36
    assert rows[1]["TENURE_MONTHS"] == 72
    assert rows[1]["PRIMARY_ADDRESS"] == "1 Main St, Apt 2"
    assert float(rows[1]["CREDIT_UTILIZATION_PCT"]) == 25.00
    # no ADDRESS_LINE_2 -> no separator, and no accounts -> utilisation defaults to 0.00
    assert rows[3]["PRIMARY_ADDRESS"] == "9 Only St"
    assert float(rows[3]["CREDIT_UTILIZATION_PCT"]) == 0.00
    assert rows[3]["NUM_ACCOUNTS"] is None


def test_customer_360_matches_the_ddl_contract(customers, make_df, load_ts):
    result = transform_customer_360(
        customers,
        make_df(schemas.ADDRESSES, []),
        make_df(schemas.ACCOUNTS, []),
        run_date=RUN_DATE,
        load_ts=load_ts,
    )

    assert result.columns == list(schemas.STG_CUSTOMER_360.column_names)
