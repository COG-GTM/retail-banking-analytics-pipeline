"""Unit tests for staging.stg_customer_360."""
from datetime import date

from conftest import make_df

from staging.stg_customer_360 import OUTPUT_COLUMNS, transform

AS_OF = date(2026, 4, 10)


def _sources():
    customers = [
        {"customer_id": 1, "first_name": "Ada", "last_name": "Byron",
         "date_of_birth": date(2002, 4, 24), "customer_since": date(2023, 12, 6),
         "customer_status": "A", "segment_code": "DIGITAL", "branch_id": 10},
        {"customer_id": 2, "first_name": "Grace", "last_name": "Hopper",
         "date_of_birth": date(1980, 1, 1), "customer_since": date(2010, 1, 1),
         "customer_status": "I", "segment_code": "PREMIER", "branch_id": 20},
        # customer_status 'C' (closed) must be filtered out.
        {"customer_id": 3, "first_name": "Closed", "last_name": "Account",
         "date_of_birth": date(1990, 1, 1), "customer_since": date(2015, 1, 1),
         "customer_status": "C", "segment_code": "MASS", "branch_id": 30},
    ]
    accounts = [
        {"account_id": 100, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "open_date": date(2024, 1, 1), "current_balance": 500.00},
        {"account_id": 101, "customer_id": 1, "account_type": "CREDIT",
         "account_status": "O", "open_date": date(2024, 1, 1),
         "current_balance": 200.00, "credit_limit": 1000.00},
        {"account_id": 102, "customer_id": 1, "account_type": "SAVINGS",
         "account_status": "C", "open_date": date(2024, 1, 1), "current_balance": 50.00},
    ]
    addresses = [
        # Older HOME address (should NOT win).
        {"address_id": 1, "customer_id": 1, "address_type": "HOME",
         "address_line_1": "1 Old St", "city": "Oldtown", "state_code": "NY",
         "zip_code": "00001", "effective_date": date(2020, 1, 1)},
        # Latest HOME address with a line 2 (should win, tests concatenation).
        {"address_id": 2, "customer_id": 1, "address_type": "HOME",
         "address_line_1": "2 New Ave", "address_line_2": "Apt 5",
         "city": "Newtown", "state_code": "CA", "zip_code": "90002",
         "effective_date": date(2025, 1, 1)},
        # Expired HOME address must be excluded.
        {"address_id": 3, "customer_id": 2, "address_type": "HOME",
         "address_line_1": "9 Expired Rd", "city": "Gone", "state_code": "TX",
         "zip_code": "70003", "effective_date": date(2026, 1, 1),
         "expiration_date": date(2026, 1, 5)},
    ]
    return customers, accounts, addresses


def _run(spark):
    customers, accounts, addresses = _sources()
    return transform(
        make_df(spark, "customers", customers),
        make_df(spark, "accounts", accounts),
        make_df(spark, "addresses", addresses),
        AS_OF,
    )


def test_output_schema(spark):
    assert _run(spark).columns == OUTPUT_COLUMNS


def test_status_filter_and_derivations(spark):
    result = _run(spark)
    # Closed customer (status 'C') excluded -> only 2 rows.
    assert result.count() == 2

    row = result.filter("customer_id = 1").collect()[0]
    # age = trunc((2026-04-10 - 2002-04-24)/365.25) = 23 (birthday not yet reached).
    assert row["age"] == 23
    # Latest HOME address wins; line 2 concatenated with ", ".
    assert row["primary_address"] == "2 New Ave, Apt 5"
    assert row["state_code"] == "CA"
    # Portfolio flags.
    assert row["num_accounts"] == 3
    assert row["num_active_accounts"] == 2  # CHECKING + CREDIT open, SAVINGS closed
    assert row["has_checking"] == "Y"
    assert row["has_credit"] == "Y"
    assert row["has_loan"] == "N"
    # credit_utilization_pct = 200 / 1000 * 100 = 20.00
    assert float(row["credit_utilization_pct"]) == 20.00
    # total_balance = 500 + 200 + 50 (exact decimal, no float noise).
    assert float(row["total_balance"]) == 750.00


def test_expired_address_excluded(spark):
    row = _run(spark).filter("customer_id = 2").collect()[0]
    # Customer 2's only HOME address is expired -> address fields are NULL.
    assert row["primary_address"] is None
    assert row["state_code"] is None
