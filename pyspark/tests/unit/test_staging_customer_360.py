"""Unit tests for the STG_CUSTOMER_360 transform (BTEQ 01)."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pytest

from common.config import PipelineConfig
from jobs import staging_customer_360 as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _customers(spark):
    return fx.customers(spark, [
        # active, DOB truncates to 23; tenure 28 months
        {"customer_id": 1, "first_name": "Ann", "last_name": "Lee",
         "date_of_birth": _dt.date(2002, 4, 24), "customer_since": _dt.date(2023, 12, 6),
         "customer_status": "A", "segment_code": "DIGITAL", "branch_id": 10},
        # inactive but retained (status I)
        {"customer_id": 2, "first_name": "Bo", "last_name": "Ng",
         "date_of_birth": _dt.date(1980, 1, 1), "customer_since": _dt.date(2010, 1, 1),
         "customer_status": "I", "segment_code": "PREMIER", "branch_id": 11},
        # closed -> excluded
        {"customer_id": 3, "customer_status": "C",
         "date_of_birth": _dt.date(1990, 1, 1), "customer_since": _dt.date(2015, 1, 1)},
    ])


def test_excludes_closed_customers(spark, cfg):
    out = job.transform(_customers(spark), fx.accounts(spark, []), fx.addresses(spark, []), cfg)
    ids = {r.customer_id for r in out.collect()}
    assert ids == {1, 2}


def test_age_and_tenure_truncate(spark, cfg):
    out = job.transform(_customers(spark), fx.accounts(spark, []), fx.addresses(spark, []), cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.age == 23          # 23.96 truncated (SMALLINT), not rounded
    assert row.tenure_months == 28


def test_account_flags_and_balances(spark, cfg):
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "current_balance": Decimal("100.00")},
        {"account_id": 11, "customer_id": 1, "account_type": "CREDIT",
         "account_status": "O", "current_balance": Decimal("50.00"), "credit_limit": Decimal("200.00")},
        {"account_id": 12, "customer_id": 1, "account_type": "SAVINGS",
         "account_status": "C", "current_balance": Decimal("25.00")},
    ])
    out = job.transform(_customers(spark), accts, fx.addresses(spark, []), cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.num_accounts == 3
    assert row.num_active_accounts == 2
    assert row.has_checking == "Y" and row.has_credit == "Y" and row.has_savings == "Y"
    assert row.has_loan == "N"
    assert row.total_balance == Decimal("175.00")
    assert row.total_credit_limit == Decimal("200.00")
    # credit utilization = credit_balance(50) / credit_limit(200) * 100 = 25.00
    assert row.credit_utilization_pct == Decimal("25.00")


def test_credit_utilization_zero_when_no_limit(spark, cfg):
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "current_balance": Decimal("100.00")},
    ])
    out = job.transform(_customers(spark), accts, fx.addresses(spark, []), cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.credit_utilization_pct == Decimal("0.00")


def test_primary_address_most_recent_home(spark, cfg):
    addrs = fx.addresses(spark, [
        {"address_id": 1, "customer_id": 1, "address_type": "HOME",
         "address_line_1": "1 Old St", "address_line_2": None, "city": "Old",
         "state_code": "NY", "zip_code": "10001", "effective_date": _dt.date(2020, 1, 1)},
        {"address_id": 2, "customer_id": 1, "address_type": "HOME",
         "address_line_1": "9 New Ave", "address_line_2": "Apt 5", "city": "New",
         "state_code": "CA", "zip_code": "90001", "effective_date": _dt.date(2024, 1, 1)},
        {"address_id": 3, "customer_id": 1, "address_type": "MAIL",
         "address_line_1": "PO Box", "city": "Mail", "state_code": "TX",
         "zip_code": "75001", "effective_date": _dt.date(2025, 1, 1)},
    ])
    out = job.transform(_customers(spark), fx.accounts(spark, []), addrs, cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.primary_address == "9 New Ave, Apt 5"
    assert row.city == "New" and row.state_code == "CA" and row.zip_code == "90001"


def test_expired_home_address_excluded(spark, cfg):
    addrs = fx.addresses(spark, [
        {"address_id": 1, "customer_id": 1, "address_type": "HOME",
         "address_line_1": "Expired", "city": "Old", "state_code": "NY", "zip_code": "1",
         "effective_date": _dt.date(2024, 1, 1), "expiration_date": _dt.date(2025, 1, 1)},
    ])
    out = job.transform(_customers(spark), fx.accounts(spark, []), addrs, cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.primary_address is None


def test_output_schema_matches_ddl(spark, cfg):
    from common import schemas
    out = job.transform(_customers(spark), fx.accounts(spark, []), fx.addresses(spark, []), cfg)
    assert out.columns == schemas.STG_CUSTOMER_360.column_names
