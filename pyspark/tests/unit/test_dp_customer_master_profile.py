"""Unit tests for the CUSTOMER_MASTER_PROFILE transform (SAS 04)."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pytest

from common import schemas
from common.config import PipelineConfig
from jobs import dp_customer_master_profile as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _stg(spark, rows):
    return fx.make_df(spark, schemas.STG_CUSTOMER_360, rows)


def _segments(spark, rows):
    return fx.make_df(spark, schemas.CUSTOMER_SEGMENTS, rows)


def _txn(spark, rows):
    return fx.make_df(spark, schemas.TRANSACTION_ANALYTICS, rows)


def _risk(spark, rows):
    return fx.make_df(spark, schemas.CUSTOMER_RISK_SCORES, rows)


# A fully-populated active customer (id=1) plus a base-only active customer (id=2).
def _base_rows():
    return [
        {"customer_id": 1, "first_name": "Ann ", "last_name": " Lee",
         "customer_status": "A", "age": 33, "state_code": "CA",
         "customer_since": _dt.date(2020, 1, 1), "tenure_months": 75,
         "num_accounts": 4, "num_active_accounts": 3,
         "total_balance": Decimal("1000.00"), "total_credit_limit": Decimal("5000.00"),
         "credit_utilization_pct": Decimal("12.50")},
        {"customer_id": 2, "first_name": "Bo", "last_name": "Ng",
         "customer_status": "A", "age": 40, "state_code": "NY",
         "customer_since": _dt.date(2015, 6, 1), "tenure_months": 130,
         "num_accounts": 1, "num_active_accounts": 1,
         "total_balance": Decimal("50.00"), "total_credit_limit": Decimal("0.00"),
         "credit_utilization_pct": Decimal("0.00")},
        # closed customer -> excluded by the BASE where clause.
        {"customer_id": 3, "first_name": "Cy", "last_name": "Fox", "customer_status": "C"},
    ]


def _populated_products(spark):
    seg = _segments(spark, [
        {"customer_id": 1, "segment_name": "PREMIER", "lifetime_value_score": Decimal("999.99"),
         "engagement_score": Decimal("88.50"), "cross_sell_flag": "Y", "upsell_flag": "Y",
         "retention_risk_flag": "N"},
    ])
    txn = _txn(spark, [
        {"customer_id": 1, "total_transactions": 42, "total_debit_amt": Decimal("1234.56"),
         "net_cash_flow": Decimal("-100.00"), "top_spend_category": "TRAVEL",
         "digital_txn_pct": Decimal("75.00"), "effective_date": RUN_DATE},
    ])
    risk = _risk(spark, [
        {"customer_id": 1, "composite_risk_score": Decimal("620.00"), "risk_tier": "MODERATE",
         "probability_of_default": Decimal("0.012500"), "watch_list_flag": "Y"},
    ])
    return seg, txn, risk


def test_base_only_customer_gets_all_defaults(spark, cfg):
    seg, txn, risk = _populated_products(spark)
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    row = out.filter("customer_id = 2").collect()[0]
    # segment defaults
    assert row.segment_name == "UNCLASSIFIED"
    assert row.lifetime_value_score == Decimal("0.00")
    assert row.engagement_score == Decimal("0.00")
    assert row.cross_sell_flag == "N" and row.upsell_flag == "N" and row.retention_risk_flag == "N"
    # txn defaults
    assert row.monthly_transactions == 0
    assert row.monthly_spend == Decimal("0.00")
    assert row.net_cash_flow == Decimal("0.00")
    assert row.top_spend_category == ""
    assert row.digital_txn_pct == Decimal("0.00")
    # risk defaults
    assert row.composite_risk_score is None
    assert row.risk_tier == "UNKNOWN"
    assert row.probability_of_default is None
    assert row.watch_list_flag == "N"


def test_fully_populated_customer_keeps_joined_values(spark, cfg):
    seg, txn, risk = _populated_products(spark)
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.segment_name == "PREMIER"
    assert row.lifetime_value_score == Decimal("999.99")
    assert row.engagement_score == Decimal("88.50")
    assert row.cross_sell_flag == "Y" and row.upsell_flag == "Y"
    assert row.monthly_transactions == 42
    assert row.monthly_spend == Decimal("1234.56")
    assert row.net_cash_flow == Decimal("-100.00")
    assert row.top_spend_category == "TRAVEL"
    assert row.digital_txn_pct == Decimal("75.00")
    assert row.composite_risk_score == Decimal("620.00")
    assert row.risk_tier == "MODERATE"
    assert row.probability_of_default == Decimal("0.012500")
    assert row.watch_list_flag == "Y"


def test_full_name_and_renamed_columns(spark, cfg):
    seg, txn, risk = _populated_products(spark)
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    row = out.filter("customer_id = 1").collect()[0]
    # trim(first) || ' ' || trim(last): "Ann " and " Lee" -> "Ann Lee".
    assert row.full_name == "Ann Lee"
    # renamed base columns.
    assert row.total_accounts == 4
    assert row.active_accounts == 3
    assert row.total_balance == Decimal("1000.00")
    assert row.total_credit_limit == Decimal("5000.00")
    assert row.credit_utilization_pct == Decimal("12.50")


def test_only_active_customers_in_output(spark, cfg):
    seg, txn, risk = _populated_products(spark)
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    ids = {r.customer_id for r in out.collect()}
    assert ids == {1, 2}


def test_txn_filtered_to_run_date(spark, cfg):
    # customer 1's only txn row is for a different period -> treated as absent.
    seg = _segments(spark, [])
    txn = _txn(spark, [
        {"customer_id": 1, "total_transactions": 99, "total_debit_amt": Decimal("500.00"),
         "net_cash_flow": Decimal("10.00"), "top_spend_category": "GROCERY",
         "digital_txn_pct": Decimal("50.00"), "effective_date": _dt.date(2026, 3, 10)},
    ])
    risk = _risk(spark, [])
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.monthly_transactions == 0
    assert row.monthly_spend == Decimal("0.00")
    assert row.top_spend_category == ""


def test_metadata_and_schema(spark, cfg):
    seg, txn, risk = _populated_products(spark)
    out = job.transform(_stg(spark, _base_rows()), seg, txn, risk, cfg)
    assert out.columns == schemas.CUSTOMER_MASTER_PROFILE.column_names
    schemas.assert_schema(out, schemas.CUSTOMER_MASTER_PROFILE)
    row = out.filter("customer_id = 1").collect()[0]
    assert row.model_version == "MASTER_V1.5"
    assert row.effective_date == RUN_DATE
