"""Unit tests for the STG_TXN_SUMMARY transform (BTEQ 02)."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pytest

from common import schemas
from common.config import PipelineConfig
from jobs import staging_txn_summary as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)          # lookback_start -> 2025-04-10
IN_WINDOW = _dt.date(2026, 1, 1)
OUT_OF_WINDOW = _dt.date(2024, 1, 1)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _types(spark):
    return fx.transaction_types(spark, [
        {"transaction_type_cd": "PUR", "category": "DEBIT"},
        {"transaction_type_cd": "DEP", "category": "CREDIT"},
        {"transaction_type_cd": "FEE", "category": "FEE"},
    ])


def _accounts(spark):
    return fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CHECKING"},
        {"account_id": 20, "customer_id": 2, "account_type": "SAVINGS"},
    ])


def _txns(spark):
    return fx.transactions(spark, [
        # ---- account 10 / customer 1: 2 debit, 1 credit, 1 fee (all in window, P) ----
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _dt.date(2026, 1, 1), "amount": Decimal("-100.00"),
         "merchant_name": "A", "merchant_category": "GROCERY", "channel_code": "ATM",
         "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _dt.date(2026, 2, 1), "amount": Decimal("-300.00"),
         "merchant_name": "B", "merchant_category": "TRAVEL", "channel_code": "POS",
         "status_code": "P"},
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "DEP",
         "transaction_date": _dt.date(2026, 3, 1), "amount": Decimal("500.00"),
         "merchant_name": "C", "merchant_category": None, "channel_code": "WEB",
         "status_code": "P"},
        {"transaction_id": 4, "account_id": 10, "transaction_type_cd": "FEE",
         "transaction_date": _dt.date(2026, 3, 15), "amount": Decimal("-25.00"),
         "merchant_name": None, "merchant_category": None, "channel_code": "MOB",
         "status_code": "P"},
        # excluded: before the lookback window
        {"transaction_id": 5, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": OUT_OF_WINDOW, "amount": Decimal("-9999.00"),
         "merchant_name": "OLD", "merchant_category": "TRAVEL", "channel_code": "ATM",
         "status_code": "P"},
        # excluded: not posted (Hold)
        {"transaction_id": 6, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": IN_WINDOW, "amount": Decimal("-8888.00"),
         "merchant_name": "HOLD", "merchant_category": "TRAVEL", "channel_code": "ATM",
         "status_code": "H"},
        # ---- account 20 / customer 2: single credit, no debit ----
        {"transaction_id": 7, "account_id": 20, "transaction_type_cd": "DEP",
         "transaction_date": _dt.date(2026, 4, 1), "amount": Decimal("200.00"),
         "merchant_name": "Z", "merchant_category": "SHOPPING", "channel_code": "WEB",
         "status_code": "P"},
    ])


@pytest.fixture(scope="module")
def out(spark, cfg):
    df = job.transform(_txns(spark), _accounts(spark), _types(spark), cfg)
    return {r.account_id: r for r in df.collect()}


def test_output_schema_matches_ddl(spark, cfg):
    df = job.transform(_txns(spark), _accounts(spark), _types(spark), cfg)
    assert df.columns == schemas.STG_TXN_SUMMARY.column_names


def test_period_literals_and_window_filter(out):
    r = out[10]
    assert r.summary_period_start == _dt.date(2025, 4, 10)
    assert r.summary_period_end == RUN_DATE
    # rows 5 (out-of-window) and 6 (status H) are excluded -> 4 remain, not 6
    assert r.txn_count_total == 4


def test_debit_credit_fee_counts(out):
    r = out[10]
    assert (r.txn_count_debit, r.txn_count_credit, r.txn_count_fee) == (2, 1, 1)


def test_amount_aggregates_abs_vs_raw(out):
    r = out[10]
    assert r.amt_total_debit == Decimal("400.00")   # abs(-100)+abs(-300)
    assert r.amt_total_credit == Decimal("500.00")  # raw signed
    assert r.amt_total_fees == Decimal("25.00")     # abs(-25)
    assert r.amt_avg_debit == Decimal("200.00")     # (100+300)/2
    assert r.amt_avg_credit == Decimal("500.00")
    assert r.amt_max_single_debit == Decimal("300.00")
    assert r.amt_max_single_credit == Decimal("500.00")


def test_distinct_merchants_ignores_null(out):
    # merchants A, B, C counted; the fee row's NULL merchant is ignored
    assert out[10].distinct_merchants == 3


def test_channel_mix_percentages(out):
    r = out[10]
    assert r.pct_atm == Decimal("25.00")
    assert r.pct_pos == Decimal("25.00")
    assert r.pct_web == Decimal("25.00")
    assert r.pct_mobile == Decimal("25.00")


def test_days_since_last_txn(out):
    # datediff(2026-04-10, 2026-03-15) = 26
    assert out[10].days_since_last_txn == 26


def test_top_merchant_category_by_abs_spend(out):
    # GROCERY spend 100 vs TRAVEL spend 300 -> TRAVEL; NULL categories excluded
    assert out[10].top_merchant_category == "TRAVEL"


def test_account_with_no_debits(out):
    r = out[20]
    assert r.txn_count_debit == 0
    assert r.amt_total_debit == Decimal("0.00")
    assert r.amt_avg_debit is None            # AVG over empty set -> NULL
    assert r.amt_max_single_debit == Decimal("0.00")   # ELSE 0 branch
    assert r.amt_total_credit == Decimal("200.00")
    assert r.amt_avg_credit == Decimal("200.00")
    assert r.pct_web == Decimal("100.00")
    assert r.pct_atm == Decimal("0.00")
    assert r.top_merchant_category == "SHOPPING"
    assert r.days_since_last_txn == 9


def test_top_merchant_category_tiebreak_is_deterministic(spark, cfg):
    # Two categories with equal ABS spend -> lexicographically smaller wins.
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": IN_WINDOW, "amount": Decimal("-50.00"),
         "merchant_name": "M1", "merchant_category": "ZED", "channel_code": "POS",
         "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": IN_WINDOW, "amount": Decimal("-50.00"),
         "merchant_name": "M2", "merchant_category": "ALPHA", "channel_code": "POS",
         "status_code": "P"},
    ])
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1, "account_type": "CHECKING"}])
    row = job.transform(txns, accts, _types(spark), cfg).collect()[0]
    assert row.top_merchant_category == "ALPHA"
