"""Unit tests for the STG_RISK_FACTORS transform helpers (BTEQ 03)."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pytest

from common import schemas
from common.config import PipelineConfig
from jobs import staging_risk_factors as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


# Standard TRANSACTION_TYPES dimension used across the txn-based helpers.
def _types(spark):
    return fx.transaction_types(spark, [
        {"transaction_type_cd": "PUR", "category": "DEBIT", "description": "Purchase"},
        {"transaction_type_cd": "WDR", "category": "DEBIT", "description": "Withdrawal"},
        {"transaction_type_cd": "PMT", "category": "CREDIT", "description": "Payment"},
        {"transaction_type_cd": "FEE", "category": "FEE", "description": "Account Fee"},
        {"transaction_type_cd": "NSF", "category": "FEE", "description": "NSF Fee"},
    ])


def _d(s: str) -> _dt.date:
    return _dt.date.fromisoformat(s)


def _ts(s: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(s)


# --------------------------------------------------------------------------- #
# Overdraft / NSF                                                             #
# --------------------------------------------------------------------------- #
def test_overdraft_and_nsf(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        # two negative running balances -> overdraft_cnt = 2
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-01-10"), "amount": Decimal("-40.00"),
         "running_balance": Decimal("-5.00"), "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "WDR",
         "transaction_date": _d("2026-02-10"), "amount": Decimal("-10.00"),
         "running_balance": Decimal("-1.00"), "status_code": "P"},
        # NSF fee -> counted into nsf_fee_total (abs), positive balance (no overdraft)
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "NSF",
         "transaction_date": _d("2026-03-01"), "amount": Decimal("-35.00"),
         "running_balance": Decimal("100.00"), "status_code": "P"},
        # ordinary FEE (not NSF) -> excluded from nsf_fee_total
        {"transaction_id": 4, "account_id": 10, "transaction_type_cd": "FEE",
         "transaction_date": _d("2026-03-02"), "amount": Decimal("-12.00"),
         "running_balance": Decimal("88.00"), "status_code": "P"},
        # non-posted -> excluded entirely
        {"transaction_id": 5, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-03-03"), "amount": Decimal("-99.00"),
         "running_balance": Decimal("-50.00"), "status_code": "H"},
    ])
    row = job.overdraft_nsf(txns, accts, _types(spark), RUN_DATE).collect()[0]
    assert row.account_overdraft_cnt == 2
    assert Decimal(str(row.nsf_fee_total)) == Decimal("35.00")


# --------------------------------------------------------------------------- #
# Large withdrawals                                                           #
# --------------------------------------------------------------------------- #
def test_large_withdrawals(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "WDR",
         "transaction_date": _d("2026-01-10"), "amount": Decimal("-5000.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-02-10"), "amount": Decimal("-9000.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        # below threshold -> excluded
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "WDR",
         "transaction_date": _d("2026-02-11"), "amount": Decimal("-4999.99"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        # CREDIT category, big amount -> excluded (not a DEBIT)
        {"transaction_id": 4, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-02-12"), "amount": Decimal("8000.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
    ])
    row = job.large_withdrawals(txns, accts, _types(spark), RUN_DATE).collect()[0]
    assert row.large_withdrawal_cnt == 2
    assert Decimal(str(row.large_withdrawal_amt)) == Decimal("14000.00")


# --------------------------------------------------------------------------- #
# Credit utilization                                                          #
# --------------------------------------------------------------------------- #
def test_credit_util_ratio(spark):
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CREDIT", "account_status": "O",
         "current_balance": Decimal("300.00"), "credit_limit": Decimal("1000.00")},
        {"account_id": 11, "customer_id": 1, "account_type": "CREDIT", "account_status": "O",
         "current_balance": Decimal("200.00"), "credit_limit": Decimal("1000.00")},
        # closed credit account -> excluded
        {"account_id": 12, "customer_id": 1, "account_type": "CREDIT", "account_status": "C",
         "current_balance": Decimal("999.00"), "credit_limit": Decimal("1000.00")},
    ])
    row = job.credit_util(accts).collect()[0]
    # (300 + 200) / (1000 + 1000) = 0.25
    assert abs(float(row.credit_util_ratio) - 0.25) < 1e-9


def test_credit_util_zero_limit(spark):
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CREDIT", "account_status": "O",
         "current_balance": Decimal("50.00"), "credit_limit": Decimal("0.00")},
    ])
    row = job.credit_util(accts).collect()[0]
    assert float(row.credit_util_ratio) == 0.0


# --------------------------------------------------------------------------- #
# Balance volatility / averages                                              #
# --------------------------------------------------------------------------- #
def test_balance_metrics_volatility_and_windows(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    # eod balances 100 and 200 -> stddev_pop = 50; both within 3-month window.
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-01"), "transaction_ts": _ts("2026-04-01 10:00:00"),
         "amount": Decimal("-1.00"), "running_balance": Decimal("100.00"), "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-02-01"), "transaction_ts": _ts("2026-02-01 10:00:00"),
         "amount": Decimal("-1.00"), "running_balance": Decimal("200.00"), "status_code": "P"},
    ])
    wdb = job.wrk_daily_balance(txns, accts, RUN_DATE)
    row = job.balance_metrics(wdb, RUN_DATE).collect()[0]
    assert abs(float(row.balance_volatility) - 50.0) < 1e-9
    # 30d window (>= 2026-03-11) only sees the 2026-04-01 balance
    assert abs(float(row.avg_daily_balance_30d) - 100.0) < 1e-9
    # 90d window (>= 2026-01-10) sees both -> avg 150
    assert abs(float(row.avg_daily_balance_90d) - 150.0) < 1e-9


def test_wrk_daily_balance_keeps_last_txn_per_day(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-01"), "transaction_ts": _ts("2026-04-01 09:00:00"),
         "amount": Decimal("-1.00"), "running_balance": Decimal("10.00"), "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-01"), "transaction_ts": _ts("2026-04-01 18:00:00"),
         "amount": Decimal("-1.00"), "running_balance": Decimal("99.00"), "status_code": "P"},
    ])
    rows = job.wrk_daily_balance(txns, accts, RUN_DATE).collect()
    assert len(rows) == 1
    assert Decimal(str(rows[0].eod_balance)) == Decimal("99.00")


# --------------------------------------------------------------------------- #
# Payment history proxy                                                       #
# --------------------------------------------------------------------------- #
def test_payment_ontime_proxy(spark):
    # due = add_months(open, CAST(months_between(txn, open) AS INT) + 1). Because
    # floor(mb)+1 >= mb, `due` is always on/after the payment date, so this legacy
    # proxy classifies every payment as on-time (payment_late_cnt is always 0 --
    # see the fidelity note in the module/PR). Here all 3 payments -> ontime 100%.
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CREDIT",
         "account_status": "O", "open_date": _d("2026-01-15")},
    ])
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-02-15"), "amount": Decimal("50.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-04-20"), "amount": Decimal("50.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-03-01"), "amount": Decimal("50.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
    ])
    wph = job.wrk_payment_history(txns, accts, _types(spark), RUN_DATE)
    out = job.payment_history(wph).collect()[0]
    assert out.payment_late_cnt == 0
    assert float(out.payment_ontime_pct) == 100.0
    # No late payment -> months_since_last_late derives from open_date (2026-01-15):
    # int(months_between(2026-04-10, 2026-01-15)) = 2.
    assert out.months_since_last_late == 2


def test_payment_history_only_credit_and_loan_credit_txns(spark):
    accts = fx.accounts(spark, [
        {"account_id": 10, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "open_date": _d("2026-01-15")},
    ])
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-02-15"), "amount": Decimal("50.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
    ])
    # CHECKING account -> no payment history rows
    assert job.wrk_payment_history(txns, accts, _types(spark), RUN_DATE).count() == 0


# --------------------------------------------------------------------------- #
# Bureau latest-by-date                                                       #
# --------------------------------------------------------------------------- #
def test_bureau_latest_by_report_date(spark):
    bureau = fx.bureau_scores(spark, [
        {"customer_id": 1, "external_credit_score": 700, "report_date": _d("2025-06-01")},
        {"customer_id": 1, "external_credit_score": 760, "report_date": _d("2026-02-01")},
        {"customer_id": 2, "external_credit_score": 610, "report_date": _d("2026-01-01")},
    ])
    rows = {r.customer_id: r.external_credit_score for r in job.bureau_scores(bureau).collect()}
    assert rows == {1: 760, 2: 610}


# --------------------------------------------------------------------------- #
# Debit velocity                                                              #
# --------------------------------------------------------------------------- #
def test_debit_velocity_7d_30d(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        # within 7 days (>= 2026-04-03)
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-05"), "amount": Decimal("-100.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        # within 30 days but not 7 (>= 2026-03-11, < 2026-04-03)
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "WDR",
         "transaction_date": _d("2026-03-20"), "amount": Decimal("-250.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
        # CREDIT -> excluded from debit velocity
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "PMT",
         "transaction_date": _d("2026-04-06"), "amount": Decimal("500.00"),
         "running_balance": Decimal("0.00"), "status_code": "P"},
    ])
    row = job.debit_velocity(txns, accts, _types(spark), RUN_DATE).collect()[0]
    assert Decimal(str(row.debit_velocity_7d)) == Decimal("100.00")
    assert Decimal(str(row.debit_velocity_30d)) == Decimal("350.00")


# --------------------------------------------------------------------------- #
# New merchant first-seen rewrite                                             #
# --------------------------------------------------------------------------- #
def test_new_merchant_first_seen_rewrite(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        # ACME first used long ago -> NOT new even though used again in last 30 days
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2025-01-01"), "amount": Decimal("-10.00"),
         "running_balance": Decimal("0.00"), "merchant_name": "ACME", "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-05"), "amount": Decimal("-10.00"),
         "running_balance": Decimal("0.00"), "merchant_name": "ACME", "status_code": "P"},
        # NEWCO first (and only) seen within last 30 days -> new
        {"transaction_id": 3, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-04-02"), "amount": Decimal("-20.00"),
         "running_balance": Decimal("0.00"), "merchant_name": "NEWCO", "status_code": "P"},
    ])
    row = job.new_merchant_counts(txns, accts, RUN_DATE).collect()[0]
    assert row.new_merchant_cnt_30d == 1


def test_merchant_activity_intl_and_high_risk(spark):
    accts = fx.accounts(spark, [{"account_id": 10, "customer_id": 1}])
    txns = fx.transactions(spark, [
        {"transaction_id": 1, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-03-01"), "amount": Decimal("-10.00"),
         "running_balance": Decimal("0.00"), "channel_code": "INTL",
         "merchant_category": "GROCERY", "status_code": "P"},
        {"transaction_id": 2, "account_id": 10, "transaction_type_cd": "PUR",
         "transaction_date": _d("2026-03-02"), "amount": Decimal("-10.00"),
         "running_balance": Decimal("0.00"), "channel_code": "POS",
         "merchant_category": "CRYPTO_EXCHANGE", "status_code": "P"},
    ])
    row = job.merchant_activity(txns, accts, RUN_DATE).collect()[0]
    assert row.international_txn_cnt == 1
    assert row.high_risk_merchant_cnt == 1


# --------------------------------------------------------------------------- #
# Defaults for customers with no matching activity                            #
# --------------------------------------------------------------------------- #
def test_defaults_for_customer_without_activity(spark, cfg):
    customers = fx.customers(spark, [
        {"customer_id": 1, "customer_status": "A"},
    ])
    empty_accts = fx.accounts(spark, [])
    empty_txns = fx.transactions(spark, [])
    empty_types = _types(spark)
    empty_bureau = fx.bureau_scores(spark, [])
    out = job.transform(customers, empty_accts, empty_txns, empty_types, empty_bureau, cfg)
    row = out.collect()[0]
    assert row.account_overdraft_cnt == 0
    assert Decimal(str(row.nsf_fee_total)) == Decimal("0.00")
    assert row.large_withdrawal_cnt == 0
    assert Decimal(str(row.credit_util_ratio)) == Decimal("0.0000")
    assert Decimal(str(row.payment_ontime_pct)) == Decimal("100.00")
    assert row.payment_late_cnt == 0
    assert row.months_since_last_late == 999
    assert row.external_credit_score == 0
    assert row.new_merchant_cnt_30d == 0
    assert row.international_txn_cnt == 0


def test_output_schema_matches_ddl(spark, cfg):
    customers = fx.customers(spark, [{"customer_id": 1, "customer_status": "A"}])
    out = job.transform(
        customers, fx.accounts(spark, []), fx.transactions(spark, []),
        _types(spark), fx.bureau_scores(spark, []), cfg,
    )
    assert out.columns == schemas.STG_RISK_FACTORS.column_names
    schemas.assert_schema(out, schemas.STG_RISK_FACTORS)


def test_excludes_closed_customers(spark, cfg):
    customers = fx.customers(spark, [
        {"customer_id": 1, "customer_status": "A"},
        {"customer_id": 2, "customer_status": "I"},
        {"customer_id": 3, "customer_status": "C"},
    ])
    out = job.transform(
        customers, fx.accounts(spark, []), fx.transactions(spark, []),
        _types(spark), fx.bureau_scores(spark, []), cfg,
    )
    assert {r.customer_id for r in out.collect()} == {1, 2}
