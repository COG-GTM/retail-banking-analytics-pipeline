"""Unit tests for staging.stg_risk_factors."""
from datetime import date, datetime

from conftest import make_df

from staging.stg_risk_factors import OUTPUT_COLUMNS, transform

AS_OF = date(2026, 4, 10)


def _tx(tid, acct, cd, d, amount, balance, merchant=None, category=None,
        channel="POS", status="P"):
    return {
        "transaction_id": tid, "account_id": acct, "transaction_type_cd": cd,
        "transaction_date": d, "transaction_ts": datetime(d.year, d.month, d.day, 12),
        "amount": amount, "running_balance": balance, "merchant_name": merchant,
        "merchant_category": category, "channel_code": channel, "status_code": status,
    }


def _run(spark):
    customers = [
        {"customer_id": 1, "first_name": "Ada", "last_name": "B",
         "date_of_birth": date(1990, 1, 1), "customer_since": date(2019, 1, 1),
         "customer_status": "A"},
        # Customer with no accounts/transactions -> should get default values.
        {"customer_id": 2, "first_name": "No", "last_name": "Activity",
         "date_of_birth": date(1990, 1, 1), "customer_since": date(2019, 1, 1),
         "customer_status": "A"},
    ]
    accounts = [
        {"account_id": 100, "customer_id": 1, "account_type": "CREDIT",
         "account_status": "O", "open_date": date(2024, 1, 1),
         "current_balance": 300.00, "credit_limit": 1000.00},
        {"account_id": 101, "customer_id": 1, "account_type": "CHECKING",
         "account_status": "O", "open_date": date(2020, 1, 1)},
        {"account_id": 200, "customer_id": 2, "account_type": "CHECKING",
         "account_status": "O", "open_date": date(2021, 1, 1)},
    ]
    transaction_types = [
        {"transaction_type_cd": "PUR", "description": "Purchase", "category": "DEBIT"},
        {"transaction_type_cd": "PMT", "description": "Payment", "category": "CREDIT"},
        {"transaction_type_cd": "DEP", "description": "Deposit", "category": "CREDIT"},
        {"transaction_type_cd": "NSF", "description": "NSF Fee", "category": "FEE"},
    ]
    txns = [
        # Large withdrawal (>= 5000), within 7d/30d; new merchant "FreshMerchant".
        _tx(1, 101, "PUR", date(2026, 4, 5), -6000.00, 100.00, "FreshMerchant", "RETAIL"),
        # Overdraft (running_balance < 0), prior merchant "OldShop".
        _tx(2, 101, "PUR", date(2026, 2, 1), -20.00, -50.00, "OldShop", "RETAIL"),
        # NSF fee.
        _tx(3, 101, "NSF", date(2026, 3, 1), -35.00, 200.00, None, None, channel="WEB"),
        # Older deposit (prior merchant "OldShop").
        _tx(4, 101, "DEP", date(2026, 1, 15), 100.00, 100.00, "OldShop", "RETAIL", channel="WEB"),
        # High-risk + international, also a new merchant "Casino" (within 30d).
        _tx(5, 101, "PUR", date(2026, 4, 2), -40.00, 300.00, "Casino", "GAMBLING", channel="INTL"),
        # Recent "OldShop" purchase (within 30d) — NOT new (seen before) -> anti-join test.
        _tx(6, 101, "PUR", date(2026, 4, 4), -10.00, 90.00, "OldShop", "RETAIL", channel="WEB"),
        # Credit-account payments (on-time by the due-date proxy).
        _tx(7, 100, "PMT", date(2026, 2, 15), 100.00, 250.00, None, None, channel="WEB"),
        _tx(8, 100, "PMT", date(2025, 8, 10), 100.00, 260.00, None, None, channel="WEB"),
    ]
    bureau = [
        {"customer_id": 1, "external_credit_score": 700, "report_date": date(2026, 1, 1)},
        {"customer_id": 1, "external_credit_score": 780, "report_date": date(2026, 3, 1)},
    ]
    return transform(
        make_df(spark, "customers", customers),
        make_df(spark, "accounts", accounts),
        make_df(spark, "transactions", txns),
        make_df(spark, "transaction_types", transaction_types),
        make_df(spark, "customer_bureau_scores", bureau),
        AS_OF,
    )


def test_output_schema(spark):
    assert _run(spark).columns == OUTPUT_COLUMNS


def test_risk_metrics(spark):
    result = _run(spark)
    assert result.count() == 2  # both active customers
    row = result.filter("customer_id = 1").collect()[0]

    assert row["account_overdraft_cnt"] == 1
    assert float(row["nsf_fee_total"]) == 35.00
    assert row["large_withdrawal_cnt"] == 1
    assert float(row["large_withdrawal_amt"]) == 6000.00
    # credit_util_ratio = 300 / 1000 = 0.3000
    assert float(row["credit_util_ratio"]) == 0.3000
    # Latest bureau report wins.
    assert row["external_credit_score"] == 780
    assert row["international_txn_cnt"] == 1
    assert row["high_risk_merchant_cnt"] == 1
    # New merchants in last 30d = FreshMerchant + Casino (OldShop seen before).
    assert row["new_merchant_cnt_30d"] == 2
    # Balance stats are populated (STDDEV_POP > 0 with varied balances).
    assert float(row["balance_volatility"]) > 0
    assert float(row["avg_daily_balance_30d"]) > 0
    # Two on-time payments, none late.
    assert float(row["payment_ontime_pct"]) == 100.00
    assert row["payment_late_cnt"] == 0


def test_defaults_for_inactive_customer(spark):
    row = _run(spark).filter("customer_id = 2").collect()[0]
    assert row["account_overdraft_cnt"] == 0
    assert float(row["nsf_fee_total"]) == 0.00
    assert float(row["credit_util_ratio"]) == 0.0000
    # No payment history -> defaults to perfect on-time and the 999 sentinel.
    assert float(row["payment_ontime_pct"]) == 100.00
    assert row["months_since_last_late"] == 999
    assert row["external_credit_score"] == 0
    assert row["new_merchant_cnt_30d"] == 0
