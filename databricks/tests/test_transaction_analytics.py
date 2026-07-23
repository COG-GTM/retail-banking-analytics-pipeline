"""Ticket 8 - Transaction analytics business logic."""
from __future__ import annotations

from datetime import date, datetime

from jobs.transaction_analytics import MODEL_VERSION, build_transaction_analytics
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_SCHEMA = (
    "customer_id long, account_id long, txn_count_total int, days_since_last_txn int, "
    "amt_total_debit decimal(18,2), amt_total_credit decimal(18,2), "
    "amt_total_fees decimal(18,2), top_merchant_category string, "
    "pct_web decimal(5,2), pct_mobile decimal(5,2)"
)


def _stg(spark, rows):
    return make_df(spark, _SCHEMA, rows)


def test_spend_trend_and_revenue(spark):
    # net_cash_flow=600, avg_size=(100+700)/10=80, 5*avg=400 -> UP.
    rows = [(1, 10, 10, 5, 100, 700, 20, "TRAVEL", 50, 10)]
    out = _stg(spark, rows)
    r = build_transaction_analytics(out, RUN_DATE, LOAD_TS).collect()[0]
    assert r.monthly_spend_trend == "UP"
    assert float(r.net_cash_flow) == 600.0
    assert float(r.interest_income) == 2.0     # debit 100 * 0.02
    assert float(r.fee_income) == 20.0
    assert float(r.revenue_contribution) == 22.0
    assert r.reporting_period == "2026-04"
    assert r.model_version == MODEL_VERSION


def test_spend_trend_down_and_stable(spark):
    down = build_transaction_analytics(
        _stg(spark, [(1, 10, 10, 5, 700, 100, 0, "X", 0, 0)]), RUN_DATE, LOAD_TS
    ).collect()[0]
    assert down.monthly_spend_trend == "DOWN"   # net -600 < -400
    stable = build_transaction_analytics(
        _stg(spark, [(1, 10, 10, 5, 400, 410, 0, "X", 0, 0)]), RUN_DATE, LOAD_TS
    ).collect()[0]
    assert stable.monthly_spend_trend == "STABLE"  # net 10 within +/- avg*5


def test_top_spend_category_is_alphabetical_max_across_accounts(spark):
    rows = [
        (1, 10, 5, 5, 100, 0, 0, "APPAREL", 0, 0),
        (1, 11, 5, 5, 900, 0, 0, "GROCERY", 0, 0),
    ]
    r = build_transaction_analytics(_stg(spark, rows), RUN_DATE, LOAD_TS).collect()[0]
    # SAS max(TOP_MERCHANT_CATEGORY) -> alphabetical max, regardless of spend.
    assert r.top_spend_category == "GROCERY"


def test_iqr_anomaly_flag(spark):
    # One clear outlier well above median + 3*IQR.
    rows = [(i, 10 + i, 1, 5, float(v), 0, 0, "X", 0, 0)
            for i, v in enumerate([10, 12, 14, 16, 18, 20, 5000], start=1)]
    out = build_transaction_analytics(_stg(spark, rows), RUN_DATE, LOAD_TS)
    flags = {r.customer_id: r.anomaly_flag for r in out.collect()}
    assert flags[7] == "Y"                       # 5000 is the anomaly
    assert all(flags[i] == "N" for i in range(1, 7))
