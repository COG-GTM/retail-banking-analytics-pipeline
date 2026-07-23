"""Ticket 10 - Customer master profile (4-way join + default handling)."""
from __future__ import annotations

from datetime import date, datetime

from jobs.master_profile import build_master_profile
from _helpers import make_df

RUN_DATE = date(2026, 4, 10)
LOAD_TS = datetime(2026, 4, 10, 3, 0, 0)

_C360_SCHEMA = (
    "customer_id long, first_name string, last_name string, age smallint, "
    "state_code string, customer_since date, tenure_months int, customer_status string, "
    "num_accounts smallint, num_active_accounts smallint, total_balance decimal(18,2), "
    "total_credit_limit decimal(18,2), credit_utilization_pct decimal(5,2)"
)
_SEG_SCHEMA = (
    "customer_id long, segment_name string, lifetime_value_score decimal(10,2), "
    "engagement_score decimal(5,2), cross_sell_flag string, upsell_flag string, "
    "retention_risk_flag string"
)
_TXN_SCHEMA = (
    "customer_id long, total_transactions int, total_debit_amt decimal(18,2), "
    "net_cash_flow decimal(18,2), top_spend_category string, digital_txn_pct decimal(5,2), "
    "effective_date date"
)
_RISK_SCHEMA = (
    "customer_id long, composite_risk_score decimal(6,2), risk_tier string, "
    "probability_of_default decimal(7,6), watch_list_flag string"
)


def _base(spark, ids):
    return make_df(
        spark, _C360_SCHEMA,
        [(i, "First", "Last", 40, "CA", date(2020, 1, 1), 60, "A", 3, 2, 5000, 1000, 25) for i in ids],
    )


def test_missing_upstream_products_get_defaults(spark):
    base = _base(spark, [1])
    empty_seg = make_df(spark, _SEG_SCHEMA, [])
    empty_txn = make_df(spark, _TXN_SCHEMA, [])
    empty_risk = make_df(spark, _RISK_SCHEMA, [])
    r = build_master_profile(base, empty_seg, empty_txn, empty_risk, RUN_DATE, LOAD_TS).collect()[0]
    assert r.full_name == "First Last"
    assert r.segment_name == "UNCLASSIFIED"
    assert r.risk_tier == "UNKNOWN"
    assert r.watch_list_flag == "N"
    assert r.cross_sell_flag == "N"
    assert int(r.monthly_transactions) == 0
    assert float(r.monthly_spend) == 0.0
    assert r.top_spend_category == ""


def test_joins_populate_from_all_products(spark):
    base = _base(spark, [1])
    seg = make_df(spark, _SEG_SCHEMA, [(1, "PREMIUM_WEALTH", 88.5, 42.0, "Y", "N", "N")])
    txn = make_df(spark, _TXN_SCHEMA, [(1, 12, 3400.0, 500.0, "TRAVEL", 60.0, RUN_DATE)])
    risk = make_df(spark, _RISK_SCHEMA, [(1, 72.5, "HIGH", 0.6, "Y")])
    r = build_master_profile(base, seg, txn, risk, RUN_DATE, LOAD_TS).collect()[0]
    assert r.segment_name == "PREMIUM_WEALTH"
    assert int(r.monthly_transactions) == 12
    assert r.top_spend_category == "TRAVEL"
    assert r.risk_tier == "HIGH"
    assert r.watch_list_flag == "Y"


def test_only_current_period_transactions_join(spark):
    base = _base(spark, [1])
    empty_seg = make_df(spark, _SEG_SCHEMA, [])
    # transaction row for a different period should be ignored.
    txn = make_df(spark, _TXN_SCHEMA, [(1, 99, 9999.0, 0.0, "OLD", 0.0, date(2025, 1, 1))])
    empty_risk = make_df(spark, _RISK_SCHEMA, [])
    r = build_master_profile(base, empty_seg, txn, empty_risk, RUN_DATE, LOAD_TS).collect()[0]
    assert int(r.monthly_transactions) == 0     # stale period filtered out
