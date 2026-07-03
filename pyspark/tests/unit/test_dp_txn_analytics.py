"""Unit tests for the TRANSACTION_ANALYTICS transforms (SAS 02)."""

from __future__ import annotations

import datetime as _dt
from decimal import Decimal

import pytest

from common import schemas
from common.config import PipelineConfig
from jobs import dp_txn_analytics as job
from tests import _fixtures as fx

pytestmark = pytest.mark.unit

RUN_DATE = _dt.date(2026, 4, 10)


@pytest.fixture(scope="module")
def cfg():
    return PipelineConfig(run_date=RUN_DATE)


def _stg(spark, rows):
    return fx.make_df(spark, schemas.STG_TXN_SUMMARY, rows)


def test_customer_level_aggregation(spark):
    txn = _stg(spark, [
        {"customer_id": 1, "account_id": 100, "txn_count_total": 10,
         "amt_total_debit": Decimal("100.00"), "amt_total_credit": Decimal("50.00"),
         "amt_total_fees": Decimal("5.00"), "days_since_last_txn": 10,
         "top_merchant_category": "TRAVEL", "pct_web": Decimal("20.00"),
         "pct_mobile": Decimal("30.00")},
        {"customer_id": 1, "account_id": 101, "txn_count_total": 10,
         "amt_total_debit": Decimal("200.00"), "amt_total_credit": Decimal("100.00"),
         "amt_total_fees": Decimal("5.00"), "days_since_last_txn": 40,
         "top_merchant_category": "DINING", "pct_web": Decimal("10.00"),
         "pct_mobile": Decimal("10.00")},
    ])
    row = job.aggregate_customer(txn).collect()[0]
    assert row.total_accounts == 2
    assert row.active_accounts == 1                    # only account 100 (10 <= 30)
    assert row.total_transactions == 20
    assert row.total_debit_amt == Decimal("300.00")
    assert row.total_credit_amt == Decimal("150.00")
    assert row.net_cash_flow == Decimal("-150.00")
    assert row.total_fees == Decimal("10.00")
    assert row.top_spend_category == "TRAVEL"          # max of the strings
    # avg = sum(debit+credit)/sum(txn) = 450 / 20 = 22.5
    assert row.avg_transaction_size == pytest.approx(22.5)
    # digital = (10*(20+30)/100 + 10*(10+10)/100) / 20 * 100 = 7/20*100 = 35.0
    assert row.digital_txn_pct == pytest.approx(35.0)


def test_avg_transaction_size_zero_guard(spark):
    txn = _stg(spark, [
        {"customer_id": 9, "account_id": 1, "txn_count_total": 0,
         "amt_total_debit": Decimal("0.00"), "amt_total_credit": Decimal("0.00"),
         "amt_total_fees": Decimal("0.00"), "days_since_last_txn": 5,
         "pct_web": Decimal("0.00"), "pct_mobile": Decimal("0.00")},
    ])
    row = job.aggregate_customer(txn).collect()[0]
    assert row.avg_transaction_size == pytest.approx(0.0)
    assert row.digital_txn_pct == pytest.approx(0.0)


def test_active_accounts_threshold_boundary(spark):
    txn = _stg(spark, [
        {"customer_id": 1, "account_id": 1, "txn_count_total": 1,
         "days_since_last_txn": 30, "amt_total_debit": Decimal("1.00"),
         "amt_total_credit": Decimal("1.00"), "amt_total_fees": Decimal("0.00"),
         "pct_web": Decimal("0.00"), "pct_mobile": Decimal("0.00")},
        {"customer_id": 1, "account_id": 2, "txn_count_total": 1,
         "days_since_last_txn": 31, "amt_total_debit": Decimal("1.00"),
         "amt_total_credit": Decimal("1.00"), "amt_total_fees": Decimal("0.00"),
         "pct_web": Decimal("0.00"), "pct_mobile": Decimal("0.00")},
    ])
    row = job.aggregate_customer(txn).collect()[0]
    assert row.total_accounts == 2
    assert row.active_accounts == 1                    # 30 counts, 31 does not


def _trend_df(spark, net, avg, fees, debit):
    return spark.createDataFrame(
        [(1, Decimal(net), float(avg), Decimal(fees), Decimal(debit))],
        ["customer_id", "net_cash_flow", "avg_transaction_size", "total_fees", "total_debit_amt"],
    )


def test_spend_trend_up(spark):
    # net 990 > avg(101)*5 = 505 -> UP
    row = job.add_trend_and_revenue(_trend_df(spark, "990.00", 101.0, "50.00", "500.00")).collect()[0]
    assert row.monthly_spend_trend == "UP"


def test_spend_trend_down(spark):
    # net -990 < -505 -> DOWN
    row = job.add_trend_and_revenue(_trend_df(spark, "-990.00", 101.0, "50.00", "500.00")).collect()[0]
    assert row.monthly_spend_trend == "DOWN"


def test_spend_trend_stable(spark):
    # net 0 within +/- 100 band -> STABLE
    row = job.add_trend_and_revenue(_trend_df(spark, "0.00", 20.0, "50.00", "500.00")).collect()[0]
    assert row.monthly_spend_trend == "STABLE"


def test_revenue_proxies(spark):
    # fee_income = total_fees; interest = debit*0.02; revenue = sum
    row = job.add_trend_and_revenue(_trend_df(spark, "0.00", 20.0, "50.00", "500.00")).collect()[0]
    assert float(row.fee_income) == pytest.approx(50.0)
    assert float(row.interest_income) == pytest.approx(10.0)      # 500 * 0.02
    assert float(row.revenue_contribution) == pytest.approx(60.0)


def _debit_df(spark, values):
    return spark.createDataFrame(
        [(i, Decimal(str(v))) for i, v in enumerate(values)],
        ["customer_id", "total_debit_amt"],
    )


def test_anomaly_flag_outlier(spark):
    df = _debit_df(spark, [100, 110, 120, 130, 140, 10000])
    flags = {r.customer_id: r.anomaly_flag for r in job.add_anomaly_flag(df).collect()}
    assert flags[5] == "Y"                             # 10000 is the clear outlier
    assert all(v == "N" for k, v in flags.items() if k != 5)


def test_anomaly_flag_zero_iqr_all_n(spark):
    df = _debit_df(spark, [100, 100, 100, 100])        # IQR = 0 -> no anomalies
    flags = [r.anomaly_flag for r in job.add_anomaly_flag(df).collect()]
    assert set(flags) == {"N"}


def test_spend_percentile_ordering(spark):
    df = _debit_df(spark, [50, 10, 30, 20, 40])        # 5 distinct rows
    ranked = {r.total_debit_amt: r.spend_percentile for r in job.add_spend_percentile(df).collect()}
    # ntile(100) ascending over 5 rows -> 1..5, minus 1 -> 0..4
    assert ranked[Decimal("10")] == 0                  # lowest spender
    assert ranked[Decimal("50")] == 4                  # highest spender
    assert ranked[Decimal("10")] < ranked[Decimal("30")] < ranked[Decimal("50")]


def test_output_schema_matches_ddl(spark, cfg):
    txn = _stg(spark, [
        {"customer_id": 1, "account_id": 1, "txn_count_total": 5,
         "amt_total_debit": Decimal("100.00"), "amt_total_credit": Decimal("80.00"),
         "amt_total_fees": Decimal("2.00"), "days_since_last_txn": 3,
         "top_merchant_category": "TRAVEL", "pct_web": Decimal("10.00"),
         "pct_mobile": Decimal("20.00")},
    ])
    out = job.transform(txn, cfg)
    assert out.columns == schemas.TRANSACTION_ANALYTICS.column_names
    row = out.collect()[0]
    assert row.reporting_period == "2026-04"
    assert row.model_version == "TXN_V2.1"
    assert row.effective_date == RUN_DATE
