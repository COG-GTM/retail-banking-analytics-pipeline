"""Tests for the transaction analytics job (02)."""
from __future__ import annotations

import math

from pyspark.sql import functions as F

from spark.jobs.txn_analytics import (
    OUTPUT_COLUMNS,
    add_spend_percentile,
    aggregate_to_customer,
    build_transaction_analytics,
)

TXN_COLUMNS = [
    "customer_id", "account_id", "txn_count_total", "amt_total_debit",
    "amt_total_credit", "amt_total_fees", "top_merchant_category",
    "pct_web", "pct_mobile", "days_since_last_txn",
]


def _txn_rows(spark):
    # customer 1: two accounts aggregate to one row
    data = [
        (1, 10, 30.0, 1000.0, 400.0, 10.0, "RETAIL", 20.0, 30.0, 5),
        (1, 11, 20.0, 500.0, 200.0, 5.0, "HEALTHCARE", 40.0, 10.0, 45),
        (2, 20, 10.0, 200.0, 900.0, 2.0, "TRAVEL", 0.0, 0.0, 2),
    ]
    return spark.createDataFrame(data, TXN_COLUMNS)


def test_aggregate_to_customer(spark):
    out = {r["customer_id"]: r for r in aggregate_to_customer(_txn_rows(spark)).collect()}
    c1 = out[1]
    assert c1["total_accounts"] == 2
    assert c1["total_transactions"] == 50
    assert math.isclose(c1["total_debit_amt"], 1500.0)
    assert math.isclose(c1["total_credit_amt"], 600.0)
    # net_cash_flow = credit - debit
    assert math.isclose(c1["net_cash_flow"], -900.0)
    # active_accounts: only account 10 (days<=30)
    assert c1["active_accounts"] == 1
    # avg_transaction_size = (1500+600)/50
    assert math.isclose(c1["avg_transaction_size"], 2100.0 / 50.0)


def test_spend_percentile_range(spark, config):
    agg = aggregate_to_customer(
        spark.createDataFrame(
            [(i, i, 1.0, float(i * 100), 0.0, 0.0, "X", 0.0, 0.0, 1) for i in range(1, 21)],
            TXN_COLUMNS,
        )
    )
    ranked = add_spend_percentile(agg, groups=config.rank_groups)
    pcts = [r["spend_percentile"] for r in ranked.collect()]
    assert all(0 <= p <= config.rank_groups - 1 for p in pcts)
    # smallest debit -> lowest bucket, largest -> highest
    smallest = ranked.orderBy("total_debit_amt").first()["spend_percentile"]
    largest = ranked.orderBy(F.col("total_debit_amt").desc()).first()["spend_percentile"]
    assert smallest < largest


def test_build_contract_and_defaults(spark, config):
    result = build_transaction_analytics(_txn_rows(spark), config)
    assert result.columns == OUTPUT_COLUMNS
    assert result.count() == 2
    row = result.where(F.col("customer_id") == 1).first()
    # interest_income = total_debit_amt * interest_income_rate
    assert math.isclose(row["interest_income"], 1500.0 * config.interest_income_rate)
    assert row["reporting_period"] == config.reporting_period
    # top_spend_category uses SAS MAX() semantics (alphabetical max): RETAIL > HEALTHCARE
    assert row["top_spend_category"] == "RETAIL"
    assert row["anomaly_flag"] in {"Y", "N"}
