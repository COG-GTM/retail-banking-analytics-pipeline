"""Unit + functional tests for ``jobs.stg_txn_summary``.

Unit tests exercise the pure ``transform`` on a controlled synthetic dataset
(exact aggregate values, channel-mix summing to 100%, window/status filtering).
Functional tests run the full ``run`` against the committed source CSVs and
cross-check parity against ``data/02_bteq_staging/stg_txn_summary.csv`` where
practical (grain, exact integer counts, top merchant category).
"""

from __future__ import annotations

import datetime as _dt

import pytest
from pyspark.sql import functions as F

from common.config import Config
from common.validation import ValidationError, validate_table
from jobs import stg_txn_summary as job

RUN_DATE = _dt.date(2026, 4, 10)

_TXN_SCHEMA = (
    "account_id BIGINT, transaction_type_cd STRING, transaction_date DATE, amount DOUBLE, "
    "merchant_name STRING, merchant_category STRING, channel_code STRING, status_code STRING"
)
_ACCT_SCHEMA = "account_id BIGINT, customer_id BIGINT, account_type STRING"
_TYPE_SCHEMA = "transaction_type_cd STRING, category STRING"


def _d(s: str) -> _dt.date:
    return _dt.date.fromisoformat(s)


@pytest.fixture(scope="module")
def synthetic(spark):
    txns = spark.createDataFrame(
        [
            # account 1 — in-window, posted
            (1, "PUR", _d("2026-01-01"), -100.0, "A", "GROCERY", "ATM", "P"),
            (1, "DEP", _d("2026-02-01"), 200.0, "B", "SALARY", "POS", "P"),
            (1, "FEE", _d("2026-03-01"), -30.0, None, None, "WEB", "P"),
            (1, "PUR", _d("2026-04-05"), -300.0, "A", "TRAVEL", "MOB", "P"),
            # account 1 — excluded: out of lookback window
            (1, "PUR", _d("2024-01-01"), -50.0, "Z", "GROCERY", "ATM", "P"),
            # account 1 — excluded: not posted (status H)
            (1, "PUR", _d("2026-03-15"), -50.0, "Z", "GROCERY", "ATM", "H"),
            # account 2 — single credit
            (2, "DEP", _d("2026-03-01"), 500.0, "C", "SALARY", "WEB", "P"),
        ],
        schema=_TXN_SCHEMA,
    )
    accts = spark.createDataFrame(
        [(1, 100, "CHECKING"), (2, 100, "SAVINGS")], schema=_ACCT_SCHEMA
    )
    types = spark.createDataFrame(
        [("PUR", "DEBIT"), ("DEP", "CREDIT"), ("FEE", "FEE")], schema=_TYPE_SCHEMA
    )
    cfg = Config(catalog="spark_catalog", run_date=RUN_DATE)
    out = job.transform(txns, accts, types, cfg)
    return {r["account_id"]: r for r in out.collect()}


def test_period_bounds_from_lookback():
    cfg = Config(run_date=RUN_DATE, lookback_months=12)
    assert job._period_start(cfg) == _d("2025-04-10")


def test_add_months_clamps_day():
    assert job._add_months(_d("2026-03-31"), -1) == _d("2026-02-28")


def test_synthetic_volume_and_dollar_aggregates(synthetic):
    a1 = synthetic[1]
    assert a1["customer_id"] == 100
    assert a1["account_type"] == "CHECKING"
    assert a1["txn_count_total"] == 4
    assert a1["txn_count_debit"] == 2
    assert a1["txn_count_credit"] == 1
    assert a1["txn_count_fee"] == 1
    assert float(a1["amt_total_debit"]) == 400.0
    assert float(a1["amt_total_credit"]) == 200.0
    assert float(a1["amt_total_fees"]) == 30.0
    assert float(a1["amt_avg_debit"]) == 200.0
    assert float(a1["amt_avg_credit"]) == 200.0
    assert float(a1["amt_max_single_debit"]) == 300.0
    assert float(a1["amt_max_single_credit"]) == 200.0


def test_synthetic_merchant_and_recency(synthetic):
    a1 = synthetic[1]
    # merchants A, B present; the fee row's NULL merchant is not counted.
    assert a1["distinct_merchants"] == 2
    # TRAVEL has the highest absolute spend (300) among non-null categories.
    assert a1["top_merchant_category"] == "TRAVEL"
    # run_date - max(transaction_date) = 2026-04-10 - 2026-04-05
    assert a1["days_since_last_txn"] == 5


def test_synthetic_channel_mix_sums_to_100(synthetic):
    a1 = synthetic[1]
    pcts = [a1["pct_atm"], a1["pct_pos"], a1["pct_web"], a1["pct_mobile"]]
    assert [float(p) for p in pcts] == [25.0, 25.0, 25.0, 25.0]
    assert float(sum(pcts)) == pytest.approx(100.0, abs=0.01)


def test_synthetic_window_and_status_filter(synthetic):
    # account 2 has exactly one posted, in-window credit txn.
    a2 = synthetic[2]
    assert a2["txn_count_total"] == 1
    assert a2["txn_count_credit"] == 1
    assert float(a2["pct_web"]) == 100.0
    # The out-of-window and non-posted rows on account 1 were dropped.
    assert synthetic[1]["txn_count_total"] == 4


def test_synthetic_period_columns(synthetic):
    a1 = synthetic[1]
    assert a1["summary_period_start"] == _d("2025-04-10")
    assert a1["summary_period_end"] == RUN_DATE


# --------------------------------------------------------------------------- #
# Functional tests against the committed source CSVs
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="module")
def result(spark, seeded_sources):
    return job.run(spark, seeded_sources)


def test_output_schema_matches_ddl(result):
    expected = [
        "customer_id", "account_id", "account_type", "summary_period_start",
        "summary_period_end", "txn_count_total", "txn_count_debit", "txn_count_credit",
        "txn_count_fee", "amt_total_debit", "amt_total_credit", "amt_total_fees",
        "amt_avg_debit", "amt_avg_credit", "amt_max_single_debit", "amt_max_single_credit",
        "distinct_merchants", "top_merchant_category", "pct_atm", "pct_pos", "pct_web",
        "pct_mobile", "days_since_last_txn", "load_ts",
    ]
    assert result.columns == expected


def test_key_uniqueness_and_not_null(result):
    total = result.count()
    assert total > 0
    assert result.select("customer_id", "account_id").distinct().count() == total
    assert result.where(F.col("customer_id").isNull() | F.col("account_id").isNull()).count() == 0
    # validate_table is the on-load contract used by run().
    validate_table(
        result, not_null_cols=["customer_id", "account_id"],
        unique_keys=["customer_id", "account_id"],
    )


def test_category_counts_sum_to_total(result):
    mismatched = result.where(
        F.col("txn_count_debit") + F.col("txn_count_credit") + F.col("txn_count_fee")
        != F.col("txn_count_total")
    ).count()
    assert mismatched == 0


def test_channel_percentages_in_range(result):
    bad = result.where(
        (F.col("pct_atm") < 0) | (F.col("pct_atm") > 100)
        | (F.col("pct_pos") < 0) | (F.col("pct_pos") > 100)
        | (F.col("pct_web") < 0) | (F.col("pct_web") > 100)
        | (F.col("pct_mobile") < 0) | (F.col("pct_mobile") > 100)
        | (F.col("pct_atm") + F.col("pct_pos") + F.col("pct_web") + F.col("pct_mobile") > 100.01)
    ).count()
    assert bad == 0


def test_idempotent_overwrite(spark, seeded_sources):
    first = job.run(spark, seeded_sources).count()
    second = job.run(spark, seeded_sources).count()
    assert first == second


def test_parity_grain_and_counts(result, expected_stg_txn_summary):
    """Cross-check exact integer metrics vs the committed BTEQ-staging fixture."""
    expected = expected_stg_txn_summary
    # Same grain (one row per customer/account).
    assert result.count() == expected.count()

    join_keys = ["customer_id", "account_id"]
    merged = (
        result.alias("act")
        .join(expected.alias("exp"), join_keys, "inner")
    )
    assert merged.count() == result.count()

    # Exact integer-count parity (deterministic aggregates).
    mism = merged.where(
        (F.col("act.txn_count_total") != F.col("exp.txn_count_total").cast("int"))
        | (F.col("act.txn_count_debit") != F.col("exp.txn_count_debit").cast("int"))
        | (F.col("act.txn_count_credit") != F.col("exp.txn_count_credit").cast("int"))
        | (F.col("act.txn_count_fee") != F.col("exp.txn_count_fee").cast("int"))
        | (F.col("act.distinct_merchants") != F.col("exp.distinct_merchants").cast("int"))
        | (F.col("act.days_since_last_txn") != F.col("exp.days_since_last_txn").cast("int"))
        | (F.col("act.top_merchant_category") != F.col("exp.top_merchant_category"))
    ).count()
    assert mism == 0


def test_parity_dollar_amounts_within_tolerance(result, expected_stg_txn_summary):
    """Totals match the fixture within rounding tolerance (DDL DECIMAL(18,2))."""
    expected = expected_stg_txn_summary
    merged = result.alias("act").join(
        expected.alias("exp"), ["customer_id", "account_id"], "inner"
    )
    bad = merged.where(
        (F.abs(F.col("act.amt_total_debit") - F.col("exp.amt_total_debit")) > 0.01)
        | (F.abs(F.col("act.amt_total_credit") - F.col("exp.amt_total_credit")) > 0.01)
        | (F.abs(F.col("act.amt_total_fees") - F.col("exp.amt_total_fees")) > 0.01)
    ).count()
    assert bad == 0


def test_validation_raises_on_duplicate_keys(spark):
    df = spark.createDataFrame(
        [(1, 1, "x"), (1, 1, "y")], schema="customer_id BIGINT, account_id BIGINT, v STRING"
    )
    with pytest.raises(ValidationError):
        validate_table(df, unique_keys=["customer_id", "account_id"])
