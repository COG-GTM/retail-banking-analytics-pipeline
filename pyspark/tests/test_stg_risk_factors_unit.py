"""Unit tests for the individual risk-factor transforms (exact math).

These exercise the module-level helper transforms with small, hand-built
DataFrames so the volatility / velocity / credit-util / payment / merchant math
can be asserted exactly.
"""
from __future__ import annotations

import math

from pyspark.sql import functions as F

from jobs import stg_risk_factors as job

REF = "2026-06-30"


def _ref():
    return F.to_date(F.lit(REF))


def _accounts(spark, rows):
    # Dates/decimals supplied as STRING/DOUBLE then cast (Spark won't coerce
    # python str->DateType / float->DecimalType at createDataFrame time).
    schema = (
        "account_id BIGINT, customer_id BIGINT, account_type STRING, "
        "account_status STRING, open_date STRING, current_balance DOUBLE, "
        "credit_limit DOUBLE"
    )
    return spark.createDataFrame(rows, schema)


def _txn_types(spark):
    schema = "transaction_type_cd STRING, category STRING, description STRING"
    rows = [
        ("PUR", "DEBIT", "Purchase"),
        ("WDR", "DEBIT", "Withdrawal"),
        ("DEP", "CREDIT", "Deposit"),
        ("PMT", "CREDIT", "Payment"),
        ("FEE", "FEE", "Account Fee"),
        ("NSF", "FEE", "NSF Fee"),
    ]
    return spark.createDataFrame(rows, schema)


def _transactions(spark, rows):
    schema = (
        "account_id BIGINT, transaction_type_cd STRING, transaction_date STRING, "
        "transaction_ts STRING, amount DOUBLE, running_balance DOUBLE, "
        "merchant_name STRING, merchant_category STRING, channel_code STRING, "
        "status_code STRING"
    )
    return spark.createDataFrame(rows, schema)


def test_balance_metrics_volatility_and_windows(spark):
    schema = "customer_id BIGINT, account_id BIGINT, transaction_date STRING, eod_balance DOUBLE"
    daily = spark.createDataFrame(
        [
            (1, 10, "2026-06-25", 100.0),  # within 7d / 30d / 90d
            (1, 10, "2026-06-01", 200.0),  # within 30d / 90d
            (1, 10, "2026-04-15", 300.0),  # within 90d only
        ],
        schema,
    ).withColumn("transaction_date", F.to_date("transaction_date"))

    out = {
        r["customer_id"]: r
        for r in job._balance_metrics(daily, _ref()).collect()
    }
    row = out[1]
    assert row["avg_bal_30d"] == 150.0  # (100 + 200) / 2
    assert row["avg_bal_90d"] == 200.0  # (100 + 200 + 300) / 3
    expected_std = math.sqrt(((100 - 200) ** 2 + 0 + (300 - 200) ** 2) / 3)
    assert abs(float(row["bal_stddev"]) - expected_std) < 1e-6


def test_debit_velocity_7d_30d(spark):
    accounts = _accounts(
        spark,
        [(10, 1, "CHECKING", "O", "2020-01-01", 0.0, None)],
    ).withColumn("open_date", F.to_date("open_date"))
    txns = _transactions(
        spark,
        [
            (10, "PUR", "2026-06-25", "2026-06-25 09:00:00", -50.0, 0.0, "M", "GROCERY", "POS", "P"),  # 7d & 30d
            (10, "PUR", "2026-06-10", "2026-06-10 09:00:00", -30.0, 0.0, "M", "GROCERY", "POS", "P"),  # 30d only
            (10, "PUR", "2026-05-20", "2026-05-20 09:00:00", -100.0, 0.0, "M", "GROCERY", "POS", "P"),  # >30d, excluded
            (10, "DEP", "2026-06-24", "2026-06-24 09:00:00", -20.0, 0.0, "M", "GROCERY", "POS", "P"),  # CREDIT, excluded
            (10, "PUR", "2026-06-24", "2026-06-24 09:00:00", -40.0, 0.0, "M", "GROCERY", "POS", "H"),  # not posted
        ],
    ).withColumn("transaction_date", F.to_date("transaction_date")).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )

    row = job._debit_velocity(txns, accounts, _txn_types(spark), _ref()).collect()[0]
    assert float(row["debit_7d"]) == 50.0
    assert float(row["debit_30d"]) == 80.0


def test_credit_utilization(spark):
    accounts = _accounts(
        spark,
        [
            (11, 1, "CREDIT", "O", "2020-01-01", 200.0, 1000.0),
            (12, 1, "CREDIT", "O", "2020-01-01", 300.0, 1000.0),
            (13, 2, "CREDIT", "C", "2020-01-01", 500.0, 1000.0),  # closed -> excluded
            (14, 3, "CREDIT", "O", "2020-01-01", 100.0, 0.0),  # zero limit -> ratio 0
        ],
    ).withColumn("open_date", F.to_date("open_date"))
    out = {
        r["customer_id"]: float(r["credit_util_ratio"])
        for r in job._credit_utilization(accounts).collect()
    }
    assert out[1] == 0.25  # 500 / 2000
    assert 2 not in out  # only closed credit account
    assert out[3] == 0.0  # zero limit guard


def test_overdraft_and_nsf(spark):
    accounts = _accounts(
        spark, [(10, 1, "CHECKING", "O", "2020-01-01", 0.0, None)]
    ).withColumn("open_date", F.to_date("open_date"))
    txns = _transactions(
        spark,
        [
            (10, "PUR", "2026-06-01", "2026-06-01 09:00:00", -10.0, -5.0, "M", "G", "POS", "P"),  # overdraft
            (10, "PUR", "2026-06-02", "2026-06-02 09:00:00", -10.0, 20.0, "M", "G", "POS", "P"),  # not overdraft
            (10, "NSF", "2026-06-03", "2026-06-03 09:00:00", -35.0, 10.0, None, None, "POS", "P"),  # NSF fee
            (10, "PUR", "2024-01-01", "2024-01-01 09:00:00", -10.0, -99.0, "M", "G", "POS", "P"),  # >12mo, excluded
        ],
    ).withColumn("transaction_date", F.to_date("transaction_date")).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )
    row = job._overdraft_nsf(txns, accounts, _txn_types(spark), _ref()).collect()[0]
    assert row["overdraft_count"] == 1
    assert float(row["nsf_total"]) == 35.0


def test_large_withdrawals(spark):
    accounts = _accounts(
        spark, [(10, 1, "CHECKING", "O", "2020-01-01", 0.0, None)]
    ).withColumn("open_date", F.to_date("open_date"))
    txns = _transactions(
        spark,
        [
            (10, "WDR", "2026-06-01", "2026-06-01 09:00:00", -6000.0, 0.0, "M", "G", "POS", "P"),  # large
            (10, "WDR", "2026-06-02", "2026-06-02 09:00:00", -5000.0, 0.0, "M", "G", "POS", "P"),  # large (== 5000)
            (10, "WDR", "2026-06-03", "2026-06-03 09:00:00", -4999.0, 0.0, "M", "G", "POS", "P"),  # below threshold
            (10, "DEP", "2026-06-04", "2026-06-04 09:00:00", -9000.0, 0.0, "M", "G", "POS", "P"),  # CREDIT category
        ],
    ).withColumn("transaction_date", F.to_date("transaction_date")).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )
    row = job._large_withdrawals(txns, accounts, _txn_types(spark), _ref()).collect()[0]
    assert row["large_wd_cnt"] == 2
    assert float(row["large_wd_amt"]) == 11000.0


def test_merchant_risk_new_intl_highrisk(spark):
    accounts = _accounts(
        spark, [(10, 1, "CHECKING", "O", "2020-01-01", 0.0, None)]
    ).withColumn("open_date", F.to_date("open_date"))
    txns = _transactions(
        spark,
        [
            # recent (last 30d) merchants
            (10, "PUR", "2026-06-01", "2026-06-01 09:00:00", -10.0, 0.0, "NewStore", "GROCERY", "POS", "P"),
            (10, "PUR", "2026-06-02", "2026-06-02 09:00:00", -10.0, 0.0, "OldStore", "GROCERY", "POS", "P"),
            (10, "PUR", "2026-06-03", "2026-06-03 09:00:00", -10.0, 0.0, "NewStore", "GROCERY", "POS", "P"),  # dup
            # prior history (before 30d window) establishing known merchants.
            # prior_merchants keys only on (account_id, merchant_name), so the
            # neutral category/channel here does not affect intl/high-risk counts.
            (10, "PUR", "2026-01-01", "2026-01-01 09:00:00", -10.0, 0.0, "OldStore", "GROCERY", "POS", "P"),
            (10, "PUR", "2026-05-01", "2026-05-01 09:00:00", -10.0, 0.0, "X", "GROCERY", "POS", "P"),
            (10, "PUR", "2026-05-01", "2026-05-01 09:00:00", -10.0, 0.0, "Y", "GROCERY", "POS", "P"),
            (10, "PUR", "2026-05-01", "2026-05-01 09:00:00", -10.0, 0.0, "Z", "GROCERY", "POS", "P"),
            # international + high-risk within 6mo window (recent, so also "new"
            # unless known above — X/Y/Z are known, so only NewStore is new)
            (10, "PUR", "2026-06-01", "2026-06-01 09:00:00", -10.0, 0.0, "X", "GROCERY", "INTL", "P"),
            (10, "PUR", "2026-06-02", "2026-06-02 09:00:00", -10.0, 0.0, "Y", "GAMBLING", "POS", "P"),
            (10, "PUR", "2026-06-02", "2026-06-02 09:00:00", -10.0, 0.0, "Z", "CRYPTO_EXCHANGE", "POS", "P"),
        ],
    ).withColumn("transaction_date", F.to_date("transaction_date")).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )
    row = job._merchant_risk(txns, accounts, _ref()).collect()[0]
    assert row["new_merch_30d"] == 1  # NewStore only (OldStore known)
    assert row["intl_txn_cnt"] == 1
    assert row["high_risk_cnt"] == 2  # GAMBLING + CRYPTO_EXCHANGE


def test_payment_history_ontime_and_months_since(spark):
    accounts = _accounts(
        spark, [(20, 1, "CREDIT", "O", "2026-01-15", 0.0, 1000.0)]
    ).withColumn("open_date", F.to_date("open_date"))
    txns = _transactions(
        spark,
        [
            (20, "PMT", "2026-02-10", "2026-02-10 09:00:00", 50.0, 0.0, None, None, "ACH", "P"),
            (20, "PMT", "2026-03-20", "2026-03-20 09:00:00", 50.0, 0.0, None, None, "ACH", "P"),
            (20, "PUR", "2026-03-25", "2026-03-25 09:00:00", -20.0, 0.0, None, None, "POS", "P"),  # DEBIT, ignored
        ],
    ).withColumn("transaction_date", F.to_date("transaction_date")).withColumn(
        "transaction_ts", F.to_timestamp("transaction_ts")
    )
    hist = job._build_payment_history(txns, accounts, _txn_types(spark), _ref())
    summary = job._payment_summary(hist).collect()[0]
    assert summary["total_payments"] == 2
    assert summary["ontime_payments"] == 2
    assert summary["late_payments"] == 0
    # No late payment -> months since account open (2026-01-15 -> 2026-06-30).
    assert summary["months_since_last_late"] == 5


def test_bureau_latest_score(spark):
    schema = "customer_id BIGINT, external_credit_score INT, report_date STRING"
    bureau = spark.createDataFrame(
        [
            (1, 700, "2026-01-01"),
            (1, 720, "2026-03-01"),  # latest
            (2, 650, "2025-12-01"),
        ],
        schema,
    ).withColumn("report_date", F.to_date("report_date"))
    out = {r["customer_id"]: r["credit_score"] for r in job._bureau_scores(bureau).collect()}
    assert out[1] == 720
    assert out[2] == 650
