"""Tests for the migrated transaction analytics Spark job (MBA-2208)."""

from __future__ import annotations

import math
import os
import sys
from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "synapse", "spark", "jobs"))

import txn_analytics_job as job  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_txn_analytics")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def stg_row(customer_id, account_id, **overrides):
    values = dict(
        CUSTOMER_ID=customer_id,
        ACCOUNT_ID=account_id,
        TXN_COUNT_TOTAL=10,
        AMT_TOTAL_DEBIT=100.0,
        AMT_TOTAL_CREDIT=50.0,
        AMT_TOTAL_FEES=5.0,
        TOP_MERCHANT_CATEGORY="GROCERY",
        PCT_WEB=20.0,
        PCT_MOBILE=30.0,
        DAYS_SINCE_LAST_TXN=10,
    )
    values.update(overrides)
    return Row(**values)


def test_aggregate_to_customer(spark):
    df = spark.createDataFrame(
        [
            stg_row(1, 11, TXN_COUNT_TOTAL=10, AMT_TOTAL_DEBIT=100.0, AMT_TOTAL_CREDIT=50.0),
            stg_row(1, 12, TXN_COUNT_TOTAL=30, AMT_TOTAL_DEBIT=200.0, AMT_TOTAL_CREDIT=400.0,
                    DAYS_SINCE_LAST_TXN=90, PCT_WEB=0.0, PCT_MOBILE=0.0, TOP_MERCHANT_CATEGORY="TRAVEL"),
        ]
    )

    result = job.aggregate_to_customer(df).collect()[0]

    assert result["TOTAL_ACCOUNTS"] == 2
    assert result["ACTIVE_ACCOUNTS"] == 1
    assert result["TOTAL_TRANSACTIONS"] == 40
    assert result["TOTAL_DEBIT_AMT"] == pytest.approx(300.0)
    assert result["TOTAL_CREDIT_AMT"] == pytest.approx(450.0)
    assert result["NET_CASH_FLOW"] == pytest.approx(150.0)
    assert result["AVG_TRANSACTION_SIZE"] == pytest.approx(750.0 / 40)
    assert result["TOP_SPEND_CATEGORY"] == "TRAVEL"
    # 10 txns at 50% digital out of 40 txns -> 12.5%
    assert result["DIGITAL_TXN_PCT"] == pytest.approx(12.5)


def test_spend_trend_thresholds(spark):
    df = spark.createDataFrame(
        [
            Row(CUSTOMER_ID=1, NET_CASH_FLOW=100.0, AVG_TRANSACTION_SIZE=10.0,
                TOTAL_FEES=5.0, TOTAL_DEBIT_AMT=1000.0),
            Row(CUSTOMER_ID=2, NET_CASH_FLOW=-100.0, AVG_TRANSACTION_SIZE=10.0,
                TOTAL_FEES=0.0, TOTAL_DEBIT_AMT=0.0),
            Row(CUSTOMER_ID=3, NET_CASH_FLOW=10.0, AVG_TRANSACTION_SIZE=10.0,
                TOTAL_FEES=0.0, TOTAL_DEBIT_AMT=0.0),
        ]
    )

    rows = {r["CUSTOMER_ID"]: r for r in job.add_spend_trend(df).collect()}

    assert rows[1]["MONTHLY_SPEND_TREND"] == "UP"
    assert rows[2]["MONTHLY_SPEND_TREND"] == "DOWN"
    assert rows[3]["MONTHLY_SPEND_TREND"] == "STABLE"
    assert rows[1]["INTEREST_INCOME"] == pytest.approx(20.0)
    assert rows[1]["REVENUE_CONTRIBUTION"] == pytest.approx(25.0)
    assert rows[1]["ANOMALY_FLAG"] == "N"


def sas_rank_group(mean_rank: float, n: int, groups: int) -> int:
    return int(math.floor(mean_rank * groups / (n + 1)))


def test_spend_percentile_matches_proc_rank_formula(spark):
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    df = spark.createDataFrame([Row(CUSTOMER_ID=i, TOTAL_DEBIT_AMT=v) for i, v in enumerate(values)])

    rows = {r["CUSTOMER_ID"]: r["SPEND_PERCENTILE"] for r in job.add_spend_percentile(df, groups=5).collect()}

    for i in range(len(values)):
        assert rows[i] == sas_rank_group(i + 1, len(values), 5)


def test_spend_percentile_ties_share_a_bucket(spark):
    # Ranks 1, 2.5, 2.5, 4 under TIES=MEAN.
    values = [10.0, 20.0, 20.0, 30.0]
    df = spark.createDataFrame([Row(CUSTOMER_ID=i, TOTAL_DEBIT_AMT=v) for i, v in enumerate(values)])

    rows = {r["CUSTOMER_ID"]: r["SPEND_PERCENTILE"] for r in job.add_spend_percentile(df, groups=100).collect()}

    assert rows[1] == rows[2] == sas_rank_group(2.5, 4, 100)
    assert rows[0] == sas_rank_group(1, 4, 100)
    assert rows[3] == sas_rank_group(4, 4, 100)


def test_spend_percentile_keeps_missing_values_null(spark):
    df = spark.createDataFrame(
        [(1, 10.0), (2, None)], "CUSTOMER_ID bigint, TOTAL_DEBIT_AMT double"
    )

    rows = {r["CUSTOMER_ID"]: r["SPEND_PERCENTILE"] for r in job.add_spend_percentile(df).collect()}

    assert rows[2] is None
    # Only one non-missing value, so n = 1.
    assert rows[1] == sas_rank_group(1, 1, 100)


def test_sas_quantiles_averaging_definition(spark):
    # n = 8, so n*0.25 = 2 and n*0.5 = 4 are integers -> averaged order statistics.
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    df = spark.createDataFrame([Row(TOTAL_DEBIT_AMT=v) for v in values])

    quantiles = job.sas_quantiles(df)

    assert quantiles.q1 == pytest.approx(2.5)
    assert quantiles.median == pytest.approx(4.5)
    assert quantiles.q3 == pytest.approx(6.5)
    assert quantiles.iqr == pytest.approx(4.0)


def test_sas_quantiles_non_integer_position(spark):
    # n = 5: 5*0.25 = 1.25 -> x[2]; 5*0.5 = 2.5 -> x[3]; 5*0.75 = 3.75 -> x[4].
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    df = spark.createDataFrame([Row(TOTAL_DEBIT_AMT=v) for v in values])

    quantiles = job.sas_quantiles(df)

    assert quantiles.q1 == pytest.approx(2.0)
    assert quantiles.median == pytest.approx(3.0)
    assert quantiles.q3 == pytest.approx(4.0)


def test_anomaly_flag_sas_parity_and_tukey(spark):
    quantiles = job.SasQuantiles(q1=2.0, median=4.0, q3=6.0)
    df = spark.createDataFrame(
        [
            Row(CUSTOMER_ID=1, TOTAL_DEBIT_AMT=-5.0),
            Row(CUSTOMER_ID=2, TOTAL_DEBIT_AMT=4.0),
            Row(CUSTOMER_ID=3, TOTAL_DEBIT_AMT=20.0),
        ]
    )

    parity = {r["CUSTOMER_ID"]: r["ANOMALY_FLAG"] for r in job.add_anomaly_flag(df, quantiles).collect()}
    tukey = {
        r["CUSTOMER_ID"]: r["ANOMALY_FLAG"] for r in job.add_anomaly_flag(df, quantiles, "tukey").collect()
    }

    # median + 3*IQR = 16 ; Tukey fences = [-4, 12]
    assert parity == {1: "N", 2: "N", 3: "Y"}
    assert tukey == {1: "Y", 2: "N", 3: "Y"}


def test_anomaly_flag_noop_when_iqr_zero(spark):
    quantiles = job.SasQuantiles(q1=5.0, median=5.0, q3=5.0)
    df = spark.createDataFrame([Row(CUSTOMER_ID=1, TOTAL_DEBIT_AMT=1000.0)])

    assert job.add_anomaly_flag(df, quantiles).collect()[0]["ANOMALY_FLAG"] == "N"


def test_finalize_projects_data_product_schema(spark):
    df = spark.createDataFrame([stg_row(1, 11)])
    built = job.finalize(
        job.add_spend_percentile(job.add_spend_trend(job.aggregate_to_customer(df))),
        "2026-08",
        run_ts=datetime(2026, 8, 21, 12, 0, 0),
    )

    assert built.columns == list(job.OUTPUT_COLUMNS)
    row = built.collect()[0]
    assert row["REPORTING_PERIOD"] == "2026-08"
    assert row["MODEL_VERSION"] == job.MODEL_VERSION
    assert row["EFFECTIVE_DATE"] == date(2026, 8, 21)


def test_validate_rejects_small_row_count(spark):
    df = spark.createDataFrame(
        [Row(CUSTOMER_ID=1, REPORTING_PERIOD="2026-08", TOTAL_TRANSACTIONS=5)]
    )

    with pytest.raises(job.ValidationError, match="rows"):
        job.validate(df, min_rows=1000)


def test_validate_rejects_duplicate_keys_and_nulls(spark):
    duplicates = spark.createDataFrame(
        [
            Row(CUSTOMER_ID=1, REPORTING_PERIOD="2026-08", TOTAL_TRANSACTIONS=5),
            Row(CUSTOMER_ID=1, REPORTING_PERIOD="2026-08", TOTAL_TRANSACTIONS=7),
        ]
    )
    with pytest.raises(job.ValidationError, match="duplicate"):
        job.validate(duplicates, min_rows=1)

    nulls = spark.createDataFrame(
        [(1, "2026-08", None)], "CUSTOMER_ID bigint, REPORTING_PERIOD string, TOTAL_TRANSACTIONS int"
    )
    with pytest.raises(job.ValidationError, match="NULL"):
        job.validate(nulls, min_rows=1)


def test_build_transaction_analytics_end_to_end(spark):
    rows = [stg_row(c, 100 + c, AMT_TOTAL_DEBIT=float(c * 100)) for c in range(1, 11)]
    rows.append(stg_row(99, 199, AMT_TOTAL_DEBIT=100000.0))
    df = spark.createDataFrame(rows)

    result = job.build_transaction_analytics(df, "2026-08", run_ts=datetime(2026, 8, 21, 12, 0, 0))
    flags = {r["CUSTOMER_ID"]: r["ANOMALY_FLAG"] for r in result.collect()}

    assert result.count() == 11
    assert flags[99] == "Y"
    assert all(flag == "N" for cid, flag in flags.items() if cid != 99)
    assert job.validate(result, min_rows=1) == 11


def test_current_reporting_period():
    assert job.current_reporting_period(date(2026, 8, 3)) == "2026-08"
