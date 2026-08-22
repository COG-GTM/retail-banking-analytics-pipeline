"""Parity tests for the TRANSACTION_ANALYTICS Synapse Spark job.

Run with: pytest synapse/spark/tests
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from txn_analytics_job import (  # noqa: E402
    ValidationError,
    add_proc_rank_groups,
    add_spend_trend,
    aggregate_customer_txn,
    build_transaction_analytics,
    compute_iqr_stats,
    default_reporting_period,
    flag_anomalies,
    sas_quantiles,
    validate,
)

STG_COLUMNS = [
    "CUSTOMER_ID",
    "ACCOUNT_ID",
    "TXN_COUNT_TOTAL",
    "AMT_TOTAL_DEBIT",
    "AMT_TOTAL_CREDIT",
    "AMT_TOTAL_FEES",
    "TOP_MERCHANT_CATEGORY",
    "PCT_WEB",
    "PCT_MOBILE",
    "DAYS_SINCE_LAST_TXN",
]


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("txn-analytics-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def staging_df(spark: SparkSession, rows):
    return spark.createDataFrame(rows, STG_COLUMNS)


def test_aggregate_customer_txn(spark: SparkSession):
    df = staging_df(
        spark,
        [
            (1, 10, 100, 1000.0, 400.0, 50.0, "TRAVEL", 20.0, 30.0, 5),
            (1, 11, 100, 200.0, 100.0, 10.0, "GROCERY", 10.0, 10.0, 90),
            (2, 20, 0, 0.0, 0.0, 0.0, "FUEL", 0.0, 0.0, 400),
        ],
    )
    out = {r["CUSTOMER_ID"]: r for r in aggregate_customer_txn(df).collect()}

    c1 = out[1]
    assert c1["TOTAL_ACCOUNTS"] == 2
    assert c1["ACTIVE_ACCOUNTS"] == 1
    assert c1["TOTAL_TRANSACTIONS"] == 200
    assert c1["NET_CASH_FLOW"] == pytest.approx(-700.0)
    assert c1["AVG_TRANSACTION_SIZE"] == pytest.approx(1700.0 / 200)
    # lexical MAX, as in the SAS PROC SQL
    assert c1["TOP_SPEND_CATEGORY"] == "TRAVEL"
    # count-weighted mean of PCT_WEB + PCT_MOBILE
    assert c1["DIGITAL_TXN_PCT"] == pytest.approx(35.0)

    # zero transactions must not divide by zero
    assert out[2]["AVG_TRANSACTION_SIZE"] == pytest.approx(0.0)
    assert out[2]["DIGITAL_TXN_PCT"] == pytest.approx(0.0)


def test_spend_trend_thresholds(spark: SparkSession):
    df = spark.createDataFrame(
        [
            (1, 600.0, 100.0, 10.0, 1000.0),  # NET > AVG*5 -> UP
            (2, -600.0, 100.0, 10.0, 1000.0),  # NET < -AVG*5 -> DOWN
            (3, 500.0, 100.0, 10.0, 1000.0),  # exactly AVG*5 -> STABLE
        ],
        ["CUSTOMER_ID", "NET_CASH_FLOW", "AVG_TRANSACTION_SIZE", "TOTAL_FEES", "TOTAL_DEBIT_AMT"],
    )
    out = {r["CUSTOMER_ID"]: r for r in add_spend_trend(df).collect()}

    assert [out[i]["MONTHLY_SPEND_TREND"] for i in (1, 2, 3)] == ["UP", "DOWN", "STABLE"]
    assert out[1]["INTEREST_INCOME"] == pytest.approx(20.0)
    assert out[1]["REVENUE_CONTRIBUTION"] == pytest.approx(30.0)
    assert out[1]["ANOMALY_FLAG"] == "N"


def test_proc_rank_groups_matches_sas_formula(spark: SparkSession):
    """PROC RANK GROUPS=4: FLOOR(mean_rank * 4 / (n + 1)) with TIES=MEAN."""
    values = [10.0, 20.0, 20.0, 30.0, 40.0, 50.0, 60.0]
    df = spark.createDataFrame([(i, v) for i, v in enumerate(values)], ["ID", "V"])
    out = {r["ID"]: r["G"] for r in add_proc_rank_groups(df, "V", "G", groups=4).collect()}

    # n = 7; ranks: 1, 2.5, 2.5, 4, 5, 6, 7 -> floor(rank*4/8)
    assert [out[i] for i in range(7)] == [0, 1, 1, 2, 2, 3, 3]


def test_proc_rank_groups_leaves_missing_missing(spark: SparkSession):
    df = spark.createDataFrame([(1, 5.0), (2, None), (3, 15.0)], ["ID", "V"])
    out = {r["ID"]: r["G"] for r in add_proc_rank_groups(df, "V", "G", groups=100).collect()}

    assert out[2] is None
    # n (non-missing) = 2: floor(1*100/3)=33, floor(2*100/3)=66
    assert out[1] == 33
    assert out[3] == 66


@pytest.mark.parametrize(
    "values, prob, expected",
    [
        # n*p integral -> average of the two neighbouring order statistics
        ([1.0, 2.0, 3.0, 4.0], 0.25, 1.5),
        ([1.0, 2.0, 3.0, 4.0], 0.5, 2.5),
        ([1.0, 2.0, 3.0, 4.0], 0.75, 3.5),
        # n*p not integral -> ceil(n*p)-th order statistic
        ([1.0, 2.0, 3.0, 4.0, 5.0], 0.25, 2.0),
        ([1.0, 2.0, 3.0, 4.0, 5.0], 0.5, 3.0),
        ([1.0, 2.0, 3.0, 4.0, 5.0], 0.75, 4.0),
        # np == n edge case
        ([7.0, 9.0], 1.0, 9.0),
    ],
)
def test_sas_quantiles_qntldef5(spark: SparkSession, values, prob, expected):
    df = spark.createDataFrame([(v,) for v in values], ["V"])
    assert sas_quantiles(df, "V", (prob,))[prob] == pytest.approx(expected)


def test_compute_iqr_stats_and_bounds(spark: SparkSession):
    values = [1.0, 2.0, 3.0, 4.0]
    df = spark.createDataFrame([(v,) for v in values], ["TOTAL_DEBIT_AMT"])
    stats = compute_iqr_stats(df, "TOTAL_DEBIT_AMT")

    assert (stats.n, stats.q1, stats.median, stats.q3) == (4, 1.5, 2.5, 3.5)
    assert stats.iqr == pytest.approx(2.0)
    assert stats.lower_fence == pytest.approx(-1.5)
    assert stats.upper_fence == pytest.approx(6.5)
    assert stats.sas_upper_bound == pytest.approx(8.5)


def test_flag_anomalies_rules(spark: SparkSession):
    values = [1.0, 2.0, 3.0, 4.0, 100.0, 0.0]
    df = spark.createDataFrame([(v,) for v in values], ["TOTAL_DEBIT_AMT"]).withColumn(
        "ANOMALY_FLAG", F.lit("N")
    )
    stats = compute_iqr_stats(df, "TOTAL_DEBIT_AMT")

    sas_flags = [r["ANOMALY_FLAG"] for r in flag_anomalies(df, stats).orderBy("TOTAL_DEBIT_AMT").collect()]
    tukey_flags = [
        r["ANOMALY_FLAG"]
        for r in flag_anomalies(df, stats, rule="tukey_fences").orderBy("TOTAL_DEBIT_AMT").collect()
    ]

    # median + 3*IQR only flags the high outlier; Tukey fences flag both tails
    assert sas_flags == ["N", "N", "N", "N", "N", "Y"]
    assert tukey_flags[-1] == "Y"
    assert tukey_flags.count("Y") >= 1


def test_flag_anomalies_noop_when_iqr_zero(spark: SparkSession):
    df = spark.createDataFrame([(5.0,), (5.0,), (5.0,)], ["TOTAL_DEBIT_AMT"])
    stats = compute_iqr_stats(df, "TOTAL_DEBIT_AMT")

    assert stats.iqr == pytest.approx(0.0)
    assert {r["ANOMALY_FLAG"] for r in flag_anomalies(df, stats).collect()} == {"N"}


def test_build_transaction_analytics_schema_and_metadata(spark: SparkSession):
    rows = [(cid, cid * 10, 10, float(cid) * 100, 50.0, 5.0, "TRAVEL", 20.0, 20.0, 3) for cid in range(1, 11)]
    df, stats = build_transaction_analytics(
        staging_df(spark, rows),
        reporting_period="2026-08",
        effective_date=date(2026, 8, 22),
        load_ts=datetime(2026, 8, 22, 1, 2, 3),
    )
    result = df.collect()

    assert df.columns == [
        "CUSTOMER_ID",
        "REPORTING_PERIOD",
        "TOTAL_ACCOUNTS",
        "ACTIVE_ACCOUNTS",
        "TOTAL_TRANSACTIONS",
        "TOTAL_DEBIT_AMT",
        "TOTAL_CREDIT_AMT",
        "NET_CASH_FLOW",
        "AVG_TRANSACTION_SIZE",
        "MONTHLY_SPEND_TREND",
        "SPEND_PERCENTILE",
        "TOP_SPEND_CATEGORY",
        "DIGITAL_TXN_PCT",
        "FEE_INCOME",
        "INTEREST_INCOME",
        "REVENUE_CONTRIBUTION",
        "ANOMALY_FLAG",
        "MODEL_VERSION",
        "EFFECTIVE_DATE",
        "LOAD_TS",
    ]
    assert stats.n == 10
    assert {r["REPORTING_PERIOD"] for r in result} == {"2026-08"}
    assert {r["MODEL_VERSION"] for r in result} == {"TXN_V2.1"}
    assert {r["EFFECTIVE_DATE"] for r in result} == {date(2026, 8, 22)}
    percentiles = sorted(int(r["SPEND_PERCENTILE"]) for r in result)
    assert percentiles == [9, 18, 27, 36, 45, 54, 63, 72, 81, 90]


def test_validate_rejects_low_row_count_and_nulls(spark: SparkSession):
    df = spark.createDataFrame(
        [(1, "2026-08", 5), (2, "2026-08", 7)],
        ["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"],
    )
    assert validate(df, min_rows=2) == 2

    with pytest.raises(ValidationError, match="row count"):
        validate(df, min_rows=1000)

    with_nulls = spark.createDataFrame(
        [(1, "2026-08", 5), (2, None, 7)],
        ["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"],
    )
    with pytest.raises(ValidationError, match="null rate"):
        validate(with_nulls, min_rows=1)

    dupes = spark.createDataFrame(
        [(1, "2026-08", 5), (1, "2026-08", 7)],
        ["CUSTOMER_ID", "REPORTING_PERIOD", "TOTAL_TRANSACTIONS"],
    )
    with pytest.raises(ValidationError, match="duplicate"):
        validate(dupes, min_rows=1)


def test_default_reporting_period():
    assert default_reporting_period(date(2026, 8, 22)) == "2026-08"
