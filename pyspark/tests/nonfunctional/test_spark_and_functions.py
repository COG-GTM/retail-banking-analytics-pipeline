"""Session configuration and the shared legacy-construct helpers."""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import functions as F

from common import functions as fn
from common.spark import DEFAULT_CONF, build_spark_session

pytestmark = pytest.mark.nonfunctional


def test_session_enables_aqe_and_skew_handling(spark):
    # the test session is built through the same factory
    assert spark.conf.get("spark.sql.adaptive.enabled") == "true"
    assert spark.conf.get("spark.sql.adaptive.skewJoin.enabled") == "true"
    assert DEFAULT_CONF["spark.sql.adaptive.coalescePartitions.enabled"] == "true"
    assert DEFAULT_CONF["spark.sql.sources.partitionOverwriteMode"] == "dynamic"


def test_build_spark_session_reuses_the_active_session(spark):
    assert build_spark_session("retail_banking_analytics_tests") is spark


def test_date_helpers_reproduce_teradata_semantics(spark):
    df = spark.createDataFrame([("2026-04-10", "1990-05-11")], "d string, dob string").select(
        F.col("d").cast("date").alias("d"), F.col("dob").cast("date").alias("dob")
    )

    row = df.select(
        fn.td_date_diff_days(F.col("d"), F.col("dob")).alias("days"),
        fn.td_months_between(F.col("d"), F.col("dob")).alias("months"),
        fn.td_add_months(F.col("d"), -12).alias("minus_year"),
        fn.run_date_col(date(2026, 4, 10)).alias("pinned"),
    ).collect()[0]

    assert row["days"] == 13118
    # 35 years and ~11 months, truncated by the CAST to INTEGER (Teradata truncates)
    assert row["months"] == 430
    assert str(row["minus_year"]) == "2025-04-10"
    assert str(row["pinned"]) == "2026-04-10"


def test_value_helpers(spark):
    df = spark.createDataFrame([(0.0, 5.0, None)], "zero double, five double, missing double")

    row = df.select(
        fn.nullif_zero(F.col("zero")).alias("nz"),
        fn.zero_if_null(F.col("missing")).alias("zin"),
        fn.yn(F.col("five") > 1).alias("flag"),
        fn.sas_round(F.lit(1.005), 0.01).alias("rounded"),
        fn.sas_round(F.lit(1.5), 1).alias("rounded_unit"),
        fn.clamp_0_100(F.lit(150.0)).alias("high"),
        fn.clamp_0_100(F.lit(-3.0)).alias("low"),
    ).collect()[0]

    assert row["nz"] is None
    assert row["zin"] == 0.0
    assert row["flag"] == "Y"
    assert row["rounded"] == pytest.approx(1.01, abs=0.005)
    assert row["rounded_unit"] == 2.0
    assert (row["high"], row["low"]) == (100.0, 0.0)


def test_qualify_row_number_needs_no_extra_columns(spark):
    df = spark.createDataFrame(
        [(1, 10, "a"), (1, 20, "b"), (2, 5, "c")], "k int, ord int, v string"
    )

    result = fn.qualify_row_number(df, ("k",), (F.col("ord").desc(),))

    assert result.columns == ["k", "ord", "v"]
    assert sorted(row["v"] for row in result.collect()) == ["b", "c"]
