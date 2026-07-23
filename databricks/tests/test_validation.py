"""Ticket 3 - PySpark validation that raises (vs SAS warn-only)."""
from __future__ import annotations

import pytest

from common.validation import DataValidationError, validate_dataframe


def _df(spark):
    return spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, None)], ["id", "val"]
    )


def test_validate_passes_and_returns_row_count(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert validate_dataframe(df, name="t", key_cols=["id"], not_null=["id", "val"]) == 2


def test_min_rows_violation_raises(spark):
    df = _df(spark)
    with pytest.raises(DataValidationError, match="below minimum"):
        validate_dataframe(df, name="t", min_rows=1000)


def test_duplicate_key_raises(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b")], ["id", "val"])
    with pytest.raises(DataValidationError, match="duplicate key"):
        validate_dataframe(df, name="t", key_cols=["id"])


def test_null_in_not_null_column_raises(spark):
    df = _df(spark)
    with pytest.raises(DataValidationError, match="NULLs found"):
        validate_dataframe(df, name="t", not_null=["val"])


def test_validate_table_reads_and_validates(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_catalog.valtest")
    spark.createDataFrame([(1,), (2,)], ["id"]).write.format("delta").mode(
        "overwrite"
    ).saveAsTable("spark_catalog.valtest.t")
    from common.validation import validate_table

    assert validate_table(spark, "spark_catalog.valtest.t", key_cols=["id"]) == 2
