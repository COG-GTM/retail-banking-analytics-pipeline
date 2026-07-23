"""Unit tests for common.validation (validate_table)."""
from __future__ import annotations

import pytest

from common.validation import ValidationError, validate_table


@pytest.fixture()
def sample_df(spark):
    return spark.createDataFrame(
        [
            (1, "alice", "A"),
            (2, "bob", "B"),
            (3, "carol", None),
        ],
        schema="customer_id INT, name STRING, segment STRING",
    )


def test_passes_all_checks(sample_df):
    result = validate_table(
        sample_df,
        min_rows=1,
        not_null_cols=["customer_id", "name"],
        unique_keys=["customer_id"],
    )
    assert result is sample_df


def test_min_rows_pass(sample_df):
    assert validate_table(sample_df, min_rows=3) is sample_df


def test_min_rows_failure(sample_df):
    with pytest.raises(ValidationError, match="row count"):
        validate_table(sample_df, min_rows=10)


def test_empty_dataframe_failure(spark):
    empty = spark.createDataFrame([], schema="customer_id INT")
    with pytest.raises(ValidationError, match="row count"):
        validate_table(empty)


def test_not_null_pass(sample_df):
    assert validate_table(sample_df, not_null_cols=["customer_id", "name"]) is sample_df


def test_not_null_failure(sample_df):
    with pytest.raises(ValidationError, match="NULL values"):
        validate_table(sample_df, not_null_cols=["segment"])


def test_unique_keys_pass(sample_df):
    assert validate_table(sample_df, unique_keys=["customer_id"]) is sample_df


def test_unique_keys_failure(spark):
    df = spark.createDataFrame(
        [(1, "a"), (1, "b"), (2, "c")],
        schema="customer_id INT, name STRING",
    )
    with pytest.raises(ValidationError, match="duplicate key"):
        validate_table(df, unique_keys=["customer_id"])


def test_composite_unique_keys(spark):
    df = spark.createDataFrame(
        [(1, "x", 10), (1, "y", 20), (1, "x", 30)],
        schema="customer_id INT, k STRING, v INT",
    )
    # unique on (customer_id) fails, but (customer_id, k) also has a dup (1, x)
    with pytest.raises(ValidationError, match="duplicate key"):
        validate_table(df, unique_keys=["customer_id", "k"])


def test_missing_column_raises(sample_df):
    with pytest.raises(ValidationError, match="not found"):
        validate_table(sample_df, not_null_cols=["nonexistent"])
