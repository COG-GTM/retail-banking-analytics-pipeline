"""Tests for the data-quality validation gate."""
from __future__ import annotations

import pytest

from spark.validation import ValidationError, enforce, validate_table


def _df(spark):
    return spark.createDataFrame(
        [(1, "a"), (2, "b"), (3, None)], ["customer_id", "name"]
    )


def test_min_rows_failure(spark):
    result = validate_table(_df(spark), "T", min_rows=10)
    assert not result.passed
    assert result.rc == 1
    assert result.row_count == 3


def test_duplicate_key_failure(spark):
    df = spark.createDataFrame([(1,), (1,), (2,)], ["customer_id"])
    result = validate_table(df, "T", key_cols=["customer_id"], min_rows=1)
    assert not result.passed


def test_not_null_is_warning_only(spark, audit):
    # NULL in a not_null column should NOT fail the gate (warning only).
    result = validate_table(
        _df(spark), "T", key_cols=["customer_id"],
        not_null=["name"], min_rows=1, audit=audit,
    )
    assert result.passed
    assert any("NULL" in m for m in result.messages)


def test_enforce_raises_on_failure(spark, audit):
    result = validate_table(_df(spark), "T", min_rows=10)
    with pytest.raises(ValidationError):
        enforce(result, "T", audit)
