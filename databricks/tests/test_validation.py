"""Unit tests for the %validate_table replacement."""

from __future__ import annotations

import pytest

from shared.validation import ValidationError, expectations, validate_and_log, validate_table

ROWS = [(1, "PREMIUM_WEALTH"), (2, "VALUE_BASIC"), (3, None)]
SCHEMA = "CUSTOMER_ID long, SEGMENT_NAME string"


def test_passing_table_has_rc_zero(spark) -> None:
    df = spark.createDataFrame(ROWS[:2], SCHEMA)
    result = validate_table(spark, "t", key_cols=["CUSTOMER_ID"], not_null=["SEGMENT_NAME"], df=df)
    assert result.passed and result.rc == 0
    assert result.row_count == 2


def test_row_count_below_min_rows_is_blocking(spark) -> None:
    df = spark.createDataFrame(ROWS, SCHEMA)
    result = validate_table(spark, "t", min_rows=1000, df=df)
    assert not result.passed and result.rc == 1
    assert "minimum: 1,000" in result.errors[0]


def test_duplicate_keys_are_blocking(spark) -> None:
    df = spark.createDataFrame([(1, "A"), (1, "B")], SCHEMA)
    result = validate_table(spark, "t", key_cols=["CUSTOMER_ID"], df=df)
    assert not result.passed
    assert "duplicate key groups" in result.errors[0]


def test_nulls_are_warnings_not_errors(spark) -> None:
    """SAS %validate_table only issued %put WARNING for NULLs."""
    df = spark.createDataFrame(ROWS, SCHEMA)
    result = validate_table(spark, "t", not_null=["SEGMENT_NAME"], df=df)
    assert result.passed
    assert result.warnings == ["SEGMENT_NAME has 1 NULL values"]


def test_validate_and_log_raises_and_records(spark, cfg) -> None:
    df = spark.createDataFrame([(1, "A"), (1, "B")], SCHEMA)
    with pytest.raises(ValidationError):
        validate_and_log(spark, cfg, "job", cfg.gold("X"), key_cols=["CUSTOMER_ID"], df=df)

    log = spark.table(cfg.ops("ETL_RUN_LOG")).where("STEP_NAME = 'VALIDATE:X'").collect()
    assert log and log[0].STATUS == "ERROR"


def test_expectations_are_dlt_ready() -> None:
    assert expectations(key_cols=["CUSTOMER_ID"], not_null=["SEGMENT_NAME"]) == {
        "customer_id_key_not_null": "CUSTOMER_ID IS NOT NULL",
        "segment_name_not_null": "SEGMENT_NAME IS NOT NULL",
    }
