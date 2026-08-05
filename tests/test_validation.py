"""Tests for the ``%validate_table`` port, especially its asymmetry."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from risk_scoring.audit import AuditLog
from risk_scoring.validation import ValidationError, validate_table

SCHEMA = StructType([
    StructField("CUSTOMER_ID", LongType(), True),
    StructField("COMPOSITE_RISK_SCORE", DoubleType(), True),
    StructField("RISK_TIER", StringType(), True),
])

TABLE = "CUSTOMER_RISK_FINAL"
NOT_NULL = ("CUSTOMER_ID", "COMPOSITE_RISK_SCORE", "RISK_TIER")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[2]")
        .appName("risk_scoring_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def frame(spark, rows):
    return spark.createDataFrame(rows, schema=SCHEMA)


def clean_rows(n: int):
    return [(1000 + i, 42.0 + i, "MODERATE") for i in range(n)]


def test_row_count_below_min_rows_is_an_error(spark):
    result = validate_table(
        frame(spark, clean_rows(2)), table=TABLE, key_cols=["CUSTOMER_ID"], min_rows=3
    )
    assert result.passed is False
    assert result.row_count == 2
    assert result.errors == [f"{TABLE} has 2 rows (minimum: 3)"]


def test_row_count_exactly_min_rows_passes(spark):
    result = validate_table(frame(spark, clean_rows(3)), table=TABLE, min_rows=3)
    assert result.passed is True
    assert result.errors == []


def test_row_count_above_min_rows_passes(spark):
    result = validate_table(frame(spark, clean_rows(5)), table=TABLE, min_rows=3)
    assert result.passed is True


def test_empty_table_fails_default_min_rows(spark):
    result = validate_table(frame(spark, []), table=TABLE)
    assert result.passed is False
    assert result.row_count == 0


def test_duplicate_keys_are_an_error(spark):
    rows = clean_rows(3) + [(1000, 1.0, "LOW"), (1001, 2.0, "LOW")]
    result = validate_table(
        frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID"], min_rows=1
    )
    assert result.passed is False
    assert result.errors == [f"{TABLE} has 2 duplicate key groups on (CUSTOMER_ID)"]


def test_unique_keys_pass(spark):
    result = validate_table(
        frame(spark, clean_rows(4)), table=TABLE, key_cols=["CUSTOMER_ID"], min_rows=1
    )
    assert result.passed is True
    assert result.errors == []


def test_composite_key_uniqueness(spark):
    """(CUSTOMER_ID, RISK_TIER) is unique even though CUSTOMER_ID alone is not."""
    rows = [(1, 1.0, "LOW"), (1, 2.0, "HIGH"), (2, 3.0, "LOW")]
    composite = validate_table(
        frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID", "RISK_TIER"]
    )
    single = validate_table(frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID"])
    assert composite.passed is True
    assert single.passed is False


def test_row_count_failure_short_circuits_the_key_check(spark):
    """SAS ``%return``s after the row-count error, so duplicates go unreported."""
    rows = [(1, 1.0, "LOW"), (1, 2.0, "LOW")]
    result = validate_table(
        frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID"], min_rows=10
    )
    assert result.passed is False
    assert len(result.errors) == 1
    assert "duplicate" not in result.errors[0]


def test_key_failure_short_circuits_the_not_null_check(spark):
    rows = [(1, 1.0, "LOW"), (1, None, None)]
    result = validate_table(
        frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID"], not_null=NOT_NULL
    )
    assert result.passed is False
    assert result.warnings == []


def test_nulls_only_warn(spark):
    rows = [(1, None, "LOW"), (2, 2.0, "HIGH"), (None, 3.0, "LOW")]
    result = validate_table(
        frame(spark, rows), table=TABLE, key_cols=["CUSTOMER_ID"], not_null=NOT_NULL
    )
    assert result.passed is True
    assert result.errors == []
    assert set(result.warnings) == {
        f"{TABLE}.CUSTOMER_ID has 1 NULL values",
        f"{TABLE}.COMPOSITE_RISK_SCORE has 1 NULL values",
    }


def test_blank_strings_count_as_missing(spark):
    """SAS ``where COL is missing`` is true for a blank character value."""
    rows = [(1, 1.0, ""), (2, 2.0, "   "), (3, 3.0, "LOW")]
    result = validate_table(frame(spark, rows), table=TABLE, not_null=["RISK_TIER"])
    assert result.warnings == [f"{TABLE}.RISK_TIER has 2 NULL values"]
    assert result.passed is True


def test_no_warnings_when_not_null_columns_are_populated(spark):
    result = validate_table(frame(spark, clean_rows(3)), table=TABLE, not_null=NOT_NULL)
    assert result.warnings == []
    assert result.passed is True


def test_empty_key_cols_and_not_null_skip_their_checks(spark):
    rows = [(1, None, None), (1, None, None)]
    result = validate_table(frame(spark, rows), table=TABLE, min_rows=1)
    assert result.passed is True
    assert result.errors == []
    assert result.warnings == []


def test_unknown_column_raises(spark):
    with pytest.raises(ValueError, match="NO_SUCH_COLUMN"):
        validate_table(frame(spark, clean_rows(2)), table=TABLE, not_null=["NO_SUCH_COLUMN"])


def test_audit_trail_records_error(spark):
    audit = AuditLog("03_RISK_SCORING")
    validate_table(frame(spark, clean_rows(1)), table=TABLE, min_rows=10, audit=audit)
    assert [r.status for r in audit.records] == ["ERROR"]


def test_audit_trail_records_warning_then_success(spark):
    audit = AuditLog("03_RISK_SCORING")
    rows = [(1, None, "LOW"), (2, 2.0, "HIGH")]
    validate_table(frame(spark, rows), table=TABLE, not_null=NOT_NULL, audit=audit)
    assert [r.status for r in audit.records] == ["WARNING", "SUCCESS"]


def test_caller_raises_validation_error(spark):
    """The driver mirrors ``%abort cancel``; ``validate_table`` itself does not raise."""
    result = validate_table(frame(spark, clean_rows(1)), table=TABLE, min_rows=10)
    with pytest.raises(ValidationError):
        if not result.passed:
            raise ValidationError("; ".join(result.errors))
