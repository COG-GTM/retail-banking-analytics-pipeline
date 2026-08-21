import pytest

from pipeline_utils.validation import ValidationError, validate_dataframe

ROWS = [
    (1, "Angel Johnson", "A"),
    (2, "Joshua Long", "A"),
    (3, None, "A"),
]
COLUMNS = ["CUSTOMER_ID", "FULL_NAME", "CUSTOMER_STATUS"]


@pytest.fixture
def df(spark):
    return spark.createDataFrame(ROWS, COLUMNS)


def test_row_count_below_minimum_fails(df):
    result = validate_dataframe(df, table="T", min_rows=1000)
    assert not result.passed
    assert result.rc == 1
    assert "minimum: 1000" in result.summary()
    with pytest.raises(ValidationError):
        result.raise_for_status()


def test_duplicate_keys_fail(spark):
    df = spark.createDataFrame(ROWS + [(1, "Duplicate", "A")], COLUMNS)
    result = validate_dataframe(df, table="T", key_cols=["CUSTOMER_ID"])
    assert not result.passed
    assert "duplicate key groups" in result.errors[0]


def test_not_null_columns_only_warn(df):
    result = validate_dataframe(
        df, table="T", key_cols=["CUSTOMER_ID"], not_null=COLUMNS, min_rows=1
    )
    assert result.passed
    assert result.warnings == ["FULL_NAME has 1 NULL values"]
    assert result.null_rates["FULL_NAME"] == pytest.approx(1 / 3)
    assert result.null_rates["CUSTOMER_ID"] == 0.0


def test_null_rate_threshold_breach_is_an_error(df):
    result = validate_dataframe(df, table="T", max_null_rate={"FULL_NAME": 0.1})
    assert not result.passed
    assert "null rate" in result.errors[0]


def test_null_rate_within_threshold_passes(df):
    result = validate_dataframe(df, table="T", max_null_rate={"FULL_NAME": 0.5})
    assert result.passed


def test_custom_threshold_assertion(df):
    failing = validate_dataframe(
        df, table="T", thresholds={"AT_LEAST_10_ROWS": lambda d: d.count() >= 10}
    )
    assert failing.errors == ["threshold assertion 'AT_LEAST_10_ROWS' failed"]

    passing = validate_dataframe(
        df, table="T", thresholds={"AT_LEAST_1_ROW": lambda d: d.count() >= 1}
    )
    assert passing.passed
    assert passing.raise_for_status() is passing
