import pytest

from pipeline_utils.validation import ValidationError, validate_table


@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [(1, "Ada"), (2, "Grace"), (3, None)], "CUSTOMER_ID int, FULL_NAME string"
    )


def test_all_checks_pass(customers):
    report = validate_table(
        customers,
        table="CUSTOMERS",
        key_cols=["CUSTOMER_ID"],
        not_null=["CUSTOMER_ID"],
        min_rows=3,
    )
    assert report.passed
    assert report.abort_if_failed() is report


def test_row_count_below_minimum_fails_and_short_circuits(customers):
    report = validate_table(customers, table="CUSTOMERS", min_rows=10)
    assert not report.passed
    assert [c.name for c in report.failures] == ["row_count"]

    with pytest.raises(ValidationError, match="row_count"):
        report.abort_if_failed()


def test_duplicate_keys_fail(spark):
    df = spark.createDataFrame([(1,), (1,)], "CUSTOMER_ID int")
    report = validate_table(df, table="CUSTOMERS", key_cols=["CUSTOMER_ID"])
    assert not report.passed
    assert "1 duplicate key groups" in report.failures[0].detail


def test_null_rate_threshold(customers):
    strict = validate_table(customers, table="C", not_null=["FULL_NAME"])
    assert not strict.passed

    tolerant = validate_table(
        customers, table="C", not_null=["FULL_NAME"], max_null_rate=0.5
    )
    assert tolerant.passed


def test_custom_threshold_assertion(customers):
    report = validate_table(
        customers,
        table="C",
        thresholds=[("max_rows", lambda n: n <= 2, "at most 2 rows")],
    )
    assert not report.passed
    assert report.failures[0].name == "max_rows"
    assert "at most 2 rows" in report.summary()
