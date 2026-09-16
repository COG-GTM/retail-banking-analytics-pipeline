import pytest

from retail_banking.validation import raise_if_failed, validate_table


def test_min_rows_fail(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "v"])
    res = validate_table(df, "t", min_rows=2)
    assert not res.passed
    with pytest.raises(ValueError):
        raise_if_failed(res)


def test_duplicate_key_fail(spark):
    df = spark.createDataFrame([(1, "a"), (1, "b")], ["id", "v"])
    res = validate_table(df, "t", key_cols=["id"], min_rows=1)
    assert not res.passed
    assert res.duplicate_key_groups == 1


def test_null_is_warning_only(spark):
    df = spark.createDataFrame([(1, None), (2, "x")], ["id", "v"])
    res = validate_table(df, "t", key_cols=["id"], not_null=["v"],
                         min_rows=1)
    assert res.passed
    assert res.null_counts["v"] == 1


def test_all_pass(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "v"])
    res = validate_table(df, "t", key_cols=["id"], not_null=["id", "v"],
                         min_rows=1)
    assert res.passed
    assert res.row_count == 2
    assert raise_if_failed(res) is res
