"""Shared pytest fixtures and helpers for the staging PySpark unit tests."""
from __future__ import annotations

import os
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql.types import DecimalType  # noqa: E402

from staging.spark_utils import SOURCE_SCHEMAS  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.appName("staging_tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _coerce(value: Any, dtype) -> Any:
    """Coerce plain Python literals to the exact type the schema expects."""
    if value is None:
        return None
    if isinstance(dtype, DecimalType) and not isinstance(value, Decimal):
        return Decimal(str(value))
    return value


def make_df(spark: SparkSession, table: str, rows: list[dict]) -> DataFrame:
    """Build a source DataFrame for ``table`` from a list of partial dicts.

    Unspecified columns default to ``None``; values are coerced to the
    canonical source schema (from :data:`staging.spark_utils.SOURCE_SCHEMAS`).
    """
    schema = SOURCE_SCHEMAS[table]
    data = [
        tuple(_coerce(r.get(f.name), f.dataType) for f in schema.fields)
        for r in rows
    ]
    return spark.createDataFrame(data, schema)


__all__ = ["spark", "make_df", "date", "datetime", "Decimal"]
