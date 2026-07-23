"""Shared pytest fixtures.

Provides a session-scoped, Delta-enabled local ``SparkSession`` for tests that
need Spark. Config/secret unit tests are pure-Python and do not use it, but the
fixture is defined here per the shared ``pyspark/`` layout so job/transform tests
(other tickets) can depend on it. It is lazy: no Spark is started unless a test
requests the ``spark`` fixture.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retail-banking-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "1")
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        pass

    session = builder.getOrCreate()
    yield session
    session.stop()
