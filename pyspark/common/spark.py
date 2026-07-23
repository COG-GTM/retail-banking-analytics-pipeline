"""SparkSession factory (Delta-enabled).

Minimal stub matching the shared contract so this ticket's PR is self-contained.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "retail_banking_analytics") -> SparkSession:
    """Return a Delta-enabled SparkSession.

    On Databricks the active session already has Delta configured; locally we
    wire up the Delta extension via ``delta-spark``.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        pass

    return builder.getOrCreate()


__all__ = ["get_spark"]
