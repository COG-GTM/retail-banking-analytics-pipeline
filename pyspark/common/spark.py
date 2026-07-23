"""SparkSession factory.

Minimal stub matching the shared Ticket-3 contract so this ticket's PR is
self-contained. Owned by Ticket 3 (shared utilities); superseded at merge.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "retail_banking_analytics") -> SparkSession:
    """Return a Delta-enabled local :class:`SparkSession`.

    On Databricks the active session is returned unchanged. Locally a session is
    built with the Delta Lake SQL extensions and catalog configured.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
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

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


__all__ = ["get_spark"]
