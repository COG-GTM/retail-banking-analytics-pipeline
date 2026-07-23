"""SparkSession factory (Delta Lake enabled).

MINIMAL SHARED STUB (owned by Ticket 3 — shared utilities). On Databricks the
active session is returned as-is (Delta + Unity Catalog already configured);
locally a Delta-enabled ``local`` session is built with ``delta-spark``.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "retail_banking_analytics") -> SparkSession:
    """Return a Delta-enabled :class:`SparkSession`.

    Reuses the active session when present (e.g. on a Databricks cluster). For
    local runs, configures the built-in ``spark_catalog`` to be Delta-aware so
    three-level ``catalog.schema.table`` names resolve.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[1]"))
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "1"))
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    warehouse = os.environ.get("SPARK_WAREHOUSE_DIR")
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "ERROR"))
    return spark
