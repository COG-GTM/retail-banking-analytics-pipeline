"""SparkSession factory (Delta Lake enabled).

On Databricks the active runtime session already has Delta + Unity Catalog
configured, so we reuse it.  Locally we build a Delta-enabled session backed by
the built-in ``spark_catalog`` (which is configured as a ``DeltaCatalog``); this
lets three-part ``catalog.schema.table`` identifiers resolve in tests when the
``catalog`` is set to ``spark_catalog``.

NOTE (parallel-ticket stub): tickets 1/2/3 own the canonical version; this is a
minimal contract-compatible implementation for Ticket 9.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

_LOCAL_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.session.timeZone": "UTC",
    "spark.ui.enabled": "false",
}


def get_spark(app_name: str = "retail-banking-analytics") -> SparkSession:
    """Return a Delta-enabled SparkSession, reusing the active one if present."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
    )
    for key, value in _LOCAL_CONF.items():
        builder = builder.config(key, value)

    warehouse = os.environ.get("SPARK_WAREHOUSE_DIR")
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        # delta-spark jar already on the classpath (e.g. Databricks) -- extensions
        # are enabled via config above.
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
