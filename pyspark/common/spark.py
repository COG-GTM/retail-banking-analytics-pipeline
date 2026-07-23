"""Delta-enabled :class:`SparkSession` factory.

Minimal shared stub matching the ``get_spark()`` contract (owned by Ticket 3).
On Databricks the active session is already Delta/Unity-Catalog enabled and is
reused as-is; for local runs we configure the Delta session catalog so that
three-level ``catalog.schema.table`` identifiers resolve against ``spark_catalog``.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Deterministic, timezone-stable date/time handling for reproducibility.
    "spark.sql.session.timeZone": "UTC",
}


def get_spark(app_name: str = "retail-banking-analytics") -> SparkSession:
    """Return a Delta-enabled SparkSession (reuses the active one if present)."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = SparkSession.builder.appName(app_name)
    for key, value in _DELTA_CONF.items():
        builder = builder.config(key, value)

    master = os.environ.get("RBAP_SPARK_MASTER")
    if master:
        builder = builder.master(master)
        builder = builder.config("spark.sql.shuffle.partitions", "4")
        builder = builder.config("spark.ui.enabled", "false")

    warehouse = os.environ.get("RBAP_WAREHOUSE_DIR")
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        # On Databricks the Delta jars are already on the classpath.
        pass

    return builder.getOrCreate()
