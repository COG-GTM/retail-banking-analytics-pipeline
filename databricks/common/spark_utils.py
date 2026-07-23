"""Delta-enabled :class:`SparkSession` helper.

On Databricks the active session already has Delta + Unity Catalog wired in, so
:func:`get_spark` simply returns it. Off-cluster (local runs, pytest, CI) it
builds a local session configured with the Delta extensions so the exact same
transformation and write code exercises real Delta tables.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "retail_banking_analytics", warehouse_dir: str | None = None) -> SparkSession:
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if warehouse_dir is not None:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        # Delta jars already on the classpath (e.g. on a Databricks cluster).
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
