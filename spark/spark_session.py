"""Shared SparkSession factory for the retail-banking analytics pipeline.

Centralises the Delta Lake wiring so every Spark job (DDL bootstrap, staging
transforms, data-product transforms, validation) builds an identical session.
Distributed to executors/driver via ``spark-submit --py-files``.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def build_spark(app_suffix: str | None = None) -> SparkSession:
    """Return a Delta-enabled SparkSession configured from the environment.

    Reads configuration exported by ``config/pipeline_config.cfg``:
      * SPARK_APP_NAME     - base application name
      * WAREHOUSE_LOCATION - Delta warehouse root (local path or object store)
    Master / deploy-mode / packages are supplied by ``spark-submit`` itself.
    """
    app_name = os.environ.get("SPARK_APP_NAME", "retail_banking_analytics")
    if app_suffix:
        app_name = f"{app_name}_{app_suffix}"

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    warehouse = os.environ.get("WAREHOUSE_LOCATION")
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)

    return builder.getOrCreate()
