"""Spark session factory.

Adaptive query execution and skew-join handling are on by default: the transaction tables are
heavily skewed by ``ACCOUNT_ID`` at the 10M-customer target, and the legacy Teradata plans
relied on the optimiser's statistics (``COLLECT STATISTICS``) that Spark does not have.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import SparkSession

DEFAULT_CONF: dict[str, str] = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    "spark.sql.autoBroadcastJoinThreshold": str(64 * 1024 * 1024),
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.execution.arrow.pyspark.enabled": "false",
}

TEST_CONF: dict[str, str] = {
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "4",
    "spark.ui.enabled": "false",
    "spark.sql.adaptive.enabled": "true",
}


def build_spark_session(
    app_name: str = "retail_banking_analytics",
    *,
    master: str | None = None,
    conf: dict[str, str] | None = None,
    log_level: str = "WARN",
) -> SparkSession:
    """Create (or reuse) the pipeline's Spark session."""

    builder = SparkSession.builder.appName(app_name)
    resolved_master = master or os.environ.get("SPARK_MASTER")
    if resolved_master:
        builder = builder.master(resolved_master)

    settings = dict(DEFAULT_CONF)
    settings.update(conf or {})
    for key, value in settings.items():
        builder = builder.config(key, value)

    jars = os.environ.get("SPARK_EXTRA_JARS")
    if jars:
        builder = builder.config("spark.jars", jars)

    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(log_level)
    return session


@contextmanager
def spark_session(
    app_name: str = "retail_banking_analytics", **kwargs: str
) -> Iterator[SparkSession]:
    """Context manager wrapper used by the CLI entry points."""

    session = build_spark_session(app_name, **kwargs)
    try:
        yield session
    finally:
        session.stop()
