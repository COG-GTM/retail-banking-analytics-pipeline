"""Spark session construction for the risk-scoring pipeline."""

from __future__ import annotations

from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "retail_banking_risk_scoring"


def build_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    *,
    master: str | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    """Return the shared SparkSession.

    Deterministic settings matter for oracle parity: shuffle partitions are
    pinned and adaptive coalescing is left on Spark defaults so repeated runs
    over the same input produce identical output.
    """
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    builder = builder.config("spark.sql.session.timeZone", "UTC")
    builder = builder.config("spark.sql.shuffle.partitions", "8")
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
