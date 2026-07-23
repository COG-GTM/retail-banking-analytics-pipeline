"""SparkSession factory.

Replaces the SAS ``%connect_teradata`` macro. Teradata connectivity is REMOVED:
there is no Databricks equivalent of the SAS/ACCESS LIBNAME connections, and the
hardcoded ``{SAS004}`` service-account passwords are eliminated entirely. On
Databricks the runtime provides a Delta-enabled ``spark`` session; for local runs
and tests this factory builds an equivalent local session with the Delta Lake SQL
extensions and catalog configured.
"""
from __future__ import annotations

from pyspark.sql import SparkSession

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def get_spark(app_name: str = "retail_banking_analytics") -> SparkSession:
    """Return a Delta-enabled :class:`SparkSession`.

    Reuses the active session when one exists (e.g. Databricks); otherwise builds
    a local session with the Delta packages resolved and the SQL extensions set.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.appName(app_name).master("local[*]")
    for key, value in _DELTA_CONF.items():
        builder = builder.config(key, value)

    return configure_spark_with_delta_pip(builder).getOrCreate()
