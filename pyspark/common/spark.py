"""SparkSession factory (Delta-enabled).

Minimal stub matching the shared Ticket-1/3 contract so this ticket's PR is
self-contained; the owning ticket's version supersedes it at merge.
"""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession


def _ensure_compatible_java() -> None:
    """Point ``JAVA_HOME`` at a Spark-compatible JDK (11/17) for local runs.

    PySpark 3.5 does not run on JDK 21+. On Databricks the platform sets a
    compatible ``JAVA_HOME`` and this is a no-op.
    """
    for candidate in ("/usr/lib/jvm/java-17-openjdk-amd64", "/usr/lib/jvm/java-11-openjdk-amd64"):
        if os.path.isdir(candidate):
            os.environ.setdefault("JAVA_HOME_FORCED", "1")
            os.environ["JAVA_HOME"] = candidate
            os.environ["PATH"] = os.path.join(candidate, "bin") + os.pathsep + os.environ.get("PATH", "")
            return
    matches = sorted(glob.glob("/usr/lib/jvm/java-1[17]-openjdk*"))
    if matches:
        os.environ["JAVA_HOME"] = matches[0]
        os.environ["PATH"] = os.path.join(matches[0], "bin") + os.pathsep + os.environ.get("PATH", "")


def get_spark(app_name: str = "retail_banking_analytics") -> SparkSession:
    """Return a Delta-enabled local/Databricks :class:`SparkSession`."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    _ensure_compatible_java()

    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(_builder(app_name)).getOrCreate()
    except Exception:
        # Fallback for environments where Delta is provided by the runtime.
        return _builder(app_name).getOrCreate()


def _builder(app_name: str):
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
    )
    warehouse = os.environ.get("SPARK_WAREHOUSE_DIR")
    if warehouse:
        builder = builder.config("spark.sql.warehouse.dir", warehouse)
    return builder
