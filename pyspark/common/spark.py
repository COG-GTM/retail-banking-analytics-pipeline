"""Delta-enabled :class:`SparkSession` factory.

On Databricks a Delta- and Unity-Catalog-enabled session already exists, so
``get_spark()`` simply returns the active session. For local runs (tests,
``local_runner``) it builds a session with the Delta Lake SQL extension and the
Delta catalog wired into the session catalog so that ``USING DELTA`` tables and
three-level ``catalog.schema.table`` names resolve.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # Deterministic, timezone-stable date/time handling for reproducibility (R4).
    "spark.sql.session.timeZone": "UTC",
}


def get_spark(app_name: str = "retail-banking-analytics") -> SparkSession:
    """Return a Delta-enabled :class:`SparkSession`.

    Reuses the active session when present (Databricks), otherwise builds a
    local Delta-enabled one via ``configure_spark_with_delta_pip``.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        return active
    return build_local_spark(app_name=app_name)


def build_local_spark(
    app_name: str = "retail-banking-analytics",
    master: str = "local[2]",
    shuffle_partitions: int = 4,
    warehouse_dir: str | None = None,
) -> SparkSession:
    """Build a local Delta-enabled :class:`SparkSession` (used by tests)."""
    from delta import configure_spark_with_delta_pip

    builder = SparkSession.builder.appName(app_name).master(master)
    for key, value in _DELTA_CONF.items():
        builder = builder.config(key, value)
    builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    builder = builder.config("spark.ui.enabled", "false")
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    return configure_spark_with_delta_pip(builder).getOrCreate()
