"""SparkSession factory (Delta-enabled) plus thin Delta storage helpers.

``get_spark()`` returns a Delta Lake-enabled :class:`SparkSession`.  On
Databricks the active session is reused (Unity Catalog manages storage); locally
a small session is created with the Delta SQL extensions wired in and tables are
materialised as **path-based Delta** under ``Config.warehouse_dir`` so the whole
pipeline is exercisable in tests/CI without a Unity Catalog metastore.

The ``read_delta`` / ``write_delta`` helpers are the single read/write path used
by every job; they resolve the storage location from :class:`Config` so no job
ever hardcodes a path or table name.

NOTE (parallel-migration stub): Ticket 3 owns the canonical implementation.
This contract-compatible stub keeps ``get_spark()`` identical so it is a drop-in.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession

from common.config import Config

_LOCAL_CONF = {
    "spark.sql.shuffle.partitions": "4",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
    "spark.databricks.delta.snapshotPartitions": "2",
}


def get_spark(app_name: str = "retail-banking-analytics") -> SparkSession:
    """Return a Delta-enabled SparkSession, reusing the active one if present."""
    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    for key, value in _LOCAL_CONF.items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def table_path(cfg: Config, schema: str, name: str) -> str:
    """Local Delta storage location for a ``catalog.schema.table``."""
    return str(Path(cfg.warehouse_dir) / cfg.catalog / schema / name)


def read_delta(spark: SparkSession, cfg: Config, schema: str, name: str) -> DataFrame:
    """Read a Delta table addressed via :class:`Config`."""
    return spark.read.format("delta").load(table_path(cfg, schema, name))


def write_delta(
    df: DataFrame,
    cfg: Config,
    schema: str,
    name: str,
    *,
    mode: str = "overwrite",
    merge_keys: Optional[Sequence[str]] = None,
) -> None:
    """Idempotently write a Delta table addressed via :class:`Config`.

    ``mode="overwrite"`` performs a full, idempotent refresh.  When
    ``merge_keys`` is supplied an idempotent Delta ``MERGE`` on those keys is
    used instead (upsert) -- never a blind append.
    """
    path = table_path(cfg, schema, name)
    if merge_keys:
        from delta.tables import DeltaTable

        if DeltaTable.isDeltaTable(df.sparkSession, path):
            target = DeltaTable.forPath(df.sparkSession, path)
            condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
            (
                target.alias("t")
                .merge(df.alias("s"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            return
        # First write: fall through to a plain overwrite create.
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
