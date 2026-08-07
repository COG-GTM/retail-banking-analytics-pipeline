"""Delta write helpers.

Replaces two legacy patterns:

* BTEQ ``CREATE MULTISET TABLE ... PRIMARY INDEX (CUSTOMER_ID)`` +
  ``COLLECT STATISTICS`` -> Delta ``saveAsTable`` + liquid clustering (or
  ``OPTIMIZE ... ZORDER BY``) on the former primary-index columns.
* SAS ``PROC SQL DELETE`` + ``PROC APPEND FORCE`` truncate-and-load ->
  an atomic ``overwrite`` (default) or a keyed ``MERGE``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import DataFrame, SparkSession

from shared.config import PipelineConfig
from shared.logging_utils import get_logger, log_event
from shared.schemas import CLUSTER_KEYS

_logger = get_logger()


def ensure_schemas(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Create the Unity Catalog schemas backing the medallion layers."""
    for schema in cfg.schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")


def _cluster_columns(table: str, override: Sequence[str] | None) -> list[str]:
    if override is not None:
        return list(override)
    return CLUSTER_KEYS.get(table.split(".")[-1].upper(), [])


def write_table(
    spark: SparkSession,
    cfg: PipelineConfig,
    df: DataFrame,
    table: str,
    merge_keys: Sequence[str] | None = None,
    cluster_by: Sequence[str] | None = None,
    mode: str | None = None,
) -> int:
    """Persist ``df`` to ``table`` and return the resulting row count.

    ``mode='overwrite'`` reproduces the legacy truncate-and-load exactly;
    ``mode='merge'`` upserts on ``merge_keys`` for incremental reruns. Both are
    idempotent for a given ``run_date``.
    """
    mode = (mode or cfg.write_mode).lower()
    cluster_cols = _cluster_columns(table, cluster_by)

    if mode == "merge" and merge_keys:
        _merge(spark, cfg, df, table, merge_keys, cluster_cols)
    else:
        writer = df.write.format(cfg.table_format).mode("overwrite").option(
            "overwriteSchema", "true"
        )
        if cluster_cols and cfg.table_format == "delta":
            writer = writer.clusterBy(*cluster_cols)
        writer.saveAsTable(table)

    optimize(spark, cfg, table, cluster_cols)
    return spark.table(table).count()


def _merge(
    spark: SparkSession,
    cfg: PipelineConfig,
    df: DataFrame,
    table: str,
    merge_keys: Sequence[str],
    cluster_cols: Sequence[str],
) -> None:
    if not spark.catalog.tableExists(table):
        writer = df.write.format(cfg.table_format).mode("overwrite")
        if cluster_cols and cfg.table_format == "delta":
            writer = writer.clusterBy(*cluster_cols)
        writer.saveAsTable(table)
        return

    source_view = f"_merge_src_{table.split('.')[-1].lower()}"
    df.createOrReplaceTempView(source_view)
    on_clause = " AND ".join(f"tgt.{k} = src.{k}" for k in merge_keys)
    spark.sql(
        f"""
        MERGE INTO {table} AS tgt
        USING {source_view} AS src
          ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView(source_view)


def optimize(
    spark: SparkSession, cfg: PipelineConfig, table: str, cluster_cols: Sequence[str]
) -> None:
    """Compact the table, replacing Teradata ``COLLECT STATISTICS``.

    Tables written by :func:`write_table` use liquid clustering, for which a
    bare ``OPTIMIZE`` performs the clustering. Tables created outside this
    module fall back to ``OPTIMIZE ... ZORDER BY`` on the same columns.
    """
    if not cfg.optimize_tables or cfg.table_format != "delta":
        return
    try:
        spark.sql(f"OPTIMIZE {table}")
        return
    except Exception as exc:  # noqa: BLE001 - optimization is best-effort
        if not cluster_cols:
            log_event(
                _logger,
                "optimize_failed",
                level=logging.WARNING,
                run_id=cfg.run_id,
                table=table,
                error=str(exc)[:500],
            )
            return
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({', '.join(cluster_cols)})")
    except Exception as zexc:  # noqa: BLE001
        log_event(
            _logger,
            "optimize_zorder_failed",
            level=logging.WARNING,
            run_id=cfg.run_id,
            table=table,
            zorder_by=list(cluster_cols),
            error=str(zexc)[:500],
        )
