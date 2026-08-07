"""Data quality checks — replaces the ``%validate_table`` SAS macro.

Implements the same three checks in the same order and with the same severity:

1. ``min_rows``    — table has at least N rows (hard failure)
2. ``key_cols``    — no duplicate key groups (hard failure)
3. ``not_null``    — columns free of NULLs (warning only, as in the macro)

A hard failure raises :class:`DataQualityError`, which aborts the Databricks
task the same way ``%abort cancel`` aborted the SAS step.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Raised when a validation check fails, aborting the task."""


def validate_dataframe(
    df: DataFrame,
    name: str,
    key_cols: list[str] | None = None,
    not_null: list[str] | None = None,
    min_rows: int = 1,
) -> int:
    """Run the standard checks against ``df``; returns the validated row count."""
    print(f"[validate_table] Validating {name}")

    row_count = df.count()
    if row_count < min_rows:
        raise DataQualityError(
            f"[validate_table] {name} has {row_count} rows (minimum: {min_rows})"
        )
    print(f"[validate_table] Row count OK: {row_count} (min: {min_rows})")

    if key_cols:
        dup_groups = (
            df.groupBy(*key_cols).agg(F.count(F.lit(1)).alias("_n")).where(F.col("_n") > 1).count()
        )
        if dup_groups > 0:
            raise DataQualityError(
                f"[validate_table] {name} has {dup_groups} duplicate key groups on "
                f"({', '.join(key_cols)})"
            )
        print(f"[validate_table] Key uniqueness OK on ({', '.join(key_cols)})")

    if not_null:
        null_counts = df.agg(
            *[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in not_null]
        ).collect()[0]
        for col in not_null:
            nulls = null_counts[col] or 0
            if nulls > 0:
                print(f"WARNING: [validate_table] {name}.{col} has {nulls} NULL values")
            else:
                print(f"[validate_table] NOT NULL check passed for {col}")

    print(f"[validate_table] All checks passed for {name}")
    return row_count


def validate_table(
    spark: SparkSession,
    table: str,
    key_cols: list[str] | None = None,
    not_null: list[str] | None = None,
    min_rows: int = 1,
) -> int:
    """Same as :func:`validate_dataframe` but reads a managed table by name."""
    return validate_dataframe(
        spark.table(table),
        name=table,
        key_cols=key_cols,
        not_null=not_null,
        min_rows=min_rows,
    )
