"""Ticket 3 - Data-quality validation.

PySpark reimplementation of the SAS ``%validate_table`` macro. Unlike the SAS
version (which only *warned* on nulls), every check here **raises**
:class:`DataValidationError` on failure so a bad table stops the pipeline
(equivalent to the SAS ``%abort cancel`` that followed each macro call):

* row count ``>= min_rows``
* uniqueness of ``key_cols``
* ``not_null`` columns contain no NULLs
"""
from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class DataValidationError(Exception):
    """Raised when a data-quality check fails."""


def validate_dataframe(
    df: DataFrame,
    *,
    name: str,
    key_cols: Optional[Sequence[str]] = None,
    not_null: Optional[Sequence[str]] = None,
    min_rows: int = 1,
) -> int:
    """Validate ``df`` and return its row count. Raises on any failure."""
    n = df.count()
    if n < min_rows:
        raise DataValidationError(
            f"[{name}] row count {n} is below minimum {min_rows}"
        )

    if key_cols:
        key_cols = list(key_cols)
        dup_groups = (
            df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        )
        if dup_groups > 0:
            raise DataValidationError(
                f"[{name}] has {dup_groups} duplicate key group(s) on {key_cols}"
            )

    if not_null:
        offending = []
        for col_name in not_null:
            null_cnt = df.filter(F.col(col_name).isNull()).count()
            if null_cnt > 0:
                offending.append(f"{col_name}={null_cnt}")
        if offending:
            raise DataValidationError(
                f"[{name}] NULLs found in NOT NULL column(s): {', '.join(offending)}"
            )

    return n


def validate_table(
    spark: SparkSession,
    table_fqn: str,
    *,
    key_cols: Optional[Sequence[str]] = None,
    not_null: Optional[Sequence[str]] = None,
    min_rows: int = 1,
) -> int:
    """Read a Delta/Unity Catalog table and validate it."""
    df = spark.table(table_fqn)
    return validate_dataframe(
        df,
        name=table_fqn,
        key_cols=key_cols,
        not_null=not_null,
        min_rows=min_rows,
    )
