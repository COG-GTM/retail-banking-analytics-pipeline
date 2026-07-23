"""Data-quality validation helpers.

Replaces the SAS ``%validate_table`` macro and its ``%ABORT CANCEL`` failure
path. ``validate_table`` runs row-count, not-null and key-uniqueness checks and
raises :class:`ValidationError` on the first failure (fail-fast), mirroring the
legacy ``&VALIDATION_RC ne 0`` abort semantics.
"""
from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a data-quality check fails."""


def validate_table(
    df: DataFrame,
    *,
    min_rows: int = 1,
    not_null_cols: Optional[Sequence[str]] = None,
    unique_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Validate ``df`` and return it unchanged when all checks pass.

    Checks (in order):
      * row count is at least ``min_rows``;
      * every column in ``not_null_cols`` has zero NULLs;
      * the combination of ``unique_keys`` columns has no duplicates.

    Raises :class:`ValidationError` on the first failure.
    """
    missing = [c for c in (list(not_null_cols or []) + list(unique_keys or [])) if c not in df.columns]
    if missing:
        raise ValidationError(f"columns not found in DataFrame: {missing}")

    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row count {row_count} is below minimum {min_rows}"
        )

    if not_null_cols:
        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in not_null_cols]
        ).first()
        offenders = {c: null_counts[c] for c in not_null_cols if null_counts[c]}
        if offenders:
            raise ValidationError(f"NULL values found in NOT NULL columns: {offenders}")

    if unique_keys:
        keys = list(unique_keys)
        dup_groups = (
            df.groupBy(*keys).count().filter(F.col("count") > 1).count()
        )
        if dup_groups:
            raise ValidationError(
                f"{dup_groups} duplicate key group(s) found on {keys}"
            )

    return df
