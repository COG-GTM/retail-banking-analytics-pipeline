"""Data-quality validation.

Replaces the SAS ``%validate_table`` macro + ``%ABORT CANCEL``.  ``validate_table``
raises :class:`ValidationError` (aborting the job) when a table fails any of the
requested checks: minimum row count, NOT NULL columns, and key uniqueness.

NOTE (parallel-migration stub): Ticket 3 owns the canonical implementation.
This contract-compatible stub keeps ``validate_table`` identical so it is a
drop-in for the owning ticket's version.
"""

from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a DataFrame fails a data-quality assertion."""


def validate_table(
    df: DataFrame,
    *,
    min_rows: int = 1,
    not_null_cols: Optional[Sequence[str]] = None,
    unique_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Validate ``df`` and return it unchanged, or raise :class:`ValidationError`."""
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row count {row_count} is below minimum {min_rows}"
        )

    if not_null_cols:
        null_counts = df.select(
            [
                F.sum(F.col(c).isNull().cast("int")).alias(c)
                for c in not_null_cols
            ]
        ).first()
        offenders = {
            c: null_counts[c] for c in not_null_cols if (null_counts[c] or 0) > 0
        }
        if offenders:
            raise ValidationError(f"NOT NULL violation(s): {offenders}")

    if unique_keys:
        keys = list(unique_keys)
        dup_groups = (
            df.groupBy(*keys).count().filter(F.col("count") > 1).count()
        )
        if dup_groups > 0:
            raise ValidationError(
                f"{dup_groups} duplicate key group(s) on {keys}"
            )

    return df
