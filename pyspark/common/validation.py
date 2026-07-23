"""Data-quality validation.

Minimal stub matching the shared Ticket-3 contract so this ticket's PR is
self-contained. Owned by Ticket 3 (shared utilities); superseded at merge.

Replaces the SAS ``%validate_table`` macro + ``%ABORT CANCEL``.
"""
from __future__ import annotations

from typing import Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a data-quality assertion fails."""


def validate_table(
    df: DataFrame,
    *,
    min_rows: int = 1,
    not_null_cols: Optional[Sequence[str]] = None,
    unique_keys: Optional[Sequence[str]] = None,
) -> DataFrame:
    """Validate ``df`` and return it unchanged, or raise :class:`ValidationError`.

    Checks: minimum row count, no NULLs in ``not_null_cols``, and uniqueness of
    the ``unique_keys`` composite key.
    """
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row count {row_count} is below minimum {min_rows}"
        )

    if not_null_cols:
        null_counts = df.select(
            [
                F.sum(F.col(c).isNull().cast("long")).alias(c)
                for c in not_null_cols
            ]
        ).first()
        offenders = {
            c: null_counts[c] for c in not_null_cols if (null_counts[c] or 0) > 0
        }
        if offenders:
            raise ValidationError(f"NULL values found in columns: {offenders}")

    if unique_keys:
        keys = list(unique_keys)
        dup_count = (
            df.groupBy(*keys).count().where(F.col("count") > 1).count()
        )
        if dup_count > 0:
            raise ValidationError(
                f"{dup_count} duplicate key groups on {keys}"
            )

    return df


__all__ = ["validate_table", "ValidationError"]
