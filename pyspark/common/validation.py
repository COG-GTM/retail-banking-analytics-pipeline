"""Data-quality validation (validation contract).

Replaces the SAS ``%validate_table`` macro + ``%ABORT CANCEL``. Minimal shared
stub owned by Ticket 3; signature kept identical to the contract.
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
    """Assert row-count, not-null and uniqueness constraints on ``df``.

    Returns the (unchanged) DataFrame so callers can chain, and raises
    :class:`ValidationError` on the first failed check.
    """
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row-count check failed: {row_count} row(s) < min_rows={min_rows}"
        )

    if not_null_cols:
        null_counts = df.select(
            [
                F.sum(F.col(c).isNull().cast("int")).alias(c)
                for c in not_null_cols
            ]
        ).first()
        offenders = {c: null_counts[c] for c in not_null_cols if null_counts[c]}
        if offenders:
            raise ValidationError(f"not-null check failed for columns: {offenders}")

    if unique_keys:
        keys = list(unique_keys)
        dup_count = (
            df.groupBy(*keys).count().where(F.col("count") > 1).count()
        )
        if dup_count:
            raise ValidationError(
                f"uniqueness check failed: {dup_count} duplicate key group(s) "
                f"on {keys}"
            )

    return df
