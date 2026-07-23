"""Data-quality validation utilities.

Replaces the SAS ``%validate_table`` macro + ``%ABORT CANCEL``: instead of
setting a return code and aborting, :func:`validate_table` raises
:class:`ValidationError` on failure so the calling job fails fast.

NOTE (parallel-ticket stub): tickets 1/2/3 own the canonical version; this is a
minimal contract-compatible implementation for Ticket 9.
"""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a DataFrame fails a data-quality check."""


def validate_table(
    df: DataFrame,
    *,
    min_rows: int = 1,
    not_null_cols: Iterable[str] | None = None,
    unique_keys: Iterable[str] | None = None,
) -> DataFrame:
    """Assert row-count, not-null and uniqueness constraints; return ``df``.

    Raises :class:`ValidationError` on the first failing check.
    """
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row count {row_count} is below minimum {min_rows}"
        )

    if not_null_cols:
        cols = list(not_null_cols)
        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("long")).alias(c) for c in cols]
        ).first()
        offenders = {c: null_counts[c] for c in cols if null_counts[c] and null_counts[c] > 0}
        if offenders:
            raise ValidationError(f"NULL values found in columns: {offenders}")

    if unique_keys:
        keys = list(unique_keys)
        distinct_keys = df.select(*keys).distinct().count()
        if distinct_keys != row_count:
            raise ValidationError(
                f"duplicate keys on {keys}: {row_count} rows but "
                f"{distinct_keys} distinct key groups"
            )

    return df
