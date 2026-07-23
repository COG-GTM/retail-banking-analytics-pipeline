"""Data-quality assertions run on load.

Minimal stub matching the shared Ticket-3 contract so this ticket's PR is
self-contained; the owning ticket's version supersedes it at merge. Replaces the
SAS ``%validate_table`` macro + ``%ABORT CANCEL``.
"""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a DataFrame fails a data-quality assertion."""


def validate_table(
    df: DataFrame,
    *,
    min_rows: int = 1,
    not_null_cols: Sequence[str] | None = None,
    unique_keys: Sequence[str] | None = None,
) -> DataFrame:
    """Assert row-count, not-null and uniqueness constraints; raise on failure.

    Returns the input DataFrame unchanged so it can be used inline.
    """
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row-count check failed: got {row_count}, expected >= {min_rows}"
        )

    if not_null_cols:
        null_counts = df.select(
            [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in not_null_cols]
        ).collect()[0].asDict()
        offenders = {c: n for c, n in null_counts.items() if n and n > 0}
        if offenders:
            raise ValidationError(f"not-null check failed for columns: {offenders}")

    if unique_keys:
        distinct_keys = df.select(*unique_keys).distinct().count()
        if distinct_keys != row_count:
            raise ValidationError(
                "uniqueness check failed for keys "
                f"{list(unique_keys)}: {row_count} rows but {distinct_keys} distinct keys"
            )

    return df


__all__ = ["ValidationError", "validate_table"]
