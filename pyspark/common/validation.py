"""Data-quality validation.

Minimal stub matching the shared validation contract so this ticket's PR is
self-contained. Replaces the SAS ``%validate_table`` macro + ``%ABORT CANCEL``.
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
    """Validate ``df`` and return it unchanged, or raise ``ValidationError``."""
    row_count = df.count()
    if row_count < min_rows:
        raise ValidationError(
            f"row count {row_count} is below minimum {min_rows}"
        )

    if not_null_cols:
        for col in not_null_cols:
            nulls = df.filter(F.col(col).isNull()).count()
            if nulls > 0:
                raise ValidationError(
                    f"column '{col}' has {nulls} null value(s)"
                )

    if unique_keys:
        keys = list(unique_keys)
        distinct = df.select(*keys).distinct().count()
        if distinct != row_count:
            raise ValidationError(
                f"key {keys} is not unique: {row_count} rows, "
                f"{distinct} distinct"
            )

    return df


__all__ = ["ValidationError", "validate_table"]
