"""Data quality validation.

Replaces ``sas/macros/validate_table.sas`` (``%validate_table`` plus the
``%if &VALIDATION_RC. ne 0 %then %abort cancel;`` pattern) with a Python
utility supporting row-count, key-uniqueness, null-rate and generic threshold
assertions. Failing assertions raise :class:`ValidationError`, which aborts the
Synapse Spark job and is recorded in the run-log.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from pyspark.sql import functions as F


class ValidationError(RuntimeError):
    """Raised when a validation check fails (the ``%ABORT CANCEL`` equivalent)."""


@dataclass
class ValidationResult:
    """Outcome of validating one table."""

    table: str
    row_count: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    null_rates: Dict[str, float] = field(default_factory=dict)

    @property
    def rc(self) -> int:
        """The ``&VALIDATION_RC`` equivalent: 0 = pass, 1 = fail."""
        return 1 if self.errors else 0

    @property
    def passed(self) -> bool:
        return self.rc == 0

    def summary(self) -> str:
        if self.passed:
            return f"{self.table}: all checks passed ({self.row_count} rows)"
        return f"{self.table}: {'; '.join(self.errors)}"

    def raise_for_status(self) -> "ValidationResult":
        if not self.passed:
            raise ValidationError(self.summary())
        return self


def validate_dataframe(
    df,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int = 1,
    max_null_rate: Optional[Dict[str, float]] = None,
    thresholds: Optional[Dict[str, Callable[[object], bool]]] = None,
) -> ValidationResult:
    """Validate a Spark DataFrame.

    Args:
        df: DataFrame to validate.
        table: Name used in messages and the run-log.
        key_cols: Columns that must be unique together.
        not_null: Columns whose NULLs are reported (warning, as in SAS) unless a
            stricter ``max_null_rate`` is supplied for the same column.
        min_rows: Minimum acceptable row count; a breach is an error.
        max_null_rate: Per-column maximum null rate in ``[0, 1]``; a breach is an
            error.
        thresholds: Named predicates evaluated against an aggregate of the
            DataFrame, e.g. ``{"MAX_PD_LE_1": lambda d: d.agg(...)}``. A
            predicate returning False is an error.

    Returns:
        A :class:`ValidationResult`; call ``raise_for_status()`` to abort.
    """
    row_count = df.count()
    result = ValidationResult(table=table, row_count=row_count)

    if row_count < min_rows:
        result.errors.append(f"has {row_count} rows (minimum: {min_rows})")
        return result

    if key_cols:
        duplicate_groups = (
            df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        )
        if duplicate_groups:
            result.errors.append(
                f"has {duplicate_groups} duplicate key groups on ({', '.join(key_cols)})"
            )

    max_null_rate = max_null_rate or {}
    columns_to_profile = list(dict.fromkeys(list(not_null) + list(max_null_rate)))
    if columns_to_profile:
        null_counts = df.agg(
            *[
                F.sum(F.col(column).isNull().cast("long")).alias(column)
                for column in columns_to_profile
            ]
        ).collect()[0]

        for column in columns_to_profile:
            null_count = int(null_counts[column] or 0)
            rate = null_count / row_count if row_count else 0.0
            result.null_rates[column] = rate

            if column in max_null_rate and rate > max_null_rate[column]:
                result.errors.append(
                    f"{column} null rate {rate:.4f} exceeds maximum "
                    f"{max_null_rate[column]:.4f}"
                )
            elif column in not_null and null_count > 0:
                result.warnings.append(f"{column} has {null_count} NULL values")

    for name, predicate in (thresholds or {}).items():
        if not predicate(df):
            result.errors.append(f"threshold assertion '{name}' failed")

    return result
