from __future__ import annotations

from dataclasses import dataclass, field

import pyspark.sql.functions as F


@dataclass
class ValidationResult:
    name: str
    passed: bool
    row_count: int
    duplicate_key_groups: int = 0
    null_counts: dict = field(default_factory=dict)
    messages: list = field(default_factory=list)


def validate_table(df, name: str, key_cols=(), not_null=(), min_rows: int = 1) -> ValidationResult:
    """Mirror of sas/macros/validate_table.sas.

    - row_count < min_rows            -> fail
    - duplicate key groups > 0        -> fail
    - nulls in not_null columns       -> WARNING only (does not fail)
    """
    messages: list[str] = []
    null_counts: dict[str, int] = {}
    row_count = df.count()

    if row_count < min_rows:
        messages.append(
            f"ERROR: [validate_table] {name} has {row_count} rows (minimum: {min_rows})")
        return ValidationResult(name, False, row_count, 0, null_counts, messages)
    messages.append(f"NOTE: [validate_table] Row count OK: {row_count} (min: {min_rows})")

    dup = 0
    if key_cols:
        dup = (df.groupBy(*key_cols).count()
                 .filter(F.col("count") > 1).count())
        if dup > 0:
            messages.append(
                f"ERROR: [validate_table] {name} has {dup} duplicate key groups "
                f"on ({' '.join(key_cols)})")
            return ValidationResult(name, False, row_count, dup, null_counts, messages)
        messages.append(
            f"NOTE: [validate_table] Key uniqueness OK on ({' '.join(key_cols)})")

    for col in not_null:
        cnt = df.filter(F.col(col).isNull()).count()
        null_counts[col] = cnt
        if cnt > 0:
            messages.append(
                f"WARNING: [validate_table] {name}.{col} has {cnt} NULL values")
        else:
            messages.append(f"NOTE: [validate_table] NOT NULL check passed for {col}")

    messages.append(f"NOTE: [validate_table] All checks passed for {name}")
    for m in messages:
        print(m)
    return ValidationResult(name, True, row_count, dup, null_counts, messages)


def raise_if_failed(result: ValidationResult) -> ValidationResult:
    if not result.passed:
        raise ValueError(
            f"Validation failed for {result.name}: "
            + "; ".join(m for m in result.messages if m.startswith("ERROR")))
    return result
