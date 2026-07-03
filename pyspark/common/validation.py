"""Data-quality validation ported from ``sas/macros/validate_table.sas``.

Reproduces the macro's three checks and its ``&VALIDATION_RC`` semantics exactly:

1. **Minimum rows** -- ``count(*) >= min_rows`` else ``rc = 1`` (fatal, returns).
2. **Key uniqueness** -- no duplicate ``key_cols`` groups else ``rc = 1`` (fatal).
3. **NOT NULL** -- columns in ``not_null`` are checked; violations emit a
   ``WARNING`` but, matching the SAS macro, do **not** set ``rc`` (non-fatal).

Callers gate the Teradata load on ``result.rc != 0`` (the ``%if &VALIDATION_RC
ne 0 %then %abort cancel`` pattern) -- see :func:`abort_on_failure`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .audit import AuditLog


class ValidationError(RuntimeError):
    """Raised by :func:`abort_on_failure` to replicate ``%abort cancel``."""


@dataclass
class ValidationResult:
    table: str
    rc: int = 0
    row_count: int = 0
    min_rows: int = 1
    duplicate_key_groups: int = 0
    null_counts: dict[str, int] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.rc == 0

    @property
    def warnings(self) -> list[str]:
        return [m for m in self.messages if m.startswith("WARNING")]


def validate_table(
    df: DataFrame,
    table: str,
    key_cols: list[str] | None = None,
    not_null: list[str] | None = None,
    min_rows: int = 1,
    audit: AuditLog | None = None,
) -> ValidationResult:
    """Run the three ported checks and return a :class:`ValidationResult`."""
    key_cols = key_cols or []
    not_null = not_null or []
    result = ValidationResult(table=table, min_rows=min_rows)

    # Check 1: minimum row count (fatal).
    result.row_count = df.count()
    if result.row_count < min_rows:
        result.rc = 1
        result.messages.append(
            f"ERROR: {table} has {result.row_count} rows (minimum: {min_rows})"
        )
        if audit:
            audit.log_step(table, "ERROR", result.messages[-1])
        return result
    result.messages.append(f"NOTE: Row count OK: {result.row_count} (min: {min_rows})")

    # Check 2: primary key uniqueness (fatal).
    if key_cols:
        dup_groups = (
            df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        )
        result.duplicate_key_groups = dup_groups
        if dup_groups > 0:
            result.rc = 1
            result.messages.append(
                f"ERROR: {table} has {dup_groups} duplicate key groups on ({', '.join(key_cols)})"
            )
            if audit:
                audit.log_step(table, "ERROR", result.messages[-1])
            return result
        result.messages.append(f"NOTE: Key uniqueness OK on ({', '.join(key_cols)})")

    # Check 3: NOT NULL constraints (warning only, matching the SAS macro).
    if not_null:
        null_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in not_null
        ]
        counts = df.agg(*null_exprs).collect()[0].asDict()
        for col in not_null:
            n = int(counts.get(col) or 0)
            result.null_counts[col] = n
            if n > 0:
                msg = f"WARNING: {table}.{col} has {n} NULL values"
                result.messages.append(msg)
                if audit:
                    audit.log_step(table, "WARNING", msg)
            else:
                result.messages.append(f"NOTE: NOT NULL check passed for {col}")

    return result


def abort_on_failure(result: ValidationResult) -> None:
    """Replicate ``%if &VALIDATION_RC ne 0 %then %abort cancel``."""
    if result.rc != 0:
        raise ValidationError(
            f"Validation failed for {result.table}: " + "; ".join(result.messages)
        )
