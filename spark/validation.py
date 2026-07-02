"""Data-quality validation, the PySpark analogue of the SAS ``%validate_table``.

Runs the same three checks: minimum row count, primary-key uniqueness, and
NOT NULL columns. Returns a structured result whose ``rc`` field mirrors the
SAS ``&VALIDATION_RC`` global (0 = pass, 1 = fail).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .logging_utils import PipelineAudit


class ValidationError(RuntimeError):
    """Raised when a validation gate fails (analogue of ``%abort cancel``)."""


@dataclass
class ValidationResult:
    rc: int = 0
    row_count: int = 0
    messages: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.rc == 0


def validate_table(
    df: DataFrame,
    table: str,
    key_cols: Optional[Sequence[str]] = None,
    not_null: Optional[Sequence[str]] = None,
    min_rows: int = 1,
    audit: Optional[PipelineAudit] = None,
) -> ValidationResult:
    """Validate ``df`` and return a :class:`ValidationResult`.

    Mirrors ``%validate_table``: a row-count shortfall or duplicate keys set
    ``rc = 1`` (hard fail); NULLs in ``not_null`` columns are logged as
    warnings only (matching the SAS macro, which only ``%put WARNING``).
    """
    result = ValidationResult()
    key_cols = list(key_cols or [])
    not_null = list(not_null or [])

    def _msg(text: str) -> None:
        result.messages.append(text)

    # Check 1: minimum row count -------------------------------------------
    nobs = df.count()
    result.row_count = nobs
    if nobs < min_rows:
        result.rc = 1
        _msg(f"{table} has {nobs} rows (minimum: {min_rows})")
        return result
    _msg(f"Row count OK: {nobs} (min: {min_rows})")

    # Check 2: primary-key uniqueness --------------------------------------
    if key_cols:
        dup_groups = (
            df.groupBy(*key_cols).count().where(F.col("count") > 1).count()
        )
        if dup_groups > 0:
            result.rc = 1
            _msg(f"{table} has {dup_groups} duplicate key groups on ({', '.join(key_cols)})")
            return result
        _msg(f"Key uniqueness OK on ({', '.join(key_cols)})")

    # Check 3: NOT NULL columns (warning only) -----------------------------
    for col in not_null:
        null_cnt = df.where(F.col(col).isNull()).count()
        if null_cnt > 0:
            _msg(f"WARNING: {table}.{col} has {null_cnt} NULL values")
            if audit is not None:
                audit.log_step(step=f"validate:{table}", status="WARNING",
                               msg=f"{col} has {null_cnt} NULL values")

    return result


def enforce(result: ValidationResult, table: str, audit: PipelineAudit) -> None:
    """Abort the job (``%abort cancel``) if validation failed."""
    if not result.passed:
        detail = "; ".join(result.messages)
        audit.log_step(step=f"validate:{table}", status="ERROR",
                       msg=f"Validation failed - aborting load: {detail}")
        raise ValidationError(f"Validation failed for {table}: {detail}")
