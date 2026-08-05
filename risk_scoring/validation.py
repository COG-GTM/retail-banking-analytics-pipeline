"""STEP 5 — the PySpark port of ``sas/macros/validate_table.sas``.

The macro's three checks are deliberately asymmetric and that asymmetry is
part of the contract:

===================  ===============================================  ==========
Check                SAS                                              Here
===================  ===============================================  ==========
row count < min_rows ``ERROR``, ``VALIDATION_RC=1``, ``%return``       error, ``passed=False``, remaining checks skipped
duplicate key_cols   ``ERROR``, ``VALIDATION_RC=1``, ``%return``       error, ``passed=False``, remaining checks skipped
NULLs in not_null    ``WARNING`` only, ``VALIDATION_RC`` untouched     warning only, ``passed`` unaffected
===================  ===============================================  ==========

Because both error branches ``%return``, a table that fails the row count never
has its keys checked, and a table that fails either never has its NOT NULL
columns checked. :func:`validate_table` returns the result rather than raising;
the caller raises :class:`ValidationError` to mirror ``%abort cancel``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .audit import AuditLog

logger = logging.getLogger(__name__)


class ValidationError(RuntimeError):
    """Raised by the caller when a :class:`ValidationResult` fails (``%abort cancel``)."""


@dataclass
class ValidationResult:
    table: str
    row_count: int
    passed: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _missing_expr(df: DataFrame, column: str):
    """``where <column> is missing`` for one column.

    SAS treats a blank character value as missing, so string columns count
    empty/whitespace-only values alongside NULLs; numeric columns only count
    NULL (the SAS ``.``).
    """
    col = F.col(column)
    if isinstance(df.schema[column].dataType, StringType):
        return col.isNull() | (F.trim(col) == F.lit(""))
    return col.isNull()


def _require_columns(df: DataFrame, table: str, columns: Sequence[str]) -> None:
    unknown = [c for c in columns if c not in df.columns]
    if unknown:
        raise ValueError(
            f"[validate_table] {table} has no column(s) {', '.join(unknown)}; "
            f"available: {', '.join(df.columns)}"
        )


def validate_table(
    df: DataFrame,
    *,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int = 1,
    audit: AuditLog | None = None,
) -> ValidationResult:
    """Run the ``%validate_table`` checks against ``df``.

    ``min_rows`` comes from ``PipelineConfig.min_rows`` at the call site rather
    than the hardcoded ``1000`` of the SAS program.
    """
    key_cols = list(key_cols)
    not_null = list(not_null)
    _require_columns(df, table, [*key_cols, *not_null])

    logger.info("validate_table table=%s min_rows=%s", table, min_rows)
    row_count = df.count()  # single action; reused by every check below
    result = ValidationResult(table=table, row_count=row_count, passed=True)

    if row_count < min_rows:
        _fail(
            result,
            f"{table} has {row_count} rows (minimum: {min_rows})",
            audit=audit,
        )
        return result  # %return — the key and NOT NULL checks never run
    logger.info("validate_table table=%s row_count_ok=%s", table, row_count)

    if key_cols:
        dup_groups = (
            df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
        )
        if dup_groups > 0:
            _fail(
                result,
                f"{table} has {dup_groups} duplicate key groups on "
                f"({' '.join(key_cols)})",
                audit=audit,
            )
            return result  # %return — the NOT NULL check never runs
        logger.info(
            "validate_table table=%s key_uniqueness_ok=%s", table, " ".join(key_cols)
        )

    if not_null:
        # One pass for every column, not one job per column.
        counts = df.select(*[
            F.sum(_missing_expr(df, c).cast("int")).alias(c) for c in not_null
        ]).first()
        for column in not_null:
            null_count = counts[column] or 0
            if null_count > 0:
                message = f"{table}.{column} has {null_count} NULL values"
                result.warnings.append(message)
                logger.warning("validate_table %s", message)
                if audit is not None:
                    audit.log_step(
                        step=f"VALIDATE_{table}",
                        status="WARNING",
                        msg=message,
                        rowcount=null_count,
                    )

    if audit is not None:
        audit.log_step(
            step=f"VALIDATE_{table}",
            status="SUCCESS",
            msg=f"All checks passed for {table}",
            rowcount=row_count,
        )
    return result


def _fail(result: ValidationResult, message: str, *, audit: AuditLog | None) -> None:
    result.passed = False
    result.errors.append(message)
    logger.error("validate_table %s", message)
    if audit is not None:
        audit.log_step(
            step=f"VALIDATE_{result.table}",
            status="ERROR",
            msg=message,
            rowcount=result.row_count,
        )
