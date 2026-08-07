"""Data quality checks.

Direct port of ``%validate_table`` (``sas/macros/validate_table.sas``), keeping
the original severities:

* row count below ``min_rows``          -> failure (SAS ``VALIDATION_RC=1``)
* duplicate key groups                  -> failure
* NULLs in ``not_null`` columns         -> warning (SAS ``%put WARNING:``)

The same rule set is exposed as :func:`expectations` so a Delta Live Tables
pipeline can attach it with ``@dlt.expect_all`` instead of running the checks
imperatively.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from shared.audit import log_step
from shared.config import PipelineConfig
from shared.logging_utils import get_logger, log_event

_logger = get_logger()


class ValidationError(Exception):
    """Raised when a blocking data quality check fails (SAS ``%abort cancel``)."""


@dataclass
class ValidationResult:
    """Outcome of validating a single table."""

    table: str
    row_count: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors

    @property
    def rc(self) -> int:
        """SAS-compatible return code (0 = pass, 1 = fail)."""
        return 0 if self.passed else 1

    def summary(self) -> str:
        state = "PASS" if self.passed else "FAIL"
        parts = [f"[validate_table] {self.table}: {state} ({self.row_count:,} rows)"]
        parts += [f"  ERROR: {e}" for e in self.errors]
        parts += [f"  WARNING: {w}" for w in self.warnings]
        return "\n".join(parts)


def validate_table(
    spark: SparkSession,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int = 1,
    df: DataFrame | None = None,
) -> ValidationResult:
    """Run the standard checks against ``table`` (or an already-built ``df``)."""
    frame = df if df is not None else spark.table(table)
    row_count = frame.count()
    result = ValidationResult(table=table, row_count=row_count)

    if row_count < min_rows:
        result.errors.append(f"has {row_count:,} rows (minimum: {min_rows:,})")
        return result  # SAS %return: no further checks once the count fails

    if key_cols:
        dup_groups = (
            frame.groupBy(*[F.col(c) for c in key_cols]).count().filter(F.col("count") > 1).count()
        )
        if dup_groups:
            result.errors.append(
                f"has {dup_groups:,} duplicate key groups on ({', '.join(key_cols)})"
            )
            return result

    if not_null:
        null_counts = frame.select(
            *[F.sum(F.col(c).isNull().cast("long")).alias(c) for c in not_null]
        ).collect()[0]
        for column in not_null:
            nulls = null_counts[column] or 0
            if nulls:
                result.warnings.append(f"{column} has {nulls:,} NULL values")

    return result


def validate_and_log(
    spark: SparkSession,
    cfg: PipelineConfig,
    job_name: str,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int | None = None,
    df: DataFrame | None = None,
    raise_on_failure: bool = True,
) -> ValidationResult:
    """Validate, write the outcome to ``ETL_RUN_LOG``, and optionally raise."""
    result = validate_table(
        spark,
        table,
        key_cols=key_cols,
        not_null=not_null,
        min_rows=cfg.min_rows if min_rows is None else min_rows,
        df=df,
    )
    log_event(
        _logger,
        "validate_table",
        level=logging.INFO if result.passed else logging.ERROR,
        run_id=cfg.run_id,
        job=job_name,
        table=table,
        status="PASS" if result.passed else "FAIL",
        row_count=result.row_count,
        errors=result.errors or None,
        warnings=result.warnings or None,
    )
    log_step(
        spark,
        cfg,
        job_name,
        f"VALIDATE:{table.split('.')[-1]}",
        "SUCCESS" if result.passed else "ERROR",
        message="; ".join(result.errors + result.warnings)[:1000],
        row_count=result.row_count,
    )
    if raise_on_failure and not result.passed:
        raise ValidationError(result.summary())
    return result


def expectations(
    key_cols: Sequence[str] = (), not_null: Sequence[str] = ()
) -> dict[str, str]:
    """Rule set for ``@dlt.expect_all`` / ``@dlt.expect_all_or_fail``.

    Uniqueness is not expressible as a row-level expectation, so DLT pipelines
    should keep :func:`validate_table` for the key check.
    """
    rules = {f"{c.lower()}_not_null": f"{c} IS NOT NULL" for c in not_null}
    rules.update({f"{c.lower()}_key_not_null": f"{c} IS NOT NULL" for c in key_cols})
    return rules
