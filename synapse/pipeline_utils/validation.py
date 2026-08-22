"""Data quality validation.

Replaces ``%validate_table``. Checks are the same family as the macro
(minimum row count, key uniqueness, null rates) plus generic threshold
assertions; a failing check raises :class:`ValidationError`, which is the
``%ABORT CANCEL`` equivalent and aborts the job.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class ValidationError(Exception):
    """Raised when a validation check fails; aborts the run."""


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


@dataclass
class ValidationReport:
    table: str
    checks: list[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failures(self) -> list[CheckResult]:
        return [check for check in self.checks if not check.passed]

    def summary(self) -> str:
        lines = [f"[validate_table] {self.table}"]
        lines += [
            f"  {'PASS' if c.passed else 'FAIL'} {c.name}: {c.detail}"
            for c in self.checks
        ]
        return "\n".join(lines)

    def abort_if_failed(self) -> ValidationReport:
        if not self.passed:
            raise ValidationError(
                f"Validation failed for {self.table}: "
                + "; ".join(f"{c.name} -> {c.detail}" for c in self.failures)
            )
        return self


def validate_table(
    df: DataFrame,
    table: str,
    key_cols: Sequence[str] = (),
    not_null: Sequence[str] = (),
    min_rows: int = 1,
    max_null_rate: float = 0.0,
    thresholds: Sequence[tuple[str, Callable[[int], bool], str]] = (),
) -> ValidationReport:
    """Run the standard quality checks and return a report.

    ``thresholds`` holds ``(name, predicate, description)`` triples evaluated
    against the row count, for job-specific assertions.
    """
    report = ValidationReport(table=table)
    row_count = df.count()

    report.checks.append(
        CheckResult(
            name="row_count",
            passed=row_count >= min_rows,
            detail=f"{row_count} rows (minimum {min_rows})",
        )
    )
    if row_count == 0:
        return report

    if key_cols:
        duplicate_groups = (
            df.groupBy(*key_cols).count().where(F.col("count") > 1).count()
        )
        report.checks.append(
            CheckResult(
                name="key_uniqueness",
                passed=duplicate_groups == 0,
                detail=(
                    f"{duplicate_groups} duplicate key groups on "
                    f"({', '.join(key_cols)})"
                ),
            )
        )

    if not_null:
        null_counts = df.select(
            *[
                F.sum(F.col(col).isNull().cast("long")).alias(col)
                for col in not_null
            ]
        ).collect()[0]
        for col in not_null:
            nulls = int(null_counts[col] or 0)
            null_rate = nulls / row_count
            report.checks.append(
                CheckResult(
                    name=f"null_rate[{col}]",
                    passed=null_rate <= max_null_rate,
                    detail=(
                        f"{nulls} nulls ({null_rate:.4%}), "
                        f"maximum {max_null_rate:.4%}"
                    ),
                )
            )

    for name, predicate, description in thresholds:
        report.checks.append(
            CheckResult(
                name=name,
                passed=bool(predicate(row_count)),
                detail=f"{description} (row count {row_count})",
            )
        )

    return report
