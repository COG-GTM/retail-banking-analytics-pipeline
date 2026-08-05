"""Data quality validation.

Port of ``sas/macros/validate_table.sas``. The three checks and their *severities* are kept
exactly as the macro defines them:

1. minimum row count - failure (``VALIDATION_RC=1``, macro returns immediately);
2. primary key uniqueness - failure (returns immediately);
3. NOT NULL columns - **warning only**, the macro logs ``WARNING:`` and continues.

``%abort cancel`` in the callers becomes :func:`abort_on_failure`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOGGER = logging.getLogger(__name__)

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


class ValidationFailedError(RuntimeError):
    """Raised in place of the legacy ``%abort cancel``."""


@dataclass(frozen=True)
class ValidationCheck:
    name: str
    status: str
    detail: str

    @property
    def failed(self) -> bool:
        return self.status == FAIL


@dataclass
class ValidationResult:
    table: str
    checks: list[ValidationCheck] = field(default_factory=list)
    row_count: int = 0

    @property
    def rc(self) -> int:
        """Mirror of the legacy ``&VALIDATION_RC`` macro variable."""

        return 1 if any(check.failed for check in self.checks) else 0

    @property
    def passed(self) -> bool:
        return self.rc == 0

    @property
    def warnings(self) -> list[ValidationCheck]:
        return [check for check in self.checks if check.status == WARN]

    @property
    def failures(self) -> list[ValidationCheck]:
        return [check for check in self.checks if check.failed]

    def as_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "rc": self.rc,
            "row_count": self.row_count,
            "checks": [
                {"name": check.name, "status": check.status, "detail": check.detail}
                for check in self.checks
            ],
        }


def validate_table(
    df: DataFrame,
    *,
    table: str,
    key_cols: tuple[str, ...] | list[str] = (),
    not_null: tuple[str, ...] | list[str] = (),
    min_rows: int = 1,
) -> ValidationResult:
    """Run the legacy checks against ``df`` and return the aggregated result."""

    result = ValidationResult(table=table)
    row_count = df.count()
    result.row_count = row_count

    if row_count < min_rows:
        result.checks.append(
            ValidationCheck(
                "min_rows",
                FAIL,
                f"{table} has {row_count} rows (minimum: {min_rows})",
            )
        )
        LOGGER.error("[validate_table] %s has %s rows (minimum: %s)", table, row_count, min_rows)
        return result
    result.checks.append(
        ValidationCheck("min_rows", PASS, f"row count OK: {row_count} (min: {min_rows})")
    )

    if key_cols:
        duplicate_groups = (
            df.groupBy(*[F.col(f"`{column}`") for column in key_cols])
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_groups > 0:
            result.checks.append(
                ValidationCheck(
                    "key_uniqueness",
                    FAIL,
                    f"{table} has {duplicate_groups} duplicate key groups on ({', '.join(key_cols)})",
                )
            )
            LOGGER.error(
                "[validate_table] %s has %s duplicate key groups on (%s)",
                table,
                duplicate_groups,
                ", ".join(key_cols),
            )
            return result
        result.checks.append(
            ValidationCheck("key_uniqueness", PASS, f"key uniqueness OK on ({', '.join(key_cols)})")
        )

    for column in not_null:
        null_count = df.filter(F.col(f"`{column}`").isNull()).count()
        if null_count > 0:
            result.checks.append(
                ValidationCheck(
                    f"not_null.{column}", WARN, f"{table}.{column} has {null_count} NULL values"
                )
            )
            LOGGER.warning("[validate_table] %s.%s has %s NULL values", table, column, null_count)
        else:
            result.checks.append(
                ValidationCheck(f"not_null.{column}", PASS, f"NOT NULL check passed for {column}")
            )

    return result


def abort_on_failure(result: ValidationResult) -> ValidationResult:
    """Raise when validation failed - the port of ``%if &VALIDATION_RC ne 0 %then %abort cancel``."""

    if not result.passed:
        details = "; ".join(check.detail for check in result.failures)
        raise ValidationFailedError(f"validation failed for {result.table}: {details}")
    return result
