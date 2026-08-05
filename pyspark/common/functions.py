"""Shared column helpers for Teradata and SAS constructs that Spark has no direct match for.

Keeping them here means every job maps a given legacy construct the same way, and the mapping
is unit-tested once.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def run_date_col(run_date: date) -> Column:
    """The pinned ``CURRENT_DATE`` for the run.

    Every legacy script calls ``CURRENT_DATE`` / ``today()`` independently; pinning it to the
    configured run date keeps the whole DAG internally consistent and reproducible.
    """

    return F.lit(run_date).cast("date")


def td_date_diff_days(end: Column, start: Column) -> Column:
    """Teradata ``date1 - date2`` returns a whole number of days."""

    return F.datediff(end, start)


def td_months_between(end: Column, start: Column) -> Column:
    """Teradata ``MONTHS_BETWEEN`` truncated by ``CAST(... AS INTEGER)``.

    Teradata truncates towards zero when casting a decimal to INTEGER; Spark's ``months_between``
    uses the same Oracle-style 31-day fractional convention.
    """

    return F.months_between(end, start, False).cast("int")


def td_add_months(value: Column, months: int) -> Column:
    return F.add_months(value, months)


def qualify_row_number(
    df: DataFrame,
    partition_by: list[str] | tuple[str, ...],
    order_by: list[Column] | tuple[Column, ...],
    *,
    row_number_column: str = "_rn",
) -> DataFrame:
    """``QUALIFY ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1``.

    The caller is responsible for appending a deterministic tiebreaker to ``order_by``; the
    legacy ``QUALIFY`` clauses are non-deterministic when the ordering key ties.
    """

    window = Window.partitionBy(*partition_by).orderBy(*order_by)
    return (
        df.withColumn(row_number_column, F.row_number().over(window))
        .filter(F.col(row_number_column) == 1)
        .drop(row_number_column)
    )


def sas_round(value: Column, unit: float = 0.01) -> Column:
    """SAS ``round(x, unit)`` - half away from zero at the given unit."""

    scale = round(1 / unit)
    digits = len(str(scale)) - 1
    return F.round(value, digits)


def nullif_zero(value: Column) -> Column:
    """Teradata ``NULLIFZERO``."""

    return F.when(value == 0, F.lit(None)).otherwise(value)


def zero_if_null(value: Column, default: float = 0.0) -> Column:
    """Teradata ``COALESCE(x, 0)`` used pervasively in the BTEQ scripts."""

    return F.coalesce(value, F.lit(default))


def yn(condition: Column) -> Column:
    """SAS ``ifc(cond, 'Y', 'N')`` / Teradata ``CASE WHEN ... THEN 'Y' ELSE 'N' END``."""

    return F.when(condition, F.lit("Y")).otherwise(F.lit("N"))


def clamp_0_100(value: Column) -> Column:
    """SAS ``max(0, min(100, x))``."""

    return F.greatest(F.lit(0.0), F.least(F.lit(100.0), value))
