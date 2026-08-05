"""Regression harness: compare a ported job's output against the legacy reference output.

The reference CSVs under ``data/02_bteq_staging`` and ``data/03_sas_data_products`` are the
outputs of a previous run of the legacy pipeline. They are never edited; where a column cannot
match exactly, the tolerance is declared here and justified in ``MIGRATION_NOTES.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.schemas import TableSpec


@dataclass
class ColumnDiff:
    column: str
    mismatches: int
    max_abs_diff: float | None
    examples: list[tuple[object, object, object]] = field(default_factory=list)


@dataclass
class ParityReport:
    table: str
    expected_rows: int
    actual_rows: int
    missing_keys: int
    extra_keys: int
    column_diffs: list[ColumnDiff] = field(default_factory=list)

    @property
    def matched_rows(self) -> int:
        return self.expected_rows - self.missing_keys

    def summary(self) -> str:
        lines = [
            f"{self.table}: expected {self.expected_rows} rows, got {self.actual_rows} "
            f"(missing keys: {self.missing_keys}, extra keys: {self.extra_keys})"
        ]
        lines.extend(
            f"  {diff.column}: {diff.mismatches} mismatches, max|diff|={diff.max_abs_diff}, "
            f"e.g. {diff.examples[:3]}"
            for diff in self.column_diffs
        )
        return "\n".join(lines)

    def assert_parity(self) -> None:
        assert self.missing_keys == 0 and self.extra_keys == 0, self.summary()
        assert not self.column_diffs, self.summary()


def _is_numeric(dtype: str) -> bool:
    return dtype.startswith(("decimal", "double", "float", "int", "bigint", "smallint", "tinyint"))


def compare_to_reference(
    actual: DataFrame,
    expected: DataFrame,
    spec: TableSpec,
    *,
    key: tuple[str, ...] | None = None,
    tolerances: dict[str, float] | None = None,
    ignore: tuple[str, ...] = ("LOAD_TS",),
    example_limit: int = 3,
) -> ParityReport:
    """Key-wise comparison of ``actual`` against the legacy ``expected`` output.

    ``tolerances`` maps a column onto the maximum absolute difference accepted for it; every
    other column must match exactly. Columns in ``ignore`` are not compared at all.
    """

    keys = tuple(key or spec.primary_index)
    tolerances = tolerances or {}
    compared = [
        column.name
        for column in spec.columns
        if column.name not in keys and column.name not in ignore
    ]

    left = actual.select(*[F.col(name).alias(f"a_{name}") for name in spec.column_names])
    right = expected.select(*[F.col(name).alias(f"e_{name}") for name in spec.column_names])
    condition = [F.col(f"a_{name}").eqNullSafe(F.col(f"e_{name}")) for name in keys]
    joined = left.join(right, condition, "full_outer").persist()

    key_present = [F.col(f"a_{name}").isNotNull() for name in keys]
    expected_present = [F.col(f"e_{name}").isNotNull() for name in keys]
    actual_only = joined.filter(key_present[0] & ~expected_present[0]).count()
    expected_only = joined.filter(~key_present[0] & expected_present[0]).count()

    report = ParityReport(
        table=spec.qualified_name,
        expected_rows=expected.count(),
        actual_rows=actual.count(),
        missing_keys=expected_only,
        extra_keys=actual_only,
    )

    matched = joined.filter(key_present[0] & expected_present[0])
    dtypes = dict(actual.dtypes)
    for name in compared:
        actual_col, expected_col = F.col(f"a_{name}"), F.col(f"e_{name}")
        tolerance = tolerances.get(name)
        if tolerance is not None and _is_numeric(dtypes.get(name, "string")):
            delta = F.abs(actual_col.cast("double") - expected_col.cast("double"))
            mismatch = (actual_col.isNull() != expected_col.isNull()) | F.coalesce(
                delta > F.lit(tolerance), F.lit(False)
            )
        else:
            mismatch = ~actual_col.eqNullSafe(expected_col)
            delta = (
                F.abs(actual_col.cast("double") - expected_col.cast("double"))
                if _is_numeric(dtypes.get(name, "string"))
                else F.lit(None).cast("double")
            )

        offending = matched.filter(mismatch).select(
            *[F.col(f"a_{k}").alias(k) for k in keys],
            actual_col.alias("actual"),
            expected_col.alias("expected"),
            delta.alias("delta"),
        )
        stats = offending.agg(
            F.count(F.lit(1)).alias("n"), F.max("delta").alias("max_delta")
        ).collect()[0]
        if stats["n"]:
            examples = [
                (row[keys[0]], row["actual"], row["expected"])
                for row in offending.limit(example_limit).collect()
            ]
            max_delta = stats["max_delta"]
            report.column_diffs.append(
                ColumnDiff(
                    column=name,
                    mismatches=int(stats["n"]),
                    max_abs_diff=float(max_delta)
                    if isinstance(max_delta, (int, float, Decimal))
                    else None,
                    examples=examples,
                )
            )

    joined.unpersist()
    return report
