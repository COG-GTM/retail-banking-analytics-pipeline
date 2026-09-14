from dataclasses import dataclass
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from .audit import StepLogger


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class DQResult:
    row_count: int
    dup_groups: int
    null_counts: dict[str, int]


def validate_table(
    spark: SparkSession,
    fqn: str,
    key_cols: list[str],
    not_null_cols: list[str],
    min_rows: int,
) -> DQResult:
    df = spark.table(fqn)
    row_count = df.count()
    if row_count < min_rows:
        raise DataQualityError(f"{fqn} has {row_count} rows (minimum: {min_rows})")
    dup_groups = df.groupBy(*key_cols).count().where(col("count") > 1).count() if key_cols else 0
    if dup_groups:
        raise DataQualityError(f"{fqn} has {dup_groups} duplicate key groups")
    null_counts = {name: df.where(col(name).isNull()).count() for name in not_null_cols}
    warnings = {name: count for name, count in null_counts.items() if count}
    if warnings:
        parts = fqn.split(".")
        catalog = ".".join(parts[:-2]) or None

        class _WarnConfig:
            silver_schema = parts[-2]
            run_date = date.today()  # noqa: DTZ011

            @staticmethod
            def fqn(schema, table):
                return f"{catalog}.{schema}.{table}" if catalog else f"{schema}.{table}"

        StepLogger.log_step(
            spark,
            _WarnConfig(),
            "dq",
            "validate_table",
            "WARN",
            f"null values: {warnings}",
            row_count,
        )
    return DQResult(row_count, dup_groups, null_counts)
