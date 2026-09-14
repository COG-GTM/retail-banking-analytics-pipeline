from collections.abc import Iterator
from contextlib import contextmanager

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from .tables import append, ensure_schema


class ZeroRowsError(RuntimeError):
    pass


_AUDIT_SCHEMA = StructType(
    [
        StructField("job_name", StringType(), False),
        StructField("step_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("message", StringType(), True),
        StructField("row_count", LongType(), True),
        StructField("run_date", DateType(), False),
        StructField("log_ts", TimestampType(), False),
    ]
)


class StepLogger:
    @staticmethod
    def log_step(spark, cfg, job_name, step_name, status, message, row_count):
        fqn = cfg.fqn(cfg.silver_schema, "etl_run_log")
        ensure_schema(spark, fqn)
        row = spark.createDataFrame(
            [(job_name, step_name, status, message, row_count, cfg.run_date)],
            StructType(_AUDIT_SCHEMA.fields[:-1]),
        ).withColumn("log_ts", current_timestamp())
        append(row, fqn)


@contextmanager
def step(spark, cfg, job, step_name) -> Iterator[dict[str, int | None]]:
    result: dict[str, int | None] = {"row_count": None}
    try:
        yield result
        StepLogger.log_step(
            spark, cfg, job, step_name, "SUCCESS", "step completed", result["row_count"]
        )
    except Exception as exc:
        StepLogger.log_step(spark, cfg, job, step_name, "FAILED", str(exc), result["row_count"])
        raise


def assert_rows(df_or_count: DataFrame | int, name: str) -> int:
    count = df_or_count.count() if isinstance(df_or_count, DataFrame) else int(df_or_count)
    if count == 0:
        raise ZeroRowsError(f"{name} returned zero rows")
    return count
