# Databricks notebook source
# MAGIC %md
# MAGIC # Final validation
# MAGIC
# MAGIC Replaces the `envsubst`-generated BTEQ block at the end of
# MAGIC `orchestration/run_full_pipeline.sh`.
# MAGIC
# MAGIC 1. Row-count check on the four gold data products.
# MAGIC 2. Schema contract check: every gold table must match
# MAGIC    `ddl/02_data_product_tables.sql` column-for-column — names, types and
# MAGIC    ordering — as transcribed in `shared.schemas.GOLD_SCHEMAS`.

# COMMAND ----------

import os
import sys
from pathlib import Path

for _p in [os.getcwd(), *[str(p) for p in Path(os.getcwd()).parents]]:
    if os.path.isdir(os.path.join(_p, "shared")):
        if _p not in sys.path:
            sys.path.insert(0, _p)
        break

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from shared.audit import AUDIT_TABLE, AuditLogger
from shared.dq import DataQualityError
from shared.runtime import PipelineConfig, get_int_param, get_param, in_databricks
from shared.schemas import GOLD_SCHEMAS

JOB_NAME = "99_validate_data_products"

# COMMAND ----------


def schema_mismatches(spark: SparkSession, table: str, spec: list[tuple[str, str]]) -> list[str]:
    """Compare the deployed table against the DDL contract, position by position."""
    actual = [(f.name, f.dataType.simpleString()) for f in spark.table(table).schema.fields]
    problems = []
    if len(actual) != len(spec):
        problems.append(f"{table}: expected {len(spec)} columns, found {len(actual)}")
    for position, (expected, found) in enumerate(zip(spec, actual)):
        if expected != found:
            problems.append(
                f"{table}: column {position + 1} expected {expected[0]} {expected[1]}, "
                f"found {found[0]} {found[1]}"
            )
    return problems


def validate_gold(spark: SparkSession, cfg: PipelineConfig, min_rows: int) -> int:
    problems: list[str] = []
    total = 0

    for table_name, spec in GOLD_SCHEMAS.items():
        table = cfg.gold(table_name)
        rows = spark.table(table).count()
        total += rows
        print(f"{table:<60} {rows:>10,} rows")
        if rows < min_rows:
            problems.append(f"{table}: {rows} rows, expected at least {min_rows}")
        problems.extend(schema_mismatches(spark, table, spec))

    duplicates = (
        spark.table(cfg.gold("CUSTOMER_MASTER_PROFILE"))
        .groupBy("CUSTOMER_ID")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if duplicates:
        problems.append(f"CUSTOMER_MASTER_PROFILE: {duplicates} duplicate CUSTOMER_ID values")

    if problems:
        raise DataQualityError("Gold validation failed:\n  " + "\n  ".join(problems))

    print("\nAll four gold data products match ddl/02_data_product_tables.sql.")
    return total


# COMMAND ----------

if in_databricks():
    cfg = PipelineConfig.from_widgets()
    audit = AuditLogger(spark, cfg.ops(AUDIT_TABLE), JOB_NAME, get_param("run_id", ""))

    with audit.step("VALIDATE_GOLD", "row counts + schema contract") as ctx:
        ctx.row_count = validate_gold(spark, cfg, get_int_param("min_gold_rows", "1000"))

    spark.table(cfg.ops(AUDIT_TABLE)).orderBy(F.col("LOG_TS").desc()).show(50, truncate=False)
