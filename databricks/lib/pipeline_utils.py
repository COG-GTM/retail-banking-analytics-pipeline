# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Utilities
# MAGIC
# MAGIC PySpark ports of the shared SAS macros:
# MAGIC
# MAGIC | SAS macro                         | Python helper                       |
# MAGIC |-----------------------------------|-------------------------------------|
# MAGIC | `%init_audit`                     | `ensure_audit_table()`              |
# MAGIC | `%log_step(step=, status=, ...)`  | `log_step(step, status, ...)`       |
# MAGIC | `%validate_table(...)`            | `validate_table(...)` -> rc int     |
# MAGIC
# MAGIC Audit records are written to the Delta table `AUDIT_TABLE`
# MAGIC (`<catalog>.etl_staging.etl_run_log`), which replaces both the Teradata
# MAGIC `ETL_RUN_LOG` table and the SAS in-session `WORK.PIPELINE_AUDIT` dataset.
# MAGIC
# MAGIC `%run` this notebook *after* `../config/pipeline_config` so that the
# MAGIC `AUDIT_TABLE` / `spark` names are available in the shared namespace.

# COMMAND ----------

from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

# COMMAND ----------


def guard(phase: str, step: str) -> None:
    """Honour the DRY_RUN / SKIP_BTEQ / SKIP_SAS run-control flags.

    Replaces the orchestrator's --dry-run / --skip-bteq / --skip-sas behaviour.
    ``phase`` is "bteq" (staging) or "sas" (analytics). If the step should be
    skipped, this calls ``dbutils.notebook.exit`` and the notebook stops.
    """
    if DRY_RUN:  # noqa: F821 (from pipeline_config)
        msg = f"[DRY RUN] would execute {step} (phase={phase})"
        print(msg)
        dbutils.notebook.exit(msg)  # noqa: F821
    if phase == "bteq" and SKIP_BTEQ:  # noqa: F821
        dbutils.notebook.exit(f"{step} skipped (--skip-bteq)")  # noqa: F821
    if phase == "sas" and SKIP_SAS:  # noqa: F821
        dbutils.notebook.exit(f"{step} skipped (--skip-sas)")  # noqa: F821


def ensure_audit_table(audit_table: str | None = None) -> None:
    """Create the ETL audit Delta table if it does not already exist.

    Port of the SAS %init_audit macro.
    """
    audit_table = audit_table or AUDIT_TABLE  # noqa: F821 (from pipeline_config)
    spark.sql(  # noqa: F821
        f"""
        CREATE TABLE IF NOT EXISTS {audit_table} (
            job_name   STRING,
            step_name  STRING,
            status     STRING,
            message    STRING,
            row_count  BIGINT,
            start_ts   TIMESTAMP,
            end_ts     TIMESTAMP
        ) USING DELTA
        """
    )


# COMMAND ----------


def log_step(
    step: str,
    status: str,
    msg: str = "",
    rowcount: int | None = None,
    job_name: str | None = None,
    audit_table: str | None = None,
) -> None:
    """Emit a standardised log line and append a row to the audit Delta table.

    Port of the SAS %log_step macro. ``status`` is one of
    START | SUCCESS | WARNING | ERROR.
    """
    audit_table = audit_table or AUDIT_TABLE  # noqa: F821
    job_name = job_name or step
    now = datetime.now()

    print("NOTE: ================================================================")
    print(f"NOTE: [PIPELINE] {now:%Y-%m-%d %H:%M:%S.%f} | {step} | {status}")
    if msg:
        print(f"NOTE: [PIPELINE] {msg}")
    if rowcount is not None:
        print(f"NOTE: [PIPELINE] Rows: {rowcount:,}")
    print("NOTE: ================================================================")

    row = Row(
        job_name=job_name,
        step_name=step,
        status=status,
        message=msg,
        row_count=int(rowcount) if rowcount is not None else None,
        start_ts=now,
        end_ts=now,
    )
    (
        spark.createDataFrame([row])  # noqa: F821
        .select("job_name", "step_name", "status", "message", "row_count", "start_ts", "end_ts")
        .write.mode("append")
        .saveAsTable(audit_table)
    )


# COMMAND ----------


def validate_table(
    table: str,
    key_cols: list[str] | None = None,
    not_null: list[str] | None = None,
    min_rows: int = 1,
) -> int:
    """Run standard data-quality checks against a Delta table.

    Port of the SAS %validate_table macro. Returns 0 on pass, 1 on fail.

      * row count >= ``min_rows``                 -> FAIL if below
      * uniqueness of ``key_cols``                -> FAIL on duplicate keys
      * ``not_null`` columns have no NULLs        -> WARNING only (matches SAS)
    """
    key_cols = key_cols or []
    not_null = not_null or []

    print(f"NOTE: [validate_table] Validating {table}")

    # Check 1: minimum row count
    nobs = spark.table(table).count()  # noqa: F821
    if nobs < min_rows:
        print(f"ERROR: [validate_table] {table} has {nobs} rows (minimum: {min_rows})")
        return 1
    print(f"NOTE: [validate_table] Row count OK: {nobs} (min: {min_rows})")

    # Check 2: primary-key uniqueness
    if key_cols:
        dup_cnt = (
            spark.table(table)  # noqa: F821
            .groupBy(*key_cols)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if dup_cnt > 0:
            print(
                f"ERROR: [validate_table] {table} has {dup_cnt} duplicate key "
                f"groups on ({', '.join(key_cols)})"
            )
            return 1
        print(f"NOTE: [validate_table] Key uniqueness OK on ({', '.join(key_cols)})")

    # Check 3: NOT NULL constraints (warning only, as in the SAS macro)
    for col in not_null:
        null_cnt = spark.table(table).filter(F.col(col).isNull()).count()  # noqa: F821
        if null_cnt > 0:
            print(f"WARNING: [validate_table] {table}.{col} has {null_cnt} NULL values")
        else:
            print(f"NOTE: [validate_table] NOT NULL check passed for {col}")

    print(f"NOTE: [validate_table] All checks passed for {table}")
    return 0
