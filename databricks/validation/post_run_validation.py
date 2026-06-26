# Databricks notebook source
# MAGIC %md
# MAGIC # Post-Run Validation
# MAGIC
# MAGIC Replaces the final BTEQ row-count query in
# MAGIC `orchestration/run_full_pipeline.sh` (Phase 3). Reports row counts for the
# MAGIC four data product tables and fails the job if any is below `min_rows`.

# COMMAND ----------

# MAGIC %run ../config/pipeline_config

# COMMAND ----------

# MAGIC %run ../lib/pipeline_utils

# COMMAND ----------

dbutils.widgets.text("min_rows", "1", "Minimum acceptable rows per product table")
MIN_ROWS = int(dbutils.widgets.get("min_rows"))

ensure_audit_table()
log_step(step="post_run_validation", status="START", msg="Validating data products")

# COMMAND ----------

product_tables = [
    dp("customer_segments"),
    dp("transaction_analytics"),
    dp("customer_risk_scores"),
    dp("customer_master_profile"),
]

failures = []
print(f"{'Table':<55} {'Rows':>12}")
print("-" * 70)
for table in product_tables:
    cnt = spark.table(table).count()  # noqa: F821
    print(f"{table:<55} {cnt:>12,}")
    if cnt < MIN_ROWS:
        failures.append((table, cnt))

# COMMAND ----------

if failures:
    detail = ", ".join(f"{t} ({c} rows)" for t, c in failures)
    log_step(step="post_run_validation", status="ERROR", msg=f"Below min_rows: {detail}")
    raise Exception(f"Post-run validation failed for: {detail}")

log_step(step="post_run_validation", status="SUCCESS", msg="All data products validated")
dbutils.notebook.exit("post_run_validation: OK")  # noqa: F821
