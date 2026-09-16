# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Resolve run mode
# MAGIC Mirrors run_full_pipeline.sh argument handling: --skip-bteq -> skip_silver,
# MAGIC --skip-sas -> skip_gold, --dry-run lists steps and runs nothing.

# COMMAND ----------

dbutils.widgets.text("catalog", "retail_banking_dev")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("lookback_months", "12")
dbutils.widgets.text("skip_silver", "false")
dbutils.widgets.text("skip_gold", "false")
dbutils.widgets.text("dry_run", "false")

skip_silver = dbutils.widgets.get("skip_silver").lower() == "true"
skip_gold = dbutils.widgets.get("skip_gold").lower() == "true"
dry_run = dbutils.widgets.get("dry_run").lower() == "true"

run_silver = "false" if (skip_silver or dry_run) else "true"
run_gold = "false" if (skip_gold or dry_run) else "true"

if dry_run:
    print("DRY RUN mode - planned steps only:")
    print("  1. bronze_ingest")
    print("  2. silver: stg_customer_360, stg_txn_summary, stg_risk_factors")
    print("  3. gold: customer_segments, transaction_analytics, "
          "customer_risk_scores, customer_master_profile")
    print("  4. row_count_validation, parity_check")

dbutils.jobs.taskValues.set("run_silver", run_silver)
dbutils.jobs.taskValues.set("run_gold", run_gold)
print(f"run_silver={run_silver} run_gold={run_gold}")
